import { createClient } from "@supabase/supabase-js"
import YahooFinance from "yahoo-finance2"

type CompanyRow = {
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
}

type CandidateUniverseRow = {
  ticker: string
  cik: string
  name: string | null
  price: number | null
  market_cap: number | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  return_5d: number | null
  return_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  above_sma_20: boolean
  passes_price: boolean
  passes_volume: boolean
  passes_dollar_volume: boolean
  passes_market_cap: boolean
  candidate_score: number
  included: boolean
  screen_reason: string
  last_screened_at: string
  updated_at: string
}

type CandidateHistoryRow = CandidateUniverseRow & {
  screened_on: string
  snapshot_key: string
  created_at: string
}

type CandidateResult = {
  ticker: string | null
  ok: boolean
  included?: boolean
  score?: number
  reason?: string
  price?: number | null
  return5d?: number | null
  return20d?: number | null
  volumeRatio?: number | null
  error?: string
}

const yahooFinance = new YahooFinance({
  queue: { concurrency: 1 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 250
const DEFAULT_BATCH = 100
const RETENTION_DAYS = 30
const REQUEST_DELAY_MS = 120

const MIN_PRICE = 10
const MIN_AVG_VOLUME_20D = 750_000
const MIN_AVG_DOLLAR_VOLUME_20D = 20_000_000
const MIN_MARKET_CAP = 1_000_000_000

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function avg(nums: number[]) {
  if (!nums.length) return 0
  return nums.reduce((sum, n) => sum + n, 0) / nums.length
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function round2(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 100) / 100
}

function roundWhole(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value)
}

function parseInteger(value: string | null, fallback: number) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function isProbablyCommonStockTicker(ticker: string) {
  if (!ticker) return false

  const badPatterns = [
    /\^/,
    /\//,
    /-WS$/i,
    /W$/,
    /WS$/i,
    /U$/,
    /R$/,
    /P$/,
    /PR[A-Z]?$/i,
    /TEST/i,
  ]

  return !badPatterns.some((pattern) => pattern.test(ticker))
}

function calcPercentChange(current: number, prior: number) {
  if (!prior || prior <= 0) return 0
  return ((current - prior) / prior) * 100
}

function snapshotDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function buildSnapshotKey(screenedOn: string, ticker: string) {
  return `${screenedOn}_${ticker}`
}

function buildExcludedRow(params: {
  ticker: string
  cik: string
  name: string | null
  nowIso: string
  reason: string
}): CandidateUniverseRow {
  return {
    ticker: params.ticker,
    cik: params.cik,
    name: params.name,
    price: null,
    market_cap: null,
    avg_volume_20d: null,
    avg_dollar_volume_20d: null,
    return_5d: null,
    return_20d: null,
    volume_ratio: null,
    breakout_20d: false,
    above_sma_20: false,
    passes_price: false,
    passes_volume: false,
    passes_dollar_volume: false,
    passes_market_cap: false,
    candidate_score: 0,
    included: false,
    screen_reason: params.reason,
    last_screened_at: params.nowIso,
    updated_at: params.nowIso,
  }
}

async function persistCandidateRows(
  supabase: ReturnType<typeof createClient>,
  row: CandidateUniverseRow,
  screenedOn: string,
  nowIso: string
) {
  const universeResult = await supabase
    .from("candidate_universe")
    .upsert(row, { onConflict: "ticker" })

  if (universeResult.error) {
    return { ok: false as const, error: universeResult.error.message }
  }

  const historyRow: CandidateHistoryRow = {
    ...row,
    screened_on: screenedOn,
    snapshot_key: buildSnapshotKey(screenedOn, row.ticker),
    created_at: nowIso,
  }

  const historyResult = await supabase
    .from("candidate_screen_history")
    .upsert(historyRow, { onConflict: "snapshot_key" })

  if (historyResult.error) {
    return { ok: false as const, error: historyResult.error.message }
  }

  return { ok: true as const }
}

export async function GET(request: Request) {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey)
    const { searchParams } = new URL(request.url)

    const start = parseInteger(searchParams.get("start"), 0)
    const batch = parseInteger(searchParams.get("batch"), DEFAULT_BATCH)
    const onlyActiveParam = (searchParams.get("onlyActive") || "true").toLowerCase()
    const onlyActive = onlyActiveParam !== "false"

    // Optional: when true, remove candidate_universe rows that have not been screened today.
    // Best used only after the final batch has run.
    const cleanupStaleCurrent = (searchParams.get("cleanupStaleCurrent") || "false").toLowerCase() === "true"

    const safeStart = Math.max(0, start)
    const safeBatch = Math.min(Math.max(1, batch), MAX_BATCH)
    const from = safeStart
    const to = safeStart + safeBatch - 1

    const now = new Date()
    const nowIso = now.toISOString()
    const screenedOn = snapshotDateString(now)

    let companyQuery = supabase
      .from("companies")
      .select("ticker, cik, name, is_active")
      .not("cik", "is", null)
      .order("ticker", { ascending: true })
      .range(from, to)

    let countQuery = supabase
      .from("companies")
      .select("*", { count: "exact", head: true })
      .not("cik", "is", null)

    if (onlyActive) {
      companyQuery = companyQuery.eq("is_active", true)
      countQuery = countQuery.eq("is_active", true)
    }

    const [
      { data: companies, error: companiesError },
      { count: totalCompanies, error: totalCountError },
    ] = await Promise.all([companyQuery, countQuery])

    if (companiesError) {
      return Response.json(
        { ok: false, error: companiesError.message },
        { status: 500 }
      )
    }

    const results: CandidateResult[] = []
    let includedInBatch = 0
    let failedInBatch = 0
    let historyInserted = 0

    for (const company of (companies || []) as CompanyRow[]) {
      const ticker = normalizeTicker(company.ticker)

      try {
        if (!ticker || !company.cik) {
          failedInBatch += 1
          results.push({
            ticker: ticker || null,
            ok: false,
            error: "Missing ticker or cik",
          })
          await sleep(REQUEST_DELAY_MS)
          continue
        }

        if (!isProbablyCommonStockTicker(ticker)) {
          const row = buildExcludedRow({
            ticker,
            cik: company.cik,
            name: company.name,
            nowIso,
            reason: "Excluded likely non-common-share ticker",
          })

          const persistResult = await persistCandidateRows(supabase, row, screenedOn, nowIso)
          if (!persistResult.ok) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: persistResult.error,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          historyInserted += 1
          results.push({
            ticker,
            ok: true,
            included: false,
            score: 0,
            reason: row.screen_reason,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const endDate = new Date()
        const startDate = new Date()
        startDate.setDate(startDate.getDate() - 60)

        const [candles, quote] = await Promise.all([
          yahooFinance.historical(ticker, {
            period1: toIsoDateString(startDate),
            period2: toIsoDateString(endDate),
            interval: "1d",
          }),
          yahooFinance.quote(ticker),
        ])

        const clean = (candles || [])
          .filter(
            (c) =>
              c.close !== null &&
              c.close !== undefined &&
              c.volume !== null &&
              c.volume !== undefined
          )
          .sort((a, b) => +new Date(a.date) - +new Date(b.date))

        if (clean.length < 22) {
          const row = buildExcludedRow({
            ticker,
            cik: company.cik,
            name: company.name,
            nowIso,
            reason: "Not enough price history",
          })

          const persistResult = await persistCandidateRows(supabase, row, screenedOn, nowIso)
          if (!persistResult.ok) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: persistResult.error,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          historyInserted += 1
          results.push({
            ticker,
            ok: true,
            included: false,
            score: 0,
            reason: row.screen_reason,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const latest = clean[clean.length - 1]
        const fiveAgo = clean[clean.length - 6]
        const twentyAgo = clean[clean.length - 21]
        const prior20 = clean.slice(-21, -1)

        if (!latest || !fiveAgo || !twentyAgo || prior20.length === 0) {
          const row = buildExcludedRow({
            ticker,
            cik: company.cik,
            name: company.name,
            nowIso,
            reason: "Not enough valid lookback data",
          })

          const persistResult = await persistCandidateRows(supabase, row, screenedOn, nowIso)
          if (!persistResult.ok) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: persistResult.error,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          historyInserted += 1
          results.push({
            ticker,
            ok: true,
            included: false,
            score: 0,
            reason: row.screen_reason,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const latestClose = Number(latest.close || 0)
        const latestVolume = Number(latest.volume || 0)

        const avgVolume20d = avg(prior20.map((c) => Number(c.volume || 0)))
        const avgDollarVolume20d = avg(
          prior20.map((c) => Number(c.close || 0) * Number(c.volume || 0))
        )
        const high20 = Math.max(...prior20.map((c) => Number(c.high || 0)))
        const sma20 = avg(prior20.map((c) => Number(c.close || 0)))
        const return5d = calcPercentChange(latestClose, Number(fiveAgo.close || 0))
        const return20d = calcPercentChange(latestClose, Number(twentyAgo.close || 0))
        const volumeRatio = avgVolume20d > 0 ? latestVolume / avgVolume20d : 0
        const breakout20d = latestClose > high20
        const aboveSma20 = latestClose > sma20
        const marketCap = safeNumber((quote as any)?.marketCap)

        const passesPrice = latestClose >= MIN_PRICE
        const passesVolume = avgVolume20d >= MIN_AVG_VOLUME_20D
        const passesDollarVolume = avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D
        const passesMarketCap = (marketCap ?? 0) >= MIN_MARKET_CAP

        const hasMomentum5d = return5d >= 4
        const hasMomentum20d = return20d >= 10
        const hasVolumeExpansion = volumeRatio >= 1.75
        const hasBreakout = breakout20d
        const hasTrend = aboveSma20

        let score = 0
        if (passesPrice) score += 1
        if (passesVolume) score += 1
        if (passesDollarVolume) score += 2
        if (passesMarketCap) score += 1
        if (hasMomentum5d) score += 1
        if (hasMomentum20d) score += 2
        if (hasVolumeExpansion) score += 1
        if (hasBreakout) score += 2
        if (hasTrend) score += 1

        const included =
          passesPrice &&
          passesDollarVolume &&
          passesMarketCap &&
          score >= 6 &&
          (hasMomentum5d || hasMomentum20d || hasVolumeExpansion || hasBreakout)

        const reasons: string[] = []
        if (passesPrice) reasons.push("price")
        if (passesVolume) reasons.push("volume")
        if (passesDollarVolume) reasons.push("dollar volume")
        if (passesMarketCap) reasons.push("market cap")
        if (hasMomentum5d) reasons.push("5d momentum")
        if (hasMomentum20d) reasons.push("20d momentum")
        if (hasVolumeExpansion) reasons.push("volume expansion")
        if (hasBreakout) reasons.push("20d breakout")
        if (hasTrend) reasons.push("above 20d average")

        const row: CandidateUniverseRow = {
          ticker,
          cik: company.cik,
          name: company.name,
          price: round2(latestClose),
          market_cap: roundWhole(marketCap),
          avg_volume_20d: roundWhole(avgVolume20d),
          avg_dollar_volume_20d: roundWhole(avgDollarVolume20d),
          return_5d: round2(return5d),
          return_20d: round2(return20d),
          volume_ratio: round2(volumeRatio),
          breakout_20d: hasBreakout,
          above_sma_20: hasTrend,
          passes_price: passesPrice,
          passes_volume: passesVolume,
          passes_dollar_volume: passesDollarVolume,
          passes_market_cap: passesMarketCap,
          candidate_score: score,
          included,
          screen_reason: reasons.join(", ") || "No screen factors passed",
          last_screened_at: nowIso,
          updated_at: nowIso,
        }

        const persistResult = await persistCandidateRows(supabase, row, screenedOn, nowIso)
        if (!persistResult.ok) {
          failedInBatch += 1
          results.push({
            ticker,
            ok: false,
            error: persistResult.error,
          })
          await sleep(REQUEST_DELAY_MS)
          continue
        }

        historyInserted += 1
        if (included) includedInBatch += 1

        results.push({
          ticker,
          ok: true,
          included,
          score,
          reason: row.screen_reason,
          price: row.price,
          return5d: row.return_5d,
          return20d: row.return_20d,
          volumeRatio: row.volume_ratio,
        })
      } catch (err: any) {
        failedInBatch += 1
        results.push({
          ticker,
          ok: false,
          error: err?.message || "Unknown screening error",
        })
      }

      await sleep(REQUEST_DELAY_MS)
    }

    const cutoffDate = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
      .toISOString()
      .slice(0, 10)

    const { error: retentionError } = await supabase
      .from("candidate_screen_history")
      .delete()
      .lt("screened_on", cutoffDate)

    let staleCurrentCleanup: string = "skipped"

    if (cleanupStaleCurrent) {
      const { error: staleCurrentError } = await supabase
        .from("candidate_universe")
        .delete()
        .lt("last_screened_at", `${screenedOn}T00:00:00.000Z`)

      staleCurrentCleanup = staleCurrentError ? staleCurrentError.message : "ok"
    }

    const [
      { count: candidateCount, error: includedCountError },
      { count: historyCount, error: historyCountError },
    ] = await Promise.all([
      supabase
        .from("candidate_universe")
        .select("*", { count: "exact", head: true })
        .eq("included", true),
      supabase
        .from("candidate_screen_history")
        .select("*", { count: "exact", head: true }),
    ])

    const nextStart =
      totalCompanies !== null &&
      totalCompanies !== undefined &&
      to + 1 < totalCompanies
        ? to + 1
        : null

    return Response.json({
      ok: true,
      processedCompanies: companies?.length || 0,
      totalCompanies: totalCountError ? null : totalCompanies,
      start: safeStart,
      batch: safeBatch,
      nextStart,
      onlyActive,
      includedInBatch,
      failedInBatch,
      includedCount: includedCountError ? null : candidateCount,
      historyInserted,
      historyCount: historyCountError ? null : historyCount,
      retentionCleanup: retentionError ? retentionError.message : "ok",
      staleCurrentCleanup,
      retainedDays: RETENTION_DAYS,
      screenedOn,
      results,
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error?.message || "Unknown error" },
      { status: 500 }
    )
  }
}