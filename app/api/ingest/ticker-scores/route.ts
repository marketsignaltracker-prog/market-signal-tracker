import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type ChunkWriteResult = {
  insertedOrUpdated: number
  errors: Array<{
    table: string
    chunkStart: number
    chunkSize: number
    message: string
    details?: string | null
    hint?: string | null
    code?: string | null
    sampleKeys?: string[]
  }>
}

const DEFAULT_LOOKBACK_DAYS = 14
const MAX_LOOKBACK_DAYS = 30
const DEFAULT_LIMIT = 1000
const MAX_LIMIT = 3000
const RETENTION_DAYS = 30
const SCORE_VERSION = "v7-combined-tight"
const MIN_SIGNAL_APP_SCORE = 75
const MIN_TICKER_APP_SCORE = 85
const DB_CHUNK_SIZE = 100

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function round2(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 100) / 100
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function addDays(isoDate: string, days: number) {
  const d = new Date(`${isoDate}T00:00:00.000Z`)
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}

async function upsertInChunksDetailed(
  table: any,
  tableName: string,
  rows: any[],
  onConflict: string,
  sampleKeyBuilder?: (row: any) => string
): Promise<ChunkWriteResult> {
  let insertedOrUpdated = 0
  const errors: ChunkWriteResult["errors"] = []

  for (let i = 0; i < rows.length; i += DB_CHUNK_SIZE) {
    const chunk = rows.slice(i, i + DB_CHUNK_SIZE)
    const { error } = await table.upsert(chunk, { onConflict })

    if (error) {
      errors.push({
        table: tableName,
        chunkStart: i,
        chunkSize: chunk.length,
        message: error.message,
        details: (error as any)?.details ?? null,
        hint: (error as any)?.hint ?? null,
        code: (error as any)?.code ?? null,
        sampleKeys: sampleKeyBuilder ? chunk.slice(0, 10).map(sampleKeyBuilder) : undefined,
      })
    } else {
      insertedOrUpdated += chunk.length
    }
  }

  return {
    insertedOrUpdated,
    errors,
  }
}

async function deleteInChunksByTickerDetailed(table: any, tickers: string[]) {
  const unique = uniqueStrings(tickers)
  const errors: Array<{
    chunkStart: number
    chunkSize: number
    message: string
    details?: string | null
    hint?: string | null
    code?: string | null
    sampleTickers: string[]
  }> = []

  let deletedRequested = 0

  for (let i = 0; i < unique.length; i += DB_CHUNK_SIZE) {
    const chunk = unique.slice(i, i + DB_CHUNK_SIZE)
    const { error } = await table.delete().in("ticker", chunk)

    if (error) {
      errors.push({
        chunkStart: i,
        chunkSize: chunk.length,
        message: error.message,
        details: (error as any)?.details ?? null,
        hint: (error as any)?.hint ?? null,
        code: (error as any)?.code ?? null,
        sampleTickers: chunk.slice(0, 10),
      })
    } else {
      deletedRequested += chunk.length
    }
  }

  return {
    deletedRequested,
    errors,
  }
}

function getStrengthBucket(score: number): "Buy" | "Strong Buy" | "Elite Buy" {
  if (score >= 97) return "Elite Buy"
  if (score >= 92) return "Strong Buy"
  return "Buy"
}

function countPositiveEvidencePillars(breakdown: Record<string, number>) {
  return [
    (breakdown.base || 0) > 0,
    (breakdown.candidate_score || 0) > 0 || (breakdown.included || 0) > 0,
    (breakdown.breakout || 0) > 0 || (breakdown.trend || 0) > 0,
    (breakdown.volume || 0) > 0,
    (breakdown.relative_strength || 0) > 0,
    (breakdown.freshness || 0) > 0 || (breakdown.momentum || 0) > 0,
  ].filter(Boolean).length
}

function buildTickerScoresCurrentRows(signalRows: any[], runTimestamp: string) {
  const byTicker = new Map<string, any[]>()

  for (const row of signalRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const rows: any[] = []

  for (const [ticker, tickerSignalRows] of byTicker.entries()) {
    const sorted = [...tickerSignalRows].sort((a, b) => {
      const scoreDiff = Number(b.app_score ?? 0) - Number(a.app_score ?? 0)
      if (scoreDiff !== 0) return scoreDiff

      const filedAtDiff =
        new Date(b.filed_at || 0).getTime() - new Date(a.filed_at || 0).getTime()
      if (filedAtDiff !== 0) return filedAtDiff

      return String(a.signal_key || "").localeCompare(String(b.signal_key || ""))
    })

    const primary = sorted[0]
    const primaryScore = Number(primary.app_score || 0)

    /**
     * Require at least 2 supporting signals unless the primary signal is already extremely strong.
     */
    if (sorted.length < 2 && primaryScore < 95) {
      continue
    }

    const scoreBreakdown: Record<string, number> = {}
    const signalReasons = new Set<string>()
    const scoreCapsApplied = new Set<string>()
    const signalTags = new Set<string>()
    const accessionNos: string[] = []
    const signalKeys: string[] = []
    const sourceForms: string[] = []
    const signalSources = new Set<string>()
    const signalCategories = new Set<string>()

    for (const row of sorted) {
      signalKeys.push(row.signal_key)
      accessionNos.push(row.accession_no)
      if (row.source_form) sourceForms.push(row.source_form)
      if (row.signal_source) signalSources.add(row.signal_source)
      if (row.signal_category) signalCategories.add(row.signal_category)

      for (const tag of row.signal_tags || []) signalTags.add(tag)
      for (const reason of row.signal_reasons || []) signalReasons.add(reason)
      for (const cap of row.score_caps_applied || []) scoreCapsApplied.add(cap)

      const breakdown = (row.score_breakdown || {}) as Record<string, number>
      for (const [key, value] of Object.entries(breakdown)) {
        scoreBreakdown[key] = round2((scoreBreakdown[key] || 0) + Number(value || 0)) ?? 0
      }
    }

    let stackedScore = primaryScore

    if (sorted.length >= 2) stackedScore += 2
    if (sorted.length >= 3) stackedScore += 2
    if (sorted.length >= 4) stackedScore += 1
    if (sorted.length >= 5) stackedScore += 1

    if (signalSources.has("breakout") && signalSources.has("form4")) {
      stackedScore += 2
      scoreCapsApplied.add("technical-plus-insider-bonus")
    }

    if (signalSources.has("breakout") && (signalSources.has("13d") || signalSources.has("13g"))) {
      stackedScore += 2
      scoreCapsApplied.add("technical-plus-ownership-bonus")
    }

    if (signalSources.has("breakout") && signalSources.has("8k")) {
      stackedScore += 1
      scoreCapsApplied.add("technical-plus-catalyst-bonus")
    }

    const positivePillars = countPositiveEvidencePillars(scoreBreakdown)

    if (positivePillars < 3) {
      stackedScore = Math.min(stackedScore, 88)
      scoreCapsApplied.add("stacked-limited-evidence-cap")
    }

    if (positivePillars < 4) {
      stackedScore = Math.min(stackedScore, 92)
      scoreCapsApplied.add("stacked-broad-confirmation-cap")
    }

    const hasBreakoutSupport =
      (scoreBreakdown.breakout || 0) > 0 || primary.breakout_20d === true

    const hasHeavyVolume =
      (primary.volume_ratio ?? 0) >= 2 || (scoreBreakdown.volume || 0) >= 5

    const hasFilingConfirmation =
      signalSources.has("form4") ||
      signalSources.has("13d") ||
      signalSources.has("13g") ||
      signalSources.has("8k") ||
      signalSources.has("earnings")

    const hasMultipleSignalTypes = signalSources.size >= 2
    const hasThreeSignals = sorted.length >= 3
    const hasFourSignals = sorted.length >= 4

    if (!hasMultipleSignalTypes) {
      stackedScore = Math.min(stackedScore, 93)
      scoreCapsApplied.add("multi-source-required-cap")
    }

    if (!(hasBreakoutSupport && hasHeavyVolume && positivePillars >= 4)) {
      stackedScore = Math.min(stackedScore, 96)
      scoreCapsApplied.add("stacked-elite-confirmation-cap")
    }

    if (!(hasThreeSignals && hasMultipleSignalTypes && hasFilingConfirmation)) {
      stackedScore = Math.min(stackedScore, 98)
      scoreCapsApplied.add("stacked-top-tier-confirmation-cap")
    }

    if (
      !(
        hasFourSignals &&
        hasBreakoutSupport &&
        hasHeavyVolume &&
        positivePillars >= 5 &&
        hasFilingConfirmation &&
        primaryScore >= 95
      )
    ) {
      stackedScore = Math.min(stackedScore, 99)
      scoreCapsApplied.add("stacked-no-perfect-score-cap")
    }

    const finalScore = clamp(Math.round(stackedScore), 0, 100)

    /**
     * Only the best multi-signal names should survive.
     */
    if (finalScore < MIN_TICKER_APP_SCORE) continue

    const perfectTickerSetup =
      sorted.length >= 4 &&
      signalSources.size >= 3 &&
      hasBreakoutSupport &&
      hasHeavyVolume &&
      hasFilingConfirmation &&
      positivePillars >= 5 &&
      primaryScore >= 97 &&
      (primary.relative_strength_20d ?? 0) >= 8 &&
      (primary.price_return_20d ?? 0) >= 12 &&
      (primary.volume_ratio ?? 0) >= 2.2

    const finalTickerScore = perfectTickerSetup ? 100 : finalScore

    const primaryTitle =
      sorted.length >= 2
        ? `Multi-signal institutional setup (${sorted.length} signals)`
        : primary.title

    const primarySummary =
      sorted.length >= 2
        ? `Multiple signal sources are lining up for this ticker: ${Array.from(signalSources).join(", ")}. Primary setup: ${primary.title}`
        : primary.summary

    rows.push({
      ticker,
      company_name: primary.company_name,
      business_description: primary.business_description,
      app_score: finalTickerScore,
      raw_score: finalTickerScore,
      bias: "Bullish",
      board_bucket: "Buy",
      signal_strength_bucket: getStrengthBucket(finalTickerScore),
      score_version: SCORE_VERSION,
      score_updated_at: runTimestamp,
      stacked_signal_count: sorted.length,
      score_breakdown: scoreBreakdown,
      signal_reasons: Array.from(signalReasons).slice(0, 12),
      score_caps_applied: Array.from(scoreCapsApplied),
      signal_tags: Array.from(signalTags),
      primary_signal_key: primary.signal_key,
      primary_signal_type: sorted.length >= 2 ? "Multi-Signal Strong Buy" : primary.signal_type,
      primary_signal_source: sorted.length >= 2 ? "multi" : primary.signal_source,
      primary_signal_category:
        sorted.length >= 2 ? "Multi-Signal Strong Buy" : primary.signal_category,
      primary_title: primaryTitle,
      primary_summary: primarySummary,
      filed_at: primary.filed_at,
      signal_keys: signalKeys,
      accession_nos: accessionNos,
      source_forms: uniqueStrings(sourceForms),
      pe_ratio: primary.pe_ratio,
      pe_forward: primary.pe_forward,
      pe_type: primary.pe_type,
      market_cap: primary.market_cap,
      sector: primary.sector,
      industry: primary.industry,
      insider_action: primary.insider_action,
      insider_shares: primary.insider_shares,
      insider_avg_price: primary.insider_avg_price,
      insider_buy_value: primary.insider_buy_value,
      cluster_buyers: primary.cluster_buyers,
      cluster_shares: primary.cluster_shares,
      price_return_5d: primary.price_return_5d,
      price_return_20d: primary.price_return_20d,
      volume_ratio: primary.volume_ratio,
      breakout_20d: primary.breakout_20d,
      breakout_52w: primary.breakout_52w,
      above_50dma: primary.above_50dma,
      trend_aligned: primary.trend_aligned,
      price_confirmed: primary.price_confirmed,
      relative_strength_20d: primary.relative_strength_20d,
      earnings_surprise_pct: primary.earnings_surprise_pct,
      revenue_growth_pct: primary.revenue_growth_pct,
      guidance_flag: primary.guidance_flag,
      age_days: primary.age_days,
      freshness_bucket: primary.freshness_bucket,
      ticker_score_change_1d: null,
      ticker_score_change_7d: null,
      updated_at: runTimestamp,
    })
  }

  return rows
}

async function attachTickerScoreChangesToCurrentRows(
  supabase: any,
  currentRows: any[],
  runDate: string
) {
  const tickers = uniqueStrings(currentRows.map((row) => row.ticker))
  if (!tickers.length) return currentRows

  const earliestNeededDate = addDays(runDate, -14)

  const { data: historyRows, error } = await supabase
    .from("ticker_score_history")
    .select("ticker, score_date, app_score")
    .in("ticker", tickers)
    .gte("score_date", earliestNeededDate)
    .lt("score_date", runDate)
    .order("score_date", { ascending: false })

  if (error) return currentRows

  const byTicker = new Map<string, Map<string, number>>()

  for (const row of historyRows || []) {
    const ticker = normalizeTicker((row as any).ticker)
    if (!byTicker.has(ticker)) byTicker.set(ticker, new Map<string, number>())
    byTicker.get(ticker)!.set(
      String((row as any).score_date),
      Number((row as any).app_score || 0)
    )
  }

  return currentRows.map((row) => {
    const ticker = normalizeTicker(row.ticker)
    const series = byTicker.get(ticker) || new Map<string, number>()
    const currentScore = Number(row.app_score || 0)

    const oneDayDate = addDays(runDate, -1)
    const sevenDayDate = addDays(runDate, -7)

    const prev1d = series.has(oneDayDate) ? series.get(oneDayDate)! : null
    const prev7d = series.has(sevenDayDate) ? series.get(sevenDayDate)! : null

    return {
      ...row,
      ticker_score_change_1d: prev1d === null ? null : round2(currentScore - prev1d),
      ticker_score_change_7d: prev7d === null ? null : round2(currentScore - prev7d),
    }
  })
}

function buildTickerScoreHistoryRows(currentRows: any[], runDate: string, runTimestamp: string) {
  return currentRows.map((row) => ({
    ticker: row.ticker,
    company_name: row.company_name,
    score_date: runDate,
    score_timestamp: runTimestamp,
    app_score: row.app_score,
    raw_score: row.raw_score,
    bias: row.bias,
    board_bucket: row.board_bucket,
    score_version: row.score_version,
    stacked_signal_count: row.stacked_signal_count,
    score_breakdown: row.score_breakdown,
    signal_reasons: row.signal_reasons,
    score_caps_applied: row.score_caps_applied,
    source_accession_nos: row.accession_nos,
    source_signal_keys: row.signal_keys,
    created_at: runTimestamp,
  }))
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const { searchParams } = new URL(request.url)

    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )
    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)

    const { data: allSignalRows, error: allSignalsError } = await supabase
      .from("signals")
      .select("*")
      .gte("filed_at", cutoffDateString)
      .gte("app_score", MIN_SIGNAL_APP_SCORE)
      .order("app_score", { ascending: false })
      .order("filed_at", { ascending: false })
      .limit(limit)

    if (allSignalsError) {
      return Response.json(
        {
          ok: false,
          error: allSignalsError.message,
        },
        { status: 500 }
      )
    }

    const signalRows = (allSignalRows || []) as any[]
    const tickerCurrentRowsBase = buildTickerScoresCurrentRows(signalRows, runTimestamp)
    const tickerCurrentRows = await attachTickerScoreChangesToCurrentRows(
      supabase,
      tickerCurrentRowsBase,
      runDate
    )

    const tickerCurrentWriteResult =
      tickerCurrentRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("ticker_scores_current"),
            "ticker_scores_current",
            tickerCurrentRows,
            "ticker",
            (row) => row.ticker
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (tickerCurrentWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing ticker_scores_current rows",
          debug: {
            errorSamples: tickerCurrentWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const currentTickerSet = new Set(tickerCurrentRows.map((row) => normalizeTicker(row.ticker)))
    const { data: existingTickerRows, error: existingTickerRowsError } = await supabase
      .from("ticker_scores_current")
      .select("ticker")

    if (existingTickerRowsError) {
      return Response.json(
        {
          ok: false,
          error: existingTickerRowsError.message,
        },
        { status: 500 }
      )
    }

    const staleTickerList = uniqueStrings(
      (existingTickerRows || [])
        .map((row: any) => normalizeTicker(row.ticker))
        .filter((ticker) => !currentTickerSet.has(ticker))
    )

    const staleDeleteResult =
      staleTickerList.length > 0
        ? await deleteInChunksByTickerDetailed(supabase.from("ticker_scores_current"), staleTickerList)
        : { deletedRequested: 0, errors: [] as any[] }

    if (staleDeleteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed deleting stale ticker_scores_current rows",
          debug: {
            errorSamples: staleDeleteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const tickerHistoryRows = buildTickerScoreHistoryRows(tickerCurrentRows, runDate, runTimestamp)

    const tickerHistoryWriteResult =
      tickerHistoryRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("ticker_score_history"),
            "ticker_score_history",
            tickerHistoryRows,
            "ticker,score_date",
            (row) => `${row.ticker}:${row.score_date}`
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (tickerHistoryWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing ticker_score_history rows",
          debug: {
            errorSamples: tickerHistoryWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: tickerRetentionError } = await supabase
        .from("ticker_score_history")
        .delete()
        .lt("score_date", retentionCutoffString)

      retentionMessage = tickerRetentionError ? tickerRetentionError.message : "ok"
    }

    let strongBuyCount: number | null = null
    let eliteBuyCount: number | null = null

    if (includeCounts) {
      const [strongBuyRes, eliteBuyRes] = await Promise.all([
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 92),
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 97),
      ])

      strongBuyCount = strongBuyRes.error ? null : strongBuyRes.count ?? 0
      eliteBuyCount = eliteBuyRes.error ? null : eliteBuyRes.count ?? 0
    }

    return Response.json({
      ok: true,
      scannedSignals: signalRows.length,
      tickerCurrentInserted: tickerCurrentWriteResult.insertedOrUpdated,
      tickerHistoryInserted: tickerHistoryWriteResult.insertedOrUpdated,
      lookbackDays,
      limit,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      minTickerAppScore: MIN_TICKER_APP_SCORE,
      retentionCleanup: retentionMessage,
      strongBuyCount,
      eliteBuyCount,
      message: "Ticker scores rebuilt from combined technical and filing signals successfully.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown error",
      },
      { status: 500 }
    )
  }
}