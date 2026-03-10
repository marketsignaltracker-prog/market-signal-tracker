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

const yahooFinance = new YahooFinance({
  queue: { concurrency: 1 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 350
const DEFAULT_BATCH = 300
const RETENTION_DAYS = 30
const REQUEST_DELAY_MS = 120

// Strong Buy Now universe should be liquid, established, and tradable.
const MIN_PRICE = 10
const MIN_AVG_VOLUME_20D = 500_000
const MIN_AVG_DOLLAR_VOLUME_20D = 15_000_000
const MIN_MARKET_CAP = 500_000_000

// Hard gate thresholds for "Strong Buy Now".
const MIN_STRONG_BUY_SCORE = 90
const MIN_STRONG_BUY_VOLUME_RATIO = 1.75
const MIN_STRONG_BUY_RETURN_10D = 8
const MIN_STRONG_BUY_RETURN_20D = 15
const MAX_STRONG_BUY_RETURN_20D = 35
const MAX_EXTENSION_FROM_SMA20_PCT = 12
const MIN_BREAKOUT_CLEARANCE_PCT = 0.25
const MIN_CLOSE_IN_DAY_RANGE = 0.65
const MIN_CATALYST_COUNT = 7

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

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function scaleBetween(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return 0
  if (max <= min) return value >= max ? 1 : 0
  return clamp((value - min) / (max - min), 0, 1)
}

// Stricter about excluding non-common-share patterns while avoiding false positives
// on regular common tickers like AAP, ABR, ACHR, etc.
function isProbablyCommonStockTicker(ticker: string) {
  if (!ticker) return false

  const t = ticker.trim().toUpperCase()

  const badPatterns = [
    /\^/,
    /\//,

    // Units / warrants / rights / subscription receipts
    /(?:^|[-.])(WS|WT|WTS|WARRANT|WAR)$/i,
    /(?:^|[-.])(W|U|R)$/i,
    /(?:^|[-.])(RT|RIGHT|RIGHTS)$/i,

    // Preferred / preference share classes
    /(?:^|[-.])P(?:R)?[A-Z]{0,2}$/i,
    /PREFERRED/i,
    /PREF/i,

    // Common SPAC-ish odd lots / test tickers
    /TEST/i,
  ]

  return !badPatterns.some((pattern) => pattern.test(t))
}

function calcPercentChange(current: number, prior: number) {
  if (!prior || prior <= 0) return 0
  return ((current - prior) / prior) * 100
}

function snapshotDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function buildCandidateReason(params: {
  included: boolean
  strongBuyNow: boolean
  reasons: string[]
  exclusionReason?: string
}) {
  if (params.included && params.strongBuyNow) {
    return `Strong buy now: ${params.reasons.join(", ")}`
  }

  if (params.exclusionReason) {
    return params.exclusionReason
  }

  return params.reasons.length
    ? `Watchlist only: ${params.reasons.join(", ")}`
    : "No strong-buy-now factors passed"
}

type CandidateScoreInput = {
  latestClose: number
  marketCap: number
  avgVolume20d: number
  avgDollarVolume20d: number
  return5d: number
  return10d: number
  return20d: number
  oneDayReturn: number
  volumeRatio: number
  breakout20d: boolean
  breakout10d: boolean
  nearHigh20: boolean
  aboveSma20: boolean
  shortTermTrendUp: boolean
  sma10: number
  sma20: number
  high20: number
  breakoutClearancePct: number
  extensionFromSma20Pct: number
  closeInDayRange: number
  passesPrice: boolean
  passesVolume: boolean
  passesDollarVolume: boolean
  passesMarketCap: boolean
}

type CandidateScoreOutput = {
  candidateScore: number
  rawScore: number
  qualityScore: number
  momentumScore: number
  volumeScore: number
  breakoutScore: number
  trendScore: number
  penaltyScore: number
  catalystCount: number
  highConvictionSetup: boolean
  eliteSetup: boolean
}

function calculateCandidateScore(input: CandidateScoreInput): CandidateScoreOutput {
  const {
    latestClose,
    marketCap,
    avgVolume20d,
    avgDollarVolume20d,
    return5d,
    return10d,
    return20d,
    oneDayReturn,
    volumeRatio,
    breakout20d,
    breakout10d,
    nearHigh20,
    aboveSma20,
    shortTermTrendUp,
    sma10,
    sma20,
    high20,
    breakoutClearancePct,
    extensionFromSma20Pct,
    closeInDayRange,
    passesPrice,
    passesVolume,
    passesDollarVolume,
    passesMarketCap,
  } = input

  const qualityScore =
    18 *
    (
      0.15 * (passesPrice ? 1 : 0) +
      0.2 * (passesVolume ? 1 : 0) +
      0.35 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 60_000_000) +
      0.3 * scaleBetween(marketCap, MIN_MARKET_CAP, 10_000_000_000)
    )

  const momentumScore =
    18 *
    (
      0.2 * scaleBetween(oneDayReturn, 0, 5) +
      0.2 * scaleBetween(return5d, 2, 10) +
      0.25 * scaleBetween(return10d, 6, 18) +
      0.35 * scaleBetween(return20d, 10, 28)
    )

  const volumeScore =
    18 *
    (
      0.7 * scaleBetween(volumeRatio, 1.2, 3.25) +
      0.3 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 40_000_000)
    )

  const distanceFrom20dHighPct =
    high20 > 0 ? ((latestClose - high20) / high20) * 100 : 0

  let breakoutQuality = 0
  if (breakout20d) breakoutQuality += 0.6
  if (breakout10d) breakoutQuality += 0.1
  if (nearHigh20) breakoutQuality += 0.05
  breakoutQuality += 0.15 * scaleBetween(breakoutClearancePct, MIN_BREAKOUT_CLEARANCE_PCT, 3)
  breakoutQuality += 0.1 * scaleBetween(closeInDayRange, 0.6, 1)
  breakoutQuality += 0.1 * scaleBetween(distanceFrom20dHighPct, 0, 4)

  const breakoutScore = 26 * clamp(breakoutQuality, 0, 1)

  const smaSpreadPct = sma20 > 0 ? ((sma10 - sma20) / sma20) * 100 : 0

  const trendScore =
    20 *
    (
      0.35 * (aboveSma20 ? 1 : 0) +
      0.25 * (shortTermTrendUp ? 1 : 0) +
      0.2 * scaleBetween(smaSpreadPct, 0.3, 4) +
      0.2 * (extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT ? 1 : 0)
    )

  let penaltyScore = 0

  if (!passesPrice) penaltyScore -= 12
  if (!passesVolume) penaltyScore -= 8
  if (!passesDollarVolume) penaltyScore -= 14
  if (!passesMarketCap) penaltyScore -= 10
  if (!aboveSma20) penaltyScore -= 16
  if (!shortTermTrendUp) penaltyScore -= 10
  if (oneDayReturn < 0) penaltyScore -= 6
  if (return5d < 0) penaltyScore -= 8
  if (return10d < 5) penaltyScore -= 10
  if (return20d < 10) penaltyScore -= 14
  if (volumeRatio < 1.2) penaltyScore -= 12
  if (!breakout20d) penaltyScore -= 22
  if (breakoutClearancePct < MIN_BREAKOUT_CLEARANCE_PCT) penaltyScore -= 8
  if (closeInDayRange < 0.55) penaltyScore -= 8
  if (extensionFromSma20Pct > MAX_EXTENSION_FROM_SMA20_PCT) penaltyScore -= 14

  // Fresh, high-quality breakouts should be preferred over exhausted moves.
  if (return20d > MAX_STRONG_BUY_RETURN_20D && volumeRatio < 2.25) penaltyScore -= 10
  if (return5d > 12 && !breakout20d) penaltyScore -= 8

  const rawScore =
    qualityScore + momentumScore + volumeScore + breakoutScore + trendScore + penaltyScore

  const normalized = clamp(rawScore / 96, 0, 1)
  let candidateScore = Math.round(Math.pow(normalized, 1.45) * 100)

  const catalystCount = [
    oneDayReturn > 0,
    return5d >= 3,
    return10d >= MIN_STRONG_BUY_RETURN_10D,
    return20d >= MIN_STRONG_BUY_RETURN_20D,
    return20d <= MAX_STRONG_BUY_RETURN_20D,
    volumeRatio >= 1.5,
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO,
    breakout20d,
    breakout10d,
    aboveSma20,
    shortTermTrendUp,
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT,
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE,
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT,
    avgDollarVolume20d >= 20_000_000,
    marketCap >= 1_000_000_000,
  ].filter(Boolean).length

  const highConvictionSetup =
    passesPrice &&
    passesVolume &&
    passesDollarVolume &&
    passesMarketCap &&
    breakout20d &&
    breakout10d &&
    aboveSma20 &&
    shortTermTrendUp &&
    oneDayReturn >= 0 &&
    return10d >= MIN_STRONG_BUY_RETURN_10D &&
    return20d >= MIN_STRONG_BUY_RETURN_20D &&
    return20d <= MAX_STRONG_BUY_RETURN_20D &&
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT

  const eliteSetup =
    highConvictionSetup &&
    volumeRatio >= 2.25 &&
    return5d >= 5 &&
    return10d >= 10 &&
    return20d >= 18 &&
    avgDollarVolume20d >= 25_000_000 &&
    marketCap >= 1_000_000_000 &&
    catalystCount >= MIN_CATALYST_COUNT

  // Hard caps to stop mediocre setups from drifting into top scores.
  if (!highConvictionSetup) {
    candidateScore = Math.min(candidateScore, 84)
  } else if (!eliteSetup) {
    candidateScore = Math.min(candidateScore, 92)
  } else if (
    !(
      volumeRatio >= 2.5 &&
      return10d >= 12 &&
      return20d >= 20 &&
      avgDollarVolume20d >= 35_000_000 &&
      marketCap >= 2_000_000_000
    )
  ) {
    candidateScore = Math.min(candidateScore, 97)
  }

  if (
    eliteSetup &&
    volumeRatio >= 2.75 &&
    return5d >= 6 &&
    return10d >= 12 &&
    return20d >= 20 &&
    return20d <= 30 &&
    avgDollarVolume20d >= 35_000_000 &&
    marketCap >= 2_000_000_000 &&
    catalystCount >= 9
  ) {
    candidateScore = 100
  }

  return {
    candidateScore: clamp(candidateScore, 0, 100),
    rawScore: round2(rawScore) ?? 0,
    qualityScore: round2(qualityScore) ?? 0,
    momentumScore: round2(momentumScore) ?? 0,
    volumeScore: round2(volumeScore) ?? 0,
    breakoutScore: round2(breakoutScore) ?? 0,
    trendScore: round2(trendScore) ?? 0,
    penaltyScore: round2(penaltyScore) ?? 0,
    catalystCount,
    highConvictionSetup,
    eliteSetup,
  }
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
    const supabase = createClient(supabaseUrl, serviceRoleKey)
    const { searchParams } = new URL(request.url)

    const start = parseInteger(searchParams.get("start"), 0)
    const batch = parseInteger(searchParams.get("batch"), DEFAULT_BATCH)
    const onlyActiveParam = (searchParams.get("onlyActive") || "true").toLowerCase()
    const onlyActive = onlyActiveParam !== "false"

    const safeStart = Math.max(0, start)
    const safeBatch = Math.min(Math.max(1, batch), MAX_BATCH)
    const from = safeStart
    const to = safeStart + safeBatch - 1

    const now = new Date()
    const nowIso = now.toISOString()
    const screenedOn = snapshotDateString(now)

    const companiesTable = supabase.from("companies") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any
    const candidateHistoryTable = supabase.from("candidate_screen_history") as any

    let companyQuery = companiesTable
      .select("ticker, cik, name, is_active")
      .not("cik", "is", null)
      .order("ticker", { ascending: true })
      .range(from, to)

    let countQuery = companiesTable
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

    const results: Array<Record<string, any>> = []
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
          const excludedRow: CandidateUniverseRow = {
            ticker,
            cik: company.cik,
            name: company.name,
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
            screen_reason: "Excluded likely non-common-share ticker",
            last_screened_at: nowIso,
            updated_at: nowIso,
          }

          const universeResult = await candidateUniverseTable.upsert(excludedRow, {
            onConflict: "ticker",
          })

          if (universeResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: universeResult.error.message,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          const historyRow: CandidateHistoryRow = {
            ...excludedRow,
            screened_on: screenedOn,
            snapshot_key: `${screenedOn}_${ticker}`,
            created_at: nowIso,
          }

          const historyResult = await candidateHistoryTable.upsert(historyRow, {
            onConflict: "snapshot_key",
          })

          if (historyResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: historyResult.error.message,
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
            tier: "excluded",
            reason: "Excluded likely non-common-share ticker",
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
          const row: CandidateUniverseRow = {
            ticker,
            cik: company.cik,
            name: company.name,
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
            screen_reason: "Not enough price history",
            last_screened_at: nowIso,
            updated_at: nowIso,
          }

          const universeResult = await candidateUniverseTable.upsert(row, {
            onConflict: "ticker",
          })

          if (universeResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: universeResult.error.message,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          const historyRow: CandidateHistoryRow = {
            ...row,
            screened_on: screenedOn,
            snapshot_key: `${screenedOn}_${ticker}`,
            created_at: nowIso,
          }

          const historyResult = await candidateHistoryTable.upsert(historyRow, {
            onConflict: "snapshot_key",
          })

          if (historyResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: historyResult.error.message,
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
            tier: "excluded",
            reason: "Not enough price history",
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const latest = clean[clean.length - 1]
        const previous = clean[clean.length - 2]
        const fiveAgo = clean[clean.length - 6]
        const tenAgo = clean[clean.length - 11]
        const twentyAgo = clean[clean.length - 21]
        const prior20 = clean.slice(-21, -1)
        const prior10 = clean.slice(-11, -1)

        const latestClose = Number(latest.close || 0)
        const latestOpen = Number(latest.open || latestClose)
        const latestHigh = Number(latest.high || latestClose)
        const latestLow = Number(latest.low || latestClose)
        const latestVolume = Number(latest.volume || 0)
        const previousClose = Number(previous?.close || latestClose)

        const avgVolume20d = avg(prior20.map((c) => Number(c.volume || 0)))
        const avgDollarVolume20d = avg(
          prior20.map((c) => Number(c.close || 0) * Number(c.volume || 0))
        )
        const high20 = Math.max(...prior20.map((c) => Number(c.high || 0)))
        const high10 = Math.max(...prior10.map((c) => Number(c.high || 0)))
        const sma20 = avg(prior20.map((c) => Number(c.close || 0)))
        const sma10 = avg(prior10.map((c) => Number(c.close || 0)))
        const return5d = calcPercentChange(latestClose, Number(fiveAgo?.close || 0))
        const return10d = calcPercentChange(latestClose, Number(tenAgo?.close || 0))
        const return20d = calcPercentChange(latestClose, Number(twentyAgo?.close || 0))
        const oneDayReturn = calcPercentChange(latestClose, previousClose)
        const volumeRatio = avgVolume20d > 0 ? latestVolume / avgVolume20d : 0
        const breakout20d = latestClose > high20
        const breakout10d = latestClose > high10
        const nearHigh20 = high20 > 0 ? latestClose >= high20 * 0.99 : false
        const aboveSma20 = latestClose > sma20
        const shortTermTrendUp = sma10 > sma20
        const marketCap = Number((quote as any)?.marketCap || 0)

        const breakoutClearancePct =
          high20 > 0 ? ((latestClose - high20) / high20) * 100 : 0

        const extensionFromSma20Pct =
          sma20 > 0 ? ((latestClose - sma20) / sma20) * 100 : 0

        const closeInDayRange =
          latestHigh > latestLow
            ? (latestClose - latestLow) / (latestHigh - latestLow)
            : latestClose >= latestOpen
              ? 1
              : 0

        const passesPrice = latestClose >= MIN_PRICE
        const passesVolume = avgVolume20d >= MIN_AVG_VOLUME_20D
        const passesDollarVolume = avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D
        const passesMarketCap = marketCap >= MIN_MARKET_CAP

        const scoreDetails = calculateCandidateScore({
          latestClose,
          marketCap,
          avgVolume20d,
          avgDollarVolume20d,
          return5d,
          return10d,
          return20d,
          oneDayReturn,
          volumeRatio,
          breakout20d,
          breakout10d,
          nearHigh20,
          aboveSma20,
          shortTermTrendUp,
          sma10,
          sma20,
          high20,
          breakoutClearancePct,
          extensionFromSma20Pct,
          closeInDayRange,
          passesPrice,
          passesVolume,
          passesDollarVolume,
          passesMarketCap,
        })

        const score = scoreDetails.candidateScore
        const catalystCount = scoreDetails.catalystCount

        // Strong Buy Now only. No soft inclusion.
        const strongBuyNowCandidate =
          passesPrice &&
          passesVolume &&
          passesDollarVolume &&
          passesMarketCap &&
          breakout20d &&
          breakout10d &&
          aboveSma20 &&
          shortTermTrendUp &&
          oneDayReturn >= 0 &&
          return10d >= MIN_STRONG_BUY_RETURN_10D &&
          return20d >= MIN_STRONG_BUY_RETURN_20D &&
          return20d <= MAX_STRONG_BUY_RETURN_20D &&
          volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
          breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
          closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
          extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT &&
          score >= MIN_STRONG_BUY_SCORE &&
          scoreDetails.highConvictionSetup &&
          catalystCount >= MIN_CATALYST_COUNT

        const included = strongBuyNowCandidate

        const reasons: string[] = []
        if (passesPrice) reasons.push(`price >= $${MIN_PRICE}`)
        if (passesVolume) reasons.push("20d avg volume")
        if (passesDollarVolume) reasons.push("20d dollar volume")
        if (passesMarketCap) reasons.push("market cap")
        if (oneDayReturn > 0) reasons.push("positive day")
        if (return5d >= 3) reasons.push("5d momentum")
        if (return10d >= MIN_STRONG_BUY_RETURN_10D) reasons.push("10d momentum")
        if (return20d >= MIN_STRONG_BUY_RETURN_20D) reasons.push("20d momentum")
        if (return20d <= MAX_STRONG_BUY_RETURN_20D) reasons.push("not overextended on 20d move")
        if (volumeRatio >= 1.5) reasons.push("volume expansion")
        if (volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO) reasons.push("strong volume expansion")
        if (breakout10d) reasons.push("10d breakout")
        if (breakout20d) reasons.push("20d breakout")
        if (breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT) reasons.push("clean breakout clearance")
        if (aboveSma20) reasons.push("above 20d average")
        if (shortTermTrendUp) reasons.push("short-term trend acceleration")
        if (closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE) reasons.push("strong close in daily range")
        if (extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT) reasons.push("not too extended from 20d average")
        if (scoreDetails.highConvictionSetup) reasons.push("high-conviction setup")
        if (scoreDetails.eliteSetup) reasons.push("elite setup")

        let exclusionReason = ""
        if (!passesPrice) exclusionReason = `Below $${MIN_PRICE} minimum price`
        else if (!passesVolume) exclusionReason = "Below minimum average volume"
        else if (!passesDollarVolume) exclusionReason = "Below minimum dollar volume"
        else if (!passesMarketCap) exclusionReason = "Below minimum market cap"
        else if (!aboveSma20) exclusionReason = "Below 20d average"
        else if (!shortTermTrendUp) exclusionReason = "Short-term trend not aligned"
        else if (!breakout20d) exclusionReason = "No fresh 20d breakout"
        else if (!breakout10d) exclusionReason = "Breakout lacks short-term confirmation"
        else if (oneDayReturn < 0) exclusionReason = "Breakout day closed negative"
        else if (return10d < MIN_STRONG_BUY_RETURN_10D) exclusionReason = "10d momentum below strong-buy threshold"
        else if (return20d < MIN_STRONG_BUY_RETURN_20D) exclusionReason = "20d momentum below strong-buy threshold"
        else if (return20d > MAX_STRONG_BUY_RETURN_20D) exclusionReason = "Move is too extended for fresh entry"
        else if (volumeRatio < MIN_STRONG_BUY_VOLUME_RATIO) exclusionReason = "Volume expansion below strong-buy threshold"
        else if (breakoutClearancePct < MIN_BREAKOUT_CLEARANCE_PCT) exclusionReason = "Breakout clearance too small"
        else if (closeInDayRange < MIN_CLOSE_IN_DAY_RANGE) exclusionReason = "Close too weak within daily range"
        else if (extensionFromSma20Pct > MAX_EXTENSION_FROM_SMA20_PCT) exclusionReason = "Too extended from 20d average"
        else if (score < MIN_STRONG_BUY_SCORE) exclusionReason = "Score below strong-buy threshold"
        else if (catalystCount < MIN_CATALYST_COUNT) exclusionReason = "Not enough strong-buy catalysts"

        const row: CandidateUniverseRow = {
          ticker,
          cik: company.cik,
          name: company.name,
          price: round2(latestClose),
          market_cap: marketCap || null,
          avg_volume_20d: round2(avgVolume20d),
          avg_dollar_volume_20d: round2(avgDollarVolume20d),
          return_5d: round2(return5d),
          return_20d: round2(return20d),
          volume_ratio: round2(volumeRatio),
          breakout_20d: breakout20d,
          above_sma_20: aboveSma20,
          passes_price: passesPrice,
          passes_volume: passesVolume,
          passes_dollar_volume: passesDollarVolume,
          passes_market_cap: passesMarketCap,
          candidate_score: score,
          included,
          screen_reason: buildCandidateReason({
            included,
            strongBuyNow: strongBuyNowCandidate,
            reasons,
            exclusionReason,
          }),
          last_screened_at: nowIso,
          updated_at: nowIso,
        }

        const universeResult = await candidateUniverseTable.upsert(row, {
          onConflict: "ticker",
        })

        if (universeResult.error) {
          failedInBatch += 1
          results.push({
            ticker,
            ok: false,
            error: universeResult.error.message,
          })
          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const historyRow: CandidateHistoryRow = {
          ...row,
          screened_on: screenedOn,
          snapshot_key: `${screenedOn}_${ticker}`,
          created_at: nowIso,
        }

        const historyResult = await candidateHistoryTable.upsert(historyRow, {
          onConflict: "snapshot_key",
        })

        if (historyResult.error) {
          failedInBatch += 1
          results.push({
            ticker,
            ok: false,
            error: historyResult.error.message,
          })
          await sleep(REQUEST_DELAY_MS)
          continue
        }

        historyInserted += 1
        if (included) includedInBatch += 1

        const tier = strongBuyNowCandidate ? "strong_buy_now" : "watchlist"

        results.push({
          ticker,
          ok: true,
          included,
          score,
          rawScore: round2(scoreDetails.rawScore),
          tier,
          reason: row.screen_reason,
          price: round2(latestClose),
          oneDayReturn: round2(oneDayReturn),
          return5d: round2(return5d),
          return10d: round2(return10d),
          return20d: round2(return20d),
          volumeRatio: round2(volumeRatio),
          breakoutClearancePct: round2(breakoutClearancePct),
          extensionFromSma20Pct: round2(extensionFromSma20Pct),
          closeInDayRange: round2(closeInDayRange),
          scoreBreakdown: {
            quality: round2(scoreDetails.qualityScore),
            momentum: round2(scoreDetails.momentumScore),
            volume: round2(scoreDetails.volumeScore),
            breakout: round2(scoreDetails.breakoutScore),
            trend: round2(scoreDetails.trendScore),
            penalty: round2(scoreDetails.penaltyScore),
          },
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

    const { error: retentionError } = await candidateHistoryTable
      .delete()
      .lt("screened_on", cutoffDate)

    const [
      { count: candidateCount, error: includedCountError },
      { count: historyCount, error: historyCountError },
    ] = await Promise.all([
      candidateUniverseTable
        .select("*", { count: "exact", head: true })
        .eq("included", true),
      candidateHistoryTable.select("*", { count: "exact", head: true }),
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
      retainedDays: RETENTION_DAYS,
      screenedOn,
      thresholds: {
        minPrice: MIN_PRICE,
        minAvgVolume20d: MIN_AVG_VOLUME_20D,
        minAvgDollarVolume20d: MIN_AVG_DOLLAR_VOLUME_20D,
        minMarketCap: MIN_MARKET_CAP,
        minStrongBuyScore: MIN_STRONG_BUY_SCORE,
        minStrongBuyVolumeRatio: MIN_STRONG_BUY_VOLUME_RATIO,
        minStrongBuyReturn10d: MIN_STRONG_BUY_RETURN_10D,
        minStrongBuyReturn20d: MIN_STRONG_BUY_RETURN_20D,
        maxStrongBuyReturn20d: MAX_STRONG_BUY_RETURN_20D,
        maxExtensionFromSma20Pct: MAX_EXTENSION_FROM_SMA20_PCT,
        minBreakoutClearancePct: MIN_BREAKOUT_CLEARANCE_PCT,
        minCloseInDayRange: MIN_CLOSE_IN_DAY_RANGE,
        minCatalystCount: MIN_CATALYST_COUNT,
      },
      results,
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown error" },
      { status: 500 }
    )
  }
}