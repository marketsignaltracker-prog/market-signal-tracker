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
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null
  sector: string | null
  industry: string | null
  business_description: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return: number | null
  return_5d: number | null
  return_10d: number | null
  return_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  breakout_10d: boolean
  above_sma_20: boolean
  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number
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

type CandidateScoreInput = {
  latestClose: number
  marketCap: number
  avgVolume20d: number
  avgDollarVolume20d: number
  return5d: number
  return10d: number
  return20d: number
  relativeReturn5d: number
  relativeReturn10d: number
  relativeReturn20d: number
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
  relativeStrengthScore: number
  volumeScore: number
  breakoutScore: number
  trendScore: number
  penaltyScore: number
  catalystCount: number
  highConvictionSetup: boolean
  eliteSetup: boolean
}

type YahooErrorDisposition = {
  kind: "permanent" | "transient"
  reason: string
}

type TickerSnapshot = {
  peRatio: number | null
  forwardPe: number | null
  peType: "trailing" | "forward" | null
  sector: string | null
  industry: string | null
  businessDescription: string | null
}

const yahooFinance = new YahooFinance({
  queue: { concurrency: 3 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 250
const DEFAULT_BATCH = 200
const RETENTION_DAYS = 30
const REQUEST_DELAY_MS = 150

const BENCHMARK_TICKER = "SPY"

const MIN_PRICE = 5
const MIN_AVG_VOLUME_20D = 500_000
const MIN_AVG_DOLLAR_VOLUME_20D = 15_000_000
const MIN_MARKET_CAP = 500_000_000

const MIN_BOARD_SCORE = 70
const MIN_STRONG_BUY_SCORE = 70
const MIN_STRONG_BUY_VOLUME_RATIO = 1.35
const MIN_STRONG_BUY_RETURN_10D = 5
const MIN_STRONG_BUY_RETURN_20D = 12
const MIN_RELATIVE_RETURN_10D = 2
const MIN_RELATIVE_RETURN_20D = 4
const MAX_STRONG_BUY_RETURN_20D = 40
const MAX_EXTENSION_FROM_SMA20_PCT = 22
const MIN_BREAKOUT_CLEARANCE_PCT = 0.1
const MIN_CLOSE_IN_DAY_RANGE = 0.55
const MIN_CATALYST_COUNT = 6

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

function isProbablyCommonStockTicker(ticker: string) {
  if (!ticker) return false

  const t = ticker.trim().toUpperCase()

  const badPatterns = [
    /\^/,
    /\//,
    /(?:^|[-.])(WS|WT|WTS|WARRANT|WAR)$/i,
    /(?:^|[-.])(W|U|R)$/i,
    /(?:^|[-.])(RT|RIGHT|RIGHTS)$/i,
    /(?:^|[-.])P(?:R)?[A-Z]{0,2}$/i,
    /PREFERRED/i,
    /PREF/i,
    /TEST/i,
  ]

  return !badPatterns.some((pattern) => pattern.test(t))
}

function calcPercentChange(current: number, prior: number) {
  if (!prior || prior <= 0) return 0
  return ((current - prior) / prior) * 100
}

function getBenchmarkReturns(candles: any[]) {
  const clean = (candles || [])
    .filter(
      (c) =>
        c.close !== null &&
        c.close !== undefined &&
        Number.isFinite(Number(c.close))
    )
    .sort((a, b) => +new Date(a.date) - +new Date(b.date))

  if (clean.length < 22) {
    return {
      return5d: 0,
      return10d: 0,
      return20d: 0,
    }
  }

  const latest = clean[clean.length - 1]
  const fiveAgo = clean[clean.length - 6]
  const tenAgo = clean[clean.length - 11]
  const twentyAgo = clean[clean.length - 21]

  return {
    return5d: calcPercentChange(Number(latest.close || 0), Number(fiveAgo?.close || 0)),
    return10d: calcPercentChange(Number(latest.close || 0), Number(tenAgo?.close || 0)),
    return20d: calcPercentChange(Number(latest.close || 0), Number(twentyAgo?.close || 0)),
  }
}

function snapshotDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function buildCandidateReason(params: {
  included: boolean
  strongBuyNow: boolean
  boardCandidate: boolean
  reasons: string[]
  exclusionReason?: string
  score?: number
}) {
  if (params.included && params.strongBuyNow) {
    return `Strong buy now (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.included && params.boardCandidate) {
    return `Board candidate (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.exclusionReason) {
    return params.exclusionReason
  }

  return params.reasons.length
    ? `Not included: ${params.reasons.join(", ")}`
    : "No board-worthy factors passed"
}

function sanitizeYahooErrorMessage(raw: unknown) {
  const message = String((raw as any)?.message || raw || "Unknown screening error").trim()

  if (message.includes("<!doctype html") || message.includes("<html>")) {
    return "Yahoo returned an HTML error page"
  }

  return message
}

function classifyYahooError(raw: unknown): YahooErrorDisposition {
  const message = sanitizeYahooErrorMessage(raw).toLowerCase()

  if (
    message.includes("no data found") ||
    message.includes("symbol may be delisted") ||
    message.includes("possibly delisted") ||
    message.includes("not found") ||
    message.includes("invalid ticker")
  ) {
    return {
      kind: "permanent",
      reason: sanitizeYahooErrorMessage(raw),
    }
  }

  if (
    message.includes("schema validation") ||
    message.includes("html error page") ||
    message.includes("bad request") ||
    message.includes("status 400") ||
    message.includes("status 401") ||
    message.includes("status 403") ||
    message.includes("status 404") ||
    message.includes("status 429") ||
    message.includes("timeout") ||
    message.includes("network") ||
    message.includes("fetch") ||
    message.includes("connection") ||
    message.includes("temporarily unavailable")
  ) {
    return {
      kind: "transient",
      reason: sanitizeYahooErrorMessage(raw),
    }
  }

  return {
    kind: "transient",
    reason: sanitizeYahooErrorMessage(raw),
  }
}

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(String(value).replace(/,/g, ""))
  return Number.isFinite(n) ? n : null
}

async function getTickerSnapshot(ticker: string): Promise<TickerSnapshot> {
  try {
    const [summary, quote] = await Promise.all([
      yahooFinance.quoteSummary(ticker, {
        modules: [
          "summaryDetail",
          "defaultKeyStatistics",
          "financialData",
          "assetProfile",
          "price",
        ],
      }),
      yahooFinance.quote(ticker).catch(() => null),
    ])

    const currentPrice =
      safeNumber((summary.financialData as any)?.currentPrice) ??
      safeNumber((quote as any)?.regularMarketPrice)

    const trailingEps =
      safeNumber((summary.defaultKeyStatistics as any)?.trailingEps) ??
      safeNumber((quote as any)?.epsTrailingTwelveMonths)

    const derivedTrailingPe =
      currentPrice !== null && trailingEps !== null && trailingEps > 0
        ? currentPrice / trailingEps
        : null

    const trailingPeCandidates = [
      safeNumber((summary.summaryDetail as any)?.trailingPE),
      safeNumber((summary.defaultKeyStatistics as any)?.trailingPE),
      safeNumber((summary.financialData as any)?.trailingPE),
      derivedTrailingPe,
      safeNumber((quote as any)?.trailingPE),
    ].filter((v) => v !== null && Number.isFinite(v as number)) as number[]

    const forwardPeCandidates = [
      safeNumber((summary.summaryDetail as any)?.forwardPE),
      safeNumber((summary.defaultKeyStatistics as any)?.forwardPE),
      safeNumber((summary.financialData as any)?.forwardPE),
      safeNumber((quote as any)?.forwardPE),
    ].filter((v) => v !== null && Number.isFinite(v as number)) as number[]

    const rawTrailingPe = trailingPeCandidates.length > 0 ? trailingPeCandidates[0] : null
    const rawForwardPe = forwardPeCandidates.length > 0 ? forwardPeCandidates[0] : null

    const peRatio = rawTrailingPe !== null && rawTrailingPe > 0 ? rawTrailingPe : null
    const forwardPe = rawForwardPe !== null && rawForwardPe > 0 ? rawForwardPe : null
    const peType = peRatio !== null ? "trailing" : forwardPe !== null ? "forward" : null

    const sector = ((summary.assetProfile as any)?.sector as string | undefined)?.trim() ?? null
    const industry = ((summary.assetProfile as any)?.industry as string | undefined)?.trim() ?? null
    const businessDescription =
      ((summary.assetProfile as any)?.longBusinessSummary as string | undefined)?.trim() ?? null

    return {
      peRatio,
      forwardPe,
      peType,
      sector,
      industry,
      businessDescription,
    }
  } catch {
    return {
      peRatio: null,
      forwardPe: null,
      peType: null,
      sector: null,
      industry: null,
      businessDescription: null,
    }
  }
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
    relativeReturn5d,
    relativeReturn10d,
    relativeReturn20d,
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
      0.1 * (passesPrice ? 1 : 0) +
      0.2 * (passesVolume ? 1 : 0) +
      0.35 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 60_000_000) +
      0.35 * scaleBetween(marketCap, MIN_MARKET_CAP, 10_000_000_000)
    )

  const momentumScore =
    18 *
    (
      0.12 * scaleBetween(oneDayReturn, 0, 6) +
      0.18 * scaleBetween(return5d, 1, 12) +
      0.3 * scaleBetween(return10d, 4, 20) +
      0.4 * scaleBetween(return20d, 8, 30)
    )

  const relativeStrengthScore =
    16 *
    (
      0.2 * scaleBetween(relativeReturn5d, 0, 8) +
      0.35 * scaleBetween(relativeReturn10d, MIN_RELATIVE_RETURN_10D, 12) +
      0.45 * scaleBetween(relativeReturn20d, MIN_RELATIVE_RETURN_20D, 18)
    )

  const volumeScore =
    16 *
    (
      0.75 * scaleBetween(volumeRatio, 1.1, 3.25) +
      0.25 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 40_000_000)
    )

  const distanceFrom20dHighPct =
    high20 > 0 ? ((latestClose - high20) / high20) * 100 : 0

  let breakoutQuality = 0
  if (breakout20d) breakoutQuality += 0.5
  if (breakout10d) breakoutQuality += 0.12
  if (nearHigh20) breakoutQuality += 0.08
  breakoutQuality += 0.14 * scaleBetween(breakoutClearancePct, MIN_BREAKOUT_CLEARANCE_PCT, 3)
  breakoutQuality += 0.16 * scaleBetween(closeInDayRange, 0.55, 1)
  breakoutQuality += 0.1 * scaleBetween(distanceFrom20dHighPct, 0, 4)

  const breakoutScore = 18 * clamp(breakoutQuality, 0, 1)

  const smaSpreadPct = sma20 > 0 ? ((sma10 - sma20) / sma20) * 100 : 0

  const trendScore =
    16 *
    (
      0.32 * (aboveSma20 ? 1 : 0) +
      0.24 * (shortTermTrendUp ? 1 : 0) +
      0.2 * scaleBetween(smaSpreadPct, 0.2, 4) +
      0.24 * (extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT ? 1 : 0)
    )

  let penaltyScore = 0

  if (!passesPrice) penaltyScore -= 6
  if (!passesVolume) penaltyScore -= 4
  if (!passesDollarVolume) penaltyScore -= 6
  if (!passesMarketCap) penaltyScore -= 4
  if (!aboveSma20) penaltyScore -= 6
  if (!shortTermTrendUp) penaltyScore -= 4
  if (oneDayReturn < 0) penaltyScore -= 2
  if (return5d < 0) penaltyScore -= 3
  if (return10d < 3) penaltyScore -= 4
  if (return20d < 8) penaltyScore -= 6
  if (relativeReturn5d < 0) penaltyScore -= 2
  if (relativeReturn10d < MIN_RELATIVE_RETURN_10D) penaltyScore -= 5
  if (relativeReturn20d < MIN_RELATIVE_RETURN_20D) penaltyScore -= 7
  if (volumeRatio < 1.0) penaltyScore -= 5
  if (!breakout20d) penaltyScore -= 8
  if (breakoutClearancePct < MIN_BREAKOUT_CLEARANCE_PCT) penaltyScore -= 5
  if (closeInDayRange < 0.5) penaltyScore -= 6
  if (extensionFromSma20Pct > MAX_EXTENSION_FROM_SMA20_PCT) penaltyScore -= 10

  if (return20d > MAX_STRONG_BUY_RETURN_20D) penaltyScore -= 8
  if (extensionFromSma20Pct > 28) penaltyScore -= 8
  if (extensionFromSma20Pct > 35) penaltyScore -= 10
  if (return20d > 55) penaltyScore -= 10

  const rawScore =
    qualityScore +
    momentumScore +
    relativeStrengthScore +
    volumeScore +
    breakoutScore +
    trendScore +
    penaltyScore

  const normalized = clamp((rawScore + 25) / 105, 0, 1)
  let candidateScore = Math.round(Math.pow(normalized, 1.16) * 100)

  const catalystCount = [
    oneDayReturn > 0,
    return5d >= 2,
    return10d >= MIN_STRONG_BUY_RETURN_10D,
    return20d >= MIN_STRONG_BUY_RETURN_20D,
    return20d <= MAX_STRONG_BUY_RETURN_20D,
    relativeReturn5d > 0,
    relativeReturn10d >= MIN_RELATIVE_RETURN_10D,
    relativeReturn20d >= MIN_RELATIVE_RETURN_20D,
    volumeRatio >= 1.25,
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
    relativeReturn10d >= MIN_RELATIVE_RETURN_10D &&
    relativeReturn20d >= MIN_RELATIVE_RETURN_20D &&
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT

  const eliteSetup =
    highConvictionSetup &&
    volumeRatio >= 2.1 &&
    return5d >= 4 &&
    return10d >= 10 &&
    return20d >= 18 &&
    relativeReturn10d >= 4 &&
    relativeReturn20d >= 8 &&
    avgDollarVolume20d >= 25_000_000 &&
    marketCap >= 1_000_000_000 &&
    catalystCount >= MIN_CATALYST_COUNT + 1

  if (!breakout20d || !aboveSma20) {
    candidateScore = Math.min(candidateScore, 60)
  } else if (!highConvictionSetup) {
    candidateScore = Math.min(candidateScore, 84)
  } else if (!eliteSetup) {
    candidateScore = Math.min(candidateScore, 93)
  } else if (
    !(
      volumeRatio >= 2.4 &&
      return10d >= 12 &&
      return20d >= 20 &&
      relativeReturn10d >= 5 &&
      relativeReturn20d >= 10 &&
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
    return10d >= 14 &&
    return20d >= 22 &&
    return20d <= 30 &&
    relativeReturn5d >= 3 &&
    relativeReturn10d >= 6 &&
    relativeReturn20d >= 12 &&
    avgDollarVolume20d >= 40_000_000 &&
    marketCap >= 2_500_000_000 &&
    catalystCount >= 10 &&
    closeInDayRange >= 0.8 &&
    breakoutClearancePct >= 1
  ) {
    candidateScore = 100
  }

  return {
    candidateScore: clamp(candidateScore, 0, 100),
    rawScore: round2(rawScore) ?? 0,
    qualityScore: round2(qualityScore) ?? 0,
    momentumScore: round2(momentumScore) ?? 0,
    relativeStrengthScore: round2(relativeStrengthScore) ?? 0,
    volumeScore: round2(volumeScore) ?? 0,
    breakoutScore: round2(breakoutScore) ?? 0,
    trendScore: round2(trendScore) ?? 0,
    penaltyScore: round2(penaltyScore) ?? 0,
    catalystCount,
    highConvictionSetup,
    eliteSetup,
  }
}

async function writeHistoryRow(
  candidateHistoryTable: any,
  row: CandidateUniverseRow,
  screenedOn: string,
  nowIso: string
) {
  const historyRow: CandidateHistoryRow = {
    ...row,
    screened_on: screenedOn,
    snapshot_key: `${screenedOn}_${row.ticker}`,
    created_at: nowIso,
  }

  return candidateHistoryTable.upsert(historyRow, {
    onConflict: "snapshot_key",
  })
}

async function removeFromUniverse(candidateUniverseTable: any, ticker: string) {
  return candidateUniverseTable.delete().eq("ticker", ticker)
}

async function upsertUniverseRow(candidateUniverseTable: any, row: CandidateUniverseRow) {
  return candidateUniverseTable.upsert(row, {
    onConflict: "ticker",
  })
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

    const benchmarkEndDate = new Date()
    const benchmarkStartDate = new Date()
    benchmarkStartDate.setDate(benchmarkStartDate.getDate() - 60)

    let benchmarkCandles: any[] = []

    try {
      benchmarkCandles = await yahooFinance.historical(BENCHMARK_TICKER, {
        period1: toIsoDateString(benchmarkStartDate),
        period2: toIsoDateString(benchmarkEndDate),
        interval: "1d",
      })
    } catch {
      benchmarkCandles = []
    }

    const benchmarkReturns = getBenchmarkReturns(benchmarkCandles)

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
    let strongBuyNowInBatch = 0
    let failedInBatch = 0
    let historyInserted = 0
    let historyWriteErrors = 0
    let removedFromUniverseInBatch = 0
    let keptUniverseOnTransientError = 0

    for (const company of (companies || []) as CompanyRow[]) {
      const ticker = normalizeTicker(company.ticker)

      try {
        if (!ticker || !company.cik) {
          failedInBatch += 1

          if (ticker) {
            const deleteResult = await removeFromUniverse(candidateUniverseTable, ticker)
            if (!deleteResult.error) removedFromUniverseInBatch += 1
          }

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
            pe_ratio: null,
            pe_forward: null,
            pe_type: null,
            sector: null,
            industry: null,
            business_description: null,
            avg_volume_20d: null,
            avg_dollar_volume_20d: null,
            one_day_return: null,
            return_5d: null,
            return_10d: null,
            return_20d: null,
            volume_ratio: null,
            breakout_20d: false,
            breakout_10d: false,
            above_sma_20: false,
            breakout_clearance_pct: null,
            extension_from_sma20_pct: null,
            close_in_day_range: null,
            catalyst_count: 0,
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

          const deleteResult = await removeFromUniverse(candidateUniverseTable, ticker)
          if (deleteResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: deleteResult.error.message,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          removedFromUniverseInBatch += 1

          const historyResult = await writeHistoryRow(
            candidateHistoryTable,
            excludedRow,
            screenedOn,
            nowIso
          )

          if (historyResult.error) {
            historyWriteErrors += 1
            results.push({
              ticker,
              ok: true,
              included: false,
              score: 0,
              tier: "excluded",
              reason: excludedRow.screen_reason,
              historyWarning: historyResult.error.message,
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
            reason: excludedRow.screen_reason,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        const endDate = new Date()
        const startDate = new Date()
        startDate.setDate(startDate.getDate() - 60)

        let candles: any[] | null = null
        let quote: any = null
        let snapshot: TickerSnapshot = {
          peRatio: null,
          forwardPe: null,
          peType: null,
          sector: null,
          industry: null,
          businessDescription: null,
        }

        try {
          ;[candles, quote, snapshot] = await Promise.all([
            yahooFinance.historical(ticker, {
              period1: toIsoDateString(startDate),
              period2: toIsoDateString(endDate),
              interval: "1d",
            }),
            yahooFinance.quote(ticker),
            getTickerSnapshot(ticker),
          ])
        } catch (err: any) {
          const disposition = classifyYahooError(err)

          if (disposition.kind === "permanent") {
            const deleteResult = await removeFromUniverse(candidateUniverseTable, ticker)
            if (!deleteResult.error) removedFromUniverseInBatch += 1

            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: disposition.reason,
              errorKind: "permanent_yahoo_error",
              removedFromUniverse: true,
            })

            await sleep(REQUEST_DELAY_MS)
            continue
          }

          keptUniverseOnTransientError += 1
          failedInBatch += 1
          results.push({
            ticker,
            ok: false,
            error: disposition.reason,
            errorKind: "transient_yahoo_error",
            removedFromUniverse: false,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

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
            pe_ratio: snapshot.peRatio,
            pe_forward: snapshot.forwardPe,
            pe_type: snapshot.peType,
            sector: snapshot.sector,
            industry: snapshot.industry,
            business_description: snapshot.businessDescription,
            avg_volume_20d: null,
            avg_dollar_volume_20d: null,
            one_day_return: null,
            return_5d: null,
            return_10d: null,
            return_20d: null,
            volume_ratio: null,
            breakout_20d: false,
            breakout_10d: false,
            above_sma_20: false,
            breakout_clearance_pct: null,
            extension_from_sma20_pct: null,
            close_in_day_range: null,
            catalyst_count: 0,
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

          const deleteResult = await removeFromUniverse(candidateUniverseTable, ticker)
          if (deleteResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: deleteResult.error.message,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          removedFromUniverseInBatch += 1

          const historyResult = await writeHistoryRow(
            candidateHistoryTable,
            row,
            screenedOn,
            nowIso
          )

          if (historyResult.error) {
            historyWriteErrors += 1
            results.push({
              ticker,
              ok: true,
              included: false,
              score: 0,
              tier: "not_included",
              reason: row.screen_reason,
              historyWarning: historyResult.error.message,
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
            tier: "not_included",
            reason: row.screen_reason,
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

        const relativeReturn5d = return5d - benchmarkReturns.return5d
        const relativeReturn10d = return10d - benchmarkReturns.return10d
        const relativeReturn20d = return20d - benchmarkReturns.return20d

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
          relativeReturn5d,
          relativeReturn10d,
          relativeReturn20d,
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

        const strongBuyNowCandidate =
          passesPrice &&
          passesVolume &&
          passesDollarVolume &&
          passesMarketCap &&
          breakout20d &&
          breakout10d &&
          aboveSma20 &&
          oneDayReturn >= 0 &&
          return10d >= MIN_STRONG_BUY_RETURN_10D &&
          return20d >= MIN_STRONG_BUY_RETURN_20D &&
          return20d <= MAX_STRONG_BUY_RETURN_20D &&
          relativeReturn10d >= MIN_RELATIVE_RETURN_10D &&
          relativeReturn20d >= MIN_RELATIVE_RETURN_20D &&
          volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
          breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
          closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
          extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT &&
          score >= MIN_STRONG_BUY_SCORE &&
          catalystCount >= MIN_CATALYST_COUNT

        const boardCandidate = score >= MIN_BOARD_SCORE
        const included = boardCandidate

        const reasons: string[] = []
        if (passesPrice) reasons.push(`price >= $${MIN_PRICE}`)
        if (passesVolume) reasons.push("20d avg volume")
        if (passesDollarVolume) reasons.push("20d dollar volume")
        if (passesMarketCap) reasons.push("market cap")
        if (oneDayReturn > 0) reasons.push("positive day")
        if (return5d >= 2) reasons.push("5d momentum")
        if (return10d >= MIN_STRONG_BUY_RETURN_10D) reasons.push("10d momentum")
        if (return20d >= MIN_STRONG_BUY_RETURN_20D) reasons.push("20d momentum")
        if (relativeReturn5d > 0) reasons.push("beats SPY over 5d")
        if (relativeReturn10d >= MIN_RELATIVE_RETURN_10D) reasons.push("beats SPY over 10d")
        if (relativeReturn20d >= MIN_RELATIVE_RETURN_20D) reasons.push("beats SPY over 20d")
        if (return20d <= MAX_STRONG_BUY_RETURN_20D) reasons.push("not overextended on 20d move")
        if (volumeRatio >= 1.25) reasons.push("volume expansion")
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
        if (score < MIN_BOARD_SCORE) exclusionReason = `Score below board threshold (${MIN_BOARD_SCORE})`
        else if (!passesPrice) exclusionReason = `Below $${MIN_PRICE} minimum price`
        else if (!passesVolume) exclusionReason = "Below minimum average volume"
        else if (!passesDollarVolume) exclusionReason = "Below minimum dollar volume"
        else if (!passesMarketCap) exclusionReason = "Below minimum market cap"
        else exclusionReason = "Did not qualify for board"

        const row: CandidateUniverseRow = {
          ticker,
          cik: company.cik,
          name: company.name,
          price: round2(latestClose),
          market_cap: marketCap || null,
          pe_ratio: round2(snapshot.peRatio),
          pe_forward: round2(snapshot.forwardPe),
          pe_type: snapshot.peType,
          sector: snapshot.sector,
          industry: snapshot.industry,
          business_description: snapshot.businessDescription,
          avg_volume_20d: round2(avgVolume20d),
          avg_dollar_volume_20d: round2(avgDollarVolume20d),
          one_day_return: round2(oneDayReturn),
          return_5d: round2(return5d),
          return_10d: round2(return10d),
          return_20d: round2(return20d),
          volume_ratio: round2(volumeRatio),
          breakout_20d: breakout20d,
          breakout_10d: breakout10d,
          above_sma_20: aboveSma20,
          breakout_clearance_pct: round2(breakoutClearancePct),
          extension_from_sma20_pct: round2(extensionFromSma20Pct),
          close_in_day_range: round2(closeInDayRange),
          catalyst_count: catalystCount,
          passes_price: passesPrice,
          passes_volume: passesVolume,
          passes_dollar_volume: passesDollarVolume,
          passes_market_cap: passesMarketCap,
          candidate_score: score,
          included,
          screen_reason: buildCandidateReason({
            included,
            strongBuyNow: strongBuyNowCandidate,
            boardCandidate,
            reasons,
            exclusionReason,
            score,
          }),
          last_screened_at: nowIso,
          updated_at: nowIso,
        }

        if (included) {
          const universeResult = await upsertUniverseRow(candidateUniverseTable, row)

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

          includedInBatch += 1
          if (strongBuyNowCandidate) strongBuyNowInBatch += 1
        } else {
          const deleteResult = await removeFromUniverse(candidateUniverseTable, ticker)

          if (deleteResult.error) {
            failedInBatch += 1
            results.push({
              ticker,
              ok: false,
              error: deleteResult.error.message,
            })
            await sleep(REQUEST_DELAY_MS)
            continue
          }

          removedFromUniverseInBatch += 1
        }

        const historyResult = await writeHistoryRow(
          candidateHistoryTable,
          row,
          screenedOn,
          nowIso
        )

        if (historyResult.error) {
          historyWriteErrors += 1
        } else {
          historyInserted += 1
        }

        results.push({
          ticker,
          ok: true,
          included,
          strongBuyNowCandidate,
          score,
          rawScore: round2(scoreDetails.rawScore),
          tier: strongBuyNowCandidate
            ? "strong_buy_now"
            : included
              ? "board_candidate"
              : "not_included",
          reason: row.screen_reason,
          price: round2(latestClose),
          oneDayReturn: round2(oneDayReturn),
          return5d: round2(return5d),
          return10d: round2(return10d),
          return20d: round2(return20d),
          relativeReturn5d: round2(relativeReturn5d),
          relativeReturn10d: round2(relativeReturn10d),
          relativeReturn20d: round2(relativeReturn20d),
          benchmarkReturn5d: round2(benchmarkReturns.return5d),
          benchmarkReturn10d: round2(benchmarkReturns.return10d),
          benchmarkReturn20d: round2(benchmarkReturns.return20d),
          volumeRatio: round2(volumeRatio),
          breakoutClearancePct: round2(breakoutClearancePct),
          extensionFromSma20Pct: round2(extensionFromSma20Pct),
          closeInDayRange: round2(closeInDayRange),
          catalystCount,
          historyWarning: historyResult.error ? historyResult.error.message : null,
          scoreBreakdown: {
            quality: round2(scoreDetails.qualityScore),
            momentum: round2(scoreDetails.momentumScore),
            relativeStrength: round2(scoreDetails.relativeStrengthScore),
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
      strongBuyNowInBatch,
      failedInBatch,
      historyWriteErrors,
      keptUniverseOnTransientError,
      removedFromUniverseInBatch,
      includedCount: includedCountError ? null : candidateCount,
      historyInserted,
      historyCount: historyCountError ? null : historyCount,
      retentionCleanup: retentionError ? retentionError.message : "ok",
      retainedDays: RETENTION_DAYS,
      screenedOn,
      thresholds: {
        benchmarkTicker: BENCHMARK_TICKER,
        minBoardScore: MIN_BOARD_SCORE,
        minPrice: MIN_PRICE,
        minAvgVolume20d: MIN_AVG_VOLUME_20D,
        minAvgDollarVolume20d: MIN_AVG_DOLLAR_VOLUME_20D,
        minMarketCap: MIN_MARKET_CAP,
        minStrongBuyScore: MIN_STRONG_BUY_SCORE,
        minStrongBuyVolumeRatio: MIN_STRONG_BUY_VOLUME_RATIO,
        minStrongBuyReturn10d: MIN_STRONG_BUY_RETURN_10D,
        minStrongBuyReturn20d: MIN_STRONG_BUY_RETURN_20D,
        minRelativeReturn10d: MIN_RELATIVE_RETURN_10D,
        minRelativeReturn20d: MIN_RELATIVE_RETURN_20D,
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