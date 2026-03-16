import { createClient } from "@supabase/supabase-js"
import YahooFinance from "yahoo-finance2"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CompanyRow = {
  id: number
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
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
  relative_strength_20d: number | null
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
  passed: boolean
  screen_reason: string
  last_screened_at: string
  as_of_date: string
  updated_at: string
}

type CandidateHistoryRow = CandidateUniverseRow & {
  screened_on: string
  snapshot_key: string
  created_at: string
}

type YahooErrorDisposition = {
  kind: "permanent" | "transient"
  reason: string
}

type StrongCompanyProfile = {
  profitMargin: number | null
  operatingMargin: number | null
  grossMargin: number | null
  returnOnEquity: number | null
  debtToEquity: number | null
  currentRatio: number | null
  revenueGrowth: number | null
  earningsGrowth: number | null
  freeCashflow: number | null
  operatingCashflow: number | null
  recommendationKey: string | null
}

type TickerSnapshot = {
  peRatio: number | null
  forwardPe: number | null
  peType: "trailing" | "forward" | null
  sector: string | null
  industry: string | null
  businessDescription: string | null
  companyProfile: StrongCompanyProfile
}

type BenchmarkReturns = {
  return5d: number
  return10d: number
  return20d: number
}

type CandidateMetricRow = {
  company: CompanyRow
  ticker: string
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
  relative_strength_20d: number
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
  snapshot: TickerSnapshot
  strongCompanyScore: number
  passesStrongCompanyGate: boolean
  strongCompanyReasons: string[]
  speculativePenalty: number
  ptrPriorityBonus: number
  filingPriorityBonus: number
  clusterPriorityBonus: number
}

type ScreeningPreparationResult =
  | {
      kind: "metric"
      metric: CandidateMetricRow
      result?: Record<string, any>
    }
  | {
      kind: "final_row"
      row: CandidateUniverseRow
      result?: Record<string, any>
    }
  | {
      kind: "error"
      ticker: string | null
      error: string
      errorKind?: string
      historyRow?: CandidateUniverseRow
      result?: Record<string, any>
    }

type StrongCompanyEvaluation = {
  passes: boolean
  score: number
  reasons: string[]
  failures: string[]
  speculativePenalty: number
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
  strongCompanyScore: number
  passesStrongCompanyGate: boolean
  speculativePenalty: number
  ptrPriorityBonus: number
  filingPriorityBonus: number
  clusterPriorityBonus: number
}

type CandidateScoreOutput = {
  candidateScore: number
  rawScore: number
  qualityScore: number
  fundamentalScore: number
  momentumScore: number
  relativeStrengthScore: number
  leadershipScore: number
  volumeScore: number
  breakoutScore: number
  trendScore: number
  evidenceScore: number
  penaltyScore: number
  catalystCount: number
  highConvictionSetup: boolean
  eliteSetup: boolean
}

const yahooFinance = new YahooFinance({
  queue: { concurrency: 2 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 150
const DEFAULT_BATCH = 100
const RETENTION_DAYS = 30

const BENCHMARK_TICKER = "SPY"

const MIN_PRICE = 12
const MIN_AVG_VOLUME_20D = 750_000
const MIN_AVG_DOLLAR_VOLUME_20D = 20_000_000
const MIN_MARKET_CAP = 2_500_000_000

const MIN_CANDIDATE_SCORE = 60
const MIN_PREQUALIFIED_SCORE = 68
const MIN_STRONG_BUY_SCORE = 82

const MIN_STRONG_BUY_VOLUME_RATIO = 1.35
const MIN_STRONG_BUY_RETURN_10D = 4
const MIN_STRONG_BUY_RETURN_20D = 8
const MIN_RELATIVE_RETURN_10D = 1
const MIN_RELATIVE_RETURN_20D = 2
const MAX_STRONG_BUY_RETURN_20D = 30
const MAX_EXTENSION_FROM_SMA20_PCT = 14
const MIN_BREAKOUT_CLEARANCE_PCT = 0.2
const MIN_CLOSE_IN_DAY_RANGE = 0.58
const MIN_CATALYST_COUNT = 7

const MIN_STRONG_COMPANY_SCORE = 62

const TICKER_CONCURRENCY = 2
const DB_CHUNK_SIZE = 250

const YAHOO_RETRY_ATTEMPTS = 3
const YAHOO_RETRY_BASE_DELAY_MS = 1200
const MAX_TRANSIENT_ERROR_RATE = 0.35

function avg(nums: number[]) {
  if (!nums.length) return 0
  return nums.reduce((sum, n) => sum + n, 0) / nums.length
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function snapshotDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function round2(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 100) / 100
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") return fallback
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

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(String(value).replace(/,/g, ""))
  return Number.isFinite(n) ? n : null
}

function normalizePercent(value: number | null): number | null {
  if (value === null || !Number.isFinite(value)) return null
  if (Math.abs(value) <= 1) return value * 100
  return value
}

function calcPercentChange(current: number, prior: number) {
  if (!prior || prior <= 0) return 0
  return ((current - prior) / prior) * 100
}

function getBenchmarkReturns(candles: any[]): BenchmarkReturns {
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

function buildCandidateReason(params: {
  prequalified: boolean
  strongBuyNow: boolean
  reasons: string[]
  exclusionReason?: string
  score?: number
}) {
  if (params.prequalified && params.strongBuyNow) {
    return `High-priority candidate (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.prequalified) {
    return `Candidate passed (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.exclusionReason) return params.exclusionReason

  return params.reasons.length
    ? `Not passed (${params.score ?? 0}): ${params.reasons.join(", ")}`
    : "No significant factors passed"
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

  return {
    kind: "transient",
    reason: sanitizeYahooErrorMessage(raw),
  }
}

function isProbablyCommonStockTicker(ticker: string) {
  if (!ticker) return false

  const t = ticker.trim().toUpperCase()

  if (t.includes("^")) return false
  if (t.includes("/")) return false
  if (/-P[A-Z0-9]+$/.test(t)) return false
  if (/\.P[R]?[A-Z0-9]+$/.test(t)) return false
  if (/-WT$/.test(t) || /-WTS$/.test(t) || /-WS$/.test(t)) return false
  if (/\.WT$/.test(t) || /\.WTS$/.test(t) || /\.WS$/.test(t)) return false
  if (/-RT$/.test(t) || /-RIGHT$/.test(t) || /-RIGHTS$/.test(t)) return false
  if (/\.RT$/.test(t) || /\.RGT$/.test(t)) return false
  if (/-U$/.test(t) || /\.U$/.test(t)) return false
  if (/PREFERRED/i.test(t) || /PREF/i.test(t) || /TEST/i.test(t)) return false

  return /^[A-Z0-9.-]{1,10}$/.test(t)
}

function isDebtMetricApplicable(sector: string | null) {
  const s = (sector || "").toLowerCase()
  return !["financial services", "real estate", "utilities"].includes(s)
}

function isLiquidityMetricApplicable(sector: string | null) {
  const s = (sector || "").toLowerCase()
  return !["financial services"].includes(s)
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function withYahooRetry<T>(fn: () => Promise<T>): Promise<T> {
  let lastError: unknown = null

  for (let attempt = 1; attempt <= YAHOO_RETRY_ATTEMPTS; attempt += 1) {
    try {
      return await fn()
    } catch (error) {
      lastError = error
      const disposition = classifyYahooError(error)

      if (disposition.kind === "permanent") {
        throw error
      }

      if (attempt < YAHOO_RETRY_ATTEMPTS) {
        const delayMs = YAHOO_RETRY_BASE_DELAY_MS * attempt
        await sleep(delayMs)
        continue
      }
    }
  }

  throw lastError
}

function evaluateStrongCompany(params: {
  marketCap: number
  avgDollarVolume20d: number
  return20d: number
  volumeRatio: number
  sector: string | null
  industry: string | null
  peRatio: number | null
  forwardPe: number | null
  companyProfile: StrongCompanyProfile
}): StrongCompanyEvaluation {
  const {
    marketCap,
    avgDollarVolume20d,
    return20d,
    volumeRatio,
    sector,
    industry,
    peRatio,
    forwardPe,
    companyProfile,
  } = params

  const reasons: string[] = []
  const failures: string[] = []
  let speculativePenalty = 0

  const positiveEarnings =
    (peRatio !== null && peRatio > 0 && peRatio < 90) ||
    (forwardPe !== null && forwardPe > 0 && forwardPe < 70)

  const positiveFreeCashFlow =
    companyProfile.freeCashflow !== null && companyProfile.freeCashflow > 0

  const positiveOperatingCashFlow =
    companyProfile.operatingCashflow !== null && companyProfile.operatingCashflow > 0

  const profitMargin = normalizePercent(companyProfile.profitMargin)
  const operatingMargin = normalizePercent(companyProfile.operatingMargin)
  const grossMargin = normalizePercent(companyProfile.grossMargin)
  const roe = normalizePercent(companyProfile.returnOnEquity)
  const revenueGrowth = normalizePercent(companyProfile.revenueGrowth)
  const earningsGrowth = normalizePercent(companyProfile.earningsGrowth)

  let score = 0

  if (marketCap >= MIN_MARKET_CAP) {
    score += 16
    reasons.push("mid/large-cap business")
  } else {
    failures.push("market cap below floor")
  }

  if (avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D) {
    score += 10
    reasons.push("solid liquidity")
  } else {
    failures.push("dollar volume too light")
  }

  if (positiveEarnings) {
    score += 14
    reasons.push("positive earnings profile")
  }

  if (positiveFreeCashFlow) {
    score += 12
    reasons.push("positive free cash flow")
  } else if (positiveOperatingCashFlow) {
    score += 7
    reasons.push("positive operating cash flow")
  }

  if (profitMargin !== null) {
    if (profitMargin >= 8) {
      score += 10
      reasons.push("healthy profit margin")
    } else if (profitMargin >= 0) {
      score += 5
      reasons.push("profitable")
    }
  }

  if (operatingMargin !== null) {
    if (operatingMargin >= 8) {
      score += 8
      reasons.push("healthy operating margin")
    } else if (operatingMargin >= 0) {
      score += 4
      reasons.push("positive operating margin")
    }
  }

  if (grossMargin !== null && grossMargin >= 30) {
    score += 4
    reasons.push("solid gross margin")
  }

  if (roe !== null && roe >= 10) {
    score += 6
    reasons.push("strong return on equity")
  }

  if (revenueGrowth !== null) {
    if (revenueGrowth >= 6) {
      score += 5
      reasons.push("solid revenue growth")
    } else if (revenueGrowth < -8) {
      failures.push("revenue shrinking")
    }
  }

  if (earningsGrowth !== null) {
    if (earningsGrowth >= 6) {
      score += 5
      reasons.push("solid earnings growth")
    } else if (earningsGrowth < -12) {
      failures.push("earnings shrinking")
    }
  }

  if (isDebtMetricApplicable(sector)) {
    if (companyProfile.debtToEquity !== null) {
      if (companyProfile.debtToEquity <= 140) {
        score += 6
        reasons.push("manageable leverage")
      } else if (companyProfile.debtToEquity > 260) {
        failures.push("leverage too high")
      }
    }
  }

  if (isLiquidityMetricApplicable(sector)) {
    if (companyProfile.currentRatio !== null) {
      if (companyProfile.currentRatio >= 1.0) {
        score += 3
        reasons.push("healthy short-term liquidity")
      } else if (companyProfile.currentRatio < 0.8) {
        failures.push("weak current ratio")
      }
    }
  }

  const industryText = `${sector || ""} ${industry || ""}`.toLowerCase()

  if (industryText.includes("biotechnology")) {
    if (!positiveEarnings && !positiveFreeCashFlow) {
      speculativePenalty -= 22
      failures.push("speculative biotech profile")
    }
  }

  if (return20d > 32 && volumeRatio > 3.5 && marketCap < 10_000_000_000) {
    speculativePenalty -= 12
    failures.push("too extended / momentum spike")
  }

  if (!positiveEarnings && !positiveFreeCashFlow) {
    speculativePenalty -= 16
  }

  const passes =
    score >= MIN_STRONG_COMPANY_SCORE &&
    marketCap >= MIN_MARKET_CAP &&
    avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D &&
    speculativePenalty > -30

  return {
    passes,
    score: clamp(score + speculativePenalty, 0, 100),
    reasons,
    failures,
    speculativePenalty,
  }
}

function emptySnapshot(): TickerSnapshot {
  return {
    peRatio: null,
    forwardPe: null,
    peType: null,
    sector: null,
    industry: null,
    businessDescription: null,
    companyProfile: {
      profitMargin: null,
      operatingMargin: null,
      grossMargin: null,
      returnOnEquity: null,
      debtToEquity: null,
      currentRatio: null,
      revenueGrowth: null,
      earningsGrowth: null,
      freeCashflow: null,
      operatingCashflow: null,
      recommendationKey: null,
    },
  }
}

function buildTickerSnapshot(summary: any, quote: any): TickerSnapshot {
  const currentPrice =
    safeNumber((summary?.financialData as any)?.currentPrice) ??
    safeNumber((quote as any)?.regularMarketPrice)

  const trailingEps =
    safeNumber((summary?.defaultKeyStatistics as any)?.trailingEps) ??
    safeNumber((quote as any)?.epsTrailingTwelveMonths)

  const derivedTrailingPe =
    currentPrice !== null && trailingEps !== null && trailingEps > 0
      ? currentPrice / trailingEps
      : null

  const trailingPeCandidates = [
    safeNumber((summary?.summaryDetail as any)?.trailingPE),
    safeNumber((summary?.defaultKeyStatistics as any)?.trailingPE),
    safeNumber((summary?.financialData as any)?.trailingPE),
    derivedTrailingPe,
    safeNumber((quote as any)?.trailingPE),
  ].filter((v) => v !== null && Number.isFinite(v as number)) as number[]

  const forwardPeCandidates = [
    safeNumber((summary?.summaryDetail as any)?.forwardPE),
    safeNumber((summary?.defaultKeyStatistics as any)?.forwardPE),
    safeNumber((summary?.financialData as any)?.forwardPE),
    safeNumber((quote as any)?.forwardPE),
  ].filter((v) => v !== null && Number.isFinite(v as number)) as number[]

  const rawTrailingPe = trailingPeCandidates.length > 0 ? trailingPeCandidates[0] : null
  const rawForwardPe = forwardPeCandidates.length > 0 ? forwardPeCandidates[0] : null

  const peRatio = rawTrailingPe !== null && rawTrailingPe > 0 ? rawTrailingPe : null
  const forwardPe = rawForwardPe !== null && rawForwardPe > 0 ? rawForwardPe : null
  const peType = peRatio !== null ? "trailing" : forwardPe !== null ? "forward" : null

  return {
    peRatio,
    forwardPe,
    peType,
    sector: ((summary?.assetProfile as any)?.sector as string | undefined)?.trim() ?? null,
    industry: ((summary?.assetProfile as any)?.industry as string | undefined)?.trim() ?? null,
    businessDescription:
      ((summary?.assetProfile as any)?.longBusinessSummary as string | undefined)?.trim() ?? null,
    companyProfile: {
      profitMargin: safeNumber((summary?.financialData as any)?.profitMargins),
      operatingMargin: safeNumber((summary?.financialData as any)?.operatingMargins),
      grossMargin: safeNumber((summary?.financialData as any)?.grossMargins),
      returnOnEquity: safeNumber((summary?.financialData as any)?.returnOnEquity),
      debtToEquity: safeNumber((summary?.financialData as any)?.debtToEquity),
      currentRatio: safeNumber((summary?.financialData as any)?.currentRatio),
      revenueGrowth: safeNumber((summary?.financialData as any)?.revenueGrowth),
      earningsGrowth: safeNumber((summary?.financialData as any)?.earningsGrowth),
      freeCashflow: safeNumber((summary?.financialData as any)?.freeCashflow),
      operatingCashflow: safeNumber((summary?.financialData as any)?.operatingCashflow),
      recommendationKey:
        ((summary?.financialData as any)?.recommendationKey as string | undefined)?.trim() ?? null,
    },
  }
}

async function getTickerData(ticker: string) {
  return await withYahooRetry(async () => {
    const endDate = new Date()
    const startDate = new Date()
    startDate.setDate(startDate.getDate() - 60)

    const [candles, quote, summary] = await Promise.all([
      yahooFinance.historical(ticker, {
        period1: toIsoDateString(startDate),
        period2: toIsoDateString(endDate),
        interval: "1d",
      }),
      yahooFinance.quote(ticker),
      yahooFinance.quoteSummary(ticker, {
        modules: [
          "summaryDetail",
          "defaultKeyStatistics",
          "financialData",
          "assetProfile",
          "price",
        ],
      }),
    ])

    return {
      candles,
      quote,
      snapshot: buildTickerSnapshot(summary, quote),
    }
  })
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
    strongCompanyScore,
    passesStrongCompanyGate,
    speculativePenalty,
    ptrPriorityBonus,
    filingPriorityBonus,
    clusterPriorityBonus,
  } = input

  const qualityScore =
    10 *
    (
      0.1 * (passesPrice ? 1 : 0) +
      0.15 * (passesVolume ? 1 : 0) +
      0.25 * (passesDollarVolume ? 1 : 0) +
      0.25 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 100_000_000) +
      0.25 * scaleBetween(marketCap, MIN_MARKET_CAP, 40_000_000_000)
    )

  const fundamentalScore =
    20 *
    (
      0.4 * scaleBetween(strongCompanyScore, 50, 100) +
      0.6 * (passesStrongCompanyGate ? 1 : 0)
    )

  const momentumScore =
    13 *
    (
      0.08 * scaleBetween(oneDayReturn, -1, 3) +
      0.16 * scaleBetween(return5d, 0, 8) +
      0.3 * scaleBetween(return10d, 2, 14) +
      0.46 * scaleBetween(return20d, 5, 22)
    )

  const relativeStrengthScore =
    14 *
    (
      0.14 * scaleBetween(relativeReturn5d, 0, 5) +
      0.34 * scaleBetween(relativeReturn10d, 1, 10) +
      0.52 * scaleBetween(relativeReturn20d, 2, 16)
    )

  const leadershipScore =
    7 *
    (
      0.15 * scaleBetween(relativeReturn5d, 1, 4) +
      0.35 * scaleBetween(relativeReturn10d, 2, 8) +
      0.5 * scaleBetween(relativeReturn20d, 3, 12)
    )

  const volumeScore =
    7 *
    (
      0.75 * scaleBetween(volumeRatio, 1, 2.8) +
      0.25 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 80_000_000)
    )

  const distanceFrom20dHighPct =
    high20 > 0 ? ((latestClose - high20) / high20) * 100 : 0

  let breakoutQuality = 0
  if (breakout20d) breakoutQuality += 0.46
  if (breakout10d) breakoutQuality += 0.12
  if (nearHigh20) breakoutQuality += 0.16
  breakoutQuality += 0.14 * scaleBetween(breakoutClearancePct, MIN_BREAKOUT_CLEARANCE_PCT, 2.0)
  breakoutQuality += 0.08 * scaleBetween(closeInDayRange, MIN_CLOSE_IN_DAY_RANGE, 1)
  breakoutQuality += 0.04 * scaleBetween(distanceFrom20dHighPct, -0.5, 2.5)

  const breakoutScore = 7 * clamp(breakoutQuality, 0, 1)

  const smaSpreadPct = sma20 > 0 ? ((sma10 - sma20) / sma20) * 100 : 0

  const trendScore =
    7 *
    (
      0.36 * (aboveSma20 ? 1 : 0) +
      0.24 * (shortTermTrendUp ? 1 : 0) +
      0.16 * scaleBetween(smaSpreadPct, 0.2, 3) +
      0.24 * scaleBetween(-extensionFromSma20Pct, -MAX_EXTENSION_FROM_SMA20_PCT, 0)
    )

  const evidenceScore = clamp(
    ptrPriorityBonus + filingPriorityBonus + clusterPriorityBonus,
    0,
    20
  )

  let penaltyScore = 0

  if (!passesStrongCompanyGate) penaltyScore -= 10
  if (strongCompanyScore < MIN_STRONG_COMPANY_SCORE) penaltyScore -= 5
  if (!passesPrice) penaltyScore -= 6
  if (!passesVolume) penaltyScore -= 4
  if (!passesDollarVolume) penaltyScore -= 6
  if (!passesMarketCap) penaltyScore -= 5
  if (!aboveSma20) penaltyScore -= 4
  if (!shortTermTrendUp) penaltyScore -= 3
  if (oneDayReturn < -2) penaltyScore -= 2
  if (return5d < 0) penaltyScore -= 3
  if (return10d < 1) penaltyScore -= 4
  if (return20d < 3) penaltyScore -= 5
  if (relativeReturn10d < 0) penaltyScore -= 3
  if (relativeReturn20d < 1) penaltyScore -= 5
  if (volumeRatio < 1.0) penaltyScore -= 3
  if (!nearHigh20) penaltyScore -= 2
  if (!breakout10d && !breakout20d) penaltyScore -= 3
  if (closeInDayRange < 0.45) penaltyScore -= 3
  if (extensionFromSma20Pct > MAX_EXTENSION_FROM_SMA20_PCT) penaltyScore -= 8
  if (return20d > MAX_STRONG_BUY_RETURN_20D) penaltyScore -= 6
  if (return20d > 35) penaltyScore -= 8

  penaltyScore += speculativePenalty

  const rawScore =
    qualityScore +
    fundamentalScore +
    momentumScore +
    relativeStrengthScore +
    leadershipScore +
    volumeScore +
    breakoutScore +
    trendScore +
    evidenceScore +
    penaltyScore

  const normalized = clamp((rawScore + 25) / 100, 0, 1)
  let candidateScore = Math.round(Math.pow(normalized, 1.08) * 100)

  const catalystCount = [
    passesStrongCompanyGate,
    strongCompanyScore >= MIN_STRONG_COMPANY_SCORE,
    oneDayReturn > 0,
    return5d >= 1.5,
    return10d >= MIN_STRONG_BUY_RETURN_10D,
    return20d >= MIN_STRONG_BUY_RETURN_20D,
    return20d <= MAX_STRONG_BUY_RETURN_20D,
    relativeReturn5d > 0,
    relativeReturn10d >= MIN_RELATIVE_RETURN_10D,
    relativeReturn20d >= MIN_RELATIVE_RETURN_20D,
    volumeRatio >= 1.15,
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO,
    breakout20d,
    breakout10d,
    nearHigh20,
    aboveSma20,
    shortTermTrendUp,
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT,
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE,
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT,
    avgDollarVolume20d >= 40_000_000,
    marketCap >= 8_000_000_000,
    ptrPriorityBonus > 0,
    filingPriorityBonus > 0,
    clusterPriorityBonus > 0,
  ].filter(Boolean).length

  const highConvictionSetup =
    passesStrongCompanyGate &&
    passesPrice &&
    passesVolume &&
    passesDollarVolume &&
    passesMarketCap &&
    aboveSma20 &&
    shortTermTrendUp &&
    nearHigh20 &&
    return10d >= MIN_STRONG_BUY_RETURN_10D &&
    return20d >= MIN_STRONG_BUY_RETURN_20D &&
    relativeReturn10d >= MIN_RELATIVE_RETURN_10D &&
    relativeReturn20d >= MIN_RELATIVE_RETURN_20D &&
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT

  const eliteSetup =
    highConvictionSetup &&
    strongCompanyScore >= 78 &&
    volumeRatio >= 1.8 &&
    return5d >= 3 &&
    return10d >= 6 &&
    return20d >= 12 &&
    relativeReturn10d >= 2.5 &&
    relativeReturn20d >= 5 &&
    avgDollarVolume20d >= 45_000_000 &&
    marketCap >= 10_000_000_000 &&
    catalystCount >= MIN_CATALYST_COUNT + 1

  if (!passesStrongCompanyGate) {
    candidateScore = Math.min(candidateScore, 64)
  } else if (!aboveSma20 || !shortTermTrendUp) {
    candidateScore = Math.min(candidateScore, 72)
  } else if (!highConvictionSetup) {
    candidateScore = Math.min(candidateScore, 89)
  } else if (!eliteSetup) {
    candidateScore = Math.min(candidateScore, 96)
  } else {
    candidateScore = Math.min(candidateScore, 100)
  }

  return {
    candidateScore: clamp(candidateScore, 0, 100),
    rawScore: round2(rawScore) ?? 0,
    qualityScore: round2(qualityScore) ?? 0,
    fundamentalScore: round2(fundamentalScore) ?? 0,
    momentumScore: round2(momentumScore) ?? 0,
    relativeStrengthScore: round2(relativeStrengthScore) ?? 0,
    leadershipScore: round2(leadershipScore) ?? 0,
    volumeScore: round2(volumeScore) ?? 0,
    breakoutScore: round2(breakoutScore) ?? 0,
    trendScore: round2(trendScore) ?? 0,
    evidenceScore: round2(evidenceScore) ?? 0,
    penaltyScore: round2(penaltyScore) ?? 0,
    catalystCount,
    highConvictionSetup,
    eliteSetup,
  }
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length)
  let nextIndex = 0

  async function runner() {
    while (true) {
      const current = nextIndex
      nextIndex += 1
      if (current >= items.length) return
      results[current] = await worker(items[current], current)
    }
  }

  const runners = Array.from(
    { length: Math.min(concurrency, Math.max(items.length, 1)) },
    () => runner()
  )

  await Promise.all(runners)
  return results
}

async function upsertRowsInChunksDetailed(
  table: any,
  rows: CandidateUniverseRow[] | CandidateHistoryRow[],
  onConflict: string
) {
  let insertedOrUpdated = 0
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict })
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    } else {
      insertedOrUpdated += chunk.length
    }
  }

  return {
    insertedOrUpdated,
    errorCount,
    errors,
  }
}

function makeHistoryRow(
  row: CandidateUniverseRow,
  screenedOn: string,
  nowIso: string
): CandidateHistoryRow {
  return {
    ...row,
    screened_on: screenedOn,
    snapshot_key: `${screenedOn}_${row.ticker}`,
    created_at: nowIso,
  }
}

function makeExcludedRow(params: {
  companyId?: number | null
  ticker: string
  cik: string
  name: string | null
  isActive?: boolean | null
  isEligible?: boolean | null
  hasInsiderTrades?: boolean | null
  hasPtrForms?: boolean | null
  hasClusters?: boolean | null
  eligibilityReason?: string | null
  peRatio?: number | null
  forwardPe?: number | null
  peType?: string | null
  sector?: string | null
  industry?: string | null
  businessDescription?: string | null
  screenReason: string
  nowIso: string
}): CandidateUniverseRow {
  return {
    company_id: params.companyId ?? null,
    ticker: params.ticker,
    cik: params.cik,
    name: params.name,
    is_active: params.isActive ?? true,
    is_eligible: params.isEligible ?? null,
    has_insider_trades: params.hasInsiderTrades ?? null,
    has_ptr_forms: params.hasPtrForms ?? null,
    has_clusters: params.hasClusters ?? null,
    eligibility_reason: params.eligibilityReason ?? null,
    price: null,
    market_cap: null,
    pe_ratio: round2(params.peRatio ?? null),
    pe_forward: round2(params.forwardPe ?? null),
    pe_type: params.peType ?? null,
    sector: params.sector ?? null,
    industry: params.industry ?? null,
    business_description: params.businessDescription ?? null,
    avg_volume_20d: null,
    avg_dollar_volume_20d: null,
    one_day_return: null,
    return_5d: null,
    return_10d: null,
    return_20d: null,
    relative_strength_20d: null,
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
    passed: false,
    screen_reason: params.screenReason,
    last_screened_at: params.nowIso,
    as_of_date: params.nowIso,
    updated_at: params.nowIso,
  }
}

async function prepareTickerForScoring(
  company: CompanyRow,
  benchmarkReturns: BenchmarkReturns,
  nowIso: string,
  includeResults: boolean
): Promise<ScreeningPreparationResult> {
  const ticker = normalizeTicker(company.ticker)

  if (!ticker || !company.cik) {
    return {
      kind: "error",
      ticker: ticker || null,
      error: "Missing ticker or cik",
      result: includeResults
        ? {
            ticker: ticker || null,
            ok: false,
            error: "Missing ticker or cik",
          }
        : undefined,
    }
  }

  if (!isProbablyCommonStockTicker(ticker)) {
    const row = makeExcludedRow({
      companyId: company.id,
      ticker,
      cik: company.cik,
      name: company.name,
      isActive: company.is_active,
      isEligible: company.is_eligible,
      hasInsiderTrades: company.has_insider_trades,
      hasPtrForms: company.has_ptr_forms,
      hasClusters: company.has_clusters,
      eligibilityReason: company.eligibility_reason,
      screenReason: "Excluded likely non-common-share ticker",
      nowIso,
    })

    return {
      kind: "final_row",
      row,
      result: includeResults
        ? {
            ticker,
            ok: true,
            passed: false,
            score: 0,
            tier: "excluded",
            reason: row.screen_reason,
          }
        : undefined,
    }
  }

  let candles: any[] = []
  let quote: any = null
  let snapshot: TickerSnapshot = emptySnapshot()

  try {
    const tickerData = await getTickerData(ticker)
    candles = tickerData.candles || []
    quote = tickerData.quote
    snapshot = tickerData.snapshot
  } catch (err: any) {
    const disposition = classifyYahooError(err)

    const historyRow = makeExcludedRow({
      companyId: company.id,
      ticker,
      cik: company.cik,
      name: company.name,
      isActive: company.is_active,
      isEligible: company.is_eligible,
      hasInsiderTrades: company.has_insider_trades,
      hasPtrForms: company.has_ptr_forms,
      hasClusters: company.has_clusters,
      eligibilityReason: company.eligibility_reason,
      screenReason:
        disposition.kind === "permanent"
          ? `Permanent Yahoo error: ${disposition.reason}`
          : `Transient Yahoo error: ${disposition.reason}`,
      nowIso,
    })

    return {
      kind: "error",
      ticker,
      error: disposition.reason,
      errorKind:
        disposition.kind === "permanent"
          ? "permanent_yahoo_error"
          : "transient_yahoo_error",
      historyRow,
      result: includeResults
        ? {
            ticker,
            ok: false,
            error: disposition.reason,
            errorKind:
              disposition.kind === "permanent"
                ? "permanent_yahoo_error"
                : "transient_yahoo_error",
          }
        : undefined,
    }
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
    const row = makeExcludedRow({
      companyId: company.id,
      ticker,
      cik: company.cik,
      name: company.name,
      isActive: company.is_active,
      isEligible: company.is_eligible,
      hasInsiderTrades: company.has_insider_trades,
      hasPtrForms: company.has_ptr_forms,
      hasClusters: company.has_clusters,
      eligibilityReason: company.eligibility_reason,
      peRatio: snapshot.peRatio,
      forwardPe: snapshot.forwardPe,
      peType: snapshot.peType,
      sector: snapshot.sector,
      industry: snapshot.industry,
      businessDescription: snapshot.businessDescription,
      screenReason: "Not enough price history",
      nowIso,
    })

    return {
      kind: "final_row",
      row,
      result: includeResults
        ? {
            ticker,
            ok: true,
            passed: false,
            score: 0,
            tier: "not_passed",
            reason: row.screen_reason,
          }
        : undefined,
    }
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
  const avgDollarVolume20d = avg(prior20.map((c) => Number(c.close || 0) * Number(c.volume || 0)))
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

  const breakoutClearancePct = high20 > 0 ? ((latestClose - high20) / high20) * 100 : 0
  const extensionFromSma20Pct = sma20 > 0 ? ((latestClose - sma20) / sma20) * 100 : 0

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

  const strongCompanyEval = evaluateStrongCompany({
    marketCap,
    avgDollarVolume20d,
    return20d,
    volumeRatio,
    sector: snapshot.sector,
    industry: snapshot.industry,
    peRatio: snapshot.peRatio,
    forwardPe: snapshot.forwardPe,
    companyProfile: snapshot.companyProfile,
  })

  return {
    kind: "metric",
    metric: {
      company,
      ticker,
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
      relative_strength_20d: relativeReturn20d,
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
      snapshot,
      strongCompanyScore: strongCompanyEval.score,
      passesStrongCompanyGate: strongCompanyEval.passes,
      strongCompanyReasons: strongCompanyEval.reasons,
      speculativePenalty: strongCompanyEval.speculativePenalty,
      ptrPriorityBonus: company.has_ptr_forms ? 8 : 0,
      filingPriorityBonus:
        company.has_insider_trades
          ? 6
          : (company.eligibility_reason || "").includes("high_priority_filings")
            ? 4
            : 0,
      clusterPriorityBonus: company.has_clusters ? 5 : 0,
    },
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
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const { searchParams } = new URL(request.url)

    const start = parseInteger(searchParams.get("start"), 0)
    const batch = parseInteger(searchParams.get("batch"), DEFAULT_BATCH)
    const onlyActiveParam = (searchParams.get("onlyActive") || "true").toLowerCase()
    const onlyActive = onlyActiveParam !== "false"

    const includeResults = (searchParams.get("includeResults") || "false").toLowerCase() === "true"
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

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
      benchmarkCandles = await withYahooRetry(() =>
        yahooFinance.historical(BENCHMARK_TICKER, {
          period1: toIsoDateString(benchmarkStartDate),
          period2: toIsoDateString(benchmarkEndDate),
          interval: "1d",
        })
      )
    } catch {
      benchmarkCandles = []
    }

    const benchmarkReturns = getBenchmarkReturns(benchmarkCandles)

    const universe = (searchParams.get("universe") || "all").toLowerCase()

    const sourceTableName =
      universe === "eligible" ? "candidate_universe" : "companies"

    const sourceTable = supabase.from(sourceTableName) as any
    const candidateHistoryTable = supabase.from("candidate_screen_history") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any

    let companyQuery =
      universe === "eligible"
        ? sourceTable
            .select(
              "company_id, ticker, cik, name, is_active, is_eligible, has_insider_trades, has_ptr_forms, has_clusters, eligibility_reason"
            )
            .eq("is_eligible", true)
            .not("cik", "is", null)
            .order("ticker", { ascending: true })
            .range(from, to)
        : sourceTable
            .select("id, ticker, cik, name, is_active, is_eligible, has_insider_trades, has_ptr_forms, has_clusters, eligibility_reason")
            .not("cik", "is", null)
            .order("id", { ascending: true })
            .range(from, to)

    let countQuery =
      universe === "eligible"
        ? sourceTable
            .select("*", { count: "exact", head: true })
            .eq("is_eligible", true)
            .not("cik", "is", null)
        : sourceTable
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

    const companyList: CompanyRow[] = ((companies || []) as any[]).map((row) => ({
      id: row.company_id ?? row.id,
      ticker: row.ticker,
      cik: row.cik,
      name: row.name ?? null,
      is_active: row.is_active ?? true,
      is_eligible: row.is_eligible ?? null,
      has_insider_trades: row.has_insider_trades ?? null,
      has_ptr_forms: row.has_ptr_forms ?? null,
      has_clusters: row.has_clusters ?? null,
      eligibility_reason: row.eligibility_reason ?? null,
    }))

    const preparation = await mapWithConcurrency(
      companyList,
      TICKER_CONCURRENCY,
      async (company) =>
        prepareTickerForScoring(company, benchmarkReturns, nowIso, includeResults)
    )

    const results: Array<Record<string, any>> = []
    const metricRows: CandidateMetricRow[] = []
    const historyRows: CandidateHistoryRow[] = []
    const currentRows: CandidateUniverseRow[] = []

    let failedInBatch = 0
    let transientYahooErrorsInBatch = 0

    for (const item of preparation) {
      if (item.kind === "metric") {
        metricRows.push(item.metric)
        continue
      }

      if (item.kind === "error") {
        failedInBatch += 1

        if (item.errorKind === "transient_yahoo_error") {
          transientYahooErrorsInBatch += 1
        }

        if (item.historyRow) {
          historyRows.push(makeHistoryRow(item.historyRow, screenedOn, nowIso))
          currentRows.push(item.historyRow)
        }

        if (item.result && includeResults) {
          results.push(item.result)
        }

        continue
      }

      historyRows.push(makeHistoryRow(item.row, screenedOn, nowIso))
      currentRows.push(item.row)

      if (item.result && includeResults) {
        results.push(item.result)
      }
    }

    let passedInBatch = 0
    let highPriorityInBatch = 0

    for (const metric of metricRows) {
      const scoreDetails = calculateCandidateScore({
        latestClose: metric.latestClose,
        marketCap: metric.marketCap,
        avgVolume20d: metric.avgVolume20d,
        avgDollarVolume20d: metric.avgDollarVolume20d,
        return5d: metric.return5d,
        return10d: metric.return10d,
        return20d: metric.return20d,
        relativeReturn5d: metric.relativeReturn5d,
        relativeReturn10d: metric.relativeReturn10d,
        relativeReturn20d: metric.relativeReturn20d,
        oneDayReturn: metric.oneDayReturn,
        volumeRatio: metric.volumeRatio,
        breakout20d: metric.breakout20d,
        breakout10d: metric.breakout10d,
        nearHigh20: metric.nearHigh20,
        aboveSma20: metric.aboveSma20,
        shortTermTrendUp: metric.shortTermTrendUp,
        sma10: metric.sma10,
        sma20: metric.sma20,
        high20: metric.high20,
        breakoutClearancePct: metric.breakoutClearancePct,
        extensionFromSma20Pct: metric.extensionFromSma20Pct,
        closeInDayRange: metric.closeInDayRange,
        passesPrice: metric.passesPrice,
        passesVolume: metric.passesVolume,
        passesDollarVolume: metric.passesDollarVolume,
        passesMarketCap: metric.passesMarketCap,
        strongCompanyScore: metric.strongCompanyScore,
        passesStrongCompanyGate: metric.passesStrongCompanyGate,
        speculativePenalty: metric.speculativePenalty,
        ptrPriorityBonus: metric.ptrPriorityBonus,
        filingPriorityBonus: metric.filingPriorityBonus,
        clusterPriorityBonus: metric.clusterPriorityBonus,
      })

      const score = scoreDetails.candidateScore
      const catalystCount = scoreDetails.catalystCount

      const highPriorityCandidate =
        metric.passesStrongCompanyGate &&
        metric.strongCompanyScore >= 72 &&
        metric.passesPrice &&
        metric.passesVolume &&
        metric.passesDollarVolume &&
        metric.passesMarketCap &&
        metric.aboveSma20 &&
        metric.shortTermTrendUp &&
        metric.return10d >= MIN_STRONG_BUY_RETURN_10D &&
        metric.return20d >= MIN_STRONG_BUY_RETURN_20D &&
        metric.relativeReturn10d >= MIN_RELATIVE_RETURN_10D &&
        metric.relativeReturn20d >= MIN_RELATIVE_RETURN_20D &&
        metric.volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
        score >= MIN_STRONG_BUY_SCORE &&
        catalystCount >= MIN_CATALYST_COUNT &&
        (
          metric.company.has_ptr_forms ||
          metric.company.has_insider_trades ||
          metric.company.has_clusters
        )

      const passed =
        metric.passesStrongCompanyGate &&
        metric.passesPrice &&
        metric.passesVolume &&
        metric.passesDollarVolume &&
        metric.passesMarketCap &&
        metric.aboveSma20 &&
        metric.shortTermTrendUp &&
        metric.return10d >= 2 &&
        metric.return20d >= 4 &&
        metric.relativeReturn20d >= 1 &&
        score >= MIN_PREQUALIFIED_SCORE

      const reasons: string[] = []

      if (metric.company.has_ptr_forms) reasons.push("PTR support")
      if (metric.company.has_insider_trades) reasons.push("insider filing support")
      if (metric.company.has_clusters) reasons.push("cluster activity support")
      if ((metric.company.eligibility_reason || "").includes("high_priority_filings")) {
        reasons.push("high-priority filing support")
      }

      if (metric.passesStrongCompanyGate) reasons.push("strong underlying company")
      if (metric.strongCompanyScore >= 75) reasons.push("high fundamental quality")
      reasons.push(...metric.strongCompanyReasons)

      if (metric.passesPrice) reasons.push(`price >= $${MIN_PRICE}`)
      if (metric.passesVolume) reasons.push("20d avg volume")
      if (metric.passesDollarVolume) reasons.push("20d dollar volume")
      if (metric.passesMarketCap) reasons.push("market cap")
      if (metric.oneDayReturn > 0) reasons.push("positive day")
      if (metric.return5d >= 1.5) reasons.push("5d momentum")
      if (metric.return10d >= MIN_STRONG_BUY_RETURN_10D) reasons.push("10d momentum")
      if (metric.return20d >= MIN_STRONG_BUY_RETURN_20D) reasons.push("20d momentum")
      if (metric.relativeReturn10d >= MIN_RELATIVE_RETURN_10D) reasons.push("beats SPY over 10d")
      if (metric.relativeReturn20d >= MIN_RELATIVE_RETURN_20D) reasons.push("beats SPY over 20d")
      if (metric.volumeRatio >= 1.2) reasons.push("volume expansion")
      if (metric.volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO) reasons.push("strong volume expansion")
      if (metric.breakout10d) reasons.push("10d breakout")
      if (metric.breakout20d) reasons.push("20d breakout")
      if (metric.nearHigh20) reasons.push("trading near 20d high")
      if (metric.breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT) reasons.push("clean breakout clearance")
      if (metric.aboveSma20) reasons.push("above 20d average")
      if (metric.shortTermTrendUp) reasons.push("short-term trend acceleration")
      if (metric.closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE) reasons.push("strong close in daily range")
      if (metric.extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT) reasons.push("not too extended from 20d average")
      if (scoreDetails.highConvictionSetup) reasons.push("high-conviction setup")
      if (scoreDetails.eliteSetup) reasons.push("elite setup")

      let exclusionReason = ""
      if (!metric.passesPrice) {
        exclusionReason = `Below $${MIN_PRICE} minimum price`
      } else if (!metric.passesVolume) {
        exclusionReason = "Below minimum average volume"
      } else if (!metric.passesDollarVolume) {
        exclusionReason = "Below minimum dollar volume"
      } else if (!metric.passesMarketCap) {
        exclusionReason = "Below minimum market cap"
      } else if (!metric.aboveSma20) {
        exclusionReason = "Below 20-day moving average"
      } else if (metric.return20d <= 0) {
        exclusionReason = "Negative 20-day momentum"
      } else if (metric.relativeReturn20d <= 0) {
        exclusionReason = "Underperforming SPY over 20d"
      } else if (score < MIN_CANDIDATE_SCORE) {
        exclusionReason = "Score below candidate floor"
      } else {
        exclusionReason = "Did not pass candidate screen"
      }

      const row: CandidateUniverseRow = {
        company_id: metric.company.id,
        ticker: metric.ticker,
        cik: metric.company.cik,
        name: metric.company.name,
        is_active: metric.company.is_active ?? true,
        is_eligible: metric.company.is_eligible ?? null,
        has_insider_trades: metric.company.has_insider_trades ?? null,
        has_ptr_forms: metric.company.has_ptr_forms ?? null,
        has_clusters: metric.company.has_clusters ?? null,
        eligibility_reason: metric.company.eligibility_reason ?? null,
        price: round2(metric.latestClose),
        market_cap: metric.marketCap || null,
        pe_ratio: round2(metric.snapshot.peRatio),
        pe_forward: round2(metric.snapshot.forwardPe),
        pe_type: metric.snapshot.peType,
        sector: metric.snapshot.sector,
        industry: metric.snapshot.industry,
        business_description: metric.snapshot.businessDescription,
        avg_volume_20d: round2(metric.avgVolume20d),
        avg_dollar_volume_20d: round2(metric.avgDollarVolume20d),
        one_day_return: round2(metric.oneDayReturn),
        return_5d: round2(metric.return5d),
        return_10d: round2(metric.return10d),
        return_20d: round2(metric.return20d),
        relative_strength_20d: round2(metric.relative_strength_20d),
        volume_ratio: round2(metric.volumeRatio),
        breakout_20d: metric.breakout20d,
        breakout_10d: metric.breakout10d,
        above_sma_20: metric.aboveSma20,
        breakout_clearance_pct: round2(metric.breakoutClearancePct),
        extension_from_sma20_pct: round2(metric.extensionFromSma20Pct),
        close_in_day_range: round2(metric.closeInDayRange),
        catalyst_count: catalystCount,
        passes_price: metric.passesPrice,
        passes_volume: metric.passesVolume,
        passes_dollar_volume: metric.passesDollarVolume,
        passes_market_cap: metric.passesMarketCap,
        candidate_score: score,
        included: passed,
        passed,
        screen_reason: buildCandidateReason({
          prequalified: passed,
          strongBuyNow: highPriorityCandidate,
          reasons,
          exclusionReason,
          score,
        }),
        last_screened_at: nowIso,
        as_of_date: nowIso,
        updated_at: nowIso,
      }

      historyRows.push(makeHistoryRow(row, screenedOn, nowIso))
      currentRows.push(row)

      if (passed) passedInBatch += 1
      if (highPriorityCandidate) highPriorityInBatch += 1

      if (includeResults) {
        results.push({
          ticker: metric.ticker,
          ok: true,
          passed,
          highPriorityCandidate,
          score,
          rawScore: round2(scoreDetails.rawScore),
          tier: highPriorityCandidate
            ? "high_priority_candidate"
            : passed
              ? "passed"
              : "screened",
          reason: row.screen_reason,
          price: round2(metric.latestClose),
          oneDayReturn: round2(metric.oneDayReturn),
          return5d: round2(metric.return5d),
          return10d: round2(metric.return10d),
          return20d: round2(metric.return20d),
          relativeReturn5d: round2(metric.relativeReturn5d),
          relativeReturn10d: round2(metric.relativeReturn10d),
          relativeReturn20d: round2(metric.relativeReturn20d),
          benchmarkReturn5d: round2(benchmarkReturns.return5d),
          benchmarkReturn10d: round2(benchmarkReturns.return10d),
          benchmarkReturn20d: round2(benchmarkReturns.return20d),
          volumeRatio: round2(metric.volumeRatio),
          breakoutClearancePct: round2(metric.breakoutClearancePct),
          extensionFromSma20Pct: round2(metric.extensionFromSma20Pct),
          closeInDayRange: round2(metric.closeInDayRange),
          strongCompanyScore: round2(metric.strongCompanyScore),
          passesStrongCompanyGate: metric.passesStrongCompanyGate,
          catalystCount,
          priorityBonuses: {
            ptr: metric.ptrPriorityBonus,
            filings: metric.filingPriorityBonus,
            clusters: metric.clusterPriorityBonus,
          },
          scoreBreakdown: {
            quality: round2(scoreDetails.qualityScore),
            fundamental: round2(scoreDetails.fundamentalScore),
            momentum: round2(scoreDetails.momentumScore),
            relativeStrength: round2(scoreDetails.relativeStrengthScore),
            leadership: round2(scoreDetails.leadershipScore),
            volume: round2(scoreDetails.volumeScore),
            breakout: round2(scoreDetails.breakoutScore),
            trend: round2(scoreDetails.trendScore),
            evidence: round2(scoreDetails.evidenceScore),
            penalty: round2(scoreDetails.penaltyScore),
          },
        })
      }
    }

    const processedCount = companyList.length
    const transientErrorRate =
      processedCount > 0 ? transientYahooErrorsInBatch / processedCount : 0

    if (processedCount > 0 && transientErrorRate > MAX_TRANSIENT_ERROR_RATE) {
      return Response.json(
        {
          ok: false,
          error: "Aborting candidate batch because Yahoo transient error rate is too high",
          debug: {
            processedCount,
            transientYahooErrorsInBatch,
            transientErrorRate,
            batchStart: safeStart,
            batchSize: safeBatch,
          },
        },
        { status: 503 }
      )
    }

    const currentWriteResult = await upsertRowsInChunksDetailed(
      candidateUniverseTable,
      currentRows,
      "ticker"
    )

    if (currentWriteResult.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Candidate universe write failed",
          debug: {
            currentWriteErrors: currentWriteResult.errorCount,
            currentWriteErrorSamples: currentWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const historyWriteResult = await upsertRowsInChunksDetailed(
      candidateHistoryTable,
      historyRows,
      "snapshot_key"
    )

    if (historyWriteResult.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Candidate history write failed",
          debug: {
            historyWriteErrors: historyWriteResult.errorCount,
            historyWriteErrorSamples: historyWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    let retentionMessage = "skipped"
    if (runRetention) {
      const cutoffDate = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10)

      const { error: retentionError } = await candidateHistoryTable
        .delete()
        .lt("screened_on", cutoffDate)

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    let candidateCount: number | null = null
    let historyCount: number | null = null

    if (includeCounts) {
      const [
        { count: candidateCountValue, error: includedCountError },
        { count: historyCountValue, error: historyCountError },
      ] = await Promise.all([
        candidateUniverseTable.select("*", { count: "exact", head: true }),
        candidateHistoryTable.select("*", { count: "exact", head: true }),
      ])

      candidateCount = includedCountError ? null : candidateCountValue
      historyCount = historyCountError ? null : historyCountValue
    }

    const nextStart =
      totalCompanies !== null &&
      totalCompanies !== undefined &&
      to + 1 < totalCompanies
        ? to + 1
        : null

    return Response.json({
      ok: true,
      stage: "screening",
      targetTable: "candidate_universe",
      processedCompanies: companyList.length,
      totalCompanies: totalCountError ? null : totalCompanies,
      start: safeStart,
      batch: safeBatch,
      nextStart,
      onlyActive,
      universe,
      passedInBatch,
      highPriorityInBatch,
      failedInBatch,
      transientYahooErrorsInBatch,
      transientErrorRate: round2(transientErrorRate * 100),
      currentInserted: currentWriteResult.insertedOrUpdated,
      historyInserted: historyWriteResult.insertedOrUpdated,
      retentionCleanup: retentionMessage,
      retainedDays: RETENTION_DAYS,
      screenedOn,
      thresholds: {
        benchmarkTicker: BENCHMARK_TICKER,
        minCandidateScore: MIN_CANDIDATE_SCORE,
        minPrequalifiedScore: MIN_PREQUALIFIED_SCORE,
        minStrongBuyScore: MIN_STRONG_BUY_SCORE,
        minStrongCompanyScore: MIN_STRONG_COMPANY_SCORE,
        minPrice: MIN_PRICE,
        minAvgVolume20d: MIN_AVG_VOLUME_20D,
        minAvgDollarVolume20d: MIN_AVG_DOLLAR_VOLUME_20D,
        minMarketCap: MIN_MARKET_CAP,
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
        yahooRetryAttempts: YAHOO_RETRY_ATTEMPTS,
        maxTransientErrorRate: MAX_TRANSIENT_ERROR_RATE,
      },
      counts: includeCounts
        ? {
            candidateUniverse: candidateCount,
            history: historyCount,
          }
        : undefined,
      results: includeResults ? results : [],
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown error" },
      { status: 500 }
    )
  }
}