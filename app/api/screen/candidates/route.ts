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
  screen_reason: string
  last_screened_at: string
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
  penaltyScore: number
  catalystCount: number
  highConvictionSetup: boolean
  eliteSetup: boolean
}

const yahooFinance = new YahooFinance({
  queue: { concurrency: 2 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 250
const DEFAULT_BATCH = 200
const RETENTION_DAYS = 30

const BENCHMARK_TICKER = "SPY"

/**
 * Broad elite pool, not the final shortlist.
 */
const MIN_PRICE = 15
const MIN_AVG_VOLUME_20D = 1_000_000
const MIN_AVG_DOLLAR_VOLUME_20D = 35_000_000
const MIN_MARKET_CAP = 5_000_000_000

const MIN_STRONG_BUY_SCORE = 85
const MIN_STRONG_BUY_VOLUME_RATIO = 1.5
const MIN_STRONG_BUY_RETURN_10D = 5
const MIN_STRONG_BUY_RETURN_20D = 10
const MIN_RELATIVE_RETURN_10D = 2
const MIN_RELATIVE_RETURN_20D = 4
const MAX_STRONG_BUY_RETURN_20D = 28
const MAX_EXTENSION_FROM_SMA20_PCT = 12
const MIN_BREAKOUT_CLEARANCE_PCT = 0.35
const MIN_CLOSE_IN_DAY_RANGE = 0.65
const MIN_CATALYST_COUNT = 8

const MIN_STRONG_COMPANY_SCORE = 70

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
    return `Strong setup (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.prequalified) {
    return `Prequalified (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.exclusionReason) return params.exclusionReason

  return params.reasons.length
    ? `Not prequalified: ${params.reasons.join(", ")}`
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
    (peRatio !== null && peRatio > 0 && peRatio < 80) ||
    (forwardPe !== null && forwardPe > 0 && forwardPe < 60)

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
    score += 18
    reasons.push("mid/large-cap business")
  } else {
    failures.push("market cap below strong-company floor")
  }

  if (avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D) {
    score += 12
    reasons.push("strong liquidity")
  } else {
    failures.push("dollar volume too light")
  }

  if (positiveEarnings) {
    score += 16
    reasons.push("positive earnings profile")
  } else {
    failures.push("no reliable positive earnings profile")
  }

  if (positiveFreeCashFlow) {
    score += 14
    reasons.push("positive free cash flow")
  } else if (positiveOperatingCashFlow) {
    score += 8
    reasons.push("positive operating cash flow")
  } else {
    failures.push("cash flow not strong enough")
  }

  if (profitMargin !== null) {
    if (profitMargin >= 8) {
      score += 12
      reasons.push("healthy profit margin")
    } else if (profitMargin >= 0) {
      score += 6
      reasons.push("profitable")
    } else {
      failures.push("negative profit margin")
    }
  }

  if (operatingMargin !== null) {
    if (operatingMargin >= 10) {
      score += 10
      reasons.push("healthy operating margin")
    } else if (operatingMargin >= 0) {
      score += 5
      reasons.push("positive operating margin")
    } else {
      failures.push("negative operating margin")
    }
  }

  if (grossMargin !== null && grossMargin >= 35) {
    score += 5
    reasons.push("solid gross margin")
  }

  if (roe !== null && roe >= 12) {
    score += 7
    reasons.push("strong return on equity")
  }

  if (revenueGrowth !== null) {
    if (revenueGrowth >= 8) {
      score += 6
      reasons.push("solid revenue growth")
    } else if (revenueGrowth < -5) {
      failures.push("revenue shrinking")
    }
  }

  if (earningsGrowth !== null) {
    if (earningsGrowth >= 8) {
      score += 6
      reasons.push("solid earnings growth")
    } else if (earningsGrowth < -10) {
      failures.push("earnings shrinking")
    }
  }

  if (isDebtMetricApplicable(sector)) {
    if (companyProfile.debtToEquity !== null) {
      if (companyProfile.debtToEquity <= 120) {
        score += 8
        reasons.push("manageable leverage")
      } else if (companyProfile.debtToEquity > 220) {
        failures.push("leverage too high")
      }
    }
  }

  if (isLiquidityMetricApplicable(sector)) {
    if (companyProfile.currentRatio !== null) {
      if (companyProfile.currentRatio >= 1.1) {
        score += 4
        reasons.push("healthy short-term liquidity")
      } else if (companyProfile.currentRatio < 0.85) {
        failures.push("weak current ratio")
      }
    }
  }

  const industryText = `${sector || ""} ${industry || ""}`.toLowerCase()

  if (industryText.includes("biotechnology")) {
    if (!positiveEarnings && !positiveFreeCashFlow) {
      speculativePenalty -= 25
      failures.push("speculative biotech profile")
    }
  }

  if (return20d > 28 && volumeRatio > 3.5 && marketCap < 10_000_000_000) {
    speculativePenalty -= 15
    failures.push("too extended / momentum spike")
  }

  if (!positiveEarnings && !positiveFreeCashFlow) {
    speculativePenalty -= 20
  }

  const passes =
    score >= MIN_STRONG_COMPANY_SCORE &&
    marketCap >= MIN_MARKET_CAP &&
    avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D &&
    positiveEarnings &&
    (positiveFreeCashFlow || positiveOperatingCashFlow) &&
    (profitMargin === null || profitMargin >= 0) &&
    (operatingMargin === null || operatingMargin >= 0) &&
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
  } = input

  const qualityScore =
    12 *
    (
      0.1 * (passesPrice ? 1 : 0) +
      0.15 * (passesVolume ? 1 : 0) +
      0.25 * (passesDollarVolume ? 1 : 0) +
      0.25 * scaleBetween(avgDollarVolume20d, MIN_AVG_DOLLAR_VOLUME_20D, 100_000_000) +
      0.25 * scaleBetween(marketCap, MIN_MARKET_CAP, 40_000_000_000)
    )

  const fundamentalScore =
    24 *
    (
      0.4 * scaleBetween(strongCompanyScore, 55, 100) +
      0.6 * (passesStrongCompanyGate ? 1 : 0)
    )

  const momentumScore =
    14 *
    (
      0.08 * scaleBetween(oneDayReturn, -1, 3) +
      0.16 * scaleBetween(return5d, 0, 8) +
      0.3 * scaleBetween(return10d, 2, 14) +
      0.46 * scaleBetween(return20d, 6, 22)
    )

  const relativeStrengthScore =
    18 *
    (
      0.14 * scaleBetween(relativeReturn5d, 0, 5) +
      0.34 * scaleBetween(relativeReturn10d, 1, 10) +
      0.52 * scaleBetween(relativeReturn20d, 3, 16)
    )

  const leadershipScore =
    8 *
    (
      0.15 * scaleBetween(relativeReturn5d, 1, 4) +
      0.35 * scaleBetween(relativeReturn10d, 2, 8) +
      0.5 * scaleBetween(relativeReturn20d, 4, 12)
    )

  const volumeScore =
    8 *
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

  const breakoutScore = 8 * clamp(breakoutQuality, 0, 1)

  const smaSpreadPct = sma20 > 0 ? ((sma10 - sma20) / sma20) * 100 : 0

  const trendScore =
    8 *
    (
      0.36 * (aboveSma20 ? 1 : 0) +
      0.24 * (shortTermTrendUp ? 1 : 0) +
      0.16 * scaleBetween(smaSpreadPct, 0.2, 3) +
      0.24 * scaleBetween(-extensionFromSma20Pct, -MAX_EXTENSION_FROM_SMA20_PCT, 0)
    )

  let penaltyScore = 0

  if (!passesStrongCompanyGate) penaltyScore -= 16
  if (strongCompanyScore < MIN_STRONG_COMPANY_SCORE) penaltyScore -= 8
  if (!passesPrice) penaltyScore -= 6
  if (!passesVolume) penaltyScore -= 5
  if (!passesDollarVolume) penaltyScore -= 7
  if (!passesMarketCap) penaltyScore -= 6
  if (!aboveSma20) penaltyScore -= 6
  if (!shortTermTrendUp) penaltyScore -= 4
  if (oneDayReturn < -2) penaltyScore -= 3
  if (return5d < 0) penaltyScore -= 4
  if (return10d < 2) penaltyScore -= 5
  if (return20d < 5) penaltyScore -= 7
  if (relativeReturn5d < 0) penaltyScore -= 3
  if (relativeReturn10d < 1) penaltyScore -= 5
  if (relativeReturn20d < 3) penaltyScore -= 8
  if (volumeRatio < 1.0) penaltyScore -= 4
  if (!nearHigh20) penaltyScore -= 3
  if (!breakout10d && !breakout20d) penaltyScore -= 5
  if (closeInDayRange < 0.5) penaltyScore -= 4
  if (extensionFromSma20Pct > MAX_EXTENSION_FROM_SMA20_PCT) penaltyScore -= 10
  if (extensionFromSma20Pct > 15) penaltyScore -= 8
  if (return20d > MAX_STRONG_BUY_RETURN_20D) penaltyScore -= 8
  if (return20d > 35) penaltyScore -= 10

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
    penaltyScore

  const normalized = clamp((rawScore + 30) / 100, 0, 1)
  let candidateScore = Math.round(Math.pow(normalized, 1.12) * 100)

  const catalystCount = [
    passesStrongCompanyGate,
    strongCompanyScore >= MIN_STRONG_COMPANY_SCORE,
    oneDayReturn > 0,
    return5d >= 2,
    return10d >= MIN_STRONG_BUY_RETURN_10D,
    return20d >= MIN_STRONG_BUY_RETURN_20D,
    return20d <= MAX_STRONG_BUY_RETURN_20D,
    relativeReturn5d > 0,
    relativeReturn10d >= MIN_RELATIVE_RETURN_10D,
    relativeReturn20d >= MIN_RELATIVE_RETURN_20D,
    volumeRatio >= 1.2,
    volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO,
    breakout20d,
    breakout10d,
    nearHigh20,
    aboveSma20,
    shortTermTrendUp,
    breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT,
    closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE,
    extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT,
    avgDollarVolume20d >= 50_000_000,
    marketCap >= 10_000_000_000,
  ].filter(Boolean).length

  const highConvictionSetup =
    passesStrongCompanyGate &&
    passesPrice &&
    passesVolume &&
    passesDollarVolume &&
    passesMarketCap &&
    breakout20d &&
    aboveSma20 &&
    shortTermTrendUp &&
    nearHigh20 &&
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
    strongCompanyScore >= 80 &&
    volumeRatio >= 1.9 &&
    return5d >= 3 &&
    return10d >= 7 &&
    return20d >= 12 &&
    relativeReturn10d >= 3 &&
    relativeReturn20d >= 6 &&
    avgDollarVolume20d >= 55_000_000 &&
    marketCap >= 10_000_000_000 &&
    catalystCount >= MIN_CATALYST_COUNT + 1

  if (!passesStrongCompanyGate) {
    candidateScore = Math.min(candidateScore, 68)
  } else if (!aboveSma20 || !shortTermTrendUp) {
    candidateScore = Math.min(candidateScore, 74)
  } else if (!highConvictionSetup) {
    candidateScore = Math.min(candidateScore, 88)
  } else if (!eliteSetup) {
    candidateScore = Math.min(candidateScore, 95)
  } else {
    candidateScore = Math.min(candidateScore, 99)
  }

  const perfectInstitutionalSetup =
    eliteSetup &&
    strongCompanyScore >= 90 &&
    passesStrongCompanyGate &&
    volumeRatio >= 2.4 &&
    return5d >= 4.5 &&
    return10d >= 11 &&
    return20d >= 16 &&
    return20d <= 24 &&
    relativeReturn5d >= 2.5 &&
    relativeReturn10d >= 5.5 &&
    relativeReturn20d >= 9 &&
    avgDollarVolume20d >= 80_000_000 &&
    marketCap >= 20_000_000_000 &&
    breakout20d &&
    nearHigh20 &&
    breakoutClearancePct >= 0.8 &&
    closeInDayRange >= 0.82 &&
    extensionFromSma20Pct >= 1 &&
    extensionFromSma20Pct <= 9 &&
    catalystCount >= 11

  if (perfectInstitutionalSetup) {
    candidateScore = 100
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
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict })
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    }
  }

  return {
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
  ticker: string
  cik: string
  name: string | null
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
    ticker: params.ticker,
    cik: params.cik,
    name: params.name,
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
    screen_reason: params.screenReason,
    last_screened_at: params.nowIso,
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
      ticker,
      cik: company.cik,
      name: company.name,
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
            included: false,
            prequalified: false,
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
      ticker,
      cik: company.cik,
      name: company.name,
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
      ticker,
      cik: company.cik,
      name: company.name,
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
            included: false,
            prequalified: false,
            score: 0,
            tier: "not_included",
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

    const companiesTable = supabase.from("companies") as any
    const candidateHistoryTable = supabase.from("candidate_screen_history") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any

    let companyQuery = companiesTable
      .select("id, ticker, cik, name, is_active")
      .not("cik", "is", null)
      .order("id", { ascending: true })
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
      return Response.json({ ok: false, error: companiesError.message }, { status: 500 })
    }

    const companyList = (companies || []) as CompanyRow[]

    const preparation = await mapWithConcurrency(
      companyList,
      TICKER_CONCURRENCY,
      async (company) =>
        prepareTickerForScoring(company, benchmarkReturns, nowIso, includeResults)
    )

    const results: Array<Record<string, any>> = []
    const metricRows: CandidateMetricRow[] = []
    const historyRows: CandidateHistoryRow[] = []
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
        }

        if (item.result && includeResults) {
          results.push(item.result)
        }

        continue
      }

      historyRows.push(makeHistoryRow(item.row, screenedOn, nowIso))
      if (item.result && includeResults) {
        results.push(item.result)
      }
    }

    let prequalifiedInBatch = 0
    let strongBuyNowInBatch = 0

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
      })

      const score = scoreDetails.candidateScore
      const catalystCount = scoreDetails.catalystCount

      const strongBuyNowCandidate =
        metric.passesStrongCompanyGate &&
        metric.strongCompanyScore >= 78 &&
        metric.passesPrice &&
        metric.passesVolume &&
        metric.passesDollarVolume &&
        metric.passesMarketCap &&
        metric.breakout20d &&
        metric.nearHigh20 &&
        metric.aboveSma20 &&
        metric.shortTermTrendUp &&
        metric.oneDayReturn >= -0.25 &&
        metric.return10d >= MIN_STRONG_BUY_RETURN_10D &&
        metric.return20d >= MIN_STRONG_BUY_RETURN_20D &&
        metric.return20d <= MAX_STRONG_BUY_RETURN_20D &&
        metric.relativeReturn10d >= MIN_RELATIVE_RETURN_10D &&
        metric.relativeReturn20d >= MIN_RELATIVE_RETURN_20D &&
        metric.volumeRatio >= MIN_STRONG_BUY_VOLUME_RATIO &&
        metric.breakoutClearancePct >= MIN_BREAKOUT_CLEARANCE_PCT &&
        metric.closeInDayRange >= MIN_CLOSE_IN_DAY_RANGE &&
        metric.extensionFromSma20Pct <= MAX_EXTENSION_FROM_SMA20_PCT &&
        score >= MIN_STRONG_BUY_SCORE &&
        catalystCount >= MIN_CATALYST_COUNT

      const prequalified =
        metric.passesStrongCompanyGate &&
        metric.strongCompanyScore >= MIN_STRONG_COMPANY_SCORE &&
        metric.passesPrice &&
        metric.passesVolume &&
        metric.passesDollarVolume &&
        metric.passesMarketCap &&
        metric.aboveSma20 &&
        metric.shortTermTrendUp &&
        metric.nearHigh20 &&
        metric.return10d >= 3 &&
        metric.return20d >= 6 &&
        metric.relativeReturn10d >= 1 &&
        metric.relativeReturn20d >= 3 &&
        metric.volumeRatio >= 1.1 &&
        metric.breakoutClearancePct >= 0.15 &&
        metric.closeInDayRange >= 0.55 &&
        metric.extensionFromSma20Pct <= 14 &&
        score >= 70

      const reasons: string[] = []

      if (metric.passesStrongCompanyGate) reasons.push("strong underlying company")
      if (metric.strongCompanyScore >= 75) reasons.push("high fundamental quality")
      reasons.push(...metric.strongCompanyReasons)

      if (metric.passesPrice) reasons.push(`price >= $${MIN_PRICE}`)
      if (metric.passesVolume) reasons.push("20d avg volume")
      if (metric.passesDollarVolume) reasons.push("20d dollar volume")
      if (metric.passesMarketCap) reasons.push("market cap")
      if (metric.oneDayReturn > 0) reasons.push("positive day")
      if (metric.return5d >= 2) reasons.push("5d momentum")
      if (metric.return10d >= MIN_STRONG_BUY_RETURN_10D) reasons.push("10d momentum")
      if (metric.return20d >= MIN_STRONG_BUY_RETURN_20D) reasons.push("20d momentum")
      if (metric.relativeReturn5d > 0) reasons.push("beats SPY over 5d")
      if (metric.relativeReturn10d >= MIN_RELATIVE_RETURN_10D) reasons.push("beats SPY over 10d")
      if (metric.relativeReturn20d >= MIN_RELATIVE_RETURN_20D) reasons.push("beats SPY over 20d")
      if (metric.volumeRatio >= 1.25) reasons.push("volume expansion")
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
      } else if (score < 70) {
        exclusionReason = "Score below screening floor"
      } else {
        exclusionReason = "Did not prequalify"
      }

      const row: CandidateUniverseRow = {
        ticker: metric.ticker,
        cik: metric.company.cik,
        name: metric.company.name,
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
        included: false,
        screen_reason: buildCandidateReason({
          prequalified,
          strongBuyNow: strongBuyNowCandidate,
          reasons,
          exclusionReason,
          score,
        }),
        last_screened_at: nowIso,
        updated_at: nowIso,
      }

      historyRows.push(makeHistoryRow(row, screenedOn, nowIso))

      if (prequalified) prequalifiedInBatch += 1
      if (strongBuyNowCandidate) strongBuyNowInBatch += 1

      if (includeResults) {
        results.push({
          ticker: metric.ticker,
          ok: true,
          included: false,
          prequalified,
          strongBuyNowCandidate,
          score,
          rawScore: round2(scoreDetails.rawScore),
          tier: strongBuyNowCandidate
            ? "strong_buy_now"
            : prequalified
              ? "prequalified"
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
          scoreBreakdown: {
            quality: round2(scoreDetails.qualityScore),
            fundamental: round2(scoreDetails.fundamentalScore),
            momentum: round2(scoreDetails.momentumScore),
            relativeStrength: round2(scoreDetails.relativeStrengthScore),
            leadership: round2(scoreDetails.leadershipScore),
            volume: round2(scoreDetails.volumeScore),
            breakout: round2(scoreDetails.breakoutScore),
            trend: round2(scoreDetails.trendScore),
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

    const historyWriteResult = await upsertRowsInChunksDetailed(
      candidateHistoryTable,
      historyRows,
      "snapshot_key"
    )

    if (historyWriteResult.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Candidate screening had database write failures",
          debug: {
            historyWriteErrors: historyWriteResult.errorCount,
            historyWriteErrorSamples: historyWriteResult.errors.slice(0, 5),
            batchStart: safeStart,
            batchSize: safeBatch,
            processedCompanies: companyList.length,
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
      processedCompanies: companyList.length,
      totalCompanies: totalCountError ? null : totalCompanies,
      start: safeStart,
      batch: safeBatch,
      nextStart,
      onlyActive,
      prequalifiedInBatch,
      strongBuyNowInBatch,
      failedInBatch,
      transientYahooErrorsInBatch,
      transientErrorRate: round2(transientErrorRate * 100),
      historyWriteErrors: 0,
      historyInserted: historyRows.length,
      retentionCleanup: retentionMessage,
      retainedDays: RETENTION_DAYS,
      screenedOn,
      thresholds: {
        benchmarkTicker: BENCHMARK_TICKER,
        minStrongCompanyScore: MIN_STRONG_COMPANY_SCORE,
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
        yahooRetryAttempts: YAHOO_RETRY_ATTEMPTS,
        maxTransientErrorRate: MAX_TRANSIENT_ERROR_RATE,
      },
      results: includeResults ? results : [],
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown error" },
      { status: 500 }
    )
  }
}