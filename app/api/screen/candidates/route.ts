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
  as_of_date: string
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
  pegRatio: number | null
  ma200: number | null
  beta: number | null
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

type CandidateMetricRow = {
  company: CompanyRow
  ticker: string
  latestClose: number
  marketCap: number
  avgVolume20d: number
  avgDollarVolume20d: number
  passesPrice: boolean
  passesVolume: boolean
  passesDollarVolume: boolean
  passesMarketCap: boolean
  snapshot: TickerSnapshot
}

type LTCSScoreInput = {
  grossMargin: number | null
  operatingMargin: number | null
  profitMargin: number | null
  roe: number | null
  debtToEquity: number | null
  currentRatio: number | null
  revenueGrowth: number | null
  earningsGrowth: number | null
  freeCashflow: number | null
  beta: number | null
  sector: string | null
  pegRatio: number | null
  forwardPE: number | null
  currentPrice: number | null
  ma200: number | null
  marketCap: number | null
}

type LTCSScoreOutput = {
  ltcsScore: number
  moatScore: number
  financialScore: number
  profitabilityScore: number
  stabilityScore: number
  valuationScore: number
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


const yahooFinance = new YahooFinance({
  queue: { concurrency: 2 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const MAX_BATCH = 150
const DEFAULT_BATCH = 100
const RETENTION_DAYS = 30

const MIN_PRICE = 15
const MIN_AVG_VOLUME_20D = 1_000_000
const MIN_AVG_DOLLAR_VOLUME_20D = 35_000_000
const MIN_MARKET_CAP = 5_000_000_000

const LTCS_INCLUDED_THRESHOLD = 50
const DEFENSIVE_SECTORS = ["Healthcare", "Consumer Staples", "Utilities", "Consumer Defensive", "Health Care"]

const TICKER_CONCURRENCY = 2
const DB_CHUNK_SIZE = 250

const YAHOO_RETRY_ATTEMPTS = 2
const YAHOO_RETRY_BASE_DELAY_MS = 800
const MAX_TRANSIENT_ERROR_RATE = 0.9
const MIN_TRANSIENT_ERRORS_TO_ABORT = 20

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


function buildCandidateReason(params: {
  prequalified: boolean
  strongBuyNow: boolean
  reasons: string[]
  exclusionReason?: string
  score?: number
}) {
  if (params.strongBuyNow) {
    return `High-conviction setup (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.prequalified) {
    return `Qualified setup (${params.score ?? 0}): ${params.reasons.join(", ")}`
  }

  if (params.exclusionReason) return params.exclusionReason

  return params.reasons.length
    ? `Not qualified: ${params.reasons.join(", ")}`
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
      pegRatio: null,
      ma200: null,
      beta: null,
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
      pegRatio: safeNumber((summary?.defaultKeyStatistics as any)?.pegRatio),
      ma200: safeNumber((summary?.summaryDetail as any)?.twoHundredDayAverage),
      beta: safeNumber((summary?.summaryDetail as any)?.beta) ?? safeNumber((summary?.defaultKeyStatistics as any)?.beta),
    },
  }
}

const FMP_API_KEY = process.env.FMP_API_KEY || ""
const FMP_BASE_URL = "https://financialmodelingprep.com/stable"

async function fmpFetch(endpoint: string, ticker: string): Promise<any> {
  const url = `${FMP_BASE_URL}/${endpoint}?symbol=${encodeURIComponent(ticker)}&apikey=${FMP_API_KEY}`
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 12_000)
  try {
    const res = await fetch(url, { cache: "no-store", signal: controller.signal })
    if (!res.ok) throw new Error(`FMP ${endpoint} ${res.status}`)
    const data = await res.json()
    return Array.isArray(data) ? data[0] ?? null : data
  } finally {
    clearTimeout(timeout)
  }
}

async function getTickerData(ticker: string) {
  // Try FMP first, fall back to Yahoo if FMP key missing
  if (FMP_API_KEY) {
    const [profile, ratios, metrics] = await Promise.all([
      fmpFetch("profile", ticker),
      fmpFetch("ratios-ttm", ticker),
      fmpFetch("key-metrics-ttm", ticker),
    ])

    if (!profile) throw new Error(`FMP: no profile data for ${ticker}`)

    const price = safeNumber(profile.price)
    const marketCap = safeNumber(profile.marketCap)
    const beta = safeNumber(profile.beta)

    const peRatio = safeNumber(ratios?.priceToEarningsRatioTTM)
    const forwardPeRaw = safeNumber(ratios?.forwardPriceToEarningsGrowthRatioTTM)
    const pegRatio = safeNumber(ratios?.priceToEarningsGrowthRatioTTM)
    const forwardPeg = safeNumber(ratios?.forwardPriceToEarningsGrowthRatioTTM)

    // FMP returns margins as decimals (0.50 = 50%)
    const grossMargin = safeNumber(ratios?.grossProfitMarginTTM)
    const operatingMargin = safeNumber(ratios?.operatingProfitMarginTTM)
    const profitMargin = safeNumber(ratios?.netProfitMarginTTM)

    const debtToEquity = safeNumber(ratios?.debtToEquityRatioTTM)
    const currentRatio = safeNumber(ratios?.currentRatioTTM) ?? safeNumber(metrics?.currentRatioTTM)

    const roe = safeNumber(metrics?.returnOnEquityTTM)

    // freeCashFlowToEquityTTM from key-metrics, or derive from operatingCashFlow
    const fcf = safeNumber(metrics?.freeCashFlowToEquityTTM)

    const snapshot: TickerSnapshot = {
      peRatio: peRatio !== null && peRatio > 0 ? peRatio : null,
      forwardPe: forwardPeg !== null && forwardPeg > 0 ? forwardPeg : null,
      peType: peRatio !== null && peRatio > 0 ? "trailing" : forwardPeg !== null ? "forward" : null,
      sector: profile.sector?.trim() ?? null,
      industry: profile.industry?.trim() ?? null,
      businessDescription: profile.description?.trim() ?? null,
      companyProfile: {
        profitMargin,
        operatingMargin,
        grossMargin,
        returnOnEquity: roe,
        debtToEquity: debtToEquity !== null ? debtToEquity * 100 : null, // FMP returns as ratio, LTCS expects percentage
        currentRatio,
        revenueGrowth: null, // not available in TTM ratios
        earningsGrowth: null, // not available in TTM ratios
        freeCashflow: fcf,
        operatingCashflow: null,
        recommendationKey: null,
        pegRatio: pegRatio !== null && pegRatio > 0 ? pegRatio : forwardPeg,
        ma200: null, // not available from FMP profile/ratios
        beta,
      },
    }

    return {
      quote: {
        regularMarketPrice: price,
        marketCap,
        averageDailyVolume3Month: safeNumber(profile.averageVolume),
      },
      snapshot,
    }
  }

  // Fallback to Yahoo Finance
  return await withYahooRetry(async () => {
    const [quote, summary] = await Promise.all([
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
      quote,
      snapshot: buildTickerSnapshot(summary, quote),
    }
  })
}

function calculateLTCS(input: LTCSScoreInput): LTCSScoreOutput {
  // Moat: gross margin, operating margin, revenue growth, market cap (max 100)
  let moat = 0
  if ((input.grossMargin ?? 0) > 0.40) moat += 25
  if ((input.operatingMargin ?? 0) > 0.12) moat += 25
  if ((input.revenueGrowth ?? 0) > 0.05) moat += 25
  if ((input.marketCap ?? 0) > 10_000_000_000) moat += 25

  // Financial Health: debt/equity, current ratio, profit margin (max 100)
  // Note: Yahoo Finance reports debtToEquity as percentage × 1 (e.g. 50 = 0.5×)
  let financial = 0
  const sector = (input.sector || "").toLowerCase()
  const isFinancialSector = sector.includes("financial") || sector.includes("real estate") || sector.includes("insurance")
  if (isFinancialSector || (input.debtToEquity ?? 999) < 100) financial += 40
  if ((input.currentRatio ?? 0) > 1.5 || isFinancialSector) financial += 30
  if ((input.profitMargin ?? -1) > 0) financial += 30

  // Profitability: ROE, free cash flow, earnings growth (max 100)
  let profitability = 0
  if ((input.roe ?? 0) > 0.15) profitability += 40
  if ((input.freeCashflow ?? -1) > 0) profitability += 35
  if ((input.earningsGrowth ?? 0) > 0.05) profitability += 25

  // Stability: beta and defensive sector (max 100)
  let stability = 0
  const beta = input.beta ?? 999
  if (beta < 1.5) stability += 40
  if (beta < 1.0) stability += 20
  const sectorStr = input.sector ?? ""
  if (DEFENSIVE_SECTORS.some((s) => sectorStr.toLowerCase().includes(s.toLowerCase()))) stability += 40

  // Valuation: PEG, forward PE, price vs 200-day MA (max 100)
  let valuation = 0
  const peg = input.pegRatio
  const fpe = input.forwardPE
  const price = input.currentPrice
  const ma200 = input.ma200
  if (peg !== null && peg > 0 && peg < 2) valuation += 35
  if (fpe !== null && fpe > 0 && fpe < 30) valuation += 35
  if (price !== null && ma200 !== null && ma200 > 0 && price < ma200) valuation += 30

  const ltcsScore = Math.round(
    moat * 0.25 +
    financial * 0.25 +
    profitability * 0.20 +
    stability * 0.10 +
    valuation * 0.20
  )

  return {
    ltcsScore: clamp(ltcsScore, 0, 100),
    moatScore: moat,
    financialScore: financial,
    profitabilityScore: profitability,
    stabilityScore: stability,
    valuationScore: valuation,
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
  companyId?: number | null
  ticker: string
  cik: string
  name: string | null
  isActive?: boolean | null
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
  screenedOn: string
}): CandidateUniverseRow {
  return {
    company_id: params.companyId ?? null,
    ticker: params.ticker,
    cik: params.cik,
    name: params.name,
    is_active: params.isActive ?? true,
    is_eligible: false,
    has_insider_trades: params.hasInsiderTrades ?? false,
    has_ptr_forms: params.hasPtrForms ?? false,
    has_clusters: params.hasClusters ?? false,
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
    as_of_date: params.screenedOn,
    screen_reason: params.screenReason,
    last_screened_at: params.nowIso,
    updated_at: params.nowIso,
  }
}

async function prepareTickerForScoring(
  company: CompanyRow,
  nowIso: string,
  screenedOn: string,
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
      hasInsiderTrades: company.has_insider_trades,
      hasPtrForms: company.has_ptr_forms,
      hasClusters: company.has_clusters,
      eligibilityReason: company.eligibility_reason,
      screenReason: "Excluded likely non-common-share ticker",
      nowIso,
      screenedOn,
    })

    return {
      kind: "final_row",
      row,
      result: includeResults
        ? {
            ticker,
            ok: true,
            included: false,
            passed: false,
            prequalified: false,
            score: 0,
            tier: "excluded",
            reason: row.screen_reason,
          }
        : undefined,
    }
  }

  let quote: any = null
  let snapshot: TickerSnapshot = emptySnapshot()

  try {
    const tickerData = await getTickerData(ticker)
    quote = tickerData.quote
    snapshot = tickerData.snapshot
  } catch (err: any) {
    // FMP errors should be permanent (no rate limit), not transient
    const isFmpError = String(err?.message || "").startsWith("FMP")
    const disposition = isFmpError
      ? { kind: "permanent" as const, reason: String(err?.message || "FMP error") }
      : classifyYahooError(err)

    const historyRow = makeExcludedRow({
      companyId: company.id,
      ticker,
      cik: company.cik,
      name: company.name,
      isActive: company.is_active,
      hasInsiderTrades: company.has_insider_trades,
      hasPtrForms: company.has_ptr_forms,
      hasClusters: company.has_clusters,
      eligibilityReason: company.eligibility_reason,
      screenReason:
        disposition.kind === "permanent"
          ? `Permanent Yahoo error: ${disposition.reason}`
          : `Transient Yahoo error: ${disposition.reason}`,
      nowIso,
      screenedOn,
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

  const latestClose = safeNumber((quote as any)?.regularMarketPrice) ?? 0
  const marketCap = safeNumber((quote as any)?.marketCap) ?? 0
  const avgVolume20d = safeNumber((quote as any)?.averageDailyVolume3Month) ?? safeNumber((quote as any)?.averageDailyVolume10Day) ?? 0
  const avgDollarVolume20d = latestClose * avgVolume20d

  const passesPrice = latestClose >= MIN_PRICE
  const passesVolume = avgVolume20d >= MIN_AVG_VOLUME_20D
  const passesDollarVolume = avgDollarVolume20d >= MIN_AVG_DOLLAR_VOLUME_20D
  const passesMarketCap = marketCap >= MIN_MARKET_CAP

  return {
    kind: "metric",
    metric: {
      company,
      ticker,
      latestClose,
      marketCap,
      avgVolume20d,
      avgDollarVolume20d,
      passesPrice,
      passesVolume,
      passesDollarVolume,
      passesMarketCap,
      snapshot,
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
            .select("id, ticker, cik, name, is_active")
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

    // Skip tickers already screened today to avoid exhausting Yahoo Finance rate limits.
    // They remain in candidate_screen_history from the earlier run and will be picked
    // up by the eligible_universe stage normally.
    const tickersInBatch = companyList.map((c) => c.ticker)
    const { data: alreadyScreened } = await supabase
      .from("candidate_screen_history")
      .select("ticker")
      .in("ticker", tickersInBatch)
      .eq("screened_on", screenedOn)
    const alreadyScreenedSet = new Set((alreadyScreened || []).map((r: any) => r.ticker))
    const tickersNeedingYahoo = companyList.filter((c) => !alreadyScreenedSet.has(c.ticker))
    const skippedCount = companyList.length - tickersNeedingYahoo.length

    const preparation = await mapWithConcurrency(
      tickersNeedingYahoo,
      TICKER_CONCURRENCY,
      async (company) =>
        prepareTickerForScoring(company, nowIso, screenedOn, includeResults)
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

    for (const metric of metricRows) {
      const ltcsResult = calculateLTCS({
        grossMargin: metric.snapshot.companyProfile.grossMargin,
        operatingMargin: metric.snapshot.companyProfile.operatingMargin,
        profitMargin: metric.snapshot.companyProfile.profitMargin,
        roe: metric.snapshot.companyProfile.returnOnEquity,
        debtToEquity: metric.snapshot.companyProfile.debtToEquity,
        currentRatio: metric.snapshot.companyProfile.currentRatio,
        revenueGrowth: metric.snapshot.companyProfile.revenueGrowth,
        earningsGrowth: metric.snapshot.companyProfile.earningsGrowth,
        freeCashflow: metric.snapshot.companyProfile.freeCashflow,
        beta: metric.snapshot.companyProfile.beta,
        sector: metric.snapshot.sector,
        pegRatio: metric.snapshot.companyProfile.pegRatio,
        forwardPE: metric.snapshot.forwardPe,
        currentPrice: metric.latestClose,
        ma200: metric.snapshot.companyProfile.ma200,
        marketCap: metric.marketCap,
      })

      const score = ltcsResult.ltcsScore
      const passed = score >= LTCS_INCLUDED_THRESHOLD

      const reasons: string[] = [
        `moat: ${ltcsResult.moatScore}/100`,
        `financial health: ${ltcsResult.financialScore}/100`,
        `profitability: ${ltcsResult.profitabilityScore}/100`,
        `stability: ${ltcsResult.stabilityScore}/100`,
        `valuation: ${ltcsResult.valuationScore}/100`,
      ]

      const exclusionReason = !metric.passesPrice
        ? `Below $${MIN_PRICE} minimum price`
        : !metric.passesMarketCap
          ? "Below minimum market cap ($5B)"
          : score < LTCS_INCLUDED_THRESHOLD
            ? `LTCS score ${score} below threshold ${LTCS_INCLUDED_THRESHOLD}`
            : "Did not qualify"

      const row: CandidateUniverseRow = {
        company_id: metric.company.id,
        ticker: metric.ticker,
        cik: metric.company.cik,
        name: metric.company.name,
        is_active: metric.company.is_active ?? true,
        is_eligible: passed,
        has_insider_trades: metric.company.has_insider_trades ?? false,
        has_ptr_forms: metric.company.has_ptr_forms ?? false,
        has_clusters: metric.company.has_clusters ?? false,
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
        passes_price: metric.passesPrice,
        passes_volume: metric.passesVolume,
        passes_dollar_volume: metric.passesDollarVolume,
        passes_market_cap: metric.passesMarketCap,
        candidate_score: score,
        included: passed,
        passed,
        as_of_date: screenedOn,
        screen_reason: passed
          ? `LTCS ${score}/100: ${reasons.join(", ")}`
          : `${exclusionReason} | ${reasons.join(", ")}`,
        last_screened_at: nowIso,
        updated_at: nowIso,
      }

      historyRows.push(makeHistoryRow(row, screenedOn, nowIso))

      if (passed) prequalifiedInBatch += 1

      if (includeResults) {
        results.push({
          ticker: metric.ticker,
          ok: true,
          included: passed,
          passed,
          score,
          tier: passed ? "ltcs_qualified" : "screened",
          reason: row.screen_reason,
          price: round2(metric.latestClose),
          marketCap: metric.marketCap,
          ltcsBreakdown: {
            moat: ltcsResult.moatScore,
            financialHealth: ltcsResult.financialScore,
            profitability: ltcsResult.profitabilityScore,
            stability: ltcsResult.stabilityScore,
            valuation: ltcsResult.valuationScore,
          },
        })
      }
    }

    const processedCount = tickersNeedingYahoo.length
    const transientErrorRate =
      processedCount > 0 ? transientYahooErrorsInBatch / processedCount : 0

    const severeYahooFailure =
      processedCount > 0 &&
      transientYahooErrorsInBatch >= MIN_TRANSIENT_ERRORS_TO_ABORT &&
      transientErrorRate > MAX_TRANSIENT_ERROR_RATE

    if (severeYahooFailure) {
      // Yahoo's daily rate limit is exhausted for this Vercel IP.
      // Rather than blocking the pipeline indefinitely, treat this as
      // "done screening for today" so eligible_universe → signals →
      // ticker_scores can run on whatever companies were screened so far.
      // The next daily cycle will pick up where we left off (skip-already-
      // screened prevents re-fetching tickers we've already processed).
      const sampleErrors = preparation
        .filter((item): item is Extract<typeof item, { kind: "error" }> => item.kind === "error" && item.errorKind === "transient_yahoo_error")
        .slice(0, 5)
        .map((item) => ({ ticker: item.ticker, error: item.error }))
      return Response.json({
        ok: true,
        yahooRateLimitReached: true,
        nextStart: null,  // advance pipeline to eligible_universe
        processedCompanies: companyList.length,
        totalCompanies: totalCountError ? null : totalCompanies,
        start: safeStart,
        batch: safeBatch,
        screenedOn,
        transientYahooErrorsInBatch,
        transientErrorRate: round2(transientErrorRate * 100),
        message: `Yahoo rate limit reached at batch start=${safeStart}. Advancing pipeline with partial screening data.`,
        debug: { sampleErrors },
      })
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
      skippedAlreadyScreened: skippedCount,
      totalCompanies: totalCountError ? null : totalCompanies,
      start: safeStart,
      batch: safeBatch,
      nextStart,
      onlyActive,
      universe,
      prequalifiedInBatch,
      failedInBatch,
      transientYahooErrorsInBatch,
      transientErrorRate: round2(transientErrorRate * 100),
      historyWriteErrors: 0,
      historyInserted: historyRows.length,
      retentionCleanup: retentionMessage,
      retainedDays: RETENTION_DAYS,
      screenedOn,
      thresholds: {
        ltcsIncludedThreshold: LTCS_INCLUDED_THRESHOLD,
        minPrice: MIN_PRICE,
        minAvgVolume20d: MIN_AVG_VOLUME_20D,
        minAvgDollarVolume20d: MIN_AVG_DOLLAR_VOLUME_20D,
        minMarketCap: MIN_MARKET_CAP,
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
    console.error("screen/candidates fatal error", {
      message: error?.message || "Unknown error",
      stack: error?.stack || null,
    })

    return Response.json(
      { ok: false, error: error.message || "Unknown error" },
      { status: 500 }
    )
  }
}