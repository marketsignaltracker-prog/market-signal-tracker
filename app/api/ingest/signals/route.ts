import { createClient } from "@supabase/supabase-js"
import YahooFinance from "yahoo-finance2"
import { XMLParser } from "fast-xml-parser"

type RawFiling = {
  ticker: string
  company_name: string | null
  form_type: string | null
  filed_at: string | null
  filing_url: string | null
  accession_no: string
  cik?: string | null
  primary_doc?: string | null
  fetched_at?: string | null
}

type CandidateUniverseSignalInput = {
  ticker: string
  cik: string | null
  name: string | null
  price: number | null
  market_cap: number | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return?: number | null
  return_5d: number | null
  return_10d?: number | null
  return_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean | null
  breakout_10d?: boolean | null
  above_sma_20: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  candidate_score: number | null
  included: boolean | null
  screen_reason: string | null
  last_screened_at: string | null
}

type InsiderParseResult = {
  action: "Buy" | "Sell" | "Mixed" | "Other" | null
  shares: number | null
  avgPrice: number | null
  role: string | null
  insiderName: string | null
}

type PriceConfirmation = {
  return5d: number | null
  return20d: number | null
  volumeRatio: number | null
  breakout20d: boolean
  breakout52w: boolean
  relativeStrength20d: number | null
  above50dma: boolean
  trendAligned: boolean
  confirmed: boolean
}

type TickerSnapshot = {
  peRatio: number | null
  forwardPe: number | null
  peType: "trailing" | "forward" | null
  psRatio: number | null
  marketCap: number | null
  sector: string | null
  industry: string | null
  businessDescription: string | null
  companyName: string | null
}

type EarningsSignal = {
  hasSignal: boolean
  surprisePct: number | null
  revenueGrowthPct: number | null
  guidanceFlag: boolean
  summary: string | null
}

type ClusterInfo = {
  clusterSize: number
  totalShares: number
  uniqueInsiders: number
  repeatBuyer: boolean
}

type SignalCategory =
  | "Insider Buying"
  | "Breakout"
  | "Earnings Breakout"
  | "Catalyst"
  | "Ownership"
  | "Multi-Signal Strong Buy"

type SignalSource =
  | "form4"
  | "13d"
  | "13g"
  | "8k"
  | "earnings"
  | "breakout"

type StrengthBucket = "Buy" | "Strong Buy" | "Elite Buy"
type ScoreBreakdown = Record<string, number>

type BaseSignalData = {
  signal_type: string
  signal_source: SignalSource
  bias: "Bullish"
  score: number
  title: string
  summary: string
}

type PreparedSignal = {
  filing: RawFiling
  base: BaseSignalData
  insider: InsiderParseResult
  price: PriceConfirmation
  snapshot: TickerSnapshot
  earnings: EarningsSignal
  catalystType: string | null
  candidate: CandidateUniverseSignalInput | null
}

type EnhancedSignal = {
  signal_type: string
  signal_source: SignalSource
  signal_category: SignalCategory
  signal_strength_bucket: StrengthBucket
  signal_tags: string[]
  bias: "Bullish"
  score: number
  app_score: number
  board_bucket: "Buy"
  title: string
  summary: string
  insiderBuyValue: number | null
  insiderSignalFlavor: string
  ageDays: number | null
  score_breakdown: ScoreBreakdown
  signal_reasons: string[]
  score_caps_applied: string[]
  stacked_signal_count: number
  freshness_bucket: string | null
}

type ParsedTransactionRow = {
  action: "Buy" | "Sell" | "Other"
  code: string | null
  shares: number | null
  price: number | null
  transactionDate: string | null
  order: number
}

type Diagnostics = {
  scanned: number
  skippedNoTicker: number
  skippedNoBaseSignal: number
  skippedNotBullishEnough: number
  skippedFailedEligibility: number
  preparedCount: number
  enhancedNull: number
  filteredByMinScore: number
  filingSignalsBuilt: number
  filingSignalsInserted: number
  tickerCurrentBuilt: number
  tickerCurrentInserted: number
  tickerHistoryInserted: number
  tickerCurrentBuiltFromSignalsTable: number
  unsupportedForms: Record<string, number>
  candidateRowsLoaded: number
  candidateTechnicalSignalsBuilt: number
  candidateTechnicalSignalsInserted: number
}

const yahooFinance = new YahooFinance({
  queue: { concurrency: 6 },
  suppressNotices: ["ripHistorical", "yahooSurvey"],
})

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  parseTagValue: true,
  trimValues: true,
})

const SEC_USER_AGENT =
  process.env.SEC_USER_AGENT ||
  "Market Signal Tracker marketsignaltracker@gmail.com"

const DEFAULT_LIMIT = 250
const MAX_LIMIT = 1000
const DEFAULT_LOOKBACK_DAYS = 14
const MAX_LOOKBACK_DAYS = 30
const RETENTION_DAYS = 30
const SCORE_VERSION = "v5"

const SEC_FETCH_TIMEOUT_MS = 8000
const TEXT_FETCH_TIMEOUT_MS = 8000
const YAHOO_TIMEOUT_MS = 9000
const CANDIDATE_SIGNAL_LOOKBACK_DAYS = 3

const MIN_SIGNAL_APP_SCORE = 80
const MIN_TICKER_APP_SCORE = 82
const MIN_CANDIDATE_SCORE_FOR_SYNTHETIC_SIGNAL = 90
const MAX_SIGNAL_AGE_DAYS = 14

const PREPARE_CONCURRENCY = 8
const CATALYST_FETCH_CONCURRENCY = 4
const DB_CHUNK_SIZE = 250

function normalizeFormType(formType: string | null) {
  const normalized = (formType || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/^FORM\s+/i, "")

  if (normalized === "8K") return "8-K"
  if (normalized === "6K") return "6-K"
  if (normalized === "4A" || normalized === "4 /A") return "4/A"
  if (normalized === "13DA" || normalized === "SCHEDULE 13D/A") return "13D/A"
  if (normalized === "13GA" || normalized === "SCHEDULE 13G/A") return "13G/A"
  if (normalized === "SCHEDULE 13D") return "13D"
  if (normalized === "SCHEDULE 13G") return "13G"
  if (normalized === "SC13D") return "SC 13D"
  if (normalized === "SC13D/A") return "SC 13D/A"
  if (normalized === "SC13G") return "SC 13G"
  if (normalized === "SC13G/A") return "SC 13G/A"
  if (normalized === "SC14D9" || normalized === "SC14D9C") return "SC 14D9"

  return normalized
}

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

function toArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return []
  return Array.isArray(value) ? value : [value]
}

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(String(value).replace(/,/g, ""))
  return Number.isFinite(n) ? n : null
}

function round2(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 100) / 100
}

function roundWhole(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value)
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function stripHtml(input: string) {
  return input
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#160;/gi, " ")
    .replace(/&#8217;/gi, "'")
    .replace(/\s+/g, " ")
    .trim()
}

function daysBetween(dateString: string | null) {
  if (!dateString) return null
  const filedAt = new Date(dateString).getTime()
  if (Number.isNaN(filedAt)) return null
  const now = Date.now()
  return Math.max(0, Math.floor((now - filedAt) / (24 * 60 * 60 * 1000)))
}

function freshnessBucketFromAge(ageDays: number | null) {
  if (ageDays === null) return null
  if (ageDays <= 1) return "today"
  if (ageDays <= 3) return "fresh"
  if (ageDays <= 7) return "recent"
  if (ageDays <= 14) return "aging"
  return "stale"
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function sum(nums: number[]) {
  return nums.reduce((a, b) => a + b, 0)
}

function average(nums: number[]) {
  if (!nums.length) return null
  return sum(nums) / nums.length
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function normalizeTransactionDate(value: unknown): string | null {
  const raw = String(value ?? "").trim()
  if (!raw) return null

  const matchIso = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (matchIso) return raw

  const matchUs = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/)
  if (matchUs) {
    const [, mm, dd, yyyy] = matchUs
    return `${yyyy}-${mm}-${dd}`
  }

  const parsed = new Date(raw)
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10)
  }

  return null
}

function addDays(isoDate: string, days: number) {
  const d = new Date(`${isoDate}T00:00:00.000Z`)
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}

async function withTimeout<T>(
  promiseFactory: () => Promise<T>,
  ms: number,
  fallback: T
): Promise<T> {
  return await Promise.race<T>([
    promiseFactory(),
    new Promise<T>((resolve) => setTimeout(() => resolve(fallback), ms)),
  ])
}

async function fetchWithTimeout(url: string, options: RequestInit, timeoutMs: number) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    })
  } finally {
    clearTimeout(timeout)
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
    { length: Math.min(Math.max(1, concurrency), Math.max(1, items.length)) },
    () => runner()
  )

  await Promise.all(runners)
  return results
}

async function upsertInChunks(
  table: any,
  rows: any[],
  onConflict: string
) {
  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict })
    if (error) {
      throw new Error(error.message)
    }
  }
}

async function deleteInChunksByTicker(table: any, tickers: string[]) {
  const unique = uniqueStrings(tickers)
  for (const chunk of chunkArray(unique, DB_CHUNK_SIZE)) {
    const { error } = await table.delete().in("ticker", chunk)
    if (error) {
      throw new Error(error.message)
    }
  }
}

function buildSignalKey(filing: RawFiling, source: SignalSource) {
  const ticker = normalizeTicker(filing.ticker)
  return `${source}:${filing.accession_no}:${ticker}`
}

function transactionCodeToAction(
  code: string | null,
  acquiredDisposed: string | null
): "Buy" | "Sell" | "Other" {
  const c = (code || "").trim().toUpperCase()
  const ad = (acquiredDisposed || "").trim().toUpperCase()

  if (c === "P") return "Buy"
  if (c === "S") return "Sell"
  if (ad === "A" && c === "") return "Buy"
  if (ad === "D" && c === "") return "Sell"

  return "Other"
}

function deriveInsiderRole(doc: any): string | null {
  const reportingOwner = doc?.reportingOwner ?? {}
  const relationship = reportingOwner?.reportingOwnerRelationship ?? {}
  const title = String(relationship?.officerTitle || "").trim()

  if (title) return title
  if (String(relationship?.isDirector || "").trim() === "1") return "Director"
  if (String(relationship?.isOfficer || "").trim() === "1") return "Officer"
  if (String(relationship?.isTenPercentOwner || "").trim() === "1") return "10% Owner"
  return null
}

function deriveInsiderName(doc: any): string | null {
  const reportingOwner = doc?.reportingOwner ?? {}
  const ownerId = reportingOwner?.reportingOwnerId ?? {}
  const name = String(ownerId?.rptOwnerName || "").trim()
  return name || null
}

function chooseMostRecentRelevantAction(
  rows: ParsedTransactionRow[],
  role: string | null,
  insiderName: string | null
): InsiderParseResult | null {
  if (!rows.length) return null

  const relevant = rows.filter((row) => row.action === "Buy" || row.action === "Sell")
  if (!relevant.length) {
    return {
      action: "Other",
      shares: null,
      avgPrice: null,
      role,
      insiderName,
    }
  }

  const rowsWithDate = relevant.filter((row) => row.transactionDate)
  let workingSet = relevant

  if (rowsWithDate.length > 0) {
    const latestDate = rowsWithDate.reduce((latest, current) => {
      return (current.transactionDate || "") > latest ? current.transactionDate || "" : latest
    }, "")
    workingSet = rowsWithDate.filter((row) => row.transactionDate === latestDate)
  }

  const lastRelevantRow = [...workingSet].sort((a, b) => a.order - b.order)[workingSet.length - 1]
  if (!lastRelevantRow) {
    return {
      action: "Other",
      shares: null,
      avgPrice: null,
      role,
      insiderName,
    }
  }

  const latestAction = lastRelevantRow.action
  const sameActionRows = workingSet.filter((row) => row.action === latestAction)

  const shares = sum(
    sameActionRows.map((row) => row.shares).filter((v): v is number => v !== null && v > 0)
  )

  const prices = sameActionRows
    .map((row) => row.price)
    .filter((v): v is number => v !== null && v > 0)

  const avgPrice = prices.length > 0 ? average(prices) : null

  return {
    action: latestAction,
    shares: shares > 0 ? shares : null,
    avgPrice,
    role,
    insiderName,
  }
}

function parseOwnershipDocument(doc: any): InsiderParseResult | null {
  if (!doc) return null

  const rows = [
    ...toArray(doc.nonDerivativeTable?.nonDerivativeTransaction),
    ...toArray(doc.derivativeTable?.derivativeTransaction),
  ]

  const parsedRows: ParsedTransactionRow[] = []
  const role = deriveInsiderRole(doc)
  const insiderName = deriveInsiderName(doc)

  rows.forEach((row, index) => {
    const coding = row?.transactionCoding || {}
    const amounts = row?.transactionAmounts || {}
    const dates = row?.transactionDate || {}

    const code = String(coding.transactionCode || "").trim().toUpperCase() || null
    const acquiredDisposed = (
      String(
        amounts.transactionAcquiredDisposedCode?.value ??
          amounts.transactionAcquiredDisposedCode ??
          ""
      )
        .trim()
        .toUpperCase() || null
    )

    const shares = safeNumber(amounts.transactionShares?.value ?? amounts.transactionShares)
    const price = safeNumber(
      amounts.transactionPricePerShare?.value ?? amounts.transactionPricePerShare
    )
    const transactionDate = normalizeTransactionDate(dates.value ?? dates ?? null)

    parsedRows.push({
      action: transactionCodeToAction(code, acquiredDisposed),
      code,
      shares,
      price,
      transactionDate,
      order: index,
    })
  })

  const structuredResult = chooseMostRecentRelevantAction(parsedRows, role, insiderName)
  if (structuredResult) return structuredResult

  return {
    action: "Other",
    shares: null,
    avgPrice: null,
    role,
    insiderName,
  }
}

function parseTransactionBlocksFromText(text: string): ParsedTransactionRow[] {
  const blocks: ParsedTransactionRow[] = []
  const normalized = text.replace(/\s+/g, " ")
  let order = 0

  const patterns: RegExp[] = [
    /(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2}).{0,80}?\b([PS])\b.{0,180}?([\d,]+(?:\.\d+)?)(?:.{0,120}?\$?([\d,]+(?:\.\d+)?))?/gi,
    /\b([PS])\b.{0,120}?(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2}).{0,180}?([\d,]+(?:\.\d+)?)(?:.{0,120}?\$?([\d,]+(?:\.\d+)?))?/gi,
    /transaction\s+date\s*[:\-]?\s*(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2}).{0,120}?transaction\s+code\s*[:\-]?\s*([PS]).{0,180}?([\d,]+(?:\.\d+)?)(?:.{0,120}?\$?([\d,]+(?:\.\d+)?))?/gi,
  ]

  for (const pattern of patterns) {
    let match: RegExpExecArray | null
    while ((match = pattern.exec(normalized)) !== null) {
      let rawDate: string | null = null
      let code: string | null = null
      let shares: number | null = null
      let price: number | null = null

      const first = match[1] || ""
      if (/^\d{2}\/\d{2}\/\d{4}$|^\d{4}-\d{2}-\d{2}$/.test(first)) {
        rawDate = match[1]
        code = String(match[2] || "").trim().toUpperCase() || null
        shares = safeNumber(match[3])
        price = safeNumber(match[4])
      } else {
        code = String(match[1] || "").trim().toUpperCase() || null
        rawDate = match[2]
        shares = safeNumber(match[3])
        price = safeNumber(match[4])
      }

      blocks.push({
        action: transactionCodeToAction(code, null),
        code,
        shares,
        price,
        transactionDate: normalizeTransactionDate(rawDate),
        order: order++,
      })
    }
  }

  return blocks
}

function parseLineBasedTransactionRows(text: string): ParsedTransactionRow[] {
  const lines = text
    .split(/\r?\n+/)
    .map((line) => line.trim())
    .filter(Boolean)

  const rows: ParsedTransactionRow[] = []
  let order = 0

  for (const line of lines) {
    const dateMatch = line.match(/(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2})/)
    const codeMatch = line.match(/\b([PS])\b/)
    if (!dateMatch || !codeMatch) continue

    const numericMatches = Array.from(line.matchAll(/\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b/g))
      .map((m) => safeNumber(m[0]))
      .filter((v): v is number => v !== null)

    const priceCandidates = numericMatches.filter((n) => n > 0 && n < 10000)
    const shareCandidates = numericMatches.filter((n) => n >= 100)

    rows.push({
      action: transactionCodeToAction(codeMatch[1], null),
      code: codeMatch[1].toUpperCase(),
      shares: shareCandidates.length ? shareCandidates[shareCandidates.length - 1] : null,
      price: priceCandidates.length ? priceCandidates[priceCandidates.length - 1] : null,
      transactionDate: normalizeTransactionDate(dateMatch[1]),
      order: order++,
    })
  }

  return rows
}

function parseCodeDatePairs(text: string): ParsedTransactionRow[] {
  const rows: ParsedTransactionRow[] = []
  let order = 0

  const regexes = [
    /\btransaction\s+date\s*[:\-]?\s*(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2}).{0,80}?transaction\s+code\s*[:\-]?\s*([PS])/gi,
    /(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2}).{0,40}?\b([PS])\b/gi,
  ]

  for (const regex of regexes) {
    let match: RegExpExecArray | null
    while ((match = regex.exec(text)) !== null) {
      rows.push({
        action: transactionCodeToAction(match[2], null),
        code: match[2].toUpperCase(),
        shares: null,
        price: null,
        transactionDate: normalizeTransactionDate(match[1]),
        order: order++,
      })
    }
  }

  return rows
}

function parseTransformedHtmlFallback(body: string): InsiderParseResult | null {
  const text = stripHtml(body)
  const lower = text.toLowerCase()

  const blockRows = parseTransactionBlocksFromText(text)
  const blockResult = chooseMostRecentRelevantAction(blockRows, null, null)
  if (blockResult && blockResult.action !== "Other") return blockResult

  const lineRows = parseLineBasedTransactionRows(text)
  const lineResult = chooseMostRecentRelevantAction(lineRows, null, null)
  if (lineResult && lineResult.action !== "Other") return lineResult

  const pairRows = parseCodeDatePairs(text)
  const pairResult = chooseMostRecentRelevantAction(pairRows, null, null)
  if (pairResult && pairResult.action !== "Other") return pairResult

  const hasStrongBuyEvidence =
    /\btransaction\s+code\s*[:\-]?\s*P\b/i.test(text) ||
    /\bopen market purchase\b/i.test(lower) ||
    /\bshares were purchased\b/i.test(lower) ||
    /\bpurchased in multiple transactions\b/i.test(lower)

  if (hasStrongBuyEvidence) {
    return { action: "Buy", shares: null, avgPrice: null, role: null, insiderName: null }
  }

  return { action: "Other", shares: null, avgPrice: null, role: null, insiderName: null }
}

async function fetchAndParseOwnershipXml(url: string): Promise<InsiderParseResult | null> {
  try {
    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          "User-Agent": SEC_USER_AGENT,
          Accept: "application/xml,text/xml,text/html;q=0.9,*/*;q=0.8",
        },
        cache: "no-store",
      },
      SEC_FETCH_TIMEOUT_MS
    )

    if (!res.ok) return null
    const body = await res.text()

    try {
      const parsed = xmlParser.parse(body)
      const doc = parsed?.ownershipDocument
      const xmlResult = parseOwnershipDocument(doc)
      if (xmlResult) return xmlResult
    } catch {
      // fall through
    }

    return parseTransformedHtmlFallback(body)
  } catch {
    return null
  }
}

function buildPossibleForm4Urls(filing: RawFiling) {
  const possibleUrls = new Set<string>()

  if (filing.filing_url) {
    possibleUrls.add(filing.filing_url)
    if (filing.filing_url.endsWith(".htm") || filing.filing_url.endsWith(".html")) {
      possibleUrls.add(filing.filing_url.replace(/\.html?$/i, ".xml"))
    }
  }

  if (filing.cik && filing.accession_no) {
    const cik = String(Number(filing.cik))
    const accession = filing.accession_no.replace(/-/g, "")
    const base = `https://www.sec.gov/Archives/edgar/data/${cik}/${accession}`

    possibleUrls.add(`${base}/doc4.xml`)
    possibleUrls.add(`${base}/ownership.xml`)
    possibleUrls.add(`${base}/primary_doc.xml`)
    possibleUrls.add(`${base}/form4.xml`)
    possibleUrls.add(`${base}/xslF345X05/doc4.xml`)
    possibleUrls.add(`${base}/xslF345X05/ownership.xml`)
    possibleUrls.add(`${base}/xslF345X05/primary_doc.xml`)
    possibleUrls.add(`${base}/xslF345X05/wk-form4.xml`)

    if (filing.primary_doc) {
      possibleUrls.add(`${base}/${filing.primary_doc}`)
      if (filing.primary_doc.endsWith(".htm") || filing.primary_doc.endsWith(".html")) {
        possibleUrls.add(`${base}/${filing.primary_doc.replace(/\.html?$/i, ".xml")}`)
      }
    }
  }

  return Array.from(possibleUrls)
}

async function parseForm4(filing: RawFiling): Promise<InsiderParseResult> {
  const urls = buildPossibleForm4Urls(filing)
  let fallbackResult: InsiderParseResult | null = null

  for (const url of urls) {
    const parsed = await fetchAndParseOwnershipXml(url)
    if (!parsed) continue
    if (parsed.action === "Buy") return parsed
    if (!fallbackResult) fallbackResult = parsed
  }

  return fallbackResult ?? {
    action: "Other",
    shares: null,
    avgPrice: null,
    role: null,
    insiderName: null,
  }
}

async function fetchFilingText(url: string | null) {
  if (!url) return null
  try {
    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          "User-Agent": SEC_USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        cache: "no-store",
      },
      TEXT_FETCH_TIMEOUT_MS
    )
    if (!res.ok) return null
    const body = await res.text()
    return stripHtml(body)
  } catch {
    return null
  }
}

function classify8kEvent(text: string | null): string | null {
  if (!text) return null
  const t = text.toLowerCase()

  const checks: Array<[string, RegExp[]]> = [
    [
      "guidance",
      [
        /\bguidance\b/,
        /\braised guidance\b/,
        /\bincreased outlook\b/,
        /\bupdated outlook\b/,
        /\bpreliminary results\b/,
      ],
    ],
    [
      "partnership",
      [
        /\bpartnership\b/,
        /\bcollaboration\b/,
        /\bdistribution agreement\b/,
        /\bcommercial agreement\b/,
        /\bcustomer agreement\b/,
        /\bstrategic alliance\b/,
      ],
    ],
    [
      "m&a",
      [
        /\bacquisition\b/,
        /\bmerger\b/,
        /\bdefinitive agreement\b/,
        /\basset purchase\b/,
        /\bpurchase agreement\b/,
      ],
    ],
    [
      "customer",
      [
        /\bmaterial customer\b/,
        /\bmaster services agreement\b/,
        /\bpurchase order\b/,
        /\bcommercial launch\b/,
        /\baward\b/,
      ],
    ],
    ["product", [/\bproduct launch\b/, /\bcommercial launch\b/, /\bapproval\b/, /\bclearance\b/]],
    ["asset-sale", [/\basset sale\b/, /\bdivestiture\b/, /\bsale of assets\b/]],
  ]

  for (const [label, patterns] of checks) {
    if (patterns.some((p) => p.test(t))) return label
  }

  return null
}

function isPositive8kCatalyst(catalystType: string | null) {
  return (
    catalystType === "guidance" ||
    catalystType === "partnership" ||
    catalystType === "customer" ||
    catalystType === "product" ||
    catalystType === "m&a" ||
    catalystType === "asset-sale"
  )
}

async function getPriceConfirmation(ticker: string): Promise<PriceConfirmation> {
  return await withTimeout(
    async () => {
      try {
        const endDate = new Date()
        const startDate = new Date()
        startDate.setDate(startDate.getDate() - 400)

        const [candles, spyCandles] = await Promise.all([
          yahooFinance.historical(ticker, {
            period1: toIsoDateString(startDate),
            period2: toIsoDateString(endDate),
            interval: "1d",
          }),
          yahooFinance
            .historical("SPY", {
              period1: toIsoDateString(startDate),
              period2: toIsoDateString(endDate),
              interval: "1d",
            })
            .catch(() => null),
        ])

        if (!candles || candles.length < 60) {
          return {
            return5d: null,
            return20d: null,
            volumeRatio: null,
            breakout20d: false,
            breakout52w: false,
            relativeStrength20d: null,
            above50dma: false,
            trendAligned: false,
            confirmed: false,
          }
        }

        const clean = candles
          .filter((c) => c.close !== null && c.volume !== null)
          .sort((a, b) => +new Date(a.date) - +new Date(b.date))

        if (clean.length < 60) {
          return {
            return5d: null,
            return20d: null,
            volumeRatio: null,
            breakout20d: false,
            breakout52w: false,
            relativeStrength20d: null,
            above50dma: false,
            trendAligned: false,
            confirmed: false,
          }
        }

        const latest = clean[clean.length - 1]
        const prev5 = clean[clean.length - 6]
        const prev20 = clean[clean.length - 21]
        const prior20 = clean.slice(-21, -1)
        const prior252 = clean.slice(-253, -1)
        const prior50 = clean.slice(-50)

        const latestClose = Number(latest.close)
        const latestVolume = Number(latest.volume)
        const close5Ago = Number(prev5.close)
        const close20Ago = Number(prev20.close)

        const avg20Volume = prior20.reduce((s, c) => s + Number(c.volume || 0), 0) / prior20.length

        const high20 = Math.max(...prior20.map((c) => Number(c.high || 0)))
        const high52w =
          prior252.length > 0 ? Math.max(...prior252.map((c) => Number(c.high || 0))) : high20

        const avg50Close = prior50.reduce((s, c) => s + Number(c.close || 0), 0) / prior50.length

        const return5d = close5Ago > 0 ? ((latestClose - close5Ago) / close5Ago) * 100 : null
        const return20d = close20Ago > 0 ? ((latestClose - close20Ago) / close20Ago) * 100 : null
        const volumeRatio = avg20Volume > 0 ? latestVolume / avg20Volume : null
        const breakout20d = latestClose > high20
        const breakout52w = latestClose > high52w
        const above50dma = latestClose > avg50Close
        const trendAligned = above50dma && return20d !== null && return20d > 0

        let relativeStrength20d: number | null = null

        if (spyCandles && spyCandles.length >= 21) {
          const spyClean = spyCandles
            .filter((c) => c.close !== null)
            .sort((a, b) => +new Date(a.date) - +new Date(b.date))

          if (spyClean.length >= 21) {
            const spyLatest = Number(spyClean[spyClean.length - 1].close)
            const spy20Ago = Number(spyClean[spyClean.length - 21].close)
            const spyReturn20d = spy20Ago > 0 ? ((spyLatest - spy20Ago) / spy20Ago) * 100 : null

            if (return20d !== null && spyReturn20d !== null) {
              relativeStrength20d = return20d - spyReturn20d
            }
          }
        }

        const confirmed =
          breakout20d ||
          breakout52w ||
          ((return5d ?? 0) >= 4 && (volumeRatio ?? 0) >= 1.5) ||
          ((return20d ?? 0) >= 12 && above50dma)

        return {
          return5d,
          return20d,
          volumeRatio,
          breakout20d,
          breakout52w,
          relativeStrength20d,
          above50dma,
          trendAligned,
          confirmed,
        }
      } catch {
        return {
          return5d: null,
          return20d: null,
          volumeRatio: null,
          breakout20d: false,
          breakout52w: false,
          relativeStrength20d: null,
          above50dma: false,
          trendAligned: false,
          confirmed: false,
        }
      }
    },
    YAHOO_TIMEOUT_MS,
    {
      return5d: null,
      return20d: null,
      volumeRatio: null,
      breakout20d: false,
      breakout52w: false,
      relativeStrength20d: null,
      above50dma: false,
      trendAligned: false,
      confirmed: false,
    }
  )
}

async function getTickerSnapshot(ticker: string): Promise<TickerSnapshot> {
  return await withTimeout(
    async () => {
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
          safeNumber((summary.price as any)?.regularMarketPrice) ??
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

        const psRatio =
          safeNumber((summary.summaryDetail as any)?.priceToSalesTrailing12Months) ??
          safeNumber((quote as any)?.priceToSalesTrailing12Months)

        const marketCap =
          safeNumber((summary.price as any)?.marketCap) ??
          safeNumber((quote as any)?.marketCap)

        const sector = ((summary.assetProfile as any)?.sector as string | undefined)?.trim() ?? null
        const industry =
          ((summary.assetProfile as any)?.industry as string | undefined)?.trim() ?? null

        const businessDescription =
          ((summary.assetProfile as any)?.longBusinessSummary as string | undefined)?.trim() ?? null

        const companyName =
          ((summary.price as any)?.longName as string | undefined)?.trim() ??
          ((summary.price as any)?.shortName as string | undefined)?.trim() ??
          ((quote as any)?.longName as string | undefined)?.trim() ??
          ((quote as any)?.shortName as string | undefined)?.trim() ??
          null

        return {
          peRatio,
          forwardPe,
          peType,
          psRatio: psRatio !== null && psRatio > 0 ? psRatio : null,
          marketCap,
          sector,
          industry,
          businessDescription,
          companyName,
        }
      } catch {
        return {
          peRatio: null,
          forwardPe: null,
          peType: null,
          psRatio: null,
          marketCap: null,
          sector: null,
          industry: null,
          businessDescription: null,
          companyName: null,
        }
      }
    },
    YAHOO_TIMEOUT_MS,
    {
      peRatio: null,
      forwardPe: null,
      peType: null,
      psRatio: null,
      marketCap: null,
      sector: null,
      industry: null,
      businessDescription: null,
      companyName: null,
    }
  )
}

async function getEarningsSignal(ticker: string): Promise<EarningsSignal> {
  return await withTimeout(
    async () => {
      try {
        const summary = await yahooFinance.quoteSummary(ticker, {
          modules: ["earningsHistory", "earningsTrend", "financialData"],
        })

        const history = toArray((summary as any)?.earningsHistory?.history)
        const latest = history[history.length - 1] ?? history[0]

        const epsActual = safeNumber(latest?.epsActual)
        const epsEstimate = safeNumber(latest?.epsEstimate)

        const surprisePct =
          epsActual !== null &&
          epsEstimate !== null &&
          Math.abs(epsEstimate) > 0
            ? ((epsActual - epsEstimate) / Math.abs(epsEstimate)) * 100
            : safeNumber(latest?.surprisePercent)

        const revenueGrowthPct =
          safeNumber((summary.financialData as any)?.revenueGrowth) !== null
            ? Number((summary.financialData as any).revenueGrowth) * 100
            : null

        const trend = toArray((summary as any)?.earningsTrend?.trend)
        const guidanceFlag = trend.some((row: any) => {
          const growth = safeNumber(row?.growth)
          return growth !== null && growth > 0.15
        })

        const hasSignal =
          (surprisePct !== null && surprisePct >= 10) ||
          (revenueGrowthPct !== null && revenueGrowthPct >= 15) ||
          guidanceFlag

        let summaryText: string | null = null
        if (hasSignal) {
          const pieces = uniqueStrings([
            surprisePct !== null ? `EPS surprise ≈ ${surprisePct.toFixed(1)}%` : null,
            revenueGrowthPct !== null ? `revenue growth ≈ ${revenueGrowthPct.toFixed(1)}%` : null,
            guidanceFlag ? "forward outlook appears constructive" : null,
          ])
          summaryText = pieces.length
            ? `Recent earnings data looks supportive: ${pieces.join(", ")}.`
            : "Recent earnings data appears supportive."
        }

        return {
          hasSignal,
          surprisePct,
          revenueGrowthPct,
          guidanceFlag,
          summary: summaryText,
        }
      } catch {
        return {
          hasSignal: false,
          surprisePct: null,
          revenueGrowthPct: null,
          guidanceFlag: false,
          summary: null,
        }
      }
    },
    YAHOO_TIMEOUT_MS,
    {
      hasSignal: false,
      surprisePct: null,
      revenueGrowthPct: null,
      guidanceFlag: false,
      summary: null,
    }
  )
}

function getInsiderBuyValue(insider: InsiderParseResult) {
  if (insider.action !== "Buy" || insider.shares === null || insider.avgPrice === null) {
    return null
  }
  return insider.shares * insider.avgPrice
}

function isSeniorRole(role: string | null) {
  const normalized = (role || "").toLowerCase()
  return (
    normalized.includes("chief executive officer") ||
    normalized.includes("ceo") ||
    normalized.includes("chief financial officer") ||
    normalized.includes("cfo") ||
    normalized.includes("president")
  )
}

function isDirectorRole(role: string | null) {
  return (role || "").toLowerCase().includes("director")
}

function baseSignal(formType: string | null): BaseSignalData | null {
  const normalized = normalizeFormType(formType)

  if (normalized === "4" || normalized === "4/A") {
    return {
      signal_type: "Insider Buy Signal",
      signal_source: "form4",
      bias: "Bullish",
      score: 52,
      title: "Insider buying setup detected",
      summary: "A Form 4 filing was detected and will only be kept if buying is confirmed.",
    }
  }

  if (
    normalized === "13D" ||
    normalized === "SC 13D" ||
    normalized === "13D/A" ||
    normalized === "SC 13D/A"
  ) {
    return {
      signal_type: "Ownership Catalyst",
      signal_source: "13d",
      bias: "Bullish",
      score: 58,
      title: "Major ownership catalyst detected",
      summary: "A 13D-style filing can indicate a meaningful investor building influence.",
    }
  }

  if (
    normalized === "13G" ||
    normalized === "SC 13G" ||
    normalized === "13G/A" ||
    normalized === "SC 13G/A"
  ) {
    return {
      signal_type: "Institutional Accumulation",
      signal_source: "13g",
      bias: "Bullish",
      score: 50,
      title: "Institutional ownership signal detected",
      summary: "A 13G-style filing can support a strong-buy setup when price action confirms.",
    }
  }

  if (normalized === "8-K" || normalized === "6-K") {
    return {
      signal_type: "Positive Catalyst",
      signal_source: "8k",
      bias: "Bullish",
      score: 48,
      title: "Potential positive catalyst detected",
      summary: "A current report filing may contain a constructive catalyst if follow-through is present.",
    }
  }

  if (normalized === "10-Q" || normalized === "10-K") {
    return {
      signal_type: "Earnings-Fundamental Support",
      signal_source: "earnings",
      bias: "Bullish",
      score: 46,
      title: "Constructive earnings setup detected",
      summary: "A periodic report can support a strong-buy setup when earnings and price action align.",
    }
  }

  return null
}

function maybeCreateEarningsBreakoutBase(
  filing: RawFiling,
  price: PriceConfirmation,
  earnings: EarningsSignal
): BaseSignalData | null {
  const form = normalizeFormType(filing.form_type)
  if (form) return null

  if (
    earnings.hasSignal &&
    (price.breakout20d || price.breakout52w || (price.volumeRatio ?? 0) >= 1.75) &&
    (price.relativeStrength20d ?? 0) >= 3
  ) {
    return {
      signal_type: "Earnings Breakout",
      signal_source: "earnings",
      bias: "Bullish",
      score: 62,
      title: "Earnings-backed breakout setup",
      summary:
        earnings.summary ??
        "Recent earnings support and breakout behavior are showing up together.",
    }
  }

  return null
}

function buildSyntheticCandidateFiling(
  candidate: CandidateUniverseSignalInput,
  runTimestamp: string
): RawFiling {
  const ticker = normalizeTicker(candidate.ticker)
  const screenedDate =
    candidate.last_screened_at && !Number.isNaN(new Date(candidate.last_screened_at).getTime())
      ? new Date(candidate.last_screened_at).toISOString()
      : runTimestamp

  const screenedDay = screenedDate.slice(0, 10)

  return {
    ticker,
    company_name: candidate.name,
    form_type: null,
    filed_at: screenedDay,
    filing_url: null,
    accession_no: `TECH_${screenedDay}_${ticker}`,
    cik: candidate.cik,
    primary_doc: null,
    fetched_at: screenedDate,
  }
}

function maybeCreateCandidateTechnicalBase(
  candidate: CandidateUniverseSignalInput,
  price: PriceConfirmation,
  earnings: EarningsSignal
): BaseSignalData | null {
  const candidateScore = Number(candidate.candidate_score || 0)
  const included = candidate.included === true
  const hasBreakout = candidate.breakout_20d === true || price.breakout20d || price.breakout52w
  const hasTrend = candidate.above_sma_20 === true || price.trendAligned || price.above50dma
  const strongVolume = (candidate.volume_ratio ?? 0) >= 2 || (price.volumeRatio ?? 0) >= 2
  const strongMomentum =
    (candidate.return_20d ?? 0) >= 18 ||
    (candidate.return_5d ?? 0) >= 6 ||
    (price.return20d ?? 0) >= 18 ||
    (price.return5d ?? 0) >= 6

  const technicalSetup =
    included ||
    (candidateScore >= MIN_CANDIDATE_SCORE_FOR_SYNTHETIC_SIGNAL &&
      hasBreakout &&
      hasTrend &&
      strongVolume &&
      strongMomentum)

  if (!technicalSetup) return null

  let baseScore = included ? 66 : 62
  if (candidateScore >= 98) baseScore = 74
  else if (candidateScore >= 95) baseScore = 71
  else if (candidateScore >= 90) baseScore = 68

  if (hasBreakout) baseScore += 3
  if (strongVolume) baseScore += 2
  if (earnings.hasSignal) baseScore += 2

  const title =
    candidateScore >= 95
      ? "Elite technical strong-buy setup"
      : candidateScore >= 90
        ? "High-conviction technical strong-buy setup"
        : "Technical strong-buy setup"

  const summaryParts = uniqueStrings([
    candidate.screen_reason,
    hasBreakout ? "Breakout behavior is present" : null,
    hasTrend ? "Trend support is intact" : null,
    strongVolume ? "Volume is elevated" : null,
    earnings.hasSignal ? "earnings data also looks supportive" : null,
  ])

  return {
    signal_type: "Technical Strong Buy",
    signal_source: "breakout",
    bias: "Bullish",
    score: clamp(baseScore, 60, 80),
    title,
    summary: summaryParts.length
      ? `Technical screen is highly constructive: ${summaryParts.join(", ")}.`
      : "Technical screen and market action point to a highly constructive setup.",
  }
}

function getStrengthBucket(score: number): StrengthBucket {
  if (score >= 97) return "Elite Buy"
  if (score >= 90) return "Strong Buy"
  return "Buy"
}

function deriveSignalCategory(params: {
  source: SignalSource
  clusterInfo: ClusterInfo | null
  candidate: CandidateUniverseSignalInput | null
  earnings: EarningsSignal
  catalystType: string | null
}) {
  if ((params.clusterInfo?.clusterSize ?? 0) >= 2) return "Multi-Signal Strong Buy" as const
  if (params.source === "form4") return "Insider Buying" as const
  if (params.source === "breakout") return "Breakout" as const
  if (params.source === "earnings" && params.earnings.hasSignal) return "Earnings Breakout" as const
  if (params.source === "8k" && params.catalystType) return "Catalyst" as const
  if (params.source === "13d" || params.source === "13g") return "Ownership" as const
  if (params.candidate?.included) return "Breakout" as const
  return "Multi-Signal Strong Buy" as const
}

function buildSignalTags(params: {
  source: SignalSource
  insider: InsiderParseResult
  clusterInfo: ClusterInfo | null
  insiderBuyValue: number | null
  price: PriceConfirmation
  snapshot: TickerSnapshot
  earnings: EarningsSignal
  catalystType: string | null
  candidate: CandidateUniverseSignalInput | null
}) {
  const tags: string[] = []

  tags.push(`source:${params.source}`)
  tags.push("bullish")
  tags.push("strong-buy-only")

  if (params.insider.action === "Buy") tags.push("insider-buy")
  if ((params.clusterInfo?.clusterSize ?? 0) >= 2) tags.push("cluster-buy")
  if ((params.clusterInfo?.clusterSize ?? 0) >= 3) tags.push("cluster-strong")
  if (params.clusterInfo?.repeatBuyer) tags.push("repeat-buyer")

  if ((params.insiderBuyValue ?? 0) >= 250_000) tags.push("large-insider-buy")
  if ((params.insiderBuyValue ?? 0) >= 1_000_000) tags.push("very-large-insider-buy")

  if ((params.price.volumeRatio ?? 0) >= 1.5) tags.push("volume-confirmed")
  if ((params.price.volumeRatio ?? 0) >= 2.0) tags.push("heavy-volume")
  if ((params.price.return5d ?? 0) >= 5) tags.push("momentum-confirmed")
  if (params.price.breakout20d) tags.push("breakout-20d")
  if (params.price.breakout52w) tags.push("breakout-52w")
  if (params.price.above50dma) tags.push("above-50dma")
  if (params.price.trendAligned) tags.push("trend-aligned")
  if ((params.price.relativeStrength20d ?? 0) >= 5) tags.push("relative-strength")

  const effectivePe = params.snapshot.peRatio ?? params.snapshot.forwardPe
  if (effectivePe !== null && effectivePe <= 18) tags.push("reasonable-valuation")
  if (effectivePe !== null && effectivePe <= 10) tags.push("deep-value")

  if (params.earnings.hasSignal) tags.push("earnings-support")
  if ((params.earnings.surprisePct ?? 0) >= 10) tags.push("eps-beat")
  if ((params.earnings.revenueGrowthPct ?? 0) >= 15) tags.push("revenue-growth")
  if (params.earnings.guidanceFlag) tags.push("guidance-support")

  if (params.candidate) {
    tags.push("candidate-screen")
    if (params.candidate.included) tags.push("candidate-included")
    if ((params.candidate.candidate_score ?? 0) >= 95) tags.push("candidate-elite")
    else if ((params.candidate.candidate_score ?? 0) >= 90) tags.push("candidate-strong-buy")
    if ((params.candidate.volume_ratio ?? 0) >= 2) tags.push("screen-heavy-volume")
    if ((params.candidate.return_20d ?? 0) >= 15) tags.push("screen-momentum")
  }

  if (params.catalystType) tags.push(`8k:${params.catalystType}`)

  return uniqueStrings(tags)
}

function scorePositive8kCatalyst(catalystType: string | null) {
  if (!catalystType) return 0
  if (catalystType === "guidance") return 10
  if (catalystType === "partnership") return 8
  if (catalystType === "customer") return 8
  if (catalystType === "product") return 7
  if (catalystType === "m&a") return 8
  if (catalystType === "asset-sale") return 3
  return 0
}

function scoreEarnings(earnings: EarningsSignal) {
  let score = 0

  if ((earnings.surprisePct ?? 0) >= 25) score += 8
  else if ((earnings.surprisePct ?? 0) >= 15) score += 6
  else if ((earnings.surprisePct ?? 0) >= 10) score += 4

  if ((earnings.revenueGrowthPct ?? 0) >= 25) score += 6
  else if ((earnings.revenueGrowthPct ?? 0) >= 15) score += 4

  if (earnings.guidanceFlag) score += 4

  return score
}

function applyBreakdown(
  breakdown: ScoreBreakdown,
  reasons: string[],
  key: string,
  value: number,
  reason?: string | null
) {
  if (!Number.isFinite(value) || value === 0) return
  breakdown[key] = round2((breakdown[key] || 0) + value) ?? value
  if (reason) reasons.push(reason)
}

function getRoleWeight(role: string | null) {
  if (!role) return 0
  if (isSeniorRole(role)) return 8
  if (isDirectorRole(role)) return 4
  if (role.toLowerCase().includes("officer")) return 5
  return 2
}

function scoreInsiderBuy(
  insiderBuyValue: number | null,
  role: string | null,
  clusterInfo: ClusterInfo | null
) {
  let score = 0
  if (insiderBuyValue !== null) {
    if (insiderBuyValue >= 5_000_000) score += 18
    else if (insiderBuyValue >= 1_000_000) score += 14
    else if (insiderBuyValue >= 500_000) score += 10
    else if (insiderBuyValue >= 100_000) score += 7
    else score += 4
  } else {
    score += 3
  }

  score += getRoleWeight(role)

  if ((clusterInfo?.clusterSize ?? 0) >= 5) score += 16
  else if ((clusterInfo?.clusterSize ?? 0) === 4) score += 13
  else if ((clusterInfo?.clusterSize ?? 0) === 3) score += 10
  else if ((clusterInfo?.clusterSize ?? 0) === 2) score += 7

  if (clusterInfo?.repeatBuyer) score += 4

  return score
}

function scoreMomentum(price: PriceConfirmation) {
  let score = 0

  if ((price.return5d ?? 0) >= 10) score += 8
  else if ((price.return5d ?? 0) >= 6) score += 6
  else if ((price.return5d ?? 0) >= 3) score += 3

  if ((price.return20d ?? 0) >= 20) score += 7
  else if ((price.return20d ?? 0) >= 12) score += 5
  else if ((price.return20d ?? 0) >= 8) score += 3

  if (price.breakout20d) score += 5
  if (price.breakout52w) score += 7
  if (price.above50dma) score += 2
  if (price.trendAligned) score += 3

  if ((price.volumeRatio ?? 0) >= 2.5) score += 6
  else if ((price.volumeRatio ?? 0) >= 2) score += 4
  else if ((price.volumeRatio ?? 0) >= 1.5) score += 2

  return score
}

function scoreRelativeStrength(value: number | null) {
  if (value === null) return 0
  if (value >= 12) return 9
  if (value >= 8) return 6
  if (value >= 4) return 4
  if (value >= 2) return 2
  return 0
}

function scoreValuation(snapshot: TickerSnapshot) {
  let score = 0
  const pe = snapshot.peRatio ?? snapshot.forwardPe

  if (pe !== null) {
    if (pe <= 10) score += 6
    else if (pe <= 18) score += 4
    else if (pe <= 25) score += 2
  }

  if (snapshot.psRatio !== null) {
    if (snapshot.psRatio <= 2) score += 3
    else if (snapshot.psRatio <= 5) score += 1
  }

  if (snapshot.marketCap !== null) {
    if (snapshot.marketCap >= 1_000_000_000 && snapshot.marketCap <= 50_000_000_000) score += 1
  }

  return score
}

function scoreCandidateTechnical(candidate: CandidateUniverseSignalInput | null) {
  if (!candidate) return 0

  let score = 0
  const candidateScore = Number(candidate.candidate_score || 0)

  if (candidate.included) score += 8
  if (candidateScore >= 98) score += 16
  else if (candidateScore >= 95) score += 13
  else if (candidateScore >= 90) score += 10
  else if (candidateScore >= 85) score += 6

  if ((candidate.return_5d ?? 0) >= 6) score += 3
  else if ((candidate.return_5d ?? 0) >= 3) score += 1

  if ((candidate.return_20d ?? 0) >= 20) score += 5
  else if ((candidate.return_20d ?? 0) >= 12) score += 3

  if ((candidate.volume_ratio ?? 0) >= 2.5) score += 4
  else if ((candidate.volume_ratio ?? 0) >= 1.75) score += 2

  if (candidate.breakout_20d) score += 4
  if (candidate.above_sma_20) score += 2

  return score
}

function ageAdjustment(ageDays: number | null) {
  if (ageDays === null) return 0
  if (ageDays <= 1) return 6
  if (ageDays <= 3) return 4
  if (ageDays <= 7) return 2
  if (ageDays <= 14) return 0
  return -8
}

function getValuationFlavor(
  peRatio: number | null,
  forwardPe: number | null,
  psRatio: number | null
) {
  const pe = peRatio ?? forwardPe
  if (pe !== null) {
    if (pe <= 10) return "deep value"
    if (pe <= 18) return "reasonable value"
    if (pe <= 25) return "fair value"
  }

  if (psRatio !== null) {
    if (psRatio <= 2) return "low sales multiple"
  }

  return null
}

function countPositiveEvidencePillars(breakdown: ScoreBreakdown) {
  return [
    (breakdown.insider_buying || 0) > 0 ||
      (breakdown.repeat_buying || 0) > 0 ||
      (breakdown.senior_executive_buy || 0) > 0,
    (breakdown.momentum || 0) > 0 || (breakdown.relative_strength || 0) > 0,
    (breakdown.earnings || 0) > 0 || (breakdown.catalyst || 0) > 0,
    (breakdown.candidate_screen || 0) > 0 || (breakdown.candidate_breakout || 0) > 0,
    (breakdown.valuation || 0) > 0,
  ].filter(Boolean).length
}

function hasStrongPositivePriceConfirmation(price: PriceConfirmation) {
  return (
    price.confirmed &&
    (price.relativeStrength20d ?? 0) >= 2 &&
    ((price.breakout20d || price.breakout52w) ||
      ((price.return5d ?? 0) >= 4 && (price.volumeRatio ?? 0) >= 1.5) ||
      ((price.return20d ?? 0) >= 12 && price.trendAligned))
  )
}

function isEligibleStrongBuySignal(params: {
  filing: RawFiling
  base: BaseSignalData
  insider: InsiderParseResult
  price: PriceConfirmation
  earnings: EarningsSignal
  catalystType: string | null
  candidate: CandidateUniverseSignalInput | null
  ageDays: number | null
  clusterInfo: ClusterInfo | null
}) {
  const form = normalizeFormType(params.filing.form_type)
  const candidateScore = Number(params.candidate?.candidate_score || 0)
  const candidateIncluded = params.candidate?.included === true
  const strongPrice = hasStrongPositivePriceConfirmation(params.price)
  const freshEnough = (params.ageDays ?? 999) <= MAX_SIGNAL_AGE_DAYS

  if (!freshEnough) return false
  if ((params.price.relativeStrength20d ?? 0) < 2) return false
  if ((params.price.return5d ?? 0) < -1) return false

  if (params.base.signal_source === "breakout") {
    return (
      strongPrice &&
      (candidateIncluded ||
        candidateScore >= MIN_CANDIDATE_SCORE_FOR_SYNTHETIC_SIGNAL ||
        ((params.candidate?.breakout_20d === true || params.price.breakout20d) &&
          (params.candidate?.volume_ratio ?? params.price.volumeRatio ?? 0) >= 1.75))
    )
  }

  if (form === "4" || form === "4/A") {
    if (params.insider.action !== "Buy") return false

    const buyValue = getInsiderBuyValue(params.insider)
    const hasMeaningfulInsiderSize =
      (buyValue ?? 0) >= 100_000 || (params.insider.shares ?? 0) >= 10_000

    return (
      strongPrice &&
      hasMeaningfulInsiderSize &&
      ((params.clusterInfo?.clusterSize ?? 0) >= 2 ||
        (buyValue ?? 0) >= 250_000 ||
        isSeniorRole(params.insider.role))
    )
  }

  if (
    form === "13D" ||
    form === "SC 13D" ||
    form === "13D/A" ||
    form === "SC 13D/A"
  ) {
    return strongPrice
  }

  if (
    form === "13G" ||
    form === "SC 13G" ||
    form === "13G/A" ||
    form === "SC 13G/A"
  ) {
    return strongPrice && (candidateIncluded || candidateScore >= 90)
  }

  if (form === "8-K" || form === "6-K") {
    return strongPrice && isPositive8kCatalyst(params.catalystType)
  }

  if (form === "10-Q" || form === "10-K" || params.base.signal_source === "earnings") {
    return strongPrice && params.earnings.hasSignal
  }

  return false
}

function mapToAppScore(rawScore: number, params: {
  breakoutConfirmed: boolean
  strongVolume: boolean
  clusterBuy: boolean
  largeInsiderBuy: boolean
  candidateScore: number | null
  positivePillars: number
  hasFreshCatalyst: boolean
  hasEarningsSupport: boolean
  freshSignal: boolean
}) {
  let appScore = Math.round(Math.pow(clamp(rawScore, 0, 100) / 100, 1.35) * 100)

  const eliteEvidenceCount = [
    params.breakoutConfirmed,
    params.strongVolume,
    params.clusterBuy || params.largeInsiderBuy,
    (params.candidateScore ?? 0) >= 95,
    params.hasFreshCatalyst || params.hasEarningsSupport,
    params.positivePillars >= 4,
    params.freshSignal,
  ].filter(Boolean).length

  if (params.positivePillars < 3) appScore = Math.min(appScore, 87)
  if (params.positivePillars < 4) appScore = Math.min(appScore, 93)
  if (eliteEvidenceCount < 4) appScore = Math.min(appScore, 96)
  if (eliteEvidenceCount < 5) appScore = Math.min(appScore, 98)

  if (
    !(
      params.breakoutConfirmed &&
      params.strongVolume &&
      params.positivePillars >= 4 &&
      eliteEvidenceCount >= 5 &&
      (params.clusterBuy ||
        params.largeInsiderBuy ||
        (params.candidateScore ?? 0) >= 98)
    )
  ) {
    appScore = Math.min(appScore, 99)
  }

  return clamp(appScore, 0, 100)
}

function applyEnhancements(
  filing: RawFiling,
  base: BaseSignalData,
  insider: InsiderParseResult,
  price: PriceConfirmation,
  snapshot: TickerSnapshot,
  clusterInfo: ClusterInfo | null,
  earnings: EarningsSignal,
  catalystType: string | null,
  candidate: CandidateUniverseSignalInput | null
): EnhancedSignal | null {
  let title = base.title
  let summary = base.summary
  let insiderSignalFlavor = "Standard"
  let insiderBuyValue: number | null = null
  const source = base.signal_source
  const ageDays = daysBetween(filing.filed_at)
  const freshnessBucket = freshnessBucketFromAge(ageDays)

  const breakdown: ScoreBreakdown = {}
  const reasons: string[] = []
  const scoreCapsApplied: string[] = []

  applyBreakdown(breakdown, reasons, "base", base.score, "Base strong-buy signal")

  const form = normalizeFormType(filing.form_type)

  if (form === "4" || form === "4/A") {
    if (insider.action !== "Buy") return null

    insiderBuyValue = getInsiderBuyValue(insider)
    const insiderScore = scoreInsiderBuy(insiderBuyValue, insider.role, clusterInfo)
    applyBreakdown(
      breakdown,
      reasons,
      "insider_buying",
      insiderScore,
      insiderBuyValue !== null ? "Insider buying support" : "Recent insider buy"
    )

    title = "Insider buy detected"
    summary = `Form 4 shows the most recent relevant insider action is a buy${
      insider.shares ? ` of about ${Math.round(insider.shares).toLocaleString()} shares` : ""
    }.`

    if (insiderBuyValue !== null && insiderBuyValue >= 1_000_000) {
      insiderSignalFlavor = "Large Buy"
      title = "Large Insider Buy"
    }

    if ((clusterInfo?.clusterSize ?? 0) >= 2) {
      insiderSignalFlavor = "Cluster Buy"
      title = "Cluster Insider Buying"
      summary =
        `${clusterInfo?.clusterSize} recent insider buy filings were detected within 7 days` +
        `${clusterInfo?.totalShares ? `, total shares ≈ ${Math.round(
          clusterInfo.totalShares
        ).toLocaleString()}` : ""}.`
    }

    if (clusterInfo?.repeatBuyer) {
      applyBreakdown(breakdown, reasons, "repeat_buying", 4, "Repeat insider buying")
    }

    if (isSeniorRole(insider.role)) {
      applyBreakdown(breakdown, reasons, "senior_executive_buy", 6, "Senior executive buying")
    }
  }

  if (source === "breakout" && candidate) {
    const candidateScore = scoreCandidateTechnical(candidate)
    applyBreakdown(
      breakdown,
      reasons,
      "candidate_screen",
      candidateScore,
      candidate.included
        ? "Candidate screen approved the setup"
        : "Technical candidate screen support"
    )

    title =
      (candidate.candidate_score ?? 0) >= 95
        ? "Elite technical strong-buy setup"
        : (candidate.candidate_score ?? 0) >= 90
          ? "High-conviction technical strong-buy setup"
          : "Technical strong-buy setup"

    summary =
      candidate.screen_reason
        ? `The technical candidate screen is highly constructive: ${candidate.screen_reason}.`
        : "The technical candidate screen is highly constructive."

    if ((candidate.candidate_score ?? 0) >= 95) {
      applyBreakdown(breakdown, reasons, "candidate_tier_bonus", 6, "Elite candidate tier")
    } else if ((candidate.candidate_score ?? 0) >= 90) {
      applyBreakdown(breakdown, reasons, "candidate_tier_bonus", 4, "Strong buy tier")
    }

    if ((candidate.volume_ratio ?? 0) >= 2) {
      applyBreakdown(breakdown, reasons, "candidate_volume", 3, "Elevated screen volume")
    }

    if ((candidate.return_20d ?? 0) >= 15) {
      applyBreakdown(breakdown, reasons, "candidate_momentum", 4, "Strong screened momentum")
    }

    if (candidate.breakout_20d) {
      applyBreakdown(breakdown, reasons, "candidate_breakout", 4, "Candidate breakout")
    }
  }

  const momentumScore = scoreMomentum(price)
  applyBreakdown(breakdown, reasons, "momentum", momentumScore, "Price and trend support")

  const rsScore = scoreRelativeStrength(price.relativeStrength20d)
  applyBreakdown(breakdown, reasons, "relative_strength", rsScore, "Stronger than market")

  const valuationScore = scoreValuation(snapshot)
  applyBreakdown(
    breakdown,
    reasons,
    "valuation",
    valuationScore,
    valuationScore > 0 ? "Valuation support" : null
  )

  const earningsScore = scoreEarnings(earnings)
  applyBreakdown(
    breakdown,
    reasons,
    "earnings",
    earningsScore,
    earningsScore > 0 ? "Earnings support" : null
  )

  const catalystScore = scorePositive8kCatalyst(catalystType)
  applyBreakdown(
    breakdown,
    reasons,
    "catalyst",
    catalystScore,
    catalystScore > 0 ? "Positive catalyst" : null
  )

  const ageScore = ageAdjustment(ageDays)
  applyBreakdown(
    breakdown,
    reasons,
    "freshness",
    ageScore,
    ageScore > 0 ? "Fresh signal" : null
  )

  const valuationFlavor = getValuationFlavor(snapshot.peRatio, snapshot.forwardPe, snapshot.psRatio)
  if (valuationFlavor) summary += ` Valuation currently looks ${valuationFlavor}.`

  if (price.confirmed) {
    title = `${title} with price/volume confirmation`
    summary += " Price and volume action are also confirming the setup."
  }

  if (price.volumeRatio !== null && price.volumeRatio >= 2.0) {
    summary += " Trading activity is elevated versus normal volume."
  }

  if (earnings.hasSignal && earnings.summary) {
    summary += ` ${earnings.summary}`
  }

  const positivePillars = countPositiveEvidencePillars(breakdown)
  let preCapScore = Object.values(breakdown).reduce((a, b) => a + b, 0)

  if (preCapScore > 88 && positivePillars < 3) {
    const target = 88
    const capAdjustment = target - preCapScore
    applyBreakdown(
      breakdown,
      reasons,
      "minimum_evidence_cap",
      capAdjustment,
      "High score capped due to limited confirming evidence"
    )
    scoreCapsApplied.push("minimum-evidence-cap")
    preCapScore = target
  }

  if (preCapScore > 94 && positivePillars < 4) {
    const target = 94
    const capAdjustment = target - preCapScore
    applyBreakdown(
      breakdown,
      reasons,
      "broad_confirmation_cap",
      capAdjustment,
      "Top-end score capped until more independent pillars align"
    )
    scoreCapsApplied.push("broad-confirmation-cap")
    preCapScore = target
  }

  const rawScore = clamp(Math.round(preCapScore), 0, 100)

  const appScore = mapToAppScore(rawScore, {
    breakoutConfirmed: price.breakout20d || price.breakout52w || candidate?.breakout_20d === true,
    strongVolume: (price.volumeRatio ?? 0) >= 2 || (candidate?.volume_ratio ?? 0) >= 2,
    clusterBuy: (clusterInfo?.clusterSize ?? 0) >= 2,
    largeInsiderBuy: (insiderBuyValue ?? 0) >= 1_000_000,
    candidateScore: candidate?.candidate_score ?? null,
    positivePillars,
    hasFreshCatalyst: catalystScore > 0 && (ageDays ?? 99) <= 7,
    hasEarningsSupport: earningsScore > 0,
    freshSignal: (ageDays ?? 999) <= 3,
  })

  const signalCategory = deriveSignalCategory({
    source,
    clusterInfo,
    candidate,
    earnings,
    catalystType,
  })

  const tags = buildSignalTags({
    source,
    insider,
    clusterInfo,
    insiderBuyValue,
    price,
    snapshot,
    earnings,
    catalystType,
    candidate,
  })

  return {
    signal_type: base.signal_type,
    signal_source: source,
    signal_category: signalCategory,
    signal_strength_bucket: getStrengthBucket(appScore),
    signal_tags: tags,
    bias: "Bullish",
    score: rawScore,
    app_score: appScore,
    board_bucket: "Buy",
    title,
    summary,
    insiderBuyValue,
    insiderSignalFlavor,
    ageDays,
    score_breakdown: breakdown,
    signal_reasons: uniqueStrings(reasons).slice(0, 12),
    score_caps_applied: uniqueStrings(scoreCapsApplied),
    stacked_signal_count: 1,
    freshness_bucket: freshnessBucket,
  }
}

function buildClusterMap(items: PreparedSignal[]) {
  const result = new Map<string, ClusterInfo>()
  const buysByTicker = new Map<
    string,
    { accessionNo: string; filedAt: number; shares: number; insiderName: string | null }[]
  >()

  for (const item of items) {
    const form = normalizeFormType(item.filing.form_type)
    const isBuyForm4 = (form === "4" || form === "4/A") && item.insider.action === "Buy"

    if (!isBuyForm4 || !item.filing.filed_at) continue

    const ticker = normalizeTicker(item.filing.ticker)
    const filedAt = new Date(item.filing.filed_at).getTime()
    if (Number.isNaN(filedAt)) continue

    const shares = Number(item.insider.shares || 0)

    if (!buysByTicker.has(ticker)) buysByTicker.set(ticker, [])

    buysByTicker.get(ticker)!.push({
      accessionNo: item.filing.accession_no,
      filedAt,
      shares,
      insiderName: item.insider.insiderName ?? null,
    })
  }

  for (const [, rows] of buysByTicker) {
    rows.sort((a, b) => a.filedAt - b.filedAt)

    for (let i = 0; i < rows.length; i++) {
      const current = rows[i]
      const windowStart = current.filedAt - 7 * 24 * 60 * 60 * 1000

      let clusterSize = 0
      let totalShares = 0
      const insiderNames = new Set<string>()
      let repeatBuyer = false

      for (let j = 0; j < rows.length; j++) {
        const candidate = rows[j]
        if (candidate.filedAt >= windowStart && candidate.filedAt <= current.filedAt) {
          clusterSize += 1
          totalShares += candidate.shares

          if (candidate.insiderName) {
            if (insiderNames.has(candidate.insiderName)) repeatBuyer = true
            insiderNames.add(candidate.insiderName)
          }
        }
      }

      result.set(current.accessionNo, {
        clusterSize,
        totalShares,
        uniqueInsiders: insiderNames.size,
        repeatBuyer,
      })
    }
  }

  return result
}

function buildHistoryKey(runDate: string, signalKey: string) {
  return `${runDate}_${signalKey}`
}

function buildSignalHistoryRow(signalRow: any, runDate: string, runTimestamp: string) {
  return {
    signal_key: signalRow.signal_key,
    ticker: signalRow.ticker,
    company_name: signalRow.company_name,
    business_description: signalRow.business_description,
    pe_ratio: signalRow.pe_ratio,
    pe_forward: signalRow.pe_forward,
    pe_type: signalRow.pe_type,
    signal_type: signalRow.signal_type,
    signal_source: signalRow.signal_source,
    signal_category: signalRow.signal_category,
    signal_strength_bucket: signalRow.signal_strength_bucket,
    signal_tags: signalRow.signal_tags,
    catalyst_type: signalRow.catalyst_type,
    bias: signalRow.bias,
    score: signalRow.score,
    app_score: signalRow.app_score,
    board_bucket: signalRow.board_bucket,
    title: signalRow.title,
    summary: signalRow.summary,
    source_form: signalRow.source_form,
    filed_at: signalRow.filed_at,
    filing_url: signalRow.filing_url,
    accession_no: signalRow.accession_no,
    insider_action: signalRow.insider_action,
    insider_shares: signalRow.insider_shares,
    insider_avg_price: signalRow.insider_avg_price,
    insider_buy_value: signalRow.insider_buy_value,
    insider_signal_flavor: signalRow.insider_signal_flavor,
    cluster_buyers: signalRow.cluster_buyers,
    cluster_shares: signalRow.cluster_shares,
    price_return_5d: signalRow.price_return_5d,
    price_return_20d: signalRow.price_return_20d,
    volume_ratio: signalRow.volume_ratio,
    breakout_20d: signalRow.breakout_20d,
    breakout_52w: signalRow.breakout_52w,
    above_50dma: signalRow.above_50dma,
    trend_aligned: signalRow.trend_aligned,
    price_confirmed: signalRow.price_confirmed,
    earnings_surprise_pct: signalRow.earnings_surprise_pct,
    revenue_growth_pct: signalRow.revenue_growth_pct,
    guidance_flag: signalRow.guidance_flag,
    market_cap: signalRow.market_cap,
    sector: signalRow.sector,
    industry: signalRow.industry,
    relative_strength_20d: signalRow.relative_strength_20d,
    age_days: signalRow.age_days,
    freshness_bucket: signalRow.freshness_bucket,
    score_breakdown: signalRow.score_breakdown,
    score_version: signalRow.score_version,
    stacked_signal_count: signalRow.stacked_signal_count,
    signal_reasons: signalRow.signal_reasons,
    score_caps_applied: signalRow.score_caps_applied,
    ticker_score_change_1d: signalRow.ticker_score_change_1d,
    ticker_score_change_7d: signalRow.ticker_score_change_7d,
    signal_history_key: buildHistoryKey(runDate, signalRow.signal_key),
    scored_on: runDate,
    created_at: runTimestamp,
  }
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
      return new Date(b.filed_at || 0).getTime() - new Date(a.filed_at || 0).getTime()
    })

    const primary = sorted[0]

    const scoreBreakdown: ScoreBreakdown = {}
    const signalReasons = new Set<string>()
    const scoreCapsApplied = new Set<string>()
    const signalTags = new Set<string>()
    const accessionNos: string[] = []
    const signalKeys: string[] = []
    const sourceForms: string[] = []

    for (const row of sorted) {
      signalKeys.push(row.signal_key)
      accessionNos.push(row.accession_no)
      if (row.source_form) sourceForms.push(row.source_form)

      for (const tag of row.signal_tags || []) signalTags.add(tag)
      for (const reason of row.signal_reasons || []) signalReasons.add(reason)
      for (const cap of row.score_caps_applied || []) scoreCapsApplied.add(cap)

      const breakdown = (row.score_breakdown || {}) as ScoreBreakdown
      for (const [key, value] of Object.entries(breakdown)) {
        scoreBreakdown[key] = round2((scoreBreakdown[key] || 0) + Number(value || 0)) ?? 0
      }
    }

    let stackedScore = Number(primary.app_score || 0)

    if (sorted.length >= 2) stackedScore += 2
    if (sorted.length >= 3) stackedScore += 1
    if (sorted.length >= 4) stackedScore += 1

    const positivePillars = countPositiveEvidencePillars(scoreBreakdown)

    if (positivePillars < 3) {
      stackedScore = Math.min(stackedScore, 89)
      scoreCapsApplied.add("stacked-limited-evidence-cap")
    }

    if (positivePillars < 4) {
      stackedScore = Math.min(stackedScore, 94)
      scoreCapsApplied.add("stacked-broad-confirmation-cap")
    }

    const hasBreakoutSupport =
      (scoreBreakdown.candidate_breakout || 0) > 0 ||
      primary.breakout_20d === true ||
      primary.breakout_52w === true

    const hasHeavyVolume =
      (primary.volume_ratio ?? 0) >= 2 || (scoreBreakdown.candidate_volume || 0) > 0

    const hasEliteInsiderSupport =
      (scoreBreakdown.insider_buying || 0) >= 14 || (primary.cluster_buyers ?? 0) >= 2

    if (!(hasBreakoutSupport && hasHeavyVolume && positivePillars >= 4)) {
      stackedScore = Math.min(stackedScore, 97)
      scoreCapsApplied.add("stacked-elite-confirmation-cap")
    }

    if (
      !(hasBreakoutSupport &&
        hasHeavyVolume &&
        positivePillars >= 4 &&
        (hasEliteInsiderSupport || Number(primary.app_score || 0) >= 97))
    ) {
      stackedScore = Math.min(stackedScore, 99)
      scoreCapsApplied.add("stacked-no-perfect-score-cap")
    }

    const finalScore = clamp(Math.round(stackedScore), 0, 100)

    if (finalScore < MIN_TICKER_APP_SCORE) continue

    rows.push({
      ticker,
      company_name: primary.company_name,
      business_description: primary.business_description,
      app_score: finalScore,
      raw_score: finalScore,
      bias: "Bullish",
      board_bucket: "Buy",
      signal_strength_bucket: getStrengthBucket(finalScore),
      score_version: SCORE_VERSION,
      score_updated_at: runTimestamp,
      stacked_signal_count: sorted.length,
      score_breakdown: scoreBreakdown,
      signal_reasons: Array.from(signalReasons).slice(0, 12),
      score_caps_applied: Array.from(scoreCapsApplied),
      signal_tags: Array.from(signalTags),
      primary_signal_key: primary.signal_key,
      primary_signal_type: primary.signal_type,
      primary_signal_source: primary.signal_source,
      primary_signal_category: primary.signal_category,
      primary_title: primary.title,
      primary_summary: primary.summary,
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

  if (error) {
    return currentRows
  }

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

type SharedCaches = {
  snapshotCache: Map<string, Promise<TickerSnapshot>>
  priceCache: Map<string, Promise<PriceConfirmation>>
  earningsCache: Map<string, Promise<EarningsSignal>>
  catalystCache: Map<string, Promise<string | null>>
}

async function getCachedSnapshot(caches: SharedCaches, ticker: string) {
  if (!caches.snapshotCache.has(ticker)) {
    caches.snapshotCache.set(ticker, getTickerSnapshot(ticker))
  }
  return await caches.snapshotCache.get(ticker)!
}

async function getCachedPrice(caches: SharedCaches, ticker: string) {
  if (!caches.priceCache.has(ticker)) {
    caches.priceCache.set(ticker, getPriceConfirmation(ticker))
  }
  return await caches.priceCache.get(ticker)!
}

async function getCachedEarnings(caches: SharedCaches, ticker: string) {
  if (!caches.earningsCache.has(ticker)) {
    caches.earningsCache.set(ticker, getEarningsSignal(ticker))
  }
  return await caches.earningsCache.get(ticker)!
}

async function getCachedCatalyst(caches: SharedCaches, filing: RawFiling) {
  const key = filing.accession_no
  if (!caches.catalystCache.has(key)) {
    caches.catalystCache.set(
      key,
      (async () => {
        const filingText = await fetchFilingText(filing.filing_url)
        return classify8kEvent(filingText)
      })()
    )
  }
  return await caches.catalystCache.get(key)!
}

type FilingPreparationResult =
  | { kind: "prepared"; item: PreparedSignal }
  | { kind: "unsupported"; form: string }
  | { kind: "skip"; reason: "noTicker" | "noBaseSignal" | "notBullishEnough" }

async function prepareFilingSignal(
  filing: RawFiling,
  caches: SharedCaches,
  candidateByTicker: Map<string, CandidateUniverseSignalInput>
): Promise<FilingPreparationResult> {
  const ticker = normalizeTicker(filing.ticker)
  if (!ticker) {
    return { kind: "skip", reason: "noTicker" }
  }

  const [price, snapshot, earnings] = await Promise.all([
    getCachedPrice(caches, ticker),
    getCachedSnapshot(caches, ticker),
    getCachedEarnings(caches, ticker),
  ])

  let base = baseSignal(filing.form_type)
  if (!base) {
    base = maybeCreateEarningsBreakoutBase({ ...filing, ticker }, price, earnings)
  }

  if (!base) {
    const form = normalizeFormType(filing.form_type) || "UNKNOWN"
    return { kind: "unsupported", form }
  }

  let insider: InsiderParseResult = {
    action: null,
    shares: null,
    avgPrice: null,
    role: null,
    insiderName: null,
  }

  const form = normalizeFormType(filing.form_type)

  if (form === "4" || form === "4/A") {
    insider = await parseForm4({ ...filing, ticker })
  }

  let catalystType: string | null = null
  if (form === "8-K" || form === "6-K") {
    catalystType = await getCachedCatalyst(caches, filing)
  }

  if ((form === "4" || form === "4/A") && insider.action !== "Buy") {
    return { kind: "skip", reason: "notBullishEnough" }
  }

  if ((form === "8-K" || form === "6-K") && !isPositive8kCatalyst(catalystType)) {
    return { kind: "skip", reason: "notBullishEnough" }
  }

  return {
    kind: "prepared",
    item: {
      filing: { ...filing, ticker },
      base,
      insider,
      price,
      snapshot,
      earnings,
      catalystType,
      candidate: candidateByTicker.get(ticker) ?? null,
    },
  }
}

async function prepareCandidateSignal(
  candidate: CandidateUniverseSignalInput,
  caches: SharedCaches
): Promise<PreparedSignal | null> {
  const ticker = normalizeTicker(candidate.ticker)
  if (!ticker) return null

  const [price, snapshot, earnings] = await Promise.all([
    getCachedPrice(caches, ticker),
    getCachedSnapshot(caches, ticker),
    getCachedEarnings(caches, ticker),
  ])

  const syntheticFiling = buildSyntheticCandidateFiling(candidate, new Date().toISOString())
  const base = maybeCreateCandidateTechnicalBase(candidate, price, earnings)
  if (!base) return null

  return {
    filing: syntheticFiling,
    base,
    insider: {
      action: null,
      shares: null,
      avgPrice: null,
      role: null,
      insiderName: null,
    },
    price,
    snapshot,
    earnings,
    catalystType: null,
    candidate,
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

    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )

    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)

    const candidateCutoffDate = new Date(now)
    candidateCutoffDate.setDate(candidateCutoffDate.getDate() - CANDIDATE_SIGNAL_LOOKBACK_DAYS)
    const candidateCutoffDateString = candidateCutoffDate.toISOString()

    const diagnostics: Diagnostics = {
      scanned: 0,
      skippedNoTicker: 0,
      skippedNoBaseSignal: 0,
      skippedNotBullishEnough: 0,
      skippedFailedEligibility: 0,
      preparedCount: 0,
      enhancedNull: 0,
      filteredByMinScore: 0,
      filingSignalsBuilt: 0,
      filingSignalsInserted: 0,
      tickerCurrentBuilt: 0,
      tickerCurrentInserted: 0,
      tickerHistoryInserted: 0,
      tickerCurrentBuiltFromSignalsTable: 0,
      unsupportedForms: {},
      candidateRowsLoaded: 0,
      candidateTechnicalSignalsBuilt: 0,
      candidateTechnicalSignalsInserted: 0,
    }

    const [{ data: filings, error }, { data: candidateRows, error: candidateError }] =
      await Promise.all([
        supabase
          .from("raw_filings")
          .select(
            "ticker, company_name, form_type, filed_at, filing_url, accession_no, cik, primary_doc, fetched_at"
          )
          .gte("filed_at", cutoffDateString)
          .order("filed_at", { ascending: false })
          .limit(limit),
        supabase
          .from("candidate_universe")
          .select(
            "ticker, cik, name, price, market_cap, avg_volume_20d, avg_dollar_volume_20d, one_day_return, return_5d, return_10d, return_20d, volume_ratio, breakout_20d, breakout_10d, above_sma_20, breakout_clearance_pct, extension_from_sma20_pct, close_in_day_range, catalyst_count, candidate_score, included, screen_reason, last_screened_at"
          )
          .or(`included.eq.true,candidate_score.gte.${MIN_CANDIDATE_SCORE_FOR_SYNTHETIC_SIGNAL}`)
          .gte("last_screened_at", candidateCutoffDateString)
          .order("candidate_score", { ascending: false })
          .limit(limit),
      ])

    if (error) {
      return Response.json({ ok: false, error: error.message }, { status: 500 })
    }

    if (candidateError) {
      return Response.json({ ok: false, error: candidateError.message }, { status: 500 })
    }

    diagnostics.scanned = filings?.length || 0
    diagnostics.candidateRowsLoaded = candidateRows?.length || 0

    const caches: SharedCaches = {
      snapshotCache: new Map(),
      priceCache: new Map(),
      earningsCache: new Map(),
      catalystCache: new Map(),
    }

    const candidateByTicker = new Map<string, CandidateUniverseSignalInput>()
    for (const candidate of (candidateRows || []) as CandidateUniverseSignalInput[]) {
      const ticker = normalizeTicker(candidate.ticker)
      if (!ticker) continue
      candidateByTicker.set(ticker, { ...candidate, ticker })
    }

    const filingPreparations = await mapWithConcurrency(
      (filings || []) as RawFiling[],
      PREPARE_CONCURRENCY,
      async (filing) => prepareFilingSignal(filing, caches, candidateByTicker)
    )

    const prepared: PreparedSignal[] = []
    const preparedKeys = new Set<string>()

    for (const result of filingPreparations) {
      if (result.kind === "prepared") {
        prepared.push(result.item)
        preparedKeys.add(`filing:${result.item.filing.accession_no}`)
        continue
      }

      if (result.kind === "unsupported") {
        diagnostics.skippedNoBaseSignal += 1
        diagnostics.unsupportedForms[result.form] =
          (diagnostics.unsupportedForms[result.form] || 0) + 1
        continue
      }

      if (result.reason === "noTicker") diagnostics.skippedNoTicker += 1
      else if (result.reason === "noBaseSignal") diagnostics.skippedNoBaseSignal += 1
      else if (result.reason === "notBullishEnough") diagnostics.skippedNotBullishEnough += 1
    }

    const candidatePreparations = await mapWithConcurrency(
      (candidateRows || []) as CandidateUniverseSignalInput[],
      PREPARE_CONCURRENCY,
      async (candidate) => prepareCandidateSignal(candidate, caches)
    )

    for (const item of candidatePreparations) {
      if (!item) continue
      const preparedKey = `candidate:${item.filing.accession_no}`
      if (preparedKeys.has(preparedKey)) continue
      prepared.push(item)
      preparedKeys.add(preparedKey)
    }

    diagnostics.preparedCount = prepared.length

    const clusterMap = buildClusterMap(prepared)
    const signalRows: any[] = []
    const historyRows: any[] = []

    for (const item of prepared) {
      const clusterInfo = clusterMap.get(item.filing.accession_no) ?? null
      const ageDays = daysBetween(item.filing.filed_at)

      const eligible = isEligibleStrongBuySignal({
        filing: item.filing,
        base: item.base,
        insider: item.insider,
        price: item.price,
        earnings: item.earnings,
        catalystType: item.catalystType,
        candidate: item.candidate,
        ageDays,
        clusterInfo,
      })

      if (!eligible) {
        diagnostics.skippedFailedEligibility += 1
        continue
      }

      const enhanced = applyEnhancements(
        item.filing,
        item.base,
        item.insider,
        item.price,
        item.snapshot,
        clusterInfo,
        item.earnings,
        item.catalystType,
        item.candidate
      )

      if (!enhanced) {
        diagnostics.enhancedNull += 1
        continue
      }

      if (enhanced.app_score < MIN_SIGNAL_APP_SCORE) {
        diagnostics.filteredByMinScore += 1
        continue
      }

      const signalKey = buildSignalKey(item.filing, enhanced.signal_source)

      const signalRow = {
        signal_key: signalKey,
        ticker: item.filing.ticker,
        company_name:
          item.filing.company_name || item.snapshot.companyName || item.candidate?.name || null,
        business_description: item.snapshot.businessDescription,
        pe_ratio: round2(item.snapshot.peRatio),
        pe_forward: round2(item.snapshot.forwardPe),
        pe_type: item.snapshot.peType,
        signal_type: enhanced.signal_type,
        signal_source: enhanced.signal_source,
        signal_category: enhanced.signal_category,
        signal_strength_bucket: enhanced.signal_strength_bucket,
        signal_tags: enhanced.signal_tags,
        catalyst_type: item.catalystType,
        bias: enhanced.bias,
        score: enhanced.score,
        app_score: enhanced.app_score,
        board_bucket: enhanced.board_bucket,
        title: enhanced.title,
        summary: enhanced.summary,
        source_form: item.filing.form_type,
        filed_at: item.filing.filed_at,
        filing_url: item.filing.filing_url,
        accession_no: item.filing.accession_no,
        insider_action: item.insider.action,
        insider_shares: roundWhole(item.insider.shares),
        insider_avg_price: round2(item.insider.avgPrice),
        insider_buy_value: roundWhole(enhanced.insiderBuyValue),
        insider_signal_flavor: enhanced.insiderSignalFlavor,
        cluster_buyers: clusterInfo?.clusterSize ?? null,
        cluster_shares: roundWhole(clusterInfo?.totalShares ?? null),
        price_return_5d: round2(item.price.return5d),
        price_return_20d: round2(item.price.return20d),
        volume_ratio: round2(item.price.volumeRatio),
        breakout_20d: item.price.breakout20d || item.candidate?.breakout_20d === true,
        breakout_52w: item.price.breakout52w,
        above_50dma: item.price.above50dma,
        trend_aligned: item.price.trendAligned || item.candidate?.above_sma_20 === true,
        price_confirmed: item.price.confirmed || item.candidate?.included === true,
        earnings_surprise_pct: round2(item.earnings.surprisePct),
        revenue_growth_pct: round2(item.earnings.revenueGrowthPct),
        guidance_flag: item.earnings.guidanceFlag,
        market_cap: roundWhole(item.snapshot.marketCap ?? item.candidate?.market_cap),
        sector: item.snapshot.sector,
        industry: item.snapshot.industry,
        relative_strength_20d: round2(item.price.relativeStrength20d),
        age_days: enhanced.ageDays,
        last_scored_at: runTimestamp,
        updated_at: runTimestamp,
        score_breakdown: enhanced.score_breakdown,
        score_version: SCORE_VERSION,
        score_updated_at: runTimestamp,
        stacked_signal_count: enhanced.stacked_signal_count,
        freshness_bucket: enhanced.freshness_bucket,
        signal_reasons: enhanced.signal_reasons,
        score_caps_applied: enhanced.score_caps_applied,
        ticker_score_change_1d: null,
        ticker_score_change_7d: null,
      }

      signalRows.push(signalRow)

      if (enhanced.signal_source === "breakout") {
        diagnostics.candidateTechnicalSignalsBuilt += 1
      }

      historyRows.push(buildSignalHistoryRow(signalRow, runDate, runTimestamp))
    }

    diagnostics.filingSignalsBuilt = signalRows.length

    if (signalRows.length > 0) {
      await upsertInChunks(supabase.from("signals"), signalRows, "signal_key")
    }

    diagnostics.filingSignalsInserted = signalRows.length
    diagnostics.candidateTechnicalSignalsInserted = diagnostics.candidateTechnicalSignalsBuilt

    if (historyRows.length > 0) {
      await upsertInChunks(supabase.from("signal_history"), historyRows, "signal_history_key")
    }

    if (runRetention) {
      const { error: staleSignalsCleanupError } = await supabase
        .from("signals")
        .delete()
        .lt("filed_at", cutoffDateString)

      if (staleSignalsCleanupError) {
        return Response.json(
          {
            ok: false,
            error: staleSignalsCleanupError.message,
            diagnostics,
          },
          { status: 500 }
        )
      }
    }

    const { data: allSignalRows, error: allSignalsError } = await supabase
      .from("signals")
      .select("*")
      .gte("filed_at", cutoffDateString)
      .gte("app_score", MIN_SIGNAL_APP_SCORE)
      .order("app_score", { ascending: false })
      .order("filed_at", { ascending: false })

    if (allSignalsError) {
      return Response.json(
        {
          ok: false,
          error: allSignalsError.message,
          diagnostics,
        },
        { status: 500 }
      )
    }

    diagnostics.tickerCurrentBuiltFromSignalsTable = (allSignalRows || []).length

    let tickerCurrentRows = buildTickerScoresCurrentRows((allSignalRows || []) as any[], runTimestamp)
    diagnostics.tickerCurrentBuilt = tickerCurrentRows.length

    tickerCurrentRows = await attachTickerScoreChangesToCurrentRows(supabase, tickerCurrentRows, runDate)

    if (tickerCurrentRows.length > 0) {
      await upsertInChunks(supabase.from("ticker_scores_current"), tickerCurrentRows, "ticker")
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
          diagnostics,
        },
        { status: 500 }
      )
    }

    const staleTickerList = uniqueStrings(
      (existingTickerRows || [])
        .map((row: any) => normalizeTicker(row.ticker))
        .filter((ticker) => !currentTickerSet.has(ticker))
    )

    if (staleTickerList.length > 0) {
      await deleteInChunksByTicker(supabase.from("ticker_scores_current"), staleTickerList)
    }

    diagnostics.tickerCurrentInserted = tickerCurrentRows.length

    const tickerHistoryRows = buildTickerScoreHistoryRows(tickerCurrentRows, runDate, runTimestamp)

    if (tickerHistoryRows.length > 0) {
      await upsertInChunks(
        supabase.from("ticker_score_history"),
        tickerHistoryRows,
        "ticker,score_date"
      )
    }

    diagnostics.tickerHistoryInserted = tickerHistoryRows.length

    let retentionMessage = "skipped"
    let tickerRetentionMessage = "skipped"

    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("signal_history")
        .delete()
        .lt("scored_on", retentionCutoffString)

      const { error: tickerRetentionError } = await supabase
        .from("ticker_score_history")
        .delete()
        .lt("score_date", retentionCutoffString)

      retentionMessage = retentionError ? retentionError.message : "ok"
      tickerRetentionMessage = tickerRetentionError ? tickerRetentionError.message : "ok"
    }

    let strongBuyCount: number | null = null
    let eliteBuyCount: number | null = null

    if (includeCounts) {
      const [{ count: strongBuy }, { count: eliteBuy }] = await Promise.all([
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 90),
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 97),
      ])

      strongBuyCount = strongBuy ?? 0
      eliteBuyCount = eliteBuy ?? 0
    }

    return Response.json({
      ok: true,
      scanned: filings?.length || 0,
      candidateRowsLoaded: candidateRows?.length || 0,
      filingSignalsInserted: signalRows.length,
      candidateTechnicalSignalsInserted: diagnostics.candidateTechnicalSignalsInserted,
      historyInserted: historyRows.length,
      tickerCurrentInserted: tickerCurrentRows.length,
      tickerHistoryInserted: tickerHistoryRows.length,
      limit,
      lookbackDays,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      minTickerAppScore: MIN_TICKER_APP_SCORE,
      retentionCleanup: retentionMessage,
      tickerRetentionCleanup: tickerRetentionMessage,
      staleSignalsCleanup: runRetention ? "ok" : "skipped",
      strongBuyCount,
      eliteBuyCount,
      diagnostics,
      message:
        "Strong-buy-only signals generated from filings and technical candidate setups, with ticker-level grading, history, and optional freshness cleanup.",
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