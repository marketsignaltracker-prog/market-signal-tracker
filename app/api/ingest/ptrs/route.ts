import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type Scope = "all" | "eligible" | "candidates"

type RawPtrTradeRow = {
  disclosure_source: string
  filer_name: string
  chamber: string | null
  district_or_state: string | null
  report_date: string | null
  transaction_date: string | null
  ticker: string | null
  asset_name: string | null
  asset_type: string | null
  action: string | null
  amount_range: string | null
  amount_low: number | null
  amount_high: number | null
  owner: string | null
  ptr_url: string | null
  raw_document_url: string | null
  trade_key: string
  fetched_at: string
  updated_at: string

  // New-model compatibility columns
  politician_name: string
  transaction_type: "buy" | "sell" | "exchange" | "unknown"
  trade_date: string | null
  disclosure_date: string | null
  source_url: string | null
  ptr_id: string
}

type CompanyRow = {
  id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
  passed?: boolean | null
  as_of_date?: string | null
}

type CandidateScreenHistoryRow = {
  company_id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  passed?: boolean | null
  as_of_date?: string | null
  is_active?: boolean | null
}

type SourceRow = CompanyRow | CandidateUniverseRow | CandidateScreenHistoryRow

type AInvestTradeRow = {
  ticker?: string | null
  symbol?: string | null
  stock_symbol?: string | null
  stockTicker?: string | null
  code?: string | null
  security_code?: string | null
  securityCode?: string | null
  asset_symbol?: string | null
  assetSymbol?: string | null

  name?: string
  politician?: string
  filer?: string
  member?: string
  party?: string
  state?: string
  district?: string
  chamber?: string

  trade_date?: string
  transactionDate?: string
  transaction_date?: string

  filing_date?: string
  disclosure_date?: string
  disclosureDate?: string
  reportDate?: string
  report_date?: string

  reporting_gap?: string

  trade_type?: string
  transaction_type?: string
  type?: string
  action?: string

  size?: string
  amount?: string
  amount_range?: string
  amountLow?: number | string | null
  amountHigh?: number | string | null
  amount_low?: number | string | null
  amount_high?: number | string | null

  link?: string | null
  url?: string | null
  source_url?: string | null
  sourceUrl?: string | null

  owner?: string | null
  asset?: string | null
  asset_name?: string | null
  assetType?: string | null
  asset_type?: string | null
}

type AInvestResponse = {
  data?:
    | AInvestTradeRow[]
    | {
        data?: AInvestTradeRow[]
        rows?: AInvestTradeRow[]
        list?: AInvestTradeRow[]
        items?: AInvestTradeRow[]
      }
  result?: AInvestTradeRow[]
  rows?: AInvestTradeRow[]
  list?: AInvestTradeRow[]
  items?: AInvestTradeRow[]
  status_code?: number
  status_msg?: string
  message?: string
}

type PageFetchResult = {
  page: number
  ok: boolean
  rows: RawPtrTradeRow[]
  upstreamCount: number
  error: string | null
}

type Diagnostics = {
  scope: Scope
  companiesRowsLoaded: number
  candidateUniverseRowsLoaded: number
  candidateScreenHistoryRowsLoaded: number
  sourceRowsLoaded: number
  fallbackCandidateHistoryUsed: boolean

  pagesRequested: number
  pagesSucceeded: number
  pagesFailed: number
  pageSize: number
  maxPages: number

  upstreamRows: number
  normalizedRows: number
  dedupedRows: number
  insertedOrUpdated: number

  stoppedBecauseShortPage: boolean
  stoppedBecauseMaxPages: boolean
  allowlistTickerCount: number
}

const DB_CHUNK_SIZE = 200
const API_TIMEOUT_MS = 15000
const DEFAULT_BATCH = 50
const MAX_BATCH = 100
const DEFAULT_START = 0

const DEFAULT_PAGE_SIZE = 100
const MAX_PAGE_SIZE = 200
const DEFAULT_MAX_PAGES = 5
const MAX_MAX_PAGES = 25

const CANDIDATE_LOOKBACK_DAYS = 60
const AINVEST_CONGRESS_URL = "https://openapi.ainvest.com/open/ownership/congress"

function getSupabaseAdmin() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error("Missing Supabase environment variables")
  }

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  })
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (!value || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function normalizeTicker(ticker: string | null | undefined) {
  const t = (ticker || "").trim().toUpperCase()
  return t || null
}

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(String(value).replace(/[$,]/g, "").trim())
  return Number.isFinite(n) ? n : null
}

function normalizeDate(value: unknown): string | null {
  const raw = String(value ?? "").trim()
  if (!raw) return null

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (isoMatch) return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`

  const parsed = new Date(raw)
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10)
  }

  return null
}

function parseAmountRange(range: string | null) {
  if (!range) {
    return {
      amount_low: null,
      amount_high: null,
    }
  }

  const cleaned = range.replace(/\s+/g, " ").trim()

  const commonMap: Record<string, { low: number; high: number | null }> = {
    "$1,001 - $15,000": { low: 1001, high: 15000 },
    "$15,001 - $50,000": { low: 15001, high: 50000 },
    "$50,001 - $100,000": { low: 50001, high: 100000 },
    "$100,001 - $250,000": { low: 100001, high: 250000 },
    "$250,001 - $500,000": { low: 250001, high: 500000 },
    "$500,001 - $1,000,000": { low: 500001, high: 1000000 },
    "$1,000,001 - $5,000,000": { low: 1000001, high: 5000000 },
    "$5,000,001 - $25,000,000": { low: 5000001, high: 25000000 },
    "$25,000,001 - $50,000,000": { low: 25000001, high: 50000000 },
    "$50,000,001+": { low: 50000001, high: null },
  }

  if (commonMap[cleaned]) {
    return {
      amount_low: commonMap[cleaned].low,
      amount_high: commonMap[cleaned].high,
    }
  }

  const rangeMatch = cleaned.match(/\$?([\d,]+)\s*-\s*\$?([\d,]+)/)
  if (rangeMatch) {
    return {
      amount_low: Number(rangeMatch[1].replace(/,/g, "")) || null,
      amount_high: Number(rangeMatch[2].replace(/,/g, "")) || null,
    }
  }

  const plusMatch = cleaned.match(/\$?([\d,]+)\s*\+/)
  if (plusMatch) {
    return {
      amount_low: Number(plusMatch[1].replace(/,/g, "")) || null,
      amount_high: null,
    }
  }

  return {
    amount_low: null,
    amount_high: null,
  }
}

function normalizeAction(value: string | null | undefined) {
  const v = (value || "").trim().toLowerCase()
  if (!v) return null

  if (v.includes("buy") || v.includes("purchase")) return "Buy"
  if (v.includes("sell") || v.includes("sale")) return "Sell"
  if (v.includes("exchange")) return "Exchange"

  return value || null
}

function normalizeTransactionType(
  value: string | null | undefined
): "buy" | "sell" | "exchange" | "unknown" {
  const v = (value || "").trim().toLowerCase()

  if (!v) return "unknown"
  if (v.includes("buy") || v.includes("purchase")) return "buy"
  if (v.includes("sell") || v.includes("sale")) return "sell"
  if (v.includes("exchange")) return "exchange"

  return "unknown"
}

function buildTradeKey(row: {
  filer: string
  ticker: string | null
  transaction_date: string | null
  action: string | null
  amount_range: string | null
  report_date?: string | null
  ptr_url?: string | null
}) {
  return [
    row.filer.toLowerCase(),
    row.ticker || "unknown",
    row.transaction_date || "unknown",
    row.action || "unknown",
    row.amount_range || "unknown",
    row.report_date || "unknown",
    row.ptr_url || "unknown",
  ].join("::")
}

async function fetchWithTimeout(url: string, token: string) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), API_TIMEOUT_MS)

  try {
    return await fetch(url, {
      method: "GET",
      headers: {
        "x-AINVEST_API_TOKEN": token,
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
      cache: "no-store",
      signal: controller.signal,
    })
  } finally {
    clearTimeout(timeout)
  }
}

async function upsertRows(supabase: any, rows: RawPtrTradeRow[]) {
  const chunks = chunkArray(rows, DB_CHUNK_SIZE)
  let inserted = 0

  for (const chunk of chunks) {
    const { error } = await supabase.from("raw_ptr_trades").upsert(chunk, {
      onConflict: "trade_key",
    })

    if (error) {
      throw error
    }

    inserted += chunk.length
  }

  return inserted
}

async function loadCompaniesContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<{
  rows: CompanyRow[]
  companiesRowsLoaded: number
}> {
  let query = supabase
    .from("companies")
    .select("id, ticker, cik, name, is_active")
    .not("ticker", "is", null)
    .order("id", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) throw error

  return {
    rows: (data || []) as CompanyRow[],
    companiesRowsLoaded: (data || []).length,
  }
}

async function loadEligibleContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<{
  rows: CandidateUniverseRow[]
  candidateUniverseRowsLoaded: number
}> {
  let query = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, passed, as_of_date")
    .eq("passed", true)
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) throw error

  return {
    rows: (data || []) as CandidateUniverseRow[],
    candidateUniverseRowsLoaded: (data || []).length,
  }
}

async function loadCandidatesContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<{
  candidateRows: SourceRow[]
  candidateUniverseRowsLoaded: number
  candidateScreenHistoryRowsLoaded: number
  fallbackCandidateHistoryUsed: boolean
}> {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - CANDIDATE_LOOKBACK_DAYS)

  let universeQuery = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, passed, as_of_date")
    .eq("passed", true)
    .gte("as_of_date", cutoff.toISOString())
    .not("ticker", "is", null)
    .order("as_of_date", { ascending: false })
    .range(start, start + batch - 1)

  if (onlyActive) {
    universeQuery = universeQuery.eq("is_active", true)
  }

  const universeResult = await universeQuery
  if (universeResult.error) throw universeResult.error

  const universeRows = (universeResult.data || []) as CandidateUniverseRow[]

  if (universeRows.length >= Math.min(25, batch)) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateScreenHistoryRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,
    }
  }

  const latestSnapshot = await supabase
    .from("candidate_screen_history")
    .select("as_of_date")
    .order("as_of_date", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (latestSnapshot.error) throw latestSnapshot.error

  const latestAsOfDate = latestSnapshot.data?.as_of_date ?? null
  if (!latestAsOfDate) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateScreenHistoryRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,
    }
  }

  let historyQuery = supabase
    .from("candidate_screen_history")
    .select("company_id, ticker, cik, name, passed, as_of_date, is_active")
    .eq("as_of_date", latestAsOfDate)
    .eq("passed", true)
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    historyQuery = historyQuery.eq("is_active", true)
  }

  const historyResult = await historyQuery
  if (historyResult.error) throw historyResult.error

  const historyRows = (historyResult.data || []) as CandidateScreenHistoryRow[]

  const deduped = new Map<string, SourceRow>()

  for (const row of universeRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    deduped.set(ticker, row)
  }

  for (const row of historyRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!deduped.has(ticker)) {
      deduped.set(ticker, row)
    }
  }

  return {
    candidateRows: [...deduped.values()].slice(0, batch),
    candidateUniverseRowsLoaded: universeRows.length,
    candidateScreenHistoryRowsLoaded: historyRows.length,
    fallbackCandidateHistoryUsed: historyRows.length > 0,
  }
}

function extractTradeArray(parsed: AInvestResponse): AInvestTradeRow[] {
  if (Array.isArray(parsed?.data)) return parsed.data
  if (Array.isArray(parsed?.result)) return parsed.result
  if (Array.isArray(parsed?.rows)) return parsed.rows
  if (Array.isArray(parsed?.list)) return parsed.list
  if (Array.isArray(parsed?.items)) return parsed.items

  if (parsed?.data && typeof parsed.data === "object") {
    if (Array.isArray(parsed.data.data)) return parsed.data.data
    if (Array.isArray(parsed.data.rows)) return parsed.data.rows
    if (Array.isArray(parsed.data.list)) return parsed.data.list
    if (Array.isArray(parsed.data.items)) return parsed.data.items
  }

  return []
}

function extractTickerFromTrade(row: AInvestTradeRow) {
  return normalizeTicker(
    row.ticker ||
      row.symbol ||
      row.stock_symbol ||
      row.stockTicker ||
      row.code ||
      row.security_code ||
      row.securityCode ||
      row.asset_symbol ||
      row.assetSymbol
  )
}

function normalizeAInvestTrade(
  row: AInvestTradeRow,
  fetchedAt: string
): RawPtrTradeRow | null {
  const ticker = extractTickerFromTrade(row)
  if (!ticker) return null

  const filerName = String(
    row.name || row.politician || row.filer || row.member || ""
  ).trim()

  if (!filerName) return null

  const transactionDate = normalizeDate(
    row.trade_date || row.transactionDate || row.transaction_date
  )
  const reportDate = normalizeDate(
    row.disclosure_date ||
      row.disclosureDate ||
      row.filing_date ||
      row.reportDate ||
      row.report_date
  )

  const rawAction =
    row.transaction_type || row.trade_type || row.type || row.action || null

  const normalizedAction = normalizeAction(rawAction)
  const transactionType = normalizeTransactionType(rawAction)

  const amountRange =
    String(row.size || row.amount || row.amount_range || "").trim() || null

  const amounts = parseAmountRange(amountRange)
  const ptrUrl = row.link || row.url || row.source_url || row.sourceUrl || null

  const tradeKey = buildTradeKey({
    filer: filerName,
    ticker,
    transaction_date: transactionDate,
    action: normalizedAction,
    amount_range: amountRange,
    report_date: reportDate,
    ptr_url: ptrUrl,
  })

  return {
    disclosure_source: "AINVEST",
    filer_name: filerName,
    chamber: row.chamber || null,
    district_or_state: row.district || row.state || null,
    report_date: reportDate,
    transaction_date: transactionDate,
    ticker,
    asset_name: row.asset_name || row.asset || ticker,
    asset_type: row.asset_type || row.assetType || "Stock",
    action: normalizedAction,
    amount_range: amountRange,
    amount_low: safeNumber(row.amountLow) ?? safeNumber(row.amount_low) ?? amounts.amount_low,
    amount_high: safeNumber(row.amountHigh) ?? safeNumber(row.amount_high) ?? amounts.amount_high,
    owner: row.owner || null,
    ptr_url: ptrUrl,
    raw_document_url: ptrUrl,
    trade_key: tradeKey,
    fetched_at: fetchedAt,
    updated_at: fetchedAt,

    politician_name: filerName,
    transaction_type: transactionType,
    trade_date: transactionDate,
    disclosure_date: reportDate,
    source_url: ptrUrl,
    ptr_id: tradeKey,
  }
}

function buildAllowlistFromSourceRows(rows: SourceRow[]) {
  const out = new Set<string>()
  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (ticker) out.add(ticker)
  }
  return out
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const ainvestToken = process.env.AINVEST_API_TOKEN?.trim()
  if (!ainvestToken) {
    return Response.json(
      { ok: false, error: "Missing AINVEST_API_TOKEN environment variable" },
      { status: 500 }
    )
  }

  try {
    const supabase = getSupabaseAdmin()
    const { searchParams } = new URL(request.url)

    const scopeParam = (searchParams.get("scope") || "all").toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), DEFAULT_START))
    const batch = Math.min(
      Math.max(1, parseInteger(searchParams.get("batch"), DEFAULT_BATCH)),
      MAX_BATCH
    )

    const pageSize = Math.min(
      Math.max(1, parseInteger(searchParams.get("size"), DEFAULT_PAGE_SIZE)),
      MAX_PAGE_SIZE
    )

    const maxPages = Math.min(
      Math.max(1, parseInteger(searchParams.get("maxPages"), DEFAULT_MAX_PAGES)),
      MAX_MAX_PAGES
    )

    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"
    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"

    if (!["all", "eligible", "candidates"].includes(scopeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, candidates`,
        },
        { status: 400 }
      )
    }

    const scope = scopeParam as Scope

    const diagnostics: Diagnostics = {
      scope,
      companiesRowsLoaded: 0,
      candidateUniverseRowsLoaded: 0,
      candidateScreenHistoryRowsLoaded: 0,
      sourceRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,

      pagesRequested: 0,
      pagesSucceeded: 0,
      pagesFailed: 0,
      pageSize,
      maxPages,

      upstreamRows: 0,
      normalizedRows: 0,
      dedupedRows: 0,
      insertedOrUpdated: 0,

      stoppedBecauseShortPage: false,
      stoppedBecauseMaxPages: false,
      allowlistTickerCount: 0,
    }

    let sourceRows: SourceRow[] = []
    let allowlist: Set<string> | null = null

    if (scope === "all") {
      const allContext = await loadCompaniesContext(supabase, start, batch, onlyActive)
      sourceRows = allContext.rows
      diagnostics.companiesRowsLoaded = allContext.companiesRowsLoaded
      diagnostics.sourceRowsLoaded = allContext.rows.length
      allowlist = null
    }

    if (scope === "eligible") {
      const eligibleContext = await loadEligibleContext(supabase, start, batch, onlyActive)
      sourceRows = eligibleContext.rows
      diagnostics.candidateUniverseRowsLoaded = eligibleContext.candidateUniverseRowsLoaded
      diagnostics.sourceRowsLoaded = eligibleContext.rows.length
      allowlist = buildAllowlistFromSourceRows(sourceRows)
    }

    if (scope === "candidates") {
      const candidateContext = await loadCandidatesContext(supabase, start, batch, onlyActive)
      sourceRows = candidateContext.candidateRows
      diagnostics.candidateUniverseRowsLoaded = candidateContext.candidateUniverseRowsLoaded
      diagnostics.candidateScreenHistoryRowsLoaded =
        candidateContext.candidateScreenHistoryRowsLoaded
      diagnostics.fallbackCandidateHistoryUsed = candidateContext.fallbackCandidateHistoryUsed
      diagnostics.sourceRowsLoaded = candidateContext.candidateRows.length
      allowlist = buildAllowlistFromSourceRows(sourceRows)
    }

    diagnostics.allowlistTickerCount = allowlist?.size ?? 0

    const fetchedAt = new Date().toISOString()
    const pageDiagnostics: Array<{
      page: number
      ok: boolean
      upstreamCount: number
      normalizedRows: number
      keptRows: number
      error: string | null
    }> = []

    const allRows: RawPtrTradeRow[] = []

    for (let page = 1; page <= maxPages; page += 1) {
      diagnostics.pagesRequested += 1

      const url = new URL(AINVEST_CONGRESS_URL)
      url.searchParams.set("page", String(page))
      url.searchParams.set("size", String(pageSize))

      let upstreamRows: AInvestTradeRow[] = []

      try {
        const res = await fetchWithTimeout(url.toString(), ainvestToken)
        const contentType = res.headers.get("content-type") || ""
        const rawBody = await res.text()

        if (!res.ok) {
          let message = rawBody.slice(0, 300)

          if (contentType.includes("application/json")) {
            try {
              const parsedError = JSON.parse(rawBody)
              message =
                parsedError?.message ||
                parsedError?.status_msg ||
                JSON.stringify(parsedError).slice(0, 300)
            } catch {
              // keep raw fallback
            }
          }

          diagnostics.pagesFailed += 1
          pageDiagnostics.push({
            page,
            ok: false,
            upstreamCount: 0,
            normalizedRows: 0,
            keptRows: 0,
            error: `AInvest ${res.status}: ${message}`,
          })
          break
        }

        let parsed: AInvestResponse
        try {
          parsed = JSON.parse(rawBody) as AInvestResponse
        } catch {
          diagnostics.pagesFailed += 1
          pageDiagnostics.push({
            page,
            ok: false,
            upstreamCount: 0,
            normalizedRows: 0,
            keptRows: 0,
            error: `AInvest returned non-JSON response: ${rawBody.slice(0, 300)}`,
          })
          break
        }

        upstreamRows = extractTradeArray(parsed)
        diagnostics.pagesSucceeded += 1
        diagnostics.upstreamRows += upstreamRows.length

        const normalizedRows = upstreamRows
          .map((trade) => normalizeAInvestTrade(trade, fetchedAt))
          .filter((trade): trade is RawPtrTradeRow => trade !== null)

        diagnostics.normalizedRows += normalizedRows.length

        const keptRows =
          allowlist && allowlist.size > 0
            ? normalizedRows.filter((row) => {
                const ticker = normalizeTicker(row.ticker)
                return Boolean(ticker && allowlist?.has(ticker))
              })
            : normalizedRows

        allRows.push(...keptRows)

        pageDiagnostics.push({
          page,
          ok: true,
          upstreamCount: upstreamRows.length,
          normalizedRows: normalizedRows.length,
          keptRows: keptRows.length,
          error: null,
        })

        if (upstreamRows.length < pageSize) {
          diagnostics.stoppedBecauseShortPage = true
          break
        }

        if (page === maxPages) {
          diagnostics.stoppedBecauseMaxPages = true
        }
      } catch (error: any) {
        diagnostics.pagesFailed += 1
        pageDiagnostics.push({
          page,
          ok: false,
          upstreamCount: upstreamRows.length,
          normalizedRows: 0,
          keptRows: 0,
          error: error?.message || "Unknown AInvest error",
        })
        break
      }
    }

    const dedupe = new Map<string, RawPtrTradeRow>()
    for (const row of allRows) {
      if (!dedupe.has(row.trade_key)) {
        dedupe.set(row.trade_key, row)
      }
    }

    const dedupedRows = Array.from(dedupe.values())
    diagnostics.dedupedRows = dedupedRows.length

    const inserted =
      dedupedRows.length > 0 ? await upsertRows(supabase, dedupedRows) : 0

    diagnostics.insertedOrUpdated = inserted

    let ptrCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_ptr_trades")
        .select("*", { count: "exact", head: true })

      ptrCount = error ? null : count ?? 0
    }

    return Response.json({
      ok: true,
      stage: "ptrs",
      targetTable: "raw_ptr_trades",
      scope,
      start,
      batch,
      nextStart: scope === "all" || scope === "eligible" || scope === "candidates"
        ? sourceRows.length < batch
          ? null
          : start + batch
        : null,
      page: 1,
      size: pageSize,
      maxPages,
      processedTickers: sourceRows.length,
      ptrCount,
      diagnostics,
      pageDiagnostics,
      message:
        dedupedRows.length === 0
          ? "PTR route ran successfully but no normalized trades were returned from AInvest."
          : "Raw PTR trades ingested successfully using page-based congress ingestion.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown PTR ingest error",
      },
      { status: 500 }
    )
  }
}