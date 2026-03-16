import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

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

type AInvestTradeRow = {
  id?: string | number | null
  ptr_id?: string | number | null
  symbol?: string | null
  ticker?: string | null
  code?: string | null

  name?: string | null
  politician?: string | null
  filer?: string | null
  member?: string | null

  chamber?: string | null
  state?: string | null
  district?: string | null

  report_date?: string | null
  reportDate?: string | null
  disclosure_date?: string | null
  disclosureDate?: string | null
  filing_date?: string | null

  trade_date?: string | null
  tradeDate?: string | null
  transaction_date?: string | null
  transactionDate?: string | null

  action?: string | null
  trade_type?: string | null
  transaction_type?: string | null
  type?: string | null

  amount_range?: string | null
  amount?: string | null
  size?: string | null
  amountLow?: string | number | null
  amountHigh?: string | number | null
  amount_low?: string | number | null
  amount_high?: string | number | null

  asset?: string | null
  asset_name?: string | null
  assetType?: string | null
  asset_type?: string | null

  owner?: string | null
  link?: string | null
  url?: string | null
  source_url?: string | null
  raw_document_url?: string | null
}

type AInvestEnvelope = {
  code?: number | string
  message?: string
  msg?: string
  status?: number | string
  status_code?: number | string
  status_msg?: string
  data?:
    | AInvestTradeRow[]
    | {
        data?: AInvestTradeRow[]
        rows?: AInvestTradeRow[]
        list?: AInvestTradeRow[]
        items?: AInvestTradeRow[]
      }
  rows?: AInvestTradeRow[]
  list?: AInvestTradeRow[]
  items?: AInvestTradeRow[]
  result?: AInvestTradeRow[]
}

type Diagnostics = {
  pagesRequested: number
  pagesSucceeded: number
  pagesFailed: number
  pageSize: number
  maxPages: number | null
  upstreamRows: number
  normalizedRows: number
  dedupedRows: number
  insertedOrUpdated: number
  stoppedBecauseShortPage: boolean
  stoppedBecauseMaxPages: boolean
}

const AINVEST_CONGRESS_URL = "https://openapi.ainvest.com/open/ownership/congress"

const API_TIMEOUT_MS = 15000
const DB_CHUNK_SIZE = 200

const DEFAULT_PAGE = 1
const DEFAULT_PAGE_SIZE = 100
const MAX_PAGE_SIZE = 200

const DEFAULT_MAX_PAGES = 10
const MAX_MAX_PAGES = 200

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
  if (!t) return null

  const cleaned = t
    .replace(/\s+/g, "")
    .replace(/^\$/, "")
    .replace(/^\./, "")
    .replace(/[^A-Z0-9.\-]/g, "")

  return cleaned || null
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

  const usMatch = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)
  if (usMatch) {
    const mm = usMatch[1].padStart(2, "0")
    const dd = usMatch[2].padStart(2, "0")
    const yyyy = usMatch[3]
    return `${yyyy}-${mm}-${dd}`
  }

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

  return (value || "").trim() || null
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
  asset_name?: string | null
}) {
  return [
    row.filer.toLowerCase(),
    row.ticker || "unknown",
    row.transaction_date || "unknown",
    row.action || "unknown",
    row.amount_range || "unknown",
    row.report_date || "unknown",
    row.asset_name || "unknown",
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

function extractTradeArray(parsed: AInvestEnvelope): AInvestTradeRow[] {
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

function normalizeAInvestTrade(
  row: AInvestTradeRow,
  fetchedAt: string
): RawPtrTradeRow | null {
  const filerName = String(
    row.name || row.politician || row.filer || row.member || ""
  ).trim()

  if (!filerName) return null

  const ticker = normalizeTicker(row.ticker || row.symbol || row.code || null)
  const assetName = String(row.asset_name || row.asset || ticker || "").trim() || null
  const assetType = String(row.asset_type || row.assetType || "Stock").trim() || "Stock"

  const transactionDate = normalizeDate(
    row.trade_date || row.tradeDate || row.transaction_date || row.transactionDate
  )

  const reportDate = normalizeDate(
    row.disclosure_date ||
      row.disclosureDate ||
      row.report_date ||
      row.reportDate ||
      row.filing_date
  )

  const rawAction =
    row.transaction_type || row.trade_type || row.type || row.action || null

  const normalizedAction = normalizeAction(rawAction)
  const transactionType = normalizeTransactionType(rawAction)

  const amountRange =
    String(row.amount_range || row.amount || row.size || "").trim() || null

  const parsedRange = parseAmountRange(amountRange)

  const amountLow =
    safeNumber(row.amount_low) ??
    safeNumber(row.amountLow) ??
    parsedRange.amount_low

  const amountHigh =
    safeNumber(row.amount_high) ??
    safeNumber(row.amountHigh) ??
    parsedRange.amount_high

  const ptrUrl =
    String(row.source_url || row.raw_document_url || row.link || row.url || "").trim() || null

  const ptrId =
    String(row.ptr_id || row.id || "").trim() ||
    [
      filerName.toLowerCase(),
      ticker || "unknown",
      transactionDate || "unknown",
      normalizedAction || "unknown",
      amountRange || "unknown",
      reportDate || "unknown",
    ].join("::")

  const tradeKey = buildTradeKey({
    filer: filerName,
    ticker,
    transaction_date: transactionDate,
    action: normalizedAction,
    amount_range: amountRange,
    report_date: reportDate,
    ptr_url: ptrUrl,
    asset_name: assetName,
  })

  return {
    disclosure_source: "AINVEST",
    filer_name: filerName,
    chamber: row.chamber ? String(row.chamber).trim() : null,
    district_or_state: String(row.district || row.state || "").trim() || null,
    report_date: reportDate,
    transaction_date: transactionDate,
    ticker,
    asset_name: assetName,
    asset_type: assetType,
    action: normalizedAction,
    amount_range: amountRange,
    amount_low: amountLow,
    amount_high: amountHigh,
    owner: row.owner ? String(row.owner).trim() : null,
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
    ptr_id: ptrId,
  }
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

    const startPage = Math.max(
      1,
      parseInteger(searchParams.get("page"), DEFAULT_PAGE)
    )

    const pageSize = Math.min(
      Math.max(1, parseInteger(searchParams.get("size"), DEFAULT_PAGE_SIZE)),
      MAX_PAGE_SIZE
    )

    const maxPages = Math.min(
      Math.max(1, parseInteger(searchParams.get("maxPages"), DEFAULT_MAX_PAGES)),
      MAX_MAX_PAGES
    )

    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"

    const fetchedAt = new Date().toISOString()

    const diagnostics: Diagnostics = {
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
    }

    const allRows: RawPtrTradeRow[] = []
    const pageDiagnostics: Array<{
      page: number
      ok: boolean
      upstreamCount: number
      normalizedRows: number
      error: string | null
    }> = []

    for (let page = startPage; page < startPage + maxPages; page += 1) {
      diagnostics.pagesRequested += 1

      const url = new URL(AINVEST_CONGRESS_URL)
      url.searchParams.set("page", String(page))
      url.searchParams.set("size", String(pageSize))

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
                parsedError?.msg ||
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
            error: `AInvest ${res.status}: ${message}`,
          })
          break
        }

        let parsed: AInvestEnvelope
        try {
          parsed = JSON.parse(rawBody) as AInvestEnvelope
        } catch {
          diagnostics.pagesFailed += 1
          pageDiagnostics.push({
            page,
            ok: false,
            upstreamCount: 0,
            normalizedRows: 0,
            error: `AInvest returned non-JSON response: ${rawBody.slice(0, 300)}`,
          })
          break
        }

        const tradeRows = extractTradeArray(parsed)
        const normalizedRows = tradeRows
          .map((trade) => normalizeAInvestTrade(trade, fetchedAt))
          .filter((trade): trade is RawPtrTradeRow => trade !== null)

        diagnostics.pagesSucceeded += 1
        diagnostics.upstreamRows += tradeRows.length
        diagnostics.normalizedRows += normalizedRows.length

        allRows.push(...normalizedRows)

        pageDiagnostics.push({
          page,
          ok: true,
          upstreamCount: tradeRows.length,
          normalizedRows: normalizedRows.length,
          error: null,
        })

        if (tradeRows.length < pageSize) {
          diagnostics.stoppedBecauseShortPage = true
          break
        }
      } catch (error: any) {
        diagnostics.pagesFailed += 1
        pageDiagnostics.push({
          page,
          ok: false,
          upstreamCount: 0,
          normalizedRows: 0,
          error: error?.message || "Unknown AInvest error",
        })
        break
      }
    }

    if (!diagnostics.stoppedBecauseShortPage && diagnostics.pagesRequested >= maxPages) {
      diagnostics.stoppedBecauseMaxPages = true
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
      page: startPage,
      size: pageSize,
      maxPages,
      ptrCount,
      diagnostics,
      pageDiagnostics,
      message:
        dedupedRows.length === 0
          ? "PTR route ran successfully but no normalized trades were returned from AInvest."
          : "Raw PTR trades ingested successfully from paginated AInvest congress feed.",
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