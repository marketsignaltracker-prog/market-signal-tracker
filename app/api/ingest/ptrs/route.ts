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
}

type SourceRow = {
  id?: number | null
  company_id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  last_screened_at?: string | null
}

type AInvestTradeRow = {
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
  reportDate?: string
  report_date?: string
  reporting_gap?: string
  trade_type?: string
  type?: string
  action?: string
  size?: string
  amount?: string
  amount_range?: string
  amountLow?: number | string | null
  amountHigh?: number | string | null
  link?: string | null
  owner?: string | null
  asset?: string | null
  assetType?: string | null
}

type AInvestResponse = {
  data?: AInvestTradeRow[] | { data?: AInvestTradeRow[] }
  result?: AInvestTradeRow[]
  rows?: AInvestTradeRow[]
  status_code?: number
  status_msg?: string
  message?: string
}

const DB_CHUNK_SIZE = 200
const API_TIMEOUT_MS = 12000
const API_CONCURRENCY = 4
const DEFAULT_BATCH = 25
const MAX_BATCH = 100
const DEFAULT_START = 0
const DEFAULT_LIMIT_PER_TICKER = 10
const MAX_LIMIT_PER_TICKER = 50
const CANDIDATE_LOOKBACK_DAYS = 10
const MIN_CANDIDATE_SCORE = 65

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

function buildTradeKey(row: {
  filer: string
  ticker: string | null
  transaction_date: string | null
  action: string | null
  amount_range: string | null
}) {
  return [
    row.filer.toLowerCase(),
    row.ticker || "unknown",
    row.transaction_date || "unknown",
    row.action || "unknown",
    row.amount_range || "unknown",
  ].join("::")
}

async function fetchWithTimeout(url: string, token: string) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), API_TIMEOUT_MS)

  try {
    return await fetch(url, {
      headers: {
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

async function upsertRows(supabase: any, rows: RawPtrTradeRow[]) {
  const chunks = chunkArray(rows, DB_CHUNK_SIZE)
  let inserted = 0

  for (const chunk of chunks) {
    const { error } = await supabase
      .from("raw_ptr_trades")
      .upsert(chunk, {
        onConflict: "trade_key",
      })

    if (error) {
      throw error
    }

    inserted += chunk.length
  }

  return inserted
}

async function loadSourceRows(
  supabase: any,
  scope: "all" | "eligible" | "candidates",
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<SourceRow[]> {
  if (scope === "all") {
    let query = supabase
      .from("companies")
      .select("id, ticker, cik, name, is_active")
      .not("ticker", "is", null)
      .order("id", { ascending: true })
      .range(start, start + batch - 1)

    if (onlyActive) query = query.eq("is_active", true)

    const { data, error } = await query
    if (error) throw error
    return (data || []) as SourceRow[]
  }

  if (scope === "eligible") {
    let query = supabase
      .from("candidate_universe")
      .select("company_id, ticker, cik, name, is_active, is_eligible")
      .eq("is_eligible", true)
      .not("ticker", "is", null)
      .order("ticker", { ascending: true })
      .range(start, start + batch - 1)

    if (onlyActive) query = query.eq("is_active", true)

    const { data, error } = await query
    if (error) throw error
    return (data || []) as SourceRow[]
  }

  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - CANDIDATE_LOOKBACK_DAYS)

  let query = supabase
    .from("candidate_universe")
    .select(
      "company_id, ticker, cik, name, is_active, candidate_score, included, last_screened_at"
    )
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .gte("last_screened_at", cutoff.toISOString())
    .not("ticker", "is", null)
    .order("candidate_score", { ascending: false })
    .range(start, start + batch - 1)

  if (onlyActive) query = query.eq("is_active", true)

  const { data, error } = await query
  if (error) throw error
  return (data || []) as SourceRow[]
}

function extractTradeArray(parsed: AInvestResponse): AInvestTradeRow[] {
  if (Array.isArray(parsed?.data)) return parsed.data
  if (Array.isArray(parsed?.result)) return parsed.result
  if (Array.isArray(parsed?.rows)) return parsed.rows
  if (Array.isArray(parsed?.data?.data)) return parsed.data.data
  return []
}

function normalizeAInvestTrade(
  ticker: string,
  row: AInvestTradeRow,
  fetchedAt: string
): RawPtrTradeRow | null {
  const filer_name = String(
    row.name || row.politician || row.filer || row.member || ""
  ).trim()

  if (!filer_name) return null

  const transaction_date = normalizeDate(
    row.trade_date || row.transactionDate || row.transaction_date
  )
  const report_date = normalizeDate(
    row.filing_date || row.reportDate || row.report_date
  )
  const action = normalizeAction(row.trade_type || row.type || row.action)
  const amount_range = String(row.size || row.amount || row.amount_range || "").trim() || null
  const amounts = parseAmountRange(amount_range)

  return {
    disclosure_source: "AINVEST",
    filer_name,
    chamber: row.chamber || null,
    district_or_state: row.district || row.state || null,
    report_date,
    transaction_date,
    ticker,
    asset_name: row.asset || ticker,
    asset_type: row.assetType || "Stock",
    action,
    amount_range,
    amount_low:
      safeNumber(row.amountLow) ?? amounts.amount_low,
    amount_high:
      safeNumber(row.amountHigh) ?? amounts.amount_high,
    owner: row.owner || null,
    ptr_url: row.link || null,
    raw_document_url: row.link || null,
    trade_key: buildTradeKey({
      filer: filer_name,
      ticker,
      transaction_date,
      action,
      amount_range,
    }),
    fetched_at: fetchedAt,
    updated_at: fetchedAt,
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

    const scopeParam = (searchParams.get("scope") || "eligible").toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), DEFAULT_START))
    const batch = Math.min(
      Math.max(1, parseInteger(searchParams.get("batch"), DEFAULT_BATCH)),
      MAX_BATCH
    )
    const perTickerLimit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT_PER_TICKER)),
      MAX_LIMIT_PER_TICKER
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

    const scope = scopeParam as "all" | "eligible" | "candidates"
    const sourceRows = await loadSourceRows(
      supabase,
      scope,
      start,
      batch,
      onlyActive
    )

    const fetchedAt = new Date().toISOString()

    const fetchResults = await mapWithConcurrency(
      sourceRows,
      API_CONCURRENCY,
      async (row) => {
        const ticker = normalizeTicker(row.ticker)

        if (!ticker) {
          return {
            ticker: null,
            ok: false,
            rows: [] as RawPtrTradeRow[],
            upstreamCount: 0,
            error: "Missing ticker",
          }
        }

        const url = new URL("https://docs.ainvest.com/open/ownership/congress")
        url.searchParams.set("ticker", ticker)
        url.searchParams.set("page", "1")
        url.searchParams.set("size", String(perTickerLimit))

        try {
          const res = await fetchWithTimeout(url.toString(), ainvestToken)
          const body = await res.text()

          if (!res.ok) {
            return {
              ticker,
              ok: false,
              rows: [] as RawPtrTradeRow[],
              upstreamCount: 0,
              error: `AInvest ${res.status}: ${body.slice(0, 300)}`,
            }
          }

          let parsed: AInvestResponse
          try {
            parsed = JSON.parse(body) as AInvestResponse
          } catch {
            return {
              ticker,
              ok: false,
              rows: [] as RawPtrTradeRow[],
              upstreamCount: 0,
              error: `AInvest returned non-JSON response: ${body.slice(0, 300)}`,
            }
          }

          const tradeRows = extractTradeArray(parsed)
          const normalizedRows = tradeRows
            .map((trade) => normalizeAInvestTrade(ticker, trade, fetchedAt))
            .filter((trade): trade is RawPtrTradeRow => trade !== null)

          return {
            ticker,
            ok: true,
            rows: normalizedRows,
            upstreamCount: tradeRows.length,
            error: null,
          }
        } catch (error: any) {
          return {
            ticker,
            ok: false,
            rows: [] as RawPtrTradeRow[],
            upstreamCount: 0,
            error: error?.message || "Unknown AInvest error",
          }
        }
      }
    )

    const allRows = fetchResults.flatMap((r) => r.rows)
    const dedupe = new Map<string, RawPtrTradeRow>()

    for (const row of allRows) {
      if (!dedupe.has(row.trade_key)) {
        dedupe.set(row.trade_key, row)
      }
    }

    const dedupedRows = Array.from(dedupe.values())
    const inserted = dedupedRows.length > 0 ? await upsertRows(supabase, dedupedRows) : 0

    let ptrCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_ptr_trades")
        .select("*", { count: "exact", head: true })

      ptrCount = error ? null : count ?? 0
    }

    return Response.json({
      ok: true,
      scope,
      start,
      batch,
      perTickerLimit,
      processedTickers: sourceRows.length,
      successfulTickerFetches: fetchResults.filter((r) => r.ok).length,
      failedTickerFetches: fetchResults.filter((r) => !r.ok).length,
      scannedRows: allRows.length,
      dedupedRows: dedupedRows.length,
      insertedOrUpdated: inserted,
      ptrCount,
      diagnostics: fetchResults.map((r) => ({
        ticker: r.ticker,
        ok: r.ok,
        upstreamCount: r.upstreamCount,
        normalizedRows: r.rows.length,
        error: r.error,
      })),
      message:
        dedupedRows.length === 0
          ? "PTR route ran successfully but no normalized trades were returned from AInvest."
          : "PTR trades ingested successfully",
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