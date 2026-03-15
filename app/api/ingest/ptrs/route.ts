import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type SourceRow = {
  id?: number | null
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  last_screened_at?: string | null
}

type AInvestTradeRow = {
  name?: string
  party?: string
  state?: string
  trade_date?: string
  filing_date?: string
  reporting_gap?: string
  trade_type?: string
  size?: string
}

type AInvestResponse = {
  data?: {
    data?: AInvestTradeRow[]
  }
  status_code?: number
  status_msg?: string
}

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
  updated_at?: string
}

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

const DEFAULT_BATCH = 50
const MAX_BATCH = 100
const DEFAULT_START = 0
const DEFAULT_LIMIT_PER_TICKER = 10
const MAX_LIMIT_PER_TICKER = 50
const DB_CHUNK_SIZE = 200
const API_TIMEOUT_MS = 12000
const API_CONCURRENCY = 4
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

function normalizeTicker(ticker: string | null | undefined) {
  const t = (ticker || "").trim().toUpperCase()
  return t || null
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
  disclosure_source: string
  filer_name: string
  transaction_date: string | null
  ticker: string | null
  asset_name: string | null
  action: string | null
  amount_range: string | null
}) {
  return [
    row.disclosure_source.trim().toLowerCase(),
    row.filer_name.trim().toLowerCase(),
    row.transaction_date || "unknown-date",
    (row.ticker || "").trim().toUpperCase() || "unknown-ticker",
    (row.asset_name || "").trim().toLowerCase() || "unknown-asset",
    (row.action || "").trim().toLowerCase() || "unknown-action",
    (row.amount_range || "").trim().toLowerCase() || "unknown-amount",
  ].join("::")
}

async function fetchWithTimeout(url: string, token: string) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), API_TIMEOUT_MS)

  try {
    return await fetch(url, {
      cache: "no-store",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      },
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

async function loadSourceRows(
  supabase: any,
  scope: "all" | "eligible" | "candidates",
  start: number,
  batch: number,
  onlyActive: boolean
) {
  if (scope === "all") {
    let query = supabase
      .from("companies")
      .select("id, ticker, cik, name, is_active")
      .not("ticker", "is", null)
      .order("id", { ascending: true })
      .range(start, start + batch - 1)

    if (onlyActive) query = query.eq("is_active", true)

    const { data, error } = await query
    if (error) throw new Error(`companies load failed: ${error.message}`)
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
    if (error) throw new Error(`candidate_universe eligible load failed: ${error.message}`)
    return (data || []) as SourceRow[]
  }

  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - CANDIDATE_LOOKBACK_DAYS)

  let query = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, candidate_score, included, last_screened_at")
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .gte("last_screened_at", cutoff.toISOString())
    .not("ticker", "is", null)
    .order("candidate_score", { ascending: false })
    .range(start, start + batch - 1)

  if (onlyActive) query = query.eq("is_active", true)

  const { data, error } = await query
  if (error) throw new Error(`candidate_universe candidates load failed: ${error.message}`)
  return (data || []) as SourceRow[]
}

function normalizeAInvestTrade(ticker: string, row: AInvestTradeRow, fetchedAt: string): RawPtrTradeRow | null {
  const filer_name = String(row.name || "").trim()
  if (!filer_name) return null

  const transaction_date = normalizeDate(row.trade_date)
  const report_date = normalizeDate(row.filing_date)
  const action = normalizeAction(row.trade_type)
  const amount_range = String(row.size || "").trim() || null
  const amounts = parseAmountRange(amount_range)

  const normalized: RawPtrTradeRow = {
    disclosure_source: "AInvest Congress",
    filer_name,
    chamber: null,
    district_or_state: String(row.state || "").trim() || null,
    report_date,
    transaction_date,
    ticker,
    asset_name: ticker,
    asset_type: "Stock",
    action,
    amount_range,
    amount_low: amounts.amount_low,
    amount_high: amounts.amount_high,
    owner: null,
    ptr_url: null,
    raw_document_url: null,
    trade_key: buildTradeKey({
      disclosure_source: "AInvest Congress",
      filer_name,
      transaction_date,
      ticker,
      asset_name: ticker,
      action,
      amount_range,
    }),
    fetched_at: fetchedAt,
    updated_at: fetchedAt,
  }

  return normalized
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const ainvestToken = process.env.AINVEST_API_TOKEN?.trim()
  if (!ainvestToken) {
    return NextResponse.json(
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
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const onlyActive = (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"

    if (!["all", "eligible", "candidates"].includes(scopeParam)) {
      return NextResponse.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, candidates`,
        },
        { status: 400 }
      )
    }

    const scope = scopeParam as "all" | "eligible" | "candidates"
    const sourceRows = await loadSourceRows(supabase, scope, start, batch, onlyActive)
    const nowIso = new Date().toISOString()

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
            error: "Missing ticker",
          }
        }

        const url = new URL("https://openapi.ainvest.com/open/ownership/congress")
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
              error: `AInvest ${res.status}: ${body.slice(0, 300)}`,
            }
          }

          const parsed = JSON.parse(body) as AInvestResponse
          const dataRows = parsed?.data?.data || []

          const normalizedRows = dataRows
            .map((trade) => normalizeAInvestTrade(ticker, trade, nowIso))
            .filter((trade): trade is RawPtrTradeRow => trade !== null)

          return {
            ticker,
            ok: true,
            rows: normalizedRows,
            error: null,
          }
        } catch (error: any) {
          return {
            ticker,
            ok: false,
            rows: [] as RawPtrTradeRow[],
            error: error?.message || "Unknown AInvest error",
          }
        }
      }
    )

    const allRows = fetchResults.flatMap((r) => r.rows)

    const deduped = new Map<string, RawPtrTradeRow>()
    for (const row of allRows) {
      if (!deduped.has(row.trade_key)) {
        deduped.set(row.trade_key, row)
      }
    }

    const finalRows = Array.from(deduped.values())

    const writeResult =
      finalRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("raw_ptr_trades"),
            "raw_ptr_trades",
            finalRows,
            "trade_key",
            (row) => row.trade_key
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (writeResult.errors.length > 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed writing PTR rows",
          debug: {
            errorSamples: writeResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    let ptrCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_ptr_trades")
        .select("*", { count: "exact", head: true })

      ptrCount = error ? null : count ?? 0
    }

    return NextResponse.json({
      ok: true,
      scope,
      start,
      batch,
      perTickerLimit,
      processedTickers: sourceRows.length,
      successfulTickerFetches: fetchResults.filter((r) => r.ok).length,
      failedTickerFetches: fetchResults.filter((r) => !r.ok).length,
      insertedOrUpdated: writeResult.insertedOrUpdated,
      dedupedRows: finalRows.length,
      ptrCount,
      diagnostics: fetchResults.map((r) => ({
        ticker: r.ticker,
        ok: r.ok,
        rows: r.rows.length,
        error: r.error,
      })),
      message: "Congress trades ingested from AInvest by ticker.",
    })
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: error?.message || "Unknown PTR ingest error",
      },
      { status: 500 }
    )
  }
}