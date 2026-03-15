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

const DB_CHUNK_SIZE = 200

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
  return t
}

function safeNumber(value: any): number | null {
  if (!value) return null
  const n = Number(String(value).replace(/[$,]/g, "").trim())
  return Number.isFinite(n) ? n : null
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
    (row.ticker || "unknown"),
    (row.transaction_date || "unknown"),
    (row.action || "unknown"),
    (row.amount_range || "unknown")
  ].join("::")
}

async function fetchAinvestTrades(token: string, ticker: string) {

  const url = `https://openapi.ainvest.com/open/ownership/congress?ticker=${ticker}&page=1&size=50`

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json"
    },
    cache: "no-store"
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`AINVEST error ${res.status}: ${text.slice(0,200)}`)
  }

  const json = await res.json()

  return json?.data ?? []
}

function normalizeTrade(raw: any, fetchedAt: string): RawPtrTradeRow | null {

  const filer = raw?.politician || raw?.filer || raw?.member
  const ticker = normalizeTicker(raw?.ticker || raw?.symbol)

  if (!filer || !ticker) return null

  const transactionDate = raw?.transactionDate ?? raw?.transaction_date ?? null
  const reportDate = raw?.reportDate ?? raw?.report_date ?? null
  const action = raw?.type ?? raw?.action ?? null
  const amountRange = raw?.amount ?? raw?.amount_range ?? null

  const tradeKey = buildTradeKey({
    filer,
    ticker,
    transaction_date: transactionDate,
    action,
    amount_range: amountRange
  })

  return {
    disclosure_source: "AINVEST",
    filer_name: filer,
    chamber: raw?.chamber ?? null,
    district_or_state: raw?.district ?? raw?.state ?? null,
    report_date: reportDate,
    transaction_date: transactionDate,
    ticker,
    asset_name: raw?.asset ?? ticker,
    asset_type: raw?.assetType ?? "Stock",
    action,
    amount_range: amountRange,
    amount_low: safeNumber(raw?.amountLow),
    amount_high: safeNumber(raw?.amountHigh),
    owner: raw?.owner ?? null,
    ptr_url: raw?.link ?? null,
    raw_document_url: raw?.link ?? null,
    trade_key: tradeKey,
    fetched_at: fetchedAt,
    updated_at: fetchedAt
  }
}

async function upsertRows(supabase: any, rows: RawPtrTradeRow[]) {

  const chunks = chunkArray(rows, DB_CHUNK_SIZE)

  let inserted = 0

  for (const chunk of chunks) {

    const { error } = await supabase
      .from("raw_ptr_trades")
      .upsert(chunk, {
        onConflict: "trade_key"
      })

    if (error) {
      throw error
    }

    inserted += chunk.length
  }

  return inserted
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

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey)

  try {

    const { data: companies, error: companyError } = await supabase
      .from("companies")
      .select("ticker")
      .eq("is_active", true)

    if (companyError) throw companyError

    const tickers = companies.map((c: any) => c.ticker)

    const fetchedAt = new Date().toISOString()

    let allRows: RawPtrTradeRow[] = []

    for (const ticker of tickers) {

      try {

        const trades = await fetchAinvestTrades(ainvestToken, ticker)

        for (const trade of trades) {

          const normalized = normalizeTrade(trade, fetchedAt)

          if (normalized) {
            allRows.push(normalized)
          }
        }

      } catch (err) {
        console.error("PTR fetch failed for", ticker)
      }

    }

    const dedupe = new Map<string, RawPtrTradeRow>()

    for (const row of allRows) {
      if (!dedupe.has(row.trade_key)) {
        dedupe.set(row.trade_key, row)
      }
    }

    const dedupedRows = Array.from(dedupe.values())

    const inserted = await upsertRows(supabase, dedupedRows)

    return Response.json({
      ok: true,
      scannedRows: allRows.length,
      dedupedRows: dedupedRows.length,
      insertedOrUpdated: inserted,
      message: "PTR trades ingested successfully"
    })

  } catch (error: any) {

    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown PTR ingest error"
      },
      { status: 500 }
    )
  }
}