import { NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CompanyRow = {
  ticker: string
  cik: string | null
  name: string | null
  is_active: boolean
  source: string | null
  last_seen_at: string
  updated_at: string
}

type MassiveTickerResult = {
  ticker?: string
  name?: string
  cik?: string | null
  market?: string
  locale?: string
  type?: string
  active?: boolean
  primary_exchange?: string
  currency_name?: string
}

type MassiveTickersResponse = {
  results?: MassiveTickerResult[]
  next_url?: string | null
  count?: number
  status?: string
}

const MASSIVE_BASE = "https://api.massive.com"
const UPSERT_CHUNK_SIZE = 500
const MAX_PAGES = 50 // safety limit — usually ~12 pages for all US stocks

function normalizeTicker(value: string | null | undefined) {
  return (value || "").trim().toUpperCase()
}

function cleanString(value: unknown) {
  if (value === null || value === undefined) return null
  const s = String(value).trim()
  return s.length ? s : null
}

function normalizeCik(value: unknown) {
  if (value === null || value === undefined) return null
  const digits = String(value).replace(/\D/g, "")
  return digits.length ? digits : null
}

async function fetchMassivePage(
  url: string,
  apiKey: string
): Promise<MassiveTickersResponse> {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 30_000)

  try {
    // If it's a next_url, it already includes apiKey param from Massive.
    // But Massive uses Bearer auth, so we always send the header.
    const res = await fetch(url, {
      cache: "no-store",
      signal: controller.signal,
      headers: { Authorization: `Bearer ${apiKey}` },
    })

    if (!res.ok) {
      throw new Error(`Massive tickers fetch failed: ${res.status}`)
    }

    return (await res.json()) as MassiveTickersResponse
  } finally {
    clearTimeout(timeout)
  }
}

async function fetchAllMassiveTickers(apiKey: string) {
  const allTickers: MassiveTickerResult[] = []
  let page = 0

  // Initial URL: all active US stock tickers, 1000 per page
  let url = `${MASSIVE_BASE}/v3/reference/tickers?market=stocks&active=true&limit=1000&locale=us`

  while (url && page < MAX_PAGES) {
    page++
    const response = await fetchMassivePage(url, apiKey)
    const results = response.results || []
    allTickers.push(...results)

    // Massive returns next_url for pagination
    if (response.next_url) {
      // next_url may be absolute or may need base URL
      url = response.next_url.startsWith("http")
        ? response.next_url
        : `${MASSIVE_BASE}${response.next_url}`
    } else {
      break
    }
  }

  return { tickers: allTickers, pages: page }
}

function mapMassiveRowToCompany(
  row: MassiveTickerResult,
  nowIso: string
): CompanyRow | null {
  const ticker = normalizeTicker(row.ticker)
  if (!ticker) return null

  // Skip non-common-stock types (warrants, rights, units, preferred, etc.)
  const tickerType = (row.type || "").toUpperCase()
  if (tickerType && !["CS", "ADRC", ""].includes(tickerType)) return null

  // Skip tickers with special characters (warrants, units)
  if (/[.\-\/+]/.test(ticker) && ticker.length > 5) return null

  const cik = normalizeCik(row.cik)
  const name = cleanString(row.name)

  return {
    ticker,
    cik,
    name,
    is_active: true,
    source: "massive_reference_tickers",
    last_seen_at: nowIso,
    updated_at: nowIso,
  }
}

async function upsertCompaniesInChunks(table: any, rows: CompanyRow[]) {
  let upsertedCount = 0
  const errors: Array<{
    chunkStart: number
    chunkSize: number
    message: string
    details?: string | null
    hint?: string | null
    code?: string | null
    sampleTickers: string[]
  }> = []

  for (let i = 0; i < rows.length; i += UPSERT_CHUNK_SIZE) {
    const chunk = rows.slice(i, i + UPSERT_CHUNK_SIZE)

    const { error } = await table.upsert(chunk, { onConflict: "ticker" })

    if (error) {
      errors.push({
        chunkStart: i,
        chunkSize: chunk.length,
        message: error.message,
        details: (error as any)?.details ?? null,
        hint: (error as any)?.hint ?? null,
        code: (error as any)?.code ?? null,
        sampleTickers: chunk.slice(0, 10).map((row) => row.ticker),
      })
    } else {
      upsertedCount += chunk.length
    }
  }

  return {
    upsertedCount,
    errors,
  }
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing Supabase environment variables",
        debug: {
          hasSupabaseUrl: Boolean(supabaseUrl),
          hasServiceRoleKey: Boolean(serviceRoleKey),
        },
      },
      { status: 500 }
    )
  }

  const massiveApiKey = process.env.MASSIVE_API_KEY

  if (!massiveApiKey) {
    return NextResponse.json(
      { ok: false, error: "Missing MASSIVE_API_KEY environment variable" },
      { status: 500 }
    )
  }

  try {
    const nowIso = new Date().toISOString()

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const companiesTable = supabase.from("companies") as any

    const { tickers: massiveRows, pages } = await fetchAllMassiveTickers(massiveApiKey)

    const mappedRows = massiveRows
      .map((row) => mapMassiveRowToCompany(row, nowIso))
      .filter((row): row is CompanyRow => Boolean(row))

    const dedupedMap = new Map<string, CompanyRow>()
    for (const row of mappedRows) {
      dedupedMap.set(row.ticker, row)
    }

    const dedupedRows = [...dedupedMap.values()]

    if (!dedupedRows.length) {
      return NextResponse.json(
        {
          ok: false,
          error: "No valid companies were parsed from Massive source",
          debug: {
            sourceRowCount: massiveRows.length,
            mappedRowCount: mappedRows.length,
            pagesLoaded: pages,
          },
        },
        { status: 500 }
      )
    }

    const upsertResult = await upsertCompaniesInChunks(companiesTable, dedupedRows)

    if (upsertResult.errors.length > 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed writing one or more company chunks to Supabase",
          debug: {
            sourceRowCount: massiveRows.length,
            mappedRowCount: mappedRows.length,
            dedupedRowCount: dedupedRows.length,
            pagesLoaded: pages,
            errorCount: upsertResult.errors.length,
            errorSamples: upsertResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const [{ count: totalCount }, { count: activeCount }] = await Promise.all([
      companiesTable.select("*", { count: "exact", head: true }),
      companiesTable.select("*", { count: "exact", head: true }).eq("is_active", true),
    ])

    return NextResponse.json({
      ok: true,
      source: "massive_reference_tickers",
      sourceRowCount: massiveRows.length,
      mappedRowCount: mappedRows.length,
      dedupedRowCount: dedupedRows.length,
      pagesLoaded: pages,
      upsertedCount: upsertResult.upsertedCount,
      totalCompanies: totalCount ?? null,
      activeCompanies: activeCount ?? null,
      sampleTickers: dedupedRows.slice(0, 10).map((row) => row.ticker),
    })
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: error?.message || "Unknown ingest companies error",
      },
      { status: 500 }
    )
  }
}
