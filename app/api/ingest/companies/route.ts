import { createClient } from "@supabase/supabase-js"
import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CompanyRow = {
  ticker: string
  cik: string
  name: string | null
  exchange: string | null
  is_active: boolean
  updated_at?: string
}

function normalizeTicker(value: string | null | undefined) {
  return (value || "").trim().toUpperCase()
}

function cleanString(value: unknown) {
  if (value === null || value === undefined) return null
  const s = String(value).trim()
  return s.length ? s : null
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

async function upsertCompaniesInChunks(table: any, rows: CompanyRow[], chunkSize = 500) {
  let insertedOrUpdated = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, chunkSize)) {
    const { error } = await table.upsert(chunk, { onConflict: "ticker" })
    if (error) {
      errors.push(error.message)
    } else {
      insertedOrUpdated += chunk.length
    }
  }

  return {
    insertedOrUpdated,
    errors,
  }
}

async function fetchSecCompanyFactsLikeList() {
  // Replace this URL with your real source if different.
  // This is just the common SEC company_tickers.json source pattern.
  const url = "https://www.sec.gov/files/company_tickers.json"

  const response = await fetch(url, {
    headers: {
      "User-Agent": "MarketSignalTracker admin@marketsignaltracker.com",
      "Accept": "application/json",
    },
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`Upstream fetch failed with status ${response.status}`)
  }

  const json = await response.json()

  const values = Array.isArray(json)
    ? json
    : typeof json === "object" && json !== null
      ? Object.values(json)
      : []

  return values as Array<Record<string, unknown>>
}

function mapSourceRowToCompany(row: Record<string, unknown>): CompanyRow | null {
  const ticker =
    normalizeTicker(
      cleanString(row.ticker) ??
      cleanString(row.symbol)
    )

  const cikRaw = cleanString(row.cik_str) ?? cleanString(row.cik)
  const name = cleanString(row.title) ?? cleanString(row.name) ?? cleanString(row.company)
  const exchange = cleanString(row.exchange)

  if (!ticker || !cikRaw) return null

  const cik = String(cikRaw).replace(/\D/g, "")
  if (!cik) return null

  return {
    ticker,
    cik,
    name,
    exchange,
    is_active: true,
  }
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized" },
      { status: 401 }
    )
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

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const companiesTable = supabase.from("companies") as any

    let sourceRows: Array<Record<string, unknown>> = []

    try {
      sourceRows = await fetchSecCompanyFactsLikeList()
    } catch (error: any) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed fetching upstream company source",
          detail: error?.message || "Unknown upstream error",
        },
        { status: 500 }
      )
    }

    const mapped = sourceRows
      .map(mapSourceRowToCompany)
      .filter((row): row is CompanyRow => Boolean(row))

    const dedupedMap = new Map<string, CompanyRow>()
    for (const row of mapped) {
      dedupedMap.set(row.ticker, row)
    }
    const companies = [...dedupedMap.values()]

    if (!companies.length) {
      return NextResponse.json(
        {
          ok: false,
          error: "No companies were parsed from upstream source",
          debug: {
            sourceRowCount: sourceRows.length,
            mappedRowCount: mapped.length,
          },
        },
        { status: 500 }
      )
    }

    const upsertResult = await upsertCompaniesInChunks(companiesTable, companies, 500)

    if (upsertResult.errors.length > 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed writing one or more company chunks to Supabase",
          debug: {
            sourceRowCount: sourceRows.length,
            mappedRowCount: mapped.length,
            dedupedRowCount: companies.length,
            errorSamples: upsertResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const { count, error: countError } = await companiesTable.select("*", {
      count: "exact",
      head: true,
    })

    return NextResponse.json({
      ok: true,
      sourceRowCount: sourceRows.length,
      mappedRowCount: mapped.length,
      dedupedRowCount: companies.length,
      upsertedCount: upsertResult.insertedOrUpdated,
      totalCompanies: countError ? null : count,
      sample: companies.slice(0, 5),
    })
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: error?.message || "Unknown ingest companies error",
        stack:
          process.env.NODE_ENV !== "production" && error?.stack
            ? String(error.stack)
            : undefined,
      },
      { status: 500 }
    )
  }
}