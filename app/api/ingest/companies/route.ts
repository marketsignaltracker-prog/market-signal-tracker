import { NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type SecCompanyTickerRow = {
  cik_str?: number | string | null
  ticker?: string | null
  title?: string | null
}

type CompanyRow = {
  ticker: string
  cik: string | null
  name: string | null
  is_active: boolean
  source: string | null
  last_seen_at: string
  updated_at: string
}

const SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
const UPSERT_CHUNK_SIZE = 500

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

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

async function fetchSecCompanies() {
  const response = await fetch(SEC_COMPANY_TICKERS_URL, {
    method: "GET",
    headers: {
      "User-Agent": "MarketSignalTracker/1.0 support@marketsignaltracker.com",
      Accept: "application/json",
    },
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`SEC fetch failed with status ${response.status}`)
  }

  const json = await response.json()

  if (!json || typeof json !== "object") {
    throw new Error("SEC response was not a valid object")
  }

  return Object.values(json) as SecCompanyTickerRow[]
}

function mapSecRowToCompany(row: SecCompanyTickerRow, nowIso: string): CompanyRow | null {
  const ticker = normalizeTicker(row.ticker)
  const cik = normalizeCik(row.cik_str)
  const name = cleanString(row.title)

  if (!ticker) return null

  return {
    ticker,
    cik,
    name,
    is_active: true,
    source: "sec_company_tickers",
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
    const nowIso = new Date().toISOString()

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const companiesTable = supabase.from("companies") as any

    const secRows = await fetchSecCompanies()

    const mappedRows = secRows
      .map((row) => mapSecRowToCompany(row, nowIso))
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
          error: "No valid companies were parsed from SEC source",
          debug: {
            sourceRowCount: secRows.length,
            mappedRowCount: mappedRows.length,
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
            sourceRowCount: secRows.length,
            mappedRowCount: mappedRows.length,
            dedupedRowCount: dedupedRows.length,
            errorCount: upsertResult.errors.length,
            errorSamples: upsertResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

        const activeTickers = dedupedRows.map((row) => row.ticker)

    for (const chunk of chunkArray(activeTickers, UPSERT_CHUNK_SIZE)) {
      const { error } = await companiesTable
        .update({
          is_active: false,
          updated_at: nowIso,
        })
        .not("ticker", "in", `(${chunk.map((t) => `"${t}"`).join(",")})`)

      if (error) {
        return NextResponse.json(
          {
            ok: false,
            error: "Failed updating inactive company flags",
            debug: {
              message: error.message,
              details: (error as any)?.details ?? null,
              hint: (error as any)?.hint ?? null,
              code: (error as any)?.code ?? null,
            },
          },
          { status: 500 }
        )
      }

      break
    }

    const [{ count: totalCount }, { count: activeCount }] = await Promise.all([
      companiesTable.select("*", { count: "exact", head: true }),
      companiesTable.select("*", { count: "exact", head: true }).eq("is_active", true),
    ])

    return NextResponse.json({
      ok: true,
      source: "sec_company_tickers",
      sourceRowCount: secRows.length,
      mappedRowCount: mappedRows.length,
      dedupedRowCount: dedupedRows.length,
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
        stack:
          process.env.NODE_ENV !== "production" && error?.stack
            ? String(error.stack)
            : undefined,
      },
      { status: 500 }
    )
  }
}