import { createClient, type SupabaseClient } from "@supabase/supabase-js"

type CompanyRow = {
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  candidate_score?: number | null
}

type SecSubmissions = {
  name?: string
  filings?: {
    recent?: {
      accessionNumber?: string[]
      filingDate?: string[]
      form?: string[]
      primaryDocument?: string[]
    }
  }
}

type FilingInsertRow = {
  cik: string
  ticker: string
  company_name: string | null
  form_type: string | null
  accession_no: string
  filed_at: string | null
  primary_doc: string | null
  filing_url: string | null
  filing_json: Record<string, any>
  fetched_at: string
  updated_at: string
}

type Diagnostics = {
  companiesLoaded: number
  companiesAttempted: number
  companiesWithRecentFilings: number
  companiesWithNoRecentFilings: number
  companiesWithFetchErrors: number
  companiesWithUpsertErrors: number
  invalidCompanies: number
  totalFetchedRows: number
  totalInsertedRows: number
  totalErrors: number
  formsSeen: Record<string, number>
}

type Database = {
  public: {
    Tables: {
      companies: {
        Row: {
          id: number
          ticker: string
          company_name: string | null
          cik: string | null
          created_at: string | null
          name: string | null
          is_active: boolean
          source: string | null
          last_seen_at: string | null
          updated_at: string | null
        }
        Insert: {
          id?: number
          ticker: string
          company_name?: string | null
          cik?: string | null
          created_at?: string | null
          name?: string | null
          is_active?: boolean
          source?: string | null
          last_seen_at?: string | null
          updated_at?: string | null
        }
        Update: {
          id?: number
          ticker?: string
          company_name?: string | null
          cik?: string | null
          created_at?: string | null
          name?: string | null
          is_active?: boolean
          source?: string | null
          last_seen_at?: string | null
          updated_at?: string | null
        }
      }
      candidate_universe: {
        Row: {
          ticker: string
          cik: string | null
          name: string | null
          price: number | null
          market_cap: number | null
          avg_volume_20d: number | null
          avg_dollar_volume_20d: number | null
          return_5d: number | null
          volume_ratio: number | null
          breakout_20d: boolean | null
          passes_price: boolean | null
          passes_volume: boolean | null
          passes_dollar_volume: boolean | null
          passes_market_cap: boolean | null
          candidate_score: number | null
          included: boolean | null
          screen_reason: string | null
          last_screened_at: string | null
          return_20d: number | null
          above_sma_20: boolean | null
          updated_at: string | null
          created_at: string | null
        }
        Insert: {
          ticker: string
          cik?: string | null
          name?: string | null
          price?: number | null
          market_cap?: number | null
          avg_volume_20d?: number | null
          avg_dollar_volume_20d?: number | null
          return_5d?: number | null
          volume_ratio?: number | null
          breakout_20d?: boolean | null
          passes_price?: boolean | null
          passes_volume?: boolean | null
          passes_dollar_volume?: boolean | null
          passes_market_cap?: boolean | null
          candidate_score?: number | null
          included?: boolean | null
          screen_reason?: string | null
          last_screened_at?: string | null
          return_20d?: number | null
          above_sma_20?: boolean | null
          updated_at?: string | null
          created_at?: string | null
        }
        Update: {
          ticker?: string
          cik?: string | null
          name?: string | null
          price?: number | null
          market_cap?: number | null
          avg_volume_20d?: number | null
          avg_dollar_volume_20d?: number | null
          return_5d?: number | null
          volume_ratio?: number | null
          breakout_20d?: boolean | null
          passes_price?: boolean | null
          passes_volume?: boolean | null
          passes_dollar_volume?: boolean | null
          passes_market_cap?: boolean | null
          candidate_score?: number | null
          included?: boolean | null
          screen_reason?: string | null
          last_screened_at?: string | null
          return_20d?: number | null
          above_sma_20?: boolean | null
          updated_at?: string | null
          created_at?: string | null
        }
      }
      raw_filings: {
        Row: {
          id: number
          company_id: number | null
          ticker: string
          accession_no: string
          filing_type: string | null
          filed_at: string | null
          filing_url: string | null
          filing_json: Record<string, any> | null
          created_at: string | null
          cik: string | null
          company_name: string | null
          form_type: string | null
          primary_doc: string | null
          fetched_at: string | null
          updated_at: string | null
        }
        Insert: {
          id?: number
          company_id?: number | null
          ticker: string
          accession_no: string
          filing_type?: string | null
          filed_at?: string | null
          filing_url?: string | null
          filing_json?: Record<string, any> | null
          created_at?: string | null
          cik?: string | null
          company_name?: string | null
          form_type?: string | null
          primary_doc?: string | null
          fetched_at?: string | null
          updated_at?: string | null
        }
        Update: {
          id?: number
          company_id?: number | null
          ticker?: string
          accession_no?: string
          filing_type?: string | null
          filed_at?: string | null
          filing_url?: string | null
          filing_json?: Record<string, any> | null
          created_at?: string | null
          cik?: string | null
          company_name?: string | null
          form_type?: string | null
          primary_doc?: string | null
          fetched_at?: string | null
          updated_at?: string | null
        }
      }
    }
  }
}

const SEC_USER_AGENT =
  process.env.SEC_USER_AGENT ||
  "Market Signal Tracker marketsignaltracker@gmail.com"

const SEC_BASE_URL = "https://data.sec.gov/submissions"
const MAX_BATCH = 250
const DEFAULT_BATCH = 100
const REQUEST_DELAY_MS = 175
const MAX_FILINGS_PER_COMPANY = 25
const RETENTION_DAYS = 30
const SEC_FETCH_TIMEOUT_MS = 12000
const MAX_UPSERT_RETRIES = 4

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function normalizeFormType(formType: string | null | undefined) {
  return (formType || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/^FORM\s+/i, "")
}

function padCik(cik: string | number | null | undefined) {
  return String(cik || "").replace(/\D/g, "").padStart(10, "0")
}

function buildSecSubmissionUrl(cikPadded: string) {
  return `${SEC_BASE_URL}/CIK${cikPadded}.json`
}

function buildFilingUrl(cikPadded: string, accessionNo: string, primaryDoc: string | null) {
  if (!primaryDoc) return null

  const accessionNoNoDashes = accessionNo.replace(/-/g, "")
  const cikNoLeadingZeros = String(parseInt(cikPadded, 10))

  return `https://www.sec.gov/Archives/edgar/data/${cikNoLeadingZeros}/${accessionNoNoDashes}/${primaryDoc}`
}

function isDeadlockError(message: string | undefined | null) {
  return (message || "").toLowerCase().includes("deadlock detected")
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

async function upsertWithRetry(
  supabase: SupabaseClient<Database>,
  rows: FilingInsertRow[]
) {
  let attempt = 0

  while (attempt < MAX_UPSERT_RETRIES) {
    const typedRows: Database["public"]["Tables"]["raw_filings"]["Insert"][] = rows.map(
      (row) => ({
        cik: row.cik,
        ticker: row.ticker,
        company_name: row.company_name,
        form_type: row.form_type,
        accession_no: row.accession_no,
        filed_at: row.filed_at,
        primary_doc: row.primary_doc,
        filing_url: row.filing_url,
        filing_json: row.filing_json,
        fetched_at: row.fetched_at,
        updated_at: row.updated_at,
      })
    )

    const rawFilingsTable = supabase.from("raw_filings") as any
    const { error } = await rawFilingsTable.upsert(typedRows, {
      onConflict: "accession_no",
    })

    if (!error) return

    if (!isDeadlockError(error.message)) {
      throw new Error(`Supabase upsert failed: ${error.message}`)
    }

    attempt += 1

    if (attempt >= MAX_UPSERT_RETRIES) {
      throw new Error(`Supabase upsert failed after retries: ${error.message}`)
    }

    await sleep(250 * Math.pow(2, attempt - 1))
  }
}

async function fetchRecentFilingsForCompany(company: CompanyRow) {
  const cikPadded = padCik(company.cik)
  const secUrl = buildSecSubmissionUrl(cikPadded)

  const response = await fetchWithTimeout(
    secUrl,
    {
      headers: {
        "User-Agent": SEC_USER_AGENT,
        Accept: "application/json",
      },
      cache: "no-store",
    },
    SEC_FETCH_TIMEOUT_MS
  )

  if (!response.ok) {
    throw new Error(`SEC request failed with status ${response.status}`)
  }

  const json = (await response.json()) as SecSubmissions

  return {
    cikPadded,
    secUrl,
    json,
  }
}

function mapRecentFilingsToRows(
  company: CompanyRow,
  cikPadded: string,
  secUrl: string,
  json: SecSubmissions,
  fetchedAt: string,
  diagnostics: Diagnostics
): FilingInsertRow[] {
  const recent = json.filings?.recent
  if (!recent) return []

  const accessionNumbers = recent.accessionNumber || []
  const filingDates = recent.filingDate || []
  const forms = recent.form || []
  const primaryDocuments = recent.primaryDocument || []

  const rows: FilingInsertRow[] = []

  for (let i = 0; i < Math.min(accessionNumbers.length, MAX_FILINGS_PER_COMPANY); i++) {
    const accessionNo = accessionNumbers[i]?.trim()
    const filingDate = filingDates[i]?.trim() || null
    const formType = normalizeFormType(forms[i] || null) || null
    const primaryDoc = primaryDocuments[i]?.trim() || null

    if (!accessionNo || !formType) continue

    diagnostics.formsSeen[formType] = (diagnostics.formsSeen[formType] || 0) + 1

    rows.push({
      cik: cikPadded,
      ticker: normalizeTicker(company.ticker),
      company_name: json.name || company.name || null,
      form_type: formType,
      accession_no: accessionNo,
      filed_at: filingDate,
      primary_doc: primaryDoc,
      filing_url: buildFilingUrl(cikPadded, accessionNo, primaryDoc),
      filing_json: {
        accessionNumber: accessionNo,
        filingDate,
        form: formType,
        primaryDocument: primaryDoc,
        source: secUrl,
      },
      fetched_at: fetchedAt,
      updated_at: fetchedAt,
    })
  }

  return rows
}

async function loadCompaniesForBatch(
  supabase: SupabaseClient<Database>,
  scope: "all" | "active" | "candidates",
  from: number,
  to: number
) {
  if (scope === "candidates") {
    const candidateUniverseTable = supabase.from("candidate_universe") as any

    const { data, error } = await candidateUniverseTable
      .select("ticker, cik, name, candidate_score")
      .eq("included", true)
      .order("candidate_score", { ascending: false })
      .order("ticker", { ascending: true })
      .range(from, to)

    if (error) throw new Error(`Candidate load failed: ${error.message}`)

    const { count, error: countError } = await candidateUniverseTable
      .select("*", { count: "exact", head: true })
      .eq("included", true)

    if (countError) throw new Error(`Candidate count failed: ${countError.message}`)

    return {
      companies: (data || []) as CompanyRow[],
      totalCompanies: count || 0,
      sourceTable: "candidate_universe",
    }
  }

  const companiesTable = supabase.from("companies") as any

  let query = companiesTable
    .select("ticker, cik, name, is_active")
    .not("cik", "is", null)
    .order("ticker", { ascending: true })
    .range(from, to)

  let countQuery = companiesTable
    .select("*", { count: "exact", head: true })
    .not("cik", "is", null)

  if (scope === "active") {
    query = query.eq("is_active", true)
    countQuery = countQuery.eq("is_active", true)
  }

  const [{ data, error }, { count, error: countError }] = await Promise.all([
    query,
    countQuery,
  ])

  if (error) throw new Error(`Company load failed: ${error.message}`)
  if (countError) throw new Error(`Company count failed: ${countError.message}`)

  return {
    companies: (data || []) as CompanyRow[],
    totalCompanies: count || 0,
    sourceTable: "companies",
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
    const supabase = createClient<Database>(supabaseUrl, serviceRoleKey)
    const { searchParams } = new URL(request.url)

    const scopeParam = (searchParams.get("scope") || "candidates").toLowerCase()
    const scope: "all" | "active" | "candidates" =
      scopeParam === "all" || scopeParam === "active" || scopeParam === "candidates"
        ? scopeParam
        : "candidates"

    const start = parseInteger(searchParams.get("start"), 0)
    const batch = parseInteger(searchParams.get("batch"), DEFAULT_BATCH)

    const safeStart = Math.max(0, start)
    const safeBatch = Math.min(Math.max(1, batch), MAX_BATCH)
    const from = safeStart
    const to = safeStart + safeBatch - 1

    const fetchedAt = new Date().toISOString()

    const diagnostics: Diagnostics = {
      companiesLoaded: 0,
      companiesAttempted: 0,
      companiesWithRecentFilings: 0,
      companiesWithNoRecentFilings: 0,
      companiesWithFetchErrors: 0,
      companiesWithUpsertErrors: 0,
      invalidCompanies: 0,
      totalFetchedRows: 0,
      totalInsertedRows: 0,
      totalErrors: 0,
      formsSeen: {},
    }

    const { companies, totalCompanies, sourceTable } = await loadCompaniesForBatch(
      supabase,
      scope,
      from,
      to
    )

    diagnostics.companiesLoaded = companies.length

    const results: Array<Record<string, any>> = []
    const sampleInsertedRows: FilingInsertRow[] = []

    for (const company of companies) {
      diagnostics.companiesAttempted += 1

      const normalizedTicker = normalizeTicker(company.ticker)
      const cikPadded = padCik(company.cik)

      if (!normalizedTicker || !cikPadded || cikPadded.length !== 10) {
        diagnostics.invalidCompanies += 1
        diagnostics.totalErrors += 1

        results.push({
          ticker: normalizedTicker || null,
          cik: cikPadded || null,
          ok: false,
          stage: "validate",
          error: "Missing or invalid ticker/cik",
        })

        await sleep(REQUEST_DELAY_MS)
        continue
      }

      try {
        const { secUrl, json } = await fetchRecentFilingsForCompany(company)
        const rows = mapRecentFilingsToRows(
          company,
          cikPadded,
          secUrl,
          json,
          fetchedAt,
          diagnostics
        )

        diagnostics.totalFetchedRows += rows.length

        if (rows.length === 0) {
          diagnostics.companiesWithNoRecentFilings += 1

          results.push({
            ticker: normalizedTicker,
            ok: true,
            stage: "no_recent_filings",
            inserted: 0,
            secUrl,
          })

          await sleep(REQUEST_DELAY_MS)
          continue
        }

        await upsertWithRetry(supabase, rows)

        diagnostics.companiesWithRecentFilings += 1
        diagnostics.totalInsertedRows += rows.length

        if (sampleInsertedRows.length < 5) {
          for (const row of rows.slice(0, 2)) {
            if (sampleInsertedRows.length < 5) {
              sampleInsertedRows.push(row)
            }
          }
        }

        results.push({
          ticker: normalizedTicker,
          ok: true,
          stage: "upsert",
          inserted: rows.length,
          firstForm: rows[0]?.form_type ?? null,
          firstFiledAt: rows[0]?.filed_at ?? null,
          secUrl,
        })
      } catch (error: any) {
        const message = error?.message || "Unknown error"
        diagnostics.totalErrors += 1

        if (message.toLowerCase().includes("upsert failed")) {
          diagnostics.companiesWithUpsertErrors += 1
          results.push({
            ticker: normalizedTicker,
            ok: false,
            stage: "upsert",
            error: message,
          })
        } else {
          diagnostics.companiesWithFetchErrors += 1
          results.push({
            ticker: normalizedTicker,
            ok: false,
            stage: "fetch_or_parse",
            error: message,
          })
        }
      }

      await sleep(REQUEST_DELAY_MS)
    }

    const retentionCutoffDate = new Date(
      Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000
    )
      .toISOString()
      .slice(0, 10)

    const retentionCutoffTimestamp = new Date(
      Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000
    ).toISOString()

    const rawFilingsTable = supabase.from("raw_filings") as any

    const { error: retentionErrorByFiledAt } = await rawFilingsTable
      .delete()
      .lt("filed_at", retentionCutoffDate)

    const { error: retentionErrorNullFiledAt } = await rawFilingsTable
      .delete()
      .is("filed_at", null)
      .lt("fetched_at", retentionCutoffTimestamp)

    const { count: rawFilingsCount, error: rawCountError } = await rawFilingsTable.select("*", {
      count: "exact",
      head: true,
    })

    const nextStart = to + 1 < totalCompanies ? to + 1 : null

    return Response.json({
      ok: true,
      scope,
      sourceTable,
      processedCompanies: companies.length,
      totalCompanies,
      start: safeStart,
      batch: safeBatch,
      nextStart,
      retainedDays: RETENTION_DAYS,
      retentionCleanupByFiledAt: retentionErrorByFiledAt
        ? retentionErrorByFiledAt.message
        : "ok",
      retentionCleanupNullFiledAt: retentionErrorNullFiledAt
        ? retentionErrorNullFiledAt.message
        : "ok",
      rawFilingsCount: rawCountError ? null : rawFilingsCount,
      fetchedAt,
      diagnostics,
      sampleInsertedRows,
      results,
      message:
        diagnostics.totalInsertedRows > 0
          ? "Filings ingest completed and rows were written to raw_filings."
          : "Filings ingest completed but no rows were written. Check diagnostics and results.",
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