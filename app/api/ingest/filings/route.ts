import { createClient } from "@supabase/supabase-js"

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
  rowsFilteredOutByForm: number
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
const MAX_UPSERT_RETRIES = 5

const DEFAULT_ALLOWED_FORMS = new Set([
  "4",
  "4/A",
  "8-K",
  "6-K",
  "10-Q",
  "10-K",
  "13D",
  "13D/A",
  "13G",
  "13G/A",
  "SC 13D",
  "SC 13D/A",
  "SC 13G",
  "SC 13G/A",
])

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function parseInteger(value: string | null, fallback: number) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function normalizeFormType(formType: string | null | undefined) {
  const normalized = (formType || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/^FORM\s+/i, "")

  if (normalized === "8K") return "8-K"
  if (normalized === "6K") return "6-K"
  if (normalized === "4A" || normalized === "4 /A") return "4/A"
  if (normalized === "13DA") return "13D/A"
  if (normalized === "13GA") return "13G/A"
  if (normalized === "SC13D") return "SC 13D"
  if (normalized === "SC13D/A") return "SC 13D/A"
  if (normalized === "SC13G") return "SC 13G"
  if (normalized === "SC13G/A") return "SC 13G/A"

  return normalized
}

function padCik(cik: string | number | null | undefined) {
  const digits = String(cik || "").replace(/\D/g, "")
  return digits ? digits.padStart(10, "0") : ""
}

function stripLeadingZeros(value: string) {
  const stripped = value.replace(/^0+/, "")
  return stripped || "0"
}

function buildSecSubmissionUrl(cikPadded: string) {
  return `${SEC_BASE_URL}/CIK${cikPadded}.json`
}

function buildFilingUrl(cikPadded: string, accessionNo: string, primaryDoc: string | null) {
  if (!primaryDoc) return null
  if (!cikPadded || cikPadded.length !== 10) return null
  if (!accessionNo) return null

  const accessionNoNoDashes = accessionNo.replace(/-/g, "")
  const cikNoLeadingZeros = stripLeadingZeros(cikPadded)

  return `https://www.sec.gov/Archives/edgar/data/${cikNoLeadingZeros}/${accessionNoNoDashes}/${primaryDoc}`
}

function isRetryableUpsertError(message: string | undefined | null) {
  const lower = (message || "").toLowerCase()
  return (
    lower.includes("deadlock detected") ||
    lower.includes("could not serialize access") ||
    lower.includes("connection") ||
    lower.includes("timeout") ||
    lower.includes("temporar")
  )
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
  supabase: ReturnType<typeof createClient>,
  rows: FilingInsertRow[]
) {
  let attempt = 0

  while (attempt < MAX_UPSERT_RETRIES) {
    const { error } = await supabase.from("raw_filings").upsert(rows, {
      onConflict: "accession_no",
    })

    if (!error) return

    if (!isRetryableUpsertError(error.message)) {
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

function parseAllowedForms(searchValue: string | null) {
  if (!searchValue) return DEFAULT_ALLOWED_FORMS

  const values = searchValue
    .split(",")
    .map((v) => normalizeFormType(v))
    .filter(Boolean)

  return new Set(values)
}

function mapRecentFilingsToRows(
  company: CompanyRow,
  cikPadded: string,
  secUrl: string,
  json: SecSubmissions,
  fetchedAt: string,
  diagnostics: Diagnostics,
  allowedForms: Set<string>
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

    if (!allowedForms.has(formType)) {
      diagnostics.rowsFilteredOutByForm += 1
      continue
    }

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
  supabase: ReturnType<typeof createClient>,
  scope: "all" | "active" | "candidates",
  from: number,
  to: number
) {
  if (scope === "candidates") {
    const { data, error } = await supabase
      .from("candidate_universe")
      .select("ticker, cik, name, candidate_score")
      .eq("included", true)
      .order("candidate_score", { ascending: false })
      .order("ticker", { ascending: true })
      .range(from, to)

    if (error) throw new Error(`Candidate load failed: ${error.message}`)

    const { count, error: countError } = await supabase
      .from("candidate_universe")
      .select("*", { count: "exact", head: true })
      .eq("included", true)

    if (countError) throw new Error(`Candidate count failed: ${countError.message}`)

    return {
      companies: (data || []) as CompanyRow[],
      totalCompanies: count || 0,
      sourceTable: "candidate_universe",
    }
  }

  const query = supabase
    .from("companies")
    .select("ticker, cik, name, is_active")
    .order("ticker", { ascending: true })
    .range(from, to)

  const countQuery = supabase.from("companies").select("*", { count: "exact", head: true })

  if (scope === "active") {
    query.eq("is_active", true)
    countQuery.eq("is_active", true)
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
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey)
    const { searchParams } = new URL(request.url)

    const scopeParam = (searchParams.get("scope") || "active").toLowerCase()
    const scope: "all" | "active" | "candidates" =
      scopeParam === "all" || scopeParam === "candidates" ? scopeParam : "active"

    const start = parseInteger(searchParams.get("start"), 0)
    const batch = parseInteger(searchParams.get("batch"), DEFAULT_BATCH)
    const includeResults = (searchParams.get("includeResults") || "true").toLowerCase() !== "false"
    const allowedForms = parseAllowedForms(searchParams.get("forms"))

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
      rowsFilteredOutByForm: 0,
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

        if (includeResults) {
          results.push({
            ticker: normalizedTicker || null,
            cik: cikPadded || null,
            ok: false,
            stage: "validate",
            error: "Missing or invalid ticker/cik",
          })
        }

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
          diagnostics,
          allowedForms
        )

        diagnostics.totalFetchedRows += rows.length

        if (rows.length === 0) {
          diagnostics.companiesWithNoRecentFilings += 1

          if (includeResults) {
            results.push({
              ticker: normalizedTicker,
              ok: true,
              stage: "no_recent_filings",
              inserted: 0,
              secUrl,
            })
          }

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

        if (includeResults) {
          results.push({
            ticker: normalizedTicker,
            ok: true,
            stage: "upsert",
            inserted: rows.length,
            firstForm: rows[0]?.form_type ?? null,
            firstFiledAt: rows[0]?.filed_at ?? null,
            secUrl,
          })
        }
      } catch (error: any) {
        const message = error?.message || "Unknown error"
        diagnostics.totalErrors += 1

        if (message.toLowerCase().includes("upsert failed")) {
          diagnostics.companiesWithUpsertErrors += 1
          if (includeResults) {
            results.push({
              ticker: normalizedTicker,
              ok: false,
              stage: "upsert",
              error: message,
            })
          }
        } else {
          diagnostics.companiesWithFetchErrors += 1
          if (includeResults) {
            results.push({
              ticker: normalizedTicker,
              ok: false,
              stage: "fetch_or_parse",
              error: message,
            })
          }
        }
      }

      await sleep(REQUEST_DELAY_MS)
    }

    const retentionCutoffDate = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000)
      .toISOString()
      .slice(0, 10)

    const { error: retentionErrorByFiledAt } = await supabase
      .from("raw_filings")
      .delete()
      .lt("filed_at", retentionCutoffDate)

    // Optional fallback cleanup for malformed/null filed_at rows that are old fetches
    const { error: retentionErrorNullFiledAt } = await supabase
      .from("raw_filings")
      .delete()
      .is("filed_at", null)
      .lt("fetched_at", new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString())

    const { count: rawFilingsCount, error: rawCountError } = await supabase
      .from("raw_filings")
      .select("*", { count: "exact", head: true })

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
      retentionCleanup:
        retentionErrorByFiledAt?.message ||
        retentionErrorNullFiledAt?.message ||
        "ok",
      rawFilingsCount: rawCountError ? null : rawFilingsCount,
      fetchedAt,
      allowedForms: Array.from(allowedForms),
      diagnostics,
      sampleInsertedRows,
      results: includeResults ? results : undefined,
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