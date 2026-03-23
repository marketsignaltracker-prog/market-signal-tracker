import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type Scope = "all" | "eligible" | "screened"

type CompanyRow = {
  id: number
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  included?: boolean | null
  is_eligible?: boolean | null
  updated_at?: string | null
}

type CandidateHistoryRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  included?: boolean | null
  screened_on?: string | null
}

type SourceRow = {
  company_id: number | null
  ticker: string
  cik: string
  name: string | null
  is_active: boolean
}

type RawFilingInsertRow = {
  company_id: number | null
  ticker: string
  company_name: string | null
  filed_at: string | null
  form_type: string | null
  filing_url: string | null
  accession_no: string
  cik: string | null
  primary_doc: string | null
  fetched_at: string
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

type SecSubmissionRecent = {
  accessionNumber?: string[]
  filingDate?: string[]
  form?: string[]
  primaryDocument?: string[]
}

type SecSubmissionJson = {
  cik?: string
  name?: string
  tickers?: string[]
  filings?: {
    recent?: SecSubmissionRecent
  }
}

type Diagnostics = {
  scope: Scope
  companiesRowsLoaded: number
  candidateUniverseRowsLoaded: number
  candidateScreenHistoryRowsLoaded: number
  sourceRowsLoaded: number
  fallbackCandidateHistoryUsed: boolean
  sourceRowsWithoutCik: number
  secSubmissionsFetched: number
  secSubmissionsFailed: number
  filingRowsBuilt: number
  filingRowsInserted: number
  unsupportedFormsSkipped: number
  olderThanCutoffSkipped: number
  duplicateRowsCollapsed: number
}

const DEFAULT_SCOPE: Scope = "eligible"
const DEFAULT_BATCH = 50
const MAX_BATCH = 200
const DB_CHUNK_SIZE = 250
const RETENTION_DAYS = 30

const SUPPORTED_FORMS = new Set(["3", "3/A", "4", "4/A", "5", "5/A"])

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
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  const t = (ticker || "").trim().toUpperCase()
  return t || null
}

function normalizeCik(cik: string | number | null | undefined) {
  const digits = String(cik || "").replace(/\D/g, "")
  return digits || null
}

function padCik10(cik: string | number | null | undefined) {
  const normalized = normalizeCik(cik)
  if (!normalized) return null
  return normalized.padStart(10, "0")
}

function nowIso() {
  return new Date().toISOString()
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function normalizeFormType(formType: string | null | undefined) {
  const normalized = (formType || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/^FORM\s+/i, "")

  if (normalized === "3A" || normalized === "3 /A") return "3/A"
  if (normalized === "4A" || normalized === "4 /A") return "4/A"
  if (normalized === "5A" || normalized === "5 /A") return "5/A"

  return normalized
}

function buildSecSubmissionsUrl(cik: string) {
  const padded = padCik10(cik)
  if (!padded) return null
  return `https://data.sec.gov/submissions/CIK${padded}.json`
}

function buildFilingUrl(
  cik: string | null | undefined,
  accessionNo: string,
  primaryDoc: string | null | undefined
) {
  const normalizedCik = normalizeCik(cik)
  const normalizedAccession = String(accessionNo || "").replace(/-/g, "").trim()
  const normalizedPrimaryDoc = String(primaryDoc || "").trim()

  if (!normalizedCik || !normalizedAccession || !normalizedPrimaryDoc) return null

  return `https://www.sec.gov/Archives/edgar/data/${normalizedCik}/${normalizedAccession}/${normalizedPrimaryDoc}`
}

function shouldKeepForm(formType: string) {
  return SUPPORTED_FORMS.has(formType)
}

function buildSourceRowFromCompany(row: CompanyRow): SourceRow | null {
  const ticker = normalizeTicker(row.ticker)
  const cik = normalizeCik(row.cik)

  if (!ticker || !cik) return null

  return {
    company_id: row.id,
    ticker,
    cik,
    name: row.name ?? null,
    is_active: row.is_active ?? true,
  }
}

function buildSourceRowFromCandidateUniverse(row: CandidateUniverseRow): SourceRow | null {
  const ticker = normalizeTicker(row.ticker)
  const cik = normalizeCik(row.cik)

  if (!ticker || !cik) return null

  return {
    company_id: row.company_id ?? null,
    ticker,
    cik,
    name: row.name ?? null,
    is_active: row.is_active ?? true,
  }
}

function buildSourceRowFromCandidateHistory(row: CandidateHistoryRow): SourceRow | null {
  const ticker = normalizeTicker(row.ticker)
  const cik = normalizeCik(row.cik)

  if (!ticker || !cik) return null

  return {
    company_id: row.company_id ?? null,
    ticker,
    cik,
    name: row.name ?? null,
    is_active: row.is_active ?? true,
  }
}

async function fetchSecSubmissions(cik: string): Promise<SecSubmissionJson> {
  const url = buildSecSubmissionsUrl(cik)

  if (!url) {
    throw new Error("Missing or invalid CIK")
  }

  const response = await fetch(url, {
    method: "GET",
    cache: "no-store",
    headers: {
      "User-Agent": "Market Signal Tracker support@marketsignaltracker.com",
      Accept: "application/json, text/plain, */*",
      "Accept-Encoding": "gzip, deflate",
      Host: "data.sec.gov",
    },
  })

  if (!response.ok) {
    throw new Error(`SEC submissions request failed with status ${response.status}`)
  }

  return (await response.json()) as SecSubmissionJson
}

function buildRecentRowsFromSubmission(params: {
  submission: SecSubmissionJson
  sourceRow: SourceRow
  fetchedAt: string
  lookbackDays: number
}) {
  const { submission, sourceRow, fetchedAt, lookbackDays } = params
  const recent = submission.filings?.recent

  if (!recent) {
    return {
      rows: [] as RawFilingInsertRow[],
      unsupportedFormsSkipped: 0,
      olderThanCutoffSkipped: 0,
    }
  }

  const accessionNumbers = recent.accessionNumber || []
  const filingDates = recent.filingDate || []
  const forms = recent.form || []
  const primaryDocuments = recent.primaryDocument || []

  const maxLen = Math.max(
    accessionNumbers.length,
    filingDates.length,
    forms.length,
    primaryDocuments.length
  )

  const rows: RawFilingInsertRow[] = []
  let unsupportedFormsSkipped = 0
  let olderThanCutoffSkipped = 0

  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - lookbackDays)

  for (let i = 0; i < maxLen; i += 1) {
    const accessionNo = String(accessionNumbers[i] || "").trim()
    const filedAtRaw = String(filingDates[i] || "").trim()
    const rawForm = String(forms[i] || "").trim()
    const formType = normalizeFormType(rawForm)
    const primaryDoc = String(primaryDocuments[i] || "").trim() || null

    if (!accessionNo || !formType || !filedAtRaw) {
      continue
    }

    const filedAtDate = new Date(`${filedAtRaw}T00:00:00.000Z`)
    if (Number.isNaN(filedAtDate.getTime())) {
      continue
    }

    if (filedAtDate < cutoff) {
      olderThanCutoffSkipped += 1
      continue
    }

    if (!shouldKeepForm(formType)) {
      unsupportedFormsSkipped += 1
      continue
    }

    rows.push({
      company_id: sourceRow.company_id,
      ticker: sourceRow.ticker,
      company_name: sourceRow.name ?? submission.name ?? null,
      filed_at: filedAtRaw,
      form_type: formType,
      filing_url: buildFilingUrl(sourceRow.cik, accessionNo, primaryDoc),
      accession_no: accessionNo,
      cik: sourceRow.cik,
      primary_doc: primaryDoc,
      fetched_at: fetchedAt,
    })
  }

  return {
    rows,
    unsupportedFormsSkipped,
    olderThanCutoffSkipped,
  }
}

function dedupeFilingRows(rows: RawFilingInsertRow[]) {
  const map = new Map<string, RawFilingInsertRow>()

  for (const row of rows) {
    const key = `${row.accession_no}::${row.form_type || ""}::${row.primary_doc || ""}`

    if (!map.has(key)) {
      map.set(key, row)
      continue
    }

    const existing = map.get(key)!
    map.set(key, {
      ...existing,
      company_name: existing.company_name || row.company_name,
      filing_url: existing.filing_url || row.filing_url,
      primary_doc: existing.primary_doc || row.primary_doc,
      filed_at: existing.filed_at || row.filed_at,
      fetched_at: row.fetched_at || existing.fetched_at,
    })
  }

  return Array.from(map.values())
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
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

async function loadCompaniesContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
) {
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

  const typedRows = (data || []) as CompanyRow[]
  const sourceRows = typedRows
    .map(buildSourceRowFromCompany)
    .filter((row): row is SourceRow => row !== null)

  return {
    sourceRows,
    diagnostics: {
      companiesRowsLoaded: typedRows.length,
      candidateUniverseRowsLoaded: 0,
      candidateScreenHistoryRowsLoaded: 0,
      sourceRowsLoaded: sourceRows.length,
      fallbackCandidateHistoryUsed: false,
      sourceRowsWithoutCik: typedRows.length - sourceRows.length,
    },
  }
}

async function loadEligibleContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
) {
  let query = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, included, is_eligible, updated_at")
    .or("included.eq.true,is_eligible.eq.true")
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) throw error

  const typedRows = (data || []) as CandidateUniverseRow[]
  const sourceRows = typedRows
    .map(buildSourceRowFromCandidateUniverse)
    .filter((row): row is SourceRow => row !== null)

  return {
    sourceRows,
    diagnostics: {
      companiesRowsLoaded: 0,
      candidateUniverseRowsLoaded: typedRows.length,
      candidateScreenHistoryRowsLoaded: 0,
      sourceRowsLoaded: sourceRows.length,
      fallbackCandidateHistoryUsed: false,
      sourceRowsWithoutCik: typedRows.length - sourceRows.length,
    },
  }
}

async function loadScreenedContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
) {
  const latestSnapshot = await supabase
    .from("candidate_screen_history")
    .select("screened_on")
    .order("screened_on", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (latestSnapshot.error) throw latestSnapshot.error

  const latestScreenedOn = latestSnapshot.data?.screened_on ?? null
  if (!latestScreenedOn) {
    return {
      sourceRows: [] as SourceRow[],
      diagnostics: {
        companiesRowsLoaded: 0,
        candidateUniverseRowsLoaded: 0,
        candidateScreenHistoryRowsLoaded: 0,
        sourceRowsLoaded: 0,
        fallbackCandidateHistoryUsed: false,
        sourceRowsWithoutCik: 0,
      },
    }
  }

  let query = supabase
    .from("candidate_screen_history")
    .select("company_id, ticker, cik, name, included, screened_on, is_active")
    .eq("screened_on", latestScreenedOn)
    .eq("included", true)
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) throw error

  const typedRows = (data || []) as CandidateHistoryRow[]
  const sourceRows = typedRows
    .map(buildSourceRowFromCandidateHistory)
    .filter((row): row is SourceRow => row !== null)

  return {
    sourceRows,
    diagnostics: {
      companiesRowsLoaded: 0,
      candidateUniverseRowsLoaded: 0,
      candidateScreenHistoryRowsLoaded: typedRows.length,
      sourceRowsLoaded: sourceRows.length,
      fallbackCandidateHistoryUsed: typedRows.length > 0,
      sourceRowsWithoutCik: typedRows.length - sourceRows.length,
    },
  }
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  try {
    const supabase = getSupabaseAdmin()
    const { searchParams } = new URL(request.url)

    const scopeParam = (searchParams.get("scope") || DEFAULT_SCOPE).toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), 0))
    const batch = Math.min(
      Math.max(1, parseInteger(searchParams.get("batch"), DEFAULT_BATCH)),
      MAX_BATCH
    )
    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"
    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"
    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), RETENTION_DAYS)),
      90
    )

    if (!["all", "eligible", "screened"].includes(scopeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, screened`,
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
      sourceRowsWithoutCik: 0,
      secSubmissionsFetched: 0,
      secSubmissionsFailed: 0,
      filingRowsBuilt: 0,
      filingRowsInserted: 0,
      unsupportedFormsSkipped: 0,
      olderThanCutoffSkipped: 0,
      duplicateRowsCollapsed: 0,
    }

    let sourceRows: SourceRow[] = []

    if (scope === "all") {
      const context = await loadCompaniesContext(supabase, start, batch, onlyActive)
      sourceRows = context.sourceRows
      Object.assign(diagnostics, {
        ...diagnostics,
        ...context.diagnostics,
      })
    }

    if (scope === "eligible") {
      const context = await loadEligibleContext(supabase, start, batch, onlyActive)
      sourceRows = context.sourceRows
      Object.assign(diagnostics, {
        ...diagnostics,
        ...context.diagnostics,
      })
    }

    if (scope === "screened") {
      const context = await loadScreenedContext(supabase, start, batch, onlyActive)
      sourceRows = context.sourceRows
      Object.assign(diagnostics, {
        ...diagnostics,
        ...context.diagnostics,
      })
    }

    const fetchedAt = nowIso()
    const builtRows: RawFilingInsertRow[] = []

    // Process tickers with concurrency (SEC EDGAR allows ~10 req/sec)
    const SEC_CONCURRENCY = 8
    for (let i = 0; i < sourceRows.length; i += SEC_CONCURRENCY) {
      const chunk = sourceRows.slice(i, i + SEC_CONCURRENCY)
      const results = await Promise.allSettled(
        chunk.map(async (sourceRow) => {
          const submission = await fetchSecSubmissions(sourceRow.cik)
          return buildRecentRowsFromSubmission({
            submission,
            sourceRow,
            fetchedAt,
            lookbackDays,
          })
        })
      )
      for (const result of results) {
        if (result.status === "fulfilled") {
          diagnostics.secSubmissionsFetched += 1
          diagnostics.unsupportedFormsSkipped += result.value.unsupportedFormsSkipped
          diagnostics.olderThanCutoffSkipped += result.value.olderThanCutoffSkipped
          builtRows.push(...result.value.rows)
        } else {
          diagnostics.secSubmissionsFailed += 1
        }
      }
    }

    diagnostics.filingRowsBuilt = builtRows.length

    const dedupedRows = dedupeFilingRows(builtRows)
    diagnostics.duplicateRowsCollapsed = builtRows.length - dedupedRows.length

    const writeResult =
      dedupedRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("raw_filings"),
            "raw_filings",
            dedupedRows,
            "accession_no",
            (row) => row.accession_no
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (writeResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing raw filings rows",
          debug: {
            diagnostics,
            errorSamples: writeResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.filingRowsInserted = writeResult.insertedOrUpdated

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date()
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("raw_filings")
        .delete()
        .or(
          `filed_at.lt.${retentionCutoffString},filed_at.is.null,form_type.not.in.(3,3/A,4,4/A,5,5/A)`
        )

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    let filingCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_filings")
        .select("*", { count: "exact", head: true })

      filingCount = error ? null : count ?? 0
    }

    const nextStart = sourceRows.length < batch ? null : start + batch

    return Response.json({
      ok: true,
      stage: "filings",
      targetTable: "raw_filings",
      scope,
      start,
      batch,
      nextStart,
      retainedDays: RETENTION_DAYS,
      retentionCleanup: retentionMessage,
      filingRowsInserted: diagnostics.filingRowsInserted,
      filingCount,
      diagnostics,
      message:
        diagnostics.filingRowsInserted === 0
          ? "Insider filings route ran successfully but no recent insider filings were found."
          : "Recent insider filings ingested successfully.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown filings ingest error",
      },
      { status: 500 }
    )
  }
}