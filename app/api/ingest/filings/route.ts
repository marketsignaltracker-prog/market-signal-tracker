import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

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
}

type CandidateHistoryRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
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
  primaryDocDescription?: string[]
  reportDate?: string[]
}

type SecSubmissionFile = {
  name?: string
  filingCount?: number
  filingFrom?: string
  filingTo?: string
}

type SecSubmissionJson = {
  cik?: string
  name?: string
  tickers?: string[]
  filings?: {
    recent?: SecSubmissionRecent
    files?: SecSubmissionFile[]
  }
}

type Diagnostics = {
  scope: string
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
  duplicateRowsCollapsed: number
}

const DEFAULT_BATCH = 15
const MAX_BATCH = 50
const DB_CHUNK_SIZE = 250
const RETENTION_DAYS = 30
const MAX_INTERNAL_BATCHES_PER_RUN = 2

const SUPPORTED_FORMS = new Set([
  "3",
  "3/A",
  "4",
  "4/A",
  "5",
  "5/A",
])

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

  if (normalized === "4A" || normalized === "4 /A") return "4/A"
  if (normalized === "3A" || normalized === "3 /A") return "3/A"
  if (normalized === "5A" || normalized === "5 /A") return "5/A"

  return normalized
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function buildSecSubmissionsUrl(cik: string) {
  const padded = padCik10(cik)
  if (!padded) return null
  return `https://data.sec.gov/submissions/CIK${padded}.json`
}

function buildFilingUrl(cik: string | null | undefined, accessionNo: string, primaryDoc: string | null | undefined) {
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
      "Accept-Encoding": "gzip, deflate",
      Accept: "application/json, text/plain, */*",
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
}) {
  const { submission, sourceRow, fetchedAt } = params
  const recent = submission.filings?.recent

  if (!recent) {
    return {
      rows: [] as RawFilingInsertRow[],
      unsupportedFormsSkipped: 0,
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

  // 🔥 30-day cutoff
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - 30)

  for (let i = 0; i < maxLen; i += 1) {
    const accessionNo = String(accessionNumbers[i] || "").trim()
    const filedAtRaw = String(filingDates[i] || "").trim()
    const rawForm = String(forms[i] || "").trim()
    const formType = normalizeFormType(rawForm)
    const primaryDoc = String(primaryDocuments[i] || "").trim() || null

    if (!accessionNo || !formType || !filedAtRaw) continue

    const filedAtDate = new Date(filedAtRaw)

    // ⛔ Skip anything older than 30 days
    if (filedAtDate < cutoff) continue

    // ⛔ Only keep insider forms
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

  for (let i = 0; i < maxLen; i += 1) {
    const accessionNo = String(accessionNumbers[i] || "").trim()
    const filedAt = String(filingDates[i] || "").trim() || null
    const rawForm = String(forms[i] || "").trim()
    const formType = normalizeFormType(rawForm)
    const primaryDoc = String(primaryDocuments[i] || "").trim() || null

    if (!accessionNo || !formType) continue

    if (!shouldKeepForm(formType)) {
      unsupportedFormsSkipped += 1
      continue
    }

    rows.push({
      company_id: sourceRow.company_id,
      ticker: sourceRow.ticker,
      company_name: sourceRow.name ?? submission.name ?? null,
      filed_at: filedAt,
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

async function loadCandidateContextSourceRows(
  supabase: any,
  scope: string,
  start: number,
  batch: number
): Promise<{
  sourceRows: SourceRow[]
  diagnostics: Pick<
    Diagnostics,
    | "companiesRowsLoaded"
    | "candidateUniverseRowsLoaded"
    | "candidateScreenHistoryRowsLoaded"
    | "sourceRowsLoaded"
    | "fallbackCandidateHistoryUsed"
    | "sourceRowsWithoutCik"
  >
}> {
  if (scope === "eligible") {
    const { data, error } = await supabase
      .from("candidate_universe")
      .select("company_id,ticker,cik,name,is_active")
      .eq("is_eligible", true)
      .order("ticker", { ascending: true })
      .range(start, start + batch - 1)

    if (error) {
      throw new Error(`candidate_universe load failed: ${error.message}`)
    }

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

  if (scope === "screened") {
    const latestScreenedQuery = await supabase
      .from("candidate_screen_history")
      .select("screened_on")
      .order("screened_on", { ascending: false })
      .limit(1)
      .maybeSingle()

    if (latestScreenedQuery.error) {
      throw new Error(
        `candidate_screen_history latest snapshot lookup failed: ${latestScreenedQuery.error.message}`
      )
    }

    const latestScreenedOn = latestScreenedQuery.data?.screened_on ?? null
    if (!latestScreenedOn) {
      return {
        sourceRows: [],
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

    const { data, error } = await supabase
      .from("candidate_screen_history")
      .select("company_id,ticker,cik,name,is_active")
      .eq("screened_on", latestScreenedOn)
      .order("ticker", { ascending: true })
      .range(start, start + batch - 1)

    if (error) {
      throw new Error(`candidate_screen_history load failed: ${error.message}`)
    }

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
        fallbackCandidateHistoryUsed: false,
        sourceRowsWithoutCik: typedRows.length - sourceRows.length,
      },
    }
  }

  const { data, error } = await supabase
    .from("companies")
    .select("id,ticker,cik,name,is_active")
    .order("id", { ascending: true })
    .range(start, start + batch - 1)

  if (error) {
    throw new Error(`companies load failed: ${error.message}`)
  }

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
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const { searchParams } = new URL(request.url)

    const scope = (searchParams.get("scope") || "all").toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), 0))
    const batch = Math.min(
      Math.max(1, parseInteger(searchParams.get("batch"), DEFAULT_BATCH)),
      MAX_BATCH
    )
    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const overallDiagnostics: Diagnostics = {
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
      duplicateRowsCollapsed: 0,
    }

    let currentStart = start
    let internalRuns = 0
    let nextStart: number | null = null

    while (internalRuns < MAX_INTERNAL_BATCHES_PER_RUN) {
      const sourceLoad = await loadCandidateContextSourceRows(
        supabase,
        scope,
        currentStart,
        batch
      )

      overallDiagnostics.companiesRowsLoaded += sourceLoad.diagnostics.companiesRowsLoaded
      overallDiagnostics.candidateUniverseRowsLoaded += sourceLoad.diagnostics.candidateUniverseRowsLoaded
      overallDiagnostics.candidateScreenHistoryRowsLoaded += sourceLoad.diagnostics.candidateScreenHistoryRowsLoaded
      overallDiagnostics.sourceRowsLoaded += sourceLoad.diagnostics.sourceRowsLoaded
      overallDiagnostics.sourceRowsWithoutCik += sourceLoad.diagnostics.sourceRowsWithoutCik
      overallDiagnostics.fallbackCandidateHistoryUsed =
        overallDiagnostics.fallbackCandidateHistoryUsed ||
        sourceLoad.diagnostics.fallbackCandidateHistoryUsed

      const sourceRows = sourceLoad.sourceRows

      if (sourceRows.length === 0) {
        nextStart = null
        break
      }

      const fetchedAt = nowIso()
      const chunkBuiltRows: RawFilingInsertRow[] = []

      for (const sourceRow of sourceRows) {
        try {
          const submission = await fetchSecSubmissions(sourceRow.cik)
          overallDiagnostics.secSubmissionsFetched += 1

          const built = buildRecentRowsFromSubmission({
            submission,
            sourceRow,
            fetchedAt,
          })

          overallDiagnostics.unsupportedFormsSkipped += built.unsupportedFormsSkipped
          chunkBuiltRows.push(...built.rows)
        } catch {
          overallDiagnostics.secSubmissionsFailed += 1
        }
      }

      overallDiagnostics.filingRowsBuilt += chunkBuiltRows.length

      const dedupedRows = dedupeFilingRows(chunkBuiltRows)
      overallDiagnostics.duplicateRowsCollapsed +=
        chunkBuiltRows.length - dedupedRows.length

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
              diagnostics: overallDiagnostics,
              errorSamples: writeResult.errors.slice(0, 5),
              currentStart,
              batch,
              internalRuns,
            },
          },
          { status: 500 }
        )
      }

      overallDiagnostics.filingRowsInserted += writeResult.insertedOrUpdated

      if (sourceRows.length < batch) {
        nextStart = null
        break
      }

      currentStart += batch
      nextStart = currentStart
      internalRuns += 1
    }

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date()
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("raw_filings")
        .delete()
        .lt("filed_at", retentionCutoffString)

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    let filingCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_filings")
        .select("*", { count: "exact", head: true })

      filingCount = error ? null : count ?? 0
    }

    return Response.json({
      ok: true,
      stage: "filings",
      targetTable: "raw_filings",
      scope,
      start,
      batch,
      internalRuns: internalRuns + 1,
      nextStart,
      retainedDays: RETENTION_DAYS,
      retentionCleanup: retentionMessage,
      filingRowsInserted: overallDiagnostics.filingRowsInserted,
      filingCount,
      diagnostics: overallDiagnostics,
      message:
        nextStart === null
          ? "Insider filings ingestion completed for the remaining source rows in this run."
          : "Insider filings ingestion advanced multiple internal batches in this run.",
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