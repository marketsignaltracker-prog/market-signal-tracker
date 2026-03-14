import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CompanyTickerRow = {
  id?: number | null
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  last_screened_at?: string | null
  is_eligible?: boolean | null
}

type CandidateHistoryTickerRow = {
  ticker: string
  cik: string | null
  name: string | null
  candidate_score?: number | null
  included?: boolean | null
  last_screened_at?: string | null
  screened_on: string
}

type SecSubmissionRecent = {
  accessionNumber?: string[]
  filingDate?: string[]
  form?: string[]
  primaryDocument?: string[]
}

type SecSubmissionResponse = {
  cik?: string
  name?: string
  tickers?: string[]
  filings?: {
    recent?: SecSubmissionRecent
  }
}

type RawFilingInsertRow = {
  ticker: string
  company_name: string | null
  form_type: string | null
  filed_at: string | null
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

type Diagnostics = {
  sourceScope: "all" | "eligible" | "candidates"
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  companyRowsLoaded: number
  candidateRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
  candidateRowsWithoutCik: number
  secSubmissionsFetched: number
  secSubmissionsFailed: number
  filingRowsBuilt: number
  filingRowsInserted: number
  unsupportedFormsSkipped: number
  duplicateRowsCollapsed: number
}

const DEFAULT_BATCH = 100
const MAX_BATCH = 150
const DEFAULT_START = 0
const RETENTION_DAYS = 30
const CANDIDATE_LOOKBACK_DAYS = 10
const MIN_CANDIDATE_SCORE = 65
const SEC_TIMEOUT_MS = 5000
const DB_CHUNK_SIZE = 100
const SEC_FETCH_CONCURRENCY = 6

const ALLOWED_FORMS = new Set([
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

const SEC_USER_AGENT =
  process.env.SEC_USER_AGENT ||
  "Market Signal Tracker marketsignaltracker@gmail.com"

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function normalizeCik(cik: string | null | undefined) {
  const digits = String(cik || "").replace(/\D/g, "")
  return digits || null
}

function normalizeFormType(formType: string | null) {
  const normalized = (formType || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/^FORM\s+/i, "")

  if (normalized === "8K") return "8-K"
  if (normalized === "6K") return "6-K"
  if (normalized === "4A" || normalized === "4 /A") return "4/A"
  if (normalized === "13DA" || normalized === "SCHEDULE 13D/A") return "13D/A"
  if (normalized === "13GA" || normalized === "SCHEDULE 13G/A") return "13G/A"
  if (normalized === "SCHEDULE 13D") return "13D"
  if (normalized === "SCHEDULE 13G") return "13G"
  if (normalized === "SC13D") return "SC 13D"
  if (normalized === "SC13D/A") return "SC 13D/A"
  if (normalized === "SC13G") return "SC 13G"
  if (normalized === "SC13G/A") return "SC 13G/A"

  return normalized
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number
) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
      cache: "no-store",
    })
  } finally {
    clearTimeout(timeout)
  }
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
        sampleKeys: sampleKeyBuilder
          ? chunk.slice(0, 10).map(sampleKeyBuilder)
          : undefined,
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
    { length: Math.min(concurrency, Math.max(1, items.length)) },
    () => runner()
  )

  await Promise.all(runners)
  return results
}

function buildSubmissionUrl(cik: string) {
  const padded = cik.padStart(10, "0")
  return `https://data.sec.gov/submissions/CIK${padded}.json`
}

function buildFilingUrl(
  cik: string,
  accessionNo: string,
  primaryDoc: string | null
) {
  const normalizedCik = String(Number(cik))
  const accessionNoNoDash = accessionNo.replace(/-/g, "")

  if (!primaryDoc) {
    return `https://www.sec.gov/Archives/edgar/data/${normalizedCik}/${accessionNoNoDash}/`
  }

  return `https://www.sec.gov/Archives/edgar/data/${normalizedCik}/${accessionNoNoDash}/${primaryDoc}`
}

async function fetchSecSubmission(
  cik: string
): Promise<SecSubmissionResponse | null> {
  try {
    const url = buildSubmissionUrl(cik)
    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          "User-Agent": SEC_USER_AGENT,
          Accept: "application/json,text/plain,*/*",
        },
      },
      SEC_TIMEOUT_MS
    )

    if (!res.ok) return null
    return (await res.json()) as SecSubmissionResponse
  } catch {
    return null
  }
}

function extractRecentFilingsFromSubmission(params: {
  submission: SecSubmissionResponse
  fallbackTicker: string
  fallbackCompanyName: string | null
  fallbackCik: string
  nowIso: string
}) {
  const { submission, fallbackTicker, fallbackCompanyName, fallbackCik, nowIso } =
    params

  const recent = submission.filings?.recent
  if (!recent) return []

  const accessionNumbers = recent.accessionNumber || []
  const filingDates = recent.filingDate || []
  const forms = recent.form || []
  const primaryDocs = recent.primaryDocument || []

  const ticker =
    normalizeTicker(submission.tickers?.[0]) || normalizeTicker(fallbackTicker)
  const companyName = submission.name?.trim() || fallbackCompanyName || null
  const cik = normalizeCik(submission.cik) || normalizeCik(fallbackCik)

  if (!ticker || !cik) return []

  const rows: RawFilingInsertRow[] = []

  for (let i = 0; i < accessionNumbers.length; i += 1) {
    const accessionNo = String(accessionNumbers[i] || "").trim()
    const filedAt = String(filingDates[i] || "").trim() || null
    const formType = normalizeFormType(String(forms[i] || "").trim() || null)
    const primaryDoc = String(primaryDocs[i] || "").trim() || null

    if (!accessionNo || !formType) continue
    if (!ALLOWED_FORMS.has(formType)) continue

    rows.push({
      ticker,
      company_name: companyName,
      form_type: formType,
      filed_at: filedAt,
      filing_url: buildFilingUrl(cik, accessionNo, primaryDoc),
      accession_no: accessionNo,
      cik,
      primary_doc: primaryDoc,
      fetched_at: nowIso,
    })
  }

  return rows
}

function dedupeFilings(rows: RawFilingInsertRow[]) {
  const byKey = new Map<string, RawFilingInsertRow>()

  for (const row of rows) {
    const key = `${row.accession_no}:${normalizeTicker(row.ticker)}`
    if (!byKey.has(key)) {
      byKey.set(key, row)
    }
  }

  return [...byKey.values()]
}

async function loadCandidateContext(
  supabase: any,
  start: number,
  batch: number,
  candidateCutoffDateString: string
): Promise<{
  candidateRows: CompanyTickerRow[]
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
}> {
  const universeQuery = await supabase
    .from("candidate_universe")
    .select(
      "company_id, ticker, cik, name, is_active, candidate_score, included, last_screened_at"
    )
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .gte("last_screened_at", candidateCutoffDateString)
    .order("candidate_score", { ascending: false })
    .range(start, start + batch - 1)

  if (universeQuery.error) {
    throw new Error(
      `candidate_universe load failed: ${universeQuery.error.message}`
    )
  }

  const universeRows = (universeQuery.data || []) as CompanyTickerRow[]

  if (universeRows.length >= Math.min(25, batch)) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateHistoryRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
    }
  }

  const latestScreened = await supabase
    .from("candidate_screen_history")
    .select("screened_on")
    .order("screened_on", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (latestScreened.error) {
    throw new Error(
      `candidate_screen_history latest snapshot lookup failed: ${latestScreened.error.message}`
    )
  }

  const screenedOn = latestScreened.data?.screened_on ?? null
  if (!screenedOn) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateHistoryRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
    }
  }

  const historyQuery = await supabase
    .from("candidate_screen_history")
    .select(
      "ticker, cik, name, candidate_score, included, last_screened_at, screened_on"
    )
    .eq("screened_on", screenedOn)
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .order("candidate_score", { ascending: false })
    .range(start, start + batch - 1)

  if (historyQuery.error) {
    throw new Error(
      `candidate_screen_history snapshot load failed: ${historyQuery.error.message}`
    )
  }

  const historyRows = (historyQuery.data || []) as CandidateHistoryTickerRow[]

  const deduped = new Map<string, CompanyTickerRow>()
  for (const row of universeRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    deduped.set(ticker, row)
  }

  for (const row of historyRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!deduped.has(ticker)) {
      deduped.set(ticker, {
        ticker: row.ticker,
        cik: row.cik,
        name: row.name,
        candidate_score: row.candidate_score,
        included: row.included,
        last_screened_at: row.last_screened_at,
      })
    }
  }

  return {
    candidateRows: [...deduped.values()].slice(0, batch),
    candidateUniverseRowsLoaded: universeRows.length,
    candidateHistoryRowsLoaded: historyRows.length,
    fallbackCandidateSourceUsed: historyRows.length > 0,
  }
}

async function loadAllCompaniesContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<{
  rows: CompanyTickerRow[]
  companyRowsLoaded: number
}> {
  let query = supabase
    .from("companies")
    .select("id, ticker, cik, name, is_active")
    .not("cik", "is", null)
    .order("id", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query

  if (error) {
    throw new Error(`companies load failed: ${error.message}`)
  }

  const rows = ((data || []) as CompanyTickerRow[]).map((row) => ({
    id: row.id ?? null,
    ticker: row.ticker,
    cik: row.cik,
    name: row.name,
    is_active: row.is_active ?? true,
  }))

  return {
    rows,
    companyRowsLoaded: rows.length,
  }
}

async function loadEligibleContext(
  supabase: any,
  start: number,
  batch: number,
  onlyActive: boolean
): Promise<{
  rows: CompanyTickerRow[]
  candidateUniverseRowsLoaded: number
}> {
  let query = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, is_eligible")
    .eq("is_eligible", true)
    .not("cik", "is", null)
    .order("ticker", { ascending: true })
    .range(start, start + batch - 1)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query

  if (error) {
    throw new Error(`candidate_universe eligible load failed: ${error.message}`)
  }

  const rows = ((data || []) as CompanyTickerRow[]).map((row) => ({
    id: row.company_id ?? null,
    company_id: row.company_id ?? null,
    ticker: row.ticker,
    cik: row.cik,
    name: row.name,
    is_active: row.is_active ?? true,
    is_eligible: true,
  }))

  return {
    rows,
    candidateUniverseRowsLoaded: rows.length,
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

    const scopeParam = (searchParams.get("scope") || "candidates").toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), DEFAULT_START))
    const batch = Math.min(
      Math.max(1, parseInteger(searchParams.get("batch"), DEFAULT_BATCH)),
      MAX_BATCH
    )
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"
    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"

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

    const now = new Date()
    const nowIso = now.toISOString()

    const candidateCutoffDate = new Date(now)
    candidateCutoffDate.setDate(
      candidateCutoffDate.getDate() - CANDIDATE_LOOKBACK_DAYS
    )
    const candidateCutoffDateString = candidateCutoffDate.toISOString()

    const diagnostics: Diagnostics = {
      sourceScope: scope,
      candidateUniverseRowsLoaded: 0,
      candidateHistoryRowsLoaded: 0,
      companyRowsLoaded: 0,
      candidateRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
      candidateRowsWithoutCik: 0,
      secSubmissionsFetched: 0,
      secSubmissionsFailed: 0,
      filingRowsBuilt: 0,
      filingRowsInserted: 0,
      unsupportedFormsSkipped: 0,
      duplicateRowsCollapsed: 0,
    }

    let sourceRows: CompanyTickerRow[] = []

    if (scope === "all") {
      const allContext = await loadAllCompaniesContext(
        supabase,
        start,
        batch,
        onlyActive
      )
      sourceRows = allContext.rows
      diagnostics.companyRowsLoaded = allContext.companyRowsLoaded
      diagnostics.candidateRowsLoaded = allContext.rows.length
    }

    if (scope === "eligible") {
      const eligibleContext = await loadEligibleContext(
        supabase,
        start,
        batch,
        onlyActive
      )
      sourceRows = eligibleContext.rows
      diagnostics.candidateUniverseRowsLoaded =
        eligibleContext.candidateUniverseRowsLoaded
      diagnostics.candidateRowsLoaded = eligibleContext.rows.length
    }

    if (scope === "candidates") {
      const candidateContext = await loadCandidateContext(
        supabase,
        start,
        batch,
        candidateCutoffDateString
      )

      sourceRows = candidateContext.candidateRows
      diagnostics.candidateUniverseRowsLoaded =
        candidateContext.candidateUniverseRowsLoaded
      diagnostics.candidateHistoryRowsLoaded =
        candidateContext.candidateHistoryRowsLoaded
      diagnostics.candidateRowsLoaded = candidateContext.candidateRows.length
      diagnostics.fallbackCandidateSourceUsed =
        candidateContext.fallbackCandidateSourceUsed
    }

    const filingFetchResults = await mapWithConcurrency(
      sourceRows,
      SEC_FETCH_CONCURRENCY,
      async (candidate) => {
        const ticker = normalizeTicker(candidate.ticker)
        const cik = normalizeCik(candidate.cik)

        if (!ticker || !cik) {
          return {
            missingCik: true,
            fetched: false,
            rows: [] as RawFilingInsertRow[],
            unsupportedFormsSkipped: 0,
          }
        }

        const submission = await fetchSecSubmission(cik)

        if (!submission) {
          return {
            missingCik: false,
            fetched: false,
            rows: [] as RawFilingInsertRow[],
            unsupportedFormsSkipped: 0,
          }
        }

        const extracted = extractRecentFilingsFromSubmission({
          submission,
          fallbackTicker: ticker,
          fallbackCompanyName: candidate.name,
          fallbackCik: cik,
          nowIso,
        })

        let unsupportedForThisSubmission = 0
        const recent = submission.filings?.recent

        if (recent?.form?.length) {
          for (const rawForm of recent.form) {
            const normalized = normalizeFormType(
              String(rawForm || "").trim() || null
            )
            if (!normalized) continue
            if (!ALLOWED_FORMS.has(normalized)) unsupportedForThisSubmission += 1
          }
        }

        return {
          missingCik: false,
          fetched: true,
          rows: extracted,
          unsupportedFormsSkipped: unsupportedForThisSubmission,
        }
      }
    )

    const rawRows: RawFilingInsertRow[] = []

    for (const result of filingFetchResults) {
      if (result.missingCik) {
        diagnostics.candidateRowsWithoutCik += 1
        continue
      }

      if (!result.fetched) {
        diagnostics.secSubmissionsFailed += 1
        continue
      }

      diagnostics.secSubmissionsFetched += 1
      diagnostics.unsupportedFormsSkipped += result.unsupportedFormsSkipped
      diagnostics.filingRowsBuilt += result.rows.length
      rawRows.push(...result.rows)
    }

    const dedupedRows = dedupeFilings(rawRows)
    diagnostics.duplicateRowsCollapsed = rawRows.length - dedupedRows.length

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
          error: "Failed writing one or more raw filing chunks to Supabase",
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
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("raw_filings")
        .delete()
        .lt("filed_at", retentionCutoffString)

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    const nextStart = sourceRows.length < batch ? null : start + batch

    return Response.json({
      ok: true,
      scope,
      start,
      batch,
      nextStart,
      filingRowsInserted: diagnostics.filingRowsInserted,
      retainedDays: RETENTION_DAYS,
      retentionCleanup: retentionMessage,
      diagnostics,
      message:
        scope === "all"
          ? "Raw filing metadata ingested for all companies in the requested batch."
          : scope === "eligible"
            ? "Raw filing metadata ingested for eligible-universe companies in the requested batch."
            : "Raw filing metadata ingested for candidate tickers only. Filing parsing and scoring are handled in a separate filing-signals route.",
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