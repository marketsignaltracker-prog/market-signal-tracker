import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type Scope = "all" | "eligible" | "candidates"
type SignalMode = "all" | "filings" | "ptrs"

type CompanyRow = {
  id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
  passed?: boolean | null
  as_of_date?: string | null
}

type CandidateScreenHistoryRow = {
  company_id?: number | null
  ticker: string
  cik?: string | null
  name?: string | null
  is_active?: boolean | null
  passed?: boolean | null
  as_of_date?: string | null
}

type ContextRow = CompanyRow | CandidateUniverseRow | CandidateScreenHistoryRow

type RawFilingRow = {
  company_id?: number | null
  ticker: string | null
  company_name?: string | null
  form_type?: string | null
  filed_at?: string | null
  filing_url?: string | null
  accession_no?: string | null
  cik?: string | null
  primary_doc?: string | null
  fetched_at?: string | null
}

type RawPtrTradeRow = {
  ticker: string | null
  politician_name?: string | null
  filer_name?: string | null
  chamber?: string | null
  transaction_type?: string | null
  action?: string | null
  trade_date?: string | null
  disclosure_date?: string | null
  source_url?: string | null
  ptr_url?: string | null
  trade_key?: string | null
  amount_low?: number | null
  amount_high?: number | null
  asset_name?: string | null
}

type CurrentSignalRow = {
  run_id: string
  ticker: string
  company_id: number | null
  signal_type: string
  source_type: string
  strength: number
  direction: "bullish" | "bearish" | "neutral"
  summary: string
  metadata: Record<string, unknown>
  as_of_date: string
  created_at: string
  updated_at: string
}

type SignalHistoryRow = {
  signal_history_key: string
  scored_on: string
  ticker: string
  company_name: string | null
  signal_type: string
  signal_source: string
  signal_category: string
  signal_strength_bucket: string
  signal_tags: string[]
  bias: string
  score: number
  app_score: number
  board_bucket: string
  title: string
  summary: string
  source_form: string | null
  filed_at: string | null
  filing_url: string | null
  accession_no: string | null
  last_scored_at: string
  updated_at: string
  created_at: string
  score_breakdown: Record<string, unknown>
  score_version: string
  stacked_signal_count: number
  run_id: string
  company_id: number | null
  source_type: string
  strength: number
  direction: string
  as_of_date: string
  captured_at: string
  snapshot: Record<string, unknown>
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
  scope: Scope
  mode: SignalMode
  companiesRowsLoaded: number
  candidateUniverseRowsLoaded: number
  candidateScreenHistoryRowsLoaded: number
  sourceRowsLoaded: number
  fallbackCandidateHistoryUsed: boolean
  rawFilingsLoaded: number
  rawPtrTradesLoaded: number
  filingSignalsBuilt: number
  ptrSignalsBuilt: number
  signalsInserted: number
  signalHistoryInserted: number
  filteredBelowMinStrength: number
}

const DEFAULT_LIMIT = 150
const MAX_LIMIT = 300
const DEFAULT_LOOKBACK_DAYS = 21
const MAX_LOOKBACK_DAYS = 60
const DEFAULT_MIN_SIGNAL_STRENGTH = 20
const RETENTION_DAYS = 45
const SCORE_VERSION = "v7-lenient-insider-ptr"
const DB_CHUNK_SIZE = 100
const QUERY_CHUNK_SIZE = 200
const CANDIDATE_LOOKBACK_DAYS = 10

const FILING_SIGNAL_TYPE = "insider_activity"
const PTR_SIGNAL_TYPE = "ptr_activity"

const OWNERSHIP_FORMS = new Set([
  "4",
  "4/A",
  "13D",
  "13D/A",
  "13G",
  "13G/A",
  "SC 13D",
  "SC 13D/A",
  "SC 13G",
  "SC 13G/A",
])

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function uniqueStrings(values: Array<string | null | undefined>) {
  return Array.from(
    new Set(values.map((value) => (value || "").trim()).filter(Boolean))
  )
}

function getDaysAgo(dateValue: string | null | undefined) {
  if (!dateValue) return null
  const parsed = new Date(dateValue)
  if (Number.isNaN(parsed.getTime())) return null

  const now = Date.now()
  const diffMs = now - parsed.getTime()
  return Math.max(0, Math.floor(diffMs / (1000 * 60 * 60 * 24)))
}

function getStrengthBucket(strength: number) {
  if (strength >= 75) return "high"
  if (strength >= 45) return "medium"
  return "low"
}

function getBoardBucket(strength: number) {
  if (strength >= 75) return "High"
  if (strength >= 45) return "Watch"
  return "Early"
}

function buildHistoryKey(
  runId: string,
  ticker: string,
  signalType: string,
  sourceType: string
) {
  return `${runId}::${ticker}::${signalType}::${sourceType}`
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

async function loadCompaniesContext(
  supabase: any,
  limit: number,
  onlyActive: boolean
): Promise<{
  rows: CompanyRow[]
  companiesRowsLoaded: number
}> {
  let query = supabase
    .from("companies")
    .select("id, ticker, cik, name, is_active")
    .not("ticker", "is", null)
    .order("id", { ascending: true })
    .limit(limit)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) throw new Error(`companies load failed: ${error.message}`)

  return {
    rows: (data || []) as CompanyRow[],
    companiesRowsLoaded: (data || []).length,
  }
}

async function loadEligibleContext(
  supabase: any,
  limit: number,
  onlyActive: boolean
): Promise<{
  rows: CandidateUniverseRow[]
  candidateUniverseRowsLoaded: number
}> {
  let query = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, passed, as_of_date")
    .eq("passed", true)
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .limit(limit)

  if (onlyActive) {
    query = query.eq("is_active", true)
  }

  const { data, error } = await query
  if (error) {
    throw new Error(`candidate_universe eligible load failed: ${error.message}`)
  }

  return {
    rows: (data || []) as CandidateUniverseRow[],
    candidateUniverseRowsLoaded: (data || []).length,
  }
}

async function loadCandidatesContext(
  supabase: any,
  limit: number,
  onlyActive: boolean
): Promise<{
  candidateRows: ContextRow[]
  candidateUniverseRowsLoaded: number
  candidateScreenHistoryRowsLoaded: number
  fallbackCandidateHistoryUsed: boolean
}> {
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - CANDIDATE_LOOKBACK_DAYS)

  let universeQuery = supabase
    .from("candidate_universe")
    .select("company_id, ticker, cik, name, is_active, passed, as_of_date")
    .eq("passed", true)
    .gte("as_of_date", cutoff.toISOString())
    .not("ticker", "is", null)
    .order("as_of_date", { ascending: false })
    .limit(limit)

  if (onlyActive) {
    universeQuery = universeQuery.eq("is_active", true)
  }

  const universeResult = await universeQuery
  if (universeResult.error) {
    throw new Error(
      `candidate_universe candidate load failed: ${universeResult.error.message}`
    )
  }

  const universeRows = (universeResult.data || []) as CandidateUniverseRow[]

  if (universeRows.length >= Math.min(25, limit)) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateScreenHistoryRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,
    }
  }

  const latestSnapshot = await supabase
    .from("candidate_screen_history")
    .select("as_of_date")
    .order("as_of_date", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (latestSnapshot.error) {
    throw new Error(
      `candidate_screen_history latest snapshot lookup failed: ${latestSnapshot.error.message}`
    )
  }

  const latestAsOfDate = latestSnapshot.data?.as_of_date ?? null
  if (!latestAsOfDate) {
    return {
      candidateRows: universeRows,
      candidateUniverseRowsLoaded: universeRows.length,
      candidateScreenHistoryRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,
    }
  }

  let historyQuery = supabase
    .from("candidate_screen_history")
    .select("company_id, ticker, cik, name, is_active, passed, as_of_date")
    .eq("as_of_date", latestAsOfDate)
    .eq("passed", true)
    .not("ticker", "is", null)
    .order("ticker", { ascending: true })
    .limit(limit)

  if (onlyActive) {
    historyQuery = historyQuery.eq("is_active", true)
  }

  const historyResult = await historyQuery
  if (historyResult.error) {
    throw new Error(
      `candidate_screen_history snapshot load failed: ${historyResult.error.message}`
    )
  }

  const historyRows = (historyResult.data || []) as CandidateScreenHistoryRow[]
  const deduped = new Map<string, ContextRow>()

  for (const row of universeRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    deduped.set(ticker, row)
  }

  for (const row of historyRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!deduped.has(ticker)) {
      deduped.set(ticker, row)
    }
  }

  return {
    candidateRows: [...deduped.values()].slice(0, limit),
    candidateUniverseRowsLoaded: universeRows.length,
    candidateScreenHistoryRowsLoaded: historyRows.length,
    fallbackCandidateHistoryUsed: historyRows.length > 0,
  }
}

async function loadRecentFilingsForTickers(
  supabase: any,
  tickers: string[],
  cutoffIso: string
): Promise<RawFilingRow[]> {
  const allRows: RawFilingRow[] = []

  for (const tickerChunk of chunkArray(tickers, QUERY_CHUNK_SIZE)) {
    const { data, error } = await supabase
      .from("raw_filings")
      .select(
        "company_id, ticker, company_name, form_type, filed_at, filing_url, accession_no, cik, primary_doc, fetched_at"
      )
      .in("ticker", tickerChunk)
      .gte("filed_at", cutoffIso)
      .order("filed_at", { ascending: false })

    if (error) {
      throw new Error(`raw_filings load failed: ${error.message}`)
    }

    allRows.push(...(((data || []) as RawFilingRow[]) || []))
  }

  return allRows
}

async function loadRecentPtrTradesForTickers(
  supabase: any,
  tickers: string[],
  cutoffDate: string
): Promise<RawPtrTradeRow[]> {
  const allRows: RawPtrTradeRow[] = []

  for (const tickerChunk of chunkArray(tickers, QUERY_CHUNK_SIZE)) {
    const { data, error } = await supabase
      .from("raw_ptr_trades")
      .select(
        "ticker, politician_name, filer_name, chamber, transaction_type, action, trade_date, disclosure_date, source_url, ptr_url, trade_key, amount_low, amount_high, asset_name"
      )
      .in("ticker", tickerChunk)
      .or(
        `trade_date.gte.${cutoffDate},disclosure_date.gte.${cutoffDate}`
      )
      .order("trade_date", { ascending: false })

    if (error) {
      throw new Error(`raw_ptr_trades load failed: ${error.message}`)
    }

    allRows.push(...(((data || []) as RawPtrTradeRow[]) || []))
  }

  return allRows
}

function scoreFilingSignal(rows: RawFilingRow[]) {
  const forms = rows.map((row) => (row.form_type || "").trim().toUpperCase())
  const latestRow = rows[0] || null
  const latestFiledAt = latestRow?.filed_at || null
  const latestDaysAgo = getDaysAgo(latestFiledAt)

  let strength = 25
  const breakdown: Record<string, number> = {
    base_recent_filing: 25,
  }

  const add = (key: string, value: number) => {
    if (!value) return
    breakdown[key] = value
    strength += value
  }

  const form4Count = forms.filter((form) => form === "4" || form === "4/A").length
  const ownershipCount = forms.filter((form) => OWNERSHIP_FORMS.has(form)).length

  if (form4Count > 0) {
    add("form4_presence", 20)
  }

  if (ownershipCount >= 2) {
    add("multiple_ownership_forms", 10)
  } else if (ownershipCount === 1) {
    add("ownership_form_presence", 5)
  }

  if (rows.length >= 4) {
    add("filing_count", 12)
  } else if (rows.length >= 2) {
    add("filing_count", 7)
  }

  if (latestDaysAgo !== null) {
    if (latestDaysAgo <= 3) add("recency", 15)
    else if (latestDaysAgo <= 7) add("recency", 10)
    else if (latestDaysAgo <= 14) add("recency", 5)
  }

  return {
    strength: clamp(Math.round(strength), 0, 100),
    latestRow,
    latestFiledAt,
    form4Count,
    ownershipCount,
    filingCount: rows.length,
    forms: uniqueStrings(forms),
    breakdown,
  }
}

function isPositivePtrTrade(row: RawPtrTradeRow) {
  const type = (row.transaction_type || "").trim().toLowerCase()
  const action = (row.action || "").trim().toLowerCase()

  return (
    type === "buy" ||
    type === "exchange" ||
    action.includes("buy") ||
    action.includes("purchase") ||
    action.includes("exchange")
  )
}

function scorePtrSignal(rows: RawPtrTradeRow[]) {
  const positiveRows = rows.filter(isPositivePtrTrade)
  const relevantRows = positiveRows.length > 0 ? positiveRows : rows
  const latestRow = relevantRows[0] || null
  const latestDate = latestRow?.trade_date || latestRow?.disclosure_date || null
  const latestDaysAgo = getDaysAgo(latestDate)

  let strength = positiveRows.length > 0 ? 35 : 18
  const breakdown: Record<string, number> = {
    base_ptr_activity: positiveRows.length > 0 ? 35 : 18,
  }

  const add = (key: string, value: number) => {
    if (!value) return
    breakdown[key] = value
    strength += value
  }

  if (positiveRows.length >= 3) {
    add("multiple_positive_ptrs", 20)
  } else if (positiveRows.length >= 2) {
    add("multiple_positive_ptrs", 12)
  } else if (positiveRows.length === 1) {
    add("single_positive_ptr", 6)
  }

  if (latestDaysAgo !== null) {
    if (latestDaysAgo <= 7) add("recency", 15)
    else if (latestDaysAgo <= 14) add("recency", 10)
    else if (latestDaysAgo <= 30) add("recency", 5)
  }

  const maxAmountHigh = relevantRows.reduce<number>((max, row) => {
    const value = Number(row.amount_high || 0)
    return Number.isFinite(value) ? Math.max(max, value) : max
  }, 0)

  if (maxAmountHigh >= 250000) {
    add("trade_size", 10)
  } else if (maxAmountHigh >= 50000) {
    add("trade_size", 5)
  }

  return {
    strength: clamp(Math.round(strength), 0, 100),
    latestRow,
    latestDate,
    positiveCount: positiveRows.length,
    totalCount: rows.length,
    breakdown,
  }
}

function buildFilingSignal(
  context: ContextRow,
  rows: RawFilingRow[],
  runId: string,
  runTimestamp: string
): CurrentSignalRow {
  const ticker = normalizeTicker(context.ticker)
  const companyId =
    "company_id" in context
      ? Number(context.company_id ?? null)
      : Number(context.id ?? null)

  const scored = scoreFilingSignal(rows)
  const summaryParts = uniqueStrings([
    scored.form4Count > 0
      ? `includes ${scored.form4Count} recent Form 4 filing${scored.form4Count === 1 ? "" : "s"}`
      : null,
    scored.ownershipCount > 0
      ? `${scored.ownershipCount} ownership-style filing${scored.ownershipCount === 1 ? "" : "s"} detected`
      : null,
    scored.filingCount >= 2
      ? `${scored.filingCount} total recent filing records`
      : null,
    scored.latestFiledAt ? `latest filing on ${toIsoDateString(new Date(scored.latestFiledAt))}` : null,
  ])

  const summary =
    summaryParts.length > 0
      ? `Recent insider/ownership filing activity detected: ${summaryParts.join(", ")}.`
      : "Recent insider/ownership filing activity detected."

  return {
    run_id: runId,
    ticker,
    company_id: Number.isFinite(companyId) ? companyId : null,
    signal_type: FILING_SIGNAL_TYPE,
    source_type: "filing",
    strength: scored.strength,
    direction: "bullish",
    summary,
    metadata: {
      ticker,
      filingCount: scored.filingCount,
      form4Count: scored.form4Count,
      ownershipCount: scored.ownershipCount,
      forms: scored.forms,
      latestFiledAt: scored.latestFiledAt,
      latestAccessionNo: scored.latestRow?.accession_no || null,
      latestFilingUrl: scored.latestRow?.filing_url || null,
      breakdown: scored.breakdown,
    },
    as_of_date: runTimestamp,
    created_at: runTimestamp,
    updated_at: runTimestamp,
  }
}

function buildPtrSignal(
  context: ContextRow,
  rows: RawPtrTradeRow[],
  runId: string,
  runTimestamp: string
): CurrentSignalRow {
  const ticker = normalizeTicker(context.ticker)
  const companyId =
    "company_id" in context
      ? Number(context.company_id ?? null)
      : Number(context.id ?? null)

  const scored = scorePtrSignal(rows)
  const relevantNames = uniqueStrings(
    rows.map((row) => row.politician_name || row.filer_name)
  ).slice(0, 3)

  const summaryParts = uniqueStrings([
    scored.positiveCount > 0
      ? `${scored.positiveCount} recent positive PTR trade disclosure${scored.positiveCount === 1 ? "" : "s"}`
      : `${scored.totalCount} recent PTR disclosure${scored.totalCount === 1 ? "" : "s"}`,
    relevantNames.length > 0 ? `reported by ${relevantNames.join(", ")}` : null,
    scored.latestDate ? `latest disclosure on ${scored.latestDate}` : null,
  ])

  const summary =
    summaryParts.length > 0
      ? `Recent congressional trading activity detected: ${summaryParts.join(", ")}.`
      : "Recent congressional trading activity detected."

  return {
    run_id: runId,
    ticker,
    company_id: Number.isFinite(companyId) ? companyId : null,
    signal_type: PTR_SIGNAL_TYPE,
    source_type: "ptr",
    strength: scored.strength,
    direction: scored.positiveCount > 0 ? "bullish" : "neutral",
    summary,
    metadata: {
      ticker,
      totalCount: scored.totalCount,
      positiveCount: scored.positiveCount,
      latestTradeDate: scored.latestDate,
      latestPolitician:
        scored.latestRow?.politician_name || scored.latestRow?.filer_name || null,
      latestTradeKey: scored.latestRow?.trade_key || null,
      latestSourceUrl: scored.latestRow?.source_url || scored.latestRow?.ptr_url || null,
      breakdown: scored.breakdown,
    },
    as_of_date: runTimestamp,
    created_at: runTimestamp,
    updated_at: runTimestamp,
  }
}

function buildSignalHistoryRow(
  signal: CurrentSignalRow,
  contextName: string | null,
  runDate: string,
  runId: string,
  runTimestamp: string
): SignalHistoryRow {
  const metadata = signal.metadata || {}
  const breakdown =
    typeof metadata.breakdown === "object" && metadata.breakdown !== null
      ? (metadata.breakdown as Record<string, unknown>)
      : {}

  const title =
    signal.source_type === "ptr"
      ? "Congressional trading activity detected"
      : "Insider/ownership filing activity detected"

  const signalTags =
    signal.source_type === "ptr"
      ? uniqueStrings(["ptr", "congress", signal.direction, signal.signal_type])
      : uniqueStrings(["filing", "insider", signal.direction, signal.signal_type])

  return {
    signal_history_key: buildHistoryKey(
      runId,
      signal.ticker,
      signal.signal_type,
      signal.source_type
    ),
    scored_on: runDate,
    ticker: signal.ticker,
    company_name: contextName,
    signal_type: signal.signal_type,
    signal_source: signal.source_type,
    signal_category: signal.source_type === "ptr" ? "Congress" : "Ownership",
    signal_strength_bucket: getStrengthBucket(signal.strength),
    signal_tags: signalTags,
    bias:
      signal.direction === "bullish"
        ? "Bullish"
        : signal.direction === "bearish"
          ? "Bearish"
          : "Neutral",
    score: signal.strength,
    app_score: signal.strength,
    board_bucket: getBoardBucket(signal.strength),
    title,
    summary: signal.summary,
    source_form:
      signal.source_type === "filing"
        ? String((metadata.latestAccessionNo ? metadata.forms?.[0] : metadata.forms?.[0]) || "")
            .trim() || null
        : null,
    filed_at:
      signal.source_type === "filing"
        ? (metadata.latestFiledAt as string | null) || null
        : (metadata.latestTradeDate as string | null) || null,
    filing_url:
      signal.source_type === "filing"
        ? (metadata.latestFilingUrl as string | null) || null
        : (metadata.latestSourceUrl as string | null) || null,
    accession_no:
      signal.source_type === "filing"
        ? (metadata.latestAccessionNo as string | null) || null
        : (metadata.latestTradeKey as string | null) || null,
    last_scored_at: runTimestamp,
    updated_at: runTimestamp,
    created_at: runTimestamp,
    score_breakdown: breakdown,
    score_version: SCORE_VERSION,
    stacked_signal_count: 1,
    run_id: runId,
    company_id: signal.company_id,
    source_type: signal.source_type,
    strength: signal.strength,
    direction: signal.direction,
    as_of_date: signal.as_of_date,
    captured_at: runTimestamp,
    snapshot: {
      ...signal,
      company_name: contextName,
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

    const scopeParam = (searchParams.get("scope") || "eligible").toLowerCase()
    const modeParam = (searchParams.get("mode") || "all").toLowerCase()
    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const lookbackDays = Math.min(
      Math.max(
        1,
        parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)
      ),
      MAX_LOOKBACK_DAYS
    )
    const minSignalStrength = Math.max(
      0,
      Math.min(
        100,
        parseInteger(
          searchParams.get("minSignalStrength"),
          DEFAULT_MIN_SIGNAL_STRENGTH
        )
      )
    )
    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    if (!["all", "eligible", "candidates"].includes(scopeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, candidates`,
        },
        { status: 400 }
      )
    }

    if (!["all", "filings", "ptrs"].includes(modeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid mode "${modeParam}". Expected one of: all, filings, ptrs`,
        },
        { status: 400 }
      )
    }

    const scope = scopeParam as Scope
    const mode = modeParam as SignalMode

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()
    const runId = `signals_${runTimestamp}`

    const cutoff = new Date(now)
    cutoff.setDate(cutoff.getDate() - lookbackDays)
    const cutoffIso = cutoff.toISOString()
    const cutoffDate = toIsoDateString(cutoff)

    const diagnostics: Diagnostics = {
      scope,
      mode,
      companiesRowsLoaded: 0,
      candidateUniverseRowsLoaded: 0,
      candidateScreenHistoryRowsLoaded: 0,
      sourceRowsLoaded: 0,
      fallbackCandidateHistoryUsed: false,
      rawFilingsLoaded: 0,
      rawPtrTradesLoaded: 0,
      filingSignalsBuilt: 0,
      ptrSignalsBuilt: 0,
      signalsInserted: 0,
      signalHistoryInserted: 0,
      filteredBelowMinStrength: 0,
    }

    let contextRows: ContextRow[] = []

    if (scope === "all") {
      const allContext = await loadCompaniesContext(supabase, limit, onlyActive)
      contextRows = allContext.rows
      diagnostics.companiesRowsLoaded = allContext.companiesRowsLoaded
      diagnostics.sourceRowsLoaded = allContext.rows.length
    }

    if (scope === "eligible") {
      const eligibleContext = await loadEligibleContext(
        supabase,
        limit,
        onlyActive
      )
      contextRows = eligibleContext.rows
      diagnostics.candidateUniverseRowsLoaded =
        eligibleContext.candidateUniverseRowsLoaded
      diagnostics.sourceRowsLoaded = eligibleContext.rows.length
    }

    if (scope === "candidates") {
      const candidateContext = await loadCandidatesContext(
        supabase,
        limit,
        onlyActive
      )
      contextRows = candidateContext.candidateRows
      diagnostics.candidateUniverseRowsLoaded =
        candidateContext.candidateUniverseRowsLoaded
      diagnostics.candidateScreenHistoryRowsLoaded =
        candidateContext.candidateScreenHistoryRowsLoaded
      diagnostics.fallbackCandidateHistoryUsed =
        candidateContext.fallbackCandidateHistoryUsed
      diagnostics.sourceRowsLoaded = candidateContext.candidateRows.length
    }

    const contextMap = new Map<string, ContextRow>()
    for (const row of contextRows) {
      const ticker = normalizeTicker(row.ticker)
      if (!ticker) continue
      if (!contextMap.has(ticker)) {
        contextMap.set(ticker, row)
      }
    }

    const tickers = [...contextMap.keys()]

    if (tickers.length === 0) {
      return Response.json({
        ok: true,
        stage: "signals",
        scope,
        mode,
        limit,
        lookbackDays,
        minSignalStrength,
        diagnostics,
        message: "No source tickers were available for signal generation.",
      })
    }

    let rawFilings: RawFilingRow[] = []
    let rawPtrTrades: RawPtrTradeRow[] = []

    if (mode === "all" || mode === "filings") {
      rawFilings = await loadRecentFilingsForTickers(supabase, tickers, cutoffIso)
      diagnostics.rawFilingsLoaded = rawFilings.length
    }

    if (mode === "all" || mode === "ptrs") {
      rawPtrTrades = await loadRecentPtrTradesForTickers(
        supabase,
        tickers,
        cutoffDate
      )
      diagnostics.rawPtrTradesLoaded = rawPtrTrades.length
    }

    const filingsByTicker = new Map<string, RawFilingRow[]>()
    for (const row of rawFilings) {
      const ticker = normalizeTicker(row.ticker)
      if (!ticker) continue

      const current = filingsByTicker.get(ticker) || []
      current.push(row)
      filingsByTicker.set(ticker, current)
    }

    const ptrsByTicker = new Map<string, RawPtrTradeRow[]>()
    for (const row of rawPtrTrades) {
      const ticker = normalizeTicker(row.ticker)
      if (!ticker) continue

      const current = ptrsByTicker.get(ticker) || []
      current.push(row)
      ptrsByTicker.set(ticker, current)
    }

    const currentSignals: CurrentSignalRow[] = []
    const historySignals: SignalHistoryRow[] = []

    for (const [ticker, context] of contextMap.entries()) {
      if (mode === "all" || mode === "filings") {
        const tickerFilings = filingsByTicker.get(ticker) || []

        if (tickerFilings.length > 0) {
          const filingSignal = buildFilingSignal(
            context,
            tickerFilings,
            runId,
            runTimestamp
          )

          if (filingSignal.strength >= minSignalStrength) {
            currentSignals.push(filingSignal)
            historySignals.push(
              buildSignalHistoryRow(
                filingSignal,
                context.name || null,
                runDate,
                runId,
                runTimestamp
              )
            )
            diagnostics.filingSignalsBuilt += 1
          } else {
            diagnostics.filteredBelowMinStrength += 1
          }
        }
      }

      if (mode === "all" || mode === "ptrs") {
        const tickerPtrs = ptrsByTicker.get(ticker) || []

        if (tickerPtrs.length > 0) {
          const ptrSignal = buildPtrSignal(
            context,
            tickerPtrs,
            runId,
            runTimestamp
          )

          if (ptrSignal.strength >= minSignalStrength) {
            currentSignals.push(ptrSignal)
            historySignals.push(
              buildSignalHistoryRow(
                ptrSignal,
                context.name || null,
                runDate,
                runId,
                runTimestamp
              )
            )
            diagnostics.ptrSignalsBuilt += 1
          } else {
            diagnostics.filteredBelowMinStrength += 1
          }
        }
      }
    }

    const signalsWriteResult =
      currentSignals.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("signals"),
            "signals",
            currentSignals,
            "ticker,signal_type,source_type",
            (row) => `${row.ticker}:${row.signal_type}:${row.source_type}`
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (signalsWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing current signals rows",
          debug: {
            diagnostics,
            errorSamples: signalsWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.signalsInserted = signalsWriteResult.insertedOrUpdated

    const historyWriteResult =
      historySignals.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("signal_history"),
            "signal_history",
            historySignals,
            "signal_history_key",
            (row) => row.signal_history_key
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (historyWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing signal history rows",
          debug: {
            diagnostics,
            errorSamples: historyWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.signalHistoryInserted = historyWriteResult.insertedOrUpdated

    let retentionCleanup = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffDate = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("signal_history")
        .delete()
        .lt("scored_on", retentionCutoffDate)

      retentionCleanup = retentionError ? retentionError.message : "ok"
    }

    return Response.json({
      ok: true,
      stage: mode === "filings" ? "filing_signals" : mode === "ptrs" ? "ptr_signals" : "signals",
      scope,
      mode,
      limit,
      lookbackDays,
      minSignalStrength,
      retainedDays: RETENTION_DAYS,
      retentionCleanup,
      scoreVersion: SCORE_VERSION,
      diagnostics,
      message:
        "Signals were generated leniently from recent insider/ownership filings and PTR activity. They are intended to feed ticker scoring rather than act as a hard screening gate.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown signals route error",
      },
      { status: 500 }
    )
  }
}