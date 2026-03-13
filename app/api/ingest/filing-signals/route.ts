import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type RawFiling = {
  ticker: string
  company_name: string | null
  form_type: string | null
  filed_at: string | null
  filing_url: string | null
  accession_no: string
  cik?: string | null
  primary_doc?: string | null
  fetched_at?: string | null
}

type CandidateContextRow = {
  ticker: string
  cik: string | null
  name: string | null
  price: number | null
  market_cap: number | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  sector?: string | null
  industry?: string | null
  business_description?: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return?: number | null
  return_5d: number | null
  return_10d?: number | null
  return_20d: number | null
  relative_strength_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean | null
  breakout_10d?: boolean | null
  above_sma_20: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  passes_price?: boolean | null
  passes_volume?: boolean | null
  passes_dollar_volume?: boolean | null
  passes_market_cap?: boolean | null
  candidate_score: number | null
  included: boolean | null
  screen_reason: string | null
  last_screened_at: string | null
}

type CandidateHistoryContextRow = CandidateContextRow & {
  screened_on: string
}

type Diagnostics = {
  filingsLoaded: number
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  candidateRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
  filingsSupported: number
  filingsSkippedNoTicker: number
  filingsSkippedNoCandidateContext: number
  filingsSkippedUnsupportedForm: number
  filingSignalsBuilt: number
  filingSignalsInserted: number
  signalHistoryInserted: number
  filteredBelowSignalScore: number
  unsupportedForms: Record<string, number>
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

const DEFAULT_LIMIT = 200
const MAX_LIMIT = 500
const DEFAULT_LOOKBACK_DAYS = 14
const MAX_LOOKBACK_DAYS = 30
const RETENTION_DAYS = 30
const SCORE_VERSION = "v6-filing-only"
const DB_CHUNK_SIZE = 100

const MIN_SIGNAL_APP_SCORE = 68
const MIN_CANDIDATE_SCORE = 65

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

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function round2(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 100) / 100
}

function roundWhole(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value)
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function daysBetween(dateString: string | null) {
  if (!dateString) return null
  const ts = new Date(dateString).getTime()
  if (Number.isNaN(ts)) return null
  return Math.max(0, Math.floor((Date.now() - ts) / (24 * 60 * 60 * 1000)))
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

  return { insertedOrUpdated, errors }
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

function getStrengthBucket(score: number): "Buy" | "Strong Buy" | "Elite Buy" {
  if (score >= 97) return "Elite Buy"
  if (score >= 90) return "Strong Buy"
  return "Buy"
}

function buildSignalKey(filing: RawFiling, source: string) {
  return `${source}:${filing.accession_no}:${normalizeTicker(filing.ticker)}`
}

function buildHistoryKey(runDate: string, signalKey: string) {
  return `${runDate}_${signalKey}`
}

function getBaseSignalType(formType: string | null) {
  const normalized = normalizeFormType(formType)

  if (normalized === "4" || normalized === "4/A") {
    return {
      signalType: "Insider Buy Filing",
      signalSource: "form4",
      signalCategory: "Insider Buying" as const,
      baseScore: 56,
      title: "Insider filing supports the setup",
    }
  }

  if (
    normalized === "13D" ||
    normalized === "SC 13D" ||
    normalized === "13D/A" ||
    normalized === "SC 13D/A"
  ) {
    return {
      signalType: "Ownership Catalyst",
      signalSource: "13d",
      signalCategory: "Ownership" as const,
      baseScore: 60,
      title: "13D ownership catalyst detected",
    }
  }

  if (
    normalized === "13G" ||
    normalized === "SC 13G" ||
    normalized === "13G/A" ||
    normalized === "SC 13G/A"
  ) {
    return {
      signalType: "Institutional Accumulation",
      signalSource: "13g",
      signalCategory: "Ownership" as const,
      baseScore: 52,
      title: "13G accumulation signal detected",
    }
  }

  if (normalized === "8-K" || normalized === "6-K") {
    return {
      signalType: "Corporate Catalyst Filing",
      signalSource: "8k",
      signalCategory: "Catalyst" as const,
      baseScore: 50,
      title: "Corporate catalyst filing detected",
    }
  }

  if (normalized === "10-Q" || normalized === "10-K") {
    return {
      signalType: "Fundamental Update Filing",
      signalSource: "earnings",
      signalCategory: "Earnings Breakout" as const,
      baseScore: 48,
      title: "Fundamental update filing detected",
    }
  }

  return null
}

function scoreFilingSignal(filing: RawFiling, candidate: CandidateContextRow) {
  const base = getBaseSignalType(filing.form_type)
  if (!base) return null

  const breakdown: Record<string, number> = {}
  const reasons: string[] = []
  const caps: string[] = []

  const add = (key: string, value: number, reason?: string | null) => {
    if (!Number.isFinite(value) || value === 0) return
    breakdown[key] = round2((breakdown[key] || 0) + value) ?? value
    if (reason) reasons.push(reason)
  }

  add("base", base.baseScore, `${normalizeFormType(filing.form_type)} filing detected`)

  const candidateScore = Number(candidate.candidate_score || 0)

  if (candidate.included) add("included", 10, "Ticker already made the candidate universe")

  if (candidateScore >= 95) add("candidate_score", 14, "Very strong candidate score")
  else if (candidateScore >= 90) add("candidate_score", 11, "High candidate score")
  else if (candidateScore >= 85) add("candidate_score", 8, "Strong candidate score")
  else if (candidateScore >= 78) add("candidate_score", 5, "Constructive candidate score")

  if ((candidate.relative_strength_20d ?? 0) >= 10) add("relative_strength", 8, "Exceptional relative strength")
  else if ((candidate.relative_strength_20d ?? 0) >= 6) add("relative_strength", 6, "Strong relative strength")
  else if ((candidate.relative_strength_20d ?? 0) >= 3) add("relative_strength", 3, "Positive relative strength")

  if ((candidate.return_20d ?? 0) >= 18) add("momentum", 7, "Strong 20-day momentum")
  else if ((candidate.return_20d ?? 0) >= 10) add("momentum", 5, "Healthy 20-day momentum")
  else if ((candidate.return_20d ?? 0) >= 5) add("momentum", 2, "Positive 20-day momentum")

  if ((candidate.return_5d ?? 0) >= 5) add("short_momentum", 4, "Strong recent momentum")
  else if ((candidate.return_5d ?? 0) >= 2) add("short_momentum", 2, "Constructive recent momentum")

  if ((candidate.volume_ratio ?? 0) >= 2) add("volume", 6, "Heavy volume confirmation")
  else if ((candidate.volume_ratio ?? 0) >= 1.5) add("volume", 4, "Strong volume confirmation")
  else if ((candidate.volume_ratio ?? 0) >= 1.2) add("volume", 1, "Moderate volume support")

  if (candidate.breakout_20d) add("breakout", 7, "20-day breakout present")
  else if (candidate.breakout_10d) add("breakout", 4, "10-day breakout present")

  if (candidate.above_sma_20) add("trend", 4, "Above 20-day moving average")

  const ageDays = daysBetween(filing.filed_at)
  if (ageDays !== null) {
    if (ageDays <= 1) add("freshness", 6, "Very fresh filing")
    else if (ageDays <= 3) add("freshness", 4, "Fresh filing")
    else if (ageDays <= 7) add("freshness", 2, "Recent filing")
    else if (ageDays > 14) add("freshness_penalty", -4, "Older filing")
  }

  if (!(candidate.passes_price ?? true)) add("liquidity_penalty", -5, "Failed minimum price")
  if (!(candidate.passes_volume ?? true)) add("liquidity_penalty", -4, "Failed minimum volume")
  if (!(candidate.passes_dollar_volume ?? true)) add("liquidity_penalty", -5, "Failed minimum dollar volume")
  if (!(candidate.passes_market_cap ?? true)) add("liquidity_penalty", -4, "Failed minimum market cap")

  let rawScore = Object.values(breakdown).reduce((a, b) => a + b, 0)
  rawScore = clamp(Math.round(rawScore), 0, 100)

  let appScore = Math.round(Math.pow(rawScore / 100, 1.15) * 100)

  if (!(candidate.breakout_20d || candidate.breakout_10d) && (candidate.relative_strength_20d ?? 0) < 5) {
    appScore = Math.min(appScore, 86)
    caps.push("no-breakout-no-rs-cap")
  }

  if ((candidate.volume_ratio ?? 0) < 1.2 && (candidate.return_20d ?? 0) < 8) {
    appScore = Math.min(appScore, 82)
    caps.push("weak-volume-momentum-cap")
  }

  if (!candidate.included && candidateScore < 78) {
    appScore = Math.min(appScore, 80)
    caps.push("weak-candidate-context-cap")
  }

  appScore = clamp(appScore, 0, 100)

  return {
    ...base,
    rawScore,
    appScore,
    breakdown,
    reasons: uniqueStrings(reasons),
    caps: uniqueStrings(caps),
    ageDays,
  }
}

function buildSignalRow(
  filing: RawFiling,
  candidate: CandidateContextRow,
  runDate: string,
  runTimestamp: string
) {
  const scored = scoreFilingSignal(filing, candidate)
  if (!scored) return null

  const signalKey = buildSignalKey(filing, scored.signalSource)

  const summaryParts = uniqueStrings([
    candidate.screen_reason,
    candidate.breakout_20d ? "20-day breakout present" : null,
    candidate.breakout_10d ? "10-day breakout present" : null,
    candidate.above_sma_20 ? "trend support is intact" : null,
    (candidate.volume_ratio ?? 0) >= 1.5 ? "volume is elevated" : null,
    (candidate.relative_strength_20d ?? 0) >= 3 ? "relative strength is positive" : null,
    filing.form_type ? `${normalizeFormType(filing.form_type)} filing is recent` : null,
  ])

  return {
    signal_key: signalKey,
    ticker: normalizeTicker(filing.ticker),
    company_name: filing.company_name || candidate.name,
    business_description: candidate.business_description ?? null,
    pe_ratio: round2(candidate.pe_ratio ?? null),
    pe_forward: round2(candidate.pe_forward ?? null),
    pe_type: candidate.pe_type ?? null,
    signal_type: scored.signalType,
    signal_source: scored.signalSource,
    signal_category: scored.signalCategory,
    signal_strength_bucket: getStrengthBucket(scored.appScore),
    signal_tags: uniqueStrings([
      `source:${scored.signalSource}`,
      "bullish",
      "filing-signal",
      filing.form_type ? normalizeFormType(filing.form_type).toLowerCase() : null,
      candidate.included ? "candidate-included" : null,
      candidate.breakout_20d ? "breakout-20d" : null,
      candidate.breakout_10d ? "breakout-10d" : null,
      candidate.above_sma_20 ? "above-sma20" : null,
      (candidate.volume_ratio ?? 0) >= 2 ? "heavy-volume" : null,
      (candidate.relative_strength_20d ?? 0) >= 6 ? "relative-strength" : null,
    ]),
    catalyst_type: normalizeFormType(filing.form_type),
    bias: "Bullish",
    score: scored.rawScore,
    app_score: scored.appScore,
    board_bucket: "Buy",
    title: scored.title,
    summary: summaryParts.length
      ? `Filing and technical context are constructive: ${summaryParts.join(", ")}.`
      : "Filing and technical context are constructive.",
    source_form: filing.form_type,
    filed_at: filing.filed_at ?? runDate,
    filing_url: filing.filing_url,
    accession_no: filing.accession_no,
    insider_action: null,
    insider_shares: null,
    insider_avg_price: null,
    insider_buy_value: null,
    insider_signal_flavor: "Filing",
    cluster_buyers: null,
    cluster_shares: null,
    price_return_5d: round2(candidate.return_5d),
    price_return_20d: round2(candidate.return_20d),
    volume_ratio: round2(candidate.volume_ratio),
    breakout_20d: candidate.breakout_20d === true,
    breakout_52w: false,
    above_50dma: candidate.above_sma_20 === true,
    trend_aligned: candidate.above_sma_20 === true,
    price_confirmed:
      candidate.breakout_20d === true ||
      candidate.breakout_10d === true ||
      (candidate.volume_ratio ?? 0) >= 1.5,
    earnings_surprise_pct: null,
    revenue_growth_pct: null,
    guidance_flag: false,
    market_cap: roundWhole(candidate.market_cap),
    sector: candidate.sector ?? null,
    industry: candidate.industry ?? null,
    relative_strength_20d: round2(candidate.relative_strength_20d),
    age_days: scored.ageDays,
    freshness_bucket:
      scored.ageDays === null
        ? null
        : scored.ageDays <= 1
          ? "today"
          : scored.ageDays <= 3
            ? "fresh"
            : scored.ageDays <= 7
              ? "recent"
              : "aging",
    last_scored_at: runTimestamp,
    updated_at: runTimestamp,
    score_breakdown: scored.breakdown,
    score_version: SCORE_VERSION,
    score_updated_at: runTimestamp,
    stacked_signal_count: 1,
    signal_reasons: scored.reasons,
    score_caps_applied: scored.caps,
    ticker_score_change_1d: null,
    ticker_score_change_7d: null,
  }
}

function buildSignalHistoryRow(signalRow: any, runDate: string, runTimestamp: string) {
  return {
    signal_key: signalRow.signal_key,
    ticker: signalRow.ticker,
    company_name: signalRow.company_name,
    business_description: signalRow.business_description,
    pe_ratio: signalRow.pe_ratio,
    pe_forward: signalRow.pe_forward,
    pe_type: signalRow.pe_type,
    signal_type: signalRow.signal_type,
    signal_source: signalRow.signal_source,
    signal_category: signalRow.signal_category,
    signal_strength_bucket: signalRow.signal_strength_bucket,
    signal_tags: signalRow.signal_tags,
    catalyst_type: signalRow.catalyst_type,
    bias: signalRow.bias,
    score: signalRow.score,
    app_score: signalRow.app_score,
    board_bucket: signalRow.board_bucket,
    title: signalRow.title,
    summary: signalRow.summary,
    source_form: signalRow.source_form,
    filed_at: signalRow.filed_at,
    filing_url: signalRow.filing_url,
    accession_no: signalRow.accession_no,
    insider_action: signalRow.insider_action,
    insider_shares: signalRow.insider_shares,
    insider_avg_price: signalRow.insider_avg_price,
    insider_buy_value: signalRow.insider_buy_value,
    insider_signal_flavor: signalRow.insider_signal_flavor,
    cluster_buyers: signalRow.cluster_buyers,
    cluster_shares: signalRow.cluster_shares,
    price_return_5d: signalRow.price_return_5d,
    price_return_20d: signalRow.price_return_20d,
    volume_ratio: signalRow.volume_ratio,
    breakout_20d: signalRow.breakout_20d,
    breakout_52w: signalRow.breakout_52w,
    above_50dma: signalRow.above_50dma,
    trend_aligned: signalRow.trend_aligned,
    price_confirmed: signalRow.price_confirmed,
    earnings_surprise_pct: signalRow.earnings_surprise_pct,
    revenue_growth_pct: signalRow.revenue_growth_pct,
    guidance_flag: signalRow.guidance_flag,
    market_cap: signalRow.market_cap,
    sector: signalRow.sector,
    industry: signalRow.industry,
    relative_strength_20d: signalRow.relative_strength_20d,
    age_days: signalRow.age_days,
    freshness_bucket: signalRow.freshness_bucket,
    score_breakdown: signalRow.score_breakdown,
    score_version: signalRow.score_version,
    stacked_signal_count: signalRow.stacked_signal_count,
    signal_reasons: signalRow.signal_reasons,
    score_caps_applied: signalRow.score_caps_applied,
    ticker_score_change_1d: signalRow.ticker_score_change_1d,
    ticker_score_change_7d: signalRow.ticker_score_change_7d,
    signal_history_key: buildHistoryKey(runDate, signalRow.signal_key),
    scored_on: runDate,
    created_at: runTimestamp,
  }
}

async function loadCandidateContext(
  supabase: any,
  limit: number,
  candidateCutoffDateString: string
): Promise<{
  candidateRows: CandidateContextRow[]
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
}> {
  const universeQuery = await supabase
    .from("candidate_universe")
    .select(
      "ticker, cik, name, price, market_cap, pe_ratio, pe_forward, pe_type, sector, industry, business_description, avg_volume_20d, avg_dollar_volume_20d, one_day_return, return_5d, return_10d, return_20d, relative_strength_20d, volume_ratio, breakout_20d, breakout_10d, above_sma_20, breakout_clearance_pct, extension_from_sma20_pct, close_in_day_range, catalyst_count, passes_price, passes_volume, passes_dollar_volume, passes_market_cap, candidate_score, included, screen_reason, last_screened_at"
    )
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .gte("last_screened_at", candidateCutoffDateString)
    .order("candidate_score", { ascending: false })
    .limit(limit)

  if (universeQuery.error) {
    throw new Error(`candidate_universe load failed: ${universeQuery.error.message}`)
  }

  const universeRows = (universeQuery.data || []) as CandidateContextRow[]

  if (universeRows.length >= Math.min(25, limit)) {
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
    throw new Error(`candidate_screen_history latest snapshot lookup failed: ${latestScreened.error.message}`)
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
      "ticker, cik, name, price, market_cap, pe_ratio, pe_forward, pe_type, sector, industry, business_description, avg_volume_20d, avg_dollar_volume_20d, one_day_return, return_5d, return_10d, return_20d, relative_strength_20d, volume_ratio, breakout_20d, breakout_10d, above_sma_20, breakout_clearance_pct, extension_from_sma20_pct, close_in_day_range, catalyst_count, passes_price, passes_volume, passes_dollar_volume, passes_market_cap, candidate_score, included, screen_reason, last_screened_at, screened_on"
    )
    .eq("screened_on", screenedOn)
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .order("candidate_score", { ascending: false })
    .limit(limit)

  if (historyQuery.error) {
    throw new Error(`candidate_screen_history snapshot load failed: ${historyQuery.error.message}`)
  }

  const historyRows = (historyQuery.data || []) as CandidateHistoryContextRow[]

  const deduped = new Map<string, CandidateContextRow>()
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
        price: row.price,
        market_cap: row.market_cap,
        pe_ratio: row.pe_ratio,
        pe_forward: row.pe_forward,
        pe_type: row.pe_type,
        sector: row.sector,
        industry: row.industry,
        business_description: row.business_description,
        avg_volume_20d: row.avg_volume_20d,
        avg_dollar_volume_20d: row.avg_dollar_volume_20d,
        one_day_return: row.one_day_return,
        return_5d: row.return_5d,
        return_10d: row.return_10d,
        return_20d: row.return_20d,
        relative_strength_20d: row.relative_strength_20d,
        volume_ratio: row.volume_ratio,
        breakout_20d: row.breakout_20d,
        breakout_10d: row.breakout_10d,
        above_sma_20: row.above_sma_20,
        breakout_clearance_pct: row.breakout_clearance_pct,
        extension_from_sma20_pct: row.extension_from_sma20_pct,
        close_in_day_range: row.close_in_day_range,
        catalyst_count: row.catalyst_count,
        passes_price: row.passes_price,
        passes_volume: row.passes_volume,
        passes_dollar_volume: row.passes_dollar_volume,
        passes_market_cap: row.passes_market_cap,
        candidate_score: row.candidate_score,
        included: row.included,
        screen_reason: row.screen_reason,
        last_screened_at: row.last_screened_at,
      })
    }
  }

  return {
    candidateRows: [...deduped.values()].slice(0, limit),
    candidateUniverseRowsLoaded: universeRows.length,
    candidateHistoryRowsLoaded: historyRows.length,
    fallbackCandidateSourceUsed: historyRows.length > 0,
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

    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)
    const candidateCutoffDateString = cutoffDate.toISOString()

    const diagnostics: Diagnostics = {
      filingsLoaded: 0,
      candidateUniverseRowsLoaded: 0,
      candidateHistoryRowsLoaded: 0,
      candidateRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
      filingsSupported: 0,
      filingsSkippedNoTicker: 0,
      filingsSkippedNoCandidateContext: 0,
      filingsSkippedUnsupportedForm: 0,
      filingSignalsBuilt: 0,
      filingSignalsInserted: 0,
      signalHistoryInserted: 0,
      filteredBelowSignalScore: 0,
      unsupportedForms: {},
    }

    const [filingsQuery, candidateContext] = await Promise.all([
      supabase
        .from("raw_filings")
        .select(
          "ticker, company_name, form_type, filed_at, filing_url, accession_no, cik, primary_doc, fetched_at"
        )
        .gte("filed_at", cutoffDateString)
        .order("filed_at", { ascending: false })
        .limit(limit),
      loadCandidateContext(supabase, limit, candidateCutoffDateString),
    ])

    if (filingsQuery.error) {
      return Response.json({ ok: false, error: filingsQuery.error.message }, { status: 500 })
    }

    const filings = (filingsQuery.data || []) as RawFiling[]
    diagnostics.filingsLoaded = filings.length
    diagnostics.candidateUniverseRowsLoaded = candidateContext.candidateUniverseRowsLoaded
    diagnostics.candidateHistoryRowsLoaded = candidateContext.candidateHistoryRowsLoaded
    diagnostics.candidateRowsLoaded = candidateContext.candidateRows.length
    diagnostics.fallbackCandidateSourceUsed = candidateContext.fallbackCandidateSourceUsed

    const candidateByTicker = new Map<string, CandidateContextRow>()
    for (const row of candidateContext.candidateRows) {
      const ticker = normalizeTicker(row.ticker)
      if (!ticker) continue
      candidateByTicker.set(ticker, row)
    }

    const signalRows: any[] = []
    const historyRows: any[] = []

    for (const filing of filings) {
      const ticker = normalizeTicker(filing.ticker)
      if (!ticker) {
        diagnostics.filingsSkippedNoTicker += 1
        continue
      }

      const candidate = candidateByTicker.get(ticker)
      if (!candidate) {
        diagnostics.filingsSkippedNoCandidateContext += 1
        continue
      }

      const base = getBaseSignalType(filing.form_type)
      if (!base) {
        diagnostics.filingsSkippedUnsupportedForm += 1
        const normalized = normalizeFormType(filing.form_type) || "UNKNOWN"
        diagnostics.unsupportedForms[normalized] = (diagnostics.unsupportedForms[normalized] || 0) + 1
        continue
      }

      diagnostics.filingsSupported += 1

      const signalRow = buildSignalRow(filing, candidate, runDate, runTimestamp)
      if (!signalRow) continue

      if (signalRow.app_score < MIN_SIGNAL_APP_SCORE) {
        diagnostics.filteredBelowSignalScore += 1
        continue
      }

      signalRows.push(signalRow)
      historyRows.push(buildSignalHistoryRow(signalRow, runDate, runTimestamp))
    }

    diagnostics.filingSignalsBuilt = signalRows.length

    const signalWriteResult =
      signalRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("signals"),
            "signals",
            signalRows,
            "signal_key",
            (row) => row.signal_key
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (signalWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing filing signal rows",
          debug: {
            diagnostics,
            errorSamples: signalWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.filingSignalsInserted = signalWriteResult.insertedOrUpdated

    const historyWriteResult =
      historyRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("signal_history"),
            "signal_history",
            historyRows,
            "signal_history_key",
            (row) => row.signal_history_key
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (historyWriteResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing filing signal history rows",
          debug: {
            diagnostics,
            errorSamples: historyWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.signalHistoryInserted = historyWriteResult.insertedOrUpdated

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("signal_history")
        .delete()
        .lt("scored_on", retentionCutoffString)

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    let insertedCount: number | null = null
    if (includeCounts) {
      insertedCount = diagnostics.filingSignalsInserted
    }

    return Response.json({
      ok: true,
      filingsLoaded: diagnostics.filingsLoaded,
      candidateUniverseRowsLoaded: diagnostics.candidateUniverseRowsLoaded,
      candidateHistoryRowsLoaded: diagnostics.candidateHistoryRowsLoaded,
      candidateRowsLoaded: diagnostics.candidateRowsLoaded,
      fallbackCandidateSourceUsed: diagnostics.fallbackCandidateSourceUsed,
      filingSignalsInserted: diagnostics.filingSignalsInserted,
      signalHistoryInserted: diagnostics.signalHistoryInserted,
      insertedCount,
      limit,
      lookbackDays,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      retentionCleanup: retentionMessage,
      diagnostics,
      message:
        "Filing-based signals calculated from raw_filings using current candidate context. This route is designed to run separately from technical signals.",
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