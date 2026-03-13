import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CandidateSignalInput = {
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

type CandidateHistorySignalInput = CandidateSignalInput & {
  screened_on: string
}

type Diagnostics = {
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  candidateRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
  candidateSignalsBuilt: number
  candidateSignalsInserted: number
  signalHistoryInserted: number
  tickerCurrentBuilt: number
  tickerCurrentInserted: number
  tickerHistoryInserted: number
  filteredBelowSignalScore: number
  filteredBelowTickerScore: number
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

const DEFAULT_LIMIT = 150
const MAX_LIMIT = 300
const DEFAULT_LOOKBACK_DAYS = 10
const MAX_LOOKBACK_DAYS = 30
const RETENTION_DAYS = 30
const SCORE_VERSION = "v6-tech-only"

const DB_CHUNK_SIZE = 100

const MIN_SIGNAL_APP_SCORE = 70
const MIN_TICKER_APP_SCORE = 75
const MIN_CANDIDATE_SCORE = 70

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

function addDays(isoDate: string, days: number) {
  const d = new Date(`${isoDate}T00:00:00.000Z`)
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
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

async function deleteInChunksByTickerDetailed(table: any, tickers: string[]) {
  const unique = uniqueStrings(tickers)
  const errors: Array<{
    chunkStart: number
    chunkSize: number
    message: string
    details?: string | null
    hint?: string | null
    code?: string | null
    sampleTickers: string[]
  }> = []

  let deletedRequested = 0

  for (let i = 0; i < unique.length; i += DB_CHUNK_SIZE) {
    const chunk = unique.slice(i, i + DB_CHUNK_SIZE)
    const { error } = await table.delete().in("ticker", chunk)

    if (error) {
      errors.push({
        chunkStart: i,
        chunkSize: chunk.length,
        message: error.message,
        details: (error as any)?.details ?? null,
        hint: (error as any)?.hint ?? null,
        code: (error as any)?.code ?? null,
        sampleTickers: chunk.slice(0, 10),
      })
    } else {
      deletedRequested += chunk.length
    }
  }

  return {
    deletedRequested,
    errors,
  }
}

function getStrengthBucket(score: number): "Buy" | "Strong Buy" | "Elite Buy" {
  if (score >= 97) return "Elite Buy"
  if (score >= 90) return "Strong Buy"
  return "Buy"
}

function buildSignalKey(ticker: string, runDate: string) {
  return `breakout:TECH_${runDate}_${normalizeTicker(ticker)}:${normalizeTicker(ticker)}`
}

function buildHistoryKey(runDate: string, signalKey: string) {
  return `${runDate}_${signalKey}`
}

function scoreCandidateSignal(candidate: CandidateSignalInput) {
  const candidateScore = Number(candidate.candidate_score || 0)

  const breakdown: Record<string, number> = {}
  const reasons: string[] = []
  const caps: string[] = []

  const add = (key: string, value: number, reason?: string | null) => {
    if (!Number.isFinite(value) || value === 0) return
    breakdown[key] = round2((breakdown[key] || 0) + value) ?? value
    if (reason) reasons.push(reason)
  }

  add("base", 45, "Base technical candidate signal")

  if (candidate.included) add("included", 12, "Finalized candidate universe member")

  if (candidateScore >= 98) add("candidate_score", 18, "Elite candidate score")
  else if (candidateScore >= 95) add("candidate_score", 15, "Very strong candidate score")
  else if (candidateScore >= 90) add("candidate_score", 12, "High candidate score")
  else if (candidateScore >= 85) add("candidate_score", 8, "Strong candidate score")
  else if (candidateScore >= 78) add("candidate_score", 5, "Constructive candidate score")

  if ((candidate.return_5d ?? 0) >= 6) add("short_momentum", 5, "Strong 5-day momentum")
  else if ((candidate.return_5d ?? 0) >= 3) add("short_momentum", 3, "Positive 5-day momentum")

  if ((candidate.return_10d ?? 0) >= 8) add("medium_momentum", 5, "Strong 10-day momentum")
  else if ((candidate.return_10d ?? 0) >= 4) add("medium_momentum", 3, "Positive 10-day momentum")

  if ((candidate.return_20d ?? 0) >= 20) add("trend_momentum", 7, "Strong 20-day momentum")
  else if ((candidate.return_20d ?? 0) >= 12) add("trend_momentum", 5, "Healthy 20-day momentum")
  else if ((candidate.return_20d ?? 0) >= 6) add("trend_momentum", 2, "Positive 20-day momentum")

  if ((candidate.relative_strength_20d ?? 0) >= 10) add("relative_strength", 8, "Exceptional relative strength")
  else if ((candidate.relative_strength_20d ?? 0) >= 6) add("relative_strength", 6, "Strong relative strength")
  else if ((candidate.relative_strength_20d ?? 0) >= 3) add("relative_strength", 3, "Positive relative strength")

  if ((candidate.volume_ratio ?? 0) >= 2.5) add("volume", 7, "Heavy volume confirmation")
  else if ((candidate.volume_ratio ?? 0) >= 1.8) add("volume", 5, "Strong volume confirmation")
  else if ((candidate.volume_ratio ?? 0) >= 1.3) add("volume", 2, "Moderate volume confirmation")

  if (candidate.breakout_20d) add("breakout", 8, "20-day breakout")
  else if (candidate.breakout_10d) add("breakout", 5, "10-day breakout")

  if (candidate.above_sma_20) add("trend", 4, "Above 20-day moving average")

  if ((candidate.breakout_clearance_pct ?? 0) >= 1) add("clearance", 3, "Clean breakout clearance")
  else if ((candidate.breakout_clearance_pct ?? 0) >= 0.25) add("clearance", 1, "Valid breakout clearance")

  if ((candidate.close_in_day_range ?? 0) >= 0.8) add("close_strength", 3, "Strong close in day range")
  else if ((candidate.close_in_day_range ?? 0) >= 0.6) add("close_strength", 1, "Constructive close in day range")

  if ((candidate.extension_from_sma20_pct ?? 999) > 18) {
    add("overextension_penalty", -8, "Too extended from 20-day average")
    caps.push("overextended-cap")
  } else if ((candidate.extension_from_sma20_pct ?? 999) > 14) {
    add("overextension_penalty", -4, "Somewhat extended from 20-day average")
  }

  if (!(candidate.passes_price ?? true)) add("liquidity_penalty", -5, "Failed minimum price")
  if (!(candidate.passes_volume ?? true)) add("liquidity_penalty", -4, "Failed minimum volume")
  if (!(candidate.passes_dollar_volume ?? true)) add("liquidity_penalty", -5, "Failed minimum dollar volume")
  if (!(candidate.passes_market_cap ?? true)) add("liquidity_penalty", -4, "Failed minimum market cap")

  let rawScore = Object.values(breakdown).reduce((a, b) => a + b, 0)
  rawScore = clamp(Math.round(rawScore), 0, 100)

  let appScore = Math.round(Math.pow(rawScore / 100, 1.2) * 100)

  if (!(candidate.breakout_20d || candidate.breakout_10d) && (candidate.relative_strength_20d ?? 0) < 5) {
    appScore = Math.min(appScore, 88)
    caps.push("no-breakout-no-rs-cap")
  }

  if ((candidate.volume_ratio ?? 0) < 1.2 && (candidate.return_20d ?? 0) < 10) {
    appScore = Math.min(appScore, 84)
    caps.push("weak-volume-momentum-cap")
  }

  appScore = clamp(appScore, 0, 100)

  return {
    rawScore,
    appScore,
    breakdown,
    reasons: uniqueStrings(reasons),
    caps: uniqueStrings(caps),
  }
}

function buildSignalRow(candidate: CandidateSignalInput, runDate: string, runTimestamp: string) {
  const signalScore = scoreCandidateSignal(candidate)
  const signalKey = buildSignalKey(candidate.ticker, runDate)

  const title =
    (candidate.candidate_score ?? 0) >= 95
      ? "Elite technical strong-buy setup"
      : (candidate.candidate_score ?? 0) >= 90
        ? "High-conviction technical strong-buy setup"
        : "Technical breakout candidate"

  const summaryParts = uniqueStrings([
    candidate.screen_reason,
    candidate.breakout_20d ? "20-day breakout present" : null,
    candidate.breakout_10d ? "10-day breakout present" : null,
    candidate.above_sma_20 ? "trend support is intact" : null,
    (candidate.volume_ratio ?? 0) >= 1.5 ? "volume is elevated" : null,
    (candidate.relative_strength_20d ?? 0) >= 3 ? "relative strength is positive" : null,
  ])

  return {
    signal_key: signalKey,
    ticker: normalizeTicker(candidate.ticker),
    company_name: candidate.name,
    business_description: candidate.business_description ?? null,
    pe_ratio: round2(candidate.pe_ratio ?? null),
    pe_forward: round2(candidate.pe_forward ?? null),
    pe_type: candidate.pe_type ?? null,
    signal_type: "Technical Strong Buy",
    signal_source: "breakout",
    signal_category: "Breakout",
    signal_strength_bucket: getStrengthBucket(signalScore.appScore),
    signal_tags: uniqueStrings([
      "source:breakout",
      "bullish",
      "candidate-screen",
      candidate.included ? "candidate-included" : null,
      candidate.breakout_20d ? "breakout-20d" : null,
      candidate.breakout_10d ? "breakout-10d" : null,
      candidate.above_sma_20 ? "above-sma20" : null,
      (candidate.volume_ratio ?? 0) >= 2 ? "heavy-volume" : null,
      (candidate.relative_strength_20d ?? 0) >= 6 ? "relative-strength" : null,
    ]),
    catalyst_type: null,
    bias: "Bullish",
    score: signalScore.rawScore,
    app_score: signalScore.appScore,
    board_bucket: "Buy",
    title,
    summary: summaryParts.length
      ? `Technical setup is constructive: ${summaryParts.join(", ")}.`
      : "Technical setup is constructive.",
    source_form: null,
    filed_at: runDate,
    filing_url: null,
    accession_no: `TECH_${runDate}_${normalizeTicker(candidate.ticker)}`,
    insider_action: null,
    insider_shares: null,
    insider_avg_price: null,
    insider_buy_value: null,
    insider_signal_flavor: "Technical",
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
    age_days: 0,
    freshness_bucket: "today",
    last_scored_at: runTimestamp,
    updated_at: runTimestamp,
    score_breakdown: signalScore.breakdown,
    score_version: SCORE_VERSION,
    score_updated_at: runTimestamp,
    stacked_signal_count: 1,
    signal_reasons: signalScore.reasons,
    score_caps_applied: signalScore.caps,
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

function buildTickerScoresCurrentRows(signalRows: any[], runTimestamp: string) {
  const byTicker = new Map<string, any[]>()

  for (const row of signalRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const rows: any[] = []

  for (const [ticker, tickerSignalRows] of byTicker.entries()) {
    const sorted = [...tickerSignalRows].sort((a, b) => {
      const scoreDiff = Number(b.app_score ?? 0) - Number(a.app_score ?? 0)
      if (scoreDiff !== 0) return scoreDiff
      return new Date(b.filed_at || 0).getTime() - new Date(a.filed_at || 0).getTime()
    })

    const primary = sorted[0]

    const scoreBreakdown: Record<string, number> = {}
    const signalReasons = new Set<string>()
    const scoreCapsApplied = new Set<string>()
    const signalTags = new Set<string>()
    const accessionNos: string[] = []
    const signalKeys: string[] = []
    const sourceForms: string[] = []

    for (const row of sorted) {
      signalKeys.push(row.signal_key)
      accessionNos.push(row.accession_no)
      if (row.source_form) sourceForms.push(row.source_form)

      for (const tag of row.signal_tags || []) signalTags.add(tag)
      for (const reason of row.signal_reasons || []) signalReasons.add(reason)
      for (const cap of row.score_caps_applied || []) scoreCapsApplied.add(cap)

      const breakdown = (row.score_breakdown || {}) as Record<string, number>
      for (const [key, value] of Object.entries(breakdown)) {
        scoreBreakdown[key] = round2((scoreBreakdown[key] || 0) + Number(value || 0)) ?? 0
      }
    }

    const finalScore = clamp(Math.round(Number(primary.app_score || 0)), 0, 100)
    if (finalScore < MIN_TICKER_APP_SCORE) continue

    rows.push({
      ticker,
      company_name: primary.company_name,
      business_description: primary.business_description,
      app_score: finalScore,
      raw_score: finalScore,
      bias: "Bullish",
      board_bucket: "Buy",
      signal_strength_bucket: getStrengthBucket(finalScore),
      score_version: SCORE_VERSION,
      score_updated_at: runTimestamp,
      stacked_signal_count: sorted.length,
      score_breakdown: scoreBreakdown,
      signal_reasons: Array.from(signalReasons).slice(0, 12),
      score_caps_applied: Array.from(scoreCapsApplied),
      signal_tags: Array.from(signalTags),
      primary_signal_key: primary.signal_key,
      primary_signal_type: primary.signal_type,
      primary_signal_source: primary.signal_source,
      primary_signal_category: primary.signal_category,
      primary_title: primary.title,
      primary_summary: primary.summary,
      filed_at: primary.filed_at,
      signal_keys: signalKeys,
      accession_nos: accessionNos,
      source_forms: uniqueStrings(sourceForms),
      pe_ratio: primary.pe_ratio,
      pe_forward: primary.pe_forward,
      pe_type: primary.pe_type,
      market_cap: primary.market_cap,
      sector: primary.sector,
      industry: primary.industry,
      insider_action: primary.insider_action,
      insider_shares: primary.insider_shares,
      insider_avg_price: primary.insider_avg_price,
      insider_buy_value: primary.insider_buy_value,
      cluster_buyers: primary.cluster_buyers,
      cluster_shares: primary.cluster_shares,
      price_return_5d: primary.price_return_5d,
      price_return_20d: primary.price_return_20d,
      volume_ratio: primary.volume_ratio,
      breakout_20d: primary.breakout_20d,
      breakout_52w: primary.breakout_52w,
      above_50dma: primary.above_50dma,
      trend_aligned: primary.trend_aligned,
      price_confirmed: primary.price_confirmed,
      relative_strength_20d: primary.relative_strength_20d,
      earnings_surprise_pct: primary.earnings_surprise_pct,
      revenue_growth_pct: primary.revenue_growth_pct,
      guidance_flag: primary.guidance_flag,
      age_days: primary.age_days,
      freshness_bucket: primary.freshness_bucket,
      ticker_score_change_1d: null,
      ticker_score_change_7d: null,
      updated_at: runTimestamp,
    })
  }

  return rows
}

async function attachTickerScoreChangesToCurrentRows(
  supabase: any,
  currentRows: any[],
  runDate: string
) {
  const tickers = uniqueStrings(currentRows.map((row) => row.ticker))
  if (!tickers.length) return currentRows

  const earliestNeededDate = addDays(runDate, -14)

  const { data: historyRows, error } = await supabase
    .from("ticker_score_history")
    .select("ticker, score_date, app_score")
    .in("ticker", tickers)
    .gte("score_date", earliestNeededDate)
    .lt("score_date", runDate)
    .order("score_date", { ascending: false })

  if (error) return currentRows

  const byTicker = new Map<string, Map<string, number>>()

  for (const row of historyRows || []) {
    const ticker = normalizeTicker((row as any).ticker)
    if (!byTicker.has(ticker)) byTicker.set(ticker, new Map<string, number>())
    byTicker.get(ticker)!.set(
      String((row as any).score_date),
      Number((row as any).app_score || 0)
    )
  }

  return currentRows.map((row) => {
    const ticker = normalizeTicker(row.ticker)
    const series = byTicker.get(ticker) || new Map<string, number>()
    const currentScore = Number(row.app_score || 0)

    const oneDayDate = addDays(runDate, -1)
    const sevenDayDate = addDays(runDate, -7)

    const prev1d = series.has(oneDayDate) ? series.get(oneDayDate)! : null
    const prev7d = series.has(sevenDayDate) ? series.get(sevenDayDate)! : null

    return {
      ...row,
      ticker_score_change_1d: prev1d === null ? null : round2(currentScore - prev1d),
      ticker_score_change_7d: prev7d === null ? null : round2(currentScore - prev7d),
    }
  })
}

function buildTickerScoreHistoryRows(currentRows: any[], runDate: string, runTimestamp: string) {
  return currentRows.map((row) => ({
    ticker: row.ticker,
    company_name: row.company_name,
    score_date: runDate,
    score_timestamp: runTimestamp,
    app_score: row.app_score,
    raw_score: row.raw_score,
    bias: row.bias,
    board_bucket: row.board_bucket,
    score_version: row.score_version,
    stacked_signal_count: row.stacked_signal_count,
    score_breakdown: row.score_breakdown,
    signal_reasons: row.signal_reasons,
    score_caps_applied: row.score_caps_applied,
    source_accession_nos: row.accession_nos,
    source_signal_keys: row.signal_keys,
    created_at: runTimestamp,
  }))
}

async function loadCandidateInputs(
  supabase: any,
  limit: number,
  candidateCutoffDateString: string
): Promise<{
  candidateRows: CandidateSignalInput[]
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

  const universeRows = (universeQuery.data || []) as CandidateSignalInput[]

  if (universeRows.length >= Math.min(20, limit)) {
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

  const historyRows = (historyQuery.data || []) as CandidateHistorySignalInput[]

  const deduped = new Map<string, CandidateSignalInput>()
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
    const rebuildTickerScores =
      (searchParams.get("rebuildTickerScores") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const candidateCutoffDate = new Date(now)
    candidateCutoffDate.setDate(candidateCutoffDate.getDate() - lookbackDays)
    const candidateCutoffDateString = candidateCutoffDate.toISOString()

    const diagnostics: Diagnostics = {
      candidateUniverseRowsLoaded: 0,
      candidateHistoryRowsLoaded: 0,
      candidateRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
      candidateSignalsBuilt: 0,
      candidateSignalsInserted: 0,
      signalHistoryInserted: 0,
      tickerCurrentBuilt: 0,
      tickerCurrentInserted: 0,
      tickerHistoryInserted: 0,
      filteredBelowSignalScore: 0,
      filteredBelowTickerScore: 0,
    }

    const candidateLoadResult = await loadCandidateInputs(
      supabase,
      limit,
      candidateCutoffDateString
    )

    const candidateRows = candidateLoadResult.candidateRows
    diagnostics.candidateUniverseRowsLoaded = candidateLoadResult.candidateUniverseRowsLoaded
    diagnostics.candidateHistoryRowsLoaded = candidateLoadResult.candidateHistoryRowsLoaded
    diagnostics.candidateRowsLoaded = candidateRows.length
    diagnostics.fallbackCandidateSourceUsed = candidateLoadResult.fallbackCandidateSourceUsed

    const signalRows: any[] = []
    const historyRows: any[] = []

    for (const candidate of candidateRows) {
      const signalRow = buildSignalRow(candidate, runDate, runTimestamp)

      if (signalRow.app_score < MIN_SIGNAL_APP_SCORE) {
        diagnostics.filteredBelowSignalScore += 1
        continue
      }

      signalRows.push(signalRow)
      historyRows.push(buildSignalHistoryRow(signalRow, runDate, runTimestamp))
    }

    diagnostics.candidateSignalsBuilt = signalRows.length

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
          error: "Failed writing signals rows",
          debug: {
            diagnostics,
            errorSamples: signalWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.candidateSignalsInserted = signalWriteResult.insertedOrUpdated

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

    let tickerCurrentRows: any[] = []
    let tickerHistoryRows: any[] = []

    if (rebuildTickerScores) {
      tickerCurrentRows = buildTickerScoresCurrentRows(signalRows, runTimestamp)
      diagnostics.tickerCurrentBuilt = tickerCurrentRows.length
      diagnostics.filteredBelowTickerScore = Math.max(0, signalRows.length - tickerCurrentRows.length)

      tickerCurrentRows = await attachTickerScoreChangesToCurrentRows(supabase, tickerCurrentRows, runDate)

      const tickerCurrentWriteResult =
        tickerCurrentRows.length > 0
          ? await upsertInChunksDetailed(
              supabase.from("ticker_scores_current"),
              "ticker_scores_current",
              tickerCurrentRows,
              "ticker",
              (row) => row.ticker
            )
          : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

      if (tickerCurrentWriteResult.errors.length > 0) {
        return Response.json(
          {
            ok: false,
            error: "Failed writing ticker_scores_current rows",
            debug: {
              diagnostics,
              errorSamples: tickerCurrentWriteResult.errors.slice(0, 5),
            },
          },
          { status: 500 }
        )
      }

      const currentTickerSet = new Set(tickerCurrentRows.map((row) => normalizeTicker(row.ticker)))
      const { data: existingTickerRows, error: existingTickerRowsError } = await supabase
        .from("ticker_scores_current")
        .select("ticker")

      if (existingTickerRowsError) {
        return Response.json(
          {
            ok: false,
            error: existingTickerRowsError.message,
            diagnostics,
          },
          { status: 500 }
        )
      }

      const staleTickerList = uniqueStrings(
        (existingTickerRows || [])
          .map((row: any) => normalizeTicker(row.ticker))
          .filter((ticker) => !currentTickerSet.has(ticker))
      )

      const staleDeleteResult =
        staleTickerList.length > 0
          ? await deleteInChunksByTickerDetailed(supabase.from("ticker_scores_current"), staleTickerList)
          : { deletedRequested: 0, errors: [] as any[] }

      if (staleDeleteResult.errors.length > 0) {
        return Response.json(
          {
            ok: false,
            error: "Failed deleting stale ticker_scores_current rows",
            debug: {
              diagnostics,
              errorSamples: staleDeleteResult.errors.slice(0, 5),
            },
          },
          { status: 500 }
        )
      }

      diagnostics.tickerCurrentInserted = tickerCurrentWriteResult.insertedOrUpdated

      tickerHistoryRows = buildTickerScoreHistoryRows(tickerCurrentRows, runDate, runTimestamp)

      const tickerHistoryWriteResult =
        tickerHistoryRows.length > 0
          ? await upsertInChunksDetailed(
              supabase.from("ticker_score_history"),
              "ticker_score_history",
              tickerHistoryRows,
              "ticker,score_date",
              (row) => `${row.ticker}:${row.score_date}`
            )
          : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

      if (tickerHistoryWriteResult.errors.length > 0) {
        return Response.json(
          {
            ok: false,
            error: "Failed writing ticker_score_history rows",
            debug: {
              diagnostics,
              errorSamples: tickerHistoryWriteResult.errors.slice(0, 5),
            },
          },
          { status: 500 }
        )
      }

      diagnostics.tickerHistoryInserted = tickerHistoryWriteResult.insertedOrUpdated
    }

    let retentionMessage = "skipped"
    let tickerRetentionMessage = "skipped"

    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("signal_history")
        .delete()
        .lt("scored_on", retentionCutoffString)

      retentionMessage = retentionError ? retentionError.message : "ok"

      if (rebuildTickerScores) {
        const { error: tickerRetentionError } = await supabase
          .from("ticker_score_history")
          .delete()
          .lt("score_date", retentionCutoffString)

        tickerRetentionMessage = tickerRetentionError ? tickerRetentionError.message : "ok"
      }
    }

    let strongBuyCount: number | null = null
    let eliteBuyCount: number | null = null

    if (includeCounts && rebuildTickerScores) {
      const [strongBuyRes, eliteBuyRes] = await Promise.all([
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 90),
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 97),
      ])

      strongBuyCount = strongBuyRes.error ? null : strongBuyRes.count ?? 0
      eliteBuyCount = eliteBuyRes.error ? null : eliteBuyRes.count ?? 0
    }

    return Response.json({
      ok: true,
      candidateUniverseRowsLoaded: diagnostics.candidateUniverseRowsLoaded,
      candidateHistoryRowsLoaded: diagnostics.candidateHistoryRowsLoaded,
      candidateRowsLoaded: diagnostics.candidateRowsLoaded,
      fallbackCandidateSourceUsed: diagnostics.fallbackCandidateSourceUsed,
      candidateSignalsInserted: diagnostics.candidateSignalsInserted,
      signalHistoryInserted: diagnostics.signalHistoryInserted,
      tickerCurrentInserted: diagnostics.tickerCurrentInserted,
      tickerHistoryInserted: diagnostics.tickerHistoryInserted,
      limit,
      lookbackDays,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      minTickerAppScore: MIN_TICKER_APP_SCORE,
      rebuildTickerScores,
      retentionCleanup: retentionMessage,
      tickerRetentionCleanup: tickerRetentionMessage,
      strongBuyCount,
      eliteBuyCount,
      diagnostics,
      message:
        "Technical signals generated from candidate data only. This version is optimized for fast, reliable serverless execution.",
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