import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CandidateHistoryRow = {
  company_id?: number | null
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price: number | null
  market_cap: number | null
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null
  sector: string | null
  industry: string | null
  business_description: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return: number | null
  return_5d: number | null
  return_10d: number | null
  return_20d: number | null
  relative_strength_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  breakout_10d: boolean
  above_sma_20: boolean
  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number
  passes_price: boolean
  passes_volume: boolean
  passes_dollar_volume: boolean
  passes_market_cap: boolean
  candidate_score: number
  included: boolean
  screen_reason: string
  last_screened_at: string
  updated_at: string
  screened_on: string
  snapshot_key: string
  created_at: string
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price: number | null
  market_cap: number | null
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null
  sector: string | null
  industry: string | null
  business_description: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return: number | null
  return_5d: number | null
  return_10d: number | null
  return_20d: number | null
  relative_strength_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  breakout_10d: boolean
  above_sma_20: boolean
  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number
  passes_price: boolean
  passes_volume: boolean
  passes_dollar_volume: boolean
  passes_market_cap: boolean
  candidate_score: number
  included: boolean
  screen_reason: string
  last_screened_at: string
  updated_at: string
}

type RawPtrTradeRow = {
  filer_name: string | null
  ticker: string | null
  action: string | null
  transaction_date: string | null
  report_date?: string | null
  amount_low: number | null
  amount_high: number | null
}

type PtrSignalSummary = {
  ptrBonus: number
  buyTradeCount: number
  uniqueFilers: number
  recentBuyCount: number
  totalAmountLow: number
  summary: string | null
}

type RankedRow = {
  row: CandidateHistoryRow
  ptrSummary: PtrSignalSummary | null
  selectionScore: number
  adjustedSelectionScore: number
  bucket: "strict" | "balanced" | "fallback"
  reasons: string[]
  sector: string
  industry: string
  sectorCrowdingShare: number
  industryCrowdingShare: number
  signalFamilyCount: number
}

type BreadthStats = {
  sectorCounts: Map<string, number>
  industryCounts: Map<string, number>
  totalRows: number
}

type SelectionState = {
  sectorCounts: Map<string, number>
  industryCounts: Map<string, number>
}

const DEFAULT_LIMIT = 30
const MAX_LIMIT = 60
const DEFAULT_TARGET_MIN = 12
const MAX_TARGET_MIN = 24

const MAX_FINAL_CANDIDATES = 30
const DB_CHUNK_SIZE = 250

const LTCS_INCLUDED_THRESHOLD = 50

const PTR_LOOKBACK_DAYS = 30
const PTR_RECENT_DAYS = 14
const MAX_PTR_BONUS = 8

const BASE_MAX_PER_SECTOR = 3
const BASE_MAX_PER_INDUSTRY = 2

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function normalizeLabel(value: string | null | undefined) {
  return (value || "").trim()
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function daysAgo(isoDate: string | null | undefined) {
  if (!isoDate) return null
  const ts = new Date(isoDate).getTime()
  if (Number.isNaN(ts)) return null
  return Math.max(0, Math.floor((Date.now() - ts) / (24 * 60 * 60 * 1000)))
}

async function upsertUniverseInChunks(table: any, rows: CandidateUniverseRow[]) {
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict: "ticker" })
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    }
  }

  return { errorCount, errors }
}

async function deleteAllUniverseRows(table: any) {
  const { error } = await table.delete().neq("ticker", "")
  return error ? error.message : null
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function getPtrMetrics(ptr: PtrSignalSummary | null | undefined) {
  return {
    ptrBonus: ptr?.ptrBonus ?? 0,
    buyTradeCount: ptr?.buyTradeCount ?? 0,
    recentBuyCount: ptr?.recentBuyCount ?? 0,
    uniqueFilers: ptr?.uniqueFilers ?? 0,
  }
}

function getSignalFamilyCount(
  row: CandidateHistoryRow,
  ptr: PtrSignalSummary | null | undefined
) {
  const hasPtr = Boolean((ptr?.buyTradeCount ?? 0) > 0)
  const hasFiling =
    Boolean(row.has_insider_trades) ||
    Boolean(row.has_ptr_forms) ||
    String(row.eligibility_reason || "").includes("high_priority_filings")
  const hasTechnical =
    Boolean(row.above_sma_20) ||
    Boolean(row.breakout_20d) ||
    Boolean(row.breakout_10d) ||
    (row.relative_strength_20d ?? -999) >= 1 ||
    (row.volume_ratio ?? 0) >= 1

  return Number(hasPtr) + Number(hasFiling) + Number(hasTechnical)
}

function buildBreadthStats(rows: CandidateHistoryRow[]) {
  const sectorCounts = new Map<string, number>()
  const industryCounts = new Map<string, number>()

  for (const row of rows) {
    const sector = normalizeLabel(row.sector)
    const industry = normalizeLabel(row.industry)

    if (sector) {
      sectorCounts.set(sector, (sectorCounts.get(sector) || 0) + 1)
    }

    if (industry) {
      industryCounts.set(industry, (industryCounts.get(industry) || 0) + 1)
    }
  }

  return {
    sectorCounts,
    industryCounts,
    totalRows: rows.length,
  }
}

function getSectorCrowdingShare(
  row: CandidateHistoryRow,
  breadthStats: BreadthStats
) {
  const sector = normalizeLabel(row.sector)
  if (!sector || breadthStats.totalRows <= 0) return 0
  return (breadthStats.sectorCounts.get(sector) || 0) / breadthStats.totalRows
}

function getIndustryCrowdingShare(
  row: CandidateHistoryRow,
  breadthStats: BreadthStats
) {
  const industry = normalizeLabel(row.industry)
  if (!industry || breadthStats.totalRows <= 0) return 0
  return (breadthStats.industryCounts.get(industry) || 0) / breadthStats.totalRows
}

function getDynamicSectorCap(row: CandidateHistoryRow, breadthStats: BreadthStats) {
  const share = getSectorCrowdingShare(row, breadthStats)
  if (share >= 0.22) return 2
  if (share >= 0.14) return 2
  return BASE_MAX_PER_SECTOR
}

function getDynamicIndustryCap(row: CandidateHistoryRow, breadthStats: BreadthStats) {
  const share = getIndustryCrowdingShare(row, breadthStats)
  if (share >= 0.14) return 1
  if (share >= 0.1) return 1
  return BASE_MAX_PER_INDUSTRY
}

function isLTCSEligible(row: any): boolean {
  return Boolean(
    row.passes_price &&
    row.passes_market_cap &&
    (row.candidate_score ?? 0) >= LTCS_INCLUDED_THRESHOLD
  )
}

function buildPtrSignalMap(rows: RawPtrTradeRow[]) {
  const grouped = new Map<string, RawPtrTradeRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue

    const action = String(row.action || "").trim().toLowerCase()
    if (action !== "buy" && action !== "purchase" && action !== "purchased") continue

    if (!grouped.has(ticker)) grouped.set(ticker, [])
    grouped.get(ticker)!.push(row)
  }

  const out = new Map<string, PtrSignalSummary>()

  for (const [ticker, tickerRows] of grouped.entries()) {
    const uniqueFilers = new Set(
      tickerRows.map((row) => String(row.filer_name || "").trim()).filter(Boolean)
    ).size

    const recentBuyCount = tickerRows.filter((row) => {
      const age = daysAgo(row.transaction_date || row.report_date)
      return age !== null && age <= PTR_RECENT_DAYS
    }).length

    const totalAmountLow = tickerRows.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)
    const buyTradeCount = tickerRows.length

    let ptrBonus = 0

    if (buyTradeCount >= 1) ptrBonus += 2
    if (buyTradeCount >= 2) ptrBonus += 1
    if (buyTradeCount >= 3) ptrBonus += 1
    if (uniqueFilers >= 2) ptrBonus += 1
    if (uniqueFilers >= 3) ptrBonus += 1
    if (recentBuyCount >= 1) ptrBonus += 1
    if (recentBuyCount >= 2) ptrBonus += 1
    if (totalAmountLow >= 100_001) ptrBonus += 1
    if (totalAmountLow >= 250_001) ptrBonus += 1
    if (totalAmountLow >= 500_001) ptrBonus += 1

    ptrBonus = Math.min(ptrBonus, MAX_PTR_BONUS)

    const summaryParts: string[] = []
    summaryParts.push(`${buyTradeCount} PTR buy${buyTradeCount === 1 ? "" : "s"}`)
    if (uniqueFilers > 0) summaryParts.push(`${uniqueFilers} filer${uniqueFilers === 1 ? "" : "s"}`)
    if (recentBuyCount > 0) summaryParts.push(`${recentBuyCount} recent`)
    if (totalAmountLow > 0) summaryParts.push(`min disclosed $${totalAmountLow.toLocaleString()}`)

    out.set(ticker, {
      ptrBonus,
      buyTradeCount,
      uniqueFilers,
      recentBuyCount,
      totalAmountLow,
      summary: summaryParts.length ? `PTR support: ${summaryParts.join(", ")}` : null,
    })
  }

  return out
}

function getSelectionScore(
  row: CandidateHistoryRow,
  ptr: PtrSignalSummary | null | undefined,
  breadthStats: BreadthStats
) {
  let score = Number(row.candidate_score ?? 0)
  const reasons: string[] = []

  const signalFamilyCount = getSignalFamilyCount(row, ptr)
  const sectorCrowdingShare = getSectorCrowdingShare(row, breadthStats)
  const industryCrowdingShare = getIndustryCrowdingShare(row, breadthStats)

  if (ptr?.ptrBonus) {
    score += ptr.ptrBonus + 4
    reasons.push(`PTR support +${ptr.ptrBonus + 4}`)
  }

  if ((ptr?.buyTradeCount ?? 0) >= 2) {
    score += 2
    reasons.push("multiple PTR buys")
  }

  if ((ptr?.uniqueFilers ?? 0) >= 2) {
    score += 2
    reasons.push("multiple PTR filers")
  }

  if (row.has_insider_trades) {
    score += 4
    reasons.push("insider filing support")
  }

  if ((row.eligibility_reason || "").includes("high_priority_filings")) {
    score += 3
    reasons.push("high-priority filing support")
  }

  if (row.has_ptr_forms) {
    score += 2
    reasons.push("ownership or PTR filing support")
  }

  if (row.has_clusters) {
    score += 2
    reasons.push("cluster support")
  }

  if (signalFamilyCount >= 3) {
    score += 5
    reasons.push("three signal families aligned")
  } else if (signalFamilyCount >= 2) {
    score += 2.5
    reasons.push("multi-signal alignment")
  } else {
    score -= 4
    reasons.push("single-signal setup")
  }

  if (sectorCrowdingShare >= 0.22) {
    score -= 5
    reasons.push(`crowded sector (${normalizeLabel(row.sector)})`)
  } else if (sectorCrowdingShare >= 0.14) {
    score -= 2.5
    reasons.push(`busy sector (${normalizeLabel(row.sector)})`)
  }

  if (industryCrowdingShare >= 0.14) {
    score -= 4
    reasons.push(`crowded industry (${normalizeLabel(row.industry)})`)
  } else if (industryCrowdingShare >= 0.1) {
    score -= 2
    reasons.push(`busy industry (${normalizeLabel(row.industry)})`)
  }

  return {
    selectionScore: Math.round(score * 100) / 100,
    reasons: uniqueStrings(reasons),
    sectorCrowdingShare,
    industryCrowdingShare,
    signalFamilyCount,
  }
}

function buildRankedRows(
  rows: CandidateHistoryRow[],
  ptrMap: Map<string, PtrSignalSummary>,
  breadthStats: BreadthStats
): RankedRow[] {
  return rows
    .map((row): RankedRow | null => {
      const ptrSummary = ptrMap.get(normalizeTicker(row.ticker)) ?? null
      const scoreResult = getSelectionScore(row, ptrSummary, breadthStats)

      let bucket: "strict" | "balanced" | "fallback" = "strict"
      if (!isLTCSEligible(row)) return null

      let adjustedSelectionScore = scoreResult.selectionScore + 2

      return {
        row,
        ptrSummary,
        selectionScore: scoreResult.selectionScore,
        adjustedSelectionScore: Math.round(adjustedSelectionScore * 100) / 100,
        bucket,
        reasons: scoreResult.reasons,
        sector: normalizeLabel(row.sector),
        industry: normalizeLabel(row.industry),
        sectorCrowdingShare: scoreResult.sectorCrowdingShare,
        industryCrowdingShare: scoreResult.industryCrowdingShare,
        signalFamilyCount: scoreResult.signalFamilyCount,
      }
    })
    .filter((item): item is RankedRow => item !== null)
    .sort((a, b) => {
      const bucketRank = { strict: 3, balanced: 2, fallback: 1 }
      if (bucketRank[b.bucket] !== bucketRank[a.bucket]) {
        return bucketRank[b.bucket] - bucketRank[a.bucket]
      }

      if (b.adjustedSelectionScore !== a.adjustedSelectionScore) {
        return b.adjustedSelectionScore - a.adjustedSelectionScore
      }

      if (b.signalFamilyCount !== a.signalFamilyCount) {
        return b.signalFamilyCount - a.signalFamilyCount
      }

      if ((b.ptrSummary?.ptrBonus ?? 0) !== (a.ptrSummary?.ptrBonus ?? 0)) {
        return (b.ptrSummary?.ptrBonus ?? 0) - (a.ptrSummary?.ptrBonus ?? 0)
      }

      if ((b.row.candidate_score ?? 0) !== (a.row.candidate_score ?? 0)) {
        return (b.row.candidate_score ?? 0) - (a.row.candidate_score ?? 0)
      }

      if ((b.row.relative_strength_20d ?? 0) !== (a.row.relative_strength_20d ?? 0)) {
        return (b.row.relative_strength_20d ?? 0) - (a.row.relative_strength_20d ?? 0)
      }

      if ((b.row.return_20d ?? 0) !== (a.row.return_20d ?? 0)) {
        return (b.row.return_20d ?? 0) - (a.row.return_20d ?? 0)
      }

      if ((b.row.volume_ratio ?? 0) !== (a.row.volume_ratio ?? 0)) {
        return (b.row.volume_ratio ?? 0) - (a.row.volume_ratio ?? 0)
      }

      return (b.row.market_cap ?? 0) - (a.row.market_cap ?? 0)
    })
}

function canAddRow(
  item: RankedRow,
  state: SelectionState,
  breadthStats: BreadthStats,
  relaxed: boolean
) {
  const sector = item.sector
  const industry = item.industry

  const sectorCount = sector ? state.sectorCounts.get(sector) || 0 : 0
  const industryCount = industry ? state.industryCounts.get(industry) || 0 : 0

  const sectorCap = getDynamicSectorCap(item.row, breadthStats) + (relaxed ? 1 : 0)
  const industryCap = getDynamicIndustryCap(item.row, breadthStats) + (relaxed ? 1 : 0)

  if (sector && sectorCount >= sectorCap) return false
  if (industry && industryCount >= industryCap) return false

  return true
}

function addRowToState(item: RankedRow, state: SelectionState) {
  if (item.sector) {
    state.sectorCounts.set(item.sector, (state.sectorCounts.get(item.sector) || 0) + 1)
  }

  if (item.industry) {
    state.industryCounts.set(item.industry, (state.industryCounts.get(item.industry) || 0) + 1)
  }
}

function selectWithCaps(
  source: RankedRow[],
  selected: RankedRow[],
  state: SelectionState,
  breadthStats: BreadthStats,
  maxToAdd: number,
  relaxed: boolean
) {
  const selectedTickers = new Set(selected.map((item) => normalizeTicker(item.row.ticker)))
  let added = 0

  for (const item of source) {
    if (added >= maxToAdd) break

    const ticker = normalizeTicker(item.row.ticker)
    if (!ticker || selectedTickers.has(ticker)) continue

    if (!canAddRow(item, state, breadthStats, relaxed)) continue

    selected.push(item)
    selectedTickers.add(ticker)
    addRowToState(item, state)
    added += 1
  }
}

function fillRemainingWithBestAllowed(
  ranked: RankedRow[],
  selected: RankedRow[],
  state: SelectionState,
  breadthStats: BreadthStats,
  limit: number,
  relaxed: boolean
) {
  const selectedTickers = new Set(selected.map((item) => normalizeTicker(item.row.ticker)))

  for (const item of ranked) {
    if (selected.length >= limit) break

    const ticker = normalizeTicker(item.row.ticker)
    if (!ticker || selectedTickers.has(ticker)) continue
    if (!canAddRow(item, state, breadthStats, relaxed)) continue

    selected.push(item)
    selectedTickers.add(ticker)
    addRowToState(item, state)
  }
}

function selectFinalRows(
  ranked: RankedRow[],
  limit: number,
  targetMin: number,
  breadthStats: BreadthStats
) {
  const strictRows = ranked.filter((item) => item.bucket === "strict")
  const balancedRows = ranked.filter((item) => item.bucket === "balanced")
  const fallbackRows = ranked.filter((item) => item.bucket === "fallback")

  const selected: RankedRow[] = []
  const state: SelectionState = {
    sectorCounts: new Map<string, number>(),
    industryCounts: new Map<string, number>(),
  }

  selectWithCaps(strictRows, selected, state, breadthStats, Math.min(12, limit), false)

  if (selected.length < targetMin) {
    selectWithCaps(
      balancedRows,
      selected,
      state,
      breadthStats,
      Math.min(Math.max(targetMin - selected.length, 6), limit - selected.length),
      false
    )
  } else {
    selectWithCaps(
      balancedRows,
      selected,
      state,
      breadthStats,
      Math.min(6, limit - selected.length),
      false
    )
  }

  if (selected.length < targetMin) {
    selectWithCaps(
      fallbackRows,
      selected,
      state,
      breadthStats,
      Math.min(Math.max(targetMin - selected.length, 4), limit - selected.length),
      false
    )
  } else {
    selectWithCaps(
      fallbackRows,
      selected,
      state,
      breadthStats,
      Math.min(4, limit - selected.length),
      false
    )
  }

  if (selected.length < targetMin) {
    fillRemainingWithBestAllowed(ranked, selected, state, breadthStats, targetMin, true)
  }

  if (selected.length < limit) {
    fillRemainingWithBestAllowed(ranked, selected, state, breadthStats, limit, false)
  }

  if (selected.length < limit) {
    fillRemainingWithBestAllowed(ranked, selected, state, breadthStats, limit, true)
  }

  return selected.slice(0, Math.min(limit, MAX_FINAL_CANDIDATES))
}

function toUniverseRow(
  ranked: RankedRow,
  selectedSource: string
): CandidateUniverseRow {
  const {
    row,
    ptrSummary,
    selectionScore,
    adjustedSelectionScore,
    bucket,
    reasons,
    signalFamilyCount,
  } = ranked

  const ptrReason = ptrSummary?.summary ? `; ${ptrSummary.summary}` : ""

  return {
    company_id: row.company_id ?? null,
    ticker: row.ticker,
    cik: row.cik,
    name: row.name,
    is_active: row.is_active ?? true,
    is_eligible: row.is_eligible ?? null,
    has_insider_trades: row.has_insider_trades ?? null,
    has_ptr_forms: row.has_ptr_forms ?? null,
    has_clusters: row.has_clusters ?? null,
    eligibility_reason: row.eligibility_reason ?? null,
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
    included: true,
    screen_reason: `Finalized ${selectedSource} ${bucket} candidate (candidate ${row.candidate_score}, selection ${selectionScore}, adjusted ${adjustedSelectionScore}, families ${signalFamilyCount}): ${reasons.join(", ")}${ptrReason}`,
    last_screened_at: row.last_screened_at,
    updated_at: new Date().toISOString(),
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
    const targetMin = Math.min(
      Math.max(1, parseInteger(searchParams.get("targetMin"), DEFAULT_TARGET_MIN)),
      MAX_TARGET_MIN
    )

    const candidateHistoryTable = supabase.from("candidate_screen_history") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any

    const { data: screenedDates, error: screenedDatesError } = await candidateHistoryTable
      .select("screened_on")
      .order("screened_on", { ascending: false })

    if (screenedDatesError) {
      return Response.json({ ok: false, error: screenedDatesError.message }, { status: 500 })
    }

    const orderedDates = uniqueStrings(
      (screenedDates || []).map((row: any) => String(row.screened_on || ""))
    )

    let screenedOn: string | null = null
    let snapshotRows: CandidateHistoryRow[] = []

    for (const candidateDate of orderedDates) {
      const { data: rows, error: rowsError } = await candidateHistoryTable
        .select("*")
        .eq("screened_on", candidateDate)

      if (rowsError) {
        return Response.json({ ok: false, error: rowsError.message }, { status: 500 })
      }

      const typedRows = (rows || []) as CandidateHistoryRow[]
      if (!typedRows.length) continue

      const viableRows = typedRows.filter(
        (row) =>
          (row.candidate_score ?? 0) >= 50 &&
          row.passes_price &&
          row.passes_market_cap
      )

      if (viableRows.length >= 12) {
        screenedOn = candidateDate
        snapshotRows = typedRows
        break
      }
    }

    if (!screenedOn || !snapshotRows.length) {
      return Response.json(
        {
          ok: false,
          error: "No viable candidate history snapshot found to finalize",
        },
        { status: 500 }
      )
    }

    const scoredRows = snapshotRows.filter(
      (row) =>
        row.candidate_score !== null &&
        row.candidate_score !== undefined &&
        row.is_active !== false
    )

    const snapshotTickers = uniqueStrings(scoredRows.map((row) => row.ticker))
    const ptrCutoff = new Date()
    ptrCutoff.setDate(ptrCutoff.getDate() - PTR_LOOKBACK_DAYS)
    const ptrCutoffString = toIsoDateString(ptrCutoff)

    let ptrMap = new Map<string, PtrSignalSummary>()
    let ptrDiagnostics: Record<string, any> = {
      loaded: false,
      rows: 0,
      tickersWithPtrSupport: 0,
      error: null as string | null,
    }

    if (snapshotTickers.length > 0) {
      try {
        const { data: ptrRows, error: ptrError } = await supabase
          .from("raw_ptr_trades")
          .select("filer_name, ticker, action, transaction_date, report_date, amount_low, amount_high")
          .in("ticker", snapshotTickers)
          .or(`transaction_date.gte.${ptrCutoffString},report_date.gte.${ptrCutoffString}`)

        if (ptrError) {
          ptrDiagnostics.error = ptrError.message
        } else {
          const normalizedRows = (ptrRows || []) as RawPtrTradeRow[]
          ptrMap = buildPtrSignalMap(normalizedRows)
          ptrDiagnostics = {
            loaded: true,
            rows: normalizedRows.length,
            tickersWithPtrSupport: ptrMap.size,
            error: null,
          }
        }
      } catch (error: any) {
        ptrDiagnostics.error = error?.message || "Unknown PTR lookup error"
      }
    }

    const breadthStats = buildBreadthStats(scoredRows)
    const rankedRows = buildRankedRows(scoredRows, ptrMap, breadthStats)

    const strictCount = rankedRows.filter((item) => item.bucket === "strict").length
    const balancedCount = rankedRows.filter((item) => item.bucket === "balanced").length
    const fallbackCount = rankedRows.filter((item) => item.bucket === "fallback").length

    const selectedRankedRows = selectFinalRows(rankedRows, limit, targetMin, breadthStats)

    if (!selectedRankedRows.length) {
      return Response.json(
        {
          ok: false,
          error: "Finalize step found zero eligible candidates",
          debug: {
            screenedOn,
            snapshotRowCount: snapshotRows.length,
            scoredRowCount: scoredRows.length,
            strictCount,
            balancedCount,
            fallbackCount,
            ptrDiagnostics,
          },
        },
        { status: 500 }
      )
    }

    const selectedTickers = new Set(selectedRankedRows.map((item) => item.row.ticker))
    const selectedSource =
      strictCount >= targetMin
        ? "strict-led"
        : balancedCount > 0
          ? "balanced-led"
          : "fallback-led"

    const universeRows = selectedRankedRows.map((item) =>
      toUniverseRow(item, selectedSource)
    )

    const deleteError = await deleteAllUniverseRows(candidateUniverseTable)
    if (deleteError) {
      return Response.json(
        {
          ok: false,
          error: "Failed clearing candidate universe before finalization",
          debug: { deleteError },
        },
        { status: 500 }
      )
    }

    const universeWrite = await upsertUniverseInChunks(candidateUniverseTable, universeRows)
    if (universeWrite.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing finalized candidate universe",
          debug: {
            errorCount: universeWrite.errorCount,
            errorSamples: universeWrite.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const { error: markFalseError } = await candidateHistoryTable
      .update({ included: false })
      .eq("screened_on", screenedOn)

    if (markFalseError) {
      return Response.json(
        {
          ok: false,
          error: "Failed resetting included flags on candidate history",
          debug: {
            message: markFalseError.message,
          },
        },
        { status: 500 }
      )
    }

    for (const chunk of chunkArray([...selectedTickers], DB_CHUNK_SIZE)) {
      const { error } = await candidateHistoryTable
        .update({ included: true })
        .eq("screened_on", screenedOn)
        .in("ticker", chunk)

      if (error) {
        return Response.json(
          {
            ok: false,
            error: "Failed marking finalized rows in candidate history",
            debug: {
              message: error.message,
            },
          },
          { status: 500 }
        )
      }
    }

    const ptrSelectedCount = selectedRankedRows.filter((item) =>
      ptrMap.has(normalizeTicker(item.row.ticker))
    ).length

    const selectedSectorCounts = selectedRankedRows.reduce<Record<string, number>>((acc, item) => {
      const sector = normalizeLabel(item.row.sector) || "Unknown"
      acc[sector] = (acc[sector] || 0) + 1
      return acc
    }, {})

    const selectedIndustryCounts = selectedRankedRows.reduce<Record<string, number>>((acc, item) => {
      const industry = normalizeLabel(item.row.industry) || "Unknown"
      acc[industry] = (acc[industry] || 0) + 1
      return acc
    }, {})

    return Response.json({
      ok: true,
      screenedOn,
      snapshotRowCount: snapshotRows.length,
      scoredRowCount: scoredRows.length,
      strictCount,
      balancedCount,
      fallbackCount,
      selectedSource,
      finalizedCount: universeRows.length,
      ptrDiagnostics,
      ptrSelectedCount,
      selectedSectorCounts,
      selectedIndustryCounts,
      firstTicker: universeRows[0]?.ticker ?? null,
      lastTicker: universeRows[universeRows.length - 1]?.ticker ?? null,
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown finalization error" },
      { status: 500 }
    )
  }
}