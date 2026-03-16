import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type Scope = "all" | "eligible" | "candidates"

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

type SignalRow = {
  run_id?: string | null
  ticker: string | null
  company_id?: number | null
  signal_type?: string | null
  source_type?: string | null
  strength?: number | null
  direction?: string | null
  summary?: string | null
  metadata?: Record<string, unknown> | null
  as_of_date?: string | null
}

type RawFilingRow = {
  company_id?: number | null
  ticker: string | null
  company_name?: string | null
  form_type?: string | null
  filed_at?: string | null
  filing_url?: string | null
  accession_no?: string | null
  cik?: string | null
}

type RawPtrTradeRow = {
  ticker: string | null
  politician_name?: string | null
  filer_name?: string | null
  transaction_type?: string | null
  action?: string | null
  trade_date?: string | null
  disclosure_date?: string | null
  amount_low?: number | null
  amount_high?: number | null
  source_url?: string | null
  ptr_url?: string | null
  trade_key?: string | null
}

type CandidateTechRow = {
  company_id?: number | null
  ticker: string
  name?: string | null
  market_cap?: number | null
  sector?: string | null
  industry?: string | null
  candidate_score?: number | null
  passed?: boolean | null
  as_of_date?: string | null
  breakout_20d?: boolean | null
  breakout_10d?: boolean | null
  above_sma_20?: boolean | null
  volume_ratio?: number | null
  return_20d?: number | null
  relative_strength_20d?: number | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
}

type FilingSummary = {
  ticker: string
  filingCount: number
  form4Count: number
  ownershipCount: number
  recentForm4Count: number
  clusterBuyProxy: boolean
  latestFiledAt: string | null
  latestAccessionNo: string | null
  latestFilingUrl: string | null
  notes: string[]
}

type PtrSummary = {
  ticker: string
  buyTradeCount: number
  sellTradeCount: number
  uniqueBuyFilers: number
  uniqueSellFilers: number
  recentBuyCount: number
  recentSellCount: number
  totalBuyAmountLow: number
  totalSellAmountLow: number
  biggestBuyAmountLow: number
  latestTradeDate: string | null
  latestDisclosureDate: string | null
  buyCluster: boolean
  strongBuying: boolean
  strongSelling: boolean
  notes: string[]
}

type ScoreBreakdown = {
  filingBase: number
  filingBonus: number
  ptrBase: number
  ptrBonus: number
  technical: number
  overlap: number
}

type TickerScoreRow = {
  run_id: string
  ticker: string
  company_id: number | null
  filing_signal_score: number
  ptr_signal_score: number
  combined_score: number
  rank: number | null
  confidence_label: "low" | "medium" | "high"
  as_of_date: string
  created_at: string
  updated_at: string
}

type RankedTicker = {
  ticker: string
  companyId: number | null
  filingSignalScore: number
  ptrSignalScore: number
  technicalScore: number
  combinedScore: number
  confidenceLabel: "low" | "medium" | "high"
  breakdown: ScoreBreakdown
  reasons: string[]
}

type Diagnostics = {
  scope: Scope
  signalsLoaded: number
  rawFilingsLoaded: number
  rawPtrTradesLoaded: number
  candidateRowsLoaded: number
  filingTickersScored: number
  ptrTickersScored: number
  rankedTickersBuilt: number
  tickerScoresInserted: number
  filteredBelowMinCombinedScore: number
}

const DEFAULT_LOOKBACK_DAYS = 21
const MAX_LOOKBACK_DAYS = 60
const DEFAULT_LIMIT = 1000
const MAX_LIMIT = 3000
const DEFAULT_MIN_COMBINED_SCORE = 35
const DEFAULT_PTR_LOOKBACK_DAYS = 60
const DEFAULT_PTR_RECENT_DAYS = 14
const DB_CHUNK_SIZE = 100
const QUERY_CHUNK_SIZE = 200
const SCORE_VERSION = "v9-insider-ptr-tech-blended"

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

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function round2(value: number) {
  return Math.round(value * 100) / 100
}

function uniqueStrings(values: Array<string | null | undefined>) {
  return Array.from(
    new Set(values.map((value) => (value || "").trim()).filter(Boolean))
  )
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function daysAgo(dateValue: string | null | undefined) {
  if (!dateValue) return null
  const parsed = new Date(dateValue)
  if (Number.isNaN(parsed.getTime())) return null
  return Math.max(
    0,
    Math.floor((Date.now() - parsed.getTime()) / (1000 * 60 * 60 * 24))
  )
}

function safeNumber(value: unknown) {
  const num = Number(value)
  return Number.isFinite(num) ? num : 0
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

async function loadSignals(
  supabase: any,
  limit: number
): Promise<SignalRow[]> {
  const { data, error } = await supabase
    .from("signals")
    .select(
      "run_id, ticker, company_id, signal_type, source_type, strength, direction, summary, metadata, as_of_date"
    )
    .order("as_of_date", { ascending: false })
    .limit(limit * 4)

  if (error) {
    throw new Error(`signals load failed: ${error.message}`)
  }

  return (data || []) as SignalRow[]
}

async function loadRecentFilings(
  supabase: any,
  cutoffIso: string
): Promise<RawFilingRow[]> {
  const { data, error } = await supabase
    .from("raw_filings")
    .select(
      "company_id, ticker, company_name, form_type, filed_at, filing_url, accession_no, cik"
    )
    .gte("filed_at", cutoffIso)
    .order("filed_at", { ascending: false })

  if (error) {
    throw new Error(`raw_filings load failed: ${error.message}`)
  }

  return (data || []) as RawFilingRow[]
}

async function loadRecentPtrTrades(
  supabase: any,
  ptrCutoffDate: string
): Promise<RawPtrTradeRow[]> {
  const { data, error } = await supabase
    .from("raw_ptr_trades")
    .select(
      "ticker, politician_name, filer_name, transaction_type, action, trade_date, disclosure_date, amount_low, amount_high, source_url, ptr_url, trade_key"
    )
    .or(`trade_date.gte.${ptrCutoffDate},disclosure_date.gte.${ptrCutoffDate}`)

  if (error) {
    throw new Error(`raw_ptr_trades load failed: ${error.message}`)
  }

  return (data || []) as RawPtrTradeRow[]
}

async function loadCandidateContextForTickers(
  supabase: any,
  tickers: string[],
  scope: Scope
): Promise<CandidateTechRow[]> {
  if (!tickers.length) return []

  const allRows: CandidateTechRow[] = []

  for (const tickerChunk of chunkArray(tickers, QUERY_CHUNK_SIZE)) {
    let query = supabase
      .from("candidate_universe")
      .select(
        "company_id, ticker, name, market_cap, sector, industry, candidate_score, passed, as_of_date, breakout_20d, breakout_10d, above_sma_20, volume_ratio, return_20d, relative_strength_20d, has_insider_trades, has_ptr_forms, has_clusters"
      )
      .in("ticker", tickerChunk)

    if (scope === "eligible" || scope === "candidates") {
      query = query.eq("passed", true)
    }

    const { data, error } = await query

    if (error) {
      throw new Error(`candidate_universe load failed: ${error.message}`)
    }

    allRows.push(...((data || []) as CandidateTechRow[]))
  }

  const byTicker = new Map<string, CandidateTechRow>()
  for (const row of allRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue

    const current = byTicker.get(ticker)
    if (!current) {
      byTicker.set(ticker, row)
      continue
    }

    const currentScore = safeNumber(current.candidate_score)
    const nextScore = safeNumber(row.candidate_score)
    if (nextScore > currentScore) {
      byTicker.set(ticker, row)
    }
  }

  return [...byTicker.values()]
}

function getPositivePtrTrade(row: RawPtrTradeRow) {
  const transactionType = String(row.transaction_type || "")
    .trim()
    .toLowerCase()
  const action = String(row.action || "")
    .trim()
    .toLowerCase()

  return (
    transactionType === "buy" ||
    transactionType === "exchange" ||
    action.includes("buy") ||
    action.includes("purchase") ||
    action.includes("exchange")
  )
}

function getNegativePtrTrade(row: RawPtrTradeRow) {
  const transactionType = String(row.transaction_type || "")
    .trim()
    .toLowerCase()
  const action = String(row.action || "")
    .trim()
    .toLowerCase()

  return (
    transactionType === "sell" ||
    action.includes("sell") ||
    action.includes("sale")
  )
}

function buildFilingSummaryMap(rows: RawFilingRow[]) {
  const byTicker = new Map<string, RawFilingRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const summaryMap = new Map<string, FilingSummary>()

  for (const [ticker, filingRows] of byTicker.entries()) {
    const sorted = [...filingRows].sort((a, b) => {
      return String(b.filed_at || "").localeCompare(String(a.filed_at || ""))
    })

    const form4Rows = sorted.filter((row) => {
      const form = String(row.form_type || "").trim().toUpperCase()
      return form === "4" || form === "4/A"
    })

    const ownershipRows = sorted.filter((row) => {
      const form = String(row.form_type || "").trim().toUpperCase()
      return (
        form === "13D" ||
        form === "13D/A" ||
        form === "13G" ||
        form === "13G/A" ||
        form === "SC 13D" ||
        form === "SC 13D/A" ||
        form === "SC 13G" ||
        form === "SC 13G/A"
      )
    })

    const recentForm4Count = form4Rows.filter((row) => {
      const age = daysAgo(row.filed_at)
      return age !== null && age <= 14
    }).length

    const clusterBuyProxy = form4Rows.length >= 2 || recentForm4Count >= 2

    const notes = uniqueStrings([
      form4Rows.length > 0
        ? `${form4Rows.length} recent Form 4 filing${form4Rows.length === 1 ? "" : "s"}`
        : null,
      ownershipRows.length > 0
        ? `${ownershipRows.length} ownership filing${ownershipRows.length === 1 ? "" : "s"}`
        : null,
      clusterBuyProxy ? "possible insider cluster activity" : null,
    ])

    summaryMap.set(ticker, {
      ticker,
      filingCount: sorted.length,
      form4Count: form4Rows.length,
      ownershipCount: ownershipRows.length,
      recentForm4Count,
      clusterBuyProxy,
      latestFiledAt: sorted[0]?.filed_at || null,
      latestAccessionNo: sorted[0]?.accession_no || null,
      latestFilingUrl: sorted[0]?.filing_url || null,
      notes,
    })
  }

  return summaryMap
}

function buildPtrSummaryMap(
  rows: RawPtrTradeRow[],
  ptrRecentDays: number
) {
  const byTicker = new Map<string, RawPtrTradeRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const output = new Map<string, PtrSummary>()

  for (const [ticker, trades] of byTicker.entries()) {
    const buys = trades.filter(getPositivePtrTrade)
    const sells = trades.filter(getNegativePtrTrade)

    const uniqueBuyFilers = new Set(
      buys
        .map((row) => String(row.politician_name || row.filer_name || "").trim())
        .filter(Boolean)
    ).size

    const uniqueSellFilers = new Set(
      sells
        .map((row) => String(row.politician_name || row.filer_name || "").trim())
        .filter(Boolean)
    ).size

    const recentBuyCount = buys.filter((row) => {
      const age = daysAgo(row.trade_date || row.disclosure_date)
      return age !== null && age <= ptrRecentDays
    }).length

    const recentSellCount = sells.filter((row) => {
      const age = daysAgo(row.trade_date || row.disclosure_date)
      return age !== null && age <= ptrRecentDays
    }).length

    const totalBuyAmountLow = buys.reduce(
      (sum, row) => sum + safeNumber(row.amount_low),
      0
    )
    const totalSellAmountLow = sells.reduce(
      (sum, row) => sum + safeNumber(row.amount_low),
      0
    )
    const biggestBuyAmountLow = buys.reduce(
      (max, row) => Math.max(max, safeNumber(row.amount_low)),
      0
    )

    const sortedTradeDates = trades
      .map((row) => String(row.trade_date || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))

    const sortedDisclosureDates = trades
      .map((row) => String(row.disclosure_date || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))

    const buyCluster = uniqueBuyFilers >= 2 || buys.length >= 3
    const strongBuying =
      recentBuyCount >= 1 &&
      (uniqueBuyFilers >= 2 ||
        totalBuyAmountLow >= 250001 ||
        biggestBuyAmountLow >= 100001 ||
        buys.length >= 2)

    const strongSelling =
      sells.length >= 2 &&
      (recentSellCount >= 1 || totalSellAmountLow >= 250001)

    const notes = uniqueStrings([
      buys.length > 0
        ? `${buys.length} PTR buy${buys.length === 1 ? "" : "s"}`
        : null,
      uniqueBuyFilers >= 2 ? "multiple PTR buyers" : null,
      recentBuyCount >= 1 ? `${recentBuyCount} recent PTR buy${recentBuyCount === 1 ? "" : "s"}` : null,
      totalBuyAmountLow >= 100001
        ? `meaningful disclosed buy size $${totalBuyAmountLow.toLocaleString()}+`
        : null,
      totalBuyAmountLow >= 500001
        ? "large disclosed PTR buying"
        : null,
      buyCluster ? "PTR buy cluster" : null,
      strongSelling ? "PTR selling headwind" : null,
    ])

    output.set(ticker, {
      ticker,
      buyTradeCount: buys.length,
      sellTradeCount: sells.length,
      uniqueBuyFilers,
      uniqueSellFilers,
      recentBuyCount,
      recentSellCount,
      totalBuyAmountLow,
      totalSellAmountLow,
      biggestBuyAmountLow,
      latestTradeDate: sortedTradeDates[0] || null,
      latestDisclosureDate: sortedDisclosureDates[0] || null,
      buyCluster,
      strongBuying,
      strongSelling,
      notes,
    })
  }

  return output
}

function buildSignalMap(rows: SignalRow[]) {
  const byTicker = new Map<string, SignalRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  return byTicker
}

function getMaxSignalStrength(rows: SignalRow[], sourceType: "filing" | "ptr") {
  return rows
    .filter((row) => String(row.source_type || "").trim().toLowerCase() === sourceType)
    .reduce((max, row) => Math.max(max, safeNumber(row.strength)), 0)
}

function scoreTechnicalContext(context: CandidateTechRow | null) {
  if (!context) return 0

  let score = 0

  const candidateScore = safeNumber(context.candidate_score)
  if (candidateScore >= 90) score += 22
  else if (candidateScore >= 80) score += 18
  else if (candidateScore >= 70) score += 14
  else if (candidateScore >= 60) score += 10
  else if (candidateScore >= 50) score += 6

  if (context.passed) score += 5
  if (context.breakout_20d) score += 14
  else if (context.breakout_10d) score += 9

  if (context.above_sma_20) score += 8

  const volumeRatio = safeNumber(context.volume_ratio)
  if (volumeRatio >= 2.2) score += 12
  else if (volumeRatio >= 1.6) score += 8
  else if (volumeRatio >= 1.2) score += 4

  const relativeStrength = safeNumber(context.relative_strength_20d)
  if (relativeStrength >= 8) score += 12
  else if (relativeStrength >= 4) score += 8
  else if (relativeStrength >= 1) score += 4

  const return20d = safeNumber(context.return_20d)
  if (return20d >= 15) score += 12
  else if (return20d >= 8) score += 8
  else if (return20d >= 3) score += 4

  return clamp(Math.round(score), 0, 100)
}

function scoreFilingComponent(
  signalRows: SignalRow[],
  filingSummary: FilingSummary | null,
  context: CandidateTechRow | null
) {
  let score = getMaxSignalStrength(signalRows, "filing")
  let bonus = 0
  const reasons: string[] = []

  if (filingSummary) {
    if (filingSummary.form4Count >= 1) {
      bonus += 10
      reasons.push("recent Form 4 activity")
    }
    if (filingSummary.form4Count >= 2) {
      bonus += 8
      reasons.push("multiple Form 4 filings")
    }
    if (filingSummary.recentForm4Count >= 2) {
      bonus += 8
      reasons.push("recent insider filing cluster")
    }
    if (filingSummary.ownershipCount >= 1) {
      bonus += 4
      reasons.push("ownership filing present")
    }
    if (filingSummary.ownershipCount >= 2) {
      bonus += 4
      reasons.push("multiple ownership filings")
    }
    if (filingSummary.filingCount >= 4) {
      bonus += 6
      reasons.push("heavy recent filing activity")
    }
    if (filingSummary.clusterBuyProxy) {
      bonus += 10
      reasons.push("possible cluster-buy behavior")
    }
  }

  if (context?.has_clusters) {
    bonus += 8
    reasons.push("screening flagged cluster activity")
  }

  if (context?.has_insider_trades) {
    bonus += 5
    reasons.push("screening flagged insider activity")
  }

  return {
    score: clamp(Math.round(score + bonus), 0, 100),
    bonus,
    reasons: uniqueStrings(reasons),
  }
}

function scorePtrComponent(
  signalRows: SignalRow[],
  ptrSummary: PtrSummary | null,
  context: CandidateTechRow | null
) {
  let score = getMaxSignalStrength(signalRows, "ptr")
  let bonus = 0
  const reasons: string[] = []

  if (ptrSummary) {
    if (ptrSummary.buyTradeCount >= 1) {
      bonus += 8
      reasons.push("at least one PTR buy")
    }
    if (ptrSummary.buyTradeCount >= 2) {
      bonus += 8
      reasons.push("multiple PTR buys")
    }
    if (ptrSummary.buyTradeCount >= 3) {
      bonus += 6
      reasons.push("three or more PTR buys")
    }

    if (ptrSummary.uniqueBuyFilers >= 2) {
      bonus += 8
      reasons.push("multiple PTR buyers")
    }
    if (ptrSummary.uniqueBuyFilers >= 3) {
      bonus += 6
      reasons.push("broad PTR participation")
    }

    if (ptrSummary.recentBuyCount >= 1) {
      bonus += 6
      reasons.push("recent PTR buying")
    }
    if (ptrSummary.recentBuyCount >= 2) {
      bonus += 6
      reasons.push("multiple recent PTR buys")
    }

    if (ptrSummary.totalBuyAmountLow >= 100001) {
      bonus += 6
      reasons.push("meaningful disclosed PTR amount")
    }
    if (ptrSummary.totalBuyAmountLow >= 250001) {
      bonus += 8
      reasons.push("strong disclosed PTR amount")
    }
    if (ptrSummary.totalBuyAmountLow >= 500001) {
      bonus += 10
      reasons.push("large disclosed PTR amount")
    }
    if (ptrSummary.totalBuyAmountLow >= 1000001) {
      bonus += 8
      reasons.push("very large disclosed PTR amount")
    }

    if (ptrSummary.buyCluster) {
      bonus += 10
      reasons.push("PTR buy cluster")
    }

    if (ptrSummary.strongBuying) {
      bonus += 8
      reasons.push("strong PTR buying pattern")
    }

    if (ptrSummary.strongSelling) {
      bonus -= 8
      reasons.push("PTR selling headwind")
    }
  }

  if (context?.has_ptr_forms) {
    bonus += 4
    reasons.push("screening flagged PTR activity")
  }

  return {
    score: clamp(Math.round(score + bonus), 0, 100),
    bonus,
    reasons: uniqueStrings(reasons),
  }
}

function buildCombinedScore(params: {
  filingSignalScore: number
  ptrSignalScore: number
  technicalScore: number
  filingSummary: FilingSummary | null
  ptrSummary: PtrSummary | null
  context: CandidateTechRow | null
}) {
  const {
    filingSignalScore,
    ptrSignalScore,
    technicalScore,
    filingSummary,
    ptrSummary,
    context,
  } = params

  let overlap = 0
  const reasons: string[] = []

  if (filingSignalScore >= 60 && ptrSignalScore >= 60) {
    overlap += 10
    reasons.push("both insider and PTR evidence are strong")
  }

  if (filingSignalScore >= 55 && technicalScore >= 60) {
    overlap += 5
    reasons.push("insider evidence aligns with technical confirmation")
  }

  if (ptrSignalScore >= 55 && technicalScore >= 60) {
    overlap += 5
    reasons.push("PTR evidence aligns with technical confirmation")
  }

  if (filingSummary?.clusterBuyProxy && ptrSummary?.buyCluster) {
    overlap += 8
    reasons.push("cluster activity appears on both insider and PTR sides")
  }

  if (filingSummary?.form4Count && filingSummary.form4Count >= 2 && ptrSummary?.buyTradeCount && ptrSummary.buyTradeCount >= 2) {
    overlap += 5
    reasons.push("multiple insider filings plus multiple PTR buys")
  }

  if (ptrSummary?.totalBuyAmountLow && ptrSummary.totalBuyAmountLow >= 250001 && context?.breakout_20d) {
    overlap += 4
    reasons.push("big PTR buying aligns with breakout setup")
  }

  if (filingSummary?.form4Count && filingSummary.form4Count >= 2 && context?.breakout_20d) {
    overlap += 4
    reasons.push("insider filing cluster aligns with breakout setup")
  }

  if (context?.has_clusters && ptrSummary?.uniqueBuyFilers && ptrSummary.uniqueBuyFilers >= 2) {
    overlap += 3
    reasons.push("screened cluster activity plus broad PTR participation")
  }

  if (
    filingSignalScore >= 50 &&
    ptrSignalScore >= 50 &&
    technicalScore >= 50
  ) {
    overlap += 6
    reasons.push("all three pillars are constructive")
  }

  const weighted =
    filingSignalScore * 0.42 +
    ptrSignalScore * 0.33 +
    technicalScore * 0.25 +
    overlap

  const combined = clamp(Math.round(weighted), 0, 100)

  return {
    combined,
    overlap,
    reasons: uniqueStrings(reasons),
  }
}

function getConfidenceLabel(params: {
  filingSignalScore: number
  ptrSignalScore: number
  technicalScore: number
  combinedScore: number
}) {
  const { filingSignalScore, ptrSignalScore, technicalScore, combinedScore } =
    params

  const strongPillars = [
    filingSignalScore >= 60,
    ptrSignalScore >= 60,
    technicalScore >= 60,
  ].filter(Boolean).length

  if (combinedScore >= 80 && strongPillars >= 2) return "high"
  if (combinedScore >= 60) return "medium"
  return "low"
}

function buildTickerScoreRows(params: {
  signals: SignalRow[]
  filings: RawFilingRow[]
  ptrTrades: RawPtrTradeRow[]
  candidateRows: CandidateTechRow[]
  runId: string
  runTimestamp: string
  minCombinedScore: number
  ptrRecentDays: number
  limit: number
}) {
  const {
    signals,
    filings,
    ptrTrades,
    candidateRows,
    runId,
    runTimestamp,
    minCombinedScore,
    ptrRecentDays,
    limit,
  } = params

  const signalMap = buildSignalMap(signals)
  const filingSummaryMap = buildFilingSummaryMap(filings)
  const ptrSummaryMap = buildPtrSummaryMap(ptrTrades, ptrRecentDays)

  const candidateMap = new Map<string, CandidateTechRow>()
  for (const row of candidateRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    candidateMap.set(ticker, row)
  }

  const tickerSet = new Set<string>([
    ...signalMap.keys(),
    ...filingSummaryMap.keys(),
    ...ptrSummaryMap.keys(),
  ])

  const ranked: RankedTicker[] = []
  let filingTickersScored = 0
  let ptrTickersScored = 0
  let filteredBelowMinCombinedScore = 0

  for (const ticker of tickerSet) {
    const tickerSignals = signalMap.get(ticker) || []
    const filingSummary = filingSummaryMap.get(ticker) || null
    const ptrSummary = ptrSummaryMap.get(ticker) || null
    const context = candidateMap.get(ticker) || null

    const filingComponent = scoreFilingComponent(
      tickerSignals,
      filingSummary,
      context
    )
    const ptrComponent = scorePtrComponent(tickerSignals, ptrSummary, context)
    const technicalScore = scoreTechnicalContext(context)

    if (filingComponent.score > 0) filingTickersScored += 1
    if (ptrComponent.score > 0) ptrTickersScored += 1

    const combined = buildCombinedScore({
      filingSignalScore: filingComponent.score,
      ptrSignalScore: ptrComponent.score,
      technicalScore,
      filingSummary,
      ptrSummary,
      context,
    })

    if (combined.combined < minCombinedScore) {
      filteredBelowMinCombinedScore += 1
      continue
    }

    const reasons = uniqueStrings([
      ...filingComponent.reasons,
      ...ptrComponent.reasons,
      ...combined.reasons,
      filingSummary?.notes?.[0] || null,
      filingSummary?.notes?.[1] || null,
      ptrSummary?.notes?.[0] || null,
      ptrSummary?.notes?.[1] || null,
      context?.breakout_20d ? "20-day breakout is present" : null,
      context?.breakout_10d ? "10-day breakout is present" : null,
      context?.above_sma_20 ? "price is above the 20-day average" : null,
      safeNumber(context?.volume_ratio) >= 1.6 ? "volume is elevated" : null,
      safeNumber(context?.relative_strength_20d) >= 4
        ? "relative strength is constructive"
        : null,
    ])

    const companyId = Number(context?.company_id ?? null)
    const confidenceLabel = getConfidenceLabel({
      filingSignalScore: filingComponent.score,
      ptrSignalScore: ptrComponent.score,
      technicalScore,
      combinedScore: combined.combined,
    })

    ranked.push({
      ticker,
      companyId: Number.isFinite(companyId) ? companyId : null,
      filingSignalScore: filingComponent.score,
      ptrSignalScore: ptrComponent.score,
      technicalScore,
      combinedScore: combined.combined,
      confidenceLabel,
      breakdown: {
        filingBase: filingComponent.score - filingComponent.bonus,
        filingBonus: filingComponent.bonus,
        ptrBase: ptrComponent.score - ptrComponent.bonus,
        ptrBonus: ptrComponent.bonus,
        technical: technicalScore,
        overlap: combined.overlap,
      },
      reasons,
    })
  }

  ranked.sort((a, b) => {
    if (b.combinedScore !== a.combinedScore) {
      return b.combinedScore - a.combinedScore
    }
    if (b.filingSignalScore !== a.filingSignalScore) {
      return b.filingSignalScore - a.filingSignalScore
    }
    if (b.ptrSignalScore !== a.ptrSignalScore) {
      return b.ptrSignalScore - a.ptrSignalScore
    }
    if (b.technicalScore !== a.technicalScore) {
      return b.technicalScore - a.technicalScore
    }
    return a.ticker.localeCompare(b.ticker)
  })

  const limited = ranked.slice(0, limit)

  const rows: TickerScoreRow[] = limited.map((row, index) => ({
    run_id: runId,
    ticker: row.ticker,
    company_id: row.companyId,
    filing_signal_score: row.filingSignalScore,
    ptr_signal_score: row.ptrSignalScore,
    combined_score: row.combinedScore,
    rank: index + 1,
    confidence_label: row.confidenceLabel,
    as_of_date: runTimestamp,
    created_at: runTimestamp,
    updated_at: runTimestamp,
  }))

  return {
    rows,
    ranked,
    diagnostics: {
      filingTickersScored,
      ptrTickersScored,
      filteredBelowMinCombinedScore,
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
    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )
    const ptrLookbackDays = Math.max(
      7,
      parseInteger(searchParams.get("ptrLookbackDays"), DEFAULT_PTR_LOOKBACK_DAYS)
    )
    const ptrRecentDays = Math.max(
      3,
      parseInteger(searchParams.get("ptrRecentDays"), DEFAULT_PTR_RECENT_DAYS)
    )
    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const minCombinedScore = clamp(
      parseInteger(searchParams.get("minCombinedScore"), DEFAULT_MIN_COMBINED_SCORE),
      0,
      100
    )
    const includePreview =
      (searchParams.get("includePreview") || "false").toLowerCase() === "true"

    if (!["all", "eligible", "candidates"].includes(scopeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, candidates`,
        },
        { status: 400 }
      )
    }

    const scope = scopeParam as Scope
    const now = new Date()
    const runTimestamp = now.toISOString()
    const runId = `ticker_scores_${runTimestamp}`

    const filingCutoff = new Date(now)
    filingCutoff.setDate(filingCutoff.getDate() - lookbackDays)
    const filingCutoffIso = filingCutoff.toISOString()

    const ptrCutoff = new Date(now)
    ptrCutoff.setDate(ptrCutoff.getDate() - ptrLookbackDays)
    const ptrCutoffDate = toIsoDateString(ptrCutoff)

    const diagnostics: Diagnostics = {
      scope,
      signalsLoaded: 0,
      rawFilingsLoaded: 0,
      rawPtrTradesLoaded: 0,
      candidateRowsLoaded: 0,
      filingTickersScored: 0,
      ptrTickersScored: 0,
      rankedTickersBuilt: 0,
      tickerScoresInserted: 0,
      filteredBelowMinCombinedScore: 0,
    }

    const [signals, rawFilings, rawPtrTrades] = await Promise.all([
      loadSignals(supabase, limit),
      loadRecentFilings(supabase, filingCutoffIso),
      loadRecentPtrTrades(supabase, ptrCutoffDate),
    ])

    diagnostics.signalsLoaded = signals.length
    diagnostics.rawFilingsLoaded = rawFilings.length
    diagnostics.rawPtrTradesLoaded = rawPtrTrades.length

    const tickerUniverse = uniqueStrings([
      ...signals.map((row) => row.ticker),
      ...rawFilings.map((row) => row.ticker),
      ...rawPtrTrades.map((row) => row.ticker),
    ]).slice(0, MAX_LIMIT * 2)

    const candidateRows = await loadCandidateContextForTickers(
      supabase,
      tickerUniverse,
      scope
    )
    diagnostics.candidateRowsLoaded = candidateRows.length

    const built = buildTickerScoreRows({
      signals,
      filings: rawFilings,
      ptrTrades: rawPtrTrades,
      candidateRows,
      runId,
      runTimestamp,
      minCombinedScore,
      ptrRecentDays,
      limit,
    })

    diagnostics.filingTickersScored = built.diagnostics.filingTickersScored
    diagnostics.ptrTickersScored = built.diagnostics.ptrTickersScored
    diagnostics.filteredBelowMinCombinedScore =
      built.diagnostics.filteredBelowMinCombinedScore
    diagnostics.rankedTickersBuilt = built.rows.length

    const writeResult =
      built.rows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("ticker_scores_current"),
            "ticker_scores_current",
            built.rows,
            "ticker",
            (row) => row.ticker
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (writeResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing ticker_scores_current rows",
          debug: {
            diagnostics,
            errorSamples: writeResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    diagnostics.tickerScoresInserted = writeResult.insertedOrUpdated

    return Response.json({
      ok: true,
      stage: "ticker_scores",
      scope,
      scoreVersion: SCORE_VERSION,
      lookbackDays,
      ptrLookbackDays,
      ptrRecentDays,
      limit,
      minCombinedScore,
      diagnostics,
      preview: includePreview
        ? built.ranked.slice(0, 25).map((row, index) => ({
            rank: index + 1,
            ticker: row.ticker,
            filingSignalScore: row.filingSignalScore,
            ptrSignalScore: row.ptrSignalScore,
            technicalScore: row.technicalScore,
            combinedScore: row.combinedScore,
            confidenceLabel: row.confidenceLabel,
            reasons: row.reasons.slice(0, 8),
            breakdown: row.breakdown,
          }))
        : undefined,
      message:
        "Ticker scores rebuilt using insider filings, cluster-buy signals, PTR breadth and size, plus technical confirmation.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown ticker score rebuild error",
      },
      { status: 500 }
    )
  }
}