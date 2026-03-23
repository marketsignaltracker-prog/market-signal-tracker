import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

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
  signal_key?: string | null
  ticker: string
  company_name?: string | null
  business_description?: string | null
  app_score?: number | null
  score?: number | null
  raw_score?: number | null
  bias?: string | null
  board_bucket?: string | null
  signal_strength_bucket?: string | null
  score_version?: string | null
  score_updated_at?: string | null
  stacked_signal_count?: number | null
  score_breakdown?: Record<string, number> | null
  signal_reasons?: string[] | null
  score_caps_applied?: string[] | null
  signal_tags?: string[] | null
  signal_type?: string | null
  signal_source?: string | null
  signal_category?: string | null
  title?: string | null
  summary?: string | null
  filed_at?: string | null
  accession_no?: string | null
  source_form?: string | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  market_cap?: number | null
  sector?: string | null
  industry?: string | null
  insider_action?: string | null
  insider_shares?: number | null
  insider_avg_price?: number | null
  insider_buy_value?: number | null
  cluster_buyers?: number | null
  cluster_shares?: number | null
  price_return_5d?: number | null
  price_return_20d?: number | null
  volume_ratio?: number | null
  breakout_20d?: boolean | null
  breakout_52w?: boolean | null
  above_50dma?: boolean | null
  trend_aligned?: boolean | null
  price_confirmed?: boolean | null
  relative_strength_20d?: number | null
  earnings_surprise_pct?: number | null
  revenue_growth_pct?: number | null
  guidance_flag?: boolean | null
  age_days?: number | null
  freshness_bucket?: string | null
  ticker_score_change_1d?: number | null
  ticker_score_change_7d?: number | null
  updated_at?: string | null
  created_at?: string | null
}

type RawPtrTradeRow = {
  ticker?: string | null
  filer_name?: string | null
  action?: string | null
  transaction_date?: string | null
  report_date?: string | null
  amount_low?: number | null
  amount_high?: number | null
  amount_range?: string | null
}

type PtrSummary = {
  ticker: string
  ptrBonus: number
  ptrPenalty: number
  buyTradeCount: number
  sellTradeCount: number
  uniqueBuyFilers: number
  uniqueSellFilers: number
  recentBuyCount: number
  recentSellCount: number
  totalBuyAmountLow: number
  totalSellAmountLow: number
  buyCluster: boolean
  strongBuying: boolean
  strongSelling: boolean
  latestTradeDate: string | null
  latestReportDate: string | null
  notes: string[]
  summary: string | null
}

type BreadthStats = {
  sectorCounts: Map<string, number>
  industryCounts: Map<string, number>
  totalTickers: number
}

const DEFAULT_LOOKBACK_DAYS = 31
const MAX_LOOKBACK_DAYS = 90
const DEFAULT_LIMIT = 1000
const MAX_LIMIT = 3000
const RETENTION_DAYS = 30
const SCORE_VERSION = "v12-ltcs-compounder"

const DEFAULT_PTR_LOOKBACK_DAYS = 60
const MAX_PTR_LOOKBACK_DAYS = 120
const DEFAULT_PTR_RECENT_DAYS = 14
const MAX_PTR_RECENT_DAYS = 30

const MIN_SIGNAL_APP_SCORE = 58
const MIN_TICKER_APP_SCORE = 55
const MIN_COMBINED_SCORE = 60

const DB_CHUNK_SIZE = 100

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function normalizeLabel(value: string | null | undefined) {
  return (value || "").trim()
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

function daysAgo(isoDate: string | null | undefined) {
  if (!isoDate) return null
  const ts = new Date(isoDate).getTime()
  if (Number.isNaN(ts)) return null
  return Math.max(0, Math.floor((Date.now() - ts) / (24 * 60 * 60 * 1000)))
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
  if (score >= 94) return "Elite Buy"
  if (score >= 84) return "Strong Buy"
  return "Buy"
}

function countPositiveEvidencePillars(breakdown: Record<string, number>) {
  return [
    (breakdown.base || 0) > 0,
    (breakdown.ptr || 0) > 0,
    (breakdown.filings || 0) > 0,
    (breakdown.candidate_score || 0) > 0 || (breakdown.included || 0) > 0,
    (breakdown.breakout || 0) > 0 || (breakdown.trend || 0) > 0,
    (breakdown.volume || 0) > 0,
    (breakdown.relative_strength || 0) > 0,
    (breakdown.freshness || 0) > 0 || (breakdown.momentum || 0) > 0,
  ].filter(Boolean).length
}

function getSignalFamilyCountFromRow(row: SignalRow) {
  const tags = new Set((row.signal_tags || []).map((tag) => String(tag).trim().toLowerCase()))

  const hasPtr =
    tags.has("ptr-support") ||
    tags.has("ptr-cluster") ||
    tags.has("ptr-strong-buying") ||
    String(row.signal_source || "").toLowerCase().includes("ptr")

  const hasFiling =
    tags.has("insider-filing") ||
    tags.has("ownership-filing") ||
    tags.has("catalyst-filing") ||
    ["form4", "13d", "13g", "8k"].includes(String(row.signal_source || "").toLowerCase())

  const hasTechnical =
    tags.has("breakout-20d") ||
    tags.has("breakout-10d") ||
    tags.has("above-sma20") ||
    tags.has("volume-confirmed") ||
    tags.has("relative-strength") ||
    Boolean(row.breakout_20d) ||
    Boolean(row.above_50dma) ||
    Boolean(row.price_confirmed)

  return Number(hasPtr) + Number(hasFiling) + Number(hasTechnical)
}

function buildBreadthStats(signalRows: SignalRow[]): BreadthStats {
  const byTicker = new Map<string, SignalRow>()

  for (const row of signalRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) {
      byTicker.set(ticker, row)
    }
  }

  const sectorCounts = new Map<string, number>()
  const industryCounts = new Map<string, number>()

  for (const row of byTicker.values()) {
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
    totalTickers: byTicker.size,
  }
}

function buildPtrSummaryMap(rows: RawPtrTradeRow[], ptrRecentDays: number) {
  const byTicker = new Map<string, RawPtrTradeRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const output = new Map<string, PtrSummary>()

  for (const [ticker, trades] of byTicker.entries()) {
    const buys = trades.filter((row) => {
      const action = String(row.action || "").trim().toLowerCase()
      return action === "buy" || action === "purchase" || action === "purchased"
    })

    const sells = trades.filter((row) => {
      const action = String(row.action || "").trim().toLowerCase()
      return action === "sell" || action === "sale" || action === "sold"
    })

    const uniqueBuyFilers = new Set(
      buys.map((row) => String(row.filer_name || "").trim()).filter(Boolean)
    ).size

    const uniqueSellFilers = new Set(
      sells.map((row) => String(row.filer_name || "").trim()).filter(Boolean)
    ).size

    const recentBuyCount = buys.filter((row) => {
      const age = daysAgo(row.transaction_date || row.report_date)
      return age !== null && age <= ptrRecentDays
    }).length

    const recentSellCount = sells.filter((row) => {
      const age = daysAgo(row.transaction_date || row.report_date)
      return age !== null && age <= ptrRecentDays
    }).length

    const totalBuyAmountLow = buys.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)
    const totalSellAmountLow = sells.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)

    const allTradeDates = trades
      .map((row) => String(row.transaction_date || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))

    const allReportDates = trades
      .map((row) => String(row.report_date || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))

    let ptrBonus = 0
    let ptrPenalty = 0
    const notes: string[] = []

    if (buys.length >= 1) {
      ptrBonus += 3
      notes.push("at least one PTR buy")
    }
    if (buys.length >= 2) {
      ptrBonus += 3
      notes.push("multiple PTR buys")
    }
    if (buys.length >= 3) {
      ptrBonus += 3
      notes.push("three or more PTR buys")
    }

    if (uniqueBuyFilers >= 2) {
      ptrBonus += 4
      notes.push("multiple PTR buyers")
    }
    if (uniqueBuyFilers >= 3) {
      ptrBonus += 4
      notes.push("broad PTR participation")
    }

    if (recentBuyCount >= 1) {
      ptrBonus += 3
      notes.push("recent PTR buy")
    }
    if (recentBuyCount >= 2) {
      ptrBonus += 3
      notes.push("multiple recent PTR buys")
    }

    if (totalBuyAmountLow >= 100_001) {
      ptrBonus += 2
      notes.push("meaningful disclosed buy size")
    }
    if (totalBuyAmountLow >= 250_001) {
      ptrBonus += 3
      notes.push("strong disclosed buy size")
    }
    if (totalBuyAmountLow >= 500_001) {
      ptrBonus += 4
      notes.push("very strong disclosed buy size")
    }
    if (totalBuyAmountLow >= 1_000_001) {
      ptrBonus += 4
      notes.push("institutional-scale disclosed buy size")
    }

    if (sells.length >= 2 && totalSellAmountLow > totalBuyAmountLow) {
      ptrPenalty -= 4
      notes.push("PTR selling headwind")
    }
    if (recentSellCount >= 2 && recentBuyCount === 0) {
      ptrPenalty -= 4
      notes.push("recent PTR selling")
    }
    if (uniqueSellFilers >= 2 && uniqueBuyFilers === 0) {
      ptrPenalty -= 3
      notes.push("broad PTR selling participation")
    }

    const buyCluster = uniqueBuyFilers >= 2 || buys.length >= 3
    const strongBuying =
      recentBuyCount >= 1 &&
      (uniqueBuyFilers >= 2 || totalBuyAmountLow >= 250_001 || buys.length >= 2)

    const strongSelling =
      sells.length >= 2 &&
      (recentSellCount >= 1 || totalSellAmountLow >= 250_001)

    const summaryParts: string[] = []
    if (buys.length > 0) summaryParts.push(`${buys.length} buy${buys.length === 1 ? "" : "s"}`)
    if (uniqueBuyFilers > 0) {
      summaryParts.push(`${uniqueBuyFilers} buyer${uniqueBuyFilers === 1 ? "" : "s"}`)
    }
    if (recentBuyCount > 0) {
      summaryParts.push(`${recentBuyCount} recent`)
    }
    if (totalBuyAmountLow > 0) {
      summaryParts.push(`min disclosed $${totalBuyAmountLow.toLocaleString()}`)
    }

    output.set(ticker, {
      ticker,
      ptrBonus,
      ptrPenalty,
      buyTradeCount: buys.length,
      sellTradeCount: sells.length,
      uniqueBuyFilers,
      uniqueSellFilers,
      recentBuyCount,
      recentSellCount,
      totalBuyAmountLow,
      totalSellAmountLow,
      buyCluster,
      strongBuying,
      strongSelling,
      latestTradeDate: allTradeDates[0] ?? null,
      latestReportDate: allReportDates[0] ?? null,
      notes,
      summary: summaryParts.length ? `PTR support: ${summaryParts.join(", ")}` : null,
    })
  }

  return output
}

function buildTickerScoresCurrentRows(
  signalRows: SignalRow[],
  runTimestamp: string,
  ptrMap: Map<string, PtrSummary>,
  breadthStats: BreadthStats,
  minCombinedScore: number,
  ltcsScoreMap: Map<string, number>
) {
  const byTicker = new Map<string, SignalRow[]>()

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

      const filedAtDiff =
        new Date(b.filed_at || 0).getTime() - new Date(a.filed_at || 0).getTime()
      if (filedAtDiff !== 0) return filedAtDiff

      return String(a.signal_key || "").localeCompare(String(b.signal_key || ""))
    })

    const primary = sorted[0]
    const primaryScore = Number(primary.app_score || 0)
    const ptr = ptrMap.get(ticker) ?? null

    const sector = normalizeLabel(primary.sector)
    const industry = normalizeLabel(primary.industry)
    const sectorCount = sector ? breadthStats.sectorCounts.get(sector) || 0 : 0
    const industryCount = industry ? breadthStats.industryCounts.get(industry) || 0 : 0
    const sectorShare = breadthStats.totalTickers > 0 ? sectorCount / breadthStats.totalTickers : 0
    const industryShare = breadthStats.totalTickers > 0 ? industryCount / breadthStats.totalTickers : 0

    const scoreBreakdown: Record<string, number> = {}
    const signalReasons = new Set<string>()
    const scoreCapsApplied = new Set<string>()
    const signalTags = new Set<string>()
    const accessionNos: string[] = []
    const signalKeys: string[] = []
    const sourceForms: string[] = []
    const signalSources = new Set<string>()
    const signalCategories = new Set<string>()

    let maxSignalFamilyCount = 0

    for (const row of sorted) {
      if (row.signal_key) signalKeys.push(String(row.signal_key))
      if (row.accession_no) accessionNos.push(String(row.accession_no))
      if (row.source_form) sourceForms.push(String(row.source_form))
      if (row.signal_source) signalSources.add(String(row.signal_source).toLowerCase())
      if (row.signal_category) signalCategories.add(String(row.signal_category))

      for (const tag of row.signal_tags || []) signalTags.add(tag)
      for (const reason of row.signal_reasons || []) signalReasons.add(reason)
      for (const cap of row.score_caps_applied || []) scoreCapsApplied.add(cap)

      const breakdown = (row.score_breakdown || {}) as Record<string, number>
      for (const [key, value] of Object.entries(breakdown)) {
        scoreBreakdown[key] = round2((scoreBreakdown[key] || 0) + Number(value || 0)) ?? 0
      }

      maxSignalFamilyCount = Math.max(maxSignalFamilyCount, getSignalFamilyCountFromRow(row))
    }

    let stackedScore = primaryScore

    if (sorted.length >= 2) {
      stackedScore += 3
      scoreBreakdown.stack_count = round2((scoreBreakdown.stack_count || 0) + 3) ?? 0
    }
    if (sorted.length >= 3) {
      stackedScore += 2
      scoreBreakdown.stack_count = round2((scoreBreakdown.stack_count || 0) + 2) ?? 0
    }
    if (sorted.length >= 4) {
      stackedScore += 1
      scoreBreakdown.stack_count = round2((scoreBreakdown.stack_count || 0) + 1) ?? 0
    }

    if (ptr) {
      stackedScore += ptr.ptrBonus + ptr.ptrPenalty
      scoreBreakdown.ptr =
        round2((scoreBreakdown.ptr || 0) + ptr.ptrBonus + ptr.ptrPenalty) ?? 0

      if (ptr.buyCluster) {
        stackedScore += 4
        scoreBreakdown.ptr = round2((scoreBreakdown.ptr || 0) + 4) ?? 0
        scoreCapsApplied.add("ptr-cluster-bonus")
      }

      if (ptr.strongBuying) {
        stackedScore += 5
        scoreBreakdown.ptr = round2((scoreBreakdown.ptr || 0) + 5) ?? 0
        scoreCapsApplied.add("ptr-strong-buying-bonus")
      }

      if (ptr.buyCluster && signalSources.has("form4")) {
        stackedScore += 3
        scoreBreakdown.ptr = round2((scoreBreakdown.ptr || 0) + 3) ?? 0
        scoreCapsApplied.add("ptr-plus-insider-bonus")
      }

      if (ptr.buyCluster && signalSources.has("13d")) {
        stackedScore += 2
        scoreBreakdown.ptr = round2((scoreBreakdown.ptr || 0) + 2) ?? 0
        scoreCapsApplied.add("ptr-plus-ownership-bonus")
      }

      if (ptr.strongSelling) {
        stackedScore -= 4
        scoreBreakdown.ptr = round2((scoreBreakdown.ptr || 0) - 4) ?? 0
        scoreCapsApplied.add("ptr-selling-headwind")
      }

      for (const note of ptr.notes) {
        signalReasons.add(note)
      }

      signalTags.add("ptr-priority")
      if (ptr.strongBuying) signalTags.add("ptr-strong-buying")
      if (ptr.buyCluster) signalTags.add("ptr-buy-cluster")
    }

    const hasForm4 = signalSources.has("form4")
    const has13D = signalSources.has("13d")
    const has13G = signalSources.has("13g")
    const has8K = signalSources.has("8k")
    const hasBreakout =
      signalSources.has("breakout") ||
      Boolean(primary.breakout_20d) ||
      Boolean(primary.price_confirmed)

    if (hasForm4) {
      stackedScore += 4
      scoreBreakdown.filings = round2((scoreBreakdown.filings || 0) + 4) ?? 0
    }

    if (has13D || has13G) {
      stackedScore += 4
      scoreBreakdown.filings = round2((scoreBreakdown.filings || 0) + 4) ?? 0
    }

    if (has8K) {
      stackedScore += 2
      scoreBreakdown.filings = round2((scoreBreakdown.filings || 0) + 2) ?? 0
    }

    if (hasBreakout && hasForm4) {
      stackedScore += 2
      scoreBreakdown.confirmation = round2((scoreBreakdown.confirmation || 0) + 2) ?? 0
    }

    if (hasBreakout && (has13D || has13G)) {
      stackedScore += 2
      scoreBreakdown.confirmation = round2((scoreBreakdown.confirmation || 0) + 2) ?? 0
    }

    if (hasBreakout && ptr?.buyCluster) {
      stackedScore += 2
      scoreBreakdown.confirmation = round2((scoreBreakdown.confirmation || 0) + 2) ?? 0
    }

    if (maxSignalFamilyCount >= 3) {
      stackedScore += 6
      scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) + 6) ?? 0
    } else if (maxSignalFamilyCount >= 2) {
      stackedScore += 3
      scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) + 3) ?? 0
    } else {
      // Strong cluster buy (3+ insiders) exempts from single-family penalty
      if ((primary.cluster_buyers ?? 0) >= 3) {
        stackedScore += 2
        scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) + 2) ?? 0
        scoreCapsApplied.add("cluster-conviction-exemption")
      } else {
        stackedScore -= 5
        scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) - 5) ?? 0
        scoreCapsApplied.add("single-family-penalty")
      }
    }

    const positivePillars = countPositiveEvidencePillars(scoreBreakdown)

    if (positivePillars < 3) {
      stackedScore = Math.min(stackedScore, 78)
      scoreCapsApplied.add("limited-evidence-cap")
    }

    if (positivePillars < 4) {
      stackedScore = Math.min(stackedScore, 86)
      scoreCapsApplied.add("broad-confirmation-cap")
    }

    const hasStrongPtrOrOwnership =
      Boolean(ptr?.strongBuying) || has13D || has13G || hasForm4

    if ((primary.price_return_5d ?? 0) >= 12 && !hasStrongPtrOrOwnership) {
      stackedScore -= 4
      scoreBreakdown.chase_penalty = round2((scoreBreakdown.chase_penalty || 0) - 4) ?? 0
      scoreCapsApplied.add("sharp-move-penalty")
    }

    if ((primary.price_return_20d ?? 0) >= 20 && !ptr?.buyCluster && !has13D) {
      stackedScore -= 5
      scoreBreakdown.chase_penalty = round2((scoreBreakdown.chase_penalty || 0) - 5) ?? 0
      scoreCapsApplied.add("crowded-move-penalty")
    }

    if ((primary.volume_ratio ?? 0) >= 2.8 && (primary.price_return_5d ?? 0) >= 10 && !ptr?.strongBuying) {
      stackedScore -= 3
      scoreBreakdown.chase_penalty = round2((scoreBreakdown.chase_penalty || 0) - 3) ?? 0
      scoreCapsApplied.add("volume-spike-penalty")
    }

    if (sectorShare >= 0.24) {
      stackedScore -= 7
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 7) ?? 0
      scoreCapsApplied.add("sector-crowding-penalty")
    } else if (sectorShare >= 0.16) {
      stackedScore -= 4
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 4) ?? 0
      scoreCapsApplied.add("sector-crowding-warning")
    }

    if (industryShare >= 0.14) {
      stackedScore -= 5
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 5) ?? 0
      scoreCapsApplied.add("industry-crowding-penalty")
    } else if (industryShare >= 0.1) {
      stackedScore -= 2
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 2) ?? 0
      scoreCapsApplied.add("industry-crowding-warning")
    }

    if (primary.age_days !== null && primary.age_days !== undefined) {
      const ageDays = Number(primary.age_days)
      if (ageDays <= 2) {
        stackedScore += 4
        scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) + 4) ?? 0
      } else if (ageDays <= 5) {
        stackedScore += 2
        scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) + 2) ?? 0
      } else if (ageDays >= 28) {
        stackedScore -= 15
        scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) - 15) ?? 0
        scoreCapsApplied.add("stale-signal-heavy-decay")
      } else if (ageDays >= 21) {
        stackedScore -= 10
        scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) - 10) ?? 0
        scoreCapsApplied.add("stale-signal-decay")
      } else if (ageDays >= 14) {
        stackedScore -= 5
        scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) - 5) ?? 0
        scoreCapsApplied.add("aging-signal-decay")
      }
    }

    // Score momentum: reward tickers whose score is trending up
    // The primary row is `sorted[0]` which has ticker_score_change_7d
    const primaryScoreChange7d = primary.ticker_score_change_7d ?? null
    if (primaryScoreChange7d !== null) {
      if (primaryScoreChange7d >= 8) {
        stackedScore += 3
        scoreBreakdown.momentum = round2((scoreBreakdown.momentum || 0) + 3) ?? 0
      } else if (primaryScoreChange7d >= 4) {
        stackedScore += 1.5
        scoreBreakdown.momentum = round2((scoreBreakdown.momentum || 0) + 1.5) ?? 0
      } else if (primaryScoreChange7d <= -8) {
        stackedScore -= 2
        scoreBreakdown.momentum = round2((scoreBreakdown.momentum || 0) - 2) ?? 0
        scoreCapsApplied.add("score-declining")
      }
    }

    const ltcsBase = ltcsScoreMap.get(ticker) ?? 0

    // Insider catalyst bonus (max 30 points)
    let insiderBonus = 0
    const clusterBuyers = primary.cluster_buyers ?? 0
    const hasPtrBuy = (ptr?.buyTradeCount ?? 0) > 0

    if (clusterBuyers >= 3 && hasPtrBuy) {
      insiderBonus = 30  // platinum: cluster + congress
    } else if (clusterBuyers >= 2) {
      insiderBonus = 20  // cluster buying
    } else if (hasPtrBuy) {
      insiderBonus = 15  // congressional buy
    } else if (primary.insider_action === "buy" || ((primary as any).transaction_type || "").includes("buy")) {
      insiderBonus = 10  // solo insider buy
    }

    // Freshness bonus (max 5 points)
    const ageDays = primary.age_days ?? 999
    if (ageDays <= 2) insiderBonus += 5
    else if (ageDays <= 7) insiderBonus += 3

    let finalScore = clamp(Math.round(ltcsBase + insiderBonus), 0, 100)

    // Platinum conviction: 3+ cluster buyers + PTR activity + solid primary signal
    // This combination is the highest-confidence setup — override all caps with score 100
    if (
      (primary.cluster_buyers ?? 0) >= 3 &&
      (ptr?.buyTradeCount ?? 0) > 0 &&
      primaryScore >= 60
    ) {
      finalScore = 100
      scoreCapsApplied.add("platinum-conviction")
    }

    if (ltcsBase < 40 && insiderBonus === 0) continue
    if (finalScore < 40) continue

    const sourceList = Array.from(signalSources)
    const primaryTitle =
      maxSignalFamilyCount >= 3 || ptr?.buyTradeCount
        ? `Stacked conviction setup (${sorted.length} signals${ptr?.buyTradeCount ? ` + ${ptr.buyTradeCount} PTR buys` : ""})`
        : sorted.length >= 2
          ? `Multi-signal setup (${sorted.length} signals)`
          : primary.title

    const primarySummary =
      maxSignalFamilyCount >= 2 || ptr?.buyTradeCount
        ? `Evidence is stacking for this ticker across ${maxSignalFamilyCount} signal families. Sources: ${sourceList.join(", ")}${ptr?.summary ? `. ${ptr.summary}` : ""}. Primary setup: ${primary.title || "Constructive signal"}`
        : primary.summary

    rows.push({
      ticker,
      company_name: primary.company_name,
      business_description: primary.business_description,
      app_score: finalScore,
      raw_score: finalScore,
      bias: "Bullish",
      board_bucket:
        finalScore >= 88
          ? "High Conviction"
          : finalScore >= 76
            ? "Buy"
            : "Watch",
      signal_strength_bucket: getStrengthBucket(finalScore),
      score_version: SCORE_VERSION,
      score_updated_at: runTimestamp,
      stacked_signal_count: sorted.length,
      score_breakdown: {
        ...scoreBreakdown,
        sector_count: sectorCount,
        industry_count: industryCount,
        sector_share: round2(sectorShare),
        industry_share: round2(industryShare),
        max_signal_family_count: maxSignalFamilyCount,
      },
      signal_reasons: Array.from(signalReasons).slice(0, 20),
      score_caps_applied: Array.from(scoreCapsApplied),
      signal_tags: Array.from(signalTags),
      primary_signal_key: primary.signal_key ?? null,
      primary_signal_type:
        maxSignalFamilyCount >= 3 || ptr?.buyTradeCount
          ? "Priority Multi-Signal Buy"
          : sorted.length >= 2
            ? "Multi-Signal Buy"
            : primary.signal_type,
      primary_signal_source:
        ptr?.buyTradeCount
          ? "ptr+signals"
          : sorted.length >= 2
            ? "multi"
            : primary.signal_source,
      primary_signal_category:
        maxSignalFamilyCount >= 2 || ptr?.buyTradeCount
          ? "PTR / Filings / Signals Priority Buy"
          : primary.signal_category,
      primary_title: primaryTitle,
      primary_summary: primarySummary,
      filed_at: ptr?.latestTradeDate ?? primary.filed_at ?? null,
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

    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )
    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const ptrLookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("ptrLookbackDays"), DEFAULT_PTR_LOOKBACK_DAYS)),
      MAX_PTR_LOOKBACK_DAYS
    )
    const ptrRecentDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("ptrRecentDays"), DEFAULT_PTR_RECENT_DAYS)),
      MAX_PTR_RECENT_DAYS
    )
    const minCombinedScore = Math.max(
      0,
      parseInteger(searchParams.get("minCombinedScore"), MIN_COMBINED_SCORE)
    )
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)

    const ptrCutoff = new Date(now)
    ptrCutoff.setDate(ptrCutoff.getDate() - ptrLookbackDays)
    const ptrCutoffString = toIsoDateString(ptrCutoff)

    const [{ data: allSignalRows, error: allSignalsError }, { data: ptrRows, error: ptrError }] =
      await Promise.all([
        supabase
          .from("signals")
          .select("*")
          .gte("filed_at", cutoffDateString)
          .gte("app_score", MIN_SIGNAL_APP_SCORE)
          .order("app_score", { ascending: false })
          .order("filed_at", { ascending: false })
          .limit(limit),
        supabase
          .from("raw_ptr_trades")
          .select("ticker, filer_name, action, transaction_date, report_date, amount_low, amount_high, amount_range")
          .or(`transaction_date.gte.${ptrCutoffString},report_date.gte.${ptrCutoffString}`),
      ])

    if (allSignalsError) {
      return Response.json(
        {
          ok: false,
          error: allSignalsError.message,
        },
        { status: 500 }
      )
    }

    if (ptrError) {
      return Response.json(
        {
          ok: false,
          error: `raw_ptr_trades load failed: ${ptrError.message}`,
        },
        { status: 500 }
      )
    }

    // Load latest LTCS scores from candidate_screen_history
    const { data: ltcsRows } = await supabase
      .from("candidate_screen_history")
      .select("ticker, candidate_score, screened_on")
      .gte("candidate_score", 40)
      .order("screened_on", { ascending: false })
      .order("candidate_score", { ascending: false })
      .limit(5000)

    // Build map of ticker → best LTCS score (most recent screen date)
    const ltcsScoreMap = new Map<string, number>()
    for (const row of (ltcsRows || []) as any[]) {
      const t = (row.ticker || "").toUpperCase()
      if (t && !ltcsScoreMap.has(t)) {
        ltcsScoreMap.set(t, row.candidate_score ?? 0)
      }
    }

    const signalRows = (allSignalRows || []) as SignalRow[]
    const ptrSummaryMap = buildPtrSummaryMap((ptrRows || []) as RawPtrTradeRow[], ptrRecentDays)
    const breadthStats = buildBreadthStats(signalRows)

    const tickerCurrentRowsBase = buildTickerScoresCurrentRows(
      signalRows,
      runTimestamp,
      ptrSummaryMap,
      breadthStats,
      minCombinedScore,
      ltcsScoreMap
    )

    const tickerCurrentRows = await attachTickerScoreChangesToCurrentRows(
      supabase,
      tickerCurrentRowsBase,
      runDate
    )

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
            errorSamples: staleDeleteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const tickerHistoryRows = buildTickerScoreHistoryRows(tickerCurrentRows, runDate, runTimestamp)

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
            errorSamples: tickerHistoryWriteResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - RETENTION_DAYS)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: tickerRetentionError } = await supabase
        .from("ticker_score_history")
        .delete()
        .lt("score_date", retentionCutoffString)

      retentionMessage = tickerRetentionError ? tickerRetentionError.message : "ok"
    }

    let strongBuyCount: number | null = null
    let eliteBuyCount: number | null = null

    if (includeCounts) {
      const [strongBuyRes, eliteBuyRes] = await Promise.all([
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 84),
        supabase
          .from("ticker_scores_current")
          .select("*", { count: "exact", head: true })
          .gte("app_score", 94),
      ])

      strongBuyCount = strongBuyRes.error ? null : strongBuyRes.count ?? 0
      eliteBuyCount = eliteBuyRes.error ? null : eliteBuyRes.count ?? 0
    }

    return Response.json({
      ok: true,
      scannedSignals: signalRows.length,
      ptrRowsScanned: (ptrRows || []).length,
      ptrTickersMapped: ptrSummaryMap.size,
      tickerUniverseCount: breadthStats.totalTickers,
      tickerCurrentInserted: tickerCurrentWriteResult.insertedOrUpdated,
      tickerHistoryInserted: tickerHistoryWriteResult.insertedOrUpdated,
      lookbackDays,
      limit,
      ptrLookbackDays,
      ptrRecentDays,
      minCombinedScore,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      minTickerAppScore: MIN_TICKER_APP_SCORE,
      retentionCleanup: retentionMessage,
      strongBuyCount,
      eliteBuyCount,
      message: "Ticker scores rebuilt using stacked evidence, family diversity, PTR conviction, and crowding-aware penalties.",
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