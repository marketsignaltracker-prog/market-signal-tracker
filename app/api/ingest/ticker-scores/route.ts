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

type PtrAggregate = {
  ticker: string
  totalTrades: number
  buyCount: number
  sellCount: number
  buyFilers: string[]
  sellFilers: string[]
  latestTradeDate: string | null
  latestReportDate: string | null
  buyAmountLow: number
  buyAmountHigh: number
  sellAmountLow: number
  sellAmountHigh: number
  netAmountLow: number
  netAmountHigh: number | null
  buyCluster: boolean
  strongBuying: boolean
  strongSelling: boolean
  ptrBonus: number
  ptrPenalty: number
  reasons: string[]
  tags: string[]
}

const DEFAULT_LOOKBACK_DAYS = 14
const MAX_LOOKBACK_DAYS = 30
const DEFAULT_LIMIT = 1000
const MAX_LIMIT = 3000
const RETENTION_DAYS = 30
const SCORE_VERSION = "v8-combined-tight-ptr"
const MIN_SIGNAL_APP_SCORE = 75
const MIN_TICKER_APP_SCORE = 85
const DB_CHUNK_SIZE = 100

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

function dateDiffDays(fromDate: string, toDate: string) {
  const from = new Date(`${fromDate}T00:00:00.000Z`).getTime()
  const to = new Date(`${toDate}T00:00:00.000Z`).getTime()
  if (!Number.isFinite(from) || !Number.isFinite(to)) return null
  return Math.round((to - from) / (24 * 60 * 60 * 1000))
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
  if (score >= 92) return "Strong Buy"
  return "Buy"
}

function countPositiveEvidencePillars(breakdown: Record<string, number>) {
  return [
    (breakdown.base || 0) > 0,
    (breakdown.candidate_score || 0) > 0 || (breakdown.included || 0) > 0,
    (breakdown.breakout || 0) > 0 || (breakdown.trend || 0) > 0,
    (breakdown.volume || 0) > 0,
    (breakdown.relative_strength || 0) > 0,
    (breakdown.freshness || 0) > 0 || (breakdown.momentum || 0) > 0,
    (breakdown.ptr_buying || 0) > 0,
  ].filter(Boolean).length
}

function aggregatePtrRowsByTicker(ptrRows: any[], runDate: string) {
  const byTicker = new Map<string, any[]>()

  for (const row of ptrRows || []) {
    const ticker = normalizeTicker((row as any).ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const out = new Map<string, PtrAggregate>()

  for (const [ticker, rows] of byTicker.entries()) {
    let buyCount = 0
    let sellCount = 0
    let buyAmountLow = 0
    let buyAmountHigh = 0
    let sellAmountLow = 0
    let sellAmountHigh = 0
    let latestTradeDate: string | null = null
    let latestReportDate: string | null = null

    const buyFilers = new Set<string>()
    const sellFilers = new Set<string>()
    const reasons: string[] = []
    const tags: string[] = []

    for (const row of rows) {
      const action = String((row as any).action || "").trim().toLowerCase()
      const filerName = String((row as any).filer_name || "").trim()
      const tradeDate = String((row as any).transaction_date || "")
      const reportDate = String((row as any).report_date || "")
      const amountLow = Number((row as any).amount_low || 0)
      const amountHighRaw = (row as any).amount_high
      const amountHigh =
        amountHighRaw === null || amountHighRaw === undefined
          ? amountLow
          : Number(amountHighRaw || 0)

      if (tradeDate && (!latestTradeDate || tradeDate > latestTradeDate)) {
        latestTradeDate = tradeDate
      }

      if (reportDate && (!latestReportDate || reportDate > latestReportDate)) {
        latestReportDate = reportDate
      }

      if (action.includes("buy") || action.includes("purchase")) {
        buyCount += 1
        buyAmountLow += amountLow
        buyAmountHigh += amountHigh
        if (filerName) buyFilers.add(filerName)
      } else if (action.includes("sell") || action.includes("sale")) {
        sellCount += 1
        sellAmountLow += amountLow
        sellAmountHigh += amountHigh
        if (filerName) sellFilers.add(filerName)
      }
    }

    let ptrBonus = 0
    let ptrPenalty = 0

    const buyFilerCount = buyFilers.size
    const sellFilerCount = sellFilers.size
    const latestRelevantDate = latestTradeDate || latestReportDate
    const ageDays = latestRelevantDate ? dateDiffDays(latestRelevantDate, runDate) : null

    if (buyCount >= 1) ptrBonus += 1
    if (buyCount >= 2) ptrBonus += 2
    if (buyCount >= 3) ptrBonus += 2

    if (buyFilerCount >= 2) ptrBonus += 2
    if (buyFilerCount >= 3) ptrBonus += 2

    if (buyAmountLow >= 15_001) ptrBonus += 1
    if (buyAmountLow >= 50_001) ptrBonus += 1
    if (buyAmountLow >= 100_001) ptrBonus += 2
    if (buyAmountLow >= 250_001) ptrBonus += 1

    if (sellCount >= 2) ptrPenalty -= 1
    if (sellFilerCount >= 2) ptrPenalty -= 1
    if (sellAmountLow >= 100_001) ptrPenalty -= 1
    if (sellAmountLow >= 250_001) ptrPenalty -= 2

    if (ageDays !== null) {
      if (ageDays <= 3) ptrBonus += 2
      else if (ageDays <= 7) ptrBonus += 1
      else if (ageDays > 21) ptrPenalty -= 1
    }

    const netAmountLow = buyAmountLow - sellAmountLow
    const netAmountHigh =
      buyAmountHigh > 0 || sellAmountHigh > 0 ? buyAmountHigh - sellAmountHigh : null

    const buyCluster = buyCount >= 2 && buyFilerCount >= 2 && netAmountLow > 0
    const strongBuying = buyCount >= 2 && netAmountLow >= 50_001
    const strongSelling = sellCount >= 2 && sellAmountLow >= 100_001

    if (buyCount > 0) {
      reasons.push(`Recent PTR buying (${buyCount} trade${buyCount === 1 ? "" : "s"})`)
      tags.push("ptr-buying")
    }

    if (buyCluster) {
      reasons.push(`PTR buy cluster (${buyFilerCount} filers)`)
      tags.push("ptr-cluster-buy")
    }

    if (netAmountLow >= 100_001) {
      reasons.push("Meaningful net politician buying")
      tags.push("ptr-net-buying")
    }

    if (strongSelling) {
      reasons.push("Recent politician selling pressure")
      tags.push("ptr-selling")
    }

    out.set(ticker, {
      ticker,
      totalTrades: rows.length,
      buyCount,
      sellCount,
      buyFilers: Array.from(buyFilers),
      sellFilers: Array.from(sellFilers),
      latestTradeDate,
      latestReportDate,
      buyAmountLow,
      buyAmountHigh,
      sellAmountLow,
      sellAmountHigh,
      netAmountLow,
      netAmountHigh,
      buyCluster,
      strongBuying,
      strongSelling,
      ptrBonus,
      ptrPenalty,
      reasons,
      tags,
    })
  }

  return out
}

function buildTickerScoresCurrentRows(
  signalRows: any[],
  universeRows: any[],
  ptrByTicker: Map<string, PtrAggregate>,
  runDate: string,
  runTimestamp: string
) {
  const signalsByTicker = new Map<string, any[]>()
  const universeByTicker = new Map<string, any>()

  for (const row of universeRows || []) {
    const ticker = normalizeTicker((row as any).ticker)
    if (!ticker) continue
    universeByTicker.set(ticker, row)
  }

  for (const row of signalRows || []) {
    const ticker = normalizeTicker((row as any).ticker)
    if (!ticker) continue
    if (!signalsByTicker.has(ticker)) signalsByTicker.set(ticker, [])
    signalsByTicker.get(ticker)!.push(row)
  }

  const scoringTickers = uniqueStrings([
    ...Array.from(universeByTicker.keys()),
    ...Array.from(signalsByTicker.keys()),
    ...Array.from(ptrByTicker.keys()),
  ])

  const rows: any[] = []

  for (const ticker of scoringTickers) {
    const universe = universeByTicker.get(ticker) || {}
    const signalSet = signalsByTicker.get(ticker) || []
    const ptr = ptrByTicker.get(ticker) || null

    if (!signalSet.length && !ptr) continue

    const sortedSignals = [...signalSet].sort((a, b) => {
      const scoreDiff = Number(b.app_score ?? 0) - Number(a.app_score ?? 0)
      if (scoreDiff !== 0) return scoreDiff

      const filedAtDiff =
        new Date(b.filed_at || 0).getTime() - new Date(a.filed_at || 0).getTime()
      if (filedAtDiff !== 0) return filedAtDiff

      return String(a.signal_key || "").localeCompare(String(b.signal_key || ""))
    })

    const primarySignal = sortedSignals[0] || null

    const baseFromCandidate = Number((universe as any).candidate_score || 0)
    const primaryScore = primarySignal
      ? Number(primarySignal.app_score || 0)
      : Math.max(baseFromCandidate, 0)

    const scoreBreakdown: Record<string, number> = {}
    const signalReasons = new Set<string>()
    const scoreCapsApplied = new Set<string>()
    const signalTags = new Set<string>()
    const accessionNos: string[] = []
    const signalKeys: string[] = []
    const sourceForms: string[] = []
    const signalSources = new Set<string>()
    const signalCategories = new Set<string>()

    if (!primarySignal) {
      scoreBreakdown.base = 8
      scoreBreakdown.candidate_score = round2(baseFromCandidate / 10) ?? 0

      if ((universe as any).included === true) {
        scoreBreakdown.included = 4
        signalReasons.add("Included in latest candidate universe")
        signalTags.add("candidate-included")
      } else if ((universe as any).is_eligible === true) {
        scoreBreakdown.included = 2
        signalReasons.add("Eligible in latest candidate universe")
        signalTags.add("candidate-eligible")
      }

      if ((universe as any).breakout_20d === true) scoreBreakdown.breakout = 4
      if ((universe as any).breakout_10d === true) scoreBreakdown.breakout = (scoreBreakdown.breakout || 0) + 1
      if ((universe as any).above_sma_20 === true) scoreBreakdown.trend = 3
      if (Number((universe as any).relative_strength_20d || 0) > 0) {
        scoreBreakdown.relative_strength = clamp(
          Number((universe as any).relative_strength_20d || 0) / 2,
          0,
          6
        )
      }
      if (Number((universe as any).volume_ratio || 0) >= 1.2) {
        scoreBreakdown.volume = clamp(
          Number((universe as any).volume_ratio || 0) * 2,
          0,
          6
        )
      }

      signalSources.add("candidate")
      signalCategories.add("Candidate Universe")
      signalReasons.add("No strong filing signal yet; scoring from universe setup")
      signalTags.add("candidate-only")
    }

    for (const row of sortedSignals) {
      signalKeys.push((row as any).signal_key)
      if ((row as any).accession_no) accessionNos.push((row as any).accession_no)
      if ((row as any).source_form) sourceForms.push((row as any).source_form)
      if ((row as any).signal_source) signalSources.add((row as any).signal_source)
      if ((row as any).signal_category) signalCategories.add((row as any).signal_category)

      for (const tag of (row as any).signal_tags || []) signalTags.add(tag)
      for (const reason of (row as any).signal_reasons || []) signalReasons.add(reason)
      for (const cap of (row as any).score_caps_applied || []) scoreCapsApplied.add(cap)

      const breakdown = ((row as any).score_breakdown || {}) as Record<string, number>
      for (const [key, value] of Object.entries(breakdown)) {
        scoreBreakdown[key] = round2((scoreBreakdown[key] || 0) + Number(value || 0)) ?? 0
      }
    }

    if (ptr) {
      signalSources.add("ptr")
      signalCategories.add("Congressional Trades")
      sourceForms.push("PTR")

      const ptrContribution = Math.max(ptr.ptrBonus + ptr.ptrPenalty, 0)
      const ptrPenaltyAbs = Math.abs(Math.min(ptr.ptrBonus + ptr.ptrPenalty, 0))

      if (ptrContribution > 0) {
        scoreBreakdown.ptr_buying = round2(ptrContribution) ?? 0
      }
      if (ptrPenaltyAbs > 0) {
        scoreBreakdown.ptr_penalty = round2(-ptrPenaltyAbs) ?? 0
      }

      for (const reason of ptr.reasons) signalReasons.add(reason)
      for (const tag of ptr.tags) signalTags.add(tag)

      if (ptr.latestTradeDate && dateDiffDays(ptr.latestTradeDate, runDate) !== null) {
        const ageDays = dateDiffDays(ptr.latestTradeDate, runDate) || 0
        if (ageDays <= 7) {
          scoreBreakdown.freshness = round2((scoreBreakdown.freshness || 0) + 1.5) ?? 0
        }
      }
    }

    let stackedScore = primaryScore

    if (sortedSignals.length >= 2) stackedScore += 2
    if (sortedSignals.length >= 3) stackedScore += 2
    if (sortedSignals.length >= 4) stackedScore += 1
    if (sortedSignals.length >= 5) stackedScore += 1

    if (ptr) {
      stackedScore += ptr.ptrBonus
      stackedScore += ptr.ptrPenalty

      if (ptr.buyCluster && signalSources.has("breakout")) {
        stackedScore += 2
        scoreCapsApplied.add("ptr-plus-breakout-bonus")
      }

      if (ptr.strongBuying && signalSources.has("form4")) {
        stackedScore += 2
        scoreCapsApplied.add("ptr-plus-insider-bonus")
      }

      if (ptr.strongSelling) {
        stackedScore -= 1
        scoreCapsApplied.add("ptr-selling-headwind")
      }
    }

    if (signalSources.has("breakout") && signalSources.has("form4")) {
      stackedScore += 2
      scoreCapsApplied.add("technical-plus-insider-bonus")
    }

    if (signalSources.has("breakout") && (signalSources.has("13d") || signalSources.has("13g"))) {
      stackedScore += 2
      scoreCapsApplied.add("technical-plus-ownership-bonus")
    }

    if (signalSources.has("breakout") && signalSources.has("8k")) {
      stackedScore += 1
      scoreCapsApplied.add("technical-plus-catalyst-bonus")
    }

    const positivePillars = countPositiveEvidencePillars(scoreBreakdown)

    if (positivePillars < 3) {
      stackedScore = Math.min(stackedScore, 88)
      scoreCapsApplied.add("stacked-limited-evidence-cap")
    }

    if (positivePillars < 4) {
      stackedScore = Math.min(stackedScore, 92)
      scoreCapsApplied.add("stacked-broad-confirmation-cap")
    }

    const referencePrimary = primarySignal || universe || {}
    const hasBreakoutSupport =
      (scoreBreakdown.breakout || 0) > 0 || (referencePrimary as any).breakout_20d === true

    const hasHeavyVolume =
      Number((referencePrimary as any).volume_ratio ?? 0) >= 2 ||
      (scoreBreakdown.volume || 0) >= 5

    const hasFilingConfirmation =
      signalSources.has("form4") ||
      signalSources.has("13d") ||
      signalSources.has("13g") ||
      signalSources.has("8k") ||
      signalSources.has("earnings")

    const hasPtrConfirmation = !!ptr && ptr.buyCount >= 1
    const hasMultipleSignalTypes = signalSources.size >= 2
    const hasThreeSignals = sortedSignals.length >= 3 || (!!ptr && sortedSignals.length >= 2)
    const hasFourSignals = sortedSignals.length >= 4 || (!!ptr && sortedSignals.length >= 3)

    if (!hasMultipleSignalTypes) {
      stackedScore = Math.min(stackedScore, 93)
      scoreCapsApplied.add("multi-source-required-cap")
    }

    if (!(hasBreakoutSupport && hasHeavyVolume && positivePillars >= 4)) {
      stackedScore = Math.min(stackedScore, 96)
      scoreCapsApplied.add("stacked-elite-confirmation-cap")
    }

    if (!(hasThreeSignals && hasMultipleSignalTypes && (hasFilingConfirmation || hasPtrConfirmation))) {
      stackedScore = Math.min(stackedScore, 98)
      scoreCapsApplied.add("stacked-top-tier-confirmation-cap")
    }

    if (
      !(
        hasFourSignals &&
        hasBreakoutSupport &&
        hasHeavyVolume &&
        positivePillars >= 5 &&
        (hasFilingConfirmation || hasPtrConfirmation) &&
        primaryScore >= 95
      )
    ) {
      stackedScore = Math.min(stackedScore, 99)
      scoreCapsApplied.add("stacked-no-perfect-score-cap")
    }

    if (!primarySignal && ptr && ptr.buyCluster && baseFromCandidate >= 88) {
      stackedScore = Math.max(stackedScore, 89)
      scoreCapsApplied.add("ptr-supported-candidate-floor")
    }

    if (!primarySignal && (!ptr || ptr.buyCount === 0)) {
      stackedScore = Math.min(stackedScore, 90)
      scoreCapsApplied.add("candidate-only-cap")
    }

    const finalScore = clamp(Math.round(stackedScore), 0, 100)

    if (finalScore < MIN_TICKER_APP_SCORE) continue

    const perfectTickerSetup =
      (sortedSignals.length >= 4 || (!!ptr && sortedSignals.length >= 3)) &&
      signalSources.size >= 3 &&
      hasBreakoutSupport &&
      hasHeavyVolume &&
      positivePillars >= 5 &&
      (hasFilingConfirmation || hasPtrConfirmation) &&
      primaryScore >= 97 &&
      Number((referencePrimary as any).relative_strength_20d ?? 0) >= 8 &&
      Number((referencePrimary as any).price_return_20d ?? 0) >= 12 &&
      Number((referencePrimary as any).volume_ratio ?? 0) >= 2.2

    const finalTickerScore = perfectTickerSetup ? 100 : finalScore

    const primaryTitle = primarySignal
      ? sortedSignals.length >= 2 || ptr
        ? `Multi-signal institutional setup (${sortedSignals.length + (ptr ? 1 : 0)} supports)`
        : (primarySignal as any).title
      : ptr
        ? `PTR-supported candidate setup`
        : `Candidate setup`

    const primarySummary = primarySignal
      ? sortedSignals.length >= 2 || ptr
        ? `Multiple signal sources are lining up for this ticker: ${Array.from(signalSources).join(", ")}. Primary setup: ${(primarySignal as any).title}`
        : (primarySignal as any).summary
      : ptr
        ? `No strong filing signal yet, but recent politician trading activity is reinforcing the candidate setup.`
        : `No strong filing signal yet; this ticker is being scored from the latest candidate universe setup.`

    rows.push({
      ticker,
      company_name:
        (referencePrimary as any).company_name ??
        (universe as any).name ??
        null,
      business_description:
        (referencePrimary as any).business_description ??
        (universe as any).business_description ??
        null,
      app_score: finalTickerScore,
      raw_score: finalTickerScore,
      bias: "Bullish",
      board_bucket: "Buy",
      signal_strength_bucket: getStrengthBucket(finalTickerScore),
      score_version: SCORE_VERSION,
      score_updated_at: runTimestamp,
      stacked_signal_count: sortedSignals.length + (ptr ? 1 : 0),
      score_breakdown: scoreBreakdown,
      signal_reasons: Array.from(signalReasons).slice(0, 12),
      score_caps_applied: Array.from(scoreCapsApplied),
      signal_tags: Array.from(signalTags),
      primary_signal_key: primarySignal ? (primarySignal as any).signal_key : `ptr:${ticker}:${runDate}`,
      primary_signal_type: primarySignal
        ? sortedSignals.length >= 2 || ptr
          ? "Multi-Signal Strong Buy"
          : (primarySignal as any).signal_type
        : ptr
          ? "PTR-Supported Candidate"
          : "Candidate Universe",
      primary_signal_source: primarySignal
        ? sortedSignals.length >= 2 || ptr
          ? "multi"
          : (primarySignal as any).signal_source
        : ptr
          ? "ptr"
          : "candidate",
      primary_signal_category: primarySignal
        ? sortedSignals.length >= 2 || ptr
          ? "Multi-Signal Strong Buy"
          : (primarySignal as any).signal_category
        : ptr
          ? "Congressional Trades"
          : "Candidate Universe",
      primary_title: primaryTitle,
      primary_summary: primarySummary,
      filed_at:
        (primarySignal as any)?.filed_at ??
        ptr?.latestTradeDate ??
        ptr?.latestReportDate ??
        runDate,
      signal_keys,
      accession_nos: accessionNos,
      source_forms: uniqueStrings(sourceForms),
      pe_ratio: (referencePrimary as any).pe_ratio ?? (universe as any).pe_ratio ?? null,
      pe_forward: (referencePrimary as any).pe_forward ?? (universe as any).pe_forward ?? null,
      pe_type: (referencePrimary as any).pe_type ?? (universe as any).pe_type ?? null,
      market_cap: (referencePrimary as any).market_cap ?? (universe as any).market_cap ?? null,
      sector: (referencePrimary as any).sector ?? (universe as any).sector ?? null,
      industry: (referencePrimary as any).industry ?? (universe as any).industry ?? null,
      insider_action: (referencePrimary as any).insider_action ?? null,
      insider_shares: (referencePrimary as any).insider_shares ?? null,
      insider_avg_price: (referencePrimary as any).insider_avg_price ?? null,
      insider_buy_value: (referencePrimary as any).insider_buy_value ?? null,
      cluster_buyers: (referencePrimary as any).cluster_buyers ?? null,
      cluster_shares: (referencePrimary as any).cluster_shares ?? null,
      price_return_5d:
        (referencePrimary as any).price_return_5d ?? (universe as any).return_5d ?? null,
      price_return_20d:
        (referencePrimary as any).price_return_20d ?? (universe as any).return_20d ?? null,
      volume_ratio:
        (referencePrimary as any).volume_ratio ?? (universe as any).volume_ratio ?? null,
      breakout_20d:
        (referencePrimary as any).breakout_20d ?? (universe as any).breakout_20d ?? false,
      breakout_52w: (referencePrimary as any).breakout_52w ?? false,
      above_50dma: (referencePrimary as any).above_50dma ?? null,
      trend_aligned:
        (referencePrimary as any).trend_aligned ??
        (universe as any).above_sma_20 ??
        null,
      price_confirmed: (referencePrimary as any).price_confirmed ?? null,
      relative_strength_20d:
        (referencePrimary as any).relative_strength_20d ??
        (universe as any).relative_strength_20d ??
        null,
      earnings_surprise_pct: (referencePrimary as any).earnings_surprise_pct ?? null,
      revenue_growth_pct: (referencePrimary as any).revenue_growth_pct ?? null,
      guidance_flag: (referencePrimary as any).guidance_flag ?? null,
      age_days: (referencePrimary as any).age_days ?? null,
      freshness_bucket: (referencePrimary as any).freshness_bucket ?? null,
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
    const includeCounts = (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention = (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)

    const { data: includedUniverseRows, error: includedUniverseError } = await supabase
      .from("candidate_universe")
      .select("*")
      .or("included.eq.true,is_eligible.eq.true")

    if (includedUniverseError) {
      return Response.json(
        {
          ok: false,
          error: includedUniverseError.message,
        },
        { status: 500 }
      )
    }

    const universeRows = (includedUniverseRows || []) as any[]
    const universeTickers = uniqueStrings(
      universeRows.map((row: any) => normalizeTicker(row.ticker))
    )

    if (!universeTickers.length) {
      return Response.json({
        ok: true,
        scannedSignals: 0,
        scannedPtrTrades: 0,
        universeTickerCount: 0,
        ptrTickerCount: 0,
        tickerCurrentInserted: 0,
        tickerHistoryInserted: 0,
        lookbackDays,
        limit,
        retainedDays: RETENTION_DAYS,
        scoreVersion: SCORE_VERSION,
        minSignalAppScore: MIN_SIGNAL_APP_SCORE,
        minTickerAppScore: MIN_TICKER_APP_SCORE,
        retentionCleanup: "skipped",
        strongBuyCount: 0,
        eliteBuyCount: 0,
        message:
          "No included or eligible tickers exist in candidate_universe yet, so ticker scores were skipped.",
      })
    }

    const { data: allSignalRows, error: allSignalsError } = await supabase
      .from("signals")
      .select("*")
      .in("ticker", universeTickers)
      .gte("filed_at", cutoffDateString)
      .gte("app_score", MIN_SIGNAL_APP_SCORE)
      .order("app_score", { ascending: false })
      .order("filed_at", { ascending: false })
      .limit(limit)

    if (allSignalsError) {
      return Response.json(
        {
          ok: false,
          error: allSignalsError.message,
        },
        { status: 500 }
      )
    }

    const { data: allPtrRows, error: allPtrError } = await supabase
      .from("raw_ptr_trades")
      .select("*")
      .in("ticker", universeTickers)
      .or(
        `transaction_date.gte.${cutoffDateString},report_date.gte.${cutoffDateString}`
      )
      .order("transaction_date", { ascending: false })
      .order("report_date", { ascending: false })
      .limit(limit)

    if (allPtrError) {
      return Response.json(
        {
          ok: false,
          error: allPtrError.message,
        },
        { status: 500 }
      )
    }

    const signalRows = (allSignalRows || []) as any[]
    const ptrRows = (allPtrRows || []) as any[]
    const ptrByTicker = aggregatePtrRowsByTicker(ptrRows, runDate)

    const tickerCurrentRowsBase = buildTickerScoresCurrentRows(
      signalRows,
      universeRows,
      ptrByTicker,
      runDate,
      runTimestamp
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

    const currentTickerSet = new Set(
      tickerCurrentRows.map((row) => normalizeTicker(row.ticker))
    )

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
        ? await deleteInChunksByTickerDetailed(
            supabase.from("ticker_scores_current"),
            staleTickerList
          )
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

    const tickerHistoryRows = buildTickerScoreHistoryRows(
      tickerCurrentRows,
      runDate,
      runTimestamp
    )

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
          .gte("app_score", 92),
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
      scannedSignals: signalRows.length,
      scannedPtrTrades: ptrRows.length,
      universeTickerCount: universeTickers.length,
      ptrTickerCount: ptrByTicker.size,
      tickerCurrentInserted: tickerCurrentWriteResult.insertedOrUpdated,
      tickerHistoryInserted: tickerHistoryWriteResult.insertedOrUpdated,
      lookbackDays,
      limit,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: MIN_SIGNAL_APP_SCORE,
      minTickerAppScore: MIN_TICKER_APP_SCORE,
      retentionCleanup: retentionMessage,
      strongBuyCount,
      eliteBuyCount,
      message:
        "Ticker scores rebuilt from candidate universe, filing signals, and recent PTR activity successfully.",
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