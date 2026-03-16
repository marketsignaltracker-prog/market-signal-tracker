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

type RawFilingRow = {
  company_id?: number | null
  ticker?: string | null
  company_name?: string | null
  filed_at?: string | null
  form_type?: string | null
  filing_url?: string | null
  accession_no: string
  cik?: string | null
  primary_doc?: string | null
  fetched_at?: string | null
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

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price?: number | null
  market_cap?: number | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  sector?: string | null
  industry?: string | null
  business_description?: string | null
  avg_volume_20d?: number | null
  avg_dollar_volume_20d?: number | null
  one_day_return?: number | null
  return_5d?: number | null
  return_10d?: number | null
  return_20d?: number | null
  relative_strength_20d?: number | null
  volume_ratio?: number | null
  breakout_20d?: boolean | null
  breakout_10d?: boolean | null
  above_sma_20?: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  passes_price?: boolean | null
  passes_volume?: boolean | null
  passes_dollar_volume?: boolean | null
  passes_market_cap?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  screen_reason?: string | null
  last_screened_at?: string | null
  updated_at?: string | null
}

type CandidateHistoryRow = {
  id?: number | null
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price?: number | null
  market_cap?: number | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  sector?: string | null
  industry?: string | null
  business_description?: string | null
  avg_volume_20d?: number | null
  avg_dollar_volume_20d?: number | null
  one_day_return?: number | null
  return_5d?: number | null
  return_10d?: number | null
  return_20d?: number | null
  relative_strength_20d?: number | null
  volume_ratio?: number | null
  breakout_20d?: boolean | null
  breakout_10d?: boolean | null
  above_sma_20?: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  passes_price?: boolean | null
  passes_volume?: boolean | null
  passes_dollar_volume?: boolean | null
  passes_market_cap?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  screen_reason?: string | null
  last_screened_at?: string | null
  updated_at?: string | null
  screened_on?: string | null
}

type ContextRow = CandidateUniverseRow | CandidateHistoryRow

type PtrSummary = {
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
  summary: string | null
}

type FilingSummary = {
  insiderFormCount: number
  ownershipFormCount: number
  catalystFormCount: number
  latestFiledAt: string | null
  forms: string[]
  hasForm4: boolean
  has13DOr13G: boolean
  hasCatalystForm: boolean
}

type Diagnostics = {
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  candidateRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
  rawFilingsLoaded: number
  rawPtrTradesLoaded: number
  tickersWithFilings: number
  tickersWithPtrSupport: number
  candidateSignalsBuilt: number
  candidateSignalsInserted: number
  signalHistoryInserted: number
  filteredBelowSignalScore: number
}

const DEFAULT_LIMIT = 250
const MAX_LIMIT = 1000
const DEFAULT_LOOKBACK_DAYS = 31
const MAX_LOOKBACK_DAYS = 60
const RETENTION_DAYS = 30
const SCORE_VERSION = "v9-priority-signals"
const DB_CHUNK_SIZE = 100

const DEFAULT_MIN_SIGNAL_APP_SCORE = 55
const MIN_CANDIDATE_SCORE = 56
const PTR_LOOKBACK_DAYS = 60
const PTR_RECENT_DAYS = 14

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

function daysAgo(isoDate: string | null | undefined) {
  if (!isoDate) return null
  const ts = new Date(isoDate).getTime()
  if (Number.isNaN(ts)) return null
  return Math.max(0, Math.floor((Date.now() - ts) / (24 * 60 * 60 * 1000)))
}

function normalizeFormType(formType: string | null | undefined) {
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

function getStrengthBucket(score: number): "Buy" | "Strong Buy" | "Elite Buy" {
  if (score >= 97) return "Elite Buy"
  if (score >= 90) return "Strong Buy"
  return "Buy"
}

function getContextCompanyId(context: ContextRow): number | null {
  if ("company_id" in context && context.company_id !== null && context.company_id !== undefined) {
    const value = Number(context.company_id)
    return Number.isFinite(value) ? value : null
  }

  if ("id" in context && context.id !== null && context.id !== undefined) {
    const value = Number(context.id)
    return Number.isFinite(value) ? value : null
  }

  return null
}

function buildSignalKey(ticker: string, runDate: string) {
  return `priority:${runDate}:${normalizeTicker(ticker)}`
}

function buildHistoryKey(runDate: string, signalKey: string) {
  return `${runDate}_${signalKey}`
}

function buildPtrSummaryMap(rows: RawPtrTradeRow[]) {
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
      return age !== null && age <= PTR_RECENT_DAYS
    }).length

    const recentSellCount = sells.filter((row) => {
      const age = daysAgo(row.transaction_date || row.report_date)
      return age !== null && age <= PTR_RECENT_DAYS
    }).length

    const totalBuyAmountLow = buys.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)
    const totalSellAmountLow = sells.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)

    const allDates = trades
      .map((row) => String(row.transaction_date || row.report_date || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))

    const buyCluster = uniqueBuyFilers >= 2 || buys.length >= 3
    const strongBuying =
      recentBuyCount >= 1 &&
      (uniqueBuyFilers >= 2 || totalBuyAmountLow >= 250_001 || buys.length >= 2)

    const strongSelling =
      sells.length >= 2 &&
      (recentSellCount >= 1 || totalSellAmountLow >= 250_001)

    const summaryParts: string[] = []
    if (buys.length > 0) summaryParts.push(`${buys.length} buy${buys.length === 1 ? "" : "s"}`)
    if (uniqueBuyFilers > 0) summaryParts.push(`${uniqueBuyFilers} buyer${uniqueBuyFilers === 1 ? "" : "s"}`)
    if (recentBuyCount > 0) summaryParts.push(`${recentBuyCount} recent`)
    if (totalBuyAmountLow > 0) summaryParts.push(`min disclosed $${totalBuyAmountLow.toLocaleString()}`)

    output.set(ticker, {
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
      latestTradeDate: allDates[0] ?? null,
      summary: summaryParts.length ? `PTR support: ${summaryParts.join(", ")}` : null,
    })
  }

  return output
}

function buildFilingSummaryMap(rows: RawFilingRow[]) {
  const byTicker = new Map<string, RawFilingRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    if (!byTicker.has(ticker)) byTicker.set(ticker, [])
    byTicker.get(ticker)!.push(row)
  }

  const output = new Map<string, FilingSummary>()

  for (const [ticker, filings] of byTicker.entries()) {
    let insiderFormCount = 0
    let ownershipFormCount = 0
    let catalystFormCount = 0
    const forms: string[] = []

    for (const filing of filings) {
      const form = normalizeFormType(filing.form_type)
      if (!form) continue
      forms.push(form)

      if (
        form === "3" ||
        form === "4" ||
        form === "4/A" ||
        form === "5" ||
        form === "3/A" ||
        form === "5/A"
      ) {
        insiderFormCount += 1
      }

      if (
        form === "13D" ||
        form === "13D/A" ||
        form === "13G" ||
        form === "13G/A" ||
        form === "SC 13D" ||
        form === "SC 13D/A" ||
        form === "SC 13G" ||
        form === "SC 13G/A"
      ) {
        ownershipFormCount += 1
      }

      if (form === "8-K" || form === "6-K" || form === "10-Q" || form === "10-K") {
        catalystFormCount += 1
      }
    }

    const latestFiledAt = filings
      .map((row) => String(row.filed_at || "").trim())
      .filter(Boolean)
      .sort((a, b) => b.localeCompare(a))[0] ?? null

    output.set(ticker, {
      insiderFormCount,
      ownershipFormCount,
      catalystFormCount,
      latestFiledAt,
      forms: uniqueStrings(forms),
      hasForm4: insiderFormCount > 0,
      has13DOr13G: ownershipFormCount > 0,
      hasCatalystForm: catalystFormCount > 0,
    })
  }

  return output
}

function scoreCandidateSignal(params: {
  context: ContextRow
  filingSummary: FilingSummary | null
  ptrSummary: PtrSummary | null
}) {
  const { context, filingSummary, ptrSummary } = params

  const candidateScore = Number(context.candidate_score || 0)

  const breakdown: Record<string, number> = {}
  const reasons: string[] = []
  const caps: string[] = []

  const add = (key: string, value: number, reason?: string | null) => {
    if (!Number.isFinite(value) || value === 0) return
    breakdown[key] = round2((breakdown[key] || 0) + value) ?? value
    if (reason) reasons.push(reason)
  }

  add("base", 34, "Base priority signal")

  if (context.included) add("included", 8, "Already made final candidate set")

  if (candidateScore >= 90) add("candidate_score", 12, "Strong candidate score")
  else if (candidateScore >= 80) add("candidate_score", 9, "Good candidate score")
  else if (candidateScore >= 70) add("candidate_score", 6, "Constructive candidate score")
  else if (candidateScore >= 60) add("candidate_score", 3, "Passing candidate score")

  if (ptrSummary) {
    if (ptrSummary.buyTradeCount >= 1) add("ptr", 8, "At least one PTR buy")
    if (ptrSummary.buyTradeCount >= 2) add("ptr", 4, "Multiple PTR buys")
    if (ptrSummary.buyTradeCount >= 3) add("ptr", 3, "Three or more PTR buys")

    if (ptrSummary.uniqueBuyFilers >= 2) add("ptr", 4, "Multiple PTR buyers")
    if (ptrSummary.uniqueBuyFilers >= 3) add("ptr", 3, "Broad PTR participation")

    if (ptrSummary.recentBuyCount >= 1) add("ptr", 4, "Recent PTR buy")
    if (ptrSummary.recentBuyCount >= 2) add("ptr", 3, "Multiple recent PTR buys")

    if (ptrSummary.totalBuyAmountLow >= 100_001) add("ptr", 3, "Meaningful PTR size")
    if (ptrSummary.totalBuyAmountLow >= 250_001) add("ptr", 4, "Large PTR size")
    if (ptrSummary.totalBuyAmountLow >= 500_001) add("ptr", 4, "Very large PTR size")

    if (ptrSummary.buyCluster) add("ptr", 4, "PTR buy cluster")
    if (ptrSummary.strongBuying) add("ptr", 5, "Strong PTR buying support")
    if (ptrSummary.strongSelling) add("ptr_penalty", -4, "PTR selling headwind")
  }

  if (filingSummary) {
    if (filingSummary.hasForm4) add("filings", 9, "Insider filing support")
    if (filingSummary.insiderFormCount >= 2) add("filings", 3, "Multiple insider filings")

    if (filingSummary.has13DOr13G) add("filings", 8, "Ownership filing support")
    if (filingSummary.ownershipFormCount >= 2) add("filings", 3, "Multiple ownership filings")

    if (filingSummary.hasCatalystForm) add("filings", 5, "Corporate catalyst filing support")
    if (filingSummary.catalystFormCount >= 2) add("filings", 2, "Multiple catalyst filings")

    const filingAge = daysAgo(filingSummary.latestFiledAt)
    if (filingAge !== null) {
      if (filingAge <= 1) add("freshness", 5, "Fresh filing activity")
      else if (filingAge <= 3) add("freshness", 4, "Recent filing activity")
      else if (filingAge <= 7) add("freshness", 2, "Active filing window")
      else if (filingAge > 21) add("freshness_penalty", -2, "Older filing activity")
    }
  }

  if ((context.return_20d ?? 0) >= 12) add("momentum", 5, "Strong 20-day momentum")
  else if ((context.return_20d ?? 0) >= 6) add("momentum", 3, "Positive 20-day momentum")
  else if ((context.return_20d ?? 0) >= 2) add("momentum", 1, "Constructive 20-day momentum")

  if ((context.return_10d ?? 0) >= 5) add("short_momentum", 3, "Strong 10-day momentum")
  else if ((context.return_10d ?? 0) >= 2) add("short_momentum", 2, "Positive 10-day momentum")

  if ((context.relative_strength_20d ?? 0) >= 6) add("relative_strength", 6, "Strong relative strength")
  else if ((context.relative_strength_20d ?? 0) >= 3) add("relative_strength", 4, "Positive relative strength")
  else if ((context.relative_strength_20d ?? 0) >= 1) add("relative_strength", 2, "Constructive relative strength")

  if ((context.volume_ratio ?? 0) >= 2) add("volume", 5, "Heavy volume")
  else if ((context.volume_ratio ?? 0) >= 1.3) add("volume", 3, "Good volume support")
  else if ((context.volume_ratio ?? 0) >= 1.0) add("volume", 1, "Normal volume support")

  if (context.breakout_20d) add("breakout", 5, "20-day breakout")
  else if (context.breakout_10d) add("breakout", 3, "10-day breakout")
  else if ((context.breakout_clearance_pct ?? -999) >= -0.5) add("breakout", 1, "Near breakout")

  if (context.above_sma_20) add("trend", 3, "Above 20-day moving average")

  if ((context.close_in_day_range ?? 0) >= 0.75) add("close_strength", 2, "Strong close")
  else if ((context.close_in_day_range ?? 0) >= 0.55) add("close_strength", 1, "Constructive close")

  if ((context.extension_from_sma20_pct ?? 999) > 18) {
    add("extension_penalty", -5, "Too extended")
    caps.push("overextended-cap")
  } else if ((context.extension_from_sma20_pct ?? 999) > 14) {
    add("extension_penalty", -2, "Somewhat extended")
  }

  if (!(context.passes_price ?? true)) add("liquidity_penalty", -5, "Failed minimum price")
  if (!(context.passes_volume ?? true)) add("liquidity_penalty", -4, "Failed minimum volume")
  if (!(context.passes_dollar_volume ?? true)) add("liquidity_penalty", -5, "Failed minimum dollar volume")
  if (!(context.passes_market_cap ?? true)) add("liquidity_penalty", -4, "Failed minimum market cap")

  let rawScore = Object.values(breakdown).reduce((a, b) => a + b, 0)
  rawScore = clamp(Math.round(rawScore), 0, 100)

  let appScore = Math.round(Math.pow(rawScore / 100, 1.05) * 100)

  const hasPriorityEvidence =
    Boolean(ptrSummary?.buyTradeCount) ||
    Boolean(filingSummary?.hasForm4) ||
    Boolean(filingSummary?.has13DOr13G) ||
    Boolean(filingSummary?.hasCatalystForm)

  if (!hasPriorityEvidence) {
    appScore = Math.min(appScore, 74)
    caps.push("no-priority-evidence-cap")
  }

  if (
    !ptrSummary?.strongBuying &&
    !filingSummary?.hasForm4 &&
    !filingSummary?.has13DOr13G &&
    (context.relative_strength_20d ?? 0) < 2 &&
    (context.volume_ratio ?? 0) < 1.1
  ) {
    appScore = Math.min(appScore, 70)
    caps.push("weak-confirmation-cap")
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

function buildSignalRow(
  context: ContextRow,
  filingSummary: FilingSummary | null,
  ptrSummary: PtrSummary | null,
  runDate: string,
  runTimestamp: string
) {
  const ticker = normalizeTicker(context.ticker)
  if (!ticker) return null

  const companyId = getContextCompanyId(context)
  const scored = scoreCandidateSignal({ context, filingSummary, ptrSummary })
  const signalKey = buildSignalKey(ticker, runDate)

  const title =
    ptrSummary?.strongBuying
      ? "Strong political buying support"
      : filingSummary?.hasForm4 && filingSummary?.has13DOr13G
        ? "Insider and ownership support"
        : filingSummary?.hasForm4
          ? "Insider filing support"
          : filingSummary?.has13DOr13G
            ? "Ownership support"
            : filingSummary?.hasCatalystForm
              ? "Catalyst filing support"
              : "Constructive setup support"

  const summaryParts = uniqueStrings([
    ptrSummary?.summary,
    filingSummary?.hasForm4 ? "insider filings present" : null,
    filingSummary?.has13DOr13G ? "ownership filings present" : null,
    filingSummary?.hasCatalystForm ? "recent company filings present" : null,
    context.breakout_20d ? "20-day breakout present" : null,
    context.breakout_10d ? "10-day breakout present" : null,
    context.above_sma_20 ? "trend support is intact" : null,
    (context.volume_ratio ?? 0) >= 1.3 ? "volume is elevated" : null,
    (context.relative_strength_20d ?? 0) >= 3 ? "relative strength is positive" : null,
    context.screen_reason,
  ])

  const latestFiledAt =
    ptrSummary?.latestTradeDate ??
    filingSummary?.latestFiledAt ??
    runDate

  const sourceForms = uniqueStrings([...(filingSummary?.forms || [])])

  const signalType =
    ptrSummary?.buyTradeCount
      ? "Priority Multi-Signal Buy"
      : filingSummary?.hasForm4 || filingSummary?.has13DOr13G || filingSummary?.hasCatalystForm
        ? "Priority Filing Signal"
        : "Priority Signal"

  const signalSource =
    ptrSummary?.buyTradeCount
      ? "ptr+signals"
      : filingSummary?.hasForm4
        ? "form4"
        : filingSummary?.has13DOr13G
          ? "13d"
          : filingSummary?.hasCatalystForm
            ? "8k"
            : "breakout"

  const signalCategory =
    ptrSummary?.buyTradeCount
      ? "PTR / Filings / Signals Priority Buy"
      : filingSummary?.hasForm4 || filingSummary?.has13DOr13G || filingSummary?.hasCatalystForm
        ? "Filings / Signals Priority Buy"
        : "Signals Priority Buy"

  return {
    signal_key: signalKey,
    company_id: companyId,
    ticker,
    company_name: context.name,
    business_description: context.business_description ?? null,
    pe_ratio: round2(context.pe_ratio ?? null),
    pe_forward: round2(context.pe_forward ?? null),
    pe_type: context.pe_type ?? null,
    signal_type: signalType,
    signal_source: signalSource,
    signal_category: signalCategory,
    signal_strength_bucket: getStrengthBucket(scored.appScore),
    signal_tags: uniqueStrings([
      "priority-signal",
      ptrSummary?.buyTradeCount ? "ptr-support" : null,
      ptrSummary?.buyCluster ? "ptr-cluster" : null,
      ptrSummary?.strongBuying ? "ptr-strong-buying" : null,
      filingSummary?.hasForm4 ? "insider-filing" : null,
      filingSummary?.has13DOr13G ? "ownership-filing" : null,
      filingSummary?.hasCatalystForm ? "catalyst-filing" : null,
      context.breakout_20d ? "breakout-20d" : null,
      context.breakout_10d ? "breakout-10d" : null,
      context.above_sma_20 ? "above-sma20" : null,
      (context.volume_ratio ?? 0) >= 1.5 ? "volume-confirmed" : null,
      (context.relative_strength_20d ?? 0) >= 4 ? "relative-strength" : null,
    ]),
    catalyst_type: filingSummary?.hasCatalystForm ? "filing" : null,
    bias: "Bullish",
    score: scored.rawScore,
    app_score: scored.appScore,
    board_bucket: "Buy",
    title,
    summary: summaryParts.length
      ? `Why it stands out: ${summaryParts.join(", ")}.`
      : "Why it stands out: multiple constructive signals are lining up.",
    source_form: sourceForms[0] ?? null,
    filed_at: latestFiledAt,
    filing_url: null,
    accession_no: signalKey,
    insider_action: null,
    insider_shares: null,
    insider_avg_price: null,
    insider_buy_value: ptrSummary?.totalBuyAmountLow ?? null,
    insider_signal_flavor: ptrSummary?.buyTradeCount
      ? "PTR + Filings + Technical"
      : "Filings + Technical",
    cluster_buyers: ptrSummary?.uniqueBuyFilers ?? null,
    cluster_shares: null,
    price_return_5d: round2(context.return_5d ?? null),
    price_return_20d: round2(context.return_20d ?? null),
    volume_ratio: round2(context.volume_ratio ?? null),
    breakout_20d: context.breakout_20d === true,
    breakout_52w: false,
    above_50dma: context.above_sma_20 === true,
    trend_aligned: context.above_sma_20 === true,
    price_confirmed:
      context.breakout_20d === true ||
      context.breakout_10d === true ||
      (context.volume_ratio ?? 0) >= 1.3,
    earnings_surprise_pct: null,
    revenue_growth_pct: null,
    guidance_flag: filingSummary?.hasCatalystForm === true,
    market_cap: roundWhole(context.market_cap ?? null),
    sector: context.sector ?? null,
    industry: context.industry ?? null,
    relative_strength_20d: round2(context.relative_strength_20d ?? null),
    age_days: daysAgo(latestFiledAt),
    freshness_bucket:
      daysAgo(latestFiledAt) === null
        ? null
        : (daysAgo(latestFiledAt) ?? 999) <= 1
          ? "today"
          : (daysAgo(latestFiledAt) ?? 999) <= 3
            ? "fresh"
            : (daysAgo(latestFiledAt) ?? 999) <= 7
              ? "recent"
              : "aging",
    last_scored_at: runTimestamp,
    updated_at: runTimestamp,
    created_at: runTimestamp,
    score_breakdown: scored.breakdown,
    score_version: SCORE_VERSION,
    score_updated_at: runTimestamp,
    stacked_signal_count:
      (ptrSummary?.buyTradeCount || 0) +
      (filingSummary?.insiderFormCount || 0) +
      (filingSummary?.ownershipFormCount || 0) +
      (filingSummary?.catalystFormCount || 0) +
      1,
    signal_reasons: scored.reasons,
    score_caps_applied: scored.caps,
    ticker_score_change_1d: null,
    ticker_score_change_7d: null,
    source_forms: sourceForms,
    accession_nos: [],
  }
}

function buildSignalHistoryRow(signalRow: any, runDate: string, runTimestamp: string) {
  return {
    signal_history_key: buildHistoryKey(runDate, signalRow.signal_key),
    signal_key: signalRow.signal_key,
    company_id: signalRow.company_id ?? null,
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
    created_at: runTimestamp,
  }
}

async function loadCandidateContext(
  supabase: any,
  limit: number,
  candidateCutoffDateString: string,
  onlyActive: boolean
): Promise<{
  candidateRows: ContextRow[]
  candidateUniverseRowsLoaded: number
  candidateHistoryRowsLoaded: number
  fallbackCandidateSourceUsed: boolean
}> {
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

  if (latestScreenedOn) {
    let historyQuery = supabase
      .from("candidate_screen_history")
      .select(
        "id, company_id, ticker, cik, name, is_active, is_eligible, has_insider_trades, has_ptr_forms, has_clusters, eligibility_reason, price, market_cap, pe_ratio, pe_forward, pe_type, sector, industry, business_description, avg_volume_20d, avg_dollar_volume_20d, one_day_return, return_5d, return_10d, return_20d, relative_strength_20d, volume_ratio, breakout_20d, breakout_10d, above_sma_20, breakout_clearance_pct, extension_from_sma20_pct, close_in_day_range, catalyst_count, passes_price, passes_volume, passes_dollar_volume, passes_market_cap, candidate_score, included, screen_reason, last_screened_at, updated_at, screened_on"
      )
      .eq("screened_on", latestScreenedOn)
      .gte("candidate_score", MIN_CANDIDATE_SCORE)
      .order("candidate_score", { ascending: false })
      .limit(limit)

    if (onlyActive) {
      historyQuery = historyQuery.eq("is_active", true)
    }

    const historyResult = await historyQuery

    if (historyResult.error) {
      throw new Error(`candidate_screen_history snapshot load failed: ${historyResult.error.message}`)
    }

    const historyRows = (historyResult.data || []) as CandidateHistoryRow[]

    if (historyRows.length > 0) {
      return {
        candidateRows: historyRows,
        candidateUniverseRowsLoaded: 0,
        candidateHistoryRowsLoaded: historyRows.length,
        fallbackCandidateSourceUsed: true,
      }
    }
  }

  let universeQuery = supabase
    .from("candidate_universe")
    .select(
      "company_id, ticker, cik, name, is_active, is_eligible, has_insider_trades, has_ptr_forms, has_clusters, eligibility_reason, price, market_cap, pe_ratio, pe_forward, pe_type, sector, industry, business_description, avg_volume_20d, avg_dollar_volume_20d, one_day_return, return_5d, return_10d, return_20d, relative_strength_20d, volume_ratio, breakout_20d, breakout_10d, above_sma_20, breakout_clearance_pct, extension_from_sma20_pct, close_in_day_range, catalyst_count, passes_price, passes_volume, passes_dollar_volume, passes_market_cap, candidate_score, included, screen_reason, last_screened_at, updated_at"
    )
    .gte("candidate_score", MIN_CANDIDATE_SCORE)
    .gte("last_screened_at", candidateCutoffDateString)
    .order("candidate_score", { ascending: false })
    .limit(limit)

  if (onlyActive) {
    universeQuery = universeQuery.eq("is_active", true)
  }

  const universeResult = await universeQuery

  if (universeResult.error) {
    throw new Error(`candidate_universe load failed: ${universeResult.error.message}`)
  }

  const universeRows = (universeResult.data || []) as CandidateUniverseRow[]

  return {
    candidateRows: universeRows,
    candidateUniverseRowsLoaded: universeRows.length,
    candidateHistoryRowsLoaded: 0,
    fallbackCandidateSourceUsed: false,
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
    const minSignalStrength = Math.max(
      0,
      Math.min(100, parseInteger(searchParams.get("minSignalStrength"), DEFAULT_MIN_SIGNAL_APP_SCORE))
    )
    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"
    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"

    const now = new Date()
    const runDate = toIsoDateString(now)
    const runTimestamp = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)
    const candidateCutoffDateString = cutoffDate.toISOString()

    const ptrCutoff = new Date(now)
    ptrCutoff.setDate(ptrCutoff.getDate() - PTR_LOOKBACK_DAYS)
    const ptrCutoffString = toIsoDateString(ptrCutoff)

    const diagnostics: Diagnostics = {
      candidateUniverseRowsLoaded: 0,
      candidateHistoryRowsLoaded: 0,
      candidateRowsLoaded: 0,
      fallbackCandidateSourceUsed: false,
      rawFilingsLoaded: 0,
      rawPtrTradesLoaded: 0,
      tickersWithFilings: 0,
      tickersWithPtrSupport: 0,
      candidateSignalsBuilt: 0,
      candidateSignalsInserted: 0,
      signalHistoryInserted: 0,
      filteredBelowSignalScore: 0,
    }

    const [candidateContext, rawFilingsQuery, rawPtrTradesQuery] = await Promise.all([
      loadCandidateContext(supabase, limit, candidateCutoffDateString, onlyActive),
      supabase
        .from("raw_filings")
        .select(
          "company_id, ticker, company_name, filed_at, form_type, filing_url, accession_no, cik, primary_doc, fetched_at"
        )
        .gte("filed_at", cutoffDateString)
        .order("filed_at", { ascending: false })
        .limit(limit * 10),
      supabase
        .from("raw_ptr_trades")
        .select(
          "ticker, filer_name, action, transaction_date, report_date, amount_low, amount_high, amount_range"
        )
        .or(`transaction_date.gte.${ptrCutoffString},report_date.gte.${ptrCutoffString}`)
        .limit(limit * 20),
    ])

    if (rawFilingsQuery.error) {
      return Response.json(
        { ok: false, error: rawFilingsQuery.error.message },
        { status: 500 }
      )
    }

    if (rawPtrTradesQuery.error) {
      return Response.json(
        { ok: false, error: rawPtrTradesQuery.error.message },
        { status: 500 }
      )
    }

    const candidateRows = candidateContext.candidateRows
    diagnostics.candidateUniverseRowsLoaded = candidateContext.candidateUniverseRowsLoaded
    diagnostics.candidateHistoryRowsLoaded = candidateContext.candidateHistoryRowsLoaded
    diagnostics.candidateRowsLoaded = candidateRows.length
    diagnostics.fallbackCandidateSourceUsed = candidateContext.fallbackCandidateSourceUsed

    const filings = (rawFilingsQuery.data || []) as RawFilingRow[]
    const ptrTrades = (rawPtrTradesQuery.data || []) as RawPtrTradeRow[]

    diagnostics.rawFilingsLoaded = filings.length
    diagnostics.rawPtrTradesLoaded = ptrTrades.length

    const filingSummaryMap = buildFilingSummaryMap(filings)
    const ptrSummaryMap = buildPtrSummaryMap(ptrTrades)

    diagnostics.tickersWithFilings = filingSummaryMap.size
    diagnostics.tickersWithPtrSupport = ptrSummaryMap.size

    const signalRows: any[] = []
    const historyRows: any[] = []

    for (const context of candidateRows) {
      const ticker = normalizeTicker(context.ticker)
      if (!ticker) continue

      const filingSummary = filingSummaryMap.get(ticker) ?? null
      const ptrSummary = ptrSummaryMap.get(ticker) ?? null

      const signalRow = buildSignalRow(
        context,
        filingSummary,
        ptrSummary,
        runDate,
        runTimestamp
      )

      if (!signalRow) continue

      if (signalRow.app_score < minSignalStrength) {
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
      insertedCount = diagnostics.candidateSignalsInserted
    }

    return Response.json({
      ok: true,
      candidateUniverseRowsLoaded: diagnostics.candidateUniverseRowsLoaded,
      candidateHistoryRowsLoaded: diagnostics.candidateHistoryRowsLoaded,
      candidateRowsLoaded: diagnostics.candidateRowsLoaded,
      fallbackCandidateSourceUsed: diagnostics.fallbackCandidateSourceUsed,
      rawFilingsLoaded: diagnostics.rawFilingsLoaded,
      rawPtrTradesLoaded: diagnostics.rawPtrTradesLoaded,
      tickersWithFilings: diagnostics.tickersWithFilings,
      tickersWithPtrSupport: diagnostics.tickersWithPtrSupport,
      candidateSignalsInserted: diagnostics.candidateSignalsInserted,
      signalHistoryInserted: diagnostics.signalHistoryInserted,
      insertedCount,
      limit,
      lookbackDays,
      retainedDays: RETENTION_DAYS,
      scoreVersion: SCORE_VERSION,
      minSignalAppScore: minSignalStrength,
      retentionCleanup: retentionMessage,
      diagnostics,
      message:
        "Priority signals generated using PTR support first, filings second, and technical confirmation third.",
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