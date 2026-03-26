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
  buyerNames: string[]
}

type BreadthStats = {
  sectorCounts: Map<string, number>
  industryCounts: Map<string, number>
  totalTickers: number
}

type InsiderEnrichment = {
  totalBuyShares: number
  totalSellShares: number
  avgBuyPrice: number | null
  totalBuyValue: number
  buyerNames: string[]
  sellerNames: string[]
  buyTransactionCount: number
  sellTransactionCount: number
  latestTransactionDate: string | null
  action: string // "Buy", "Sell", "Mixed"
}

// --- Form 4 XML Parser ---
function parseForm4Xml(xml: string): {
  insiderName: string
  isDirector: boolean
  isOfficer: boolean
  officerTitle: string | null
  transactions: {
    shares: number
    pricePerShare: number
    acquired: boolean // true = acquired, false = disposed
    transactionCode: string // P=purchase, S=sale, A=award, etc
    date: string | null
  }[]
} | null {
  const nameMatch = xml.match(/<rptOwnerName>([^<]+)</)
  if (!nameMatch) return null

  const isDirector = /<isDirector>1</.test(xml)
  const isOfficer = /<isOfficer>1</.test(xml)
  const titleMatch = xml.match(/<officerTitle>([^<]+)</)

  const transactions: any[] = []
  const txBlocks = xml.split(/<nonDerivativeTransaction>/).slice(1)

  for (const block of txBlocks) {
    const sharesMatch = block.match(/<transactionShares>\s*<value>([^<]+)</)
    const priceMatch = block.match(/<transactionPricePerShare>\s*<value>([^<]+)</)
    const adCode = block.match(/<transactionAcquiredDisposedCode>\s*<value>([^<]+)</)
    const txCode = block.match(/<transactionCode>([^<]+)</)
    const dateMatch = block.match(/<transactionDate>\s*<value>([^<]+)</)

    const shares = parseFloat(sharesMatch?.[1] || "0")
    const price = parseFloat(priceMatch?.[1] || "0")
    if (shares <= 0) continue

    transactions.push({
      shares,
      pricePerShare: price,
      acquired: adCode?.[1]?.trim() === "A",
      transactionCode: txCode?.[1]?.trim() || "?",
      date: dateMatch?.[1]?.trim() || null,
    })
  }

  return {
    insiderName: nameMatch[1].trim(),
    isDirector,
    isOfficer,
    officerTitle: titleMatch?.[1]?.trim() || null,
    transactions,
  }
}

async function fetchForm4Details(
  supabase: any,
  tickers: string[]
): Promise<Map<string, InsiderEnrichment>> {
  const enrichMap = new Map<string, InsiderEnrichment>()
  if (tickers.length === 0) return enrichMap

  // Load raw_filings for these tickers (Form 4 only)
  const { data: filings } = await supabase
    .from("raw_filings")
    .select("ticker, accession_no, cik, filing_url, filed_at")
    .in("ticker", tickers)
    .in("form_type", ["4", "4/A"])
    .order("filed_at", { ascending: false })
    .limit(500)

  if (!filings || filings.length === 0) return enrichMap

  // Group by ticker, take up to 5 most recent filings per ticker
  const byTicker = new Map<string, any[]>()
  for (const f of filings) {
    const t = (f.ticker || "").toUpperCase()
    if (!byTicker.has(t)) byTicker.set(t, [])
    const arr = byTicker.get(t)!
    if (arr.length < 5) arr.push(f)
  }

  // Build XML URLs from filing_url (which has the correct filename)
  const fetchTasks: { ticker: string; url: string }[] = []
  for (const [ticker, tickerFilings] of byTicker.entries()) {
    for (const f of tickerFilings) {
      // The filing_url contains the XSLT-rendered path like .../xslF345X06/wk-form4_123.xml
      // We need the raw XML which is one level up: .../wk-form4_123.xml
      const filingUrl = String(f.filing_url || "").trim()
      if (!filingUrl) continue
      // Strip the xslF345X0N/ prefix to get the raw XML URL
      const url = filingUrl.replace(/\/xslF345X\d+\//, "/")
      fetchTasks.push({ ticker, url })
    }
  }

  // Fetch XMLs with concurrency limit of 10
  const CONCURRENCY = 10
  const SEC_DELAY = 110 // SEC rate limit: 10 req/sec
  const parsedResults: { ticker: string; parsed: ReturnType<typeof parseForm4Xml> }[] = []

  for (let i = 0; i < fetchTasks.length; i += CONCURRENCY) {
    const batch = fetchTasks.slice(i, i + CONCURRENCY)
    const results = await Promise.allSettled(
      batch.map(async ({ ticker, url }) => {
        const resp = await fetch(url, {
          headers: { "User-Agent": "MarketSignalTracker research@marketsignaltracker.com" },
          signal: AbortSignal.timeout(8000),
        })
        if (!resp.ok) {
          console.warn(`Form4 fetch failed: ${ticker} ${resp.status} ${url.slice(-40)}`)
          return { ticker, parsed: null }
        }
        const xml = await resp.text()
        const parsed = parseForm4Xml(xml)
        if (parsed && parsed.transactions.length > 0) {
          console.log(`Form4 parsed: ${ticker} ${parsed.insiderName} ${parsed.transactions.length} txns`)
        }
        return { ticker, parsed }
      })
    )
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) {
        parsedResults.push(r.value)
      } else if (r.status === "rejected") {
        console.warn(`Form4 fetch rejected: ${r.reason}`)
      }
    }
    if (i + CONCURRENCY < fetchTasks.length) {
      await new Promise((r) => setTimeout(r, SEC_DELAY))
    }
  }

  // Aggregate per ticker
  for (const { ticker, parsed } of parsedResults) {
    if (!parsed || parsed.transactions.length === 0) continue

    let existing = enrichMap.get(ticker)
    if (!existing) {
      existing = {
        totalBuyShares: 0,
        totalSellShares: 0,
        avgBuyPrice: null,
        totalBuyValue: 0,
        buyerNames: [],
        sellerNames: [],
        buyTransactionCount: 0,
        sellTransactionCount: 0,
        latestTransactionDate: null,
        action: "Filed",
      }
      enrichMap.set(ticker, existing)
    }

    for (const tx of parsed.transactions) {
      // Only count open market purchases (P) as real insider buying conviction
      // A=award, M=exercise are not conviction signals
      // S=sale, F=tax withholding are disposals
      const isOpenMarketPurchase = tx.acquired && tx.transactionCode === "P"
      const isSale = !tx.acquired && (tx.transactionCode === "S" || tx.transactionCode === "F")

      if (isOpenMarketPurchase) {
        existing.totalBuyShares += tx.shares
        if (tx.pricePerShare > 0) {
          existing.totalBuyValue += tx.shares * tx.pricePerShare
        }
        existing.buyTransactionCount += 1
        if (!existing.buyerNames.includes(parsed.insiderName)) {
          existing.buyerNames.push(parsed.insiderName)
        }
      } else if (isSale) {
        existing.totalSellShares += tx.shares
        existing.sellTransactionCount += 1
        if (!existing.sellerNames.includes(parsed.insiderName)) {
          existing.sellerNames.push(parsed.insiderName)
        }
      }

      if (tx.date && (!existing.latestTransactionDate || tx.date > existing.latestTransactionDate)) {
        existing.latestTransactionDate = tx.date
      }
    }

    existing.avgBuyPrice = existing.buyTransactionCount > 0 && existing.totalBuyValue > 0
      ? Math.round((existing.totalBuyValue / existing.totalBuyShares) * 100) / 100
      : null
    existing.action = existing.buyTransactionCount > 0 && existing.sellTransactionCount > 0
      ? "Buying & Selling"
      : existing.buyTransactionCount > 0
        ? "Buying"
        : existing.sellTransactionCount > 0
          ? "Selling"
          : "Filed"
  }

  return enrichMap
}

const DEFAULT_LOOKBACK_DAYS = 31
const MAX_LOOKBACK_DAYS = 90
const DEFAULT_LIMIT = 100
const MAX_LIMIT = 3000
const RETENTION_DAYS = 30
const SCORE_VERSION = "v14-smart-buy-70-100"

const DEFAULT_PTR_LOOKBACK_DAYS = 60
const MAX_PTR_LOOKBACK_DAYS = 120
const DEFAULT_PTR_RECENT_DAYS = 14
const MAX_PTR_RECENT_DAYS = 30

const MIN_SIGNAL_APP_SCORE = 30
const MIN_TICKER_APP_SCORE = 30
const MIN_COMBINED_SCORE = 35

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
  if (score >= 90) return "Elite Buy"
  if (score >= 80) return "Strong Buy"
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
      buyerNames: Array.from(new Set(
        buys.map((r: any) => String(r.filer_name || "").trim()).filter(Boolean)
      )),
    })
  }

  return output
}

function computeExitStrategy(params: {
  ticker: string
  finalScore: number
  price: number | null
  extensionFromSma20Pct: number | null
  aboveSma20: boolean | null
  volumeRatio: number | null
  return5d: number | null
  return20d: number | null
  relativeStrength20d: number | null
  peRatio: number | null
  sector: string | null
  ageDays: number | null
  filedAt: string | null
  hasPtr: boolean
  hasInsiderBuying: boolean
  breakout20d: boolean | null
  signalReasons: string[]
}) {
  const {
    price, extensionFromSma20Pct, aboveSma20, volumeRatio,
    return5d, return20d, relativeStrength20d, peRatio, sector,
    ageDays, filedAt, hasPtr, hasInsiderBuying, breakout20d, finalScore,
  } = params

  if (!price || price <= 0) {
    return {
      stop_loss_price: null,
      stop_loss_type: "Insufficient data",
      profit_target: null,
      catalyst_expiry_days: null,
      risk_reward_ratio: null,
      exit_signals: ["Monitor for updated price data"],
      strategy_summary: "Insufficient price data to compute exit strategy.",
    }
  }

  const ext = extensionFromSma20Pct ?? 0
  const exitSignals: string[] = []

  // --- STOP-LOSS ---
  // Compute SMA20 from extension
  const sma20 = ext !== 0 ? round2(price / (1 + ext / 100)) : null

  // Primary stop: below SMA20 support (or 7% trailing if no SMA)
  let stopLossPrice: number
  let stopLossType: string

  if (sma20 && aboveSma20) {
    // Place stop just below the 20-day MA (2% below for buffer)
    stopLossPrice = round2(sma20 * 0.98) ?? Math.round(sma20 * 0.98 * 100) / 100
    stopLossType = "Below 20-day moving average"
    exitSignals.push(`Sell if price closes below $${stopLossPrice} (SMA20 support broken)`)
  } else {
    // No SMA data or below SMA — use 7% trailing stop
    stopLossPrice = round2(price * 0.93) ?? Math.round(price * 0.93 * 100) / 100
    stopLossType = "7% trailing stop-loss"
    exitSignals.push(`Sell if price drops to $${stopLossPrice} (7% trailing stop)`)
  }

  // --- PROFIT TARGET ---
  // Risk/reward ratio of at least 2:1
  const riskAmount = price - stopLossPrice
  const profitTarget = round2(price + riskAmount * 2.5) ?? Math.round((price + riskAmount * 2.5) * 100) / 100
  const riskRewardRatio = riskAmount > 0 ? (round2((profitTarget - price) / riskAmount) ?? 2.5) : null

  exitSignals.push(`Take profit near $${profitTarget} (2.5:1 risk/reward target)`)

  // --- EXTENSION WARNING ---
  if (ext > 12) {
    exitSignals.push(`Caution: ${round2(ext)}% extended above SMA20 — consider partial profit-taking`)
  } else if (ext > 8) {
    exitSignals.push(`Getting extended (${round2(ext)}% above SMA20) — tighten stop-loss`)
  }

  // --- VOLUME REVERSAL ---
  if ((volumeRatio ?? 0) >= 2.0) {
    exitSignals.push("High volume detected — watch for reversal if price turns down on heavy volume")
  }

  // --- CHASE WARNING ---
  if ((return5d ?? 0) >= 10) {
    exitSignals.push(`Up ${round2(return5d!)}% in 5 days — avoid adding here, consider trailing stop`)
  }

  // --- CATALYST EXPIRY ---
  const catalystAge = ageDays ?? 0
  const catalystExpiryDays: number | null = null

  // --- INSIDER / PTR REVERSAL WATCH ---
  if (hasInsiderBuying) {
    exitSignals.push("Watch for Form 4 insider SELL filings — would negate buy thesis")
  }
  if (hasPtr) {
    exitSignals.push("Monitor for congressional sell disclosures — would reverse PTR signal")
  }

  // --- RELATIVE STRENGTH ---
  if ((relativeStrength20d ?? 0) < 0) {
    exitSignals.push("Relative strength is negative — stock underperforming the market")
  }

  // --- SCORE DEGRADATION ---
  exitSignals.push(`Exit if score drops below 60 (currently ${finalScore})`)

  // --- STRATEGY SUMMARY ---
  const summaryParts: string[] = []
  summaryParts.push(`Stop-loss at $${stopLossPrice} (${stopLossType})`)
  summaryParts.push(`Profit target at $${profitTarget}`)
  if (ext > 8) summaryParts.push("Position is extended — manage risk closely")

  return {
    stop_loss_price: stopLossPrice,
    stop_loss_type: stopLossType,
    profit_target: profitTarget,
    catalyst_expiry_days: catalystExpiryDays,
    risk_reward_ratio: riskRewardRatio,
    exit_signals: exitSignals,
    strategy_summary: summaryParts.join(". ") + ".",
  }
}

function buildTickerScoresCurrentRows(
  signalRows: SignalRow[],
  runTimestamp: string,
  ptrMap: Map<string, PtrSummary>,
  breadthStats: BreadthStats,
  minCombinedScore: number,
  ltcsScoreMap: Map<string, number>,
  tickerPriceDataMap: Map<string, any>
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

    // Signal stacking bonus — reward multiple signal families, no penalty for single
    if (maxSignalFamilyCount >= 3) {
      stackedScore += 8
      scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) + 8) ?? 0
      scoreCapsApplied.add("triple-stacked")
    } else if (maxSignalFamilyCount >= 2) {
      stackedScore += 4
      scoreBreakdown.family_diversity = round2((scoreBreakdown.family_diversity || 0) + 4) ?? 0
      scoreCapsApplied.add("double-stacked")
    }
    // No penalty for single-family — a fresh Form 4 alone is a valid signal

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

    // Softer crowding penalties — with ~50 stocks, some concentration is expected
    if (sectorShare >= 0.30) {
      stackedScore -= 4
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 4) ?? 0
      scoreCapsApplied.add("sector-crowding-penalty")
    } else if (sectorShare >= 0.20) {
      stackedScore -= 2
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 2) ?? 0
      scoreCapsApplied.add("sector-crowding-warning")
    }

    if (industryShare >= 0.18) {
      stackedScore -= 3
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 3) ?? 0
      scoreCapsApplied.add("industry-crowding-penalty")
    } else if (industryShare >= 0.12) {
      stackedScore -= 1
      scoreBreakdown.crowding_penalty = round2((scoreBreakdown.crowding_penalty || 0) - 1) ?? 0
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

    // Use the signal's app_score (v14: 0-30 raw scale) as the primary base
    // Blend with LTCS fundamental score and insider conviction bonus
    const signalScore = primaryScore  // from the signal's app_score
    const ltcsBase = ltcsScoreMap.get(ticker) ?? 0

    // Insider catalyst bonus (max 15 pts — the dominant differentiator)
    let insiderBonus = 0
    const clusterBuyers = primary.cluster_buyers ?? 0
    const hasPtrBuy = (ptr?.buyTradeCount ?? 0) > 0
    const hasInsiderFiling = (primary.signal_tags || []).some((t: string) => t.includes("insider"))

    if (clusterBuyers >= 3 && hasPtrBuy) {
      insiderBonus = 15  // platinum: cluster + congress
    } else if (hasInsiderFiling && hasPtrBuy) {
      insiderBonus = 12  // insider filing + congress = double smart money
    } else if (clusterBuyers >= 2) {
      insiderBonus = 10  // cluster buying
    } else if (hasPtrBuy) {
      insiderBonus = 8   // congressional buy alone
    } else if (clusterBuyers >= 1) {
      insiderBonus = 5   // single confirmed insider buy
    } else if (hasInsiderFiling) {
      insiderBonus = 2   // insider filing (no confirmed purchase direction)
    }

    // Freshness bonus (max 3 pts)
    const ageDays = primary.age_days ?? 999
    if (ageDays <= 1) insiderBonus += 3
    else if (ageDays <= 3) insiderBonus += 2
    else if (ageDays <= 7) insiderBonus += 1

    // Build final score: 70-100 range (30pt spread)
    // Base 50 + components that sum to 20-50 pts
    // signalScore (~50-95) → 9-17 pts   (catalyst quality + technicals)
    // ltcsBase (0-100)     → 6-12 pts   (fundamental quality)
    // insiderBonus (0-18)  → 0-18 pts   (smart money — the make-or-break factor)
    // stackedScore (capped) → -4 to +5  (diversity/crowding)
    // Worst: 50+9+6+0-4=61→70 | No insider: 50+15+10+0+2=77 | PTR: 50+15+10+8+3=86 | Best: 50+17+12+18+5=102→100
    const cappedStacked = clamp(stackedScore * 0.2, -4, 5)
    const rawFinal = 50 + (signalScore * 0.18) + (ltcsBase * 0.12) + insiderBonus + cappedStacked
    let finalScore = clamp(Math.round(rawFinal), 70, 100)

    // Platinum conviction: insider+congress+strong fundamentals = guaranteed 97+
    if (
      hasInsiderFiling && hasPtrBuy && ltcsBase >= 70 && primaryScore >= 60
    ) {
      finalScore = Math.max(finalScore, 97)
      scoreCapsApplied.add("platinum-conviction")
    }

    if (ltcsBase < 25 && insiderBonus === 0 && signalScore < 10) continue
    if (finalScore < 70) continue

    // --- INSIDER SELLING GATE ---
    // If insiders are selling and there's no congressional buy to offset, exclude the ticker
    // A congressional buy overrides insider selling (congress may know something insiders don't)
    const insiderIsSelling = row.insider_action === "Selling" || row.insider_action === "Sell"
    if (insiderIsSelling && !hasPtrBuy) {
      continue // Skip — insiders selling without congressional offset is not a buy signal
    }
    // If insiders selling BUT congress is buying, penalize but keep
    if (insiderIsSelling && hasPtrBuy) {
      finalScore = Math.max(70, finalScore - 5)
      scoreCapsApplied.add("insider-sell-offset-by-ptr")
      scoreBreakdown.insider_sell_penalty = -5
    }

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

    // Compute exit strategy from price data + signal context
    const priceData = tickerPriceDataMap.get(ticker)
    const exitStrategy = computeExitStrategy({
      ticker,
      finalScore,
      price: priceData?.price ?? null,
      extensionFromSma20Pct: priceData?.extension_from_sma20_pct ?? null,
      aboveSma20: priceData?.above_sma_20 ?? null,
      volumeRatio: priceData?.volume_ratio ?? primary.volume_ratio ?? null,
      return5d: priceData?.return_5d ?? primary.price_return_5d ?? null,
      return20d: priceData?.return_20d ?? primary.price_return_20d ?? null,
      relativeStrength20d: priceData?.relative_strength_20d ?? primary.relative_strength_20d ?? null,
      peRatio: primary.pe_ratio ?? null,
      sector: primary.sector ?? null,
      ageDays: primary.age_days ?? null,
      filedAt: primary.filed_at ?? null,
      hasPtr: Boolean(ptr?.buyTradeCount),
      hasInsiderBuying: Boolean(primary.insider_buy_value),
      breakout20d: priceData?.breakout_20d ?? primary.breakout_20d ?? null,
      signalReasons: Array.from(signalReasons),
    })

    rows.push({
      ticker,
      company_name: primary.company_name,
      business_description: primary.business_description,
      app_score: finalScore,
      raw_score: finalScore,
      bias: "Bullish",
      board_bucket:
        finalScore >= 85
          ? "High Conviction"
          : finalScore >= 70
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
      market_cap: primary.market_cap != null ? Math.round(primary.market_cap) : null,
      sector: primary.sector,
      industry: primary.industry,
      insider_action: primary.insider_action,
      insider_shares: primary.insider_shares != null ? Math.round(primary.insider_shares) : null,
      insider_avg_price: primary.insider_avg_price,
      insider_buy_value: primary.insider_buy_value != null ? Math.round(primary.insider_buy_value) : null,
      cluster_buyers: primary.cluster_buyers,
      cluster_shares: primary.cluster_shares != null ? Math.round(primary.cluster_shares) : null,
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
      exit_strategy: exitStrategy,
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

    // Load latest LTCS scores + price/technical data from candidate_screen_history
    const { data: ltcsRows } = await supabase
      .from("candidate_screen_history")
      .select("ticker, candidate_score, screened_on, price, market_cap, above_sma_20, extension_from_sma20_pct, volume_ratio, breakout_20d, relative_strength_20d, return_5d, return_20d, pe_ratio, sector")
      .gte("candidate_score", 25)
      .order("screened_on", { ascending: false })
      .order("candidate_score", { ascending: false })
      .limit(5000)

    // Build map of ticker → best LTCS score (most recent screen date)
    const ltcsScoreMap = new Map<string, number>()
    const tickerPriceDataMap = new Map<string, any>()
    for (const row of (ltcsRows || []) as any[]) {
      const t = (row.ticker || "").toUpperCase()
      if (t && !ltcsScoreMap.has(t)) {
        ltcsScoreMap.set(t, row.candidate_score ?? 0)
        tickerPriceDataMap.set(t, row)
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
      ltcsScoreMap,
      tickerPriceDataMap
    )

    // --- Enrich with Form 4 insider details (parsed from SEC EDGAR XML) ---
    const tickersToEnrich = tickerCurrentRowsBase.map((r: any) => r.ticker).filter(Boolean)
    let insiderEnrichMap = new Map<string, InsiderEnrichment>()
    try {
      console.log(`Form4 enrichment: starting for ${tickersToEnrich.length} tickers`)
      insiderEnrichMap = await fetchForm4Details(supabase, tickersToEnrich)
      console.log(`Form4 enrichment: got data for ${insiderEnrichMap.size} tickers`)
      for (const [t, e] of insiderEnrichMap.entries()) {
        console.log(`  ${t}: buys=${e.buyTransactionCount} sells=${e.sellTransactionCount} buyShares=${e.totalBuyShares} buyVal=${Math.round(e.totalBuyValue)} buyers=[${e.buyerNames.join(",")}]`)
      }
    } catch (e) {
      console.error("Form 4 enrichment failed (non-fatal):", e)
    }

    // Apply enrichment to rows — also fix filing-count-as-shares for non-enriched tickers
    for (const row of tickerCurrentRowsBase) {
      const enrich = insiderEnrichMap.get(row.ticker)
      if (enrich) {
        if (enrich.buyTransactionCount > 0) {
          // Real open market purchases found
          row.insider_action = enrich.sellTransactionCount > 0 ? "Buying & Selling" : "Buying"
          row.insider_shares = Math.round(enrich.totalBuyShares)
          row.insider_avg_price = enrich.avgBuyPrice
          row.insider_buy_value = Math.round(enrich.totalBuyValue)
          row.cluster_buyers = enrich.buyerNames.length
          row.insider_signal_flavor = enrich.buyerNames.length > 0
            ? `${enrich.buyerNames.slice(0, 3).join(", ")}${enrich.buyerNames.length > 3 ? ` +${enrich.buyerNames.length - 3} more` : ""}`
            : row.insider_signal_flavor
        } else if (enrich.sellTransactionCount > 0) {
          // Only selling, no open market purchases
          row.insider_action = "Selling"
          row.insider_shares = Math.round(enrich.totalSellShares)
          row.insider_avg_price = null
          row.insider_buy_value = null
          row.insider_signal_flavor = enrich.sellerNames.length > 0
            ? `${enrich.sellerNames.slice(0, 3).join(", ")} selling`
            : "Insider selling"
        } else {
          // Form 4 filings exist but no P-code purchases and no sales
          // (only awards/exercises/gifts) — clear the misleading filing count
          row.insider_action = "Awards only"
          row.insider_shares = null
          row.insider_avg_price = null
          row.insider_buy_value = null
          row.insider_signal_flavor = "No open market purchases"
        }
      } else if (row.insider_action === "Filed" && (row.insider_buy_value == null || row.insider_buy_value === 0)) {
        // No enrichment data — insider_shares is a filing count, not real shares
        const filingCount = row.insider_shares
        console.log(`Clearing filing count for ${row.ticker}: was ${filingCount} "shares" (actually filings)`)
        row.insider_shares = null
        row.insider_avg_price = null
        row.insider_buy_value = null
        row.insider_signal_flavor = filingCount ? `${filingCount} Form 4 filing${filingCount === 1 ? "" : "s"}` : null
        row.insider_action = "Form 4 filed"
      }

      // PTR enrichment from ptrSummaryMap
      const ptr = ptrSummaryMap.get(row.ticker)
      if (ptr && ptr.buyTradeCount > 0) {
        row.cluster_buyers = row.cluster_buyers ?? ptr.uniqueBuyFilers
        row.cluster_shares = ptr.buyTradeCount
        row.insider_buy_value = row.insider_buy_value ?? (ptr.totalBuyAmountLow > 0 ? ptr.totalBuyAmountLow : null)
        if (ptr.buyerNames && ptr.buyerNames.length > 0) {
          row.insider_signal_flavor = `PTR: ${ptr.buyerNames.slice(0, 3).join(", ")}`
        }
      }
    }

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
      form4EnrichmentCount: insiderEnrichMap.size,
      form4EnrichmentTickers: [...insiderEnrichMap.keys()].slice(0, 10),
      form4SampleData: [...insiderEnrichMap.entries()].slice(0, 3).map(([t, e]) => ({
        ticker: t, buys: e.buyTransactionCount, sells: e.sellTransactionCount,
        buyShares: e.totalBuyShares, buyVal: Math.round(e.totalBuyValue),
        buyers: e.buyerNames.slice(0, 3),
      })),
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