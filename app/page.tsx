"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { supabase } from "../lib/supabase"

type CandidateUniverseRow = {
  ticker: string
  cik?: string | null
  name?: string | null
  business_description?: string | null
  price?: number | null
  market_cap?: number | null
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
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  pe_ratio?: number | null
  pe_forward?: number | null
  sector?: string | null
  industry?: string | null
}

type TickerScoreRow = {
  ticker: string
  company_name?: string | null
  business_description?: string | null
  app_score?: number | null
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
  primary_signal_type?: string | null
  primary_signal_source?: string | null
  primary_signal_category?: string | null
  primary_title?: string | null
  primary_summary?: string | null
  filed_at?: string | null
  accession_nos?: string[] | null
  source_forms?: string[] | null
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
  ticker: string
  amount_range?: string | null
}

type UnifiedRow = {
  ticker: string
  company_name: string | null
  business_description: string | null
  price: number | null

  candidate_score: number | null
  signal_score: number | null
  display_score: number

  included: boolean
  screen_reason: string | null
  last_screened_at: string | null

  market_cap: number | null
  sector: string | null
  industry: string | null
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null

  one_day_return: number | null
  price_return_5d: number | null
  return_10d: number | null
  price_return_20d: number | null
  volume_ratio: number | null
  relative_strength_20d: number | null

  breakout_20d: boolean | null
  breakout_10d: boolean | null
  breakout_52w: boolean | null
  above_sma_20: boolean | null
  above_50dma: boolean | null
  trend_aligned: boolean | null
  price_confirmed: boolean | null

  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number | null

  signal_strength_bucket: string | null
  bias: string | null
  board_bucket: string | null

  signal_tags: string[]
  signal_reasons: string[]
  score_breakdown: Record<string, number> | null
  score_caps_applied: string[]

  primary_signal_type: string | null
  primary_signal_source: string | null
  primary_signal_category: string | null
  primary_title: string | null
  primary_summary: string | null

  filed_at: string | null
  accession_nos: string[]
  source_forms: string[]
  age_days: number | null
  freshness_bucket: string | null

  insider_action: string | null
  insider_shares: number | null
  insider_avg_price: number | null
  insider_buy_value: number | null
  ptr_amount: string | null
  cluster_buyers: number | null
  cluster_shares: number | null

  earnings_surprise_pct: number | null
  revenue_growth_pct: number | null
  guidance_flag: boolean | null

  ticker_score_change_1d: number | null
  ticker_score_change_7d: number | null

  score_version: string | null
  score_updated_at: string | null
  stacked_signal_count: number | null
  updated_at: string | null
  created_at: string | null

  has_candidate_data: boolean
  has_signal_data: boolean
  has_insider_trades: boolean
  has_ptr_forms: boolean
  has_clusters: boolean
  data_source_label:
    | "Quality Score + Signals"
    | "Quality Score Only"
    | "Signals Only"
}

type PriceFilterType =
  | "all"
  | "under10"
  | "10to25"
  | "25to100"
  | "100plus"

type PeFilterType = "all" | "20" | "30" | "50"
type FreshnessFilterType = "all" | "today" | "3d" | "7d" | "14d"
type ScoreFilterType = "all" | "70" | "75" | "80" | "85" | "90"
type SectorFilterType = "all" | string
type SourceFilterType = "all" | "technical_only" | "filing_only" | "both"
type InsiderFilterType = "all" | "yes" | "cluster"
type CongressFilterType = "all" | "yes"

type MiniMetricItem = {
  label: string
  value: string
}

type ReasonLine = {
  label: string
  value: string
  tone: "good" | "bad" | "neutral"
  weight: number
}

const CARDS_PER_PAGE = 18
const DETAIL_TABS = ["Overview", "Fundamentals", "Numbers"] as const

function normalizeTicker(value: string | null | undefined) {
  return (value || "").trim().toUpperCase()
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function round1(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.round(value * 10) / 10
}

function getCandidateScore(row: CandidateUniverseRow | null | undefined) {
  return row?.candidate_score ?? null
}

function getSignalScore(row: TickerScoreRow | null | undefined) {
  return row?.app_score ?? null
}

function firstString(...values: Array<string | null | undefined>) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) return value
  }
  return null
}

function firstNumber(...values: Array<number | null | undefined>) {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) return value
  }
  return 0
}

function firstNumberOrNull(...values: Array<number | string | null | undefined>) {
  for (const value of values) {
    if (value === null || value === undefined) continue
    const n = typeof value === "number" ? value : Number(value)
    if (Number.isFinite(n)) return n
  }
  return null
}

function firstBooleanOrNull(...values: Array<boolean | null | undefined>) {
  for (const value of values) {
    if (typeof value === "boolean") return value
  }
  return null
}

function makeUnifiedRow(
  candidate: CandidateUniverseRow | null,
  signal: TickerScoreRow | null,
  ptrAmount: string | null
): UnifiedRow | null {
  const ticker = normalizeTicker(candidate?.ticker || signal?.ticker)
  if (!ticker) return null

  const candidateScore = getCandidateScore(candidate)
  const signalScore = getSignalScore(signal)
  const displayScore = Math.round(firstNumber(signalScore, candidateScore, 0))

  return {
    ticker,
    company_name: firstString(signal?.company_name, candidate?.name),
    business_description: firstString(
      candidate?.business_description,
      signal?.business_description,
      null
    ),
    price: firstNumberOrNull(candidate?.price, null),

    candidate_score: candidateScore,
    signal_score: signalScore,
    display_score: clamp(displayScore, 0, 100),

    included: candidate?.included === true,
    screen_reason: candidate?.screen_reason ?? null,
    last_screened_at: candidate?.last_screened_at ?? null,

    market_cap: firstNumberOrNull(signal?.market_cap, candidate?.market_cap, null),
    sector: signal?.sector ?? candidate?.sector ?? null,
    industry: signal?.industry ?? candidate?.industry ?? null,
    pe_ratio: signal?.pe_ratio ?? candidate?.pe_ratio ?? null,
    pe_forward: signal?.pe_forward ?? candidate?.pe_forward ?? null,
    pe_type: signal?.pe_type ?? null,

    one_day_return: candidate?.one_day_return ?? null,
    price_return_5d: firstNumberOrNull(
      signal?.price_return_5d,
      candidate?.return_5d,
      null
    ),
    return_10d: candidate?.return_10d ?? null,
    price_return_20d: firstNumberOrNull(
      signal?.price_return_20d,
      candidate?.return_20d,
      null
    ),
    volume_ratio: firstNumberOrNull(signal?.volume_ratio, candidate?.volume_ratio, null),
    relative_strength_20d: firstNumberOrNull(
      signal?.relative_strength_20d,
      candidate?.relative_strength_20d,
      null
    ),

    breakout_20d: firstBooleanOrNull(signal?.breakout_20d, candidate?.breakout_20d, null),
    breakout_10d: candidate?.breakout_10d ?? null,
    breakout_52w: signal?.breakout_52w ?? null,
    above_sma_20: candidate?.above_sma_20 ?? null,
    above_50dma: signal?.above_50dma ?? null,
    trend_aligned: firstBooleanOrNull(signal?.trend_aligned, null),
    price_confirmed: firstBooleanOrNull(signal?.price_confirmed, null),

    breakout_clearance_pct: candidate?.breakout_clearance_pct ?? null,
    extension_from_sma20_pct: candidate?.extension_from_sma20_pct ?? null,
    close_in_day_range: candidate?.close_in_day_range ?? null,
    catalyst_count: candidate?.catalyst_count ?? null,

    signal_strength_bucket: signal?.signal_strength_bucket ?? null,
    bias: signal?.bias ?? null,
    board_bucket: signal?.board_bucket ?? null,

    signal_tags: Array.isArray(signal?.signal_tags) ? signal.signal_tags : [],
    signal_reasons: Array.isArray(signal?.signal_reasons) ? signal.signal_reasons : [],
    score_breakdown: signal?.score_breakdown ?? null,
    score_caps_applied: Array.isArray(signal?.score_caps_applied)
      ? signal.score_caps_applied
      : [],

    primary_signal_type: signal?.primary_signal_type ?? null,
    primary_signal_source: signal?.primary_signal_source ?? null,
    primary_signal_category: signal?.primary_signal_category ?? null,
    primary_title: signal?.primary_title ?? null,
    primary_summary: signal?.primary_summary ?? null,

    filed_at: signal?.filed_at ?? null,
    accession_nos: Array.isArray(signal?.accession_nos) ? signal.accession_nos : [],
    source_forms: Array.isArray(signal?.source_forms) ? signal.source_forms : [],
    age_days: signal?.age_days ?? null,
    freshness_bucket: signal?.freshness_bucket ?? null,

    insider_action: signal?.insider_action ?? null,
    insider_shares: signal?.insider_shares ?? null,
    insider_avg_price: signal?.insider_avg_price ?? null,
    insider_buy_value: signal?.insider_buy_value ?? null,
    ptr_amount: ptrAmount,
    cluster_buyers: signal?.cluster_buyers ?? null,
    cluster_shares: signal?.cluster_shares ?? null,

    earnings_surprise_pct: signal?.earnings_surprise_pct ?? null,
    revenue_growth_pct: signal?.revenue_growth_pct ?? null,
    guidance_flag: signal?.guidance_flag ?? null,

    ticker_score_change_1d: signal?.ticker_score_change_1d ?? null,
    ticker_score_change_7d: signal?.ticker_score_change_7d ?? null,

    score_version: signal?.score_version ?? "candidate-universe",
    score_updated_at: firstString(signal?.score_updated_at, candidate?.updated_at),
    stacked_signal_count: signal?.stacked_signal_count ?? (candidate ? 1 : null),
    updated_at: firstString(signal?.updated_at, candidate?.updated_at),
    created_at: signal?.created_at ?? null,

    has_candidate_data: Boolean(candidate),
    has_signal_data: Boolean(signal),
    has_insider_trades: candidate?.has_insider_trades === true,
    has_ptr_forms: candidate?.has_ptr_forms === true,
    has_clusters: candidate?.has_clusters === true,
    data_source_label:
      candidate && signal
        ? "Quality Score + Signals"
        : candidate
          ? "Quality Score Only"
          : "Signals Only",
  }
}

export default function Home() {
  const [rows, setRows] = useState<UnifiedRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [cardIndex, setCardIndex] = useState(0)
  const [filtersOpen, setFiltersOpen] = useState(false)

  const [priceFilter, setPriceFilter] = useState<PriceFilterType>("all")
  const [peFilter, setPeFilter] = useState<PeFilterType>("all")
  const [freshnessFilter, setFreshnessFilter] = useState<FreshnessFilterType>("all")
  const [scoreFilter, setScoreFilter] = useState<ScoreFilterType>("all")
  const [sectorFilter, setSectorFilter] = useState<SectorFilterType>("all")
  const [sourceFilter, setSourceFilter] = useState<SourceFilterType>("all")
  const [insiderFilter, setInsiderFilter] = useState<InsiderFilterType>("all")
  const [congressFilter, setCongressFilter] = useState<CongressFilterType>("all")

  const [beginnerMode, setBeginnerMode] = useState(true)

  const [detailInitialTab, setDetailInitialTab] = useState(0)

  useEffect(() => {
    let isMounted = true

    async function loadData() {
      try {
        setLoading(true)
        setError(null)

        const [candidateRes, signalRes, ptrRes] = await Promise.all([
          supabase
            .from("candidate_universe")
            .select(`
              ticker,
              cik,
              name,
              business_description,
              price,
              market_cap,
              avg_volume_20d,
              avg_dollar_volume_20d,
              one_day_return,
              return_5d,
              return_10d,
              return_20d,
              relative_strength_20d,
              volume_ratio,
              breakout_20d,
              breakout_10d,
              above_sma_20,
              breakout_clearance_pct,
              extension_from_sma20_pct,
              close_in_day_range,
              catalyst_count,
              passes_price,
              passes_volume,
              passes_dollar_volume,
              passes_market_cap,
              candidate_score,
              included,
              screen_reason,
              last_screened_at,
              updated_at,
              has_insider_trades,
              has_ptr_forms,
              has_clusters,
              pe_ratio,
              pe_forward,
              sector,
              industry
            `)
            .gte("candidate_score", 50)
            .order("candidate_score", { ascending: false })
            .limit(1000),

          supabase
            .from("ticker_scores_current")
            .select(`
              ticker,
              company_name,
              business_description,
              app_score,
              raw_score,
              bias,
              board_bucket,
              signal_strength_bucket,
              score_version,
              score_updated_at,
              stacked_signal_count,
              score_breakdown,
              signal_reasons,
              score_caps_applied,
              signal_tags,
              primary_signal_type,
              primary_signal_source,
              primary_signal_category,
              primary_title,
              primary_summary,
              filed_at,
              accession_nos,
              source_forms,
              pe_ratio,
              pe_forward,
              pe_type,
              market_cap,
              sector,
              industry,
              insider_action,
              insider_shares,
              insider_avg_price,
              insider_buy_value,
              cluster_buyers,
              cluster_shares,
              price_return_5d,
              price_return_20d,
              volume_ratio,
              breakout_20d,
              breakout_52w,
              above_50dma,
              trend_aligned,
              price_confirmed,
              relative_strength_20d,
              earnings_surprise_pct,
              revenue_growth_pct,
              guidance_flag,
              age_days,
              freshness_bucket,
              ticker_score_change_1d,
              ticker_score_change_7d,
              updated_at,
              created_at
            `)
            .order("app_score", { ascending: false })
            .limit(1000),

          supabase
            .from("raw_ptr_trades")
            .select(`
              ticker,
              amount_range
            `)
            .limit(5000),
        ])

        if (!isMounted) return

        if (candidateRes.error) {
          setError(candidateRes.error.message)
          setRows([])
          setLoading(false)
          return
        }

        if (signalRes.error) {
          setError(signalRes.error.message)
          setRows([])
          setLoading(false)
          return
        }

        if (ptrRes.error) {
          setError(ptrRes.error.message)
          setRows([])
          setLoading(false)
          return
        }

        const candidateRows = (candidateRes.data || []) as CandidateUniverseRow[]
        const signalRows = (signalRes.data || []) as TickerScoreRow[]
        const ptrRows = (ptrRes.data || []) as RawPtrTradeRow[]

        const candidateMap = new Map<string, CandidateUniverseRow>()
        const signalMap = new Map<string, TickerScoreRow>()
        const ptrMap = new Map<string, string>()

        for (const row of candidateRows) {
          const ticker = normalizeTicker(row.ticker)
          if (!ticker) continue
          candidateMap.set(ticker, row)
        }

        for (const row of signalRows) {
          const ticker = normalizeTicker(row.ticker)
          if (!ticker) continue
          signalMap.set(ticker, row)
        }

        for (const row of ptrRows) {
          const ticker = normalizeTicker(row.ticker)
          const amountRange =
            typeof row.amount_range === "string" ? row.amount_range.trim() : ""
          if (!ticker || !amountRange) continue
          if (!ptrMap.has(ticker)) {
            ptrMap.set(ticker, amountRange)
          }
        }

        const allTickers = new Set<string>([
          ...candidateMap.keys(),
          ...signalMap.keys(),
        ])

        const merged: UnifiedRow[] = []

        for (const ticker of allTickers) {
          const unified = makeUnifiedRow(
            candidateMap.get(ticker) ?? null,
            signalMap.get(ticker) ?? null,
            ptrMap.get(ticker) ?? null
          )
          if (!unified) continue

          const include =
            (unified.candidate_score ?? -1) >= 50 ||
            (unified.signal_score ?? -1) >= 50

          if (include) merged.push(unified)
        }

        merged.sort(compareRows)

        setRows(merged)
        setLoading(false)
      } catch (err: any) {
        if (!isMounted) return
        setError(err?.message || "Error loading today’s list.")
        setRows([])
        setLoading(false)
      }
    }

    loadData()

    // Subscribe to realtime updates on ticker_scores_current
    // When the pipeline finishes a cycle, scores update and we refresh automatically
    const channel = supabase
      .channel("board-refresh")
      .on(
        "postgres_changes",
        { event: "*", schema: "public", table: "ticker_scores_current" },
        () => {
          if (isMounted) {
            loadData()
          }
        }
      )
      .subscribe()

    return () => {
      isMounted = false
      supabase.removeChannel(channel)
    }
  }, [])

  useEffect(() => {
    setCardIndex(0)
  }, [priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter, insiderFilter, congressFilter])

  const sectorOptions = useMemo(() => {
    const sectors = Array.from(
      new Set(rows.map((row) => (row.sector || "").trim()).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b))

    return ["all", ...sectors]
  }, [rows])

  const filteredRows = useMemo(() => {
    return rows
      .filter((row) => matchesPriceFilter(row, priceFilter))
      .filter((row) => matchesPeFilter(row, peFilter))
      .filter((row) => matchesFreshnessFilter(row, freshnessFilter))
      .filter((row) => matchesScoreFilter(row, scoreFilter))
      .filter((row) => matchesSectorFilter(row, sectorFilter))
      .filter((row) => matchesSourceFilter(row, sourceFilter))
      .filter((row) => matchesInsiderFilter(row, insiderFilter))
      .filter((row) => matchesCongressFilter(row, congressFilter))
      .sort(compareRows)
  }, [rows, priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter, insiderFilter, congressFilter])

  const safeCardIndex =
    filteredRows.length === 0 ? 0 : Math.min(cardIndex, filteredRows.length - 1)

  const selectedRow = useMemo(() => {
    if (!selectedTicker) return null
    return filteredRows.find((row) => row.ticker === selectedTicker) ?? null
  }, [filteredRows, selectedTicker])

  const selectedIndex = useMemo(() => {
    if (!selectedTicker) return -1
    return filteredRows.findIndex((row) => row.ticker === selectedTicker)
  }, [filteredRows, selectedTicker])

  const lastUpdated = getLastUpdated(rows)
  const strongBuyCount = filteredRows.length
  const eliteCount = filteredRows.filter((row) => row.display_score >= 90).length

  const activeFilterCount = useMemo(() => {
    let count = 0
    if (priceFilter !== "all") count += 1
    if (peFilter !== "all") count += 1
    if (freshnessFilter !== "all") count += 1
    if (scoreFilter !== "all") count += 1
    if (sectorFilter !== "all") count += 1
    if (sourceFilter !== "all") count += 1
    if (insiderFilter !== "all") count += 1
    if (congressFilter !== "all") count += 1
    return count
  }, [priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter, insiderFilter, congressFilter])

  function openDetails(ticker: string, initialTab = 0) {
    setDetailInitialTab(initialTab)
    setSelectedTicker(ticker)
  }

  function closeDetails() {
    setSelectedTicker(null)
    setDetailInitialTab(0)
  }

  function goToPrevSelected() {
    if (selectedIndex <= 0) return
    setDetailInitialTab(0)
    setSelectedTicker(filteredRows[selectedIndex - 1]?.ticker ?? null)
    setCardIndex(selectedIndex - 1)
  }

  function goToNextSelected() {
    if (selectedIndex < 0 || selectedIndex >= filteredRows.length - 1) return
    setDetailInitialTab(0)
    setSelectedTicker(filteredRows[selectedIndex + 1]?.ticker ?? null)
    setCardIndex(selectedIndex + 1)
  }

  function resetFilters() {
    setPriceFilter("all")
    setPeFilter("all")
    setFreshnessFilter("all")
    setScoreFilter("all")
    setSectorFilter("all")
    setSourceFilter("all")
    setInsiderFilter("all")
    setCongressFilter("all")
    setSelectedTicker(null)
    setCardIndex(0)
    setFiltersOpen(false)
  }

  useEffect(() => {
    if (selectedTicker) return
    function handleKey(e: KeyboardEvent) {
      if (e.key === "ArrowRight")
        setCardIndex((i) => Math.min(i + 1, filteredRows.length - 1))
      if (e.key === "ArrowLeft") setCardIndex((i) => Math.max(i - 1, 0))
    }
    window.addEventListener("keydown", handleKey)
    return () => window.removeEventListener("keydown", handleKey)
  }, [selectedTicker, filteredRows.length])

  return (
    <main className="flex h-[100dvh] w-full flex-col overflow-hidden text-white" style={{ background: "#080d18" }}>
      <style jsx global>{`
        @keyframes cardFadeUp {
          from { opacity: 0; transform: translateY(16px) scale(0.985); }
          to { opacity: 1; transform: translateY(0) scale(1); }
        }
        @keyframes slideFromRight {
          from { opacity: 0; transform: translateX(52px) scale(0.97); }
          to { opacity: 1; transform: translateX(0) scale(1); }
        }
        @keyframes slideFromLeft {
          from { opacity: 0; transform: translateX(-52px) scale(0.97); }
          to { opacity: 1; transform: translateX(0) scale(1); }
        }
      `}</style>

      {/* Header */}
      <header className="shrink-0 border-b border-[rgba(255,255,255,0.07)] px-3 pb-2 pt-[env(safe-area-inset-top,8px)] lg:px-4 lg:py-3" style={{ background: "#080d18" }}>
        <div className="mx-auto flex max-w-lg items-center justify-between lg:max-w-7xl">
          <div>
            <p className="text-[10px] font-bold uppercase tracking-[0.26em] text-[#f0a500]">
              Market Signal Tracker
            </p>
            <p className="mt-0.5 flex items-center gap-2 text-sm font-semibold text-white">
              {loading ? (
                <span className="text-[#7a8ba0]">Loading…</span>
              ) : (
                <>
                  <span>{filteredRows.length} ideas</span>
                  {eliteCount > 0 && (
                    <span className="rounded-full bg-[rgba(240,165,0,0.12)] px-2 py-0.5 text-[10px] font-bold text-[#f0a500]">
                      {eliteCount} top tier
                    </span>
                  )}
                  {lastUpdated && (
                    <span className="text-[11px] font-normal text-[#7a8ba0]">
                      {lastUpdated}
                    </span>
                  )}
                </>
              )}
            </p>
          </div>

          <button
            type="button"
            onClick={() => setFiltersOpen((prev) => !prev)}
            aria-expanded={filtersOpen}
            className="relative inline-flex items-center gap-2 rounded-2xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-4 py-2.5 text-sm font-semibold text-white transition hover:border-[rgba(240,165,0,0.30)] hover:bg-[rgba(240,165,0,0.10)]"
          >
            <svg
              width="13"
              height="13"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.5"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <line x1="4" y1="6" x2="20" y2="6" />
              <line x1="8" y1="12" x2="16" y2="12" />
              <line x1="11" y1="18" x2="13" y2="18" />
            </svg>
            <span>Filters</span>
            {activeFilterCount > 0 && (
              <span className="absolute -right-1.5 -top-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-[#f0a500] text-[9px] font-bold text-black">
                {activeFilterCount}
              </span>
            )}
          </button>
        </div>
      </header>

      {/* Collapsible filters */}
      <div
        className={[
          "shrink-0 overflow-hidden border-b border-[rgba(255,255,255,0.07)] transition-all duration-300 ease-out",
          filtersOpen ? "max-h-[28rem] opacity-100" : "max-h-0 opacity-0",
        ].join(" ")}
        style={{ background: "#080d18" }}
      >
        <div className="mx-auto max-w-lg px-4 pb-5 pt-4 lg:max-w-7xl">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
            <FilterSelect
              label="Min score"
              value={scoreFilter}
              onChange={(v) => setScoreFilter(v as ScoreFilterType)}
              options={[
                { value: "all", label: "Any score" },
                { value: "70", label: "70+" },
                { value: "75", label: "75+" },
                { value: "80", label: "80+" },
                { value: "85", label: "85+" },
                { value: "90", label: "90+" },
              ]}
            />
            <FilterSelect
              label="Price"
              value={priceFilter}
              onChange={(v) => setPriceFilter(v as PriceFilterType)}
              options={[
                { value: "all", label: "All prices" },
                { value: "under10", label: "Under $10" },
                { value: "10to25", label: "$10–$25" },
                { value: "25to100", label: "$25–$100" },
                { value: "100plus", label: "$100+" },
              ]}
            />
            <FilterSelect
              label="How recent"
              value={freshnessFilter}
              onChange={(v) => setFreshnessFilter(v as FreshnessFilterType)}
              options={[
                { value: "all", label: "Any time" },
                { value: "today", label: "Today" },
                { value: "3d", label: "Last 3 days" },
                { value: "7d", label: "Last 7 days" },
                { value: "14d", label: "Last 14 days" },
              ]}
            />
            <FilterSelect
              label="Business area"
              value={sectorFilter}
              onChange={(v) => setSectorFilter(v)}
              options={sectorOptions.map((s) => ({
                value: s,
                label: s === "all" ? "All sectors" : s,
              }))}
            />
            <FilterSelect
              label="Valuation"
              value={peFilter}
              onChange={(v) => setPeFilter(v as PeFilterType)}
              options={[
                { value: "all", label: "Any P/E" },
                { value: "20", label: "P/E ≤ 20" },
                { value: "30", label: "P/E ≤ 30" },
                { value: "50", label: "P/E ≤ 50" },
              ]}
            />
            <FilterSelect
              label="Insider buys"
              value={insiderFilter}
              onChange={(v) => setInsiderFilter(v as InsiderFilterType)}
              options={[
                { value: "all", label: "All" },
                { value: "yes", label: "Insiders buying" },
                { value: "cluster", label: "Clusters only" },
              ]}
            />
            <FilterSelect
              label="Congress buys"
              value={congressFilter}
              onChange={(v) => setCongressFilter(v as CongressFilterType)}
              options={[
                { value: "all", label: "All" },
                { value: "yes", label: "Congress buying" },
              ]}
            />
            {activeFilterCount > 0 && (
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={resetFilters}
                  className="w-full rounded-2xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] py-2.5 text-sm font-semibold text-white transition hover:border-[rgba(240,165,0,0.25)] hover:bg-[rgba(240,165,0,0.08)]"
                >
                  Reset
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Main swipe area */}
      <div className="min-h-0 flex-1 overflow-hidden">
        {loading ? (
          <div className="flex h-full items-center justify-center">
            <LoadingPanel />
          </div>
        ) : error ? (
          <div className="flex h-full items-center justify-center px-4">
            <ErrorPanel message={error} />
          </div>
        ) : filteredRows.length === 0 ? (
          <div className="flex h-full items-center justify-center px-4">
            <EmptyPanel />
          </div>
        ) : (
          <SwipeDeck
            rows={filteredRows}
            cardIndex={safeCardIndex}
            onIndexChange={setCardIndex}
            onOpenDetails={(ticker) => openDetails(ticker, 0)}
          />
        )}
      </div>

      {/* Disclaimer */}
      {!loading && !error && (
        <div className="shrink-0 px-2.5 pb-1.5 pt-0.5 text-center text-[9px] leading-3 text-[#7a8ba0]">
          Not financial advice. Always do your own research before acting on any idea shown here.
        </div>
      )}

      {/* Detail modal */}
      {selectedRow ? (
        <SignalDetailsModal
          row={selectedRow}
          onClose={closeDetails}
          onPrev={selectedIndex > 0 ? goToPrevSelected : undefined}
          onNext={
            selectedIndex >= 0 && selectedIndex < filteredRows.length - 1
              ? goToNextSelected
              : undefined
          }
          positionLabel={
            selectedIndex >= 0 ? `${selectedIndex + 1} of ${filteredRows.length}` : null
          }
          initialTab={detailInitialTab}
        />
      ) : null}
    </main>
  )
}

function getDotRange(current: number, total: number, maxDots = 7): number[] {
  if (total <= maxDots) return Array.from({ length: total }, (_, i) => i)
  const half = Math.floor(maxDots / 2)
  let start = Math.max(0, current - half)
  const end = Math.min(total - 1, start + maxDots - 1)
  start = Math.max(0, end - maxDots + 1)
  return Array.from({ length: end - start + 1 }, (_, i) => start + i)
}

function SwipeDeck({
  rows,
  cardIndex,
  onIndexChange,
  onOpenDetails,
}: {
  rows: UnifiedRow[]
  cardIndex: number
  onIndexChange: (index: number) => void
  onOpenDetails: (ticker: string) => void
}) {
  const touchStartX = useRef<number | null>(null)
  const touchStartY = useRef<number | null>(null)
  const [swipeDir, setSwipeDir] = useState<"left" | "right">("right")
  const [animKey, setAnimKey] = useState(0)

  function goNext() {
    if (cardIndex >= rows.length - 1) return
    setSwipeDir("left")
    onIndexChange(cardIndex + 1)
    setAnimKey((k) => k + 1)
  }

  function goPrev() {
    if (cardIndex <= 0) return
    setSwipeDir("right")
    onIndexChange(cardIndex - 1)
    setAnimKey((k) => k + 1)
  }

  function handleTouchStart(e: React.TouchEvent) {
    touchStartX.current = e.touches[0].clientX
    touchStartY.current = e.touches[0].clientY
  }

  function handleTouchEnd(e: React.TouchEvent) {
    if (touchStartX.current === null) return
    const dx = e.changedTouches[0].clientX - touchStartX.current
    const dy = Math.abs(e.changedTouches[0].clientY - (touchStartY.current ?? 0))
    if (Math.abs(dx) > 48 && Math.abs(dx) > dy) {
      if (dx < 0) goNext()
      else goPrev()
    }
    touchStartX.current = null
    touchStartY.current = null
  }

  const row = rows[cardIndex]
  const hasPrev = cardIndex > 0
  const hasNext = cardIndex < rows.length - 1
  const dots = getDotRange(cardIndex, rows.length)

  const DESKTOP_PAGE_SIZE = 3
  const desktopPage = Math.floor(cardIndex / DESKTOP_PAGE_SIZE)
  const desktopTotalPages = Math.ceil(rows.length / DESKTOP_PAGE_SIZE)
  const desktopStart = desktopPage * DESKTOP_PAGE_SIZE
  const desktopCards = rows.slice(desktopStart, desktopStart + DESKTOP_PAGE_SIZE)

  function goNextPage() {
    if (desktopPage >= desktopTotalPages - 1) return
    const nextStart = (desktopPage + 1) * DESKTOP_PAGE_SIZE
    setSwipeDir("left")
    onIndexChange(nextStart)
    setAnimKey((k) => k + 1)
  }

  function goPrevPage() {
    if (desktopPage <= 0) return
    const prevStart = (desktopPage - 1) * DESKTOP_PAGE_SIZE
    setSwipeDir("right")
    onIndexChange(prevStart)
    setAnimKey((k) => k + 1)
  }

  return (
    <div className="flex h-full flex-col items-center px-2.5 pb-0 pt-1.5 lg:px-3 lg:pt-3">
      {/* Nav row — mobile: single card dots, desktop: page dots */}
      <div className="mb-1.5 flex w-full max-w-md shrink-0 items-center justify-between lg:mb-2.5 lg:max-w-7xl">
        {/* Prev button — mobile: single, desktop: page */}
        <button
          type="button"
          onClick={goPrev}
          disabled={!hasPrev}
          aria-label="Previous idea"
          className={[
            "flex h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition lg:hidden",
            hasPrev
              ? "border-[rgba(255,255,255,0.10)] bg-[#0f1729] text-white hover:bg-[#162038] active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ‹
        </button>
        <button
          type="button"
          onClick={goPrevPage}
          disabled={desktopPage <= 0}
          aria-label="Previous page"
          className={[
            "hidden h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition lg:flex",
            desktopPage > 0
              ? "border-[rgba(255,255,255,0.10)] bg-[#0f1729] text-white hover:bg-[#162038] active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ‹
        </button>

        {/* Dots — mobile: card dots, desktop: page dots */}
        <div className="flex items-center gap-1.5 lg:hidden">
          {dots.map((i) => (
            <button
              key={i}
              type="button"
              onClick={() => {
                setSwipeDir(i > cardIndex ? "left" : "right")
                onIndexChange(i)
                setAnimKey((k) => k + 1)
              }}
              aria-label={`Go to idea ${i + 1}`}
              className={[
                "rounded-full transition-all duration-200",
                i === cardIndex
                  ? "h-2 w-5 bg-[#f0a500]"
                  : "h-1.5 w-1.5 bg-[#1e2d45] hover:bg-[#2a3d55]",
              ].join(" ")}
            />
          ))}
          <span className="ml-1.5 text-[11px] text-[#7a8ba0]">
            {cardIndex + 1}/{rows.length}
          </span>
        </div>
        <div className="hidden items-center gap-1.5 lg:flex">
          {getDotRange(desktopPage, desktopTotalPages, 9).map((i) => (
            <button
              key={i}
              type="button"
              onClick={() => {
                setSwipeDir(i > desktopPage ? "left" : "right")
                onIndexChange(i * DESKTOP_PAGE_SIZE)
                setAnimKey((k) => k + 1)
              }}
              aria-label={`Page ${i + 1}`}
              className={[
                "rounded-full transition-all duration-200",
                i === desktopPage
                  ? "h-2 w-5 bg-[#f0a500]"
                  : "h-1.5 w-1.5 bg-[#1e2d45] hover:bg-[#2a3d55]",
              ].join(" ")}
            />
          ))}
          <span className="ml-1.5 text-[11px] text-[#7a8ba0]">
            Page {desktopPage + 1}/{desktopTotalPages}
          </span>
        </div>

        {/* Next button */}
        <button
          type="button"
          onClick={goNext}
          disabled={!hasNext}
          aria-label="Next idea"
          className={[
            "flex h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition lg:hidden",
            hasNext
              ? "border-[rgba(255,255,255,0.10)] bg-[#0f1729] text-white hover:bg-[#162038] active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ›
        </button>
        <button
          type="button"
          onClick={goNextPage}
          disabled={desktopPage >= desktopTotalPages - 1}
          aria-label="Next page"
          className={[
            "hidden h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition lg:flex",
            desktopPage < desktopTotalPages - 1
              ? "border-[rgba(255,255,255,0.10)] bg-[#0f1729] text-white hover:bg-[#162038] active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ›
        </button>
      </div>

      {/* Mobile: single swipeable card */}
      <div
        className="min-h-0 w-full max-w-md flex-1 overflow-hidden lg:hidden"
        style={{ touchAction: "pan-y" }}
        onTouchStart={handleTouchStart}
        onTouchEnd={handleTouchEnd}
      >
        <div
          key={`${row.ticker}-${animKey}`}
          className="h-full"
          style={{
            animation: `${swipeDir === "left" ? "slideFromRight" : "slideFromLeft"} 260ms ease-out both`,
          }}
        >
          <SwipeStockCard
            row={row}
            rank={cardIndex + 1}
            onOpen={() => onOpenDetails(row.ticker)}
          />
        </div>
      </div>

      {/* Desktop: 3-card grid */}
      <div
        key={`desktop-page-${desktopPage}-${animKey}`}
        className="hidden min-h-0 w-full max-w-7xl flex-1 gap-4 overflow-hidden lg:grid lg:grid-cols-3"
        style={{
          animation: `${swipeDir === "left" ? "slideFromRight" : "slideFromLeft"} 260ms ease-out both`,
        }}
      >
        {desktopCards.map((cardRow, i) => (
          <SwipeStockCard
            key={cardRow.ticker}
            row={cardRow}
            rank={desktopStart + i + 1}
            onOpen={() => onOpenDetails(cardRow.ticker)}
          />
        ))}
      </div>
    </div>
  )
}

function ScoreRing({ score, palette }: { score: number; palette: ReturnType<typeof getScorePalette> }) {
  const radius = 26
  const strokeWidth = 5
  const circumference = 2 * Math.PI * radius
  const dashOffset = circumference - (score / 100) * circumference
  return (
    <div className="relative flex items-center justify-center" style={{ width: 68, height: 68 }}>
      <svg width="68" height="68" viewBox="0 0 68 68" style={{ transform: "rotate(-90deg)" }}>
        <circle cx="34" cy="34" r={radius} fill="none" stroke="#1e2d45" strokeWidth={strokeWidth} />
        <circle
          cx="34" cy="34" r={radius} fill="none"
          stroke={palette.start}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={dashOffset}
          style={{ transition: "stroke-dashoffset 600ms ease-out" }}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-lg font-black leading-none text-white">{score}</span>
        <span className="text-[9px] leading-none text-[#7a8ba0] mt-0.5">/100</span>
      </div>
    </div>
  )
}

function SwipeStockCard({
  row,
  rank,
  onOpen,
}: {
  row: UnifiedRow
  rank: number
  onOpen: () => void
}) {
  const score = row.display_score
  const palette = getScorePalette(score)
  const whyBullets = getSimpleCardBullets(row)
  const ltcs = parseScreenReasonScores(row.screen_reason)

  const pillars = [
    { label: "Moat", score: ltcs.moat, icon: "🏔" },
    { label: "Balance", score: ltcs.financial, icon: "💪" },
    { label: "Profit", score: ltcs.profitability, icon: "💰" },
    { label: "Stability", score: ltcs.stability, icon: "🛡" },
    { label: "Valuation", score: ltcs.valuation, icon: "📊" },
  ]

  return (
    <div
      className="flex h-full flex-col overflow-hidden rounded-[1.75rem] border shadow-2xl [@media(orientation:landscape)]:overflow-y-auto"
      style={{ borderColor: "rgba(255,255,255,0.08)", background: "#0f1729" }}
    >
      {/* ── Header ── */}
      <div className="shrink-0 px-4 pt-4 pb-2">
        <div className="flex items-center gap-3">
          <ScoreRing score={score} palette={palette} />
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <h2 className="text-3xl font-black tracking-tight text-white">{row.ticker}</h2>
              <span className="text-base font-bold text-white/70">
                {row.price ? formatMoney(row.price) : ""}
              </span>
            </div>
            <p className="truncate text-xs text-white/40">
              {[row.company_name, row.sector].filter(Boolean).join(" · ")}
            </p>
          </div>
          <a
            href={`https://robinhood.com/stocks/${row.ticker}`}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            className="flex h-9 shrink-0 items-center gap-1.5 rounded-full border border-emerald-500/40 bg-emerald-500/15 px-3.5 text-xs font-bold text-emerald-400 transition hover:bg-emerald-500/25 active:scale-95"
          >
            Buy
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none" className="inline-block">
              <path d="M3 9L9 3M9 3H4M9 3V8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </a>
        </div>
      </div>

      {/* ── Returns strip ── */}
      <div className="shrink-0 px-4 pb-0">
        <div className="grid grid-cols-4 gap-1.5">
          {[
            { label: "1D", value: row.one_day_return },
            { label: "5D", value: row.price_return_5d },
            { label: "10D", value: row.return_10d },
            { label: "20D", value: row.price_return_20d },
          ].map(({ label, value }) => {
            const hasVal = value !== null && value !== undefined
            const isPos = hasVal && value! >= 0
            return (
              <div
                key={label}
                className="flex flex-col items-center justify-center rounded-lg border py-2"
                style={{
                  borderColor: hasVal
                    ? isPos ? "rgba(48,209,88,0.22)" : "rgba(255,69,58,0.22)"
                    : "rgba(255,255,255,0.06)",
                  background: hasVal
                    ? isPos ? "rgba(48,209,88,0.07)" : "rgba(255,69,58,0.07)"
                    : "#162038",
                }}
              >
                <span className="text-[9px] font-medium text-white/40">{label}</span>
                <span
                  className="mt-0.5 text-sm font-black"
                  style={{ color: hasVal ? (isPos ? "#4ade80" : "#f87171") : "#7a8ba0" }}
                >
                  {hasVal
                    ? `${isPos ? "+" : ""}${round1(value!)?.toFixed(1)}%`
                    : "—"}
                </span>
              </div>
            )
          })}
        </div>
      </div>

      {/* ── Fundamentals Scanner ── */}
      <div className="min-h-0 flex-1 overflow-hidden px-4 pt-2 pb-1">
        <div className="grid h-full grid-cols-2 grid-rows-3 gap-1.5">
          {(() => {
            const ltcs = parseScreenReasonScores(row.screen_reason)

            // Mini gauge bar component
            function Gauge({ value, max, color }: { value: number | null; max: number; color: string }) {
              const pct = value != null ? Math.min((value / max) * 100, 100) : 0
              return (
                <div className="mt-2 h-[5px] overflow-hidden rounded-full bg-[#1a2540]">
                  <div
                    className="h-full rounded-full transition-all duration-700"
                    style={{ width: `${pct}%`, background: `linear-gradient(90deg, ${color}90, ${color})`, boxShadow: `0 0 8px ${color}40` }}
                  />
                </div>
              )
            }

            function TileIcon({ d, color }: { d: string; color: string }) {
              return (
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none" className="inline-block shrink-0">
                  <path d={d} stroke={color} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              )
            }

            // SVG path data for each tile icon
            const iconPaths = {
              eps: "M2 9L4.5 4L7 6.5L10 2M10 2H7.5M10 2V4.5",         // trending up arrow
              cash: "M6 1V11M3 3.5H7.5C8.88 3.5 10 4.34 10 5.25S8.88 7 7.5 7H3M3 7H8C9.38 7 10.5 7.84 10.5 8.75S9.38 10.5 8 10.5H3", // dollar sign
              debt: "M1.5 10V4L6 1.5L10.5 4V10M4 10V7H8V10",           // building/bank
              moat: "M1 8L3 6L5 8L7 4L9 7L11 5M1 10H11",              // castle battlements
              peg: "M2 2V10H10M4 8V6M6.5 8V4M9 8V5",                  // bar chart
              rs: "M6 1L8.5 4H7V7.5H5V4H3.5L6 1M2 9.5H10",           // rocket/target up
            }

            function Tile({ label, iconPath, value, sub, score, maxScore, color, borderColor }: {
              label: string; iconPath: string; value: string; sub: string
              score: number | null; maxScore: number; color: string; borderColor: string
            }) {
              const active = score != null && score > 0
              return (
                <div className="relative overflow-hidden rounded-xl p-2.5" style={{
                  background: active ? `linear-gradient(160deg, ${color}18 0%, ${color}04 100%)` : "#111827",
                  border: `1px solid ${active ? borderColor : "rgba(255,255,255,0.05)"}`,
                }}>
                  {active && (
                    <div className="absolute top-0 right-0 h-8 w-8 opacity-20" style={{
                      background: `radial-gradient(circle at top right, ${color}, transparent 70%)`,
                    }} />
                  )}
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-1.5">
                      <TileIcon d={iconPath} color={active ? color : "#374151"} />
                      <p className="text-[10px] font-bold uppercase tracking-[0.14em]" style={{ color: active ? color : "#374151" }}>
                        {label}
                      </p>
                    </div>
                    {score != null && (
                      <span className="text-[11px] font-black" style={{ color }}>{score}</span>
                    )}
                  </div>
                  <p className="mt-0.5 text-lg font-black leading-tight" style={{ color: active ? "#f0f0f0" : "#1f2937" }}>
                    {value}
                  </p>
                  <Gauge value={score} max={maxScore} color={color} />
                  <p className="mt-1 text-[9px] leading-tight" style={{ color: active ? `${color}99` : "#1f2937" }}>
                    {sub}
                  </p>
                </div>
              )
            }

            // Earnings / Profitability
            const profScore = ltcs.profitability
            const profLabel = profScore == null ? "—" : profScore >= 75 ? "Strong" : profScore >= 40 ? "Growing" : "Weak"

            // Free Cash Flow (part of profitability)
            const fcfScore = ltcs.profitability
            const fcfLabel = fcfScore == null ? "—" : fcfScore >= 65 ? "Positive" : fcfScore >= 35 ? "Mixed" : "Negative"

            // Debt / Financial Health
            const debtScore = ltcs.financial
            const debtLabel = debtScore == null ? "—" : debtScore >= 70 ? "Low Debt" : debtScore >= 40 ? "Moderate" : "High Debt"

            // Moat / Competitive Advantage
            const moatScore = ltcs.moat
            const moatLabel = moatScore == null ? "—" : moatScore >= 75 ? "Wide" : moatScore >= 50 ? "Narrow" : "None"

            // PEG / Valuation
            const valScore = ltcs.valuation
            const valLabel = valScore == null ? "—" : valScore >= 70 ? "Attractive" : valScore >= 35 ? "Fair" : "Expensive"

            // Relative Strength / Near Highs — fall back to 20d return or 1d return
            const rs = row.relative_strength_20d != null ? Number(row.relative_strength_20d)
              : row.price_return_20d != null ? Number(row.price_return_20d)
              : row.one_day_return != null ? Number(row.one_day_return)
              : null
            // Score: map rs to 0-100 where 50 = neutral, >50 = positive momentum, <50 = negative
            const rsScore = rs != null ? Math.min(Math.max(Math.round(50 + rs * 2), 5), 100) : null
            const rsLabel = rs == null ? "—" : rs >= 15 ? "Near Highs" : rs >= 8 ? "Strong" : rs >= 0 ? "Neutral" : rs >= -5 ? "Cooling" : "Weak"

            return (
              <>
                <Tile
                  iconPath={iconPaths.eps} label="Profit Growth" value={profLabel}
                  sub="Is the company making more money each year?"
                  score={profScore} maxScore={100} color="#22d3ee" borderColor="rgba(34,211,238,0.25)"
                />
                <Tile
                  iconPath={iconPaths.cash} label="Money In" value={fcfLabel}
                  sub="Does it generate real cash, not just paper profit?"
                  score={fcfScore} maxScore={100} color="#34d399" borderColor="rgba(52,211,153,0.25)"
                />
                <Tile
                  iconPath={iconPaths.debt} label="Financial Health" value={debtLabel}
                  sub="Can it pay its bills without borrowing more?"
                  score={debtScore} maxScore={100} color="#a78bfa" borderColor="rgba(167,139,250,0.25)"
                />
                <Tile
                  iconPath={iconPaths.moat} label="Edge" value={moatLabel}
                  sub="How hard is it for competitors to catch up?"
                  score={moatScore} maxScore={100} color="#f59e0b" borderColor="rgba(245,158,11,0.25)"
                />
                <Tile
                  iconPath={iconPaths.peg} label="Fair Price" value={valLabel}
                  sub="Is the stock priced right for how fast it's growing?"
                  score={valScore} maxScore={100} color="#fb923c" borderColor="rgba(251,146,60,0.25)"
                />
                <Tile
                  iconPath={iconPaths.rs} label="Momentum" value={rsLabel}
                  sub={rs != null ? `${rs >= 0 ? "+" : ""}${rs.toFixed(1)}% vs market over 20 days` : "Is the stock outperforming most others?"}
                  score={rsScore} maxScore={100} color="#ec4899" borderColor="rgba(236,72,153,0.25)"
                />
              </>
            )
          })()}
        </div>
      </div>

      {/* ── CTA ── */}
      <div className="shrink-0 px-4 pt-3 pb-3">
        <button
          type="button"
          onClick={onOpen}
          className="w-full rounded-2xl bg-[#f0a500] px-5 py-3 text-sm font-bold text-black transition active:scale-[0.98] hover:bg-[#ffb733]"
        >
          View Analysis →
        </button>
      </div>
    </div>
  )
}

function SimpleExplainerCard({
  step,
  title,
  body,
}: {
  step: string
  title: string
  body: string
}) {
  return (
    <div className="rounded-[1.25rem] border border-[rgba(255,255,255,0.07)] bg-[#141414] p-4">
      <div className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-[rgba(0,200,5,0.20)] bg-[rgba(0,200,5,0.10)] text-sm font-bold text-[#00c805]">
        {step}
      </div>
      <h3 className="mt-3 text-base font-semibold text-white">{title}</h3>
      <p className="mt-2 text-sm leading-6 text-[#b0b0b0]">{body}</p>
    </div>
  )
}

function ConfidenceLegendCard({
  color,
  label,
  body,
}: {
  color: string
  label: string
  body: string
}) {
  return (
    <div className="rounded-[1.25rem] border border-[rgba(255,255,255,0.07)] bg-[#141414] p-4">
      <div className="flex items-center gap-3">
        <span className={`h-3 w-3 rounded-full ${color}`} />
        <span className="text-sm font-semibold text-white">{label}</span>
      </div>
      <p className="mt-2 text-sm leading-6 text-[#b0b0b0]">{body}</p>
    </div>
  )
}

function CompactStatCard({
  label,
  value,
  tone,
}: {
  label: string
  value: string
  tone: "emerald" | "cyan"
}) {
  const styles = "border-[rgba(0,200,5,0.20)] bg-[rgba(0,200,5,0.08)] text-[#00c805]"

  return (
    <div className={`rounded-[1.25rem] border px-3 py-3 sm:px-4 ${styles}`}>
      <p className="text-[10px] font-semibold uppercase tracking-[0.16em] opacity-80">
        {label}
      </p>
      <p className="mt-2 text-xl font-bold leading-none tracking-tight text-white sm:text-2xl">
        {value}
      </p>
    </div>
  )
}

function formatPriceConfirmation(row: UnifiedRow) {
  if (row.price_confirmed === true) return "Confirmed"
  if (row.price_confirmed === false) return "Not confirmed"

  const hasBreakout = row.breakout_20d === true || row.breakout_52w === true
  const hasVolumeSupport = (row.volume_ratio ?? 0) >= 1.2
  const hasRelativeStrength = (row.relative_strength_20d ?? 0) > 0
  const hasTrendSupport =
    row.trend_aligned === true ||
    row.above_sma_20 === true ||
    row.above_50dma === true

  if (hasBreakout && (hasVolumeSupport || hasRelativeStrength || hasTrendSupport)) {
    return "Confirmed"
  }

  if (!hasBreakout && !hasVolumeSupport && !hasRelativeStrength && !hasTrendSupport) {
    return "Not confirmed"
  }

  return "Developing"
}

function MobileAppNav({
  onGoTop,
  onGoBoard,
  onGoFilters,
}: {
  onGoTop: () => void
  onGoBoard: () => void
  onGoFilters: () => void
}) {
  return (
    <div
      className="fixed inset-x-0 bottom-0 z-40 border-t border-[rgba(255,255,255,0.06)] bg-black px-3 pt-3 sm:hidden"
      style={{ paddingBottom: "calc(env(safe-area-inset-bottom) + 12px)" }}
    >
      <div className="mx-auto grid max-w-xl grid-cols-3 gap-2">
        <button
          type="button"
          onClick={onGoTop}
          className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-3 text-xs font-semibold text-white"
        >
          Home
        </button>
        <button
          type="button"
          onClick={onGoBoard}
          className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-3 text-xs font-semibold text-white"
        >
          Board
        </button>
        <button
          type="button"
          onClick={onGoFilters}
          className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-3 text-xs font-semibold text-white"
        >
          Filters
        </button>
      </div>
    </div>
  )
}

function FilterSelect({
  label,
  value,
  onChange,
  options,
}: {
  label: string
  value: string
  onChange: (value: string) => void
  options: Array<{ value: string; label: string }>
}) {
  return (
    <div>
      <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-[#7a8ba0]">
        {label}
      </label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-2xl border border-[rgba(255,255,255,0.08)] bg-[#0f1729] px-4 py-3.5 text-white outline-none transition focus:border-[rgba(240,165,0,0.40)] sm:py-4"
      >
        {options.map((option) => (
          <option
            key={`${label}-${option.value}`}
            value={option.value}
            className="bg-[#0f1729]"
          >
            {option.label}
          </option>
        ))}
      </select>
    </div>
  )
}

function LoadingPanel() {
  return (
    <div className="rounded-3xl border border-[rgba(255,255,255,0.07)] bg-[#0f1729] p-8 shadow-2xl">
      <h2 className="text-2xl font-semibold">Loading today’s ideas…</h2>
      <p className="mt-2 text-[#7a8ba0]">Pulling the board together now.</p>
    </div>
  )
}

function ErrorPanel({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-[rgba(255,69,58,0.25)] bg-[rgba(255,69,58,0.10)] p-4 text-[#ff453a]">
      {message}
    </div>
  )
}

function EmptyPanel() {
  return (
    <div className="rounded-3xl border border-[rgba(255,255,255,0.07)] bg-[#0f1729] p-8 shadow-2xl">
      <h2 className="text-2xl font-semibold">Nothing matched those filters</h2>
      <p className="mt-2 text-[#7a8ba0]">
        Try widening the score, freshness, or valuation filters.
      </p>
    </div>
  )
}

function compareRows(a: UnifiedRow, b: UnifiedRow) {
  if (a.display_score !== b.display_score) return b.display_score - a.display_score

  const aSignal = a.signal_score ?? -1
  const bSignal = b.signal_score ?? -1
  if (aSignal !== bSignal) return bSignal - aSignal

  const aCandidate = a.candidate_score ?? -1
  const bCandidate = b.candidate_score ?? -1
  if (aCandidate !== bCandidate) return bCandidate - aCandidate

  const aDate = getDateValue(a.filed_at ?? a.last_screened_at ?? a.updated_at)
  const bDate = getDateValue(b.filed_at ?? b.last_screened_at ?? b.updated_at)
  return bDate - aDate
}

function matchesPriceFilter(row: UnifiedRow, priceFilter: PriceFilterType) {
  if (priceFilter === "all") return true

  const price = row.price
  if (price === null || price === undefined) return false

  if (priceFilter === "under10") return price < 10
  if (priceFilter === "10to25") return price >= 10 && price < 25
  if (priceFilter === "25to100") return price >= 25 && price < 100
  if (priceFilter === "100plus") return price >= 100

  return true
}

function matchesPeFilter(row: UnifiedRow, peFilter: PeFilterType) {
  if (peFilter === "all") return true

  const pe = row.pe_ratio ?? row.pe_forward ?? null
  if (pe === null || pe === undefined) return true

  const maxPe = Number(peFilter)
  return pe <= maxPe
}

function matchesFreshnessFilter(row: UnifiedRow, freshnessFilter: FreshnessFilterType) {
  if (freshnessFilter === "all") return true

  let age = row.age_days

  if ((age === null || age === undefined) && row.last_screened_at) {
    const timestamp = new Date(row.last_screened_at).getTime()
    if (!Number.isNaN(timestamp)) {
      age = Math.max(
        0,
        Math.floor((Date.now() - timestamp) / (24 * 60 * 60 * 1000))
      )
    }
  }

  if (age === null || age === undefined) {
    return false
  }

  if (freshnessFilter === "today") return age <= 0
  if (freshnessFilter === "3d") return age <= 3
  if (freshnessFilter === "7d") return age <= 7
  if (freshnessFilter === "14d") return age <= 14

  return true
}

function matchesScoreFilter(row: UnifiedRow, scoreFilter: ScoreFilterType) {
  if (scoreFilter === "all") return true
  return row.display_score >= Number(scoreFilter)
}

function matchesSectorFilter(row: UnifiedRow, sectorFilter: SectorFilterType) {
  if (sectorFilter === "all") return true
  return (row.sector || "").trim() === sectorFilter
}

function matchesSourceFilter(row: UnifiedRow, sourceFilter: SourceFilterType) {
  if (sourceFilter === "all") return true
  if (sourceFilter === "both") return row.has_candidate_data && row.has_signal_data
  if (sourceFilter === "technical_only")
    return row.has_candidate_data && !row.has_signal_data
  if (sourceFilter === "filing_only")
    return !row.has_candidate_data && row.has_signal_data
  return true
}

function matchesInsiderFilter(row: UnifiedRow, filter: InsiderFilterType) {
  if (filter === "all") return true
  if (filter === "cluster") return (row.cluster_buyers ?? 0) >= 2
  return row.has_insider_trades === true
}

function matchesCongressFilter(row: UnifiedRow, filter: CongressFilterType) {
  if (filter === "all") return true
  return row.has_ptr_forms === true || !!row.ptr_amount
}

function getLastUpdated(rows: UnifiedRow[]) {
  const dates = rows
    .map((row) => row.score_updated_at || row.updated_at || row.last_screened_at)
    .filter((v): v is string => Boolean(v))
    .map((v) => new Date(v))
    .filter((d) => !Number.isNaN(d.getTime()))
    .sort((a, b) => b.getTime() - a.getTime())

  if (!dates.length) return null

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(dates[0])
}

function getDateValue(dateString: string | null | undefined) {
  if (!dateString) return 0
  const timestamp = new Date(dateString).getTime()
  return Number.isNaN(timestamp) ? 0 : timestamp
}

function getRowKey(row: UnifiedRow, index: number) {
  const accessionKey =
    row.accession_nos?.join("-") ||
    row.filed_at ||
    row.last_screened_at ||
    row.updated_at ||
    String(index)

  return `${row.ticker}-${accessionKey}-${index}`
}

function humanizeSignalLabel(tag: string): string {
  const map: Record<string, string> = {
    "ptr-cluster-bonus": "Multiple politicians bought this",
    "ptr-strong-buying-bonus": "Strong political buying interest",
    "ptr-plus-insider-bonus": "Politicians + insiders buying together",
    "ptr-plus-ownership-bonus": "Politicians buying a large stakeholder's stock",
    "ptr-selling-headwind": "Some political selling recently",
    "single-family-penalty": "Signal from only one category",
    "single-family-cap": "Limited to one signal type",
    "cluster-conviction-exemption": "Strong insider cluster buy",
    "limited-evidence-cap": "Few supporting data points",
    "broad-confirmation-cap": "Needs more confirming signals",
    "sharp-move-penalty": "Stock moved sharply — may be chasing",
    "crowded-move-penalty": "Large recent move without strong conviction",
    "volume-spike-penalty": "Unusual volume spike without clear catalyst",
    "sector-crowding-penalty": "Many stocks in this sector are signaling",
    "sector-crowding-warning": "Busy sector — may dilute opportunity",
    "industry-crowding-penalty": "Many stocks in this industry are signaling",
    "industry-crowding-warning": "Busy industry group",
    "score-declining": "Score has been declining recently",
    "ptr-priority": "Congressional trading activity present",
    "ptr-strong-buying": "Strong political buying",
    "ptr-buy-cluster": "Multiple politicians buying same stock",
    "breakout-20d": "Broke out to a 20-day high",
    "breakout-10d": "Broke out to a 10-day high",
    "above-sma20": "Trading above its 20-day average",
    "volume-confirmed": "Volume backs up the move",
    "relative-strength": "Outperforming the broader market",
    "insider-filing": "Insider filed a trade report",
    "ownership-filing": "Large ownership stake reported",
  }
  return map[tag] || tag
}

function TopSignalCard({
  row,
  onClick,
  isSelected,
  rank,
  animationIndex = 0,
}: {
  row: UnifiedRow
  onClick: () => void
  isSelected: boolean
  rank: number
  animationIndex?: number
}) {
  const score = row.display_score
  const palette = getScorePalette(score)
  const whyBullets = getSimpleCardBullets(row)
  const takeawayBullets = getPremiumSummaryBullets(row)

  const [showCompanyInfo, setShowCompanyInfo] = useState(false)
  const cardRef = useRef<HTMLButtonElement | null>(null)

  const hasRealCompanyInfo = Boolean(row.business_description?.trim())

  const infoTitle = hasRealCompanyInfo ? "About the company" : "Why this showed up"

  const infoBody = hasRealCompanyInfo
    ? row.business_description!.trim()
    : (row.primary_summary || "").trim() ||
      `${row.company_name || row.ticker} is on today’s board because enough good things are lining up right now.`

  const metricItems: MiniMetricItem[] = [
    { label: "Price", value: formatMoney(row.price) },
    { label: "Insider value", value: formatInsiderValue(row) || "—" },
    { label: "Vs market", value: formatRelativeStrengthForDisplay(row) },
    { label: "PTR amount", value: row.ptr_amount || "—" },
  ].filter((item) => hasDisplayValue(item.value))

  return (
    <button
      ref={cardRef}
      type="button"
      onClick={onClick}
      className={[
        "flex w-full self-start flex-col overflow-hidden rounded-[1.5rem] border p-4 text-left shadow-xl transition duration-300 hover:-translate-y-1 hover:scale-[1.01] sm:p-5",
        isSelected ? "ring-2 ring-[rgba(0,200,5,0.25)]" : "hover:ring-1 hover:ring-[rgba(255,255,255,0.06)]",
      ].join(" ")}
      style={{
        borderColor: isSelected ? "rgba(0,200,5,0.30)" : "rgba(255,255,255,0.08)",
        background: "#141414",
        animation: `cardFadeUp 480ms ease-out both`,
        animationDelay: `${animationIndex * 45}ms`,
      }}
    >
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <CardRankBadge rank={rank} />
          </div>

          <h3 className="mt-2 truncate text-2xl font-bold sm:text-3xl">{row.ticker}</h3>

          {row.company_name ? (
            <p className="mt-1 truncate text-sm text-[#8a8a8a]">
              {truncateText(row.company_name, 40)}
            </p>
          ) : null}

          <div className="mt-3">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation()
                setShowCompanyInfo((prev) => !prev)
              }}
              className="inline-flex items-center rounded-full border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-1 text-[11px] font-semibold text-[#8a8a8a] transition hover:border-[rgba(0,200,5,0.25)] hover:bg-[rgba(0,200,5,0.08)] hover:text-[#00c805]"
            >
              {showCompanyInfo
                ? "Hide details"
                : hasRealCompanyInfo
                  ? "About the company"
                  : "More context"}
            </button>
          </div>
        </div>

        <div className="flex shrink-0 flex-col items-end gap-2">
          <ScoreBadge row={row} />
          <FreshnessBadge row={row} />
        </div>
      </div>

      <div
        className={[
          "grid transition-all duration-300 ease-out",
          showCompanyInfo ? "mb-4 grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0",
        ].join(" ")}
      >
        <div className="overflow-hidden">
          <div className="w-full rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-4 py-4">
            <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-[#8a8a8a]">
              {infoTitle}
            </p>

            <p className="text-sm leading-6 text-[#b0b0b0]">{infoBody}</p>

            <div className="mt-4 flex justify-end">
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation()
                  setShowCompanyInfo(false)

                  setTimeout(() => {
                    cardRef.current?.scrollIntoView({
                      behavior: "smooth",
                      block: "start",
                    })
                  }, 150)
                }}
                className="rounded-full border border-[rgba(255,255,255,0.07)] bg-[#141414] px-4 py-1.5 text-xs font-semibold text-[#8a8a8a] transition hover:border-[rgba(0,200,5,0.25)] hover:bg-[rgba(0,200,5,0.08)] hover:text-[#00c805]"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="mb-4">
        <ScoreBar row={row} compact />
      </div>

      <div className="mb-4 rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#8a8a8a]">
          Why it made the list
        </p>

        <ul className="mt-3 space-y-2 text-sm leading-6 text-white">
          {whyBullets.map((item, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-[6px] h-1.5 w-1.5 rounded-full bg-[#00c805]" />
              <span>{item}</span>
            </li>
          ))}
        </ul>
      </div>

      {!!metricItems.length && (
        <div className="mb-4 grid grid-cols-2 gap-3 auto-rows-fr">
          {metricItems.map((item) => (
            <MiniMetric key={item.label} label={item.label} value={item.value} />
          ))}
        </div>
      )}

      <div className="mt-auto rounded-2xl bg-[#1c1c1c] p-4">
        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-[#00c805]">
          Plain-English takeaway
        </p>

        <ul className="space-y-2 text-sm leading-6 text-white">
          {takeawayBullets.map((item, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-[6px] h-1.5 w-1.5 rounded-full bg-[#00c805]" />
              <span>{item}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="mt-4 flex items-center justify-between rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-4 py-3">
        <span className="text-sm text-[#8a8a8a]">Open guided details</span>

        <span className="rounded-full border border-[rgba(255,255,255,0.07)] bg-[#141414] px-3 py-1 text-xs font-semibold text-white">
          Explore →
        </span>
      </div>
    </button>
  )
}

function ScoreBar({
  row,
  compact = false,
}: {
  row: UnifiedRow
  compact?: boolean
}) {
  const score = row.display_score

  return (
    <div className="w-full rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#0f1729] p-4">
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#7a8ba0]">
          Overall score
        </p>
        <p className="shrink-0 text-sm font-semibold text-white">{score}/100</p>
      </div>

      <div className="h-3 overflow-hidden rounded-full bg-[#1e2d45]">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{
            width: `${score}%`,
            background: "#f0a500",
          }}
        />
      </div>

      {!compact ? (
        <div className="mt-2 flex items-center justify-between text-[11px] uppercase tracking-[0.14em] text-[#7a8ba0]">
          <span>Good</span>
          <span>Strong</span>
          <span>Top tier</span>
        </div>
      ) : null}
    </div>
  )
}

function MiniMetric({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div className="flex min-h-[88px] w-full flex-col items-center justify-center rounded-[1.15rem] border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-3 text-center sm:min-h-[94px] sm:px-4">
      <p className="mb-2 break-words text-[10px] uppercase tracking-[0.22em] text-[#8a8a8a] sm:text-[11px]">
        {label}
      </p>
      <p className="break-words text-lg font-semibold tracking-tight text-white sm:text-xl">
        {value}
      </p>
    </div>
  )
}

function CardRankBadge({ rank }: { rank: number }) {
  return (
    <span className="inline-flex items-center rounded-full bg-[rgba(240,165,0,0.12)] px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
      #{rank}
    </span>
  )
}

function ScoreBadge({
  row,
  large = false,
}: {
  row: UnifiedRow
  large?: boolean
}) {
  const score = row.display_score

  return (
    <div
      className={[
        "inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full font-bold",
        large ? "px-3.5 py-1.5 text-sm sm:px-4 sm:py-2" : "px-3 py-1 text-sm",
      ].join(" ")}
      style={{
        background: "#162038",
        color: "#ffffff",
      }}
    >
      <span>{score}</span>
      {row.ticker_score_change_7d !== null && row.ticker_score_change_7d !== undefined && (
        <span className="text-xs text-[#7a8ba0]">
          {row.ticker_score_change_7d >= 3 ? "↑" : row.ticker_score_change_7d <= -3 ? "↓" : ""}
        </span>
      )}
    </div>
  )
}

function FreshnessBadge({ row }: { row: UnifiedRow }) {
  const label = getFreshnessLabel(row)
  if (!label) return null

  return (
    <span className="inline-flex shrink-0 items-center whitespace-nowrap rounded-full border border-[rgba(255,255,255,0.08)] bg-[#0f1729] px-2.5 py-1 text-[11px] font-semibold text-[#7a8ba0]">
      {label}
    </span>
  )
}

function PaginationControls({
  currentPage,
  totalPages,
  onPageChange,
}: {
  currentPage: number
  totalPages: number
  onPageChange: (page: number) => void
}) {
  const pages = buildPaginationPages(currentPage, totalPages)

  return (
    <div className="mt-8 flex flex-col items-center gap-4 sm:flex-row sm:justify-between">
      <div className="text-sm text-[#8a8a8a]">
        Page <span className="font-semibold text-white">{currentPage}</span> of{" "}
        <span className="font-semibold text-white">{totalPages}</span>
      </div>

      <div className="flex flex-wrap items-center justify-center gap-2">
        <button
          type="button"
          onClick={() => onPageChange(1)}
          disabled={currentPage === 1}
          className="rounded-xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-2 text-sm font-semibold text-[#8a8a8a] transition hover:border-[rgba(255,255,255,0.12)] hover:bg-[#2a2a2a] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          First
        </button>

        <button
          type="button"
          onClick={() => onPageChange(currentPage - 1)}
          disabled={currentPage === 1}
          className="rounded-xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-2 text-sm font-semibold text-[#8a8a8a] transition hover:border-[rgba(255,255,255,0.12)] hover:bg-[#2a2a2a] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Prev
        </button>

        {pages.map((page, index) =>
          page === "ellipsis" ? (
            <span key={`ellipsis-${index}`} className="px-2 text-[#666]">
              …
            </span>
          ) : (
            <button
              key={page}
              type="button"
              onClick={() => onPageChange(page)}
              className={[
                "min-w-[42px] rounded-xl border px-3 py-2 text-sm font-semibold transition",
                page === currentPage
                  ? "border-[rgba(0,200,5,0.30)] bg-[rgba(0,200,5,0.12)] text-white"
                  : "border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] text-[#8a8a8a] hover:border-[rgba(255,255,255,0.12)] hover:bg-[#2a2a2a] hover:text-white",
              ].join(" ")}
            >
              {page}
            </button>
          )
        )}

        <button
          type="button"
          onClick={() => onPageChange(currentPage + 1)}
          disabled={currentPage === totalPages}
          className="rounded-xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-2 text-sm font-semibold text-[#8a8a8a] transition hover:border-[rgba(255,255,255,0.12)] hover:bg-[#2a2a2a] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Next
        </button>

        <button
          type="button"
          onClick={() => onPageChange(totalPages)}
          disabled={currentPage === totalPages}
          className="rounded-xl border border-[rgba(255,255,255,0.07)] bg-[#1c1c1c] px-3 py-2 text-sm font-semibold text-[#8a8a8a] transition hover:border-[rgba(255,255,255,0.12)] hover:bg-[#2a2a2a] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Last
        </button>
      </div>
    </div>
  )
}

function buildPaginationPages(
  currentPage: number,
  totalPages: number
): Array<number | "ellipsis"> {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, i) => i + 1)
  }

  if (currentPage <= 4) {
    return [1, 2, 3, 4, 5, "ellipsis", totalPages]
  }

  if (currentPage >= totalPages - 3) {
    return [
      1,
      "ellipsis",
      totalPages - 4,
      totalPages - 3,
      totalPages - 2,
      totalPages - 1,
      totalPages,
    ]
  }

  return [1, "ellipsis", currentPage - 1, currentPage, currentPage + 1, "ellipsis", totalPages]
}

function SignalDetailsModal({
  row,
  onClose,
  onPrev,
  onNext,
  positionLabel,
  initialTab = 0,
}: {
  row: UnifiedRow
  onClose: () => void
  onPrev?: () => void
  onNext?: () => void
  positionLabel?: string | null
  initialTab?: number
}) {
  const reasons = getTopReasonLines(row)
  const tags = normalizeTags(row.signal_tags)
  const thesis = getFeaturedThesis(row)
  const confidenceBullets = getConfidenceBullets(row)
  const setupBullets = getSimpleSetupBullets(row)

  const [activeSlide, setActiveSlide] = useState(initialTab)
  const touchStartX = useRef<number | null>(null)
  const touchStartY = useRef<number | null>(null)

  useEffect(() => {
    setActiveSlide(initialTab)
  }, [row.ticker, initialTab])

  useEffect(() => {
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = "hidden"

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose()
      if (event.key === "ArrowLeft" && onPrev) onPrev()
      if (event.key === "ArrowRight" && onNext) onNext()
    }

    window.addEventListener("keydown", onKeyDown)

    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener("keydown", onKeyDown)
    }
  }, [onClose, onPrev, onNext])

  function handleTouchStart(e: React.TouchEvent<HTMLDivElement>) {
    touchStartX.current = e.touches[0]?.clientX ?? null
    touchStartY.current = e.touches[0]?.clientY ?? null
  }

  function handleTouchEnd(e: React.TouchEvent<HTMLDivElement>) {
    if (touchStartX.current === null || touchStartY.current === null) return

    const endX = e.changedTouches[0]?.clientX ?? 0
    const endY = e.changedTouches[0]?.clientY ?? 0
    const deltaX = endX - touchStartX.current
    const deltaY = endY - touchStartY.current

    if (Math.abs(deltaX) > 50 && Math.abs(deltaX) > Math.abs(deltaY) * 1.2) {
      if (deltaX < 0 && activeSlide < DETAIL_TABS.length - 1) {
        setActiveSlide((prev) => prev + 1)
      } else if (deltaX > 0 && activeSlide > 0) {
        setActiveSlide((prev) => prev - 1)
      }
    }

    touchStartX.current = null
    touchStartY.current = null
  }

  return (
    <div className="fixed inset-0 z-50 bg-black/85" onClick={onClose}>
      <div
        className="fixed inset-0 flex items-stretch justify-center p-0 sm:items-center sm:p-6"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex h-screen min-h-screen w-screen max-w-none flex-col overflow-hidden rounded-none border-0 shadow-2xl sm:h-[92vh] sm:min-h-0 sm:w-full sm:max-w-6xl sm:rounded-[2rem] sm:border sm:border-[rgba(255,255,255,0.07)]" style={{ background: "#080d18" }}>
          <div className="sticky top-0 z-20 border-b border-[rgba(255,255,255,0.07)]" style={{ background: "#080d18" }}>
            <div className="flex items-center justify-between gap-3 px-4 py-4 sm:px-6">
              <div className="flex min-w-0 items-center gap-2">
                <button
                  type="button"
                  onClick={onClose}
                  className="inline-flex shrink-0 items-center rounded-xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-3 py-2 text-sm font-semibold text-white transition hover:border-[rgba(255,255,255,0.15)] hover:bg-[#162038]"
                >
                  ← Back
                </button>

                {onPrev ? (
                  <button
                    type="button"
                    onClick={onPrev}
                    className="hidden shrink-0 rounded-xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-3 py-2 text-sm font-semibold text-white transition hover:border-[rgba(255,255,255,0.15)] hover:bg-[#162038] sm:inline-flex"
                  >
                    Prev
                  </button>
                ) : null}

                {onNext ? (
                  <button
                    type="button"
                    onClick={onNext}
                    className="hidden shrink-0 rounded-xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-3 py-2 text-sm font-semibold text-white transition hover:border-[rgba(255,255,255,0.15)] hover:bg-[#162038] sm:inline-flex"
                  >
                    Next
                  </button>
                ) : null}
              </div>

              <div className="min-w-0 text-right">
                {positionLabel ? (
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#7a8ba0]">
                    {positionLabel}
                  </p>
                ) : null}
                <p className="text-sm font-semibold text-white">Guided details</p>
              </div>
            </div>

            <div className="border-t border-[rgba(255,255,255,0.07)] px-4 py-4 sm:px-6">
              <div className="flex flex-wrap items-center gap-2">
                <h2 className="text-2xl font-bold sm:text-3xl">{row.ticker}</h2>
                <ScoreBadge row={row} large />
                <FreshnessBadge row={row} />
              </div>

              {row.company_name ? (
                <p className="mt-2 truncate text-sm text-[#7a8ba0]">{row.company_name}</p>
              ) : null}
            </div>

            <div className="border-t border-[rgba(255,255,255,0.07)] px-4 py-3 lg:hidden">
              <div className="mb-3 flex items-center justify-center gap-2">
                {DETAIL_TABS.map((slide, index) => (
                  <button
                    key={slide}
                    type="button"
                    onClick={() => setActiveSlide(index)}
                    className={[
                      "rounded-full px-3 py-1.5 text-xs font-semibold transition",
                      index === activeSlide
                        ? "bg-[rgba(240,165,0,0.15)] text-[#f0a500] ring-1 ring-[rgba(240,165,0,0.30)]"
                        : "bg-[#0f1729] text-[#7a8ba0] ring-1 ring-[rgba(255,255,255,0.08)]",
                    ].join(" ")}
                  >
                    {slide}
                  </button>
                ))}
              </div>

              <p className="text-center text-xs text-[#7a8ba0]">
                Tap a tab or swipe left and right
              </p>
            </div>
          </div>

          <div className="flex-1 overflow-hidden">
            {/* Desktop: same content as mobile tabs but in two-column layout */}
            <div className="hidden h-full overflow-y-auto lg:block">
              <div className="grid gap-6 p-6 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
                {/* Left column: pitch + smart money + fundamentals */}
                <div className="space-y-5">
                  {/* Hero pitch */}
                  <div className="rounded-2xl border border-[rgba(240,165,0,0.20)] p-5" style={{ background: "linear-gradient(160deg, rgba(240,165,0,0.14) 0%, #080d18 60%)" }}>
                    <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">Why this stock?</p>
                    <p className="mt-2 text-2xl font-bold leading-snug text-white">{thesis}</p>
                  </div>

                  {/* Smart Money */}
                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-5">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">Smart Money is Moving</p>
                    <p className="mb-3 text-sm leading-6 text-[#7a8ba0]">When insiders or Congress buy, they may know something. Here’s what we found:</p>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded-xl p-3" style={{
                        background: row.has_insider_trades ? "linear-gradient(135deg, rgba(249,115,22,0.14) 0%, rgba(249,115,22,0.03) 100%)" : "#0d1117",
                        border: row.has_insider_trades ? "1px solid rgba(249,115,22,0.25)" : "1px solid rgba(255,255,255,0.05)",
                      }}>
                        <p className="text-xs font-bold" style={{ color: row.has_insider_trades ? "#fb923c" : "#374151" }}>Insider Trades</p>
                        <p className="mt-1 text-lg font-black" style={{ color: row.has_insider_trades ? "#fed7aa" : "#1f2937" }}>
                          {(row.cluster_buyers ?? 0) >= 2 ? "Cluster" : row.has_insider_trades ? "Yes" : "No"}
                        </p>
                        <p className="mt-1 text-[11px]" style={{ color: row.has_insider_trades ? "rgba(253,186,116,0.6)" : "#1f2937" }}>
                          {(row.cluster_buyers ?? 0) >= 2 ? `${row.cluster_buyers} insiders buying together` : row.has_insider_trades ? "SEC Form 4 filed" : "None detected"}
                        </p>
                      </div>
                      <div className="rounded-xl p-3" style={{
                        background: (row.has_ptr_forms || row.ptr_amount) ? "linear-gradient(135deg, rgba(168,85,247,0.14) 0%, rgba(168,85,247,0.03) 100%)" : "#0d1117",
                        border: (row.has_ptr_forms || row.ptr_amount) ? "1px solid rgba(168,85,247,0.25)" : "1px solid rgba(255,255,255,0.05)",
                      }}>
                        <p className="text-xs font-bold" style={{ color: (row.has_ptr_forms || row.ptr_amount) ? "#c084fc" : "#374151" }}>Congress Trades</p>
                        <p className="mt-1 text-lg font-black" style={{ color: (row.has_ptr_forms || row.ptr_amount) ? "#e9d5ff" : "#1f2937" }}>
                          {(row.has_ptr_forms || row.ptr_amount) ? "Yes" : "No"}
                        </p>
                        <p className="mt-1 text-[11px]" style={{ color: (row.has_ptr_forms || row.ptr_amount) ? "rgba(196,181,253,0.6)" : "#1f2937" }}>
                          {row.ptr_amount ? `${row.ptr_amount} disclosed` : (row.has_ptr_forms) ? "PTR filed" : "None detected"}
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Bull Case */}
                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-5">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">The Bull Case</p>
                    <ul className="space-y-3">
                      {confidenceBullets.map((item, i) => (
                        <li key={i} className="flex items-start gap-3 text-sm leading-6 text-white/80">
                          <span className="mt-1.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-[#f0a500]/15 text-[10px] font-bold text-[#f0a500]">{i + 1}</span>
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>

                  <ScoreBar row={row} />

                  {row.business_description ? (
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-5">
                      <p className="mb-2 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">What does this company do?</p>
                      <p className="text-sm leading-6 text-[#b0bec8]">{row.business_description}</p>
                    </div>
                  ) : null}
                </div>

                {/* Right column: numbers */}
                <div className="space-y-4">
                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">Price & Performance</p>
                    <div className="space-y-2">
                      <MetricRow label="Current price" value={formatMoney(row.price)} />
                      <MetricRow label="Today’s move" value={formatPercent(row.one_day_return)} />
                      <MetricRow label="Last 5 days" value={formatPercent(row.price_return_5d)} />
                      <MetricRow label="Last 10 days" value={formatPercent(row.return_10d)} />
                      <MetricRow label="Last 20 days" value={formatPercent(row.price_return_20d)} />
                      <MetricRow label="Trading volume" value={row.volume_ratio != null ? `${row.volume_ratio.toFixed(1)}x average` : null} />
                      <MetricRow label="Vs. the market" value={row.relative_strength_20d != null ? `${Number(row.relative_strength_20d) >= 0 ? "+" : ""}${Number(row.relative_strength_20d).toFixed(1)}% over 20d` : null} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-orange-400">Insider & Congress</p>
                    <div className="space-y-2">
                      <MetricRow label="Insider buying?" value={row.has_insider_trades ? "Yes" : "No"} />
                      <MetricRow label="What they did" value={row.insider_action || null} />
                      <MetricRow label="Total invested" value={formatInsiderValue(row)} />
                      <MetricRow label="Buy value" value={row.insider_buy_value != null && row.insider_buy_value > 0 ? formatMoney(row.insider_buy_value) : null} />
                      <MetricRow label="Cluster buy?" value={(row.cluster_buyers ?? 0) >= 2 ? `Yes — ${row.cluster_buyers} insiders` : "No"} />
                      <MetricRow label="Cluster shares" value={formatShares(row.cluster_shares)} />
                      <MetricRow label="Congress buying?" value={(row.has_ptr_forms || row.ptr_amount) ? "Yes" : "No"} />
                      <MetricRow label="Congress amount" value={row.ptr_amount} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">Valuation</p>
                    <div className="space-y-2">
                      <MetricRow label="P/E ratio" value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)} />
                      <MetricRow label="Market cap" value={formatMarketCap(row.market_cap)} />
                      <MetricRow label="Sector" value={row.sector || null} />
                      <MetricRow label="Industry" value={row.industry || null} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                    <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">Score Breakdown</p>
                    <div className="space-y-2">
                      <MetricRow label="Overall score" value={`${row.display_score}/100`} />
                      <MetricRow label="Quality score" value={formatSimpleNumber(row.candidate_score)} />
                      <MetricRow label="Signal score" value={formatSimpleNumber(row.signal_score)} />
                      <MetricRow label="Data freshness" value={getFreshnessLabel(row)} />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div
              className="h-full overflow-y-auto lg:hidden"
              onTouchStart={handleTouchStart}
              onTouchEnd={handleTouchEnd}
            >
              <div className="p-4 pb-28">
                {/* ═══ TAB 0: OVERVIEW ═══ */}
                {activeSlide === 0 ? (
                  <div className="space-y-4">
                    {/* Hero pitch */}
                    <div className="rounded-2xl border border-[rgba(240,165,0,0.20)] p-5" style={{ background: "linear-gradient(160deg, rgba(240,165,0,0.14) 0%, #080d18 60%)" }}>
                      <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
                        Why this stock?
                      </p>
                      <p className="mt-2 text-xl font-bold leading-snug text-white">
                        {thesis}
                      </p>
                    </div>

                    {/* Smart Money section */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
                        Smart Money is Moving
                      </p>
                      <p className="mb-3 text-xs leading-5 text-[#7a8ba0]">
                        When company insiders or members of Congress buy a stock, they may know something the public doesn’t. Here’s what we found:
                      </p>
                      <div className="space-y-2">
                        {/* Insider Trades */}
                        <div className="rounded-xl p-3" style={{
                          background: row.has_insider_trades ? "linear-gradient(135deg, rgba(249,115,22,0.14) 0%, rgba(249,115,22,0.03) 100%)" : "#0d1117",
                          border: row.has_insider_trades ? "1px solid rgba(249,115,22,0.25)" : "1px solid rgba(255,255,255,0.05)",
                        }}>
                          <div className="flex items-center justify-between">
                            <p className="text-xs font-bold" style={{ color: row.has_insider_trades ? "#fb923c" : "#374151" }}>
                              Insider Trades (Form 4)
                            </p>
                            <span className="text-sm font-black" style={{ color: row.has_insider_trades ? "#fed7aa" : "#1f2937" }}>
                              {(row.cluster_buyers ?? 0) >= 2 ? "Cluster" : row.has_insider_trades ? "Yes" : "No"}
                            </span>
                          </div>
                          {row.has_insider_trades ? (
                            <p className="mt-1 text-[11px] text-orange-300/60">
                              {(row.cluster_buyers ?? 0) >= 2
                                ? `${row.cluster_buyers} insiders bought around the same time — that’s unusual and often a bullish sign.`
                                : "A company insider recently filed a purchase with the SEC — they’re putting their own money in."}
                            </p>
                          ) : (
                            <p className="mt-1 text-[11px] text-[#374151]">No recent insider purchases detected.</p>
                          )}
                          {(row.insider_buy_value ?? 0) > 0 && (
                            <p className="mt-1 text-xs font-bold text-orange-200">Value: {formatMoney(row.insider_buy_value!)}</p>
                          )}
                          {(row.insider_shares ?? 0) > 0 && (
                            <p className="mt-1 text-xs font-bold text-orange-200">Shares: {formatWholeNumber(row.insider_shares!)}</p>
                          )}
                        </div>

                        {/* Congressional Trades */}
                        <div className="rounded-xl p-3" style={{
                          background: (row.has_ptr_forms || row.ptr_amount) ? "linear-gradient(135deg, rgba(168,85,247,0.14) 0%, rgba(168,85,247,0.03) 100%)" : "#0d1117",
                          border: (row.has_ptr_forms || row.ptr_amount) ? "1px solid rgba(168,85,247,0.25)" : "1px solid rgba(255,255,255,0.05)",
                        }}>
                          <div className="flex items-center justify-between">
                            <p className="text-xs font-bold" style={{ color: (row.has_ptr_forms || row.ptr_amount) ? "#c084fc" : "#374151" }}>
                              Congress Trades (PTR)
                            </p>
                            <span className="text-sm font-black" style={{ color: (row.has_ptr_forms || row.ptr_amount) ? "#e9d5ff" : "#1f2937" }}>
                              {(row.has_ptr_forms || row.ptr_amount) ? "Yes" : "No"}
                            </span>
                          </div>
                          {(row.has_ptr_forms || row.ptr_amount) ? (
                            <>
                              <p className="mt-1 text-[11px] text-purple-300/60">
                                A member of Congress disclosed a purchase. They must report within 45 days, so the actual buy may have been earlier.
                              </p>
                              {row.ptr_amount && (
                                <p className="mt-1 text-xs font-bold text-purple-200">Amount: {row.ptr_amount}</p>
                              )}
                            </>
                          ) : (
                            <p className="mt-1 text-[11px] text-[#374151]">No congressional trades found for this stock.</p>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Why you should care bullets */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
                        The Bull Case
                      </p>
                      <ul className="space-y-3">
                        {confidenceBullets.map((item, i) => (
                          <li key={i} className="flex items-start gap-3 text-sm leading-6 text-white/80">
                            <span className="mt-1.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-[#f0a500]/15 text-[10px] font-bold text-[#f0a500]">
                              {i + 1}
                            </span>
                            <span>{item}</span>
                          </li>
                        ))}
                      </ul>
                    </div>

                    <ScoreBar row={row} />

                    {row.business_description ? (
                      <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                        <p className="mb-2 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">
                          What does this company do?
                        </p>
                        <p className="text-sm leading-6 text-[#b0bec8]">
                          {row.business_description}
                        </p>
                      </div>
                    ) : null}
                  </div>
                ) : null}

                {/* ═══ TAB 1: FUNDAMENTALS ═══ */}
                {activeSlide === 1 ? (
                  <div className="space-y-3">
                    <div className="rounded-2xl border border-[rgba(240,165,0,0.15)] p-4" style={{ background: "linear-gradient(160deg, rgba(240,165,0,0.08) 0%, #080d18 60%)" }}>
                      <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
                        Quality Report Card
                      </p>
                      <p className="mt-2 text-sm leading-6 text-[#b0bec8]">
                        Think of these like grades for a stock. We check six things that separate great long-term investments from risky bets.
                      </p>
                    </div>

                    {(() => {
                      const ltcs = parseScreenReasonScores(row.screen_reason)
                      const rs = row.relative_strength_20d != null ? Number(row.relative_strength_20d) : null

                      const tiles = [
                        {
                          label: "Profit Growth", score: ltcs.profitability, color: "#22d3ee", borderColor: "rgba(34,211,238,0.20)",
                          icon: "M2 9L4.5 4L7 6.5L10 2M10 2H7.5M10 2V4.5",
                          verdict: ltcs.profitability == null ? "No data" : ltcs.profitability >= 75 ? "Strong" : ltcs.profitability >= 40 ? "Growing" : "Weak",
                          explain: "Is the company making more money each year? We look for earnings growth of at least 25% and return on equity above 15%. Think of it like a business getting a bigger raise every quarter.",
                        },
                        {
                          label: "Money In", score: ltcs.profitability, color: "#34d399", borderColor: "rgba(52,211,153,0.20)",
                          icon: "M6 1V11M3 3.5H7.5C8.88 3.5 10 4.34 10 5.25S8.88 7 7.5 7H3M3 7H8C9.38 7 10.5 7.84 10.5 8.75S9.38 10.5 8 10.5H3",
                          verdict: ltcs.profitability == null ? "No data" : ltcs.profitability >= 65 ? "Positive" : ltcs.profitability >= 35 ? "Mixed" : "Negative",
                          explain: "Does the company generate real cash — not just profit on paper? Free cash flow means money left over after paying all the bills. Companies with strong cash flow can grow, pay dividends, or buy back shares without borrowing.",
                        },
                        {
                          label: "Financial Health", score: ltcs.financial, color: "#a78bfa", borderColor: "rgba(167,139,250,0.20)",
                          icon: "M1.5 10V4L6 1.5L10.5 4V10M4 10V7H8V10",
                          verdict: ltcs.financial == null ? "No data" : ltcs.financial >= 70 ? "Low Debt" : ltcs.financial >= 40 ? "Moderate" : "High Debt",
                          explain: "Can this company pay its bills without borrowing more? We check how much debt they have compared to what they own. Low debt means they can survive tough times and won’t get crushed by rising interest rates.",
                        },
                        {
                          label: "Edge", score: ltcs.moat, color: "#f59e0b", borderColor: "rgba(245,158,11,0.20)",
                          icon: "M1 8L3 6L5 8L7 4L9 7L11 5M1 10H11",
                          verdict: ltcs.moat == null ? "No data" : ltcs.moat >= 75 ? "Wide" : ltcs.moat >= 50 ? "Narrow" : "None",
                          explain: "How hard is it for competitors to steal this company’s customers? A ‘wide moat’ means strong brand, patents, or network effects that protect profits. Think Apple, Google, or Costco — they’re very hard to beat.",
                        },
                        {
                          label: "Fair Price", score: ltcs.valuation, color: "#fb923c", borderColor: "rgba(251,146,60,0.20)",
                          icon: "M2 2V10H10M4 8V6M6.5 8V4M9 8V5",
                          verdict: ltcs.valuation == null ? "No data" : ltcs.valuation >= 70 ? "Attractive" : ltcs.valuation >= 35 ? "Fair" : "Expensive",
                          explain: "Is the stock priced right for how fast the company is growing? We use the PEG ratio — ideally under 1.5. Even the best company is a bad investment if you overpay. We want quality at a reasonable price.",
                        },
                        {
                          label: "Momentum", score: rs != null ? Math.min(Math.max(Math.round(50 + rs * 2), 5), 100) : null, color: "#ec4899", borderColor: "rgba(236,72,153,0.20)",
                          icon: "M6 1L8.5 4H7V7.5H5V4H3.5L6 1M2 9.5H10",
                          verdict: rs == null ? "No data" : rs >= 15 ? "Near Highs" : rs >= 8 ? "Strong" : rs >= 0 ? "Neutral" : "Weak",
                          explain: rs != null
                            ? `This stock is ${rs >= 0 ? "+" : ""}${rs.toFixed(1)}% ahead of the market over the last 20 days. Stocks near their 12-month highs tend to keep going — winners keep winning.`
                            : "We check if the stock is outperforming most others. Stocks near their highs tend to have positive momentum — winners keep winning.",
                        },
                      ]

                      return tiles.map(({ label, score: s, color, borderColor, icon, verdict, explain }) => {
                        const active = s != null && s > 0
                        const pct = s != null ? Math.min(s, 100) : 0
                        return (
                          <div key={label} className="rounded-2xl p-4" style={{
                            background: active ? `linear-gradient(160deg, ${color}14 0%, #111827 50%)` : "#111827",
                            border: `1px solid ${active ? borderColor : "rgba(255,255,255,0.05)"}`,
                          }}>
                            <div className="mb-2 flex items-center justify-between">
                              <div className="flex items-center gap-2">
                                <svg width="14" height="14" viewBox="0 0 12 12" fill="none">
                                  <path d={icon} stroke={active ? color : "#374151"} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
                                </svg>
                                <span className="text-sm font-bold" style={{ color: active ? "#f0f0f0" : "#374151" }}>{label}</span>
                              </div>
                              <div className="flex items-center gap-2">
                                {s != null && <span className="text-xs font-black" style={{ color }}>{s}/100</span>}
                                <span className="text-xs font-bold" style={{ color: active ? color : "#374151" }}>{verdict}</span>
                              </div>
                            </div>
                            <div className="mb-3 h-[5px] overflow-hidden rounded-full bg-[#1a2540]">
                              <div className="h-full rounded-full transition-all duration-700" style={{
                                width: `${pct}%`,
                                background: `linear-gradient(90deg, ${color}90, ${color})`,
                                boxShadow: `0 0 8px ${color}40`,
                              }} />
                            </div>
                            <p className="text-xs leading-5 text-[#8a99ab]">{explain}</p>
                          </div>
                        )
                      })
                    })()}
                  </div>
                ) : null}

                {/* ═══ TAB 2: NUMBERS ═══ */}
                {activeSlide === 2 ? (
                  <div className="space-y-4">
                    {/* Price & Returns */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#f0a500]">
                        Price & Performance
                      </p>
                      <div className="space-y-2">
                        <MetricRow label="Current price" value={formatMoney(row.price)} />
                        <MetricRow label="Today’s move" value={formatPercent(row.one_day_return)} />
                        <MetricRow label="Last 5 days" value={formatPercent(row.price_return_5d)} />
                        <MetricRow label="Last 10 days" value={formatPercent(row.return_10d)} />
                        <MetricRow label="Last 20 days" value={formatPercent(row.price_return_20d)} />
                        <MetricRow label="Trading volume" value={row.volume_ratio != null ? `${row.volume_ratio.toFixed(1)}x average` : null} />
                        <MetricRow label="Vs. the market" value={row.relative_strength_20d != null ? `${Number(row.relative_strength_20d) >= 0 ? "+" : ""}${Number(row.relative_strength_20d).toFixed(1)}% over 20 days` : null} />
                      </div>
                    </div>

                    {/* Insider & Congress */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-orange-400">
                        Insider & Congress Trades
                      </p>
                      <div className="space-y-2">
                        <MetricRow label="Insider buying?" value={row.has_insider_trades ? "Yes" : "No"} />
                        <MetricRow label="What they did" value={row.insider_action || null} />
                        <MetricRow label="Shares bought" value={formatShares(row.insider_shares)} />
                        <MetricRow label="Price they paid" value={formatMoney(row.insider_avg_price)} />
                        <MetricRow label="Total invested" value={formatInsiderValue(row)} />
                        <MetricRow label="Buy value" value={row.insider_buy_value != null && row.insider_buy_value > 0 ? formatMoney(row.insider_buy_value) : null} />
                        <MetricRow label="Cluster buy?" value={(row.cluster_buyers ?? 0) >= 2 ? `Yes — ${row.cluster_buyers} insiders` : "No"} />
                        <MetricRow label="Cluster shares" value={formatShares(row.cluster_shares)} />
                        <MetricRow label="Congress buying?" value={(row.has_ptr_forms || row.ptr_amount) ? "Yes" : "No"} />
                        <MetricRow label="Congress amount" value={row.ptr_amount} />
                        {row.ptr_amount ? (
                          <div className="rounded-xl border border-amber-400/15 bg-amber-400/5 px-3 py-2 text-[11px] leading-5 text-amber-200/60">
                            Congress members have up to 45 days to report. The trade may have happened earlier.
                          </div>
                        ) : null}
                      </div>
                    </div>

                    {/* Valuation */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">
                        Valuation & Fundamentals
                      </p>
                      <div className="space-y-2">
                        <MetricRow label="P/E ratio" value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)} />
                        <MetricRow label="Market cap" value={formatMarketCap(row.market_cap)} />
                        <MetricRow label="Sector" value={row.sector || null} />
                        <MetricRow label="Industry" value={row.industry || null} />
                        <MetricRow label="Earnings surprise" value={formatPercent(row.earnings_surprise_pct)} />
                        <MetricRow label="Revenue growth" value={formatPercent(row.revenue_growth_pct)} />
                      </div>
                    </div>

                    {/* Scores & Meta */}
                    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#111827] p-4">
                      <p className="mb-3 text-[10px] font-bold uppercase tracking-[0.18em] text-[#7a8ba0]">
                        Score Breakdown
                      </p>
                      <div className="space-y-2">
                        <MetricRow label="Overall score" value={`${row.display_score}/100`} />
                        <MetricRow label="Quality score" value={formatSimpleNumber(row.candidate_score)} />
                        <MetricRow label="Signal score" value={formatSimpleNumber(row.signal_score)} />
                        <MetricRow label="How it qualified" value={row.data_source_label} />
                        <MetricRow label="Data freshness" value={getFreshnessLabel(row)} />
                        <MetricRow label="Last updated" value={row.last_screened_at ? formatDateLong(row.last_screened_at) : null} />
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          </div>

          <div className="border-t border-[rgba(255,255,255,0.07)] p-3 sm:hidden" style={{ background: "#080d18" }}>
            <div className="grid grid-cols-3 gap-2">
              <button
                type="button"
                onClick={onPrev}
                disabled={!onPrev}
                className="rounded-2xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-3 py-3 text-sm font-semibold text-white transition hover:bg-[#162038] disabled:opacity-40"
              >
                ← Prev
              </button>

              <button
                type="button"
                onClick={onClose}
                className="rounded-2xl border border-[rgba(240,165,0,0.25)] bg-[rgba(240,165,0,0.12)] px-3 py-3 text-sm font-semibold text-[#f0a500] transition hover:border-[rgba(240,165,0,0.35)] hover:bg-[rgba(240,165,0,0.18)]"
              >
                Back to board
              </button>

              <button
                type="button"
                onClick={onNext}
                disabled={!onNext}
                className="rounded-2xl border border-[rgba(255,255,255,0.10)] bg-[#0f1729] px-3 py-3 text-sm font-semibold text-white transition hover:bg-[#162038] disabled:opacity-40"
              >
                Next →
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function MetricRow({
  label,
  value,
}: {
  label: string
  value: string | null | undefined
}) {
  if (value === null || value === undefined || value === "" || value === "—") {
    return null
  }

  return (
    <div className="flex items-center justify-between gap-4 rounded-2xl bg-[#0f1729] px-4 py-3 text-sm">
      <span className="min-w-0 break-words text-[#7a8ba0]">{label}</span>
      <span className="max-w-[58%] truncate text-right font-semibold text-white">
        {value}
      </span>
    </div>
  )
}

function ConfirmationRow({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#0f1729] px-4 py-3">
      <p className="text-xs uppercase tracking-[0.16em] text-[#7a8ba0]">{label}</p>
      <p className="mt-2 break-words text-sm font-semibold text-white">{value}</p>
    </div>
  )
}

function ReasonCard({ reason }: { reason: ReasonLine }) {
  const classes =
    reason.tone === "good"
      ? "border-[rgba(48,209,88,0.20)] bg-[rgba(48,209,88,0.06)]"
      : reason.tone === "bad"
        ? "border-[rgba(255,69,58,0.20)] bg-[rgba(255,69,58,0.06)]"
        : "border-[rgba(255,255,255,0.07)] bg-[#0f1729]"

  const textClasses =
    reason.tone === "good"
      ? "text-[#30d158]"
      : reason.tone === "bad"
        ? "text-[#ff453a]"
        : "text-[#b0bec8]"

  return (
    <div className={`rounded-2xl border p-4 ${classes}`}>
      <p className="text-xs uppercase tracking-[0.16em] text-[#7a8ba0]">{reason.label}</p>
      <p className={`mt-2 break-words text-sm font-semibold ${textClasses}`}>
        {reason.value}
      </p>
    </div>
  )
}

function MovementCard({
  label,
  value,
}: {
  label: string
  value: number | null | undefined
}) {
  const formatted = formatScoreChange(value)

  if (formatted === "—") {
    return (
      <div className="rounded-2xl border border-[rgba(255,255,255,0.07)] bg-[#0f1729] p-4">
        <p className="text-xs uppercase tracking-[0.16em] text-[#7a8ba0]">{label}</p>
        <p className="mt-2 text-sm font-semibold text-[#b0bec8]">No history yet</p>
      </div>
    )
  }

  const isUp = (value ?? 0) > 0
  const isDown = (value ?? 0) < 0

  return (
    <div
      className={[
        "rounded-2xl border p-4",
        isUp
          ? "border-[rgba(48,209,88,0.20)] bg-[rgba(48,209,88,0.06)]"
          : isDown
            ? "border-[rgba(255,69,58,0.20)] bg-[rgba(255,69,58,0.06)]"
            : "border-[rgba(255,255,255,0.07)] bg-[#0f1729]",
      ].join(" ")}
    >
      <p className="text-xs uppercase tracking-[0.16em] text-[#7a8ba0]">{label}</p>
      <p
        className={[
          "mt-2 break-words text-sm font-semibold",
          isUp ? "text-[#30d158]" : isDown ? "text-[#ff453a]" : "text-[#b0bec8]",
        ].join(" ")}
      >
        {formatted}
      </p>
    </div>
  )
}

function TagPill({ tag }: { tag: string }) {
  const pretty = prettifyTag(tag)

  return (
    <span className="rounded-full border border-[rgba(255,255,255,0.07)] bg-[#162038] px-3 py-1 text-xs text-[#7a8ba0]">
      {pretty}
    </span>
  )
}

function getFeaturedThesis(row: UnifiedRow) {
  const ltcs = parseScreenReasonScores(row.screen_reason)

  if ((row.cluster_buyers ?? 0) >= 3 && row.ptr_amount) {
    return "Multiple insiders and Congress are buying — a rare alignment of conviction"
  }

  if ((row.cluster_buyers ?? 0) >= 2) {
    return "Multiple insiders are buying at current prices — a strong vote of confidence"
  }

  if (row.ptr_amount) {
    return "A Congressional trade is supporting this thesis"
  }

  if (
    ltcs.moat !== null && ltcs.moat >= 75 &&
    ltcs.profitability !== null && ltcs.profitability >= 75
  ) {
    return "A high-moat, cash-generative compounder at a quality price"
  }

  if (ltcs.financial !== null && ltcs.financial >= 100 && ltcs.profitability !== null && ltcs.profitability >= 75) {
    return "Rock-solid balance sheet with consistently strong earnings"
  }

  if (ltcs.valuation !== null && ltcs.valuation >= 65 && row.display_score >= 70) {
    return "A quality business available at an attractive valuation"
  }

  if (row.display_score >= 80) {
    return "Multiple quality factors are scoring well for this company"
  }

  return "Passes the long-term quality screen with several positive signals"
}

function getSimpleCardBullets(row: UnifiedRow) {
  const points: string[] = []

  // Parse LTCS sub-scores from screen_reason: "LTCS 72/100: moat: 75/100, financial health: 100/100..."
  const reason = row.screen_reason ?? ""
  const moatMatch = reason.match(/moat:\s*(\d+)\/100/)
  const financialMatch = reason.match(/financial health:\s*(\d+)\/100/)
  const profitabilityMatch = reason.match(/profitability:\s*(\d+)\/100/)
  const stabilityMatch = reason.match(/stability:\s*(\d+)\/100/)
  const valuationMatch = reason.match(/valuation:\s*(\d+)\/100/)

  const moat = moatMatch ? Number(moatMatch[1]) : null
  const financial = financialMatch ? Number(financialMatch[1]) : null
  const profitability = profitabilityMatch ? Number(profitabilityMatch[1]) : null
  const stability = stabilityMatch ? Number(stabilityMatch[1]) : null
  const valuation = valuationMatch ? Number(valuationMatch[1]) : null

  if (moat !== null && moat >= 75) points.push("Strong business moat — high margins and solid revenue growth.")
  if (financial !== null && financial >= 80) points.push("Healthy balance sheet with manageable debt.")
  if (profitability !== null && profitability >= 75) points.push("Consistently profitable with strong free cash flow.")
  if (stability !== null && stability >= 60) points.push("Lower volatility, suitable for long-term holding.")
  if (valuation !== null && valuation >= 65) points.push("Trading at a reasonable valuation relative to growth.")

  if ((row.earnings_surprise_pct ?? 0) >= 10) {
    points.push("Recent earnings came in stronger than expected.")
  }

  if (points.length === 0) {
    points.push("Passes the quality screen for long-term hold candidates.")
  }

  return points.slice(0, 3)
}

function parseScreenReasonScores(reason: string | null | undefined) {
  const r = reason ?? ""
  const moatMatch = r.match(/moat:\s*(\d+)\/100/)
  const financialMatch = r.match(/financial health:\s*(\d+)\/100/)
  const profitabilityMatch = r.match(/profitability:\s*(\d+)\/100/)
  const stabilityMatch = r.match(/stability:\s*(\d+)\/100/)
  const valuationMatch = r.match(/valuation:\s*(\d+)\/100/)
  return {
    moat: moatMatch ? Number(moatMatch[1]) : null,
    financial: financialMatch ? Number(financialMatch[1]) : null,
    profitability: profitabilityMatch ? Number(profitabilityMatch[1]) : null,
    stability: stabilityMatch ? Number(stabilityMatch[1]) : null,
    valuation: valuationMatch ? Number(valuationMatch[1]) : null,
  }
}

function getPillarVerdict(score: number | null) {
  if (score === null) return { label: "No data", color: "text-[#7a8ba0]", barColor: "#1e2d45" }
  if (score >= 75) return { label: "Strong", color: "text-[#30d158]", barColor: "#30d158" }
  if (score >= 50) return { label: "Fair", color: "text-[#f0a500]", barColor: "#f0a500" }
  return { label: "Weak", color: "text-[#ff453a]", barColor: "#ff453a" }
}

function getPremiumSummaryBullets(row: UnifiedRow) {
  const bullets: string[] = []

  if (row.has_candidate_data && row.has_signal_data) {
    bullets.push(
      "This one has both price strength and extra signal support working in its favor."
    )
  }

  if (row.has_candidate_data && !row.has_signal_data) {
    bullets.push(
      "This one mainly made the list because its price setup looks strong."
    )
  }

  if (row.has_signal_data && !row.has_candidate_data) {
    bullets.push(
      "This one mainly made the list because the signal layer was strong enough on its own."
    )
  }

  if ((row.relative_strength_20d ?? 0) > 0) {
    bullets.push("It has been beating the broader market recently.")
  }

  if ((row.volume_ratio ?? 0) >= 1.5) {
    bullets.push("Higher volume suggests more investor attention than normal.")
  }

  if ((row.candidate_score ?? 0) >= 90) {
    bullets.push("Its price-based setup is among the stronger ones on the board.")
  }

  if (!bullets.length) {
    bullets.push(
      "This setup has enough strength and support to deserve attention today."
    )
  }

  return bullets.slice(0, 3)
}

function getTopReasonLines(row: UnifiedRow): ReasonLine[] {
  const items: ReasonLine[] = []
  const breakdown = row.score_breakdown || {}

  const labelMap: Record<
    string,
    { label: string; tone: "good" | "bad" | "neutral" }
  > = {
    insider_buying: { label: "Insider buying", tone: "good" },
    repeat_buying: { label: "Repeat buying", tone: "good" },
    senior_executive_buy: { label: "Executive buy", tone: "good" },
    momentum: { label: "Momentum", tone: "good" },
    relative_strength: { label: "Vs market", tone: "good" },
    earnings: { label: "Earnings", tone: "good" },
    valuation: { label: "Valuation", tone: "neutral" },
    catalyst: { label: "Catalyst", tone: "good" },
    freshness: { label: "Freshness", tone: "neutral" },
    candidate_screen: { label: "Price setup", tone: "good" },
    candidate_tier_bonus: { label: "Tier bonus", tone: "good" },
    candidate_volume: { label: "Price volume", tone: "good" },
    candidate_momentum: { label: "Price momentum", tone: "good" },
    candidate_breakout: { label: "Breakout", tone: "good" },
    base: { label: "Base signal", tone: "neutral" },
  }

  for (const [key, rawValue] of Object.entries(breakdown)) {
    const value = Number(rawValue || 0)
    if (!Number.isFinite(value) || value === 0) continue

    const meta = labelMap[key] || {
      label: prettifyTag(key),
      tone: value > 0 ? "good" : value < 0 ? "bad" : "neutral",
    }

    items.push({
      label: meta.label,
      value: `${value > 0 ? "+" : ""}${Math.round(value * 10) / 10}`,
      tone: meta.tone,
      weight: Math.abs(value),
    })
  }

  if ((row.candidate_score ?? 0) > 0) {
    items.push({
      label: "Price strength score",
      value: String(Math.round(row.candidate_score ?? 0)),
      tone: "good",
      weight: Math.abs(row.candidate_score ?? 0),
    })
  }

  if (!items.length) {
    items.push({
      label: "Model",
      value: "The positives currently outweigh the negatives",
      tone: "good",
      weight: 1,
    })
  }

  return items.sort((a, b) => b.weight - a.weight).slice(0, 4)
}

function getConfidenceBullets(row: UnifiedRow) {
  const ltcs = parseScreenReasonScores(row.screen_reason)
  const bullets: string[] = []

  if ((row.cluster_buyers ?? 0) >= 2) {
    bullets.push(
      `${row.cluster_buyers} company insiders recently bought shares — people with inside knowledge of the business.`
    )
  }

  if (row.ptr_amount) {
    bullets.push(
      `A U.S. Congress member filed a trade for ${row.ptr_amount}. Politicians often trade ahead of policy moves.`
    )
  }

  if (ltcs.moat !== null && ltcs.moat >= 75) {
    bullets.push(
      "The company has strong margins and revenue growth — signs of a lasting competitive edge over rivals."
    )
  }

  if (ltcs.financial !== null && ltcs.financial >= 80) {
    bullets.push(
      "Balance sheet looks healthy: debt is manageable and the company is generating profit."
    )
  }

  if (ltcs.profitability !== null && ltcs.profitability >= 75) {
    bullets.push(
      "Return on equity is strong and the business generates positive free cash flow — it funds its own growth."
    )
  }

  if (ltcs.valuation !== null && ltcs.valuation >= 65) {
    bullets.push(
      "The stock looks reasonably priced relative to its growth prospects."
    )
  }

  if (!bullets.length) {
    bullets.push(
      "This company passes the quality screen and has enough positives to deserve attention."
    )
  }

  return bullets.slice(0, 4)
}

function getSimpleSetupBullets(row: UnifiedRow) {
  const ltcs = parseScreenReasonScores(row.screen_reason)
  const parts: string[] = []

  if (ltcs.moat !== null && ltcs.moat >= 75) {
    parts.push(
      "Wide economic moat: high margins, strong revenue growth, and significant market scale make it hard for rivals to compete."
    )
  }

  if (ltcs.financial !== null && ltcs.financial >= 80) {
    parts.push(
      "Healthy balance sheet with a debt-to-equity ratio under 1× — financial flexibility without the interest-rate risk."
    )
  }

  if (ltcs.profitability !== null && ltcs.profitability >= 75) {
    parts.push(
      "Consistently profitable with strong return on equity and positive free cash flow — the business funds its own growth."
    )
  }

  if (ltcs.stability !== null && ltcs.stability >= 60) {
    parts.push(
      "Lower price volatility than the average stock — a smoother ride for long-term investors."
    )
  }

  if (ltcs.valuation !== null && ltcs.valuation >= 65) {
    parts.push(
      "Valuation looks attractive: P/E or PEG ratio is reasonable relative to expected earnings growth — a fair price for quality."
    )
  }

  if ((row.revenue_growth_pct ?? 0) > 10) {
    parts.push(
      "Revenue is growing meaningfully — the business is expanding, not just coasting."
    )
  }

  if (parts.length === 0) {
    parts.push(
      "This company passes quality checks across moat, balance sheet, and profitability for long-term investors."
    )
  }

  return parts.slice(0, 4)
}

function normalizeTags(tags: string[] | null | undefined) {
  if (!tags) return []
  if (Array.isArray(tags)) return tags.filter(Boolean).map(humanizeSignalLabel)
  return []
}

function prettifyTag(tag: string) {
  return tag
    .replace(/^source:/, "")
    .replace(/^8k:/, "8-K ")
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
}

function formatSource(source?: string | null) {
  if (!source) return "Price strength"
  if (source === "form4") return "Form 4"
  if (source === "13d") return "13D"
  if (source === "13g") return "13G"
  if (source === "8k") return "8-K / current report"
  if (source === "earnings") return "Earnings"
  if (source === "breakout") return "Price breakout"
  return source
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${value >= 0 ? "+" : ""}${round1(value)?.toFixed(1)}%`
}

function formatRatio(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${round1(value)?.toFixed(2)}x`
}

function formatMoney(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: value >= 100 ? 0 : 2,
  }).format(value)
}

function formatWholeNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return Math.round(value).toLocaleString()
}

function formatSignalStack(value: number | null | undefined, row?: UnifiedRow) {
  if (value !== null && value !== undefined) {
    return `${Math.round(value)}`
  }

  if (row?.has_candidate_data && !row?.has_signal_data) {
    return "1"
  }

  return "—"
}

function formatRelativeStrengthForDisplay(row: UnifiedRow) {
  if (row.relative_strength_20d !== null && row.relative_strength_20d !== undefined) {
    return formatPercent(row.relative_strength_20d)
  }

  if (row.has_candidate_data && !row.has_signal_data) {
    return "Price only"
  }

  return "—"
}

function formatSimpleNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return String(Math.round(value))
}

function formatShares(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${Math.round(value).toLocaleString()}`
}

function formatMarketCap(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"

  if (value >= 1_000_000_000_000) {
    return `$${(value / 1_000_000_000_000).toFixed(2)}T`
  }
  if (value >= 1_000_000_000) {
    return `$${(value / 1_000_000_000).toFixed(2)}B`
  }
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(1)}M`
  }

  return formatMoney(value)
}

function formatBooleanLabel(value: boolean | null | undefined) {
  if (value === null || value === undefined) return "—"
  return value ? "Yes" : "No"
}

function formatInsiderValue(row: UnifiedRow) {
  if (row.insider_buy_value != null && row.insider_buy_value > 0) {
    return formatMoney(row.insider_buy_value)
  }

  if (
    row.insider_shares != null &&
    row.insider_shares > 0 &&
    row.insider_avg_price != null &&
    row.insider_avg_price > 0
  ) {
    return formatMoney(row.insider_shares * row.insider_avg_price)
  }

  return null
}

function formatPe(
  trailing: number | null | undefined,
  forward?: number | null | undefined,
  peType?: string | null | undefined
) {
  if (trailing !== null && trailing !== undefined) {
    return `${trailing.toFixed(1)}${peType === "trailing" ? " TTM" : ""}`
  }

  if (forward !== null && forward !== undefined) {
    return `${forward.toFixed(1)} FWD`
  }

  return "—"
}

function formatScoreChange(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  const rounded = Math.round(value * 10) / 10
  return `${rounded > 0 ? "+" : ""}${rounded}`
}

function formatDateLong(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "—"

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date)
}

function hasDisplayValue(value: string | null | undefined) {
  return !(value === null || value === undefined || value === "" || value === "—")
}

function truncateText(value: string | null | undefined, maxLength: number) {
  if (!value) return ""
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength).trim()}…`
}

function getScorePalette(score: number) {
  const s = Math.max(0, Math.min(100, score))
  if (s <= 69) return { start: "#7a8ba0", end: "#5a6a7a", text: "#ffffff" }
  if (s <= 79) return { start: "#f0a500", end: "#d49200", text: "#000000" }
  if (s <= 89) return { start: "#f0a500", end: "#ffb733", text: "#000000" }
  return { start: "#30d158", end: "#28b84a", text: "#000000" }
}

function getScoreTierLabel(score: number) {
  if (score >= 90) return "Top tier"
  if (score >= 80) return "Strong"
  if (score >= 70) return "Good"
  return "Watch"
}

function getConfidenceTierLabel(score: number) {
  if (score >= 90) return "Top tier"
  if (score >= 80) return "High confidence"
  if (score >= 70) return "Strong setup"
  return "Developing"
}

function getSignalCategory(row: UnifiedRow) {
  const storedCategory = (row.primary_signal_category ?? "").trim()
  if (storedCategory) return storedCategory
  return row.has_signal_data ? "Signals" : "Price strength"
}

function getFreshnessLabel(row: UnifiedRow) {
  const bucket = (row.freshness_bucket ?? "").trim()
  let age = row.age_days

  if ((age === null || age === undefined) && row.last_screened_at) {
    const timestamp = new Date(row.last_screened_at).getTime()
    if (!Number.isNaN(timestamp)) {
      age = Math.max(
        0,
        Math.floor((Date.now() - timestamp) / (24 * 60 * 60 * 1000))
      )
    }
  }

  if (bucket === "today") return "Today"
  if (bucket === "fresh") return "1–3D"
  if (bucket === "recent") return "4–7D"
  if (bucket === "aging") return "8–14D"
  if (bucket === "stale") return "Older"

  if (typeof age === "number") {
    if (age <= 0) return "Today"
    if (age <= 3) return "1–3D"
    if (age <= 7) return "4–7D"
    if (age <= 14) return "8–14D"
    return "Older"
  }

  return null
}