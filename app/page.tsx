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
  data_source_label:
    | "Price Strength + Signals"
    | "Price Strength Only"
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
const DETAIL_TABS = ["Overview", "Why it’s here", "Numbers"] as const

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

function firstNumberOrNull(...values: Array<number | null | undefined>) {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) return value
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
    sector: signal?.sector ?? null,
    industry: signal?.industry ?? null,
    pe_ratio: signal?.pe_ratio ?? null,
    pe_forward: signal?.pe_forward ?? null,
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
    relative_strength_20d:
      signal?.relative_strength_20d ??
      candidate?.relative_strength_20d ??
      null,

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
              updated_at
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
  }, [priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter])

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
      .sort(compareRows)
  }, [rows, priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter])

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
    return count
  }, [priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter, sourceFilter])

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
    <main className="flex h-[100dvh] w-full flex-col overflow-hidden bg-[radial-gradient(circle_at_top,_rgba(34,211,238,0.08),_transparent_20%),radial-gradient(circle_at_bottom_left,_rgba(16,185,129,0.06),_transparent_24%),linear-gradient(to_bottom,_#020617,_#081122_50%,_#020617)] text-white">
      <style jsx global>{`
        @keyframes cardFadeUp {
          from { opacity: 0; transform: translateY(16px) scale(0.985); }
          to { opacity: 1; transform: translateY(0) scale(1); }
        }
        @keyframes scoreGlow {
          0%, 100% { box-shadow: 0 0 0 rgba(34, 197, 94, 0); }
          50% { box-shadow: 0 0 24px rgba(34, 197, 94, 0.18); }
        }
        @keyframes scoreGlowCyan {
          0%, 100% { box-shadow: 0 0 0 rgba(34, 211, 238, 0); }
          50% { box-shadow: 0 0 24px rgba(34, 211, 238, 0.18); }
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
      <header className="shrink-0 border-b border-white/[0.07] bg-black/30 px-4 py-3 backdrop-blur-sm">
        <div className="mx-auto flex max-w-lg items-center justify-between">
          <div>
            <p className="text-[10px] font-bold uppercase tracking-[0.26em] text-emerald-400/90">
              Market Signal Tracker
            </p>
            <p className="mt-0.5 flex items-center gap-2 text-sm font-semibold text-white">
              {loading ? (
                <span className="text-slate-500">Loading…</span>
              ) : (
                <>
                  <span>{filteredRows.length} ideas</span>
                  {eliteCount > 0 && (
                    <span className="rounded-full bg-emerald-400/15 px-2 py-0.5 text-[10px] font-bold text-emerald-300">
                      {eliteCount} top tier
                    </span>
                  )}
                  {lastUpdated && (
                    <span className="text-[11px] font-normal text-slate-500">
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
            className="relative inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-4 py-2.5 text-sm font-semibold text-slate-300 transition hover:border-cyan-400/30 hover:bg-cyan-400/10 hover:text-cyan-200"
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
              <span className="absolute -right-1.5 -top-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-cyan-400 text-[9px] font-bold text-slate-900">
                {activeFilterCount}
              </span>
            )}
          </button>
        </div>
      </header>

      {/* Collapsible filters */}
      <div
        className={[
          "shrink-0 overflow-hidden border-b border-white/[0.07] bg-black/25 backdrop-blur-sm transition-all duration-300 ease-out",
          filtersOpen ? "max-h-80 opacity-100" : "max-h-0 opacity-0",
        ].join(" ")}
      >
        <div className="mx-auto max-w-lg px-4 pb-5 pt-4">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
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
            {activeFilterCount > 0 && (
              <div className="flex items-end">
                <button
                  type="button"
                  onClick={resetFilters}
                  className="w-full rounded-2xl border border-white/10 bg-white/5 py-2.5 text-sm font-semibold text-slate-300 transition hover:border-emerald-400/30 hover:bg-emerald-400/10 hover:text-emerald-200"
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
        <div className="shrink-0 px-4 py-2 text-center text-[10px] leading-5 text-slate-600">
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

  return (
    <div className="flex h-full flex-col items-center px-3 pb-2 pt-3">
      {/* Nav row */}
      <div className="mb-2.5 flex w-full max-w-md shrink-0 items-center justify-between">
        <button
          type="button"
          onClick={goPrev}
          disabled={!hasPrev}
          aria-label="Previous idea"
          className={[
            "flex h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition",
            hasPrev
              ? "border-white/15 bg-white/5 text-slate-200 hover:bg-white/10 active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ‹
        </button>

        <div className="flex items-center gap-1.5">
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
                  ? "h-2 w-5 bg-cyan-400"
                  : "h-1.5 w-1.5 bg-slate-600 hover:bg-slate-400",
              ].join(" ")}
            />
          ))}
          <span className="ml-1.5 text-[11px] text-slate-500">
            {cardIndex + 1}/{rows.length}
          </span>
        </div>

        <button
          type="button"
          onClick={goNext}
          disabled={!hasNext}
          aria-label="Next idea"
          className={[
            "flex h-10 w-10 items-center justify-center rounded-full border text-xl font-light transition",
            hasNext
              ? "border-white/15 bg-white/5 text-slate-200 hover:bg-white/10 active:scale-95"
              : "cursor-default border-transparent text-transparent",
          ].join(" ")}
        >
          ›
        </button>
      </div>

      {/* Swipeable card */}
      <div
        className="min-h-0 w-full max-w-md flex-1 overflow-hidden"
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

  const clusterBuyers = row.cluster_buyers ?? 0
  const hasCluster = clusterBuyers >= 2
  const hasPtr = Boolean(row.ptr_amount)
  const insiderValue = formatInsiderValue(row)
  const hasInsiderBuy =
    hasDisplayValue(insiderValue) ||
    Boolean(row.insider_action) ||
    (row.insider_shares ?? 0) > 0
  const hasPlatinum = clusterBuyers >= 3 && hasPtr
  const hasAnyInsiderSignal = hasCluster || hasPtr || hasInsiderBuy

  return (
    <div
      className="flex h-full flex-col overflow-hidden rounded-[1.75rem] border shadow-2xl"
      style={{
        borderColor: hasPlatinum
          ? "rgba(251,191,36,0.35)"
          : hasCluster
            ? "rgba(52,211,153,0.35)"
            : `${palette.end}40`,
        background: hasPlatinum
          ? "linear-gradient(155deg, rgba(251,191,36,0.12) 0%, rgba(10,18,38,0.97) 35%, rgba(2,6,23,1) 100%)"
          : hasCluster
            ? "linear-gradient(155deg, rgba(52,211,153,0.10) 0%, rgba(10,18,38,0.97) 35%, rgba(2,6,23,1) 100%)"
            : `linear-gradient(155deg, ${palette.start}16 0%, rgba(10,18,38,0.97) 38%, rgba(2,6,23,1) 100%)`,
      }}
    >
      {/* Header: rank + buy + ticker + score */}
      <div className="shrink-0 px-5 pt-4 pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <CardRankBadge rank={rank} />
              <a
                href={`https://robinhood.com/stocks/${row.ticker}`}
                target="_blank"
                rel="noopener noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-1 rounded-full border border-emerald-400/30 bg-emerald-400/10 px-2.5 py-1 text-[10px] font-bold text-emerald-300 transition hover:bg-emerald-400/20"
              >
                Buy ↗
              </a>
            </div>
            <h2 className="mt-1.5 text-4xl font-black tracking-tight">{row.ticker}</h2>
            {row.company_name ? (
              <p className="mt-0.5 truncate text-sm text-slate-400">
                {truncateText(row.company_name, 40)}
              </p>
            ) : null}
            {row.sector ? (
              <p className="text-[11px] text-slate-500">{row.sector}</p>
            ) : null}
          </div>
          <div className="flex shrink-0 flex-col items-end gap-1.5">
            <ScoreBadge row={row} large />
            <FreshnessBadge row={row} />
          </div>
        </div>

        {/* Score bar */}
        <div className="mt-3">
          <div className="mb-1 flex items-center justify-between">
            <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
              Score
            </span>
            <span className="text-xs font-semibold text-white">{score}/100</span>
          </div>
          <div className="h-1.5 overflow-hidden rounded-full bg-slate-800/80">
            <div
              className="h-full rounded-full"
              style={{
                width: `${score}%`,
                background: hasPlatinum
                  ? "linear-gradient(90deg, #f59e0b, #fbbf24)"
                  : `linear-gradient(90deg, ${palette.start}, ${palette.end})`,
                transition: "width 600ms ease-out",
              }}
            />
          </div>
        </div>
      </div>

      {/* Insider conviction section */}
      {hasAnyInsiderSignal ? (
        <div
          className="shrink-0 border-t px-5 py-3"
          style={{
            borderColor: hasPlatinum
              ? "rgba(251,191,36,0.20)"
              : hasCluster
                ? "rgba(52,211,153,0.15)"
                : "rgba(255,255,255,0.07)",
          }}
        >
          {/* Platinum banner */}
          {hasPlatinum && (
            <div className="mb-3 flex items-center gap-2 rounded-xl border border-amber-400/30 bg-amber-400/10 px-3 py-2">
              <span className="text-base">⚡</span>
              <div>
                <p className="text-xs font-black uppercase tracking-[0.18em] text-amber-300">
                  Platinum Conviction
                </p>
                <p className="text-[10px] text-amber-200/70">
                  Cluster buy + Congressional activity
                </p>
              </div>
            </div>
          )}

          <div className="space-y-2">
            {/* Cluster */}
            {hasCluster && (
              <div className="flex items-center justify-between rounded-xl border border-emerald-400/20 bg-emerald-400/10 px-3 py-2.5">
                <div className="flex items-center gap-2">
                  <span className="text-sm">👥</span>
                  <div>
                    <p className="text-xs font-bold text-emerald-300">
                      {clusterBuyers} insiders bought
                    </p>
                    <p className="text-[10px] text-emerald-400/70">Cluster buy</p>
                  </div>
                </div>
                {hasDisplayValue(insiderValue) && (
                  <span className="text-sm font-bold text-emerald-200">{insiderValue}</span>
                )}
              </div>
            )}

            {/* PTR */}
            {hasPtr && (
              <div className="flex items-center justify-between rounded-xl border border-cyan-400/20 bg-cyan-400/10 px-3 py-2.5">
                <div className="flex items-center gap-2">
                  <span className="text-sm">🏛</span>
                  <div>
                    <p className="text-xs font-bold text-cyan-300">Congressional buy</p>
                    <p className="text-[10px] text-cyan-400/70">PTR filing</p>
                  </div>
                </div>
                <span className="text-sm font-bold text-cyan-200">{row.ptr_amount}</span>
              </div>
            )}

            {/* Insider buy (no cluster) */}
            {hasInsiderBuy && !hasCluster && (
              <div className="flex items-center justify-between rounded-xl border border-white/10 bg-white/5 px-3 py-2.5">
                <div className="flex items-center gap-2">
                  <span className="text-sm">👤</span>
                  <div>
                    <p className="text-xs font-bold text-white">Insider buy</p>
                    <p className="text-[10px] text-slate-400">
                      {row.insider_action || "Purchase filed"}
                    </p>
                  </div>
                </div>
                {hasDisplayValue(insiderValue) && (
                  <span className="text-sm font-bold text-slate-200">{insiderValue}</span>
                )}
              </div>
            )}
          </div>
        </div>
      ) : null}

      {/* Why it's here */}
      <div className="shrink-0 border-t border-white/[0.07] px-5 py-3">
        <p className="mb-2 text-[10px] font-bold uppercase tracking-[0.22em] text-cyan-300/80">
          {hasAnyInsiderSignal ? "Also" : "Why it made the list"}
        </p>
        <ul className="space-y-1.5">
          {whyBullets.slice(0, hasAnyInsiderSignal ? 1 : 2).map((bullet, i) => (
            <li key={i} className="flex items-start gap-2 text-sm leading-5 text-white">
              <span className="mt-[5px] h-1.5 w-1.5 shrink-0 rounded-full bg-cyan-400" />
              <span>{bullet}</span>
            </li>
          ))}
        </ul>
      </div>

      {/* Price */}
      {row.price ? (
        <div className="shrink-0 border-t border-white/[0.07] px-5 py-3">
          <div className="flex items-center justify-between">
            <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
              Price
            </span>
            <span className="text-sm font-bold text-white">{formatMoney(row.price)}</span>
          </div>
        </div>
      ) : null}

      {/* CTA */}
      <div className="mt-auto shrink-0 px-5 py-4">
        <button
          type="button"
          onClick={onOpen}
          className="flex w-full items-center justify-between rounded-2xl border border-white/15 bg-white/[0.07] px-5 py-3 text-sm font-semibold text-white transition hover:border-cyan-400/40 hover:bg-cyan-400/10 active:scale-[0.98]"
        >
          <span>See full details</span>
          <span className="rounded-full bg-white/10 px-3 py-1 text-xs">Explore →</span>
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
    <div className="rounded-[1.25rem] border border-white/10 bg-black/20 p-4">
      <div className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-cyan-400/25 bg-cyan-400/10 text-sm font-bold text-cyan-200">
        {step}
      </div>
      <h3 className="mt-3 text-base font-semibold text-white">{title}</h3>
      <p className="mt-2 text-sm leading-6 text-slate-300">{body}</p>
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
    <div className="rounded-[1.25rem] border border-white/10 bg-black/20 p-4">
      <div className="flex items-center gap-3">
        <span className={`h-3 w-3 rounded-full ${color}`} />
        <span className="text-sm font-semibold text-white">{label}</span>
      </div>
      <p className="mt-2 text-sm leading-6 text-slate-300">{body}</p>
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
  const styles =
    tone === "emerald"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
      : "border-cyan-400/20 bg-cyan-400/10 text-cyan-200"

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
      className="fixed inset-x-0 bottom-0 z-40 border-t border-white/10 bg-slate-950/90 px-3 pt-3 backdrop-blur-md sm:hidden"
      style={{ paddingBottom: "calc(env(safe-area-inset-bottom) + 12px)" }}
    >
      <div className="mx-auto grid max-w-xl grid-cols-3 gap-2">
        <button
          type="button"
          onClick={onGoTop}
          className="rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-xs font-semibold text-slate-200"
        >
          Home
        </button>
        <button
          type="button"
          onClick={onGoBoard}
          className="rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-xs font-semibold text-slate-200"
        >
          Board
        </button>
        <button
          type="button"
          onClick={onGoFilters}
          className="rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-xs font-semibold text-slate-200"
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
      <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
        {label}
      </label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-3.5 text-white outline-none transition focus:border-cyan-400/40 focus:bg-slate-950 sm:py-4"
      >
        {options.map((option) => (
          <option
            key={`${label}-${option.value}`}
            value={option.value}
            className="bg-slate-900"
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
    <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-8 shadow-2xl">
      <h2 className="text-2xl font-semibold">Loading today’s ideas…</h2>
      <p className="mt-2 text-slate-400">Pulling the board together now.</p>
    </div>
  )
}

function ErrorPanel({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-red-500/30 bg-red-500/10 p-4 text-red-200">
      {message}
    </div>
  )
}

function EmptyPanel() {
  return (
    <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-8 shadow-2xl">
      <h2 className="text-2xl font-semibold">Nothing matched those filters</h2>
      <p className="mt-2 text-slate-400">
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
    { label: "Insider value", value: formatInsiderValue(row) },
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
        isSelected ? "ring-2 ring-cyan-300/25" : "hover:ring-1 hover:ring-white/10",
      ].join(" ")}
      style={{
        borderColor: isSelected ? `${palette.end}80` : `${palette.end}33`,
        background: `linear-gradient(135deg, ${palette.start}12 0%, rgba(15,23,42,0.92) 40%, rgba(2,6,23,1) 100%)`,
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
            <p className="mt-1 truncate text-sm text-slate-400">
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
              className="inline-flex items-center rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[11px] font-semibold text-slate-300 transition hover:border-cyan-400/25 hover:bg-cyan-400/10 hover:text-cyan-200"
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
          <div className="w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-4">
            <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-cyan-300/80">
              {infoTitle}
            </p>

            <p className="text-sm leading-6 text-slate-300">{infoBody}</p>

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
                className="rounded-full border border-white/10 bg-white/5 px-4 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-cyan-400/25 hover:bg-cyan-400/10 hover:text-cyan-200"
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

      <div className="mb-4 rounded-2xl border border-white/10 bg-black/20 p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300/80">
          Why it made the list
        </p>

        <ul className="mt-3 space-y-2 text-sm leading-6 text-white">
          {whyBullets.map((item, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-[6px] h-1.5 w-1.5 rounded-full bg-cyan-400" />
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

      <div className="mt-auto rounded-2xl bg-black/20 p-4">
        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
          Plain-English takeaway
        </p>

        <ul className="space-y-2 text-sm leading-6 text-slate-100">
          {takeawayBullets.map((item, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-[6px] h-1.5 w-1.5 rounded-full bg-emerald-400" />
              <span>{item}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="mt-4 flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
        <span className="text-sm text-slate-300">Open guided details</span>

        <span className="rounded-full border border-white/10 bg-white/10 px-3 py-1 text-xs font-semibold text-white">
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
  const palette = getScorePalette(score)
  const glowAnimation =
    score >= 90
      ? "scoreGlow 3.2s ease-in-out infinite"
      : score >= 80
        ? "scoreGlowCyan 3.6s ease-in-out infinite"
        : undefined

  return (
    <div
      className="w-full rounded-2xl border border-white/10 bg-white/5 p-4"
      style={glowAnimation ? { animation: glowAnimation } : undefined}
    >
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
          Overall score
        </p>
        <p className="shrink-0 text-sm font-semibold text-white">{score}/100</p>
      </div>

      <div className="h-3 overflow-hidden rounded-full bg-slate-800">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{
            width: `${score}%`,
            background: `linear-gradient(90deg, ${palette.start}, ${palette.end})`,
          }}
        />
      </div>

      {!compact ? (
        <div className="mt-2 flex items-center justify-between text-[11px] uppercase tracking-[0.14em] text-slate-500">
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
    <div className="flex min-h-[88px] w-full flex-col items-center justify-center rounded-[1.15rem] border border-white/10 bg-[linear-gradient(145deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03))] px-3 py-3 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.06),0_12px_30px_rgba(0,0,0,0.2)] backdrop-blur sm:min-h-[94px] sm:px-4">
      <p className="mb-2 break-words text-[10px] uppercase tracking-[0.22em] text-slate-400 sm:text-[11px]">
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
    <span className="inline-flex items-center rounded-full bg-cyan-400/15 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.18em] text-cyan-200">
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
  const palette = getScorePalette(score)
  const tier = getScoreTierLabel(score)

  const glowAnimation =
    score >= 90
      ? "scoreGlow 3.2s ease-in-out infinite"
      : score >= 80
        ? "scoreGlowCyan 3.6s ease-in-out infinite"
        : undefined

  return (
    <div
      className={[
        "inline-flex shrink-0 items-center gap-2 whitespace-nowrap rounded-full font-bold shadow-lg ring-1 ring-white/10",
        large ? "px-3.5 py-1.5 text-sm sm:px-4 sm:py-2" : "px-3 py-1 text-sm",
      ].join(" ")}
      style={{
        background: `linear-gradient(135deg, ${palette.start}, ${palette.end})`,
        color: palette.text,
        ...(glowAnimation ? { animation: glowAnimation } : {}),
      }}
    >
      <span>{score}</span>
      {row.ticker_score_change_7d !== null && row.ticker_score_change_7d !== undefined && (
        <span className="text-xs opacity-80">
          {row.ticker_score_change_7d >= 3 ? "↑" : row.ticker_score_change_7d <= -3 ? "↓" : ""}
        </span>
      )}
      <span className="opacity-90">• {tier}</span>
    </div>
  )
}

function FreshnessBadge({ row }: { row: UnifiedRow }) {
  const label = getFreshnessLabel(row)
  if (!label) return null

  return (
    <span className="inline-flex shrink-0 items-center whitespace-nowrap rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold text-slate-300">
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
      <div className="text-sm text-slate-400">
        Page <span className="font-semibold text-slate-200">{currentPage}</span> of{" "}
        <span className="font-semibold text-slate-200">{totalPages}</span>
      </div>

      <div className="flex flex-wrap items-center justify-center gap-2">
        <button
          type="button"
          onClick={() => onPageChange(1)}
          disabled={currentPage === 1}
          className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          First
        </button>

        <button
          type="button"
          onClick={() => onPageChange(currentPage - 1)}
          disabled={currentPage === 1}
          className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Prev
        </button>

        {pages.map((page, index) =>
          page === "ellipsis" ? (
            <span key={`ellipsis-${index}`} className="px-2 text-slate-500">
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
                  ? "border-cyan-400/30 bg-cyan-400/15 text-white"
                  : "border-white/10 bg-white/5 text-slate-300 hover:border-white/20 hover:bg-white/10 hover:text-white",
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
          className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Next
        </button>

        <button
          type="button"
          onClick={() => onPageChange(totalPages)}
          disabled={currentPage === totalPages}
          className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
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
    <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-md" onClick={onClose}>
      <div
        className="fixed inset-0 flex items-stretch justify-center p-0 sm:items-center sm:p-6"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex h-screen min-h-screen w-screen max-w-none flex-col overflow-hidden rounded-none border-0 bg-[linear-gradient(to_bottom,_#020617,_#081122_40%,_#020617)] shadow-2xl sm:h-[92vh] sm:min-h-0 sm:w-full sm:max-w-6xl sm:rounded-[2rem] sm:border sm:border-white/10">
          <div className="sticky top-0 z-20 border-b border-white/10 bg-slate-950/90 backdrop-blur">
            <div className="flex items-center justify-between gap-3 px-4 py-4 sm:px-6">
              <div className="flex min-w-0 items-center gap-2">
                <button
                  type="button"
                  onClick={onClose}
                  className="inline-flex shrink-0 items-center rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:bg-white/10 hover:text-white"
                >
                  ← Back
                </button>

                {onPrev ? (
                  <button
                    type="button"
                    onClick={onPrev}
                    className="hidden shrink-0 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white sm:inline-flex"
                  >
                    Prev
                  </button>
                ) : null}

                {onNext ? (
                  <button
                    type="button"
                    onClick={onNext}
                    className="hidden shrink-0 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white sm:inline-flex"
                  >
                    Next
                  </button>
                ) : null}
              </div>

              <div className="min-w-0 text-right">
                {positionLabel ? (
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                    {positionLabel}
                  </p>
                ) : null}
                <p className="text-sm font-semibold text-slate-200">Guided details</p>
              </div>
            </div>

            <div className="border-t border-white/10 px-4 py-4 sm:px-6">
              <div className="flex flex-wrap items-center gap-2">
                <h2 className="text-2xl font-bold sm:text-3xl">{row.ticker}</h2>
                <ScoreBadge row={row} large />
                <FreshnessBadge row={row} />
              </div>

              {row.company_name ? (
                <p className="mt-2 truncate text-sm text-slate-400">{row.company_name}</p>
              ) : null}
            </div>

            <div className="border-t border-white/10 px-4 py-3 lg:hidden">
              <div className="mb-3 flex items-center justify-center gap-2">
                {DETAIL_TABS.map((slide, index) => (
                  <button
                    key={slide}
                    type="button"
                    onClick={() => setActiveSlide(index)}
                    className={[
                      "rounded-full px-3 py-1.5 text-xs font-semibold transition",
                      index === activeSlide
                        ? "bg-cyan-400/15 text-cyan-200 ring-1 ring-cyan-400/30"
                        : "bg-white/5 text-slate-400 ring-1 ring-white/10",
                    ].join(" ")}
                  >
                    {slide}
                  </button>
                ))}
              </div>

              <p className="text-center text-xs text-slate-500">
                Tap a tab or swipe left and right
              </p>
            </div>
          </div>

          <div className="flex-1 overflow-hidden">
            <div className="hidden h-full overflow-y-auto lg:block">
              <div className="grid gap-6 p-4 sm:p-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
                <div>
                  <div className="mb-5 rounded-[1.75rem] border border-cyan-400/15 bg-[linear-gradient(135deg,rgba(34,211,238,0.12),rgba(2,6,23,0.9)_55%,rgba(2,6,23,1))] p-5">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300/80">
                      The simple version
                    </p>
                    <p className="mt-2 break-words text-xl font-semibold text-white sm:text-2xl">
                      {thesis}
                    </p>
                    <ul className="mt-3 space-y-2 break-words text-sm leading-7 text-slate-300 sm:text-base">
                      {confidenceBullets.map((item, i) => (
                        <li key={i} className="flex items-start gap-2">
                          <span className="mt-[8px] h-1.5 w-1.5 rounded-full bg-cyan-400" />
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>

                  {row.business_description ? (
                    <p className="mb-5 break-words text-sm leading-7 text-slate-300 sm:text-base">
                      {row.business_description}
                    </p>
                  ) : null}

                  <div className="mb-5">
                    <ScoreBar row={row} />
                  </div>

                  <div className="mb-5">
                    <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      Biggest score drivers
                    </p>
                    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                      {reasons.map((reason) => (
                        <ReasonCard
                          key={`${reason.label}-${reason.value}`}
                          reason={reason}
                        />
                      ))}
                    </div>
                  </div>

                  <div className="mb-5">
                    <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      Score movement
                    </p>
                    <div className="grid gap-3 sm:grid-cols-2">
                      <MovementCard label="1 Day" value={row.ticker_score_change_1d} />
                      <MovementCard label="7 Day" value={row.ticker_score_change_7d} />
                    </div>
                  </div>

                  <div className="mb-5 rounded-2xl border border-white/10 bg-white/5 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                      What stands out
                    </p>
                    <ul className="mt-3 space-y-2 break-words text-sm leading-7 text-slate-200 sm:text-base">
                      {setupBullets.map((item, i) => (
                        <li key={i} className="flex items-start gap-2">
                          <span className="mt-[8px] h-1.5 w-1.5 rounded-full bg-emerald-400" />
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>

                    {!!tags.length && (
                      <div className="mt-4 flex flex-wrap gap-2">
                        {tags.slice(0, 12).map((tag) => (
                          <TagPill key={tag} tag={tag} />
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      What confirms the setup
                    </p>
                    <div className="mt-4 grid gap-3 sm:grid-cols-2">
                      <ConfirmationRow
                        label="Price confirmation"
                        value={formatPriceConfirmation(row)}
                      />
                      <ConfirmationRow
                        label="Breakout"
                        value={
                          row.breakout_52w
                            ? "52-week breakout"
                            : row.breakout_20d
                              ? "20-day breakout"
                              : "No breakout flag"
                        }
                      />
                      <ConfirmationRow
                        label="Trend"
                        value={
                          row.trend_aligned === true
                            ? "Aligned"
                            : row.above_sma_20
                              ? "Constructive"
                              : "Mixed"
                        }
                      />
                      <ConfirmationRow
                        label="Vs market"
                        value={formatRelativeStrengthForDisplay(row)}
                      />
                      <ConfirmationRow
                        label="Volume"
                        value={formatRatio(row.volume_ratio)}
                      />
                      <ConfirmationRow
                        label="Signals stacked"
                        value={formatSignalStack(row.stacked_signal_count, row)}
                      />
                    </div>
                  </div>
                </div>

                <div className="rounded-3xl border border-white/10 bg-white/[0.04] p-4 sm:p-5">
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300/80">
                    Quick snapshot
                  </p>

                  <div className="mt-4 space-y-3">
                    <MetricRow label="Overall score" value={`${row.display_score}`} />
                    <MetricRow
                      label="Price strength score"
                      value={formatSimpleNumber(row.candidate_score)}
                    />
                    <MetricRow
                      label="Signals score"
                      value={formatSimpleNumber(row.signal_score)}
                    />
                    <MetricRow label="Why it’s here" value={row.data_source_label} />
                    <MetricRow
                      label="Confidence tier"
                      value={getConfidenceTierLabel(row.display_score)}
                    />
                    <MetricRow label="Price" value={formatMoney(row.price)} />
                    <MetricRow
                      label="Main reason"
                      value={row.primary_title || "Price strength"}
                    />
                    <MetricRow
                      label="Signal source"
                      value={formatSource(row.primary_signal_source)}
                    />
                    <MetricRow
                      label="Signal category"
                      value={getSignalCategory(row)}
                    />
                    <MetricRow label="Freshness" value={getFreshnessLabel(row)} />
                    <MetricRow
                      label="Filed at"
                      value={row.filed_at ? formatDateLong(row.filed_at) : null}
                    />
                    {row.ptr_amount ? (
                      <div className="rounded-xl border border-amber-400/15 bg-amber-400/5 px-3 py-2 text-xs leading-5 text-amber-200/70">
                        Note: Politicians have up to 45 days to report trades. The actual trade may have occurred before the date shown.
                      </div>
                    ) : null}
                    <MetricRow
                      label="Last screened"
                      value={
                        row.last_screened_at ? formatDateLong(row.last_screened_at) : null
                      }
                    />
                    <MetricRow
                      label="Signals stacked"
                      value={formatWholeNumber(row.stacked_signal_count)}
                    />
                    <MetricRow
                      label="1D score change"
                      value={formatScoreChange(row.ticker_score_change_1d)}
                    />
                    <MetricRow
                      label="7D score change"
                      value={formatScoreChange(row.ticker_score_change_7d)}
                    />
                  </div>

                  <div className="mt-6 border-t border-white/10 pt-6">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      Price and momentum
                    </p>

                    <div className="mt-4 space-y-3">
                      <MetricRow label="1D move" value={formatPercent(row.one_day_return)} />
                      <MetricRow label="5D move" value={formatPercent(row.price_return_5d)} />
                      <MetricRow label="10D move" value={formatPercent(row.return_10d)} />
                      <MetricRow label="20D move" value={formatPercent(row.price_return_20d)} />
                      <MetricRow label="Volume ratio" value={formatRatio(row.volume_ratio)} />
                      <MetricRow label="Vs market 20D" value={formatPercent(row.relative_strength_20d)} />
                      <MetricRow label="Breakout clearance" value={formatPercent(row.breakout_clearance_pct)} />
                      <MetricRow label="From 20D average" value={formatPercent(row.extension_from_sma20_pct)} />
                      <MetricRow label="Close in range" value={formatSimpleNumber(row.close_in_day_range)} />
                      <MetricRow label="Above 50DMA" value={formatBooleanLabel(row.above_50dma)} />
                      <MetricRow label="Above 20D avg" value={formatBooleanLabel(row.above_sma_20)} />
                      <MetricRow label="Trend aligned" value={formatBooleanLabel(row.trend_aligned)} />
                      <MetricRow label="Price confirmed" value={formatBooleanLabel(row.price_confirmed)} />
                    </div>
                  </div>

                  <div className="mt-6 border-t border-white/10 pt-6">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      Signals and filings
                    </p>

                    <div className="mt-4 space-y-3">
                      <MetricRow
                        label="Source forms"
                        value={row.source_forms.length ? row.source_forms.join(", ") : null}
                      />
                      <MetricRow
                        label="Accession nos"
                        value={
                          row.accession_nos.length
                            ? row.accession_nos.slice(0, 3).join(", ")
                            : null
                        }
                      />
                      <MetricRow label="Insider action" value={row.insider_action || null} />
                      <MetricRow label="Insider shares" value={formatShares(row.insider_shares)} />
                      <MetricRow label="Insider avg price" value={formatMoney(row.insider_avg_price)} />
                      <MetricRow label="Insider value" value={formatInsiderValue(row)} />
                      <MetricRow label="PTR amount" value={row.ptr_amount} />
                      <MetricRow label="Cluster buyers" value={formatWholeNumber(row.cluster_buyers)} />
                      <MetricRow label="Cluster shares" value={formatShares(row.cluster_shares)} />
                      <MetricRow label="Earnings surprise" value={formatPercent(row.earnings_surprise_pct)} />
                      <MetricRow label="Revenue growth" value={formatPercent(row.revenue_growth_pct)} />
                      <MetricRow label="Guidance support" value={formatBooleanLabel(row.guidance_flag)} />
                    </div>
                  </div>

                  <div className="mt-6 border-t border-white/10 pt-6">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                      Company basics
                    </p>

                    <div className="mt-4 space-y-3">
                      <MetricRow
                        label="Valuation"
                        value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)}
                      />
                      <MetricRow label="Market cap" value={formatMarketCap(row.market_cap)} />
                      <MetricRow label="Sector" value={row.sector || null} />
                      <MetricRow label="Industry" value={row.industry || null} />
                      <MetricRow label="Catalyst count" value={formatWholeNumber(row.catalyst_count)} />
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
                {activeSlide === 0 ? (
                  <div className="space-y-5">
                    <div className="rounded-[1.75rem] border border-cyan-400/15 bg-[linear-gradient(135deg,rgba(34,211,238,0.12),rgba(2,6,23,0.9)_55%,rgba(2,6,23,1))] p-5">
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300/80">
                        The simple version
                      </p>
                      <p className="mt-2 break-words text-xl font-semibold text-white">
                        {thesis}
                      </p>
                      <ul className="mt-3 space-y-2 text-sm leading-7 text-slate-300">
                        {confidenceBullets.map((item, i) => (
                          <li key={i} className="flex items-start gap-2">
                            <span className="mt-[8px] h-1.5 w-1.5 rounded-full bg-cyan-400" />
                            <span>{item}</span>
                          </li>
                        ))}
                      </ul>
                    </div>

                    <ScoreBar row={row} />

                    {row.business_description ? (
                      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                          Company
                        </p>
                        <p className="break-words text-sm leading-7 text-slate-300">
                          {row.business_description}
                        </p>
                      </div>
                    ) : null}

                    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                        What stands out
                      </p>
                      <ul className="mt-3 space-y-2 text-sm leading-7 text-slate-200">
                        {setupBullets.map((item, i) => (
                          <li key={i} className="flex items-start gap-2">
                            <span className="mt-[8px] h-1.5 w-1.5 rounded-full bg-emerald-400" />
                            <span>{item}</span>
                          </li>
                        ))}
                      </ul>

                      {!!tags.length && (
                        <div className="mt-4 flex flex-wrap gap-2">
                          {tags.slice(0, 10).map((tag) => (
                            <TagPill key={tag} tag={tag} />
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ) : null}

                {activeSlide === 1 ? (
                  <div className="space-y-5">
                    <div>
                      <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                        Biggest score drivers
                      </p>
                      <div className="grid gap-3">
                        {reasons.map((reason) => (
                          <ReasonCard
                            key={`${reason.label}-${reason.value}`}
                            reason={reason}
                          />
                        ))}
                      </div>
                    </div>

                    <div>
                      <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                        Score movement
                      </p>
                      <div className="grid gap-3">
                        <MovementCard label="1 Day" value={row.ticker_score_change_1d} />
                        <MovementCard label="7 Day" value={row.ticker_score_change_7d} />
                      </div>
                    </div>

                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                        What confirms the setup
                      </p>
                      <div className="mt-4 grid gap-3">
                        <ConfirmationRow
                          label="Price confirmation"
                          value={formatPriceConfirmation(row)}
                        />
                        <ConfirmationRow
                          label="Breakout"
                          value={
                            row.breakout_52w
                              ? "52-week breakout"
                              : row.breakout_20d
                                ? "20-day breakout"
                                : "No breakout flag"
                          }
                        />
                        <ConfirmationRow
                          label="Trend"
                          value={
                            row.trend_aligned === true
                              ? "Aligned"
                              : row.above_sma_20
                                ? "Constructive"
                                : "Mixed"
                          }
                        />
                        <ConfirmationRow
                          label="Vs market"
                          value={formatRelativeStrengthForDisplay(row)}
                        />
                        <ConfirmationRow
                          label="Volume"
                          value={formatRatio(row.volume_ratio)}
                        />
                        <ConfirmationRow
                          label="Signals stacked"
                          value={formatSignalStack(row.stacked_signal_count, row)}
                        />
                      </div>
                    </div>
                  </div>
                ) : null}

                {activeSlide === 2 ? (
                  <div className="space-y-5">
                    <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300/80">
                        Quick snapshot
                      </p>

                      <div className="mt-4 space-y-3">
                        <MetricRow label="Overall score" value={`${row.display_score}`} />
                        <MetricRow
                          label="Price strength score"
                          value={formatSimpleNumber(row.candidate_score)}
                        />
                        <MetricRow
                          label="Signals score"
                          value={formatSimpleNumber(row.signal_score)}
                        />
                        <MetricRow label="Why it’s here" value={row.data_source_label} />
                        <MetricRow
                          label="Confidence tier"
                          value={getConfidenceTierLabel(row.display_score)}
                        />
                        <MetricRow label="Price" value={formatMoney(row.price)} />
                        <MetricRow
                          label="Main reason"
                          value={row.primary_title || "Price strength"}
                        />
                        <MetricRow
                          label="Signal source"
                          value={formatSource(row.primary_signal_source)}
                        />
                        <MetricRow
                          label="Signal category"
                          value={getSignalCategory(row)}
                        />
                        <MetricRow label="Freshness" value={getFreshnessLabel(row)} />
                        <MetricRow
                          label="Filed at"
                          value={row.filed_at ? formatDateLong(row.filed_at) : null}
                        />
                        {row.ptr_amount ? (
                          <div className="rounded-xl border border-amber-400/15 bg-amber-400/5 px-3 py-2 text-xs leading-5 text-amber-200/70">
                            Note: Politicians have up to 45 days to report trades. The actual trade may have occurred before the date shown.
                          </div>
                        ) : null}
                        <MetricRow
                          label="Last screened"
                          value={
                            row.last_screened_at ? formatDateLong(row.last_screened_at) : null
                          }
                        />
                        <MetricRow
                          label="Signals stacked"
                          value={formatWholeNumber(row.stacked_signal_count)}
                        />
                        <MetricRow
                          label="1D score change"
                          value={formatScoreChange(row.ticker_score_change_1d)}
                        />
                        <MetricRow
                          label="7D score change"
                          value={formatScoreChange(row.ticker_score_change_7d)}
                        />
                        <MetricRow label="1D move" value={formatPercent(row.one_day_return)} />
                        <MetricRow label="5D move" value={formatPercent(row.price_return_5d)} />
                        <MetricRow label="10D move" value={formatPercent(row.return_10d)} />
                        <MetricRow label="20D move" value={formatPercent(row.price_return_20d)} />
                        <MetricRow label="Volume ratio" value={formatRatio(row.volume_ratio)} />
                        <MetricRow label="Vs market 20D" value={formatPercent(row.relative_strength_20d)} />
                        <MetricRow label="Valuation" value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)} />
                        <MetricRow label="Market cap" value={formatMarketCap(row.market_cap)} />
                        <MetricRow label="Sector" value={row.sector || null} />
                        <MetricRow label="Industry" value={row.industry || null} />
                        <MetricRow label="Source forms" value={row.source_forms.length ? row.source_forms.join(", ") : null} />
                        <MetricRow label="Insider action" value={row.insider_action || null} />
                        <MetricRow label="Insider shares" value={formatShares(row.insider_shares)} />
                        <MetricRow label="Insider avg price" value={formatMoney(row.insider_avg_price)} />
                        <MetricRow label="Insider value" value={formatInsiderValue(row)} />
                        <MetricRow label="PTR amount" value={row.ptr_amount} />
                        <MetricRow label="Cluster buyers" value={formatWholeNumber(row.cluster_buyers)} />
                        <MetricRow label="Cluster shares" value={formatShares(row.cluster_shares)} />
                        <MetricRow label="Earnings surprise" value={formatPercent(row.earnings_surprise_pct)} />
                        <MetricRow label="Revenue growth" value={formatPercent(row.revenue_growth_pct)} />
                        <MetricRow label="Guidance support" value={formatBooleanLabel(row.guidance_flag)} />
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          </div>

          <div className="border-t border-white/10 bg-slate-950/95 p-3 backdrop-blur sm:hidden">
            <div className="grid grid-cols-3 gap-2">
              <button
                type="button"
                onClick={onPrev}
                disabled={!onPrev}
                className="rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:opacity-40"
              >
                ← Prev
              </button>

              <button
                type="button"
                onClick={onClose}
                className="rounded-2xl border border-cyan-400/20 bg-cyan-400/10 px-3 py-3 text-sm font-semibold text-cyan-200 transition hover:border-cyan-400/30 hover:bg-cyan-400/15"
              >
                Back to board
              </button>

              <button
                type="button"
                onClick={onNext}
                disabled={!onNext}
                className="rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white disabled:opacity-40"
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
    <div className="flex items-center justify-between gap-4 rounded-2xl bg-white/5 px-4 py-3 text-sm">
      <span className="min-w-0 break-words text-slate-400">{label}</span>
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
    <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</p>
      <p className="mt-2 break-words text-sm font-semibold text-white">{value}</p>
    </div>
  )
}

function ReasonCard({ reason }: { reason: ReasonLine }) {
  const classes =
    reason.tone === "good"
      ? "border-emerald-400/20 bg-emerald-500/10"
      : reason.tone === "bad"
        ? "border-rose-400/20 bg-rose-500/10"
        : "border-white/10 bg-white/5"

  const textClasses =
    reason.tone === "good"
      ? "text-emerald-300"
      : reason.tone === "bad"
        ? "text-rose-300"
        : "text-slate-300"

  return (
    <div className={`rounded-2xl border p-4 ${classes}`}>
      <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{reason.label}</p>
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
      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</p>
        <p className="mt-2 text-sm font-semibold text-slate-300">No history yet</p>
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
          ? "border-emerald-400/20 bg-emerald-500/10"
          : isDown
            ? "border-rose-400/20 bg-rose-500/10"
            : "border-white/10 bg-white/5",
      ].join(" ")}
    >
      <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</p>
      <p
        className={[
          "mt-2 break-words text-sm font-semibold",
          isUp ? "text-emerald-300" : isDown ? "text-rose-300" : "text-slate-300",
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
    <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
      {pretty}
    </span>
  )
}

function getFeaturedThesis(row: UnifiedRow) {
  if (row.primary_signal_source === "breakout" && (row.volume_ratio ?? 0) >= 2) {
    return "Fresh breakout with strong buying interest"
  }

  if ((row.cluster_buyers ?? 0) >= 2) {
    return "More than one good signal is showing up at the same time"
  }

  if ((row.earnings_surprise_pct ?? 0) >= 10 || (row.revenue_growth_pct ?? 0) >= 15) {
    return "Business strength is helping support the move"
  }

  if ((row.relative_strength_20d ?? 0) >= 8) {
    return "This stock has been acting stronger than much of the market"
  }

  if ((row.candidate_score ?? 0) >= 85 && !row.has_signal_data) {
    return "A very strong price-based setup on its own"
  }

  return "Several things are lining up well right now"
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

  if ((row.cluster_buyers ?? 0) >= 2) {
    points.push("Multiple insiders are buying at current prices.")
  }

  if ((row.earnings_surprise_pct ?? 0) >= 10) {
    points.push("Recent earnings came in stronger than expected.")
  }

  if (points.length === 0) {
    points.push("Passes the quality screen for long-term hold candidates.")
  }

  return points.slice(0, 3)
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
  const score = row.display_score
  const tags = normalizeTags(row.signal_tags)

  const bullets: string[] = []

  if (row.has_candidate_data && !row.has_signal_data) {
    bullets.push(
      "This made the board mostly because the price-based setup was strong enough on its own."
    )
  }

  if ((row.cluster_buyers ?? 0) >= 2 || tags.includes("cluster-buy")) {
    bullets.push("More than one positive signal is showing up at the same time.")
  }

  if (
    row.primary_signal_source === "breakout" ||
    row.breakout_20d === true ||
    row.breakout_52w === true
  ) {
    bullets.push("The stock is pushing above important price levels.")
  }

  if ((row.volume_ratio ?? 0) >= 1.5 || tags.includes("volume-confirmed")) {
    bullets.push("Volume is elevated, which often means stronger buyer interest.")
  }

  if (
    (row.earnings_surprise_pct ?? 0) >= 10 ||
    (row.revenue_growth_pct ?? 0) >= 15 ||
    row.guidance_flag === true
  ) {
    bullets.push("Recent business results are helping support the setup.")
  }

  if ((row.relative_strength_20d ?? 0) > 0) {
    bullets.push("The stock has been outperforming the market recently.")
  }

  if (score >= 90) {
    bullets.push("Its overall score is near the top of today’s board.")
  }

  if (!bullets.length) {
    bullets.push(
      "The original signal is still holding up well enough to keep this name on the board."
    )
  }

  return bullets.slice(0, 4)
}

function getSimpleSetupBullets(row: UnifiedRow) {
  const parts: string[] = []

  if (row.breakout_20d) {
    parts.push(
      "The stock just moved above recent price levels, which can be a sign of fresh buying interest."
    )
  }

  if ((row.relative_strength_20d ?? 0) > 0) {
    parts.push("It has been outperforming the overall market recently.")
  }

  if ((row.volume_ratio ?? 0) > 1.3) {
    parts.push(
      "Trading volume is higher than usual, which suggests stronger participation."
    )
  }

  if ((row.earnings_surprise_pct ?? 0) > 0) {
    parts.push(
      "Recent earnings were better than expected, which can attract new buyers."
    )
  }

  if ((row.revenue_growth_pct ?? 0) > 10) {
    parts.push("The company is also showing solid revenue growth.")
  }

  if (parts.length === 0) {
    parts.push(
      "This stock is showing enough strength versus the rest of the market to deserve a closer look."
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
  if (row.insider_buy_value !== null && row.insider_buy_value !== undefined) {
    return formatMoney(row.insider_buy_value)
  }

  if (
    row.insider_shares !== null &&
    row.insider_shares !== undefined &&
    row.insider_avg_price !== null &&
    row.insider_avg_price !== undefined
  ) {
    return formatMoney(row.insider_shares * row.insider_avg_price)
  }

  return "—"
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

  if (s <= 69) {
    return { start: "#facc15", end: "#eab308", text: "#1f2937" }
  }

  if (s <= 79) {
    return { start: "#a3e635", end: "#84cc16", text: "#15210b" }
  }

  if (s <= 89) {
    return { start: "#22d3ee", end: "#06b6d4", text: "#06222a" }
  }

  return { start: "#22c55e", end: "#16a34a", text: "#08110a" }
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