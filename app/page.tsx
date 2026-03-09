"use client"

import { useEffect, useMemo, useState } from "react"
import { supabase } from "../lib/supabase"

type SignalRow = {
  id?: number
  ticker: string
  company_name?: string | null
  business_description?: string | null
  price?: number | null

  signal_type?: string | null
  signal_source?: string | null
  signal_category?: string | null
  signal_strength_bucket?: string | null
  signal_tags?: string[] | null

  catalyst_type?: string | null
  bias?: string | null
  board_bucket?: string | null
  source_form?: string | null
  filed_at?: string | null

  score?: number | null
  app_score?: number | null
  title?: string | null
  summary?: string | null
  filing_url?: string | null
  accession_no?: string | null

  insider_action?: string | null
  insider_shares?: number | null
  insider_avg_price?: number | null
  insider_buy_value?: number | null
  insider_signal_flavor?: string | null

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

  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  market_cap?: number | null
  sector?: string | null
  industry?: string | null

  age_days?: number | null
  score_version?: string | null
  score_updated_at?: string | null
  stacked_signal_count?: number | null
  score_breakdown?: Record<string, number> | null
  signal_reasons?: string[] | null
  score_caps_applied?: string[] | null
  freshness_bucket?: string | null

  ticker_score_change_1d?: number | null
  ticker_score_change_7d?: number | null

  updated_at?: string | null
  created_at?: string | null
}

type TickerScore = {
  id?: number
  ticker: string
  company_name?: string | null
  business_description?: string | null
  price?: number | null

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

  created_at?: string | null
  updated_at?: string | null
}

type TickerScoreHistoryRow = {
  ticker: string
  company_name?: string | null
  score_date?: string | null
  score_timestamp?: string | null
  app_score?: number | null
  raw_score?: number | null
  bias?: string | null
  board_bucket?: string | null
  score_version?: string | null
  stacked_signal_count?: number | null
  score_breakdown?: Record<string, number> | null
  signal_reasons?: string[] | null
  score_caps_applied?: string[] | null
  source_accession_nos?: string[] | null
  created_at?: string | null
}

type CoolingLeader = {
  ticker: string
  company_name: string | null
  peakScore: number
  lastScore: number
  lastScoreDate: string | null
  scoreDrop: number
  currentRow: TickerScore | null
}

type ViewMode = "buy" | "sell"
type SortPreset = "best" | "newest"
type SortBy = "score-desc" | "date-desc"
type PeFilterType = "all" | "15" | "25" | "40"
type PriceFilterType =
  | "all"
  | "under5"
  | "5to10"
  | "10to25"
  | "25to100"
  | "100plus"
  | "unknown"
type BoardMode = "buy" | "risk"
type SignalCategoryFilter =
  | "all"
  | "Insider Buys"
  | "Cluster Buys"
  | "Momentum"
  | "Institutional"
  | "Flow"
  | "Fundamental"
  | "Risk"
  | "Market Signal"
type ScoreBandFilter = "default" | "75" | "80" | "85" | "25" | "20" | "15"

type ReasonLine = {
  label: string
  value: string
  tone: "good" | "bad" | "neutral"
  weight: number
}

const CARDS_PER_PAGE = 24

function mapSignalRowToTickerScore(row: SignalRow): TickerScore {
  return {
    id: row.id,
    ticker: (row.ticker || "").trim().toUpperCase(),
    company_name: row.company_name ?? null,
    business_description: row.business_description ?? null,
    price: row.price ?? null,

    app_score: row.app_score ?? row.score ?? null,
    raw_score: row.score ?? row.app_score ?? null,
    bias: row.bias ?? null,
    board_bucket: row.board_bucket ?? null,
    signal_strength_bucket: row.signal_strength_bucket ?? null,

    score_version: row.score_version ?? "signals-fallback",
    score_updated_at: row.score_updated_at ?? row.updated_at ?? null,
    stacked_signal_count: row.stacked_signal_count ?? 1,

    score_breakdown: row.score_breakdown ?? null,
    signal_reasons: row.signal_reasons ?? null,
    score_caps_applied: row.score_caps_applied ?? null,
    signal_tags: Array.isArray(row.signal_tags) ? row.signal_tags : [],

    primary_signal_type: row.signal_type ?? null,
    primary_signal_source: row.signal_source ?? null,
    primary_signal_category: row.signal_category ?? null,
    primary_title: row.title ?? null,
    primary_summary: row.summary ?? null,

    filed_at: row.filed_at ?? null,
    accession_nos: row.accession_no ? [row.accession_no] : [],
    source_forms: row.source_form ? [row.source_form] : [],

    pe_ratio: row.pe_ratio ?? null,
    pe_forward: row.pe_forward ?? null,
    pe_type: row.pe_type ?? null,
    market_cap: row.market_cap ?? null,
    sector: row.sector ?? null,
    industry: row.industry ?? null,

    insider_action: row.insider_action ?? null,
    insider_shares: row.insider_shares ?? null,
    insider_avg_price: row.insider_avg_price ?? null,
    insider_buy_value: row.insider_buy_value ?? null,
    cluster_buyers: row.cluster_buyers ?? null,
    cluster_shares: row.cluster_shares ?? null,

    price_return_5d: row.price_return_5d ?? null,
    price_return_20d: row.price_return_20d ?? null,
    volume_ratio: row.volume_ratio ?? null,
    breakout_20d: row.breakout_20d ?? null,
    breakout_52w: row.breakout_52w ?? null,
    above_50dma: row.above_50dma ?? null,
    trend_aligned: row.trend_aligned ?? null,
    price_confirmed: row.price_confirmed ?? null,
    relative_strength_20d: row.relative_strength_20d ?? null,

    earnings_surprise_pct: row.earnings_surprise_pct ?? null,
    revenue_growth_pct: row.revenue_growth_pct ?? null,
    guidance_flag: row.guidance_flag ?? null,

    age_days: row.age_days ?? null,
    freshness_bucket: row.freshness_bucket ?? null,

    ticker_score_change_1d: row.ticker_score_change_1d ?? null,
    ticker_score_change_7d: row.ticker_score_change_7d ?? null,

    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
  }
}

export default function Home() {
  const [rows, setRows] = useState<TickerScore[]>([])
  const [historyRows, setHistoryRows] = useState<TickerScoreHistoryRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<ViewMode>("buy")
  const [sortPreset, setSortPreset] = useState<SortPreset>("best")
  const [peFilter, setPeFilter] = useState<PeFilterType>("all")
  const [priceFilter, setPriceFilter] = useState<PriceFilterType>("all")
  const [categoryFilter, setCategoryFilter] = useState<SignalCategoryFilter>("all")
  const [scoreBandFilter, setScoreBandFilter] = useState<ScoreBandFilter>("default")
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [hasInteracted, setHasInteracted] = useState(false)
  const [currentPage, setCurrentPage] = useState(1)
  const [showCoolingOff, setShowCoolingOff] = useState(false)

  const boardMode: BoardMode = viewMode === "sell" ? "risk" : "buy"

  useEffect(() => {
    let isMounted = true

    async function loadData() {
      try {
        if (!isMounted) return
        setLoading(true)
        setError(null)

        const thirtyDaysAgo = new Date()
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
        const historyStartDate = thirtyDaysAgo.toISOString().slice(0, 10)

        const [tickerScoresResponse, historyResponse] = await Promise.all([
          supabase
            .from("ticker_scores_current")
            .select("*")
            .order("app_score", { ascending: false })
            .order("filed_at", { ascending: false })
            .limit(500),
          supabase
            .from("ticker_score_history")
            .select("*")
            .gte("score_date", historyStartDate)
            .order("score_date", { ascending: false })
            .limit(5000),
        ])

        if (!isMounted) return

        let currentRows: TickerScore[] = []

        if (!tickerScoresResponse.error && (tickerScoresResponse.data?.length ?? 0) > 0) {
          currentRows = ((tickerScoresResponse.data as TickerScore[]) ?? []).filter(
            (row) => !!row.ticker
          )
        } else {
          const signalsResponse = await supabase
            .from("signals")
            .select("*")
            .order("app_score", { ascending: false })
            .order("filed_at", { ascending: false })
            .limit(500)

          if (!isMounted) return

          if (signalsResponse.error) {
            setError(
              tickerScoresResponse.error?.message ||
                signalsResponse.error.message ||
                "Error loading signals."
            )
            setRows([])
            setHistoryRows([])
            setLoading(false)
            return
          }

          currentRows = ((signalsResponse.data as SignalRow[]) ?? [])
            .map(mapSignalRowToTickerScore)
            .filter((row) => !!row.ticker)
        }

        setRows(currentRows)
        setHistoryRows(
          ((historyResponse.data as TickerScoreHistoryRow[]) ?? []).filter((row) => !!row.ticker)
        )

        if (historyResponse.error && !tickerScoresResponse.error) {
          setError(`Current board loaded, but history failed: ${historyResponse.error.message}`)
        }

        setLoading(false)
      } catch (err: any) {
        if (!isMounted) return
        setError(err?.message || "Error loading signals.")
        setRows([])
        setHistoryRows([])
        setLoading(false)
      }
    }

    loadData()

    return () => {
      isMounted = false
    }
  }, [])

  useEffect(() => {
    setCurrentPage(1)
  }, [viewMode, sortPreset, peFilter, priceFilter, categoryFilter, scoreBandFilter])

  useEffect(() => {
    if (boardMode === "risk") {
      setShowCoolingOff(false)
    }
  }, [boardMode])

  const sortBy = useMemo<SortBy>(() => {
    if (sortPreset === "newest") return "date-desc"
    return "score-desc"
  }, [sortPreset])

  const categoryFilteredRows = useMemo(() => {
    if (categoryFilter === "all") return rows
    return rows.filter((row) => getSignalCategory(row) === categoryFilter)
  }, [rows, categoryFilter])

  const processedRows = useMemo(() => {
    let filtered = categoryFilteredRows
      .filter((row) => matchesPeFilter(row, peFilter))
      .filter((row) => matchesPriceFilter(row, priceFilter))

    if (boardMode === "buy") {
      const minScore = getBuyMinScore(scoreBandFilter)
      filtered = filtered
        .filter((row) => getBoardBucket(row) === "Buy")
        .filter((row) => getEffectiveScore(row) >= minScore)
    } else {
      const maxScore = getRiskMaxScore(scoreBandFilter)
      filtered = filtered
        .filter((row) => getBoardBucket(row) === "Risk")
        .filter((row) => getEffectiveScore(row) <= maxScore)
    }

    return [...filtered].sort((a, b) => compareRows(a, b, sortBy, boardMode))
  }, [categoryFilteredRows, peFilter, priceFilter, sortBy, boardMode, scoreBandFilter])

  const uniqueProcessedRows = useMemo(
    () => bestRowPerTicker(processedRows, boardMode, sortBy),
    [processedRows, boardMode, sortBy]
  )

  const totalPages = Math.max(1, Math.ceil(uniqueProcessedRows.length / CARDS_PER_PAGE))
  const safeCurrentPage = Math.min(currentPage, totalPages)

  const paginatedRows = useMemo(() => {
    const startIndex = (safeCurrentPage - 1) * CARDS_PER_PAGE
    return uniqueProcessedRows.slice(startIndex, startIndex + CARDS_PER_PAGE)
  }, [uniqueProcessedRows, safeCurrentPage])

  const pageStart = uniqueProcessedRows.length === 0 ? 0 : (safeCurrentPage - 1) * CARDS_PER_PAGE + 1
  const pageEnd = Math.min(safeCurrentPage * CARDS_PER_PAGE, uniqueProcessedRows.length)

  const selectedRow = useMemo(() => {
    if (!selectedTicker) return null
    return rows.find((row) => row.ticker === selectedTicker) ?? null
  }, [rows, selectedTicker])

  const coolingLeaders = useMemo(() => {
    if (boardMode !== "buy") return []

    const currentTickers = new Set(uniqueProcessedRows.map((row) => row.ticker))
    const currentByTicker = new Map(rows.map((row) => [row.ticker, row]))
    const grouped = new Map<string, TickerScoreHistoryRow[]>()

    for (const row of historyRows) {
      const ticker = (row.ticker || "").trim().toUpperCase()
      if (!ticker) continue
      if (!grouped.has(ticker)) grouped.set(ticker, [])
      grouped.get(ticker)!.push(row)
    }

    const leaders: CoolingLeader[] = []

    for (const [ticker, series] of grouped.entries()) {
      const sorted = [...series].sort((a, b) => {
        const aDate = getDateValue(a.score_date ?? a.score_timestamp)
        const bDate = getDateValue(b.score_date ?? b.score_timestamp)
        return bDate - aDate
      })

      const latest = sorted[0]
      if (!latest) continue

      const peakScore = Math.max(...sorted.map((r) => Number(r.app_score ?? 0)))
      const lastScore = Number(latest.app_score ?? 0)
      const scoreDrop = peakScore - lastScore

      if (peakScore < 80) continue
      if (lastScore >= 70) continue
      if (currentTickers.has(ticker)) continue
      if (scoreDrop < 8) continue

      leaders.push({
        ticker,
        company_name: latest.company_name ?? null,
        peakScore,
        lastScore,
        lastScoreDate: latest.score_date ?? latest.score_timestamp ?? null,
        scoreDrop,
        currentRow: currentByTicker.get(ticker) ?? null,
      })
    }

    return leaders
      .sort((a, b) => {
        if (b.peakScore !== a.peakScore) return b.peakScore - a.peakScore
        if (b.lastScore !== a.lastScore) return b.lastScore - a.lastScore
        return getDateValue(b.lastScoreDate) - getDateValue(a.lastScoreDate)
      })
      .slice(0, 12)
  }, [boardMode, historyRows, rows, uniqueProcessedRows])

  const lastUpdated = getLastUpdated(rows)

  function openDetails(ticker: string) {
    setSelectedTicker(ticker)
    setHasInteracted(true)
  }

  function closeDetails() {
    setSelectedTicker(null)
  }

  function switchMode(mode: ViewMode) {
    setViewMode(mode)
    setSelectedTicker(null)
    setHasInteracted(false)
    setSortPreset("best")
    setCategoryFilter("all")
    setScoreBandFilter("default")
    setCurrentPage(1)
  }

  function resetFilters() {
    setSortPreset("best")
    setPeFilter("all")
    setPriceFilter("all")
    setCategoryFilter("all")
    setScoreBandFilter("default")
    setSelectedTicker(null)
    setHasInteracted(false)
    setCurrentPage(1)
  }

  const pageTitle = boardMode === "risk" ? "Biggest Sell Risks" : "Top Buy Opportunities"

  const searchFocusClass =
    boardMode === "risk"
      ? "focus:border-rose-400/50"
      : "focus:border-emerald-400/50"

  const resetHoverClass =
    boardMode === "risk"
      ? "hover:border-rose-400/40 hover:bg-rose-400/10 hover:text-rose-300"
      : "hover:border-emerald-400/40 hover:bg-emerald-400/10 hover:text-emerald-300"

  return (
    <main className="min-h-screen bg-gradient-to-b from-slate-950 via-slate-900 to-slate-950 text-white">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 sm:py-8 lg:px-8">
        <div className="mb-8 sm:mb-10">
          <p
            className={[
              "mb-3 inline-flex rounded-full border px-4 py-1 text-sm font-medium",
              boardMode === "risk"
                ? "border-rose-400/30 bg-rose-400/10 text-rose-300"
                : "border-emerald-400/30 bg-emerald-400/10 text-emerald-300",
            ].join(" ")}
          >
            {boardMode === "risk" ? "Sell / Risk Board" : "Buy Opportunity Board"}
          </p>

          <h1 className="text-3xl font-bold tracking-tight sm:text-4xl lg:text-5xl">
            {pageTitle}
          </h1>

          <p
            className={[
              "mt-2 text-base font-semibold sm:text-lg",
              boardMode === "risk" ? "text-rose-300" : "text-emerald-300",
            ].join(" ")}
          >
            {boardMode === "risk"
              ? "Find the risks. Avoid the damage."
              : "Find the signals. Capture the profit."}
          </p>

          <p className="mt-4 max-w-3xl text-sm leading-7 text-slate-300 sm:text-base lg:text-lg">
            {boardMode === "risk"
              ? "These are the weakest stacked ticker setups on the board based on insider selling, weak price behavior, negative corporate events, and other downside signals."
              : "These are the strongest stacked ticker setups on the board based on insider activity, momentum, earnings support, ownership signals, technical breakouts, and other bullish evidence."}
          </p>

          <div className="mt-5 flex flex-wrap items-center gap-3 text-sm text-slate-400">
            <span>
              Last updated{" "}
              <span className="font-semibold text-slate-200">{lastUpdated ?? "—"}</span>
            </span>
            <span className="hidden sm:inline">•</span>
            <span>
              Current board names{" "}
              <span className="font-semibold text-slate-200">{uniqueProcessedRows.length}</span>
            </span>
          </div>
        </div>

        <section className="mb-8 overflow-x-auto pb-1">
          <div className="inline-flex min-w-max rounded-2xl border border-white/10 bg-white/5 p-1 shadow-xl">
            <button
              onClick={() => switchMode("buy")}
              className={[
                "rounded-xl px-5 py-3 text-sm font-semibold transition sm:px-6",
                viewMode === "buy"
                  ? "bg-emerald-400 text-slate-950 shadow-lg shadow-emerald-900/30"
                  : "text-slate-300 hover:bg-white/5 hover:text-white",
              ].join(" ")}
            >
              Buy
            </button>
            <button
              onClick={() => switchMode("sell")}
              className={[
                "rounded-xl px-5 py-3 text-sm font-semibold transition sm:px-6",
                viewMode === "sell"
                  ? "bg-rose-400 text-slate-950 shadow-lg shadow-rose-900/30"
                  : "text-slate-300 hover:bg-white/5 hover:text-white",
              ].join(" ")}
            >
              Sell / Risk
            </button>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-3">
            <p className="text-sm text-slate-400">
              {viewMode === "buy"
                ? 'Buy = "highest scoring bullish setups"'
                : 'Sell / Risk = "lowest scoring bearish setups"'}
            </p>

            {boardMode === "buy" && coolingLeaders.length > 0 ? (
              <button
                type="button"
                onClick={() => setShowCoolingOff((prev) => !prev)}
                className={[
                  "rounded-full border px-4 py-2 text-sm font-semibold transition",
                  showCoolingOff
                    ? "border-amber-400/40 bg-amber-400/15 text-amber-200"
                    : "border-white/10 bg-white/5 text-slate-300 hover:border-amber-400/30 hover:bg-amber-400/10 hover:text-amber-200",
                ].join(" ")}
              >
                {showCoolingOff ? "Hide Cooling Off Listings" : "Cooling Off Listings"}
              </button>
            ) : null}
          </div>
        </section>

        <section className="mb-10">
          <div className="rounded-[2rem] border border-white/[0.1] bg-white/[0.04] p-4 shadow-2xl backdrop-blur-sm sm:p-5 lg:p-6">
            <div className="mb-5 grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(320px,0.9fr)] xl:items-end">
              <div className="min-w-0">
                <p
                  className={[
                    "text-xs font-semibold uppercase tracking-[0.18em]",
                    boardMode === "risk" ? "text-rose-300/80" : "text-emerald-300/80",
                  ].join(" ")}
                >
                  Filter the board
                </p>
                <h2 className="mt-1 text-xl font-semibold text-white sm:text-2xl">
                  Tune the signal view
                </h2>
              </div>

              <div className="max-w-2xl text-sm leading-6 text-slate-400 xl:justify-self-end xl:text-right">
                {boardMode === "risk"
                  ? "Narrow the weakest names by risk type, price, and score."
                  : "Narrow the strongest names by setup type, price, valuation, and score."}
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.1fr)_minmax(0,0.8fr)_minmax(0,0.8fr)_minmax(0,0.95fr)_140px] xl:gap-4">
              <div className="md:col-span-2 xl:col-span-1">
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Sort
                </label>
                <div className="grid grid-cols-2 gap-3">
                  <SortButton
                    active={sortPreset === "best"}
                    onClick={() => {
                      setSortPreset("best")
                      setHasInteracted(true)
                    }}
                    label={boardMode === "risk" ? "Worst First" : "Best Score"}
                  />
                  <SortButton
                    active={sortPreset === "newest"}
                    onClick={() => {
                      setSortPreset("newest")
                      setHasInteracted(true)
                    }}
                    label="Newest"
                  />
                </div>
              </div>

              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Signal Type
                </label>
                <select
                  value={categoryFilter}
                  onChange={(e) => {
                    setCategoryFilter(e.target.value as SignalCategoryFilter)
                    setHasInteracted(true)
                  }}
                  className={[
                    "w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-white outline-none transition focus:bg-slate-950",
                    searchFocusClass,
                  ].join(" ")}
                >
                  <option value="all" className="bg-slate-900">
                    All Signal Types
                  </option>
                  <option value="Insider Buys" className="bg-slate-900">
                    Insider Buys
                  </option>
                  <option value="Cluster Buys" className="bg-slate-900">
                    Cluster Buys
                  </option>
                  <option value="Momentum" className="bg-slate-900">
                    Momentum
                  </option>
                  <option value="Institutional" className="bg-slate-900">
                    Institutional
                  </option>
                  <option value="Flow" className="bg-slate-900">
                    Flow
                  </option>
                  <option value="Fundamental" className="bg-slate-900">
                    Fundamental
                  </option>
                  <option value="Risk" className="bg-slate-900">
                    Risk
                  </option>
                  <option value="Market Signal" className="bg-slate-900">
                    Market Signal
                  </option>
                </select>
              </div>

              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Price
                </label>
                <select
                  value={priceFilter}
                  onChange={(e) => {
                    setPriceFilter(e.target.value as PriceFilterType)
                    setHasInteracted(true)
                  }}
                  className={[
                    "w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-white outline-none transition focus:bg-slate-950",
                    searchFocusClass,
                  ].join(" ")}
                >
                  <option value="all" className="bg-slate-900">
                    All Prices
                  </option>
                  <option value="under5" className="bg-slate-900">
                    Under $5
                  </option>
                  <option value="5to10" className="bg-slate-900">
                    $5 to $10
                  </option>
                  <option value="10to25" className="bg-slate-900">
                    $10 to $25
                  </option>
                  <option value="25to100" className="bg-slate-900">
                    $25 to $100
                  </option>
                  <option value="100plus" className="bg-slate-900">
                    $100+
                  </option>
                  <option value="unknown" className="bg-slate-900">
                    Unknown Price
                  </option>
                </select>
              </div>

              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
                  Valuation
                </label>
                <select
                  value={peFilter}
                  onChange={(e) => {
                    setPeFilter(e.target.value as PeFilterType)
                    setHasInteracted(true)
                  }}
                  className={[
                    "w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-white outline-none transition focus:bg-slate-950",
                    searchFocusClass,
                  ].join(" ")}
                >
                  <option value="all" className="bg-slate-900">
                    All Valuations
                  </option>
                  <option value="15" className="bg-slate-900">
                    P/E ≤ 15
                  </option>
                  <option value="25" className="bg-slate-900">
                    P/E ≤ 25
                  </option>
                  <option value="40" className="bg-slate-900">
                    P/E ≤ 40
                  </option>
                </select>
              </div>

              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
                  {boardMode === "risk" ? "Risk Score" : "Buy Score"}
                </label>
                <select
                  value={scoreBandFilter}
                  onChange={(e) => {
                    setScoreBandFilter(e.target.value as ScoreBandFilter)
                    setHasInteracted(true)
                  }}
                  className={[
                    "w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-white outline-none transition focus:bg-slate-950",
                    searchFocusClass,
                  ].join(" ")}
                >
                  {boardMode === "buy" ? (
                    <>
                      <option value="default" className="bg-slate-900">
                        Buy Score ≥ 70
                      </option>
                      <option value="75" className="bg-slate-900">
                        Buy Score ≥ 75
                      </option>
                      <option value="80" className="bg-slate-900">
                        Buy Score ≥ 80
                      </option>
                      <option value="85" className="bg-slate-900">
                        Buy Score ≥ 85
                      </option>
                    </>
                  ) : (
                    <>
                      <option value="default" className="bg-slate-900">
                        Risk Score ≤ 30
                      </option>
                      <option value="25" className="bg-slate-900">
                        Risk Score ≤ 25
                      </option>
                      <option value="20" className="bg-slate-900">
                        Risk Score ≤ 20
                      </option>
                      <option value="15" className="bg-slate-900">
                        Risk Score ≤ 15
                      </option>
                    </>
                  )}
                </select>
              </div>

              <div className="md:col-span-2 xl:col-span-1 xl:self-end">
                <button
                  onClick={resetFilters}
                  className={[
                    "w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-sm font-semibold text-slate-300 transition",
                    resetHoverClass,
                  ].join(" ")}
                >
                  Reset
                </button>
              </div>
            </div>

            <div className="mt-5 rounded-2xl border border-white/10 bg-black/20 p-4">
              <div className="flex flex-col gap-3">
                <div className="flex flex-wrap items-center gap-2 text-sm text-slate-300">
                  <span className="text-slate-400">Showing</span>
                  <span className="rounded-full bg-white/10 px-3 py-1 font-semibold text-white">
                    {uniqueProcessedRows.length} {boardMode === "risk" ? "risk names" : "buy setups"}
                  </span>

                  <span className="hidden sm:inline text-slate-500">•</span>

                  <FilterChip
                    label="Category"
                    value={categoryFilter === "all" ? "All" : categoryFilter}
                    tone={boardMode}
                  />
                  <FilterChip
                    label="Price"
                    value={getPriceFilterLabel(priceFilter)}
                    tone={boardMode}
                  />
                  <FilterChip
                    label="Valuation"
                    value={getPeFilterLabel(peFilter)}
                    tone={boardMode}
                  />
                  <FilterChip
                    label={boardMode === "risk" ? "Risk Score" : "Buy Score"}
                    value={
                      boardMode === "risk"
                        ? `≤ ${getRiskMaxScore(scoreBandFilter)}`
                        : `≥ ${getBuyMinScore(scoreBandFilter)}`
                    }
                    tone={boardMode}
                  />
                </div>

                <p className="text-sm leading-6 text-slate-400">
                  {boardMode === "risk"
                    ? "This view is tuned for the weakest current setups. Tighten the filters to isolate the most dangerous names, or widen them to scan the broader risk board."
                    : "This view is tuned for the strongest current setups. Use category, price, valuation, and score filters to zero in on the kind of buy candidates you actually want."}
                </p>
              </div>
            </div>
          </div>
        </section>

        {loading && (
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-8 shadow-2xl">
            <h2 className="text-2xl font-semibold">
              Loading {boardMode === "risk" ? "risks" : "signals"}...
            </h2>
            <p className="mt-2 text-slate-400">Pulling the board together now.</p>
          </div>
        )}

        {error && !loading && (
          <div className="mb-6 rounded-2xl border border-red-500/30 bg-red-500/10 p-4 text-red-200">
            {error}
          </div>
        )}

        {!loading && !uniqueProcessedRows.length ? (
          <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-8 shadow-2xl">
            <h2 className="text-2xl font-semibold">
              No {boardMode === "risk" ? "risk names" : "buy signals"} found
            </h2>
            <p className="mt-2 text-slate-400">
              {boardMode === "risk"
                ? "Try widening the score ceiling, changing the signal category, or broadening the price filter."
                : "Try lowering the buy score floor, changing the signal category, or broadening the price filter."}
            </p>
          </div>
        ) : null}

        {!loading && !!uniqueProcessedRows.length && (
          <>
            <section className="mb-10">
              <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h2
                    className={[
                      "text-2xl font-semibold",
                      boardMode === "risk" ? "text-rose-300" : "text-emerald-300",
                    ].join(" ")}
                  >
                    {boardMode === "risk" ? "Sell / Risk Signals" : "Buy Signals"}
                  </h2>
                  <p className="mt-1 text-sm text-slate-400">
                    {boardMode === "risk"
                      ? "The worst names appear first. Click any card to open a larger detail view."
                      : "The best names appear first. Click any card to open a larger detail view."}
                  </p>
                </div>

                <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300">
                  {uniqueProcessedRows.length === 0
                    ? "No names"
                    : `${pageStart}-${pageEnd} of ${uniqueProcessedRows.length}`}
                </div>
              </div>

              <div className="grid gap-4 sm:gap-5 md:grid-cols-2 xl:grid-cols-3">
                {paginatedRows.map((row, i) => (
                  <TopSignalCard
                    key={getRowKey(row, i)}
                    row={row}
                    boardMode={boardMode}
                    isSelected={row.ticker === selectedTicker}
                    onClick={() => openDetails(row.ticker)}
                    rank={(safeCurrentPage - 1) * CARDS_PER_PAGE + i + 1}
                  />
                ))}
              </div>

              {uniqueProcessedRows.length > CARDS_PER_PAGE ? (
                <PaginationControls
                  currentPage={safeCurrentPage}
                  totalPages={totalPages}
                  onPageChange={(page) => {
                    setCurrentPage(page)
                    window.scrollTo({ top: 0, behavior: "smooth" })
                  }}
                />
              ) : null}
            </section>

            {boardMode === "buy" && coolingLeaders.length > 0 && showCoolingOff ? (
              <section className="mb-10">
                <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h2 className="text-2xl font-semibold text-amber-300">
                      Recent Leaders Cooling Off
                    </h2>
                    <p className="mt-1 text-sm text-slate-400">
                      These names scored highly in the last 30 days but are no longer on the strict
                      buy board. They may still be worth watching.
                    </p>
                  </div>
                  <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300">
                    {coolingLeaders.length} names
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                  {coolingLeaders.map((leader) => (
                    <CoolingLeaderCard
                      key={leader.ticker}
                      leader={leader}
                      onClick={() => {
                        if (leader.currentRow?.ticker) {
                          openDetails(leader.currentRow.ticker)
                        }
                      }}
                    />
                  ))}
                </div>
              </section>
            ) : null}
          </>
        )}

        <footer className="border-t border-white/10 pt-8 text-sm text-slate-500">
          Signal rankings are model-based and meant for idea generation, not guaranteed outcomes.
        </footer>
      </div>

      {selectedRow ? (
        <SignalDetailsModal
          row={selectedRow}
          boardMode={boardMode}
          onClose={closeDetails}
        />
      ) : null}
    </main>
  )
}

function SignalDetailsModal({
  row,
  boardMode,
  onClose,
}: {
  row: TickerScore
  boardMode: BoardMode
  onClose: () => void
}) {
  const tone = boardMode === "risk" ? "risk" : "buy"

  return (
    <div
      className="fixed inset-0 z-50 flex items-end justify-center bg-black/70 p-0 backdrop-blur-sm sm:items-center sm:p-6"
      onClick={onClose}
    >
      <div
        className="max-h-[92vh] w-full max-w-5xl overflow-y-auto rounded-t-[2rem] border border-white/10 bg-slate-950 shadow-2xl sm:rounded-[2rem]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="sticky top-0 z-10 flex items-center justify-between gap-4 border-b border-white/10 bg-slate-950/95 px-5 py-4 backdrop-blur sm:px-6">
          <div className="min-w-0">
            <p
              className={[
                "text-xs font-semibold uppercase tracking-[0.18em]",
                tone === "risk" ? "text-rose-300/80" : "text-emerald-300/80",
              ].join(" ")}
            >
              {tone === "risk" ? "Risk Detail" : "Signal Detail"}
            </p>
            <div className="mt-1 flex flex-wrap items-center gap-2">
              <h2 className="text-2xl font-bold sm:text-3xl">{row.ticker}</h2>
              <ScoreBadge row={row} large />
              <ConfidenceBadge row={row} />
              <FreshnessBadge row={row} />
              <SignalTypeBadge row={row} />
              <StrengthBadge bucket={row.signal_strength_bucket} />
              {has8kRisk(row) ? (
                <RiskAlertBadge catalystType={getCatalystTypeFromRow(row)} />
              ) : null}
            </div>
            {row.company_name ? (
              <p className="mt-1 text-sm text-slate-400">{row.company_name}</p>
            ) : null}
          </div>

          <button
            type="button"
            onClick={onClose}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10 hover:text-white"
          >
            Close
          </button>
        </div>

        <div className="grid gap-6 p-5 sm:p-6 lg:grid-cols-[1.2fr_0.8fr]">
          <div className="min-w-0">
            {row.business_description ? (
              <p className="mb-5 text-sm leading-7 text-slate-300 sm:text-base">
                {row.business_description}
              </p>
            ) : null}

            <div className="mb-5">
              <ScoreBar row={row} />
            </div>

            <div className="mb-5 rounded-2xl border border-white/10 bg-white/5 p-4">
              <p
                className={[
                  "text-xs font-semibold uppercase tracking-[0.18em]",
                  boardMode === "risk" ? "text-rose-300/80" : "text-emerald-300/80",
                ].join(" ")}
              >
                {boardMode === "risk" ? "Why Sell" : "Why Buy"}
              </p>
              <p className="mt-2 text-sm leading-6 text-slate-200">
                {getConfidenceStatement(row, boardMode)}
              </p>
            </div>

            <div className="mb-5">
              <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Score Drivers
              </p>
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {getTopReasonLines(row, boardMode).map((reason) => (
                  <ReasonCard key={`${reason.label}-${reason.value}`} reason={reason} />
                ))}
              </div>
            </div>

            <div className="mb-5">
              <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Score Movement
              </p>
              <div className="grid gap-3 sm:grid-cols-2">
                <MovementCard label="1 Day" value={row.ticker_score_change_1d} />
                <MovementCard label="7 Day" value={row.ticker_score_change_7d} />
              </div>
            </div>

            <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
              <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Summary
              </p>
              <p className="text-sm leading-7 text-slate-200 sm:text-base">
                {row.primary_summary || getSignalSummary(row)}
              </p>

              {!!normalizeTags(row.signal_tags).length && (
                <div className="mt-4 flex flex-wrap gap-2">
                  {normalizeTags(row.signal_tags)
                    .slice(0, 10)
                    .map((tag) => (
                      <TagPill key={tag} tag={tag} />
                    ))}
                </div>
              )}
            </div>
          </div>

          <div className="min-w-0 rounded-3xl border border-white/10 bg-white/[0.04] p-5">
            <p
              className={[
                "text-xs font-semibold uppercase tracking-[0.18em]",
                boardMode === "risk" ? "text-rose-300/80" : "text-emerald-300/80",
              ].join(" ")}
            >
              {boardMode === "risk" ? "Risk Snapshot" : "Conviction Snapshot"}
            </p>

            <div className="mt-4 space-y-3">
              <MetricRow label="Source" value={formatSource(row.primary_signal_source)} />
              <MetricRow label="Category" value={getSignalCategory(row)} />
              <MetricRow label="Board Score" value={formatScore(row)} />
              <MetricRow label="Price" value={formatMoney(row.price)} />
              <MetricRow
                label="Confidence Tier"
                value={getConfidenceTierLabel(getEffectiveScore(row))}
              />
              <MetricRow label="Freshness" value={getFreshnessLabel(row)} />
              <MetricRow label="Signals Stacked" value={formatWholeNumber(row.stacked_signal_count)} />
              <MetricRow label="1D Score Change" value={formatScoreChange(row.ticker_score_change_1d)} />
              <MetricRow label="7D Score Change" value={formatScoreChange(row.ticker_score_change_7d)} />
              <MetricRow label="Insider Action" value={row.insider_action || null} />
              <MetricRow label="Insider Shares" value={formatShares(row.insider_shares)} />
              <MetricRow label="Insider Avg Price" value={formatMoney(row.insider_avg_price)} />
              <MetricRow label="Insider Value" value={formatInsiderValue(row)} />
              <MetricRow
                label={boardMode === "risk" ? "Risk Cluster Size" : "Buying Wave Size"}
                value={formatWholeNumber(row.cluster_buyers)}
              />
              <MetricRow
                label={boardMode === "risk" ? "Cluster Shares" : "Buying Wave Shares"}
                value={formatShares(row.cluster_shares)}
              />
              <MetricRow
                label="Valuation"
                value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)}
              />
              <MetricRow label="5D Move" value={formatPercent(row.price_return_5d)} />
              <MetricRow label="20D Move" value={formatPercent(row.price_return_20d)} />
              <MetricRow label="Volume Ratio" value={formatRatio(row.volume_ratio)} />
              <MetricRow label="Earnings Surprise" value={formatPercent(row.earnings_surprise_pct)} />
              <MetricRow label="Revenue Growth" value={formatPercent(row.revenue_growth_pct)} />
              <MetricRow label="Vs Market 20D" value={formatPercent(row.relative_strength_20d)} />
              <MetricRow label="Age" value={formatAge(row.age_days)} />
              <MetricRow label="Model Version" value={row.score_version || null} />
            </div>
          </div>
        </div>
      </div>
    </div>
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
                  ? "border-white/20 bg-white/15 text-white"
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

function buildPaginationPages(currentPage: number, totalPages: number): Array<number | "ellipsis"> {
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

  return [
    1,
    "ellipsis",
    currentPage - 1,
    currentPage,
    currentPage + 1,
    "ellipsis",
    totalPages,
  ]
}

function TopSignalCard({
  row,
  onClick,
  isSelected,
  boardMode,
  rank,
}: {
  row: TickerScore
  onClick: () => void
  isSelected: boolean
  boardMode: BoardMode
  rank: number
}) {
  const tone = boardMode === "risk" ? "risk" : "buy"
  const reasons = getTopReasonChips(row, boardMode)
  const score = getEffectiveScore(row)
  const palette = getScorePalette(score)
  const extremeGlow = getExtremeCardGlow(score)

  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "flex h-full min-w-0 flex-col rounded-3xl border p-4 text-left shadow-xl transition duration-200 sm:p-5",
        isSelected
          ? "ring-2 ring-white/20"
          : "hover:-translate-y-0.5 hover:ring-1 hover:ring-white/10",
      ].join(" ")}
      style={{
        borderColor: isSelected ? `${palette.end}80` : `${palette.end}33`,
        background: `linear-gradient(135deg, ${palette.start}14 0%, rgba(15,23,42,0.92) 38%, rgba(2,6,23,1) 100%)`,
        boxShadow: extremeGlow
          ? isSelected
            ? `0 22px 54px ${extremeGlow}`
            : `0 16px 34px ${extremeGlow}`
          : isSelected
            ? "0 18px 42px rgba(0,0,0,0.35)"
            : "0 14px 30px rgba(0,0,0,0.28)",
      }}
    >
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <p
              className={[
                "text-xs uppercase tracking-[0.2em]",
                tone === "risk" ? "text-rose-300/80" : "text-emerald-300/80",
              ].join(" ")}
            >
              {formatSource(row.primary_signal_source)}
            </p>

            <RankBadge rank={rank} boardMode={boardMode} />
          </div>

          <h3 className="mt-2 truncate text-2xl font-bold sm:text-3xl">{row.ticker}</h3>
          {row.company_name ? (
            <p className="mt-1 truncate text-sm text-slate-400">{row.company_name}</p>
          ) : null}
        </div>

        <div className="flex shrink-0 flex-col items-end gap-2">
          <ScoreBadge row={row} />
          <ConfidenceBadge row={row} small />
          <FreshnessBadge row={row} />
        </div>
      </div>

      <div className="mb-4">
        <ScoreBar row={row} compact />
      </div>

      <div className="mb-4 flex flex-wrap gap-2">
        {reasons.map((reason) => (
          <ReasonChip key={reason} label={reason} boardMode={boardMode} />
        ))}
      </div>

      {row.business_description ? (
        <p className="mb-4 text-sm leading-6 text-slate-300">
          {truncateText(row.business_description, 110)}
        </p>
      ) : null}

      <div className="mb-4 grid grid-cols-2 gap-3 text-sm">
        <MiniMetric label="Price" value={formatMoney(row.price)} />
        <MiniMetric label="Vs Market" value={formatPercent(row.relative_strength_20d)} />
        <MiniMetric label="5D Move" value={formatPercent(row.price_return_5d)} />
        <MiniMetric label="Volume" value={formatRatio(row.volume_ratio)} />
      </div>

      <div className="mb-4 grid grid-cols-2 gap-3 text-sm">
        <MiniMetric label="1D Δ" value={formatScoreChange(row.ticker_score_change_1d)} />
        <MiniMetric label="7D Δ" value={formatScoreChange(row.ticker_score_change_7d)} />
        <MiniMetric label="Stacked" value={formatWholeNumber(row.stacked_signal_count)} />
        <MiniMetric label="Strength" value={row.signal_strength_bucket || "—"} />
      </div>

      <div className="mt-auto rounded-2xl bg-black/20 p-4">
        <p
          className={[
            "mb-2 text-xs font-semibold uppercase tracking-[0.18em]",
            tone === "risk"
              ? "text-rose-300/80"
              : isSelected
                ? "text-cyan-300/80"
                : "text-emerald-300/80",
          ].join(" ")}
        >
          Why this matters
        </p>
        <p className="text-sm leading-6 text-slate-100">
          {truncateText(getPlainEnglishSummary(row, boardMode), 180)}
        </p>
      </div>
    </button>
  )
}

function CoolingLeaderCard({
  leader,
  onClick,
}: {
  leader: CoolingLeader
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded-3xl border border-amber-400/20 bg-gradient-to-br from-amber-500/10 to-slate-900 p-5 text-left shadow-xl transition hover:-translate-y-0.5 hover:border-amber-400/35"
    >
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs uppercase tracking-[0.2em] text-amber-300/80">
            Recent leader
          </p>
          <h3 className="mt-2 truncate text-3xl font-bold">{leader.ticker}</h3>
          {leader.company_name ? (
            <p className="mt-1 truncate text-sm text-slate-400">{leader.company_name}</p>
          ) : null}
        </div>

        <div className="flex flex-col items-end gap-2">
          <span className="rounded-full bg-amber-400/15 px-3 py-1 text-sm font-semibold text-amber-200">
            Peak {leader.peakScore}
          </span>
          <span className="rounded-full bg-white/5 px-3 py-1 text-sm font-semibold text-slate-200">
            Now {leader.lastScore}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 text-sm">
        <MiniMetric label="Score Drop" value={`-${leader.scoreDrop}`} />
        <MiniMetric label="Last Seen" value={formatShortDate(leader.lastScoreDate)} />
      </div>

      <div className="mt-4 rounded-2xl bg-black/20 p-4">
        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-amber-300/80">
          Why watch it
        </p>
        <p className="text-sm leading-6 text-slate-100">
          This ticker was recently a strong leader but has slipped below the strict buy threshold. It may still be worth stalking for re-entry.
        </p>
      </div>
    </button>
  )
}

function SortButton({
  active,
  onClick,
  label,
}: {
  active: boolean
  onClick: () => void
  label: string
}) {
  return (
    <button
      onClick={onClick}
      className={[
        "w-full rounded-2xl border px-4 py-3 text-sm font-semibold transition",
        active
          ? "border-white/20 bg-white/10 text-white"
          : "border-white/10 bg-white/5 text-slate-300 hover:border-white/20 hover:bg-white/10",
      ].join(" ")}
    >
      {label}
    </button>
  )
}

function FilterChip({
  label,
  value,
  tone,
}: {
  label: string
  value: string
  tone: BoardMode
}) {
  return (
    <span
      className={[
        "inline-flex items-center gap-1 rounded-full border px-3 py-1 text-sm",
        tone === "risk"
          ? "border-rose-400/20 bg-rose-500/10 text-rose-200"
          : "border-emerald-400/20 bg-emerald-500/10 text-emerald-200",
      ].join(" ")}
    >
      <span className="text-slate-300">{label}:</span>
      <span className="font-semibold text-white">{value}</span>
    </span>
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
      <p className={`mt-2 text-sm font-semibold ${textClasses}`}>{reason.value}</p>
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
          "mt-2 text-sm font-semibold",
          isUp ? "text-emerald-300" : isDown ? "text-rose-300" : "text-slate-300",
        ].join(" ")}
      >
        {formatted}
      </p>
    </div>
  )
}

function MiniMetric({
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
    <div className="rounded-2xl border border-white/10 bg-white/5 p-3">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</p>
      <p className="mt-1 truncate text-sm font-semibold text-white">{value}</p>
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
      <span className="text-slate-400">{label}</span>
      <span className="max-w-[55%] truncate text-right font-semibold text-white">
        {value}
      </span>
    </div>
  )
}

function RankBadge({
  rank,
  boardMode,
}: {
  rank: number
  boardMode: BoardMode
}) {
  return (
    <span
      className={[
        "inline-flex shrink-0 items-center whitespace-nowrap rounded-full px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.18em]",
        boardMode === "risk"
          ? "bg-rose-400/15 text-rose-200"
          : "bg-emerald-400/15 text-emerald-200",
      ].join(" ")}
    >
      #{rank}
    </span>
  )
}

function FreshnessBadge({ row }: { row: TickerScore }) {
  const label = getFreshnessLabel(row)
  if (!label) return null

  return (
    <span className="inline-flex shrink-0 items-center whitespace-nowrap rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold text-slate-300">
      {label}
    </span>
  )
}

function ScoreBadge({
  row,
  large = false,
}: {
  row: TickerScore
  large?: boolean
}) {
  const score = getEffectiveScore(row)
  const palette = getScorePalette(score)

  return (
    <div
      className={[
        "inline-flex shrink-0 items-center whitespace-nowrap rounded-full font-bold shadow-lg ring-1 ring-white/10",
        large ? "px-4 py-2 text-sm" : "px-3 py-1 text-sm",
      ].join(" ")}
      style={{
        background: `linear-gradient(135deg, ${palette.start}, ${palette.end})`,
        color: palette.text,
      }}
    >
      Score {score}
    </div>
  )
}

function ConfidenceBadge({
  row,
  small = false,
}: {
  row: TickerScore
  small?: boolean
}) {
  const score = getEffectiveScore(row)
  const label = getConfidenceTierLabel(score)
  const extremeGlow = getExtremeCardGlow(score)

  return (
    <span
      className={[
        "inline-flex shrink-0 items-center whitespace-nowrap rounded-full border border-white/10 bg-white/5 font-semibold text-slate-200",
        small ? "px-2.5 py-1 text-[11px]" : "px-3 py-1.5 text-xs",
      ].join(" ")}
      style={extremeGlow ? { boxShadow: `0 0 18px ${extremeGlow}` } : undefined}
    >
      {label}
    </span>
  )
}

function ScoreBar({
  row,
  compact = false,
}: {
  row: TickerScore
  compact?: boolean
}) {
  const score = getEffectiveScore(row)
  const palette = getScorePalette(score)
  const extremeGlow = getExtremeCardGlow(score)

  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
          Signal Strength
        </p>
        <p className="text-sm font-semibold text-white">{score}/100</p>
      </div>

      <div className="h-3 overflow-hidden rounded-full bg-slate-800">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{
            width: `${score}%`,
            background: `linear-gradient(90deg, ${palette.start}, ${palette.end})`,
            boxShadow: extremeGlow ? `0 0 18px ${extremeGlow}` : undefined,
          }}
        />
      </div>

      {!compact ? (
        <div className="mt-2 flex items-center justify-between text-[11px] uppercase tracking-[0.14em] text-slate-500">
          <span>Strong Sell</span>
          <span>Neutral</span>
          <span>Strong Buy</span>
        </div>
      ) : null}
    </div>
  )
}

function SignalTypeBadge({ row }: { row: TickerScore }) {
  const config = getSignalBadgeConfig(row)

  return (
    <span
      className={[
        "inline-flex items-center rounded-full border px-3 py-1.5 text-xs font-semibold",
        config.className,
      ].join(" ")}
    >
      {config.label}
    </span>
  )
}

function RiskAlertBadge({ catalystType }: { catalystType?: string | null }) {
  return (
    <span className="inline-flex items-center rounded-full border border-rose-400/30 bg-rose-500/15 px-3 py-1.5 text-xs font-semibold text-rose-200">
      {formatCatalystBadgeLabel(catalystType)}
    </span>
  )
}

function StrengthBadge({ bucket }: { bucket?: string | null }) {
  const value = bucket ?? "Signal"
  const classes =
    value === "Strong Buy"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-300"
      : value === "Buy"
        ? "border-blue-400/30 bg-blue-400/10 text-blue-300"
        : value === "Risk"
          ? "border-rose-400/30 bg-rose-400/10 text-rose-300"
          : "border-yellow-400/30 bg-yellow-400/10 text-yellow-300"

  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1.5 text-xs font-semibold ${classes}`}
    >
      {value}
    </span>
  )
}

function TagPill({ tag }: { tag: string }) {
  const isRiskTag =
    tag === "8k-risk" ||
    tag === "legal-risk" ||
    tag === "bankruptcy-risk" ||
    tag === "financing-risk" ||
    tag === "debt-risk"

  return (
    <span
      className={[
        "rounded-full border px-3 py-1 text-xs",
        isRiskTag
          ? "border-rose-400/25 bg-rose-500/10 text-rose-200"
          : "border-white/10 bg-white/5 text-slate-300",
      ].join(" ")}
    >
      {prettifyTag(tag)}
    </span>
  )
}

function ReasonChip({
  label,
  boardMode,
}: {
  label: string
  boardMode: BoardMode
}) {
  return (
    <span
      className={[
        "rounded-full border px-3 py-1.5 text-xs font-semibold",
        boardMode === "risk"
          ? "border-rose-400/20 bg-rose-500/10 text-rose-200"
          : "border-emerald-400/20 bg-emerald-500/10 text-emerald-200",
      ].join(" ")}
    >
      {label}
    </span>
  )
}

function bestRowPerTicker(
  items: TickerScore[],
  boardMode: BoardMode,
  sortBy: SortBy
) {
  const map = new Map<string, TickerScore>()

  for (const item of items) {
    const ticker = (item.ticker || "").trim().toUpperCase()
    if (!ticker) continue

    const existing = map.get(ticker)
    if (!existing) {
      map.set(ticker, item)
      continue
    }

    const comparison = compareRows(item, existing, sortBy, boardMode)
    if (comparison < 0) {
      map.set(ticker, item)
    }
  }

  return Array.from(map.values()).sort((a, b) => compareRows(a, b, sortBy, boardMode))
}

function getRowKey(row: TickerScore, index: number) {
  const accessionKey =
    row.accession_nos?.join("-") ||
    row.filed_at ||
    row.updated_at ||
    String(index)

  return `${row.ticker}-${accessionKey}-${index}`
}

function getSignalBadgeConfig(row: TickerScore) {
  const category = getSignalCategory(row)
  const source = row.primary_signal_source

  if (source === "breakout") {
    return {
      label: "Technical Buy",
      className: "border-emerald-400/30 bg-emerald-400/10 text-emerald-300",
    }
  }
  if (category === "Cluster Buys") {
    return {
      label: "Buying Wave",
      className: "border-cyan-400/30 bg-cyan-400/10 text-cyan-300",
    }
  }
  if (category === "Insider Buys") {
    return {
      label: "Insider Buying",
      className: "border-cyan-400/30 bg-cyan-400/10 text-cyan-300",
    }
  }
  if (category === "Institutional") {
    return {
      label: "Big Investor",
      className: "border-violet-400/30 bg-violet-400/10 text-violet-300",
    }
  }
  if (category === "Momentum") {
    return {
      label: "Momentum",
      className: "border-emerald-400/30 bg-emerald-400/10 text-emerald-300",
    }
  }
  if (category === "Flow") {
    return {
      label: "Heavy Activity",
      className: "border-amber-400/30 bg-amber-400/10 text-amber-300",
    }
  }
  if (category === "Fundamental") {
    return {
      label: "Company Strength",
      className: "border-blue-400/30 bg-blue-400/10 text-blue-300",
    }
  }
  if (category === "Risk") {
    return {
      label: "Risk",
      className: "border-rose-400/30 bg-rose-400/10 text-rose-300",
    }
  }

  return {
    label: "Signal",
    className: "border-slate-400/30 bg-slate-400/10 text-slate-300",
  }
}

function getSignalCategory(row: TickerScore) {
  const storedCategory = (row.primary_signal_category ?? "").trim()
  if (storedCategory) return storedCategory
  return "Market Signal"
}

function matchesPeFilter(row: TickerScore, peFilter: PeFilterType) {
  if (peFilter === "all") return true

  const pe = row.pe_ratio ?? row.pe_forward ?? null
  if (pe === null || pe === undefined) return true

  const maxPe = Number(peFilter)
  return pe <= maxPe
}

function matchesPriceFilter(row: TickerScore, priceFilter: PriceFilterType) {
  if (priceFilter === "all") return true

  const price = row.price
  if (price === null || price === undefined) {
    return priceFilter === "unknown"
  }

  if (priceFilter === "under5") return price < 5
  if (priceFilter === "5to10") return price >= 5 && price < 10
  if (priceFilter === "10to25") return price >= 10 && price < 25
  if (priceFilter === "25to100") return price >= 25 && price < 100
  if (priceFilter === "100plus") return price >= 100

  return true
}

function getBoardBucket(row: TickerScore): "Buy" | "Risk" | "Watch" {
  if (
    row.board_bucket === "Buy" ||
    row.board_bucket === "Risk" ||
    row.board_bucket === "Watch"
  ) {
    return row.board_bucket
  }

  const score = getEffectiveScore(row)
  if (score >= 70) return "Buy"
  if (score <= 30) return "Risk"
  return "Watch"
}

function getEffectiveScore(row: TickerScore) {
  if (row.app_score !== null && row.app_score !== undefined) {
    return Math.max(0, Math.min(100, Math.round(row.app_score)))
  }

  const rawScore = row.raw_score ?? 0
  return Math.max(0, Math.min(100, Math.round(rawScore)))
}

function getBias(row: TickerScore): "Bullish" | "Neutral" | "Bearish" {
  if (row.bias === "Bullish" || row.bias === "Neutral" || row.bias === "Bearish") {
    return row.bias
  }

  const rawScore = getEffectiveScore(row)
  if (rawScore >= 70) return "Bullish"
  if (rawScore <= 30) return "Bearish"
  return "Neutral"
}

function getBuyMinScore(scoreBandFilter: ScoreBandFilter) {
  if (scoreBandFilter === "75") return 75
  if (scoreBandFilter === "80") return 80
  if (scoreBandFilter === "85") return 85
  return 70
}

function getRiskMaxScore(scoreBandFilter: ScoreBandFilter) {
  if (scoreBandFilter === "25") return 25
  if (scoreBandFilter === "20") return 20
  if (scoreBandFilter === "15") return 15
  return 30
}

function formatSource(source?: string | null) {
  if (!source) return "—"
  if (source === "form4") return "Form 4"
  if (source === "13d") return "13D"
  if (source === "13g") return "13G"
  if (source === "8k") return "8-K / Current Report"
  if (source === "earnings") return "Earnings"
  if (source === "breakout") return "Technical / Breakout"
  return source
}

function formatScore(row: TickerScore) {
  return getEffectiveScore(row).toString()
}

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`
}

function formatRatio(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${value.toFixed(2)}x`
}

function formatAge(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${value}d`
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

function formatShares(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  return `${Math.round(value).toLocaleString()}`
}

function formatInsiderValue(row: TickerScore) {
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

function formatCatalystBadgeLabel(catalystType?: string | null) {
  if (catalystType === "legal") return "Legal Risk"
  if (catalystType === "bankruptcy") return "Bankruptcy Risk"
  if (catalystType === "financing") return "Financing Risk"
  if (catalystType === "debt-restructuring") return "Debt Risk"
  return "Risk Alert"
}

function getCatalystTypeFromRow(row: TickerScore) {
  const tags = normalizeTags(row.signal_tags)
  if (tags.includes("bankruptcy-risk")) return "bankruptcy"
  if (tags.includes("legal-risk")) return "legal"
  if (tags.includes("financing-risk")) return "financing"
  if (tags.includes("debt-risk")) return "debt-restructuring"
  return null
}

function getFreshnessLabel(row: TickerScore) {
  const bucket = (row.freshness_bucket ?? "").trim()
  const age = row.age_days

  if (bucket === "today") return "Filed today"
  if (bucket === "fresh") return "Filed 1-3d ago"
  if (bucket === "recent") return "Filed 4-7d ago"
  if (bucket === "aging") return "Filed 8-14d ago"
  if (bucket === "stale") return "Older filing"

  if (typeof age === "number") {
    if (age <= 0) return "Filed today"
    if (age === 1) return "Filed 1d ago"
    return `Filed ${age}d ago`
  }

  return null
}

function formatScoreChange(value: number | null | undefined) {
  if (value === null || value === undefined) return "—"
  const rounded = Math.round(value * 10) / 10
  return `${rounded > 0 ? "+" : ""}${rounded}`
}

function has8kRisk(row: TickerScore) {
  const tags = normalizeTags(row.signal_tags)
  return (
    tags.includes("8k-risk") ||
    tags.includes("legal-risk") ||
    tags.includes("bankruptcy-risk") ||
    tags.includes("financing-risk") ||
    tags.includes("debt-risk")
  )
}

function normalizeTags(tags: TickerScore["signal_tags"]) {
  if (!tags) return []
  if (Array.isArray(tags)) return tags.filter(Boolean)
  return []
}

function prettifyTag(tag: string) {
  return tag
    .replace(/^source:/, "")
    .replace(/^8k:/, "8-K ")
    .replace("8k risk", "8-K Risk")
    .replace("legal risk", "Legal Risk")
    .replace("bankruptcy risk", "Bankruptcy Risk")
    .replace("financing risk", "Financing Risk")
    .replace("debt risk", "Debt Risk")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
}

function getSignalSummary(row: TickerScore) {
  if (row.primary_summary) return row.primary_summary
  return `${row.ticker} is showing a ${getSignalCategory(row).toLowerCase()} setup based on stacked signals, price action, and fundamentals.`
}

function getPlainEnglishSummary(row: TickerScore, boardMode: BoardMode) {
  const reasons = getTopReasonChips(row, boardMode)
  if (!reasons.length) {
    return boardMode === "risk"
      ? "Several bearish signals are stacking up here."
      : "Several bullish signals are stacking up here."
  }

  if (boardMode === "risk") {
    return `This name is showing ${reasons.join(", ").toLowerCase()}, which keeps it near the top of the sell board.`
  }

  return `This name is showing ${reasons.join(", ").toLowerCase()}, which keeps it near the top of the buy board.`
}

function getConfidenceStatement(row: TickerScore, boardMode: BoardMode) {
  const score = getEffectiveScore(row)
  const tags = normalizeTags(row.signal_tags)
  const hasCluster = (row.cluster_buyers ?? 0) >= 2 || tags.includes("cluster-buy")
  const heavyCluster = (row.cluster_buyers ?? 0) >= 3 || tags.includes("cluster-strong")
  const hasMomentum =
    (row.price_return_5d ?? 0) >= 5 ||
    tags.includes("momentum-confirmed") ||
    tags.includes("breakout-20d") ||
    tags.includes("breakout-52w") ||
    row.primary_signal_source === "breakout"
  const hasVolume = (row.volume_ratio ?? 0) >= 1.5 || tags.includes("volume-confirmed")
  const hasEarnings =
    (row.earnings_surprise_pct ?? 0) >= 10 ||
    (row.revenue_growth_pct ?? 0) >= 15 ||
    row.guidance_flag === true ||
    tags.includes("earnings-support")
  const hasValue =
    (row.pe_ratio ?? row.pe_forward ?? 999) <= 25 ||
    tags.includes("reasonable-valuation") ||
    tags.includes("deep-value")
  const hasSellCaution = tags.includes("insider-sell") || tags.includes("caution")
  const has8kRiskTag = has8kRisk(row)
  const weakPrice = (row.price_return_5d ?? 0) < 0
  const weakRelative = (row.relative_strength_20d ?? 0) < 0
  const source = formatSource(row.primary_signal_source)

  if (boardMode === "risk") {
    if (getCatalystTypeFromRow(row) === "bankruptcy") {
      return "This ticker is showing bankruptcy or going-concern stress, which is one of the clearest danger signs on the board."
    }

    if (getCatalystTypeFromRow(row) === "legal") {
      return "Legal or regulatory trouble is now part of the story, and that kind of event can keep pressure on a stock."
    }

    if (
      getCatalystTypeFromRow(row) === "financing" ||
      getCatalystTypeFromRow(row) === "debt-restructuring"
    ) {
      return "Balance-sheet pressure is showing up here, which raises the odds of dilution, stress, or further downside."
    }

    if (has8kRiskTag && weakPrice) {
      return "A bearish corporate event is landing while price action is already weak, which makes this a sharper risk setup."
    }

    if (hasSellCaution && weakPrice && weakRelative) {
      return "Selling pressure and weaker market behavior are showing up together, which makes this a stronger warning sign."
    }

    if ((row.price_return_5d ?? 0) <= -5 && hasVolume) {
      return "The stock is weakening on meaningful activity, which often makes downside risk harder to ignore."
    }

    if ((row.earnings_surprise_pct ?? 0) <= -10) {
      return "Weak earnings context is adding pressure here, and that can keep risk elevated beyond a single day."
    }

    if (hasSellCaution) {
      return "Insider selling is showing up alongside enough weakness to move this name onto the sell board."
    }

    return "Several signals are leaning the wrong way at once, which is why this name ranks near the top of the sell board."
  }

  if (heavyCluster && hasMomentum) {
    return "Multiple bullish signals are stacking together, and the stock is still acting well. That combination is much more interesting than a one-off filing."
  }

  if (row.primary_signal_source === "breakout" && hasMomentum && hasVolume) {
    return "This setup is not just filing-driven. Technical strength, participation, and broader momentum are all reinforcing the idea."
  }

  if (score >= 90 && hasCluster && hasMomentum) {
    return "This stands out because several bullish signals are lining up at once, including buying interest and strong price action."
  }

  if (hasEarnings && hasMomentum && hasVolume) {
    return "Recent earnings support, stronger price action, and elevated trading activity are all pointing in the same direction."
  }

  if (hasCluster && hasValue) {
    return "Buying interest looks meaningful, and valuation still appears reasonable enough to keep the setup attractive."
  }

  if (hasMomentum && hasValue) {
    return "The chart is acting well, and valuation still looks disciplined instead of obviously stretched."
  }

  if (hasVolume && hasMomentum) {
    return "This move is being supported by both price and participation, which usually matters more than a headline alone."
  }

  if (hasSellCaution && score >= 70) {
    return "The overall setup is still constructive, but insider selling takes some shine off the story and lowers conviction a bit."
  }

  if (score >= 85) {
    return "This name ranks highly because several independent signals are still leaning bullish at the same time."
  }

  return `This remains a constructive setup overall, with ${source} providing the original signal and the broader evaluation still holding up.`
}

function getTopReasonChips(row: TickerScore, boardMode: BoardMode) {
  const tags = normalizeTags(row.signal_tags)
  const chips: string[] = []

  if (boardMode === "risk") {
    if (hasTagOrCap(row, "bankruptcy-risk", "hard-risk-cap")) chips.push("Bankruptcy Risk")
    if (tags.includes("legal-risk")) chips.push("Legal Trouble")
    if (tags.includes("financing-risk")) chips.push("Financing Pressure")
    if (tags.includes("debt-risk")) chips.push("Debt Stress")
    if (tags.includes("insider-sell") || row.insider_action === "Sell") chips.push("Insider Selling")
    if ((row.relative_strength_20d ?? 0) < 0) chips.push("Weaker Than Market")
    if ((row.price_return_5d ?? 0) < 0) chips.push("Weak Price Action")
    if ((row.earnings_surprise_pct ?? 0) <= -10) chips.push("Weak Earnings")
    if ((row.volume_ratio ?? 0) >= 1.5 && (row.price_return_5d ?? 0) < 0) {
      chips.push("Heavy Selling Pressure")
    }
  } else {
    if (row.primary_signal_source === "breakout") chips.push("Technical Setup")
    if (tags.includes("cluster-buy") || (row.cluster_buyers ?? 0) >= 2) chips.push("Buying Wave")
    if (tags.includes("insider-buy") || row.insider_action === "Buy") chips.push("Insider Buying")
    if ((row.relative_strength_20d ?? 0) > 0) chips.push("Stronger Than Market")
    if (
      tags.includes("momentum-confirmed") ||
      tags.includes("breakout-20d") ||
      tags.includes("breakout-52w") ||
      (row.price_return_5d ?? 0) >= 5
    ) {
      chips.push("Strong Momentum")
    }
    if ((row.earnings_surprise_pct ?? 0) >= 10 || (row.revenue_growth_pct ?? 0) >= 15) {
      chips.push("Strong Earnings")
    }
    if ((row.volume_ratio ?? 0) >= 1.5) chips.push("Heavy Demand")
    if ((row.pe_ratio ?? row.pe_forward ?? 999) <= 25) chips.push("Reasonable Valuation")
    if (getSignalCategory(row) === "Institutional") chips.push("Big Investor Interest")
    if ((row.stacked_signal_count ?? 0) >= 3) chips.push("Multi-Signal Stack")
  }

  return Array.from(new Set(chips)).slice(0, 4)
}

function getTopReasonLines(row: TickerScore, boardMode: BoardMode): ReasonLine[] {
  const items: ReasonLine[] = []
  const breakdown = row.score_breakdown || {}

  const labelMap: Record<string, { label: string; tone: "good" | "bad" | "neutral" }> = {
    insider_buying: { label: "Insiders", tone: "good" },
    repeat_buying: { label: "Repeat Buying", tone: "good" },
    senior_executive_buy: { label: "Executive Buy", tone: "good" },
    momentum: { label: "Momentum", tone: "good" },
    relative_strength: { label: "Vs Market", tone: "good" },
    earnings: { label: "Earnings", tone: "good" },
    valuation: { label: "Valuation", tone: "neutral" },
    catalyst: { label: "Catalyst", tone: "good" },
    freshness: { label: "Freshness", tone: "neutral" },
    time_decay: { label: "Time Decay", tone: "bad" },
    insider_selling: { label: "Insider Selling", tone: "bad" },
    relative_strength_cap: { label: "Risk Cap", tone: "bad" },
    hard_risk_cap: { label: "Risk Cap", tone: "bad" },
    minimum_evidence_cap: { label: "Evidence Cap", tone: "bad" },
    candidate_screen: { label: "Technical Screen", tone: "good" },
    candidate_tier_bonus: { label: "Tier Bonus", tone: "good" },
    candidate_volume: { label: "Screen Volume", tone: "good" },
    candidate_momentum: { label: "Screen Momentum", tone: "good" },
    candidate_breakout: { label: "Screen Breakout", tone: "good" },
    base: { label: "Base Signal", tone: "neutral" },
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
      tone:
        value > 0
          ? boardMode === "risk" && key === "valuation"
            ? "neutral"
            : meta.tone
          : meta.tone,
      weight: Math.abs(value),
    })
  }

  if (!items.length) {
    items.push({
      label: "Model",
      value:
        getEffectiveScore(row) >= 70
          ? "Bullish signals outweigh negatives"
          : "Bearish signals outweigh positives",
      tone: getEffectiveScore(row) >= 70 ? "good" : "bad",
      weight: 1,
    })
  }

  return items.sort((a, b) => b.weight - a.weight).slice(0, 4)
}

function hasTagOrCap(row: TickerScore, tag: string, cap: string) {
  const tags = normalizeTags(row.signal_tags)
  const caps = normalizeStringArray(row.score_caps_applied)
  return tags.includes(tag) || caps.includes(cap)
}

function compareRows(
  a: TickerScore,
  b: TickerScore,
  sortBy: SortBy,
  boardMode: BoardMode
) {
  const aScore = getEffectiveScore(a)
  const bScore = getEffectiveScore(b)
  const aDate = getDateValue(a.filed_at ?? a.updated_at)
  const bDate = getDateValue(b.filed_at ?? b.updated_at)
  const aRisk = getRiskRank(a)
  const bRisk = getRiskRank(b)

  switch (sortBy) {
    case "score-desc":
      if (boardMode === "risk") {
        if (aScore !== bScore) return aScore - bScore
        if (aRisk !== bRisk) return bRisk - aRisk
        return bDate - aDate
      }
      if (aScore !== bScore) return bScore - aScore
      return bDate - aDate

    case "date-desc":
      if (aDate !== bDate) return bDate - aDate
      if (boardMode === "risk") {
        if (aScore !== bScore) return aScore - bScore
        return bRisk - aRisk
      }
      if (aScore !== bScore) return bScore - aScore
      return 0

    default:
      return 0
  }
}

function getRiskRank(row: TickerScore) {
  let rank = 0
  const tags = normalizeTags(row.signal_tags)

  if (getBias(row) === "Bearish") rank += 25
  if (getSignalCategory(row) === "Risk") rank += 20
  if (tags.includes("8k-risk")) rank += 20
  if (tags.includes("bankruptcy-risk")) rank += 24
  if (tags.includes("legal-risk")) rank += 16
  if (tags.includes("debt-risk")) rank += 14
  if (tags.includes("financing-risk")) rank += 12
  if (tags.includes("insider-sell")) rank += 16
  if ((row.price_return_5d ?? 0) <= -8) rank += 14
  else if ((row.price_return_5d ?? 0) <= -4) rank += 8
  if ((row.relative_strength_20d ?? 0) <= -8) rank += 10
  else if ((row.relative_strength_20d ?? 0) < 0) rank += 5
  if (row.signal_strength_bucket === "Risk") rank += 10
  if (row.signal_strength_bucket === "Neutral") rank += 4
  if ((row.earnings_surprise_pct ?? 0) <= -10) rank += 8
  if ((row.revenue_growth_pct ?? 0) <= -10) rank += 5

  rank += 100 - getEffectiveScore(row)

  return rank
}

function getLastUpdated(rows: TickerScore[]) {
  const dates = rows
    .map((row) => row.score_updated_at || row.updated_at)
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

function normalizeStringArray(values: string[] | null | undefined) {
  if (!values) return []
  return Array.isArray(values) ? values.filter(Boolean) : []
}

function getPeFilterLabel(peFilter: PeFilterType) {
  if (peFilter === "all") return "All"
  return `≤ ${peFilter}`
}

function getPriceFilterLabel(priceFilter: PriceFilterType) {
  if (priceFilter === "all") return "All"
  if (priceFilter === "under5") return "Under $5"
  if (priceFilter === "5to10") return "$5 to $10"
  if (priceFilter === "10to25") return "$10 to $25"
  if (priceFilter === "25to100") return "$25 to $100"
  if (priceFilter === "100plus") return "$100+"
  return "Unknown"
}

function getDateValue(dateString: string | null | undefined) {
  if (!dateString) return 0
  const timestamp = new Date(dateString).getTime()
  return Number.isNaN(timestamp) ? 0 : timestamp
}

function formatShortDate(dateString: string | null | undefined) {
  if (!dateString) return "—"
  const date = new Date(dateString)
  if (Number.isNaN(date.getTime())) return "—"
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
  }).format(date)
}

function getScorePalette(score: number) {
  const s = Math.max(0, Math.min(100, score))

  if (s <= 30) {
    return {
      start: "#ef4444",
      end: "#dc2626",
      glow: "rgba(239,68,68,0.38)",
      text: "#fff7f7",
    }
  }

  if (s <= 49) {
    return {
      start: "#f97316",
      end: "#ea580c",
      glow: "rgba(249,115,22,0.34)",
      text: "#fffaf5",
    }
  }

  if (s <= 69) {
    return {
      start: "#facc15",
      end: "#eab308",
      glow: "rgba(250,204,21,0.30)",
      text: "#1f2937",
    }
  }

  if (s <= 79) {
    return {
      start: "#a3e635",
      end: "#84cc16",
      glow: "rgba(163,230,53,0.30)",
      text: "#15210b",
    }
  }

  if (s <= 89) {
    return {
      start: "#4ade80",
      end: "#22c55e",
      glow: "rgba(74,222,128,0.32)",
      text: "#0b1a10",
    }
  }

  return {
    start: "#22c55e",
    end: "#16a34a",
    glow: "rgba(34,197,94,0.36)",
    text: "#08110a",
  }
}

function getExtremeCardGlow(score: number) {
  if (score === 100) return "rgba(34,197,94,0.40)"
  if (score === 0) return "rgba(239,68,68,0.40)"
  return null
}

function getConfidenceTierLabel(score: number) {
  if (score >= 90) return "Elite"
  if (score >= 80) return "Strong"
  if (score >= 70) return "Good"
  if (score >= 31) return "Mixed"
  if (score >= 11) return "Weak"
  return "Danger"
}

function truncateText(value: string | null | undefined, maxLength: number) {
  if (!value) return ""
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength).trim()}…`
}