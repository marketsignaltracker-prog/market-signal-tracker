"use client"

import { useEffect, useMemo, useState, type ReactNode } from "react"
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

type MiniMetricItem = {
  label: string
  value: string
  tooltip?: string
}

type ReasonLine = {
  label: string
  value: string
  tone: "good" | "bad" | "neutral"
  weight: number
}

const CARDS_PER_PAGE = 18

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
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState(1)

  const [priceFilter, setPriceFilter] = useState<PriceFilterType>("all")
  const [peFilter, setPeFilter] = useState<PeFilterType>("all")
  const [freshnessFilter, setFreshnessFilter] = useState<FreshnessFilterType>("all")
  const [scoreFilter, setScoreFilter] = useState<ScoreFilterType>("80")
  const [sectorFilter, setSectorFilter] = useState<SectorFilterType>("all")

  useEffect(() => {
    let isMounted = true

    async function loadData() {
      try {
        if (!isMounted) return
        setLoading(true)
        setError(null)

        const tickerScoresResponse = await supabase
          .from("ticker_scores_current")
          .select("*")
          .gte("app_score", 70)
          .order("app_score", { ascending: false })
          .order("filed_at", { ascending: false })
          .limit(500)

        if (!isMounted) return

        let currentRows: TickerScore[] = []

        if (!tickerScoresResponse.error && (tickerScoresResponse.data?.length ?? 0) > 0) {
          currentRows = ((tickerScoresResponse.data as TickerScore[]) ?? []).filter(
            (row) => !!row.ticker && getEffectiveScore(row) >= 70
          )
        } else {
          const signalsResponse = await supabase
            .from("signals")
            .select("*")
            .gte("app_score", 70)
            .order("app_score", { ascending: false })
            .order("filed_at", { ascending: false })
            .limit(500)

          if (!isMounted) return

          if (signalsResponse.error) {
            setError(
              tickerScoresResponse.error?.message ||
                signalsResponse.error.message ||
                "Error loading strong buys."
            )
            setRows([])
            setLoading(false)
            return
          }

          currentRows = ((signalsResponse.data as SignalRow[]) ?? [])
            .map(mapSignalRowToTickerScore)
            .filter((row) => !!row.ticker && getEffectiveScore(row) >= 70)
        }

        setRows(bestRowPerTicker(currentRows))
        setLoading(false)
      } catch (err: any) {
        if (!isMounted) return
        setError(err?.message || "Error loading strong buys.")
        setRows([])
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
  }, [priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter])

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
      .sort((a, b) => compareRows(a, b))
  }, [rows, priceFilter, peFilter, freshnessFilter, scoreFilter, sectorFilter])

  const featuredRows = useMemo(() => filteredRows.slice(0, 3), [filteredRows])

  const remainingRows = useMemo(() => filteredRows.slice(3), [filteredRows])

  const totalPages = Math.max(1, Math.ceil(remainingRows.length / CARDS_PER_PAGE))
  const safeCurrentPage = Math.min(currentPage, totalPages)

  const paginatedRows = useMemo(() => {
    const startIndex = (safeCurrentPage - 1) * CARDS_PER_PAGE
    return remainingRows.slice(startIndex, startIndex + CARDS_PER_PAGE)
  }, [remainingRows, safeCurrentPage])

  const pageStart = remainingRows.length === 0 ? 0 : (safeCurrentPage - 1) * CARDS_PER_PAGE + 1
  const pageEnd = Math.min(safeCurrentPage * CARDS_PER_PAGE, remainingRows.length)

  const selectedRow = useMemo(() => {
    if (!selectedTicker) return null
    return rows.find((row) => row.ticker === selectedTicker) ?? null
  }, [rows, selectedTicker])

  const lastUpdated = getLastUpdated(rows)
  const strongBuyCount = filteredRows.length
  const eliteCount = filteredRows.filter((row) => getEffectiveScore(row) >= 90).length
  const avgScore = filteredRows.length
    ? Math.round(
        filteredRows.reduce((sum, row) => sum + getEffectiveScore(row), 0) / filteredRows.length
      )
    : 0

  function openDetails(ticker: string) {
    setSelectedTicker(ticker)
  }

  function closeDetails() {
    setSelectedTicker(null)
  }

  function resetFilters() {
    setPriceFilter("all")
    setPeFilter("all")
    setFreshnessFilter("all")
    setScoreFilter("80")
    setSectorFilter("all")
    setSelectedTicker(null)
    setCurrentPage(1)
  }

  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(16,185,129,0.14),_transparent_22%),linear-gradient(to_bottom,_#020617,_#0f172a_45%,_#020617)] text-white">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 sm:py-8 lg:px-8">
                        <section className="relative overflow-hidden rounded-[2.5rem] border border-emerald-400/15 bg-white/[0.04] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.45)] backdrop-blur-sm sm:p-8 lg:p-10">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,_rgba(34,197,94,0.14),_transparent_28%),radial-gradient(circle_at_bottom_left,_rgba(234,179,8,0.08),_transparent_28%)]" />
          <div className="relative">
            <p className="inline-flex rounded-full border border-emerald-400/25 bg-emerald-400/10 px-4 py-1 text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">
              Daily Strong Buy List
            </p>

            <div className="mt-6 grid gap-8 lg:grid-cols-[1.15fr_0.85fr] lg:items-end">
              <div>
                <h1 className="max-w-4xl text-4xl font-bold tracking-tight sm:text-5xl lg:text-6xl">
                  Your cheat sheet for what to buy right now
                </h1>

                <p className="mt-4 max-w-3xl text-base leading-8 text-slate-300 sm:text-lg">
                  We give you a simple daily shortlist of the strongest stocks right now.
                  No huge watchlists. No confusing clutter. Just the clearest strong-buy names that deserve a Strong Buy rating today.
                </p>

                <div className="mt-6 rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-slate-300">
                  Updated daily and built to answer one question:
                  <span className="ml-1 font-semibold text-white">What should I buy right now?</span>
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1 xl:grid-cols-2">
                <HeroStat
                  label="Buy Now Stocks"
                  value={loading ? "…" : String(strongBuyCount)}
                  subtext="Current daily shortlist"
                />
                <HeroStat
                  label="Best of the Best"
                  value={loading ? "…" : String(eliteCount)}
                  subtext="Top-rated names today"
                />
              </div>
            </div>

            <div className="mt-8 grid gap-3 rounded-[2rem] border border-white/10 bg-black/20 p-4 sm:grid-cols-2 xl:grid-cols-4">
              <TrustPill title="Updated daily" text="Fresh names, not last months hype" />
              <TrustPill title="Only strong buys" text="Weak names don't make the cut" />
              <TrustPill title="Easy to understand" text="Built like a cheat sheet" />
              <TrustPill title="Made for action" text="Focused on what to buy right now" />
            </div>

            <div className="mt-5 flex flex-wrap items-center gap-3 text-sm text-slate-400">
              <span>
                Last updated{" "}
                <span className="font-semibold text-slate-100">{lastUpdated ?? "—"}</span>
              </span>
              <span className="hidden sm:inline">•</span>
              <span>
                Showing only{" "}
                <span className="font-semibold text-slate-100">strong buy</span> names
              </span>
            </div>
          </div>
        </section>

        <section className="mt-8 grid gap-4 lg:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] p-5 shadow-xl backdrop-blur-sm sm:p-6">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
              We Do All The Research
            </p>
            <h2 className="mt-2 text-2xl font-semibold text-white sm:text-3xl">
              What and when to buy made simple...
            </h2>
            <div className="mt-5 grid gap-3 sm:grid-cols-2">
              <WhySubscribeCard
                title="No need to learn stock screening"
                text="Most people don't want to study thousands of stocks. They just want the best opportunities right now."
              />
              <WhySubscribeCard
                title="You do not need a giant watchlist"
                text="We narrow things down fast so you can focus on the strongest buy-now stocks."
              />
              <WhySubscribeCard
                title="No need to speak market jargon"
                text="The goal is clarity. We uncover the names that are strongest right now in plain English."
              />
              <WhySubscribeCard
                title="You just need a daily shortlist"
                text="Think of this like your daily cheat sheet for the stocks most worth buying right now."
              />
            </div>
          </div>

          <div className="rounded-[2rem] border border-emerald-400/15 bg-emerald-400/[0.06] p-5 shadow-xl backdrop-blur-sm sm:p-6">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
              What makes the list
            </p>
            <h2 className="mt-2 text-2xl font-semibold text-white sm:text-3xl">
              Not every stock makes it here
            </h2>
            <div className="mt-5 space-y-3">
              <MethodBullet text="The stock needs to look strong right now" />
              <MethodBullet text="It needs real buying interest, not random noise" />
              <MethodBullet text="It needs enough support to qualify as a strong buy" />
              <MethodBullet text="Weak or stale names are left off the page" />
              <MethodBullet text="The goal is a short list you can actually use today" />
            </div>
          </div>
        </section>

        <section className="mt-8 rounded-[2rem] border border-white/10 bg-white/[0.04] p-5 shadow-xl backdrop-blur-sm sm:p-6">
          <div className="mb-5 flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                Refine today’s setups
              </p>
              <h2 className="mt-1 text-2xl font-semibold text-white sm:text-3xl">
                The board is already curated for you
              </h2>
              <p className="mt-2 max-w-3xl text-sm leading-7 text-slate-400 sm:text-base">
                Use filters to tighten the shortlist even further without turning the page into a
                complicated terminal.
              </p>
            </div>

            <button
              onClick={resetFilters}
              className="inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-slate-300 transition hover:border-emerald-400/30 hover:bg-emerald-400/10 hover:text-emerald-200"
            >
              Reset filters
            </button>
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <FilterSelect
              label="Price"
              value={priceFilter}
              onChange={(value) => setPriceFilter(value as PriceFilterType)}
              options={[
                { value: "all", label: "All prices" },
                { value: "under10", label: "Under $10" },
                { value: "10to25", label: "$10 to $25" },
                { value: "25to100", label: "$25 to $100" },
                { value: "100plus", label: "$100+" },
              ]}
            />

            <FilterSelect
              label="Valuation"
              value={peFilter}
              onChange={(value) => setPeFilter(value as PeFilterType)}
              options={[
                { value: "all", label: "All valuations" },
                { value: "20", label: "P/E ≤ 20" },
                { value: "30", label: "P/E ≤ 30" },
                { value: "50", label: "P/E ≤ 50" },
              ]}
            />

            <FilterSelect
              label="Freshness"
              value={freshnessFilter}
              onChange={(value) => setFreshnessFilter(value as FreshnessFilterType)}
              options={[
                { value: "all", label: "All freshness" },
                { value: "today", label: "Today" },
                { value: "3d", label: "Last 3 days" },
                { value: "7d", label: "Last 7 days" },
                { value: "14d", label: "Last 14 days" },
              ]}
            />

            <FilterSelect
              label="Conviction"
              value={scoreFilter}
              onChange={(value) => setScoreFilter(value as ScoreFilterType)}
              options={[
                { value: "all", label: "All scores" },
                { value: "70", label: "70+" },
                { value: "75", label: "75+" },
                { value: "80", label: "80+" },
                { value: "85", label: "85+" },
                { value: "90", label: "90+" },
              ]}
            />

            <FilterSelect
              label="Sector"
              value={sectorFilter}
              onChange={(value) => setSectorFilter(value)}
              options={sectorOptions.map((sector) => ({
                value: sector,
                label: sector === "all" ? "All sectors" : sector,
              }))}
            />
          </div>

          <div className="mt-5 flex flex-wrap items-center gap-2 text-sm text-slate-300">
            <BoardChip label="Visible strong buys" value={String(filteredRows.length)} />
            <BoardChip label="Elite" value={String(filteredRows.filter((r) => getEffectiveScore(r) >= 90).length)} />
            <BoardChip label="Avg score" value={filteredRows.length ? String(avgScore) : "—"} />
            <BoardChip
              label="Freshest"
              value={
                filteredRows[0]?.filed_at
                  ? formatDateShort(filteredRows[0].filed_at)
                  : "—"
              }
            />
          </div>
        </section>

        <section className="mt-8">
          <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                Featured today
              </p>
              <h2 className="mt-1 text-2xl font-semibold text-white sm:text-3xl">
                Today’s top strong-buy setups
              </h2>
              <p className="mt-2 text-sm leading-7 text-slate-400 sm:text-base">
                These are the strongest names on the board right now, ranked by conviction.
              </p>
            </div>

            <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300">
              {loading ? "Loading board…" : `${filteredRows.length} names on the shortlist`}
            </div>
          </div>

          {loading ? (
            <LoadingPanel />
          ) : error ? (
            <ErrorPanel message={error} />
          ) : !filteredRows.length ? (
            <EmptyPanel />
          ) : (
            <>
              <div className="grid gap-5 lg:grid-cols-3">
                {featuredRows.map((row, index) => (
                  <FeaturedStrongBuyCard
                    key={`${row.ticker}-${index}`}
                    row={row}
                    rank={index + 1}
                    onClick={() => openDetails(row.ticker)}
                  />
                ))}
              </div>

              <section id="board" className="mt-8">
                <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                      Full shortlist
                    </p>
                    <h2 className="mt-1 text-2xl font-semibold text-white sm:text-3xl">
                      More high-conviction setups
                    </h2>
                    <p className="mt-2 text-sm leading-7 text-slate-400 sm:text-base">
                      Ranked automatically by conviction so the strongest current opportunities rise
                      to the top.
                    </p>
                  </div>

                  <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300">
                    {remainingRows.length === 0
                      ? "No additional names"
                      : `${pageStart}-${pageEnd} of ${remainingRows.length}`}
                  </div>
                </div>

                <div className="grid gap-4 sm:gap-5 md:grid-cols-2 xl:grid-cols-3">
                  {paginatedRows.map((row, i) => (
                    <TopSignalCard
                      key={getRowKey(row, i)}
                      row={row}
                      isSelected={row.ticker === selectedTicker}
                      onClick={() => openDetails(row.ticker)}
                      rank={featuredRows.length + (safeCurrentPage - 1) * CARDS_PER_PAGE + i + 1}
                    />
                  ))}
                </div>

                {remainingRows.length > CARDS_PER_PAGE ? (
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
            </>
          )}
        </section>

        <footer className="mt-10 border-t border-white/10 pt-8 text-sm text-slate-500">
          Strong-buy rankings are model-based and meant for idea generation, not guaranteed
          outcomes.
        </footer>
      </div>

      {selectedRow ? <SignalDetailsModal row={selectedRow} onClose={closeDetails} /> : null}
    </main>
  )
}

function HeroStat({
  label,
  value,
  subtext,
}: {
  label: string
  value: string
  subtext: string
}) {
  return (
    <div className="rounded-[1.75rem] border border-white/10 bg-black/20 p-5">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">
        {label}
      </p>
      <p className="mt-3 text-5xl font-bold tracking-tight text-white">{value}</p>
      <p className="mt-2 text-sm text-slate-400">{subtext}</p>
    </div>
  )
}

function TrustPill({ title, text }: { title: string; text: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
      <p className="text-sm font-semibold text-white">{title}</p>
      <p className="mt-1 text-sm text-slate-400">{text}</p>
    </div>
  )
}

function WhySubscribeCard({ title, text }: { title: string; text: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
      <p className="text-base font-semibold text-white">{title}</p>
      <p className="mt-2 text-sm leading-6 text-slate-400">{text}</p>
    </div>
  )
}

function MethodBullet({ text }: { text: string }) {
  return (
    <div className="flex items-start gap-3 rounded-2xl border border-white/10 bg-black/20 px-4 py-3">
      <span className="mt-1 inline-block h-2.5 w-2.5 shrink-0 rounded-full bg-emerald-400" />
      <p className="text-sm leading-6 text-slate-300">{text}</p>
    </div>
  )
}

function UnlockCard({ title, text }: { title: string; text: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
      <p className="text-sm font-semibold text-white">{title}</p>
      <p className="mt-1 text-sm text-slate-400">{text}</p>
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
        className="w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-white outline-none transition focus:border-emerald-400/40 focus:bg-slate-950"
      >
        {options.map((option) => (
          <option key={`${label}-${option.value}`} value={option.value} className="bg-slate-900">
            {option.label}
          </option>
        ))}
      </select>
    </div>
  )
}

function BoardChip({ label, value }: { label: string; value: string }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-emerald-400/20 bg-emerald-500/10 px-3 py-1 text-sm text-emerald-200">
      <span className="text-slate-300">{label}:</span>
      <span className="font-semibold text-white">{value}</span>
    </span>
  )
}

function LoadingPanel() {
  return (
    <div className="rounded-3xl border border-slate-800 bg-slate-900/70 p-8 shadow-2xl">
      <h2 className="text-2xl font-semibold">Loading today’s strong buys…</h2>
      <p className="mt-2 text-slate-400">Pulling the shortlist together now.</p>
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
      <h2 className="text-2xl font-semibold">No strong buys found</h2>
      <p className="mt-2 text-slate-400">
        Try widening freshness, valuation, or score filters to reveal more names.
      </p>
    </div>
  )
}

function bestRowPerTicker(items: TickerScore[]) {
  const map = new Map<string, TickerScore>()

  for (const item of items) {
    const ticker = (item.ticker || "").trim().toUpperCase()
    if (!ticker) continue

    const existing = map.get(ticker)
    if (!existing) {
      map.set(ticker, item)
      continue
    }

    const comparison = compareRows(item, existing)
    if (comparison < 0) {
      map.set(ticker, item)
    }
  }

  return Array.from(map.values()).sort((a, b) => compareRows(a, b))
}

function compareRows(a: TickerScore, b: TickerScore) {
  const aScore = getEffectiveScore(a)
  const bScore = getEffectiveScore(b)
  const aDate = getDateValue(a.filed_at ?? a.updated_at)
  const bDate = getDateValue(b.filed_at ?? b.updated_at)

  if (aScore !== bScore) return bScore - aScore
  return bDate - aDate
}

function matchesPriceFilter(row: TickerScore, priceFilter: PriceFilterType) {
  if (priceFilter === "all") return true

  const price = row.price
  if (price === null || price === undefined) return false

  if (priceFilter === "under10") return price < 10
  if (priceFilter === "10to25") return price >= 10 && price < 25
  if (priceFilter === "25to100") return price >= 25 && price < 100
  if (priceFilter === "100plus") return price >= 100

  return true
}

function matchesPeFilter(row: TickerScore, peFilter: PeFilterType) {
  if (peFilter === "all") return true

  const pe = row.pe_ratio ?? row.pe_forward ?? null
  if (pe === null || pe === undefined) return true

  const maxPe = Number(peFilter)
  return pe <= maxPe
}

function matchesFreshnessFilter(row: TickerScore, freshnessFilter: FreshnessFilterType) {
  if (freshnessFilter === "all") return true

  const age = row.age_days
  if (age === null || age === undefined) return false

  if (freshnessFilter === "today") return age <= 0
  if (freshnessFilter === "3d") return age <= 3
  if (freshnessFilter === "7d") return age <= 7
  if (freshnessFilter === "14d") return age <= 14

  return true
}

function matchesScoreFilter(row: TickerScore, scoreFilter: ScoreFilterType) {
  if (scoreFilter === "all") return true
  return getEffectiveScore(row) >= Number(scoreFilter)
}

function matchesSectorFilter(row: TickerScore, sectorFilter: SectorFilterType) {
  if (sectorFilter === "all") return true
  return (row.sector || "").trim() === sectorFilter
}

function getEffectiveScore(row: TickerScore) {
  if (row.app_score !== null && row.app_score !== undefined) {
    return Math.max(0, Math.min(100, Math.round(row.app_score)))
  }

  const rawScore = row.raw_score ?? 0
  return Math.max(0, Math.min(100, Math.round(rawScore)))
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

function getDateValue(dateString: string | null | undefined) {
  if (!dateString) return 0
  const timestamp = new Date(dateString).getTime()
  return Number.isNaN(timestamp) ? 0 : timestamp
}

function formatDateShort(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "—"

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
  }).format(date)
}

function getRowKey(row: TickerScore, index: number) {
  const accessionKey =
    row.accession_nos?.join("-") ||
    row.filed_at ||
    row.updated_at ||
    String(index)

  return `${row.ticker}-${accessionKey}-${index}`
}

function Tooltip({
  content,
  children,
}: {
  content: ReactNode
  children: ReactNode
}) {
  const [open, setOpen] = useState(false)

  return (
    <span
      className="relative inline-flex"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onClick={(e) => {
        e.stopPropagation()
        setOpen((prev) => !prev)
      }}
    >
      <span className="inline-flex">{children}</span>

      <span
        className={[
          "pointer-events-none absolute bottom-[calc(100%+10px)] left-1/2 z-40 w-max max-w-[260px] -translate-x-1/2 rounded-xl border border-white/10 bg-slate-950/95 px-3 py-2 text-left text-xs leading-5 text-slate-200 shadow-2xl backdrop-blur transition",
          open ? "translate-y-0 opacity-100" : "translate-y-1 opacity-0",
        ].join(" ")}
      >
        {content}
      </span>
    </span>
  )
}

function FeaturedStrongBuyCard({
  row,
  rank,
  onClick,
}: {
  row: TickerScore
  rank: number
  onClick: () => void
}) {
  const score = getEffectiveScore(row)
  const palette = getScorePalette(score)
  const reasons = getTopReasonChips(row)
  const thesis = getFeaturedThesis(row)
  const miniMetrics: MiniMetricItem[] = [
    {
      label: "Price",
      value: formatMoney(row.price),
      tooltip: getMiniMetricTooltip("Price", row),
    },
    {
      label: "5D Move",
      value: formatPercent(row.price_return_5d),
      tooltip: getMiniMetricTooltip("5D Move", row),
    },
    {
      label: "20D Move",
      value: formatPercent(row.price_return_20d),
      tooltip: getMiniMetricTooltip("20D Move", row),
    },
    {
      label: "Volume",
      value: formatRatio(row.volume_ratio),
      tooltip: getMiniMetricTooltip("Volume", row),
    },
    {
      label: "Vs Market",
      value: formatPercent(row.relative_strength_20d),
      tooltip: getMiniMetricTooltip("Vs Market", row),
    },
    {
      label: "Stacked",
      value: formatWholeNumber(row.stacked_signal_count),
      tooltip: getMiniMetricTooltip("Stacked", row),
    },
  ].filter((item) => hasDisplayValue(item.value))

  return (
    <button
      type="button"
      onClick={onClick}
      className="group relative overflow-hidden rounded-[2rem] border p-5 text-left shadow-[0_22px_60px_rgba(0,0,0,0.36)] transition duration-200 hover:-translate-y-1"
      style={{
        borderColor: `${palette.end}45`,
        background: `linear-gradient(135deg, ${palette.start}16 0%, rgba(15,23,42,0.92) 35%, rgba(2,6,23,1) 100%)`,
      }}
    >
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,_rgba(255,255,255,0.06),_transparent_25%)] opacity-0 transition group-hover:opacity-100" />

      <div className="relative">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <FeaturedRankBadge rank={rank} />
              <SignalTypeBadge row={row} />
              <FreshnessBadge row={row} />
            </div>

            <div className="mt-3 flex flex-wrap items-center gap-3">
              <h3 className="text-3xl font-bold tracking-tight sm:text-4xl">{row.ticker}</h3>
              <ScoreBadge row={row} large />
              <ConfidenceBadge row={row} />
            </div>

            {row.company_name ? (
              <p className="mt-2 text-sm text-slate-300 sm:text-base">{row.company_name}</p>
            ) : null}
          </div>
        </div>

        <div className="mt-5">
          <ScoreBar row={row} />
        </div>

        <div className="mt-5 rounded-[1.5rem] border border-white/10 bg-black/20 p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
            Why this one stands out
          </p>
          <p className="mt-2 text-lg font-semibold text-white sm:text-xl">{thesis}</p>
          <p className="mt-3 text-sm leading-7 text-slate-300 sm:text-base">
            {getPlainEnglishSummary(row)}
          </p>
        </div>

        {!!reasons.length && (
          <div className="mt-5 flex flex-wrap gap-2">
            {reasons.map((reason) => (
              <ReasonChip key={reason} label={reason} />
            ))}
          </div>
        )}

        {!!miniMetrics.length && (
          <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {miniMetrics.map((item) => (
              <MiniMetric
                key={item.label}
                label={item.label}
                value={item.value}
                tooltip={item.tooltip}
              />
            ))}
          </div>
        )}

        <div className="mt-5 flex items-center justify-between gap-4 rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-[0.16em] text-slate-400">
              Primary driver
            </p>
            <p className="mt-1 truncate text-sm font-semibold text-white">
              {row.primary_title || "High-conviction strong buy"}
            </p>
          </div>
          <span className="shrink-0 rounded-full bg-emerald-400 px-3 py-1 text-xs font-bold text-slate-950">
            Open setup
          </span>
        </div>
      </div>
    </button>
  )
}

function TopSignalCard({
  row,
  onClick,
  isSelected,
  rank,
}: {
  row: TickerScore
  onClick: () => void
  isSelected: boolean
  rank: number
}) {
  const score = getEffectiveScore(row)
  const palette = getScorePalette(score)
  const reasons = getTopReasonChips(row)
  const metricItems: MiniMetricItem[] = [
    {
      label: "Price",
      value: formatMoney(row.price),
      tooltip: getMiniMetricTooltip("Price", row),
    },
    {
      label: "Vs Market",
      value: formatPercent(row.relative_strength_20d),
      tooltip: getMiniMetricTooltip("Vs Market", row),
    },
    {
      label: "5D Move",
      value: formatPercent(row.price_return_5d),
      tooltip: getMiniMetricTooltip("5D Move", row),
    },
    {
      label: "Volume",
      value: formatRatio(row.volume_ratio),
      tooltip: getMiniMetricTooltip("Volume", row),
    },
    {
      label: "1D Δ",
      value: formatScoreChange(row.ticker_score_change_1d),
      tooltip: getMiniMetricTooltip("1D Δ", row),
    },
    {
      label: "Stacked",
      value: formatWholeNumber(row.stacked_signal_count),
      tooltip: getMiniMetricTooltip("Stacked", row),
    },
  ].filter((item) => hasDisplayValue(item.value))

  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "flex h-full min-w-0 flex-col rounded-3xl border p-4 text-left shadow-xl transition duration-200 sm:p-5",
        isSelected
          ? "ring-2 ring-emerald-300/25"
          : "hover:-translate-y-0.5 hover:ring-1 hover:ring-white/10",
      ].join(" ")}
      style={{
        borderColor: isSelected ? `${palette.end}80` : `${palette.end}33`,
        background: `linear-gradient(135deg, ${palette.start}12 0%, rgba(15,23,42,0.92) 40%, rgba(2,6,23,1) 100%)`,
      }}
    >
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <CardRankBadge rank={rank} />
            <p className="text-xs uppercase tracking-[0.2em] text-emerald-300/80">
              {formatSource(row.primary_signal_source)}
            </p>
          </div>

          <h3 className="mt-2 truncate text-2xl font-bold sm:text-3xl">{row.ticker}</h3>
          {row.company_name ? (
            <p className="mt-1 truncate text-sm text-slate-400">{row.company_name}</p>
          ) : null}
        </div>

        <div className="flex shrink-0 flex-col items-end gap-2">
          <ScoreBadge row={row} />
          <FreshnessBadge row={row} />
        </div>
      </div>

      <div className="mb-4">
        <ScoreBar row={row} compact />
      </div>

      <div className="mb-4 rounded-2xl border border-white/10 bg-black/20 p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
          Setup thesis
        </p>
        <p className="mt-2 text-sm font-semibold leading-6 text-white">
          {getCardThesis(row)}
        </p>
      </div>

      {!!reasons.length && (
        <div className="mb-4 flex flex-wrap gap-2">
          {reasons.map((reason) => (
            <ReasonChip key={reason} label={reason} />
          ))}
        </div>
      )}

      {row.business_description ? (
        <p className="mb-4 text-sm leading-6 text-slate-300">
          {truncateText(row.business_description, 110)}
        </p>
      ) : null}

      {!!metricItems.length && (
        <div className="mb-4 grid grid-cols-2 gap-3 auto-rows-fr">
          {metricItems.map((item) => (
            <MiniMetric
              key={item.label}
              label={item.label}
              value={item.value}
              tooltip={item.tooltip}
            />
          ))}
        </div>
      )}

      <div className="mt-auto rounded-2xl bg-black/20 p-4">
        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
          Why traders could notice it
        </p>
        <p className="text-sm leading-6 text-slate-100">
          {truncateText(getPlainEnglishSummary(row), 180)}
        </p>
      </div>
    </button>
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

  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
          Conviction Score
        </p>
        <p className="text-sm font-semibold text-white">{score}/100</p>
      </div>

      <div className="h-3 overflow-hidden rounded-full bg-slate-800">
        <div
          className="h-full rounded-full transition-all duration-300"
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
          <span>Elite</span>
        </div>
      ) : null}
    </div>
  )
}

function MiniMetric({
  label,
  value,
  tooltip,
}: {
  label: string
  value: string
  tooltip?: string
}) {
  const card = (
    <div className="flex h-full min-h-[88px] flex-col justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-400">{label}</p>
      <p className="mt-2 text-sm font-semibold text-white sm:text-base">{value}</p>
    </div>
  )

  if (!tooltip) return card
  return <Tooltip content={tooltip}>{card}</Tooltip>
}

function FeaturedRankBadge({ rank }: { rank: number }) {
  return (
    <Tooltip content={`This setup is ranked #${rank} on today’s strong-buy board.`}>
      <span className="inline-flex cursor-help items-center rounded-full bg-emerald-400 px-3 py-1 text-xs font-bold uppercase tracking-[0.18em] text-slate-950">
        Top #{rank}
      </span>
    </Tooltip>
  )
}

function CardRankBadge({ rank }: { rank: number }) {
  return (
    <Tooltip content={`This setup is ranked #${rank} on today’s board.`}>
      <span className="inline-flex cursor-help items-center rounded-full bg-emerald-400/15 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.18em] text-emerald-200">
        #{rank}
      </span>
    </Tooltip>
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
    <Tooltip content={getScoreTooltip(score)}>
      <div
        className={[
          "inline-flex shrink-0 cursor-help items-center whitespace-nowrap rounded-full font-bold shadow-lg ring-1 ring-white/10",
          large ? "px-4 py-2 text-sm" : "px-3 py-1 text-sm",
        ].join(" ")}
        style={{
          background: `linear-gradient(135deg, ${palette.start}, ${palette.end})`,
          color: palette.text,
        }}
      >
        {score}
      </div>
    </Tooltip>
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

  return (
    <Tooltip content={getConfidenceTooltip(score, label)}>
      <span
        className={[
          "inline-flex shrink-0 cursor-help items-center whitespace-nowrap rounded-full border border-white/10 bg-white/5 font-semibold text-slate-200",
          small ? "px-2.5 py-1 text-[11px]" : "px-3 py-1.5 text-xs",
        ].join(" ")}
      >
        {label}
      </span>
    </Tooltip>
  )
}

function FreshnessBadge({ row }: { row: TickerScore }) {
  const label = getFreshnessLabel(row)
  if (!label) return null

  return (
    <Tooltip content={getFreshnessTooltip(row)}>
      <span className="inline-flex shrink-0 cursor-help items-center whitespace-nowrap rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-[11px] font-semibold text-slate-300">
        {label}
      </span>
    </Tooltip>
  )
}

function SignalTypeBadge({ row }: { row: TickerScore }) {
  const config = getSignalBadgeConfig(row)

  return (
    <Tooltip content={getSignalTypeTooltip(row, config.label)}>
      <span
        className={[
          "inline-flex cursor-help items-center rounded-full border px-3 py-1.5 text-xs font-semibold",
          config.className,
        ].join(" ")}
      >
        {config.label}
      </span>
    </Tooltip>
  )
}

function ReasonChip({ label }: { label: string }) {
  return (
    <Tooltip content={getReasonChipTooltip(label)}>
      <span className="cursor-help rounded-full border border-emerald-400/20 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold text-emerald-200">
        {label}
      </span>
    </Tooltip>
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
                  ? "border-emerald-400/30 bg-emerald-400/15 text-white"
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

function getSignalBadgeConfig(row: TickerScore) {
  const source = row.primary_signal_source
  const category = getSignalCategory(row)

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

  if (category === "Fundamental") {
    return {
      label: "Fundamental",
      className: "border-blue-400/30 bg-blue-400/10 text-blue-300",
    }
  }

  return {
    label: "Strong Buy",
    className: "border-emerald-400/30 bg-emerald-400/10 text-emerald-300",
  }
}

function getSignalCategory(row: TickerScore) {
  const storedCategory = (row.primary_signal_category ?? "").trim()
  if (storedCategory) return storedCategory
  return "Strong Buy"
}

function getFeaturedThesis(row: TickerScore) {
  if (row.primary_signal_source === "breakout" && (row.volume_ratio ?? 0) >= 2) {
    return "Fresh breakout with strong participation"
  }

  if ((row.cluster_buyers ?? 0) >= 2) {
    return "Multiple bullish signals are stacking together"
  }

  if ((row.earnings_surprise_pct ?? 0) >= 10 || (row.revenue_growth_pct ?? 0) >= 15) {
    return "Momentum is being reinforced by earnings support"
  }

  if ((row.relative_strength_20d ?? 0) >= 8) {
    return "This name is outperforming while conviction stays high"
  }

  return "A high-conviction setup with strong current support"
}

function getCardThesis(row: TickerScore) {
  if (row.primary_title) return row.primary_title

  if (row.primary_signal_source === "breakout") {
    return "Fresh technical setup with confirmed momentum"
  }

  if ((row.cluster_buyers ?? 0) >= 2) {
    return "Buying interest is showing up in a meaningful way"
  }

  if ((row.volume_ratio ?? 0) >= 2 && (row.price_return_5d ?? 0) >= 5) {
    return "Participation and price action are moving together"
  }

  return "Strong buy conditions are lining up"
}

function SignalDetailsModal({
  row,
  onClose,
}: {
  row: TickerScore
  onClose: () => void
}) {
  const reasons = getTopReasonLines(row)
  const tags = normalizeTags(row.signal_tags)
  const thesis = getFeaturedThesis(row)

  return (
    <div
      className="fixed inset-0 z-50 flex items-end justify-center bg-black/70 p-0 backdrop-blur-sm sm:items-center sm:p-6"
      onClick={onClose}
    >
      <div
        className="max-h-[92vh] w-full max-w-6xl overflow-y-auto rounded-t-[2rem] border border-white/10 bg-slate-950 shadow-2xl sm:rounded-[2rem]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="sticky top-0 z-10 flex items-center justify-between gap-4 border-b border-white/10 bg-slate-950/95 px-5 py-4 backdrop-blur sm:px-6">
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
              Strong Buy Detail
            </p>

            <div className="mt-1 flex flex-wrap items-center gap-2">
              <h2 className="text-2xl font-bold sm:text-3xl">{row.ticker}</h2>
              <ScoreBadge row={row} large />
              <ConfidenceBadge row={row} />
              <FreshnessBadge row={row} />
              <SignalTypeBadge row={row} />
              <StrengthBadge bucket={row.signal_strength_bucket} />
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

        <div className="grid gap-6 p-5 sm:p-6 lg:grid-cols-[1.15fr_0.85fr]">
          <div className="min-w-0">
            <div className="mb-5 rounded-[1.75rem] border border-emerald-400/15 bg-[linear-gradient(135deg,rgba(16,185,129,0.10),rgba(2,6,23,0.9)_55%,rgba(2,6,23,1))] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
                Why this made the board
              </p>
              <p className="mt-2 text-xl font-semibold text-white sm:text-2xl">{thesis}</p>
              <p className="mt-3 text-sm leading-7 text-slate-300 sm:text-base">
                {getConfidenceStatement(row)}
              </p>
            </div>

            {row.business_description ? (
              <p className="mb-5 text-sm leading-7 text-slate-300 sm:text-base">
                {row.business_description}
              </p>
            ) : null}

            <div className="mb-5">
              <ScoreBar row={row} />
            </div>

            <div className="mb-5">
              <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Score drivers
              </p>
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {reasons.map((reason) => (
                  <ReasonCard key={`${reason.label}-${reason.value}`} reason={reason} />
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
                Plain-English setup summary
              </p>
              <p className="mt-2 text-sm leading-7 text-slate-200 sm:text-base">
                {row.primary_summary || getSignalSummary(row)}
              </p>

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
                  value={row.price_confirmed === true ? "Confirmed" : "Not confirmed"}
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
                  label="Trend alignment"
                  value={row.trend_aligned === true ? "Aligned" : "Mixed"}
                />
                <ConfirmationRow
                  label="Relative strength"
                  value={formatPercent(row.relative_strength_20d)}
                />
                <ConfirmationRow
                  label="Participation"
                  value={formatRatio(row.volume_ratio)}
                />
                <ConfirmationRow
                  label="Signal stack"
                  value={formatWholeNumber(row.stacked_signal_count)}
                />
              </div>
            </div>
          </div>

          <div className="min-w-0 rounded-3xl border border-white/10 bg-white/[0.04] p-5">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
              Conviction snapshot
            </p>

            <div className="mt-4 space-y-3">
              <MetricRow label="Board score" value={formatScore(row)} />
              <MetricRow label="Confidence tier" value={getConfidenceTierLabel(getEffectiveScore(row))} />
              <MetricRow label="Price" value={formatMoney(row.price)} />
              <MetricRow label="Primary signal" value={row.primary_title || "Strong buy setup"} />
              <MetricRow label="Signal source" value={formatSource(row.primary_signal_source)} />
              <MetricRow label="Signal category" value={getSignalCategory(row)} />
              <MetricRow label="Freshness" value={getFreshnessLabel(row)} />
              <MetricRow label="Filed at" value={row.filed_at ? formatDateLong(row.filed_at) : null} />
              <MetricRow label="Signals stacked" value={formatWholeNumber(row.stacked_signal_count)} />
              <MetricRow label="1D score change" value={formatScoreChange(row.ticker_score_change_1d)} />
              <MetricRow label="7D score change" value={formatScoreChange(row.ticker_score_change_7d)} />
            </div>

            <div className="mt-6 border-t border-white/10 pt-6">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Price and momentum
              </p>

              <div className="mt-4 space-y-3">
                <MetricRow label="5D move" value={formatPercent(row.price_return_5d)} />
                <MetricRow label="20D move" value={formatPercent(row.price_return_20d)} />
                <MetricRow label="Volume ratio" value={formatRatio(row.volume_ratio)} />
                <MetricRow label="Vs market 20D" value={formatPercent(row.relative_strength_20d)} />
                <MetricRow label="Above 50DMA" value={formatBooleanLabel(row.above_50dma)} />
                <MetricRow label="Trend aligned" value={formatBooleanLabel(row.trend_aligned)} />
                <MetricRow label="Price confirmed" value={formatBooleanLabel(row.price_confirmed)} />
              </div>
            </div>

            <div className="mt-6 border-t border-white/10 pt-6">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Supporting evidence
              </p>

              <div className="mt-4 space-y-3">
                <MetricRow label="Insider action" value={row.insider_action || null} />
                <MetricRow label="Insider shares" value={formatShares(row.insider_shares)} />
                <MetricRow label="Insider avg price" value={formatMoney(row.insider_avg_price)} />
                <MetricRow label="Insider value" value={formatInsiderValue(row)} />
                <MetricRow label="Cluster buyers" value={formatWholeNumber(row.cluster_buyers)} />
                <MetricRow label="Cluster shares" value={formatShares(row.cluster_shares)} />
                <MetricRow label="Earnings surprise" value={formatPercent(row.earnings_surprise_pct)} />
                <MetricRow label="Revenue growth" value={formatPercent(row.revenue_growth_pct)} />
                <MetricRow label="Guidance support" value={formatBooleanLabel(row.guidance_flag)} />
              </div>
            </div>

            <div className="mt-6 border-t border-white/10 pt-6">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                Valuation and company
              </p>

              <div className="mt-4 space-y-3">
                <MetricRow label="Valuation" value={formatPe(row.pe_ratio, row.pe_forward, row.pe_type)} />
                <MetricRow label="Market cap" value={formatMarketCap(row.market_cap)} />
                <MetricRow label="Sector" value={row.sector || null} />
                <MetricRow label="Industry" value={row.industry || null} />
                <MetricRow label="Model version" value={row.score_version || null} />
              </div>
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
      <span className="text-slate-400">{label}</span>
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
      <p className="mt-2 text-sm font-semibold text-white">{value}</p>
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

function TagPill({ tag }: { tag: string }) {
  const pretty = prettifyTag(tag)

  return (
    <Tooltip content={getTagTooltip(tag, pretty)}>
      <span className="cursor-help rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
        {pretty}
      </span>
    </Tooltip>
  )
}

function StrengthBadge({ bucket }: { bucket?: string | null }) {
  const value = bucket ?? "Strong Buy"
  const classes =
    value === "Strong Buy"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-300"
      : value === "Buy"
        ? "border-blue-400/30 bg-blue-400/10 text-blue-300"
        : "border-yellow-400/30 bg-yellow-400/10 text-yellow-300"

  return (
    <Tooltip content={getStrengthTooltip(value)}>
      <span
        className={`inline-flex cursor-help items-center rounded-full border px-3 py-1.5 text-xs font-semibold ${classes}`}
      >
        {value}
      </span>
    </Tooltip>
  )
}

function getTopReasonChips(row: TickerScore) {
  const tags = normalizeTags(row.signal_tags)
  const chips: string[] = []

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

  return Array.from(new Set(chips)).slice(0, 4)
}

function getTopReasonLines(row: TickerScore): ReasonLine[] {
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
      tone: meta.tone,
      weight: Math.abs(value),
    })
  }

  if (!items.length) {
    items.push({
      label: "Model",
      value: "Bullish signals outweigh negatives",
      tone: "good",
      weight: 1,
    })
  }

  return items.sort((a, b) => b.weight - a.weight).slice(0, 4)
}

function getPlainEnglishSummary(row: TickerScore) {
  const reasons = getTopReasonChips(row)

  if (!reasons.length) {
    return "Several bullish signals are stacking up here, which keeps the setup on the strong-buy board."
  }

  return `This name is showing ${reasons.join(", ").toLowerCase()}, which keeps it near the top of the strong-buy board.`
}

function getConfidenceStatement(row: TickerScore) {
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

  const source = formatSource(row.primary_signal_source)

  if (heavyCluster && hasMomentum) {
    return "Multiple bullish signals are stacking together, and the stock is still acting well. That combination is much more interesting than a one-off headline."
  }

  if (row.primary_signal_source === "breakout" && hasMomentum && hasVolume) {
    return "This is not just a filing-driven idea. Technical strength, participation, and broader momentum are all reinforcing the setup."
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

  if (score >= 85) {
    return "This name ranks highly because several independent signals are still leaning bullish at the same time."
  }

  return `This remains a constructive setup overall, with ${source} providing the original signal and the broader evaluation still holding up.`
}

function getSignalSummary(row: TickerScore) {
  if (row.primary_summary) return row.primary_summary
  return `${row.ticker} is showing a strong-buy setup based on stacked signals, price action, and broader confirmation.`
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
    .replace(/_/g, " ")
    .replace(/-/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
}

function formatSource(source?: string | null) {
  if (!source) return "Model"
  if (source === "form4") return "Form 4"
  if (source === "13d") return "13D"
  if (source === "13g") return "13G"
  if (source === "8k") return "8-K / Current Report"
  if (source === "earnings") return "Earnings"
  if (source === "breakout") return "Technical / Breakout"
  return source
}

function formatScore(row: TickerScore) {
  return `${getEffectiveScore(row)}`
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

function getConfidenceTierLabel(score: number) {
  if (score >= 90) return "Elite"
  if (score >= 80) return "High Conviction"
  if (score >= 70) return "Strong"
  return "Building"
}

function getFreshnessLabel(row: TickerScore) {
  const bucket = (row.freshness_bucket ?? "").trim()
  const age = row.age_days

  if (bucket === "today") return "Today"
  if (bucket === "fresh") return "1-3D"
  if (bucket === "recent") return "4-7D"
  if (bucket === "aging") return "8-14D"
  if (bucket === "stale") return "Older"

  if (typeof age === "number") {
    if (age <= 0) return "Today"
    if (age <= 3) return "1-3D"
    if (age <= 7) return "4-7D"
    if (age <= 14) return "8-14D"
    return "Older"
  }

  return null
}

function getScoreTooltip(score: number) {
  if (score >= 90) {
    return `Conviction Score ${score}: elite setup quality. This is one of the strongest readings on the board.`
  }
  if (score >= 80) {
    return `Conviction Score ${score}: strong setup quality with multiple positives lining up.`
  }
  if (score >= 70) {
    return `Conviction Score ${score}: solid setup quality and strong enough for the main strong-buy board.`
  }
  return `Conviction Score ${score}: below the normal strong-buy threshold.`
}

function getConfidenceTooltip(score: number, label: string) {
  return `${label} is the confidence tier for a score of ${score}. Higher tiers mean the model sees stronger supporting evidence.`
}

function getFreshnessTooltip(row: TickerScore) {
  const label = getFreshnessLabel(row)
  return `${label ?? "Freshness unknown"}. Fresher signals usually matter more than older ones.`
}

function getSignalTypeTooltip(row: TickerScore, label: string) {
  return `${label} describes the main kind of signal driving this setup. Source: ${formatSource(
    row.primary_signal_source
  )}. Category: ${getSignalCategory(row)}.`
}

function getStrengthTooltip(value: string) {
  if (value === "Strong Buy") {
    return "Strong Buy means the model sees unusually strong bullish evidence."
  }
  if (value === "Buy") {
    return "Buy means the setup is constructive and clears the main bullish threshold."
  }
  return "Signal strength is a quick label for how the model buckets the setup."
}

function getReasonChipTooltip(label: string) {
  return `${label}. This is one of the main reasons the model likes the setup.`
}

function getTagTooltip(tag: string, pretty: string) {
  const map: Record<string, string> = {
    "cluster-buy": "Multiple insiders or participants bought close together.",
    "cluster-strong": "A stronger version of clustered buying interest.",
    "insider-buy": "Insiders are buying shares, which can support a bullish case.",
    "momentum-confirmed": "Price strength is being confirmed by momentum behavior.",
    "breakout-20d": "The stock is breaking above a recent 20-day range.",
    "breakout-52w": "The stock is approaching or breaking a 52-week high area.",
    "volume-confirmed": "Trading activity is elevated enough to support the move.",
    "heavy-volume": "Trading activity is meaningfully above normal.",
    "earnings-support": "Recent earnings results are helping the setup.",
    "reasonable-valuation": "Valuation still looks reasonable compared to growth or quality.",
    "deep-value": "The setup may also have a value angle.",
    "guidance-support": "Forward guidance or expectations appear constructive.",
    "relative-strength": "The stock is outperforming the broader market.",
    "candidate-screen": "The setup also passed the technical candidate screening layer.",
    "candidate-included": "The setup made it into the live strong-buy universe.",
    "candidate-strong-buy": "The technical candidate score is especially strong.",
    "screen-heavy-volume": "The screening layer also saw elevated volume.",
    "screen-momentum": "The screening layer also saw strong momentum.",
  }

  return map[tag] ?? `${pretty} is a model tag used to explain part of the setup.`
}

function getMiniMetricTooltip(label: string, row: TickerScore) {
  switch (label) {
    case "Price":
      return `Current share price. Useful for filtering by stock size and trading style. Current value: ${formatMoney(
        row.price
      )}.`
    case "Vs Market":
      return `Relative strength versus the broader market over the recent period. Positive means it is outperforming. Current value: ${formatPercent(
        row.relative_strength_20d
      )}.`
    case "5D Move":
      return `The stock’s move over the last 5 trading days. Helps show short-term momentum. Current value: ${formatPercent(
        row.price_return_5d
      )}.`
    case "20D Move":
      return `The stock’s move over the last 20 trading days. Helps show the bigger recent move. Current value: ${formatPercent(
        row.price_return_20d
      )}.`
    case "Volume":
      return `Trading volume compared with normal. Around 1.00x is normal, above that suggests heavier activity. Current value: ${formatRatio(
        row.volume_ratio
      )}.`
    case "1D Δ":
      return `Change in the model score over the last day. Positive means the setup improved. Current value: ${formatScoreChange(
        row.ticker_score_change_1d
      )}.`
    case "7D Δ":
      return `Change in the model score over the last 7 days. Good for spotting improving setups. Current value: ${formatScoreChange(
        row.ticker_score_change_7d
      )}.`
    case "Stacked":
      return `How many distinct supporting signals are stacked into this setup. More stacked signals usually means more evidence. Current value: ${formatWholeNumber(
        row.stacked_signal_count
      )}.`
    default:
      return ""
  }
}