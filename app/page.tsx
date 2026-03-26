"use client"

import { useRouter } from "next/navigation"
import { useEffect, useState } from "react"

/* ---------- Fake card data for the hero preview ---------- */
const PREVIEW_CARDS = [
  {
    ticker: "PLTR", name: "Palantir Technologies", score: 94, sector: "Technology", price: "$87.42",
    returns: [
      { label: "1D", value: 2.1 },
      { label: "5D", value: 5.4 },
      { label: "10D", value: 8.7 },
      { label: "20D", value: 12.3 },
    ],
    tiles: [
      { label: "Profit Growth", value: "Strong", score: 82, color: "#22d3ee", iconPath: "M2 9L4.5 4L7 6.5L10 2M10 2H7.5M10 2V4.5" },
      { label: "Cash Flow", value: "Positive", score: 78, color: "#34d399", iconPath: "M6 1V11M3 3.5H7.5C8.88 3.5 10 4.34 10 5.25S8.88 7 7.5 7H3M3 7H8C9.38 7 10.5 7.84 10.5 8.75S9.38 10.5 8 10.5H3" },
      { label: "Debt Level", value: "Low Debt", score: 88, color: "#a78bfa", iconPath: "M1.5 10V4L6 1.5L10.5 4V10M4 10V7H8V10" },
      { label: "Competition", value: "Hard to Beat", score: 91, color: "#f59e0b", iconPath: "M1 8L3 6L5 8L7 4L9 7L11 5M1 10H11" },
      { label: "Fair Price", value: "Attractive", score: 72, color: "#fb923c", iconPath: "M2 2V10H10M4 8V6M6.5 8V4M9 8V5" },
      { label: "Momentum", value: "Near Highs", score: 85, color: "#ec4899", iconPath: "M6 1L8.5 4H7V7.5H5V4H3.5L6 1M2 9.5H10" },
    ],
  },
  {
    ticker: "UTHR", name: "United Therapeutics", score: 93, sector: "Healthcare", price: "$342.18",
    returns: [
      { label: "1D", value: 1.3 },
      { label: "5D", value: 3.8 },
      { label: "10D", value: 6.2 },
      { label: "20D", value: 9.1 },
    ],
    tiles: [
      { label: "Profit Growth", value: "Strong", score: 90, color: "#22d3ee", iconPath: "M2 9L4.5 4L7 6.5L10 2M10 2H7.5M10 2V4.5" },
      { label: "Cash Flow", value: "Positive", score: 85, color: "#34d399", iconPath: "M6 1V11M3 3.5H7.5C8.88 3.5 10 4.34 10 5.25S8.88 7 7.5 7H3M3 7H8C9.38 7 10.5 7.84 10.5 8.75S9.38 10.5 8 10.5H3" },
      { label: "Debt Level", value: "Low Debt", score: 92, color: "#a78bfa", iconPath: "M1.5 10V4L6 1.5L10.5 4V10M4 10V7H8V10" },
      { label: "Competition", value: "Some Edge", score: 65, color: "#f59e0b", iconPath: "M1 8L3 6L5 8L7 4L9 7L11 5M1 10H11" },
      { label: "Fair Price", value: "Attractive", score: 80, color: "#fb923c", iconPath: "M2 2V10H10M4 8V6M6.5 8V4M9 8V5" },
      { label: "Momentum", value: "Strong", score: 70, color: "#ec4899", iconPath: "M6 1L8.5 4H7V7.5H5V4H3.5L6 1M2 9.5H10" },
    ],
  },
  {
    ticker: "AXON", name: "Axon Enterprise", score: 89, sector: "Technology", price: "$614.30",
    returns: [
      { label: "1D", value: 0.8 },
      { label: "5D", value: 2.4 },
      { label: "10D", value: 4.9 },
      { label: "20D", value: 7.8 },
    ],
    tiles: [
      { label: "Profit Growth", value: "Growing", score: 68, color: "#22d3ee", iconPath: "M2 9L4.5 4L7 6.5L10 2M10 2H7.5M10 2V4.5" },
      { label: "Cash Flow", value: "Positive", score: 74, color: "#34d399", iconPath: "M6 1V11M3 3.5H7.5C8.88 3.5 10 4.34 10 5.25S8.88 7 7.5 7H3M3 7H8C9.38 7 10.5 7.84 10.5 8.75S9.38 10.5 8 10.5H3" },
      { label: "Debt Level", value: "Low Debt", score: 85, color: "#a78bfa", iconPath: "M1.5 10V4L6 1.5L10.5 4V10M4 10V7H8V10" },
      { label: "Competition", value: "Hard to Beat", score: 88, color: "#f59e0b", iconPath: "M1 8L3 6L5 8L7 4L9 7L11 5M1 10H11" },
      { label: "Fair Price", value: "Fair", score: 48, color: "#fb923c", iconPath: "M2 2V10H10M4 8V6M6.5 8V4M9 8V5" },
      { label: "Momentum", value: "Strong", score: 72, color: "#ec4899", iconPath: "M6 1L8.5 4H7V7.5H5V4H3.5L6 1M2 9.5H10" },
    ],
  },
]

/* ---------- Animated counter ---------- */
function AnimatedNumber({ target, duration = 2000 }: { target: number; duration?: number }) {
  const [value, setValue] = useState(0)
  useEffect(() => {
    const start = Date.now()
    const tick = () => {
      const elapsed = Date.now() - start
      const progress = Math.min(elapsed / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setValue(Math.round(target * eased))
      if (progress < 1) requestAnimationFrame(tick)
    }
    requestAnimationFrame(tick)
  }, [target, duration])
  return <>{value}</>
}

/* ---------- Feature card ---------- */
function FeatureCard({ icon, title, description }: { icon: React.ReactNode; title: string; description: string }) {
  return (
    <div className="group relative rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6 transition hover:border-cyan-500/20 hover:bg-white/[0.04]">
      <div className="mb-4 flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-500/10 text-cyan-400">
        {icon}
      </div>
      <h3 className="mb-2 text-base font-semibold text-white">{title}</h3>
      <p className="text-sm leading-relaxed text-slate-400">{description}</p>
    </div>
  )
}

/* ---------- Score ring (matches dashboard) ---------- */
function PreviewScoreRing({ score }: { score: number }) {
  const color = score >= 90 ? "#30d158" : score >= 80 ? "#f0a500" : score >= 70 ? "#f0a500" : "#7a8ba0"
  const radius = 22
  const strokeWidth = 4
  const circumference = 2 * Math.PI * radius
  const dashOffset = circumference - (score / 100) * circumference
  return (
    <div className="relative flex items-center justify-center" style={{ width: 56, height: 56 }}>
      <svg width="56" height="56" viewBox="0 0 56 56" style={{ transform: "rotate(-90deg)" }}>
        <circle cx="28" cy="28" r={radius} fill="none" stroke="#1e2d45" strokeWidth={strokeWidth} />
        <circle cx="28" cy="28" r={radius} fill="none" stroke={color} strokeWidth={strokeWidth} strokeLinecap="round" strokeDasharray={circumference} strokeDashoffset={dashOffset} />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-base font-black leading-none text-white">{score}</span>
        <span className="text-[8px] leading-none text-[#7a8ba0] mt-0.5">/100</span>
      </div>
    </div>
  )
}

/* ---------- Preview card (faithful replica of real dashboard card) ---------- */
function PreviewCard({ card, index, isCenter }: { card: typeof PREVIEW_CARDS[0]; index: number; isCenter: boolean }) {
  return (
    <div
      className={`relative w-[300px] shrink-0 rounded-[1.75rem] border transition-all duration-500 overflow-hidden ${
        isCenter
          ? "z-20 scale-100 border-white/[0.08] opacity-100"
          : "z-10 scale-[0.88] border-white/[0.04] opacity-35"
      }`}
      style={{
        background: "#0f1729",
        transform: isCenter ? undefined : `translateX(${index === 0 ? "20px" : "-20px"})`,
      }}
    >
      {/* ── Header ── */}
      <div className="px-4 pt-4 pb-2">
        <div className="flex items-center gap-3">
          <PreviewScoreRing score={card.score} />
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline gap-2">
              <h3 className="text-2xl font-black tracking-tight text-white">{card.ticker}</h3>
              <span className="text-sm font-bold text-white/70">{card.price}</span>
            </div>
            <p className="truncate text-xs text-white/40">{card.name} · {card.sector}</p>
          </div>
          <div className="flex h-8 shrink-0 items-center gap-1 rounded-full border border-emerald-500/40 bg-emerald-500/15 px-3 text-xs font-bold text-emerald-400">
            Buy
            <svg width="10" height="10" viewBox="0 0 12 12" fill="none"><path d="M3 9L9 3M9 3H4M9 3V8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </div>
        </div>
      </div>

      {/* ── Returns strip ── */}
      <div className="px-4 pb-1">
        <div className="grid grid-cols-4 gap-1.5">
          {card.returns.map(({ label, value }) => {
            const isPos = value >= 0
            return (
              <div
                key={label}
                className="flex flex-col items-center justify-center rounded-lg border py-1.5"
                style={{
                  borderColor: isPos ? "rgba(48,209,88,0.22)" : "rgba(255,69,58,0.22)",
                  background: isPos ? "rgba(48,209,88,0.07)" : "rgba(255,69,58,0.07)",
                }}
              >
                <span className="text-[8px] font-medium text-white/40">{label}</span>
                <span className="mt-0.5 text-xs font-black" style={{ color: isPos ? "#4ade80" : "#f87171" }}>
                  {isPos ? "+" : ""}{value.toFixed(1)}%
                </span>
              </div>
            )
          })}
        </div>
      </div>

      {/* ── Fundamental tiles ── */}
      <div className="px-4 pt-1.5 pb-1">
        <div className="grid grid-cols-2 gap-1.5">
          {card.tiles.map((tile) => {
            const active = tile.score > 0
            return (
              <div
                key={tile.label}
                className="relative overflow-hidden rounded-xl p-2"
                style={{
                  background: active ? `linear-gradient(160deg, ${tile.color}18 0%, ${tile.color}04 100%)` : "#111827",
                  border: `1px solid ${active ? `${tile.color}40` : "rgba(255,255,255,0.05)"}`,
                }}
              >
                {active && (
                  <div className="absolute top-0 right-0 h-6 w-6 opacity-20" style={{
                    background: `radial-gradient(circle at top right, ${tile.color}, transparent 70%)`,
                  }} />
                )}
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-1">
                    <svg width="10" height="10" viewBox="0 0 12 12" fill="none" className="shrink-0">
                      <path d={tile.iconPath} stroke={tile.color} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                    <p className="text-[8px] font-bold uppercase tracking-[0.12em]" style={{ color: tile.color }}>{tile.label}</p>
                  </div>
                  <span className="text-[9px] font-black" style={{ color: tile.color }}>{tile.score}</span>
                </div>
                <p className="mt-0.5 text-sm font-black leading-tight text-[#f0f0f0]">{tile.value}</p>
                <div className="mt-1 h-[4px] overflow-hidden rounded-full bg-[#1a2540]">
                  <div className="h-full rounded-full" style={{
                    width: `${Math.min((tile.score / 100) * 100, 100)}%`,
                    background: `linear-gradient(90deg, ${tile.color}90, ${tile.color})`,
                    boxShadow: `0 0 6px ${tile.color}40`,
                  }} />
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* ── CTA ── */}
      <div className="px-4 pt-2 pb-3">
        <div className="w-full rounded-2xl bg-[#f0a500] px-4 py-2.5 text-center text-sm font-bold text-black">
          View Analysis →
        </div>
      </div>
    </div>
  )
}

/* ---------- How it works step ---------- */
function Step({ number, title, description }: { number: number; title: string; description: string }) {
  return (
    <div className="flex gap-4">
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-cyan-500/15 text-sm font-bold text-cyan-400">
        {number}
      </div>
      <div>
        <h3 className="mb-1 font-semibold text-white">{title}</h3>
        <p className="text-sm leading-relaxed text-slate-400">{description}</p>
      </div>
    </div>
  )
}

/* ========== MAIN LANDING PAGE ========== */
export default function LandingPage() {
  const router = useRouter()
  const [activeCard, setActiveCard] = useState(1)
  const [yearly, setYearly] = useState(true)

  useEffect(() => {
    const interval = setInterval(() => {
      setActiveCard((prev) => (prev + 1) % 3)
    }, 3000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="min-h-screen overflow-x-hidden">

      {/* ── Nav ── */}
      <nav className="fixed top-0 z-50 w-full border-b border-white/[0.06] bg-[#080d18]/80 backdrop-blur-xl pt-[env(safe-area-inset-top)]">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-5 py-3">
          <div className="flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-cyan-500/15">
              <svg className="h-4 w-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />
              </svg>
            </div>
            <span className="hidden sm:inline text-sm font-bold text-white tracking-wide">MARKET SIGNAL TRACKER</span>
            <span className="sm:hidden text-sm font-bold text-white tracking-wide">MST</span>
          </div>
          <div className="flex items-center gap-2 sm:gap-3">
            <button
              onClick={() => router.push("/login")}
              className="text-xs sm:text-sm text-slate-400 transition hover:text-white"
            >
              Sign In
            </button>
            <button
              onClick={() => router.push("/login")}
              className="rounded-full bg-cyan-500 px-3 sm:px-4 py-1.5 text-xs sm:text-sm font-semibold text-black transition hover:bg-cyan-400"
            >
              Get Started
            </button>
          </div>
        </div>
      </nav>

      {/* ── Hero ── */}
      <section className="relative pt-28 pb-28 sm:pb-20 px-5">
        {/* Background glow effects */}
        <div className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute left-1/2 top-0 h-[900px] w-[800px] -translate-x-1/2 rounded-full bg-cyan-500/[0.07] blur-[120px]" />
          <div className="absolute right-0 top-1/3 h-[400px] w-[400px] rounded-full bg-purple-500/[0.05] blur-[100px]" />
        </div>

        <div className="relative mx-auto max-w-6xl">
          <div className="mx-auto max-w-2xl text-center mb-16">
            {/* Badge */}
            <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/[0.08] px-4 py-1.5">
              <span className="relative flex h-2 w-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-cyan-400 opacity-75" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-cyan-500" />
              </span>
              <span className="text-xs font-medium text-cyan-400">Smart Buy signals updated daily</span>
            </div>

            <h1 className="mb-5 text-4xl font-black leading-[1.1] tracking-tight text-white sm:text-5xl md:text-6xl">
              When insiders AND Congress buy,
              <br />
              <span className="bg-gradient-to-r from-cyan-400 to-emerald-400 bg-clip-text text-transparent">
                that&apos;s a Smart Buy signal.
              </span>
            </h1>

            <p className="mb-8 text-base leading-relaxed text-slate-400 sm:text-lg">
              We stack insider trades + congressional trades + attractive valuations + momentum into one proprietary score.
              Out of thousands of stocks, our algorithm surfaces only 30-50 high-conviction Smart Buy picks per day.
            </p>

            <div className="flex flex-col items-center gap-3 mb-8 sm:mb-0 sm:flex-row sm:justify-center">
              <button
                onClick={() => router.push("/login")}
                className="w-full rounded-full bg-cyan-500 px-8 py-3 text-base font-bold text-black transition hover:bg-cyan-400 hover:shadow-lg hover:shadow-cyan-500/25 sm:w-auto"
              >
                Start Free — See Today&apos;s Smart Buys
              </button>
              <button
                onClick={() => {
                  document.getElementById("how-it-works")?.scrollIntoView({ behavior: "smooth" })
                }}
                className="mb-10 sm:mb-0 w-full rounded-full border border-white/10 px-8 py-3 text-base font-medium text-slate-300 transition hover:border-white/20 hover:text-white sm:w-auto"
              >
                How it works
              </button>
            </div>
          </div>

          {/* Card carousel preview */}
          <div className="flex items-center justify-center gap-0 sm:gap-2">
            {PREVIEW_CARDS.map((card, i) => (
              <PreviewCard key={card.ticker} card={card} index={i} isCenter={i === activeCard} />
            ))}
          </div>
        </div>
      </section>

      {/* ── Stats bar ── */}
      <section className="border-y border-white/[0.06] bg-white/[0.01]">
        <div className="mx-auto grid max-w-4xl grid-cols-2 gap-6 px-5 py-10 sm:grid-cols-4">
          {[
            { value: 10000, suffix: "+", label: "Stocks screened daily" },
            { value: 30, suffix: "-50", label: "Smart Buy picks per day" },
            { value: 4, suffix: "", label: "Stacked signal layers" },
            { value: 100, suffix: "", label: "Max conviction score" },
          ].map((stat) => (
            <div key={stat.label} className="text-center">
              <p className="text-2xl font-black text-white sm:text-3xl">
                <AnimatedNumber target={stat.value} />{stat.suffix}
              </p>
              <p className="mt-1 text-xs text-slate-500 sm:text-sm">{stat.label}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ── What you get ── */}
      <section className="px-5 py-20">
        <div className="mx-auto max-w-6xl">
          <div className="mx-auto mb-12 max-w-lg text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">The Smart Buy methodology. One card.</h2>
            <p className="text-sm text-slate-400 sm:text-base">Each card layers insider trades, congressional activity, valuation, and momentum — powered by our proprietary scoring algorithm.</p>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0ZM4.501 20.118a7.5 7.5 0 0 1 14.998 0" />
                </svg>
              }
              title="Insider Buy Tracking"
              description="Signal layer 1: We track when CEOs, CFOs, and directors buy shares of their own company. Cluster buys from multiple insiders get extra weight in our algorithm."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 21v-8.25M15.75 21v-8.25M8.25 21v-8.25M3 9l9-6 9 6m-1.5 12V10.332A48.36 48.36 0 0 0 12 9.75c-2.551 0-5.056.2-7.5.582V21" />
                </svg>
              }
              title="Congress Trade Data"
              description="Signal layer 2: When a member of Congress buys the same stock insiders are buying, that's the high-conviction Smart Buy signal our algorithm looks for."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75Z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625Z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />
                </svg>
              }
              title="Smart Buy Score (0-100)"
              description="Our secret sauce stacks all four signal layers into one proprietary conviction number. The highest Smart Buy scores rise to the top of your deck."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18 9 11.25l4.306 4.306a11.95 11.95 0 0 1 5.814-5.518l2.74-1.22m0 0-5.94-2.281m5.94 2.28-2.28 5.941" />
                </svg>
              }
              title="Momentum & Valuation"
              description="Signal layers 3 & 4: We layer in attractive valuations and price momentum. Stocks beating the market at a fair price get the highest Smart Buy scores."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 3c2.755 0 5.455.232 8.083.678.533.09.917.556.917 1.096v1.044a2.25 2.25 0 0 1-.659 1.591l-5.432 5.432a2.25 2.25 0 0 0-.659 1.591v2.927a2.25 2.25 0 0 1-1.244 2.013L9.75 21v-6.568a2.25 2.25 0 0 0-.659-1.591L3.659 7.409A2.25 2.25 0 0 1 3 5.818V4.774c0-.54.384-1.006.917-1.096A48.32 48.32 0 0 1 12 3Z" />
                </svg>
              }
              title="Smart Buy Filters"
              description="Filter by signal type, momentum, Smart Buy score, sector, valuation, and insider activity. Drill down to the exact setup you want."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 0 0-3.375-3.375h-1.5A1.125 1.125 0 0 1 13.5 7.125v-1.5a3.375 3.375 0 0 0-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 0 0-9-9Z" />
                </svg>
              }
              title="Exit Strategy Included"
              description="Every card includes a stop-loss, profit target, and sell triggers. Know when to get in and when to get out."
            />
          </div>
        </div>
      </section>

      {/* ── How it works ── */}
      <section id="how-it-works" className="border-y border-white/[0.06] bg-white/[0.01] px-5 py-20">
        <div className="mx-auto max-w-2xl">
          <div className="mb-12 text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">How it works</h2>
            <p className="text-sm text-slate-400 sm:text-base">Our proprietary algorithm runs daily, stacking four signal layers to find only the strongest Smart Buy picks.</p>
          </div>

          <div className="space-y-8">
            <Step
              number={1}
              title="Detect smart money moves"
              description="We scan thousands of SEC filings, insider trades, and congressional disclosures daily. When insiders AND Congress buy the same stock, our algorithm flags it immediately."
            />
            <Step
              number={2}
              title="Stack the signal layers"
              description="Each stock runs through our proprietary algorithm that stacks insider trades + congressional trades + attractive valuations + momentum into a single Smart Buy score."
            />
            <Step
              number={3}
              title="Cut to 30-50 actionable picks"
              description="Out of thousands of stocks, only 30-50 make the cut each day. Cards are ranked by Smart Buy score — the strongest stacked signals lead the deck, not just the biggest companies."
            />
            <Step
              number={4}
              title="You decide"
              description="Review the analysis, check the exit strategy, and if you like what you see — buy directly through Robinhood with one tap."
            />
          </div>
        </div>
      </section>

      {/* ── Pricing ── */}
      <section className="px-5 py-20">
        <div className="mx-auto max-w-4xl">
          <div className="mb-12 text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">Simple pricing</h2>
            <p className="text-sm text-slate-400 sm:text-base">Start free. Upgrade to unlock every Smart Buy signal.</p>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 mx-auto max-w-2xl">
            {/* Free tier */}
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6">
              <h3 className="mb-1 text-lg font-bold text-white">Free</h3>
              <p className="mb-4 text-sm text-slate-400">See one Smart Buy pick daily</p>
              <p className="mb-6">
                <span className="text-3xl font-black text-white">$0</span>
                <span className="text-sm text-slate-500"> /forever</span>
              </p>
              <ul className="mb-6 space-y-2.5">
                {["1 Smart Buy card per day", "Smart Buy scores visible", "Basic signal details"].map((f) => (
                  <li key={f} className="flex items-center gap-2 text-sm text-slate-400">
                    <svg className="h-4 w-4 shrink-0 text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                    {f}
                  </li>
                ))}
              </ul>
              <button
                onClick={() => router.push("/login")}
                className="w-full rounded-xl border border-white/10 py-2.5 text-sm font-semibold text-white transition hover:border-white/20 hover:bg-white/[0.04]"
              >
                Get Started
              </button>
            </div>

            {/* Pro tier */}
            <div className="relative rounded-2xl border border-cyan-500/30 bg-cyan-500/[0.04] p-6">
              <div className="absolute -top-3 left-1/2 -translate-x-1/2 rounded-full bg-cyan-500 px-3 py-0.5 text-xs font-bold text-black">
                MOST POPULAR
              </div>
              <h3 className="mb-1 text-lg font-bold text-white">Pro</h3>
              <p className="mb-4 text-sm text-slate-400">All 30-50 Smart Buy picks daily</p>

              {/* Monthly / Yearly toggle */}
              <div className="mb-4 inline-flex rounded-full border border-white/10 bg-white/[0.04] p-0.5">
                <button
                  onClick={() => setYearly(false)}
                  className={`rounded-full px-4 py-1 text-xs font-semibold transition ${!yearly ? "bg-cyan-500 text-black" : "text-slate-400 hover:text-white"}`}
                >
                  Monthly
                </button>
                <button
                  onClick={() => setYearly(true)}
                  className={`rounded-full px-4 py-1 text-xs font-semibold transition ${yearly ? "bg-cyan-500 text-black" : "text-slate-400 hover:text-white"}`}
                >
                  Yearly
                </button>
              </div>

              <p className="mb-1">
                <span className="text-3xl font-black text-white">{yearly ? "$99.99" : "$9.99"}</span>
                <span className="text-sm text-slate-500">{yearly ? " /year" : " /month"}</span>
              </p>
              <p className="mb-6 text-xs text-emerald-400 font-medium">{yearly ? "Save $19.89 vs monthly" : "$119.88/yr — switch to yearly & save"}</p>
              <ul className="mb-6 space-y-2.5">
                {[
                  "All Smart Buy picks unlocked",
                  "Full stacked signal breakdown",
                  "Insider + Congress trade data",
                  "Full analysis & exit strategy",
                  "All Smart Buy filters",
                  "Updated daily",
                ].map((f) => (
                  <li key={f} className="flex items-center gap-2 text-sm text-slate-300">
                    <svg className="h-4 w-4 shrink-0 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                    {f}
                  </li>
                ))}
              </ul>
              <button
                onClick={() => router.push("/login")}
                className="w-full rounded-xl bg-cyan-500 py-2.5 text-sm font-bold text-black transition hover:bg-cyan-400 hover:shadow-lg hover:shadow-cyan-500/25"
              >
                Subscribe
              </button>
            </div>
          </div>
        </div>
      </section>

      {/* ── Reviews ── */}
      <section className="border-t border-white/[0.06] bg-white/[0.01] px-5 py-20">
        <div className="mx-auto max-w-5xl">
          <div className="mb-12 text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">Traders love our Smart Buy picks</h2>
            <p className="text-sm text-slate-400 sm:text-base">See why our users keep coming back for fresh stacked signals every day.</p>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {[
              {
                name: "Marcus T.",
                role: "Swing Trader",
                stars: 5,
                text: "The Smart Buy scores are a game-changer. I used to spend hours screening stocks — now I just open the app and the best stacked signals are right there. Found 3 winners last week alone.",
              },
              {
                name: "Sarah K.",
                role: "Retail Investor",
                stars: 5,
                text: "I love how it stacks insider buys with Congress trades. When both are buying the same stock, that's a Smart Buy signal I trust. The card layout makes it so easy to swipe through ideas.",
              },
              {
                name: "David R.",
                role: "Part-Time Trader",
                stars: 5,
                text: "Best stock screener I've used. The Smart Buy picks are spot-on, and the stacked signal scores save me from chasing bad stocks. Worth every penny of the Pro subscription.",
              },
              {
                name: "Jennifer L.",
                role: "Long-Term Investor",
                stars: 5,
                text: "Finally an app that does the hard work for me. 30-50 Smart Buy picks daily, all scored by their proprietary algorithm. I just focus on the top-tier cards and my portfolio has never looked better.",
              },
              {
                name: "Alex M.",
                role: "Day Trader",
                stars: 5,
                text: "The stacked signals — insiders, Congress, value, and momentum — are incredibly powerful together. I've caught several stocks right before they broke out. The Smart Buy score gives me all the conviction I need.",
              },
              {
                name: "Rachel P.",
                role: "New Investor",
                stars: 5,
                text: "As someone new to investing, this app is perfect. The Smart Buy scores tell me exactly which stocks have the strongest stacked signals, and the thesis explains why in plain English. Couldn't ask for more.",
              },
            ].map((review) => (
              <div key={review.name} className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
                <div className="mb-3 flex items-center gap-1">
                  {Array.from({ length: review.stars }).map((_, i) => (
                    <svg key={i} className="h-4 w-4 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
                      <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
                    </svg>
                  ))}
                </div>
                <p className="mb-4 text-sm leading-relaxed text-slate-300">&ldquo;{review.text}&rdquo;</p>
                <div>
                  <p className="text-sm font-semibold text-white">{review.name}</p>
                  <p className="text-xs text-slate-500">{review.role}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Final CTA ── */}
      <section className="border-t border-white/[0.06] px-5 py-20">
        <div className="mx-auto max-w-lg text-center">
          <h2 className="mb-4 text-2xl font-bold text-white sm:text-3xl">
            Insiders + Congress + Value + Momentum = Smart Buy.
          </h2>
          <p className="mb-8 text-sm text-slate-400 sm:text-base">
            Every day our proprietary algorithm stacks four signal layers and surfaces only 30-50 high-conviction Smart Buy picks — so you can act before the crowd catches on.
          </p>
          <button
            onClick={() => router.push("/login")}
            className="rounded-full bg-cyan-500 px-10 py-3 text-base font-bold text-black transition hover:bg-cyan-400 hover:shadow-lg hover:shadow-cyan-500/25"
          >
            See Today&apos;s Smart Buys
          </button>
        </div>
      </section>

      {/* ── Footer ── */}
      <footer className="border-t border-white/[0.06] px-5 py-8">
        <div className="mx-auto flex max-w-6xl flex-col items-center gap-4 sm:flex-row sm:justify-between">
          <div className="flex items-center gap-4">
            <span className="text-xs text-slate-600">
              by{" "}
              <a href="https://zorvalabs.com" target="_blank" rel="noopener noreferrer" className="transition hover:text-slate-400" style={{ color: "#30d158" }}>
                Zorva Labs
              </a>
            </span>
          </div>
          <div className="flex items-center gap-6">
            <a href="mailto:zorvalabs@outlook.com" className="text-xs text-slate-600 transition hover:text-slate-400">
              Support
            </a>
            <span className="text-xs text-slate-700">Not financial advice.</span>
          </div>
        </div>
      </footer>
    </div>
  )
}
