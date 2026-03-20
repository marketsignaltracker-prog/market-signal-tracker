"use client"

import { useRouter } from "next/navigation"
import { useEffect, useState } from "react"

/* ---------- Fake card data for the hero preview ---------- */
const PREVIEW_CARDS = [
  { ticker: "PLTR", name: "Palantir Technologies", score: 94, sector: "Technology", price: 87.42, change: "+12.3%", insiders: 3, congress: true },
  { ticker: "UTHR", name: "United Therapeutics", score: 93, sector: "Healthcare", price: 342.18, change: "+9.1%", insiders: 5, congress: false },
  { ticker: "AXON", name: "Axon Enterprise", score: 89, sector: "Technology", price: 614.30, change: "+7.8%", insiders: 2, congress: true },
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

/* ---------- Mini preview card ---------- */
function PreviewCard({ card, index, isCenter }: { card: typeof PREVIEW_CARDS[0]; index: number; isCenter: boolean }) {
  const scoreColor = card.score >= 90 ? "#30d158" : card.score >= 75 ? "#22d3ee" : "#f0a500"
  return (
    <div
      className={`relative w-[280px] shrink-0 rounded-2xl border bg-[#0d1526] p-5 transition-all duration-500 ${
        isCenter
          ? "z-20 scale-100 border-white/10 opacity-100 shadow-2xl shadow-cyan-500/10"
          : "z-10 scale-90 border-white/[0.05] opacity-40 blur-[1px]"
      }`}
      style={{ transform: isCenter ? undefined : `translateX(${index === 0 ? "20px" : "-20px"})` }}
    >
      {/* Score badge */}
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-baseline gap-1">
          <span className="text-3xl font-black" style={{ color: scoreColor }}>{card.score}</span>
          <span className="text-sm text-slate-500">/100</span>
        </div>
        {card.congress && (
          <span className="rounded-full bg-purple-500/15 border border-purple-500/30 px-2 py-0.5 text-[9px] font-bold text-purple-400">
            CONGRESS
          </span>
        )}
      </div>

      {/* Ticker */}
      <h3 className="text-2xl font-black tracking-tight text-white">{card.ticker}</h3>
      <p className="mb-4 text-xs text-slate-500">{card.name} · {card.sector}</p>

      {/* Metrics grid */}
      <div className="grid grid-cols-3 gap-2">
        <div className="rounded-lg bg-white/[0.03] p-2 text-center">
          <p className="text-[9px] text-slate-500">PRICE</p>
          <p className="text-sm font-semibold text-white">${card.price}</p>
        </div>
        <div className="rounded-lg bg-white/[0.03] p-2 text-center">
          <p className="text-[9px] text-slate-500">20D</p>
          <p className="text-sm font-semibold text-emerald-400">{card.change}</p>
        </div>
        <div className="rounded-lg bg-white/[0.03] p-2 text-center">
          <p className="text-[9px] text-slate-500">INSIDERS</p>
          <p className="text-sm font-semibold text-amber-400">{card.insiders}</p>
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

  useEffect(() => {
    const interval = setInterval(() => {
      setActiveCard((prev) => (prev + 1) % 3)
    }, 3000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="min-h-screen overflow-x-hidden">

      {/* ── Nav ── */}
      <nav className="fixed top-0 z-50 w-full border-b border-white/[0.06] bg-[#080d18]/80 backdrop-blur-xl">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-5 py-3">
          <div className="flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-cyan-500/15">
              <svg className="h-4 w-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />
              </svg>
            </div>
            <span className="text-sm font-bold text-white tracking-wide">SIGNAL TRACKER</span>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={() => router.push("/login")}
              className="text-sm text-slate-400 transition hover:text-white"
            >
              Sign In
            </button>
            <button
              onClick={() => router.push("/login")}
              className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-semibold text-black transition hover:bg-cyan-400"
            >
              Get Started
            </button>
          </div>
        </div>
      </nav>

      {/* ── Hero ── */}
      <section className="relative pt-28 pb-20 px-5">
        {/* Background glow effects */}
        <div className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute left-1/2 top-0 h-[600px] w-[800px] -translate-x-1/2 rounded-full bg-cyan-500/[0.07] blur-[120px]" />
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
              <span className="text-xs font-medium text-cyan-400">Updated daily with fresh signals</span>
            </div>

            <h1 className="mb-5 text-4xl font-black leading-[1.1] tracking-tight text-white sm:text-5xl md:text-6xl">
              Stop guessing.
              <br />
              <span className="bg-gradient-to-r from-cyan-400 to-emerald-400 bg-clip-text text-transparent">
                Follow the smart money.
              </span>
            </h1>

            <p className="mb-8 text-base leading-relaxed text-slate-400 sm:text-lg">
              We screen every SEC filing daily to find stocks where insiders and congress members are buying.
              Each stock gets scored on fundamentals, momentum, and signal strength — so you see the best ideas first.
            </p>

            <div className="flex flex-col items-center gap-3 sm:flex-row sm:justify-center">
              <button
                onClick={() => router.push("/login")}
                className="w-full rounded-full bg-cyan-500 px-8 py-3 text-base font-bold text-black transition hover:bg-cyan-400 hover:shadow-lg hover:shadow-cyan-500/25 sm:w-auto"
              >
                Start Free — See 3 Picks
              </button>
              <button
                onClick={() => {
                  document.getElementById("how-it-works")?.scrollIntoView({ behavior: "smooth" })
                }}
                className="w-full rounded-full border border-white/10 px-8 py-3 text-base font-medium text-slate-300 transition hover:border-white/20 hover:text-white sm:w-auto"
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
            { value: 13000, suffix: "+", label: "SEC filings scanned" },
            { value: 30, suffix: "+", label: "Buy ideas daily" },
            { value: 7, suffix: "", label: "Signal categories" },
            { value: 100, suffix: "", label: "Quality score max" },
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
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">Every signal. One card.</h2>
            <p className="text-sm text-slate-400 sm:text-base">Each stock card combines insider trades, congress buys, fundamentals, and momentum into a single actionable view.</p>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 1 1-7.5 0 3.75 3.75 0 0 1 7.5 0ZM4.501 20.118a7.5 7.5 0 0 1 14.998 0" />
                </svg>
              }
              title="Insider Buy Tracking"
              description="See when CEOs, CFOs, and directors buy shares of their own company. Cluster buys from multiple insiders are flagged."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 21v-8.25M15.75 21v-8.25M8.25 21v-8.25M3 9l9-6 9 6m-1.5 12V10.332A48.36 48.36 0 0 0 12 9.75c-2.551 0-5.056.2-7.5.582V21" />
                </svg>
              }
              title="Congress Trade Data"
              description="Track what members of Congress are buying. Required disclosures (PTR forms) are parsed and matched to stocks."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75Z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625Z" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z" />
                </svg>
              }
              title="Quality Score (0-100)"
              description="Every stock is scored on profitability, cash flow, debt, moat, valuation, and momentum. Higher score = stronger signal."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 18 9 11.25l4.306 4.306a11.95 11.95 0 0 1 5.814-5.518l2.74-1.22m0 0-5.94-2.281m5.94 2.28-2.28 5.941" />
                </svg>
              }
              title="Momentum & Returns"
              description="See 1-day, 5-day, 10-day, and 20-day returns at a glance. Relative strength vs the market is calculated automatically."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 3c2.755 0 5.455.232 8.083.678.533.09.917.556.917 1.096v1.044a2.25 2.25 0 0 1-.659 1.591l-5.432 5.432a2.25 2.25 0 0 0-.659 1.591v2.927a2.25 2.25 0 0 1-1.244 2.013L9.75 21v-6.568a2.25 2.25 0 0 0-.659-1.591L3.659 7.409A2.25 2.25 0 0 1 3 5.818V4.774c0-.54.384-1.006.917-1.096A48.32 48.32 0 0 1 12 3Z" />
                </svg>
              }
              title="Smart Filters"
              description="Filter by score, price, sector, valuation, insider activity, and congress trades. Zero in on exactly what you're looking for."
            />
            <FeatureCard
              icon={
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 0 0-3.375-3.375h-1.5A1.125 1.125 0 0 1 13.5 7.125v-1.5a3.375 3.375 0 0 0-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 0 0-9-9Z" />
                </svg>
              }
              title="Full Analysis & Thesis"
              description="Tap any card for a deep-dive: AI-generated thesis, fundamental breakdown, filing details, and a direct link to buy."
            />
          </div>
        </div>
      </section>

      {/* ── How it works ── */}
      <section id="how-it-works" className="border-y border-white/[0.06] bg-white/[0.01] px-5 py-20">
        <div className="mx-auto max-w-2xl">
          <div className="mb-12 text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">How it works</h2>
            <p className="text-sm text-slate-400 sm:text-base">Our pipeline runs daily so you always have fresh ideas.</p>
          </div>

          <div className="space-y-8">
            <Step
              number={1}
              title="We scan SEC filings"
              description="Every day, we pull the latest Form 4s (insider trades) and PTR disclosures (congress trades) directly from SEC EDGAR."
            />
            <Step
              number={2}
              title="Screen for quality"
              description="Each stock with a buy signal is scored on 7 fundamental factors: profitability, cash flow, debt, moat, valuation, stability, and momentum."
            />
            <Step
              number={3}
              title="Rank and deliver"
              description="Stocks are ranked by overall score. The top ideas appear as swipeable cards with everything you need to make a decision."
            />
            <Step
              number={4}
              title="You decide"
              description="Review the analysis, check the thesis, and if you like what you see — buy directly through Robinhood with one tap."
            />
          </div>
        </div>
      </section>

      {/* ── Pricing ── */}
      <section className="px-5 py-20">
        <div className="mx-auto max-w-4xl">
          <div className="mb-12 text-center">
            <h2 className="mb-3 text-2xl font-bold text-white sm:text-3xl">Simple pricing</h2>
            <p className="text-sm text-slate-400 sm:text-base">Start free. Upgrade when you want the full picture.</p>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 mx-auto max-w-2xl">
            {/* Free tier */}
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6">
              <h3 className="mb-1 text-lg font-bold text-white">Free</h3>
              <p className="mb-4 text-sm text-slate-400">Get a taste of the signals</p>
              <p className="mb-6">
                <span className="text-3xl font-black text-white">$0</span>
                <span className="text-sm text-slate-500"> /forever</span>
              </p>
              <ul className="mb-6 space-y-2.5">
                {["3 buy cards per day", "Quality scores visible", "Basic card details"].map((f) => (
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
              <p className="mb-4 text-sm text-slate-400">Full access to every signal</p>
              <p className="mb-1">
                <span className="text-3xl font-black text-white">$99.99</span>
                <span className="text-sm text-slate-500"> /year</span>
              </p>
              <p className="mb-6 text-xs text-emerald-400 font-medium">Save $19.89 vs monthly ($9.99/mo)</p>
              <ul className="mb-6 space-y-2.5">
                {[
                  "All buy ideas unlocked",
                  "Insider buying data",
                  "Congress trade data",
                  "Full analysis & thesis",
                  "All filters unlocked",
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

      {/* ── Final CTA ── */}
      <section className="border-t border-white/[0.06] px-5 py-20">
        <div className="mx-auto max-w-lg text-center">
          <h2 className="mb-4 text-2xl font-bold text-white sm:text-3xl">
            The best trades start with the best data.
          </h2>
          <p className="mb-8 text-sm text-slate-400 sm:text-base">
            Join traders who use insider and congress signals to find high-conviction ideas before the crowd.
          </p>
          <button
            onClick={() => router.push("/login")}
            className="rounded-full bg-cyan-500 px-10 py-3 text-base font-bold text-black transition hover:bg-cyan-400 hover:shadow-lg hover:shadow-cyan-500/25"
          >
            Get Started Free
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
            <a href="mailto:support@zorvalabs.com" className="text-xs text-slate-600 transition hover:text-slate-400">
              Support
            </a>
            <span className="text-xs text-slate-700">Not financial advice.</span>
          </div>
        </div>
      </footer>
    </div>
  )
}
