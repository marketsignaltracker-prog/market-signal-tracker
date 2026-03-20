"use client"

import { useEffect, useState, useRef } from "react"

/* ═══════════════════════════════════════════
   ZORVA LABS — SVG LOGO
   ═══════════════════════════════════════════ */
function ZorvaLogo({ size = 40 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="logo-g1" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#a855f7" />
          <stop offset="50%" stopColor="#ec4899" />
          <stop offset="100%" stopColor="#f59e0b" />
        </linearGradient>
        <linearGradient id="logo-g2" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#06b6d4" />
          <stop offset="100%" stopColor="#a855f7" />
        </linearGradient>
      </defs>
      <circle cx="32" cy="32" r="30" fill="#0a0a1a" stroke="url(#logo-g1)" strokeWidth="2" />
      <path d="M18 18 L46 18 L18 46 L46 46" fill="none" stroke="url(#logo-g1)" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="44" cy="22" r="3" fill="url(#logo-g2)" opacity="0.8" />
      <circle cx="48" cy="16" r="2" fill="#ec4899" opacity="0.6" />
      <circle cx="20" cy="42" r="2.5" fill="#06b6d4" opacity="0.7" />
    </svg>
  )
}

/* ═══════════════════════════════════════════
   ANIMATED COUNTER
   ═══════════════════════════════════════════ */
function AnimatedNumber({ target, suffix = "", duration = 2000 }: { target: number; suffix?: string; duration?: number }) {
  const [value, setValue] = useState(0)
  const [started, setStarted] = useState(false)
  const ref = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) setStarted(true) },
      { threshold: 0.3 }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    if (!started) return
    const start = Date.now()
    const tick = () => {
      const elapsed = Date.now() - start
      const progress = Math.min(elapsed / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setValue(Math.round(target * eased))
      if (progress < 1) requestAnimationFrame(tick)
    }
    requestAnimationFrame(tick)
  }, [started, target, duration])

  return <span ref={ref}>{value}{suffix}</span>
}

/* ═══════════════════════════════════════════
   INTERSECTION OBSERVER HOOK
   ═══════════════════════════════════════════ */
function useInView(threshold = 0.15) {
  const ref = useRef<HTMLDivElement>(null)
  const [inView, setInView] = useState(false)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) setInView(true) },
      { threshold }
    )
    if (ref.current) observer.observe(ref.current)
    return () => observer.disconnect()
  }, [threshold])

  return { ref, inView }
}

/* ═══════════════════════════════════════════
   FLOATING PARTICLES BACKGROUND
   ═══════════════════════════════════════════ */
function FloatingParticles() {
  return (
    <div className="pointer-events-none fixed inset-0 z-0 overflow-hidden">
      {/* Large morphing blobs */}
      <div className="animate-morph absolute -left-20 top-20 h-96 w-96 bg-purple-500/[0.07] blur-[100px]" />
      <div className="animate-morph absolute right-0 top-1/3 h-80 w-80 bg-pink-500/[0.06] blur-[100px]" style={{ animationDelay: "-3s" }} />
      <div className="animate-morph absolute bottom-20 left-1/3 h-72 w-72 bg-cyan-500/[0.05] blur-[100px]" style={{ animationDelay: "-5s" }} />
      <div className="animate-morph absolute right-1/4 top-2/3 h-64 w-64 bg-amber-500/[0.04] blur-[100px]" style={{ animationDelay: "-7s" }} />

      {/* Floating dots */}
      {[
        { size: 4, color: "#a855f7", top: "10%", left: "15%", delay: "0s", dur: "float" },
        { size: 3, color: "#ec4899", top: "25%", left: "80%", delay: "-2s", dur: "float-reverse" },
        { size: 5, color: "#06b6d4", top: "60%", left: "10%", delay: "-4s", dur: "float" },
        { size: 3, color: "#f59e0b", top: "75%", left: "70%", delay: "-1s", dur: "float-reverse" },
        { size: 4, color: "#a855f7", top: "45%", left: "90%", delay: "-3s", dur: "float" },
        { size: 6, color: "#ec4899", top: "85%", left: "30%", delay: "-5s", dur: "float" },
        { size: 3, color: "#06b6d4", top: "15%", left: "55%", delay: "-2s", dur: "float-reverse" },
        { size: 4, color: "#f59e0b", top: "50%", left: "40%", delay: "-6s", dur: "float" },
      ].map((dot, i) => (
        <div
          key={i}
          className={dot.dur === "float" ? "animate-float" : "animate-float-reverse"}
          style={{
            position: "absolute",
            top: dot.top,
            left: dot.left,
            width: dot.size,
            height: dot.size,
            borderRadius: "50%",
            background: dot.color,
            opacity: 0.4,
            animationDelay: dot.delay,
          }}
        />
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════
   SERVICE CARD
   ═══════════════════════════════════════════ */
function ServiceCard({ icon, title, description, gradient, delay }: {
  icon: React.ReactNode
  title: string
  description: string
  gradient: string
  delay: number
}) {
  const { ref, inView } = useInView()
  return (
    <div
      ref={ref}
      className={`glass-card group relative rounded-2xl p-6 transition-all duration-500 hover:scale-[1.03] hover:shadow-2xl ${
        inView ? "animate-slide-up" : "opacity-0"
      }`}
      style={{ animationDelay: `${delay}s` }}
    >
      {/* Hover glow */}
      <div className={`absolute -inset-px rounded-2xl bg-gradient-to-br ${gradient} opacity-0 blur-xl transition-opacity duration-500 group-hover:opacity-20`} />

      <div className={`relative mb-4 flex h-14 w-14 items-center justify-center rounded-xl bg-gradient-to-br ${gradient} shadow-lg`}>
        {icon}
      </div>
      <h3 className="relative mb-2 text-xl font-bold text-white">{title}</h3>
      <p className="relative text-sm leading-relaxed text-slate-400">{description}</p>
    </div>
  )
}

/* ═══════════════════════════════════════════
   PROCESS STEP
   ═══════════════════════════════════════════ */
function ProcessStep({ number, title, description, color, delay }: {
  number: number
  title: string
  description: string
  color: string
  delay: number
}) {
  const { ref, inView } = useInView()
  return (
    <div
      ref={ref}
      className={`flex gap-5 ${inView ? "animate-slide-up" : "opacity-0"}`}
      style={{ animationDelay: `${delay}s` }}
    >
      <div className="relative flex flex-col items-center">
        <div
          className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full text-lg font-black text-white shadow-lg"
          style={{ background: `linear-gradient(135deg, ${color}, ${color}88)` }}
        >
          {number}
        </div>
        {number < 4 && <div className="mt-2 h-full w-px bg-gradient-to-b from-white/10 to-transparent" />}
      </div>
      <div className="pb-10">
        <h3 className="mb-1 text-lg font-bold text-white">{title}</h3>
        <p className="text-sm leading-relaxed text-slate-400">{description}</p>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════
   TECH MARQUEE
   ═══════════════════════════════════════════ */
function TechMarquee() {
  const techs = [
    "React", "Next.js", "React Native", "Swift", "Kotlin", "Flutter",
    "Node.js", "Python", "TypeScript", "Tailwind", "PostgreSQL", "AWS",
    "Google Ads", "SEO", "Analytics", "Figma", "Vercel", "Supabase",
  ]
  return (
    <div className="relative overflow-hidden py-4">
      <div className="absolute left-0 top-0 z-10 h-full w-20 bg-gradient-to-r from-[#0a0a1a] to-transparent" />
      <div className="absolute right-0 top-0 z-10 h-full w-20 bg-gradient-to-l from-[#0a0a1a] to-transparent" />
      <div className="animate-ticker flex gap-6 whitespace-nowrap">
        {[...techs, ...techs].map((tech, i) => (
          <span key={i} className="rounded-full border border-white/[0.06] bg-white/[0.02] px-5 py-2 text-sm font-medium text-slate-400">
            {tech}
          </span>
        ))}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════
   PORTFOLIO CARD
   ═══════════════════════════════════════════ */
function PortfolioCard({ title, category, gradient, delay }: {
  title: string
  category: string
  gradient: string
  delay: number
}) {
  const { ref, inView } = useInView()
  return (
    <div
      ref={ref}
      className={`group relative overflow-hidden rounded-2xl ${inView ? "animate-scale-in" : "opacity-0"}`}
      style={{ animationDelay: `${delay}s` }}
    >
      <div className={`aspect-[4/3] bg-gradient-to-br ${gradient} p-8 transition-transform duration-500 group-hover:scale-105`}>
        <div className="flex h-full flex-col justify-end">
          <span className="mb-2 text-xs font-semibold uppercase tracking-widest text-white/60">{category}</span>
          <h3 className="text-xl font-bold text-white">{title}</h3>
        </div>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════
   TESTIMONIAL CARD
   ═══════════════════════════════════════════ */
function TestimonialCard({ name, role, text, delay }: {
  name: string
  role: string
  text: string
  delay: number
}) {
  const { ref, inView } = useInView()
  return (
    <div
      ref={ref}
      className={`glass-card rounded-2xl p-6 transition-all duration-300 hover:border-purple-500/20 ${
        inView ? "animate-slide-up" : "opacity-0"
      }`}
      style={{ animationDelay: `${delay}s` }}
    >
      <div className="mb-4 flex gap-1">
        {Array.from({ length: 5 }).map((_, i) => (
          <svg key={i} className="h-4 w-4 text-amber-400" fill="currentColor" viewBox="0 0 20 20">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
        ))}
      </div>
      <p className="mb-4 text-sm leading-relaxed text-slate-300">&ldquo;{text}&rdquo;</p>
      <div>
        <p className="text-sm font-semibold text-white">{name}</p>
        <p className="text-xs text-slate-500">{role}</p>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════
   MAIN LANDING PAGE
   ═══════════════════════════════════════════════════ */
export default function ZorvaLabsPage() {
  const [menuOpen, setMenuOpen] = useState(false)
  const [scrolled, setScrolled] = useState(false)

  useEffect(() => {
    const handleScroll = () => setScrolled(window.scrollY > 50)
    window.addEventListener("scroll", handleScroll)
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  const scrollTo = (id: string) => {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth" })
    setMenuOpen(false)
  }

  return (
    <div className="relative min-h-screen overflow-x-hidden">
      <FloatingParticles />

      {/* ═══ NAVBAR ═══ */}
      <nav className={`fixed top-0 z-50 w-full transition-all duration-300 ${
        scrolled ? "border-b border-white/[0.06] bg-[#0a0a1a]/90 backdrop-blur-xl shadow-2xl" : "bg-transparent"
      }`}>
        <div className="mx-auto flex max-w-6xl items-center justify-between px-5 py-4">
          <button onClick={() => scrollTo("hero")} className="flex items-center gap-2.5">
            <ZorvaLogo size={36} />
            <span className="text-lg font-black tracking-wide text-white">
              ZORVA <span className="gradient-text">LABS</span>
            </span>
          </button>

          {/* Desktop nav */}
          <div className="hidden items-center gap-8 md:flex">
            {["Services", "Work", "Process", "Testimonials", "Contact"].map((item) => (
              <button
                key={item}
                onClick={() => scrollTo(item.toLowerCase())}
                className="text-sm font-medium text-slate-400 transition hover:text-white"
              >
                {item}
              </button>
            ))}
            <button
              onClick={() => scrollTo("contact")}
              className="animate-gradient rounded-full bg-gradient-to-r from-purple-500 via-pink-500 to-amber-500 px-6 py-2 text-sm font-bold text-white transition hover:shadow-lg hover:shadow-purple-500/25"
            >
              Get Started
            </button>
          </div>

          {/* Mobile menu button */}
          <button onClick={() => setMenuOpen(!menuOpen)} className="md:hidden text-white">
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              {menuOpen ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              )}
            </svg>
          </button>
        </div>

        {/* Mobile menu */}
        {menuOpen && (
          <div className="border-t border-white/[0.06] bg-[#0a0a1a]/95 backdrop-blur-xl px-5 py-6 md:hidden">
            {["Services", "Work", "Process", "Testimonials", "Contact"].map((item) => (
              <button
                key={item}
                onClick={() => scrollTo(item.toLowerCase())}
                className="block w-full py-3 text-left text-base font-medium text-slate-300 transition hover:text-white"
              >
                {item}
              </button>
            ))}
            <button
              onClick={() => scrollTo("contact")}
              className="mt-4 w-full animate-gradient rounded-full bg-gradient-to-r from-purple-500 via-pink-500 to-amber-500 py-3 text-sm font-bold text-white"
            >
              Get Started
            </button>
          </div>
        )}
      </nav>

      {/* ═══ HERO ═══ */}
      <section id="hero" className="relative flex min-h-screen items-center px-5 pt-20">
        {/* Animated rings */}
        <div className="pointer-events-none absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2">
          <div className="animate-spin-slow h-[600px] w-[600px] rounded-full border border-purple-500/10" />
          <div className="animate-spin-slow absolute inset-10 rounded-full border border-pink-500/10" style={{ animationDirection: "reverse", animationDuration: "25s" }} />
          <div className="animate-spin-slow absolute inset-20 rounded-full border border-cyan-500/10" style={{ animationDuration: "30s" }} />
        </div>

        <div className="relative mx-auto max-w-6xl text-center">
          {/* Badge */}
          <div className="animate-slide-up mb-8 inline-flex items-center gap-2 rounded-full border border-purple-500/20 bg-purple-500/[0.08] px-5 py-2">
            <span className="relative flex h-2 w-2">
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-purple-400 opacity-75" />
              <span className="relative inline-flex h-2 w-2 rounded-full bg-purple-500" />
            </span>
            <span className="text-xs font-semibold tracking-wide text-purple-300">NOW ACCEPTING NEW PROJECTS</span>
          </div>

          {/* Hero heading */}
          <h1 className="animate-slide-up-delay-1 mb-6 text-5xl font-black leading-[1.05] tracking-tight text-white sm:text-6xl md:text-7xl lg:text-8xl">
            Where Ideas
            <br />
            <span className="gradient-text">Ignite.</span>
          </h1>

          {/* Tagline */}
          <p className="animate-slide-up-delay-2 mx-auto mb-10 max-w-2xl text-lg leading-relaxed text-slate-400 sm:text-xl">
            We build apps, websites, and digital strategies that move your business forward.
            <span className="text-white font-medium"> Beautiful design meets bold performance.</span>
          </p>

          {/* CTA buttons */}
          <div className="animate-slide-up-delay-3 flex flex-col items-center gap-4 sm:flex-row sm:justify-center">
            <button
              onClick={() => scrollTo("contact")}
              className="animate-gradient group relative w-full rounded-full bg-gradient-to-r from-purple-500 via-pink-500 to-amber-500 px-10 py-4 text-base font-bold text-white transition hover:shadow-2xl hover:shadow-purple-500/30 sm:w-auto"
            >
              <span className="relative z-10">Start Your Project</span>
            </button>
            <button
              onClick={() => scrollTo("work")}
              className="w-full rounded-full border border-white/10 px-10 py-4 text-base font-medium text-slate-300 transition hover:border-white/20 hover:bg-white/[0.03] hover:text-white sm:w-auto"
            >
              See Our Work
            </button>
          </div>

          {/* Bouncing scroll indicator */}
          <div className="animate-slide-up-delay-4 mt-20">
            <button onClick={() => scrollTo("services")} className="animate-bounce-subtle inline-flex flex-col items-center gap-2 text-slate-500 transition hover:text-slate-300">
              <span className="text-xs tracking-widest uppercase">Explore</span>
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M19 14l-7 7m0 0l-7-7m7 7V3" />
              </svg>
            </button>
          </div>
        </div>
      </section>

      {/* ═══ TECH MARQUEE ═══ */}
      <section className="border-y border-white/[0.04] py-6">
        <TechMarquee />
      </section>

      {/* ═══ SERVICES ═══ */}
      <section id="services" className="relative px-5 py-24">
        <div className="mx-auto max-w-6xl">
          <div className="mx-auto mb-16 max-w-lg text-center">
            <span className="mb-3 inline-block text-xs font-bold uppercase tracking-[0.2em] text-purple-400">What We Do</span>
            <h2 className="mb-4 text-3xl font-black text-white sm:text-4xl">
              Services that <span className="gradient-text-pink">spark growth</span>
            </h2>
            <p className="text-slate-400">From concept to launch and beyond — we handle every stage of your digital journey.</p>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-4">
            <ServiceCard
              delay={0}
              gradient="from-purple-500 to-violet-600"
              icon={
                <svg className="h-7 w-7 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M10.5 1.5H8.25A2.25 2.25 0 0 0 6 3.75v16.5a2.25 2.25 0 0 0 2.25 2.25h7.5A2.25 2.25 0 0 0 18 20.25V3.75a2.25 2.25 0 0 0-2.25-2.25H13.5m-3 0V3h3V1.5m-3 0h3m-3 18.75h3" />
                </svg>
              }
              title="App Development"
              description="Native iOS & Android apps, cross-platform with React Native or Flutter. Sleek UX, bulletproof performance."
            />
            <ServiceCard
              delay={0.1}
              gradient="from-pink-500 to-rose-600"
              icon={
                <svg className="h-7 w-7 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 21a9.004 9.004 0 0 0 8.716-6.747M12 21a9.004 9.004 0 0 1-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 0 1 7.843 4.582M12 3a8.997 8.997 0 0 0-7.843 4.582m15.686 0A11.953 11.953 0 0 1 12 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0 1 21 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0 1 12 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 0 1 3 12c0-1.605.42-3.113 1.157-4.418" />
                </svg>
              }
              title="Website Services"
              description="Stunning, fast websites built with modern frameworks. From landing pages to full-stack web applications."
            />
            <ServiceCard
              delay={0.2}
              gradient="from-cyan-500 to-blue-600"
              icon={
                <svg className="h-7 w-7 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607ZM10.5 7.5v6m3-3h-6" />
                </svg>
              }
              title="SEO Optimization"
              description="Get found. We optimize your site structure, content, and technical SEO so you rank where it matters."
            />
            <ServiceCard
              delay={0.3}
              gradient="from-amber-500 to-orange-600"
              icon={
                <svg className="h-7 w-7 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 3v11.25A2.25 2.25 0 0 0 6 16.5h2.25M3.75 3h-1.5m1.5 0h16.5m0 0h1.5m-1.5 0v11.25A2.25 2.25 0 0 1 18 16.5h-2.25m-7.5 0h7.5m-7.5 0-1 3m8.5-3 1 3m0 0 .5 1.5m-.5-1.5h-9.5m0 0-.5 1.5m.75-9 3-3 2.148 2.148A12.061 12.061 0 0 1 16.5 7.605" />
                </svg>
              }
              title="Digital Marketing"
              description="Data-driven campaigns across Google, Meta, and more. We turn clicks into customers and scale what works."
            />
          </div>
        </div>
      </section>

      {/* ═══ STATS BAR ═══ */}
      <section className="border-y border-white/[0.04] bg-white/[0.01]">
        <div className="mx-auto grid max-w-5xl grid-cols-2 gap-8 px-5 py-14 sm:grid-cols-4">
          {[
            { value: 150, suffix: "+", label: "Projects Delivered", color: "gradient-text-purple" },
            { value: 50, suffix: "+", label: "Happy Clients", color: "gradient-text-pink" },
            { value: 99, suffix: "%", label: "Client Satisfaction", color: "gradient-text-cyan" },
            { value: 5, suffix: " yrs", label: "In Business", color: "gradient-text-amber" },
          ].map((stat) => (
            <div key={stat.label} className="text-center">
              <p className={`text-3xl font-black sm:text-4xl ${stat.color}`}>
                <AnimatedNumber target={stat.value} suffix={stat.suffix} />
              </p>
              <p className="mt-2 text-xs text-slate-500 sm:text-sm">{stat.label}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ═══ PORTFOLIO / WORK ═══ */}
      <section id="work" className="px-5 py-24">
        <div className="mx-auto max-w-6xl">
          <div className="mx-auto mb-16 max-w-lg text-center">
            <span className="mb-3 inline-block text-xs font-bold uppercase tracking-[0.2em] text-pink-400">Our Work</span>
            <h2 className="mb-4 text-3xl font-black text-white sm:text-4xl">
              Projects that <span className="gradient-text-cyan">stand out</span>
            </h2>
            <p className="text-slate-400">A glimpse of the digital experiences we&apos;ve crafted for our clients.</p>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
            <PortfolioCard delay={0} title="FinTrack Pro" category="Mobile App" gradient="from-purple-600 via-purple-800 to-indigo-900" />
            <PortfolioCard delay={0.1} title="Bloom & Co" category="E-Commerce Website" gradient="from-pink-600 via-rose-700 to-red-900" />
            <PortfolioCard delay={0.2} title="NexusAI Dashboard" category="Web Application" gradient="from-cyan-600 via-blue-700 to-indigo-900" />
            <PortfolioCard delay={0.3} title="GreenLeaf Organics" category="SEO & Marketing" gradient="from-emerald-600 via-green-700 to-teal-900" />
            <PortfolioCard delay={0.4} title="CloudSync Platform" category="SaaS Website" gradient="from-amber-600 via-orange-700 to-red-900" />
            <PortfolioCard delay={0.5} title="UrbanEats" category="Mobile App" gradient="from-violet-600 via-purple-700 to-fuchsia-900" />
          </div>
        </div>
      </section>

      {/* ═══ PROCESS ═══ */}
      <section id="process" className="border-y border-white/[0.04] bg-white/[0.01] px-5 py-24">
        <div className="mx-auto max-w-3xl">
          <div className="mx-auto mb-16 max-w-lg text-center">
            <span className="mb-3 inline-block text-xs font-bold uppercase tracking-[0.2em] text-cyan-400">Our Process</span>
            <h2 className="mb-4 text-3xl font-black text-white sm:text-4xl">
              How we <span className="gradient-text-amber">bring ideas to life</span>
            </h2>
            <p className="text-slate-400">A proven process that turns your vision into a polished digital product.</p>
          </div>

          <div className="space-y-0">
            <ProcessStep
              delay={0}
              number={1}
              color="#a855f7"
              title="Discovery & Strategy"
              description="We dive deep into your goals, audience, and competitive landscape. Together we define the scope, features, and success metrics that will guide the entire build."
            />
            <ProcessStep
              delay={0.15}
              number={2}
              color="#ec4899"
              title="Design & Prototype"
              description="Our designers create stunning visual concepts and interactive prototypes. You see and feel the product before a single line of code is written."
            />
            <ProcessStep
              delay={0.3}
              number={3}
              color="#06b6d4"
              title="Build & Iterate"
              description="Our engineers build with modern, scalable tech. We ship in sprints so you can test, provide feedback, and watch your product come to life incrementally."
            />
            <ProcessStep
              delay={0.45}
              number={4}
              color="#f59e0b"
              title="Launch & Grow"
              description="We handle deployment, SEO, analytics, and marketing setup. Post-launch, we monitor performance and continuously optimize for growth."
            />
          </div>
        </div>
      </section>

      {/* ═══ TESTIMONIALS ═══ */}
      <section id="testimonials" className="px-5 py-24">
        <div className="mx-auto max-w-6xl">
          <div className="mx-auto mb-16 max-w-lg text-center">
            <span className="mb-3 inline-block text-xs font-bold uppercase tracking-[0.2em] text-amber-400">Testimonials</span>
            <h2 className="mb-4 text-3xl font-black text-white sm:text-4xl">
              Loved by <span className="gradient-text">our clients</span>
            </h2>
            <p className="text-slate-400">Don&apos;t just take our word for it — hear from the people we&apos;ve worked with.</p>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
            <TestimonialCard
              delay={0}
              name="Sarah Chen"
              role="CEO, FinTrack Pro"
              text="Zorva Labs turned our napkin sketch into a top-rated finance app. Their attention to detail and speed of delivery blew us away. The app hit 50k downloads in the first month."
            />
            <TestimonialCard
              delay={0.1}
              name="Marcus Rivera"
              role="Founder, Bloom & Co"
              text="Our online sales tripled after Zorva rebuilt our website. The design is gorgeous, it loads instantly, and the SEO work has us ranking #1 for our key terms. Incredible team."
            />
            <TestimonialCard
              delay={0.2}
              name="Emily Watson"
              role="VP Marketing, NexusAI"
              text="Working with Zorva Labs felt like having a world-class in-house team. They understood our vision immediately and delivered a dashboard that our enterprise clients love."
            />
            <TestimonialCard
              delay={0.3}
              name="David Park"
              role="CTO, CloudSync"
              text="The technical quality is outstanding. Clean code, great architecture, and they actually care about performance. Our platform handles 10x the traffic with the new build."
            />
            <TestimonialCard
              delay={0.4}
              name="Priya Sharma"
              role="Owner, GreenLeaf Organics"
              text="As a small business owner, I needed a partner who could do it all — website, SEO, and ads. Zorva delivered on every front. Our organic traffic is up 400% in 6 months."
            />
            <TestimonialCard
              delay={0.5}
              name="James O'Brien"
              role="Product Manager, UrbanEats"
              text="From design to deployment, every step was smooth. Zorva Labs built our food delivery app in record time without cutting corners. Our users constantly praise the UX."
            />
          </div>
        </div>
      </section>

      {/* ═══ CTA / CONTACT ═══ */}
      <section id="contact" className="relative px-5 py-24">
        {/* Background glow */}
        <div className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute left-1/2 top-1/2 h-[500px] w-[500px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-purple-500/[0.08] blur-[120px]" />
        </div>

        <div className="relative mx-auto max-w-2xl text-center">
          <span className="mb-3 inline-block text-xs font-bold uppercase tracking-[0.2em] text-purple-400">Let&apos;s Talk</span>
          <h2 className="mb-6 text-3xl font-black text-white sm:text-4xl md:text-5xl">
            Ready to <span className="gradient-text">ignite your idea?</span>
          </h2>
          <p className="mb-10 text-lg text-slate-400">
            Tell us about your project and we&apos;ll get back to you within 24 hours with a free consultation and estimate.
          </p>

          {/* Contact form */}
          <form
            onSubmit={(e) => {
              e.preventDefault()
              const form = e.currentTarget
              const name = (form.elements.namedItem("name") as HTMLInputElement).value
              const email = (form.elements.namedItem("email") as HTMLInputElement).value
              const service = (form.elements.namedItem("service") as HTMLSelectElement).value
              const message = (form.elements.namedItem("message") as HTMLTextAreaElement).value
              const subject = encodeURIComponent(`New Inquiry: ${service || "General"} — from ${name}`)
              const body = encodeURIComponent(`Name: ${name}\nEmail: ${email}\nService: ${service || "Not specified"}\n\nMessage:\n${message}`)
              window.location.href = `mailto:zorvalabs@outlook.com?subject=${subject}&body=${body}`
            }}
            className="mx-auto max-w-md space-y-4"
          >
            <input
              name="name"
              type="text"
              required
              placeholder="Your name"
              className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-5 py-3.5 text-sm text-white placeholder:text-slate-500 transition focus:border-purple-500/40 focus:outline-none focus:ring-1 focus:ring-purple-500/40"
            />
            <input
              name="email"
              type="email"
              required
              placeholder="Your email"
              className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-5 py-3.5 text-sm text-white placeholder:text-slate-500 transition focus:border-purple-500/40 focus:outline-none focus:ring-1 focus:ring-purple-500/40"
            />
            <select name="service" className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-5 py-3.5 text-sm text-slate-500 transition focus:border-purple-500/40 focus:outline-none focus:ring-1 focus:ring-purple-500/40">
              <option value="">What do you need?</option>
              <option value="App Development">App Development</option>
              <option value="Website Services">Website Services</option>
              <option value="SEO Optimization">SEO Optimization</option>
              <option value="Digital Marketing">Digital Marketing</option>
              <option value="Full Package">Full Package</option>
            </select>
            <textarea
              name="message"
              rows={4}
              required
              placeholder="Tell us about your project..."
              className="w-full rounded-xl border border-white/[0.08] bg-white/[0.03] px-5 py-3.5 text-sm text-white placeholder:text-slate-500 transition focus:border-purple-500/40 focus:outline-none focus:ring-1 focus:ring-purple-500/40 resize-none"
            />
            <button type="submit" className="animate-gradient w-full rounded-xl bg-gradient-to-r from-purple-500 via-pink-500 to-amber-500 py-4 text-base font-bold text-white transition hover:shadow-2xl hover:shadow-purple-500/30">
              Send Message
            </button>
          </form>

          <p className="mt-6 text-xs text-slate-600">
            Or email us directly at{" "}
            <a href="mailto:zorvalabs@outlook.com" className="text-purple-400 transition hover:text-purple-300">
              zorvalabs@outlook.com
            </a>
          </p>
        </div>
      </section>

      {/* ═══ FOOTER ═══ */}
      <footer className="border-t border-white/[0.06] px-5 py-12">
        <div className="mx-auto max-w-6xl">
          <div className="grid gap-10 sm:grid-cols-2 lg:grid-cols-4">
            {/* Brand */}
            <div className="sm:col-span-2 lg:col-span-1">
              <div className="mb-4 flex items-center gap-2.5">
                <ZorvaLogo size={32} />
                <span className="text-base font-black tracking-wide text-white">
                  ZORVA <span className="gradient-text">LABS</span>
                </span>
              </div>
              <p className="mb-4 text-sm text-slate-500 leading-relaxed">
                Where Ideas Ignite. Building digital experiences that move businesses forward.
              </p>
            </div>

            {/* Services */}
            <div>
              <h4 className="mb-4 text-sm font-bold text-white">Services</h4>
              <ul className="space-y-2.5">
                {["App Development", "Website Services", "SEO Optimization", "Digital Marketing"].map((s) => (
                  <li key={s}>
                    <button onClick={() => scrollTo("services")} className="text-sm text-slate-500 transition hover:text-white">{s}</button>
                  </li>
                ))}
              </ul>
            </div>

            {/* Company */}
            <div>
              <h4 className="mb-4 text-sm font-bold text-white">Company</h4>
              <ul className="space-y-2.5">
                {[
                  { label: "Our Work", id: "work" },
                  { label: "Process", id: "process" },
                  { label: "Testimonials", id: "testimonials" },
                  { label: "Contact", id: "contact" },
                ].map((item) => (
                  <li key={item.label}>
                    <button onClick={() => scrollTo(item.id)} className="text-sm text-slate-500 transition hover:text-white">{item.label}</button>
                  </li>
                ))}
              </ul>
            </div>

            {/* Connect */}
            <div>
              <h4 className="mb-4 text-sm font-bold text-white">Connect</h4>
              <ul className="space-y-2.5">
                <li>
                  <a href="mailto:zorvalabs@outlook.com" className="text-sm text-slate-500 transition hover:text-white">zorvalabs@outlook.com</a>
                </li>
                <li>
                  <a href="https://twitter.com/zorvalabs" target="_blank" rel="noopener noreferrer" className="text-sm text-slate-500 transition hover:text-white">Twitter / X</a>
                </li>
                <li>
                  <a href="https://linkedin.com/company/zorvalabs" target="_blank" rel="noopener noreferrer" className="text-sm text-slate-500 transition hover:text-white">LinkedIn</a>
                </li>
                <li>
                  <a href="https://github.com/zorvalabs" target="_blank" rel="noopener noreferrer" className="text-sm text-slate-500 transition hover:text-white">GitHub</a>
                </li>
              </ul>
            </div>
          </div>

          <div className="mt-12 flex flex-col items-center gap-4 border-t border-white/[0.06] pt-8 sm:flex-row sm:justify-between">
            <p className="text-xs text-slate-600">&copy; {new Date().getFullYear()} Zorva Labs. All rights reserved.</p>
            <p className="text-xs text-slate-700">Where Ideas Ignite.</p>
          </div>
        </div>
      </footer>
    </div>
  )
}
