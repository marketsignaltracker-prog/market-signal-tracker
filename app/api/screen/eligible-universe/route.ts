import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const maxDuration = 300
export const dynamic = "force-dynamic"

type CompanyRow = {
  id: number
  ticker: string
  cik: string | null
  name: string | null
  is_active: boolean | null
}

type FilingRow = {
  company_id?: number | null
  ticker?: string | null
  filed_at?: string | null
  form_type?: string | null
}

type PtrRow = {
  ticker?: string | null
  transaction_date?: string | null
  report_date?: string | null
}

type SignalRow = {
  company_id?: number | null
  ticker?: string | null
  created_at?: string | null
  signal_type?: string | null
  signal_source?: string | null
  signal_category?: string | null
}

type EligibilityFlags = {
  hasInsiderTrades: boolean
  hasOwnershipFilings: boolean
  hasCatalystFilings: boolean
  hasPtrForms: boolean
  hasSignals: boolean
  isEligible: boolean
  reasons: string[]
}

function getSupabaseAdmin(): any {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error("Missing Supabase environment variables")
  }

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  })
}

function parseInteger(value: string | null, fallback: number) {
  if (!value || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function getPipelineToken(request: NextRequest) {
  return request.headers.get("x-pipeline-token")
}

function requirePipelineToken(request: NextRequest) {
  const expected = process.env.PIPELINE_TOKEN

  if (!expected) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const provided = getPipelineToken(request)

  if (provided !== expected) {
    return NextResponse.json(
      {
        ok: false,
        error: "Unauthorized",
      },
      { status: 401 }
    )
  }

  return null
}

function toIsoDateStringDaysAgo(days: number) {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - days)
  return d.toISOString().slice(0, 10)
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
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

function uniqueStrings(values: Array<string | null | undefined>) {
  return Array.from(new Set(values.map((value) => (value || "").trim()).filter(Boolean)))
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function isInsiderTradeForm(formType: string) {
  return (
    formType === "3" ||
    formType === "4" ||
    formType === "5" ||
    formType === "3/A" ||
    formType === "4/A" ||
    formType === "5/A"
  )
}

function isOwnershipForm(formType: string) {
  return (
    formType === "13D" ||
    formType === "13D/A" ||
    formType === "13G" ||
    formType === "13G/A" ||
    formType === "SC 13D" ||
    formType === "SC 13D/A" ||
    formType === "SC 13G" ||
    formType === "SC 13G/A"
  )
}

function isCatalystForm(formType: string) {
  return (
    formType === "8-K" ||
    formType === "6-K" ||
    formType === "10-Q" ||
    formType === "10-K"
  )
}

function buildEligibilityFlags(params: {
  hasInsiderTrades: boolean
  hasOwnershipFilings: boolean
  hasCatalystFilings: boolean
  hasPtrForms: boolean
  hasSignals: boolean
}): EligibilityFlags {
  const {
    hasInsiderTrades,
    hasOwnershipFilings,
    hasCatalystFilings,
    hasPtrForms,
    hasSignals,
  } = params

  const reasons: string[] = []

  if (hasPtrForms) reasons.push("ptr_forms")
  if (hasInsiderTrades) reasons.push("insider_trades")
  if (hasOwnershipFilings) reasons.push("ownership_filings")
  if (hasCatalystFilings) reasons.push("catalyst_filings")
  if (hasSignals) reasons.push("signals")

  const priorityEvidenceCount =
    Number(hasPtrForms) +
    Number(hasInsiderTrades) +
    Number(hasOwnershipFilings) +
    Number(hasCatalystFilings)

  const isEligible =
    hasPtrForms ||
    hasInsiderTrades ||
    hasOwnershipFilings ||
    (hasCatalystFilings && hasSignals) ||
    (priorityEvidenceCount >= 2) ||
    (hasSignals && (hasPtrForms || hasInsiderTrades || hasOwnershipFilings))

  return {
    hasInsiderTrades,
    hasOwnershipFilings,
    hasCatalystFilings,
    hasPtrForms,
    hasSignals,
    isEligible,
    reasons,
  }
}

async function resetEligibilityForTickers(
  supabase: any,
  tickers: string[],
  updatedAt: string
) {
  const table = supabase.from("candidate_universe") as any
  const chunks = chunkArray(tickers, 250)

  for (const chunk of chunks) {
    const { error } = await table
      .update({
        is_eligible: false,
        has_insider_trades: false,
        has_ptr_forms: false,
        has_clusters: false,
        eligibility_reason: null,
        updated_at: updatedAt,
      })
      .in("ticker", chunk)

    if (error) {
      throw new Error(`Failed resetting eligibility rows: ${error.message}`)
    }
  }
}

export async function GET(request: NextRequest) {
  const authError = requirePipelineToken(request)
  if (authError) return authError

  try {
    const supabase = getSupabaseAdmin()

    const lookbackDays = parseInteger(
      request.nextUrl.searchParams.get("lookbackDays"),
      30
    )

    const onlyActive =
      request.nextUrl.searchParams.get("onlyActive") !== "false"
    const includeCounts =
      request.nextUrl.searchParams.get("includeCounts") === "true"

    const sinceDate = toIsoDateStringDaysAgo(lookbackDays)
    const updatedAt = new Date().toISOString()

    let companiesQuery = (supabase.from("companies") as any)
      .select("id,ticker,cik,name,is_active")

    if (onlyActive) {
      companiesQuery = companiesQuery.eq("is_active", true)
    }

    const { data: companies, error: companiesError } = await companiesQuery

    if (companiesError) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load companies: ${companiesError.message}`,
          debug: {
            step: "companies",
            sinceDate,
            onlyActive,
          },
        },
        { status: 500 }
      )
    }

    const typedCompanies = (companies || []) as CompanyRow[]

    const companyById = new Map<number, CompanyRow>()
    for (const company of typedCompanies) {
      if (company?.id !== null && company?.id !== undefined) {
        companyById.set(company.id, company)
      }
    }

    const { data: filings, error: filingsError } = await (supabase.from("raw_filings") as any)
      .select("company_id,ticker,filed_at,form_type")
      .gte("filed_at", sinceDate)

    if (filingsError) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load filings: ${filingsError.message}`,
          debug: {
            step: "raw_filings",
            sinceDate,
          },
        },
        { status: 500 }
      )
    }

    const { data: ptrs, error: ptrsError } = await (supabase.from("raw_ptr_trades") as any)
      .select("ticker,transaction_date,report_date")
      .or(`transaction_date.gte.${sinceDate},report_date.gte.${sinceDate}`)

    if (ptrsError) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load ptr trades: ${ptrsError.message}`,
          debug: {
            step: "raw_ptr_trades",
            sinceDate,
          },
        },
        { status: 500 }
      )
    }

    const { data: signals, error: signalsError } = await (supabase.from("signals") as any)
      .select("company_id,ticker,created_at,signal_type,signal_source,signal_category")
      .gte("created_at", sinceDate)

    if (signalsError) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load signals: ${signalsError.message}`,
          debug: {
            step: "signals",
            sinceDate,
          },
        },
        { status: 500 }
      )
    }

    const insiderTradeTickers = new Set<string>()
    const ownershipFilingTickers = new Set<string>()
    const catalystFilingTickers = new Set<string>()
    const ptrTickers = new Set<string>()
    const signalTickers = new Set<string>()

    for (const filing of (filings || []) as FilingRow[]) {
      const formType = normalizeFormType(filing?.form_type)

      const ticker =
        filing?.ticker
          ? normalizeTicker(filing.ticker)
          : filing?.company_id && companyById.get(Number(filing.company_id))?.ticker
            ? normalizeTicker(companyById.get(Number(filing.company_id))?.ticker || "")
            : null

      if (!ticker) continue
      if (!formType) continue

      if (isInsiderTradeForm(formType)) insiderTradeTickers.add(ticker)
      if (isOwnershipForm(formType)) ownershipFilingTickers.add(ticker)
      if (isCatalystForm(formType)) catalystFilingTickers.add(ticker)
    }

    for (const ptr of (ptrs || []) as PtrRow[]) {
      const ticker = ptr?.ticker ? normalizeTicker(ptr.ticker) : null
      if (ticker) ptrTickers.add(ticker)
    }

    for (const signal of (signals || []) as SignalRow[]) {
      const ticker =
        signal?.ticker
          ? normalizeTicker(signal.ticker)
          : signal?.company_id && companyById.get(Number(signal.company_id))?.ticker
            ? normalizeTicker(companyById.get(Number(signal.company_id))?.ticker || "")
            : null

      if (!ticker) continue
      signalTickers.add(ticker)
    }

    const allCompanyTickers = uniqueStrings(typedCompanies.map((company) => normalizeTicker(company.ticker)))

    if (allCompanyTickers.length > 0) {
      await resetEligibilityForTickers(supabase, allCompanyTickers, updatedAt)
    }

    const eligibleRows: Array<Record<string, any>> = []

    let eligibleBecausePtr = 0
    let eligibleBecauseInsider = 0
    let eligibleBecauseOwnership = 0
    let eligibleBecauseCatalystAndSignal = 0
    let eligibleBecauseMultiplePriorityBuckets = 0

    for (const company of typedCompanies) {
      const ticker = normalizeTicker(company?.ticker)
      if (!ticker) continue

      const flags = buildEligibilityFlags({
        hasInsiderTrades: insiderTradeTickers.has(ticker),
        hasOwnershipFilings: ownershipFilingTickers.has(ticker),
        hasCatalystFilings: catalystFilingTickers.has(ticker),
        hasPtrForms: ptrTickers.has(ticker),
        hasSignals: signalTickers.has(ticker),
      })

      if (!flags.isEligible) continue

      if (flags.hasPtrForms) eligibleBecausePtr += 1
      if (flags.hasInsiderTrades) eligibleBecauseInsider += 1
      if (flags.hasOwnershipFilings) eligibleBecauseOwnership += 1
      if (flags.hasCatalystFilings && flags.hasSignals) eligibleBecauseCatalystAndSignal += 1

      const priorityBucketCount =
        Number(flags.hasPtrForms) +
        Number(flags.hasInsiderTrades) +
        Number(flags.hasOwnershipFilings) +
        Number(flags.hasCatalystFilings)

      if (priorityBucketCount >= 2) {
        eligibleBecauseMultiplePriorityBuckets += 1
      }

      eligibleRows.push({
        company_id: company.id,
        ticker,
        cik: company.cik ?? null,
        name: company.name ?? null,
        is_active: company.is_active ?? true,
        has_insider_trades: flags.hasInsiderTrades,
        has_ptr_forms: flags.hasPtrForms,
        has_clusters: flags.hasSignals,
        is_eligible: true,
        eligibility_reason: flags.reasons.join(","),
        updated_at: updatedAt,
      })
    }

    if (eligibleRows.length > 0) {
      const { error: upsertError } = await (supabase.from("candidate_universe") as any).upsert(
        eligibleRows,
        { onConflict: "ticker" }
      )

      if (upsertError) {
        return NextResponse.json(
          {
            ok: false,
            error: `Failed to upsert candidate universe: ${upsertError.message}`,
            debug: {
              step: "candidate_universe_upsert",
              sampleRow: eligibleRows[0] ?? null,
              eligibleCount: eligibleRows.length,
            },
          },
          { status: 500 }
        )
      }
    }

    return NextResponse.json({
      ok: true,
      lookbackDays,
      eligibleCount: eligibleRows.length,
      counts: includeCounts
        ? {
            companies: typedCompanies.length,
            insiderTradeTickers: insiderTradeTickers.size,
            ownershipFilingTickers: ownershipFilingTickers.size,
            catalystFilingTickers: catalystFilingTickers.size,
            ptrTickers: ptrTickers.size,
            signalTickers: signalTickers.size,
            eligibleBecausePtr,
            eligibleBecauseInsider,
            eligibleBecauseOwnership,
            eligibleBecauseCatalystAndSignal,
            eligibleBecauseMultiplePriorityBuckets,
            eligibleRows: eligibleRows.length,
          }
        : undefined,
    })
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Unknown error",
      },
      { status: 500 }
    )
  }
}