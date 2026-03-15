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

    /**
     * Priority stack:
     * 1. PTR
     * 2. Insider / ownership / catalyst filings
     * 3. Signals
     */
    const insiderTradeTickers = new Set<string>()
    const highPriorityFilingTickers = new Set<string>()
    const ptrTickers = new Set<string>()
    const signalTickers = new Set<string>()

    for (const filing of (filings || []) as FilingRow[]) {
      const formType = String(filing?.form_type || "").toUpperCase().trim()

      const ticker =
        filing?.ticker
          ? normalizeTicker(filing.ticker)
          : filing?.company_id && companyById.get(Number(filing.company_id))?.ticker
            ? normalizeTicker(companyById.get(Number(filing.company_id))?.ticker || "")
            : null

      if (!ticker) continue

      const isInsiderTradeForm =
        formType === "3" ||
        formType === "4" ||
        formType === "5" ||
        formType === "3/A" ||
        formType === "4/A" ||
        formType === "5/A" ||
        formType === "FORM 3" ||
        formType === "FORM 4" ||
        formType === "FORM 5"

      const isHighPriorityFiling =
        formType === "13D" ||
        formType === "13D/A" ||
        formType === "13G" ||
        formType === "13G/A" ||
        formType === "SC 13D" ||
        formType === "SC 13D/A" ||
        formType === "SC 13G" ||
        formType === "SC 13G/A" ||
        formType === "8-K" ||
        formType === "6-K" ||
        formType === "10-Q" ||
        formType === "10-K"

      if (isInsiderTradeForm) insiderTradeTickers.add(ticker)
      if (isHighPriorityFiling) highPriorityFilingTickers.add(ticker)
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

    const eligibleRows: Array<Record<string, any>> = []

    for (const company of typedCompanies) {
      const ticker = normalizeTicker(company?.ticker)
      if (!ticker) continue

      const hasInsiderTrades = insiderTradeTickers.has(ticker)
      const hasHighPriorityFilings = highPriorityFilingTickers.has(ticker)
      const hasPtrForms = ptrTickers.has(ticker)
      const hasSignals = signalTickers.has(ticker)

      const isEligible =
        hasPtrForms ||
        hasInsiderTrades ||
        hasHighPriorityFilings ||
        hasSignals

      if (!isEligible) continue

      const reasons: string[] = []
      if (hasPtrForms) reasons.push("ptr_forms")
      if (hasInsiderTrades) reasons.push("insider_trades")
      if (hasHighPriorityFilings) reasons.push("high_priority_filings")
      if (hasSignals) reasons.push("signals")

      eligibleRows.push({
        company_id: company.id,
        ticker,
        cik: company.cik ?? null,
        name: company.name ?? null,
        is_active: company.is_active ?? true,
        has_insider_trades: hasInsiderTrades,
        has_ptr_forms: hasPtrForms,
        has_clusters: hasSignals,
        is_eligible: true,
        eligibility_reason: reasons.join(","),
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
            highPriorityFilingTickers: highPriorityFilingTickers.size,
            ptrTickers: ptrTickers.size,
            signalTickers: signalTickers.size,
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