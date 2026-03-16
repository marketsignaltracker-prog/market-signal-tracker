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
  accession_no?: string | null
}

type PtrRow = {
  ticker?: string | null
  trade_date?: string | null
  disclosure_date?: string | null
  transaction_date?: string | null
  report_date?: string | null
  transaction_type?: string | null
  action?: string | null
  amount_low?: number | null
  amount_high?: number | null
  filer_name?: string | null
  politician_name?: string | null
}

type SignalRow = {
  company_id?: number | null
  ticker?: string | null
  created_at?: string | null
  as_of_date?: string | null
  signal_type?: string | null
  source_type?: string | null
  signal_source?: string | null
  signal_category?: string | null
  strength?: number | null
}

type EligibleUniverseRow = {
  company_id: number
  ticker: string
  cik: string | null
  name: string | null
  is_active: boolean
  is_eligible: boolean
  included: boolean
  passed: boolean
  has_insider_trades: boolean
  has_ptr_forms: boolean
  has_clusters: boolean
  eligibility_reason: string
  as_of_date: string
  last_screened_at: string
  updated_at: string
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

function uniqueStrings(values: Array<string | null | undefined>) {
  return Array.from(
    new Set(values.map((value) => (value || "").trim()).filter(Boolean))
  )
}

function isPositivePtrTrade(row: PtrRow) {
  const transactionType = String(row.transaction_type || "")
    .trim()
    .toLowerCase()
  const action = String(row.action || "")
    .trim()
    .toLowerCase()

  return (
    transactionType === "buy" ||
    transactionType === "exchange" ||
    action.includes("buy") ||
    action.includes("purchase") ||
    action.includes("exchange")
  )
}

function upsertReason(
  reasons: string[],
  reason: string,
  condition: boolean
) {
  if (condition) reasons.push(reason)
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
      companyById.set(company.id, company)
    }

    const { data: filings, error: filingsError } = await (supabase.from(
      "raw_filings"
    ) as any)
      .select("company_id,ticker,filed_at,form_type,accession_no")
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

    const { data: ptrs, error: ptrsError } = await (supabase.from(
      "raw_ptr_trades"
    ) as any)
      .select(
        "ticker,trade_date,disclosure_date,transaction_date,report_date,transaction_type,action,amount_low,amount_high,filer_name,politician_name"
      )
      .or(
        `trade_date.gte.${sinceDate},disclosure_date.gte.${sinceDate},transaction_date.gte.${sinceDate},report_date.gte.${sinceDate}`
      )

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

    const { data: signals, error: signalsError } = await (supabase.from(
      "signals"
    ) as any)
      .select(
        "company_id,ticker,created_at,as_of_date,signal_type,source_type,signal_source,signal_category,strength"
      )
      .or(`created_at.gte.${sinceDate},as_of_date.gte.${updatedAt.slice(0, 10)}`)

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
    const highPriorityFilingTickers = new Set<string>()
    const filingClusterTickers = new Set<string>()
    const ptrTickers = new Set<string>()
    const ptrPositiveTickers = new Set<string>()
    const ptrClusterTickers = new Set<string>()
    const signalTickers = new Set<string>()
    const highStrengthSignalTickers = new Set<string>()

    const filingCountByTicker = new Map<string, number>()
    const form4CountByTicker = new Map<string, number>()

    for (const filing of (filings || []) as FilingRow[]) {
      const formType = String(filing?.form_type || "").toUpperCase().trim()

      const ticker =
        filing?.ticker
          ? normalizeTicker(filing.ticker)
          : filing?.company_id && companyById.get(Number(filing.company_id))?.ticker
            ? normalizeTicker(
                companyById.get(Number(filing.company_id))?.ticker || ""
              )
            : null

      if (!ticker) continue

      filingCountByTicker.set(ticker, (filingCountByTicker.get(ticker) || 0) + 1)

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

      if (isInsiderTradeForm) {
        insiderTradeTickers.add(ticker)
        form4CountByTicker.set(ticker, (form4CountByTicker.get(ticker) || 0) + 1)
      }

      if (isHighPriorityFiling) {
        highPriorityFilingTickers.add(ticker)
      }
    }

    for (const [ticker, count] of form4CountByTicker.entries()) {
      if (count >= 2) {
        filingClusterTickers.add(ticker)
      }
    }

    for (const [ticker, count] of filingCountByTicker.entries()) {
      if (count >= 4) {
        filingClusterTickers.add(ticker)
      }
    }

    const ptrBuyersByTicker = new Map<string, Set<string>>()
    const ptrBuyCountByTicker = new Map<string, number>()

    for (const ptr of (ptrs || []) as PtrRow[]) {
      const ticker = ptr?.ticker ? normalizeTicker(ptr.ticker) : null
      if (!ticker) continue

      ptrTickers.add(ticker)

      if (isPositivePtrTrade(ptr)) {
        ptrPositiveTickers.add(ticker)
        ptrBuyCountByTicker.set(ticker, (ptrBuyCountByTicker.get(ticker) || 0) + 1)

        const buyer =
          String(ptr.politician_name || ptr.filer_name || "").trim()

        if (buyer) {
          if (!ptrBuyersByTicker.has(ticker)) {
            ptrBuyersByTicker.set(ticker, new Set<string>())
          }
          ptrBuyersByTicker.get(ticker)!.add(buyer)
        }
      }
    }

    for (const [ticker, count] of ptrBuyCountByTicker.entries()) {
      const uniqueBuyers = ptrBuyersByTicker.get(ticker)?.size || 0
      if (count >= 2 || uniqueBuyers >= 2) {
        ptrClusterTickers.add(ticker)
      }
    }

    for (const signal of (signals || []) as SignalRow[]) {
      const ticker =
        signal?.ticker
          ? normalizeTicker(signal.ticker)
          : signal?.company_id && companyById.get(Number(signal.company_id))?.ticker
            ? normalizeTicker(
                companyById.get(Number(signal.company_id))?.ticker || ""
              )
            : null

      if (!ticker) continue
      signalTickers.add(ticker)

      const strength = Number(signal.strength || 0)
      if (Number.isFinite(strength) && strength >= 55) {
        highStrengthSignalTickers.add(ticker)
      }
    }

    const eligibleRows: EligibleUniverseRow[] = []

    for (const company of typedCompanies) {
      const ticker = normalizeTicker(company?.ticker)
      if (!ticker) continue

      const hasInsiderTrades = insiderTradeTickers.has(ticker)
      const hasHighPriorityFilings = highPriorityFilingTickers.has(ticker)
      const hasPtrForms = ptrTickers.has(ticker)
      const hasPositivePtrs = ptrPositiveTickers.has(ticker)
      const hasSignals = signalTickers.has(ticker)
      const hasHighStrengthSignals = highStrengthSignalTickers.has(ticker)
      const hasClusters =
        filingClusterTickers.has(ticker) || ptrClusterTickers.has(ticker)

      const isEligible =
        hasPtrForms ||
        hasInsiderTrades ||
        hasHighPriorityFilings ||
        hasSignals

      if (!isEligible) continue

      const reasons: string[] = []
      upsertReason(reasons, "ptr_forms", hasPtrForms)
      upsertReason(reasons, "ptr_positive", hasPositivePtrs)
      upsertReason(reasons, "insider_trades", hasInsiderTrades)
      upsertReason(reasons, "high_priority_filings", hasHighPriorityFilings)
      upsertReason(reasons, "clusters", hasClusters)
      upsertReason(reasons, "signals", hasSignals)
      upsertReason(reasons, "high_strength_signals", hasHighStrengthSignals)

      eligibleRows.push({
        company_id: company.id,
        ticker,
        cik: company.cik ?? null,
        name: company.name ?? null,
        is_active: company.is_active ?? true,
        is_eligible: true,
        included: true,
        passed: true,
        has_insider_trades: hasInsiderTrades || hasHighPriorityFilings,
        has_ptr_forms: hasPtrForms,
        has_clusters: hasClusters,
        eligibility_reason: uniqueStrings(reasons).join(","),
        as_of_date: updatedAt,
        last_screened_at: updatedAt,
        updated_at: updatedAt,
      })
    }

    if (eligibleRows.length > 0) {
      const { error: upsertError } = await (supabase.from(
        "candidate_universe"
      ) as any).upsert(eligibleRows, { onConflict: "ticker" })

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
      stage: "eligible_universe",
      targetTable: "candidate_universe",
      lookbackDays,
      eligibleCount: eligibleRows.length,
      counts: includeCounts
        ? {
            companies: typedCompanies.length,
            insiderTradeTickers: insiderTradeTickers.size,
            highPriorityFilingTickers: highPriorityFilingTickers.size,
            filingClusterTickers: filingClusterTickers.size,
            ptrTickers: ptrTickers.size,
            ptrPositiveTickers: ptrPositiveTickers.size,
            ptrClusterTickers: ptrClusterTickers.size,
            signalTickers: signalTickers.size,
            highStrengthSignalTickers: highStrengthSignalTickers.size,
            eligibleRows: eligibleRows.length,
          }
        : undefined,
      message:
        "Eligible universe rebuilt from recent insider filings, PTR activity, cluster-style evidence, and current signals.",
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