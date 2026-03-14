import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const maxDuration = 300
export const dynamic = "force-dynamic"

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

function toIsoDaysAgo(days: number) {
  const d = new Date()
  d.setUTCDate(d.getUTCDate() - days)
  return d.toISOString()
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

    const sinceIso = toIsoDaysAgo(lookbackDays)

    /**
     * IMPORTANT:
     * Replace the selected column names below if your schema uses different names.
     *
     * Assumed columns:
     * - companies: id, ticker, cik, is_active
     * - raw_filings: company_id, ticker, filed_at, form_type
     * - raw_ptr_trades: company_id, ticker, filed_at
     * - signals: company_id, ticker, created_at, signal_type, cluster_id
     * - candidate_universe: company_id, ticker, cik, is_active,
     *   has_insider_trades, has_ptr_forms, has_clusters, is_eligible,
     *   eligibility_reason, updated_at
     */

    const companiesQuery = (supabase.from("companies") as any)
      .select("id,ticker,cik,is_active")

    if (onlyActive) {
      companiesQuery.eq("is_active", true)
    }

    const [{ data: companies, error: companiesError }, { data: filings, error: filingsError }, { data: ptrs, error: ptrsError }, { data: signals, error: signalsError }] =
      await Promise.all([
        companiesQuery,
        (supabase.from("raw_filings") as any)
          .select("company_id,ticker,filed_at,form_type")
          .gte("filed_at", sinceIso),
        (supabase.from("raw_ptr_trades") as any)
          .select("company_id,ticker,filed_at")
          .gte("filed_at", sinceIso),
        (supabase.from("signals") as any)
          .select("company_id,ticker,created_at,signal_type,cluster_id")
          .gte("created_at", sinceIso),
      ])

    if (companiesError) {
      throw new Error(`Failed to load companies: ${companiesError.message}`)
    }

    if (filingsError) {
      throw new Error(`Failed to load filings: ${filingsError.message}`)
    }

    if (ptrsError) {
      throw new Error(`Failed to load ptr trades: ${ptrsError.message}`)
    }

    if (signalsError) {
      throw new Error(`Failed to load signals: ${signalsError.message}`)
    }

    const companyByTicker = new Map<string, any>()
    const companyById = new Map<string | number, any>()

    for (const company of companies || []) {
      if (company?.ticker) {
        companyByTicker.set(String(company.ticker).toUpperCase(), company)
      }
      if (company?.id !== null && company?.id !== undefined) {
        companyById.set(company.id, company)
      }
    }

    const insiderTradeTickers = new Set<string>()
    const ptrTickers = new Set<string>()
    const clusterTickers = new Set<string>()

    for (const filing of filings || []) {
      const formType = String(filing?.form_type || "").toUpperCase()
      const isInsiderTradeForm =
        formType === "3" ||
        formType === "4" ||
        formType === "5" ||
        formType === "FORM 3" ||
        formType === "FORM 4" ||
        formType === "FORM 5"

      if (!isInsiderTradeForm) continue

      const ticker =
        filing?.ticker
          ? String(filing.ticker).toUpperCase()
          : companyById.get(filing?.company_id)?.ticker
          ? String(companyById.get(filing.company_id).ticker).toUpperCase()
          : null

      if (ticker) insiderTradeTickers.add(ticker)
    }

    for (const ptr of ptrs || []) {
      const ticker =
        ptr?.ticker
          ? String(ptr.ticker).toUpperCase()
          : companyById.get(ptr?.company_id)?.ticker
          ? String(companyById.get(ptr.company_id).ticker).toUpperCase()
          : null

      if (ticker) ptrTickers.add(ticker)
    }

    for (const signal of signals || []) {
      const hasCluster =
        signal?.cluster_id !== null && signal?.cluster_id !== undefined
      const signalType = String(signal?.signal_type || "").toLowerCase()
      const looksLikeCluster =
        hasCluster ||
        signalType.includes("cluster") ||
        signalType.includes("filing_cluster")

      if (!looksLikeCluster) continue

      const ticker =
        signal?.ticker
          ? String(signal.ticker).toUpperCase()
          : companyById.get(signal?.company_id)?.ticker
          ? String(companyById.get(signal.company_id).ticker).toUpperCase()
          : null

      if (ticker) clusterTickers.add(ticker)
    }

    const eligibleRows: Array<Record<string, any>> = []
    const updatedAt = new Date().toISOString()

    for (const company of companies || []) {
      const ticker = company?.ticker ? String(company.ticker).toUpperCase() : null
      if (!ticker) continue

      const hasInsiderTrades = insiderTradeTickers.has(ticker)
      const hasPtrForms = ptrTickers.has(ticker)
      const hasClusters = clusterTickers.has(ticker)

      const isEligible = hasInsiderTrades || hasPtrForms || hasClusters
      if (!isEligible) continue

      const reasons: string[] = []
      if (hasInsiderTrades) reasons.push("insider_trades")
      if (hasPtrForms) reasons.push("ptr_forms")
      if (hasClusters) reasons.push("clusters")

      eligibleRows.push({
        company_id: company.id,
        ticker,
        cik: company.cik ?? null,
        is_active: company.is_active ?? true,
        has_insider_trades: hasInsiderTrades,
        has_ptr_forms: hasPtrForms,
        has_clusters: hasClusters,
        is_eligible: true,
        eligibility_reason: reasons.join(","),
        updated_at: updatedAt,
      })
    }

    /**
     * If your candidate_universe table has different columns,
     * replace the object shape above and the onConflict key below.
     */
    if (eligibleRows.length > 0) {
      const { error: upsertError } = await (supabase.from(
        "candidate_universe"
      ) as any).upsert(eligibleRows, {
        onConflict: "ticker",
      })

      if (upsertError) {
        throw new Error(`Failed to upsert candidate universe: ${upsertError.message}`)
      }
    }

    return NextResponse.json({
      ok: true,
      lookbackDays,
      eligibleCount: eligibleRows.length,
      counts: includeCounts
        ? {
            companies: (companies || []).length,
            insiderTradeTickers: insiderTradeTickers.size,
            ptrTickers: ptrTickers.size,
            clusterTickers: clusterTickers.size,
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