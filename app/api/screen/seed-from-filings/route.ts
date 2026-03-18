import { createClient } from "@supabase/supabase-js"
import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"
export const maxDuration = 30

const FILINGS_LOOKBACK_DAYS = 60

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return NextResponse.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  const supabase = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  })

  const lookbackDate = new Date()
  lookbackDate.setDate(lookbackDate.getDate() - FILINGS_LOOKBACK_DAYS)
  const lookbackIso = lookbackDate.toISOString()
  const lookbackDateStr = lookbackDate.toISOString().split("T")[0]

  // --- 1. Collect tickers with recent Form 3/4/5 filings ---
  const { data: filingRows, error: filingError } = await supabase
    .from("raw_filings")
    .select("ticker")
    .in("form_type", ["3", "3/A", "4", "4/A", "5", "5/A"])
    .gte("filed_at", lookbackIso)
    .not("ticker", "is", null)

  if (filingError) {
    return NextResponse.json(
      { ok: false, error: `raw_filings query failed: ${filingError.message}` },
      { status: 500 }
    )
  }

  // --- 2. Collect tickers with recent PTR trades ---
  const { data: ptrRows, error: ptrError } = await supabase
    .from("raw_ptr_trades")
    .select("ticker")
    .gte("trade_date", lookbackDateStr)

  if (ptrError) {
    return NextResponse.json(
      { ok: false, error: `raw_ptr_trades query failed: ${ptrError.message}` },
      { status: 500 }
    )
  }

  const ptrTickerSet = new Set((ptrRows || []).map((r: any) => r.ticker as string))

  // Deduplicate all tickers (filings + PTRs)
  const allTickerSet = new Set<string>()
  for (const row of filingRows || []) {
    if (row.ticker) allTickerSet.add(row.ticker)
  }
  for (const ticker of ptrTickerSet) {
    if (ticker) allTickerSet.add(ticker)
  }

  const allTickers = [...allTickerSet]

  if (allTickers.length === 0) {
    return NextResponse.json({
      ok: true,
      seeded: 0,
      message: "No recent insider filings or PTR trades found — candidate_universe unchanged.",
    })
  }

  // --- 3. Look up company metadata for those tickers ---
  const CHUNK_SIZE = 400
  const companyRows: any[] = []
  for (let i = 0; i < allTickers.length; i += CHUNK_SIZE) {
    const chunk = allTickers.slice(i, i + CHUNK_SIZE)
    const { data: rows } = await supabase
      .from("companies")
      .select("id, ticker, cik, name, is_active")
      .in("ticker", chunk)
      .eq("is_active", true)
    companyRows.push(...(rows || []))
  }

  const nowIso = new Date().toISOString()

  const universeRows = companyRows.map((company: any) => {
    const hasPtrs = ptrTickerSet.has(company.ticker)
    return {
      ticker: company.ticker,
      cik: company.cik,
      name: company.name ?? null,
      company_id: company.id,
      is_active: true,
      is_eligible: true,
      has_insider_trades: true,
      has_ptr_forms: hasPtrs,
      has_clusters: false,
      eligibility_reason: hasPtrs ? "form4_filing,ptr_trade" : "form4_filing",
      as_of_date: nowIso,
      updated_at: nowIso,
      created_at: nowIso,
    }
  })

  // --- 4. Replace candidate_universe with fresh seed ---
  const { error: deleteError } = await supabase
    .from("candidate_universe")
    .delete()
    .neq("ticker", "~~NEVER_MATCHES~~") // delete all rows safely

  if (deleteError) {
    return NextResponse.json(
      { ok: false, error: `Failed to clear candidate_universe: ${deleteError.message}` },
      { status: 500 }
    )
  }

  let insertedCount = 0
  const INSERT_CHUNK = 250
  for (let i = 0; i < universeRows.length; i += INSERT_CHUNK) {
    const chunk = universeRows.slice(i, i + INSERT_CHUNK)
    const { error: insertError } = await supabase
      .from("candidate_universe")
      .insert(chunk)
    if (insertError) {
      return NextResponse.json(
        { ok: false, error: `Seed insert failed at offset ${i}: ${insertError.message}` },
        { status: 500 }
      )
    }
    insertedCount += chunk.length
  }

  return NextResponse.json({
    ok: true,
    seeded: insertedCount,
    fromFilings: allTickerSet.size - ptrTickerSet.size,
    fromPtrs: ptrTickerSet.size,
    lookbackDays: FILINGS_LOOKBACK_DAYS,
    message: `Seeded candidate_universe with ${insertedCount} companies from insider filings and PTR trades (${FILINGS_LOOKBACK_DAYS}d lookback).`,
  })
}
