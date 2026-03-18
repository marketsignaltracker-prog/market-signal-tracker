import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const maxDuration = 300
export const dynamic = "force-dynamic"

type CandidateHistoryRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price?: number | null
  market_cap?: number | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  sector?: string | null
  industry?: string | null
  business_description?: string | null
  avg_volume_20d?: number | null
  avg_dollar_volume_20d?: number | null
  one_day_return?: number | null
  return_5d?: number | null
  return_10d?: number | null
  return_20d?: number | null
  relative_strength_20d?: number | null
  volume_ratio?: number | null
  breakout_20d?: boolean | null
  breakout_10d?: boolean | null
  above_sma_20?: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  passes_price?: boolean | null
  passes_volume?: boolean | null
  passes_dollar_volume?: boolean | null
  passes_market_cap?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  passed?: boolean | null
  screen_reason?: string | null
  last_screened_at?: string | null
  updated_at?: string | null
  screened_on?: string | null
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string | null
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price?: number | null
  market_cap?: number | null
  pe_ratio?: number | null
  pe_forward?: number | null
  pe_type?: string | null
  sector?: string | null
  industry?: string | null
  business_description?: string | null
  avg_volume_20d?: number | null
  avg_dollar_volume_20d?: number | null
  one_day_return?: number | null
  return_5d?: number | null
  return_10d?: number | null
  return_20d?: number | null
  relative_strength_20d?: number | null
  volume_ratio?: number | null
  breakout_20d?: boolean | null
  breakout_10d?: boolean | null
  above_sma_20?: boolean | null
  breakout_clearance_pct?: number | null
  extension_from_sma20_pct?: number | null
  close_in_day_range?: number | null
  catalyst_count?: number | null
  passes_price?: boolean | null
  passes_volume?: boolean | null
  passes_dollar_volume?: boolean | null
  passes_market_cap?: boolean | null
  candidate_score?: number | null
  included?: boolean | null
  passed?: boolean | null
  as_of_date?: string | null
  screen_reason?: string | null
  last_screened_at?: string | null
  updated_at?: string | null
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

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function buildEligibilityReason(row: CandidateHistoryRow) {
  const reasons: string[] = ["screened_candidate"]

  if (row.included) reasons.push("included")
  if (row.passed) reasons.push("passed")
  if ((row.candidate_score ?? 0) >= 85) reasons.push("high_score")
  else if ((row.candidate_score ?? 0) >= 70) reasons.push("qualified_score")

  if (row.breakout_20d) reasons.push("breakout_20d")
  if (row.breakout_10d) reasons.push("breakout_10d")
  if (row.above_sma_20) reasons.push("above_sma_20")
  if ((row.relative_strength_20d ?? 0) >= 3) reasons.push("relative_strength")
  if ((row.volume_ratio ?? 0) >= 1.25) reasons.push("volume_confirmation")

  return reasons.join(",")
}

function shouldIncludeRow(
  row: CandidateHistoryRow,
  minCandidateScore: number
) {
  const score = Number(row.candidate_score ?? 0)

  if (row.included === true) return true
  if (row.passed === true) return true
  if (score >= minCandidateScore) return true

  return false
}

function toUniverseRow(
  row: CandidateHistoryRow,
  updatedAt: string
): CandidateUniverseRow {
  return {
    company_id: row.company_id ?? null,
    ticker: normalizeTicker(row.ticker),
    cik: row.cik ?? null,
    name: row.name ?? null,
    is_active: row.is_active ?? true,
    is_eligible: true,

    has_insider_trades: row.has_insider_trades ?? false,
    has_ptr_forms: row.has_ptr_forms ?? false,
    has_clusters: row.has_clusters ?? false,
    eligibility_reason: buildEligibilityReason(row),

    price: row.price ?? null,
    market_cap: row.market_cap ?? null,
    pe_ratio: row.pe_ratio ?? null,
    pe_forward: row.pe_forward ?? null,
    pe_type: row.pe_type ?? null,
    sector: row.sector ?? null,
    industry: row.industry ?? null,
    business_description: row.business_description ?? null,
    avg_volume_20d: row.avg_volume_20d ?? null,
    avg_dollar_volume_20d: row.avg_dollar_volume_20d ?? null,
    one_day_return: row.one_day_return ?? null,
    return_5d: row.return_5d ?? null,
    return_10d: row.return_10d ?? null,
    return_20d: row.return_20d ?? null,
    relative_strength_20d: row.relative_strength_20d ?? null,
    volume_ratio: row.volume_ratio ?? null,
    breakout_20d: row.breakout_20d ?? false,
    breakout_10d: row.breakout_10d ?? false,
    above_sma_20: row.above_sma_20 ?? false,
    breakout_clearance_pct: row.breakout_clearance_pct ?? null,
    extension_from_sma20_pct: row.extension_from_sma20_pct ?? null,
    close_in_day_range: row.close_in_day_range ?? null,
    catalyst_count: row.catalyst_count ?? 0,
    passes_price: row.passes_price ?? false,
    passes_volume: row.passes_volume ?? false,
    passes_dollar_volume: row.passes_dollar_volume ?? false,
    passes_market_cap: row.passes_market_cap ?? false,
    candidate_score: row.candidate_score ?? 0,

    included: row.included ?? true,
    passed: row.passed ?? true,
    as_of_date: row.screened_on ?? null,
    screen_reason: row.screen_reason ?? "Selected from latest screening snapshot",
    last_screened_at: row.last_screened_at ?? updatedAt,
    updated_at: updatedAt,
  }
}

async function replaceCandidateUniverse(
  supabase: any,
  rows: CandidateUniverseRow[]
) {
  const candidateUniverseTable = supabase.from("candidate_universe") as any

  const { error: deleteError } = await candidateUniverseTable.delete().neq("ticker", "")
  if (deleteError) {
    throw new Error(`Failed clearing candidate_universe: ${deleteError.message}`)
  }

  if (!rows.length) {
    return
  }

  for (const chunk of chunkArray(rows, 250)) {
    const { error } = await candidateUniverseTable.upsert(chunk, {
      onConflict: "ticker",
    })

    if (error) {
      throw new Error(`Failed upserting candidate_universe rows: ${error.message}`)
    }
  }
}

export async function GET(request: NextRequest) {
  const authError = requirePipelineToken(request)
  if (authError) return authError

  try {
    const supabase = getSupabaseAdmin()

    const minCandidateScore = parseInteger(
      request.nextUrl.searchParams.get("minCandidateScore"),
      50
    )

    const onlyActive =
      request.nextUrl.searchParams.get("onlyActive") !== "false"
    const includeCounts =
      request.nextUrl.searchParams.get("includeCounts") === "true"

    const updatedAt = new Date().toISOString()

    const latestSnapshotQuery = await (supabase.from("candidate_screen_history") as any)
      .select("screened_on")
      .order("screened_on", { ascending: false })
      .limit(1)
      .maybeSingle()

    if (latestSnapshotQuery.error) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load latest screening snapshot: ${latestSnapshotQuery.error.message}`,
        },
        { status: 500 }
      )
    }

    const latestScreenedOn = latestSnapshotQuery.data?.screened_on ?? null

    if (!latestScreenedOn) {
      return NextResponse.json(
        {
          ok: false,
          error: "No candidate_screen_history snapshot found",
        },
        { status: 500 }
      )
    }

    let historyQuery = (supabase.from("candidate_screen_history") as any)
      .select("*")
      .eq("screened_on", latestScreenedOn)

    if (onlyActive) {
      historyQuery = historyQuery.eq("is_active", true)
    }

    const { data: historyRows, error: historyError } = await historyQuery

    if (historyError) {
      return NextResponse.json(
        {
          ok: false,
          error: `Failed to load candidate_screen_history rows: ${historyError.message}`,
          debug: {
            screenedOn: latestScreenedOn,
          },
        },
        { status: 500 }
      )
    }

    const typedHistoryRows = (historyRows || []) as CandidateHistoryRow[]

    const deduped = new Map<string, CandidateHistoryRow>()
    for (const row of typedHistoryRows) {
      const ticker = normalizeTicker(row.ticker)
      if (!ticker) continue

      const existing = deduped.get(ticker)
      if (!existing) {
        deduped.set(ticker, row)
        continue
      }

      const existingScore = Number(existing.candidate_score ?? 0)
      const rowScore = Number(row.candidate_score ?? 0)

      if (rowScore > existingScore) {
        deduped.set(ticker, row)
      }
    }

    const latestRows = Array.from(deduped.values())

    const selectedRows = latestRows.filter((row) =>
      shouldIncludeRow(row, minCandidateScore)
    )

    const universeRows = selectedRows.map((row) => toUniverseRow(row, updatedAt))

    await replaceCandidateUniverse(supabase, universeRows)

    return NextResponse.json({
      ok: true,
      screenedOn: latestScreenedOn,
      minCandidateScore,
      eligibleCount: universeRows.length,
      counts: includeCounts
        ? {
            snapshotRows: typedHistoryRows.length,
            distinctTickersInSnapshot: latestRows.length,
            selectedRows: selectedRows.length,
            eligibleRows: universeRows.length,
            includedFlagRows: latestRows.filter((row) => row.included === true).length,
            passedFlagRows: latestRows.filter((row) => row.passed === true).length,
            scoreQualifiedRows: latestRows.filter(
              (row) => Number(row.candidate_score ?? 0) >= minCandidateScore
            ).length,
          }
        : undefined,
      message:
        "Eligible universe rebuilt from the latest candidate screening snapshot.",
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