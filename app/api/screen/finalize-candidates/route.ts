import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type CandidateHistoryRow = {
  company_id?: number | null
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price: number | null
  market_cap: number | null
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null
  sector: string | null
  industry: string | null
  business_description: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return: number | null
  return_5d: number | null
  return_10d: number | null
  return_20d: number | null
  relative_strength_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  breakout_10d: boolean
  above_sma_20: boolean
  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number
  passes_price: boolean
  passes_volume: boolean
  passes_dollar_volume: boolean
  passes_market_cap: boolean
  candidate_score: number
  included: boolean
  screen_reason: string
  last_screened_at: string
  updated_at: string
  screened_on: string
  snapshot_key: string
  created_at: string
}

type CandidateUniverseRow = {
  company_id?: number | null
  ticker: string
  cik: string
  name: string | null
  is_active?: boolean | null
  is_eligible?: boolean | null
  has_insider_trades?: boolean | null
  has_ptr_forms?: boolean | null
  has_clusters?: boolean | null
  eligibility_reason?: string | null
  price: number | null
  market_cap: number | null
  pe_ratio: number | null
  pe_forward: number | null
  pe_type: string | null
  sector: string | null
  industry: string | null
  business_description: string | null
  avg_volume_20d: number | null
  avg_dollar_volume_20d: number | null
  one_day_return: number | null
  return_5d: number | null
  return_10d: number | null
  return_20d: number | null
  relative_strength_20d: number | null
  volume_ratio: number | null
  breakout_20d: boolean
  breakout_10d: boolean
  above_sma_20: boolean
  breakout_clearance_pct: number | null
  extension_from_sma20_pct: number | null
  close_in_day_range: number | null
  catalyst_count: number
  passes_price: boolean
  passes_volume: boolean
  passes_dollar_volume: boolean
  passes_market_cap: boolean
  candidate_score: number
  included: boolean
  screen_reason: string
  last_screened_at: string
  updated_at: string
}

type RawPtrTradeRow = {
  filer_name: string | null
  ticker: string | null
  action: string | null
  transaction_date: string | null
  amount_low: number | null
  amount_high: number | null
}

type PtrSignalSummary = {
  ptrBonus: number
  buyTradeCount: number
  uniqueFilers: number
  recentBuyCount: number
  totalAmountLow: number
  summary: string | null
}

type RankedRow = {
  row: CandidateHistoryRow
  ptrSummary: PtrSignalSummary | null
  selectionScore: number
  bucket: "strict" | "balanced" | "fallback"
  reasons: string[]
}

const MAX_FINAL_CANDIDATES = 24
const TARGET_MIN_FINAL_CANDIDATES = 10
const STRICT_MIN_SCORE = 68
const BALANCED_MIN_SCORE = 62
const FALLBACK_MIN_SCORE = 56
const DB_CHUNK_SIZE = 250

const PTR_LOOKBACK_DAYS = 45
const PTR_RECENT_DAYS = 14
const MAX_PTR_BONUS = 8

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || "").trim().toUpperCase()
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function daysAgo(isoDate: string | null) {
  if (!isoDate) return null
  const ts = new Date(isoDate).getTime()
  if (Number.isNaN(ts)) return null
  return Math.max(0, Math.floor((Date.now() - ts) / (24 * 60 * 60 * 1000)))
}

async function upsertUniverseInChunks(table: any, rows: CandidateUniverseRow[]) {
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict: "ticker" })
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    }
  }

  return { errorCount, errors }
}

async function deleteAllUniverseRows(table: any) {
  const { error } = await table.delete().neq("ticker", "")
  return error ? error.message : null
}

function isStrictEligible(row: CandidateHistoryRow) {
  return (
    row.passes_price &&
    row.passes_volume &&
    row.passes_dollar_volume &&
    row.passes_market_cap &&
    row.above_sma_20 &&
    (row.candidate_score ?? 0) >= STRICT_MIN_SCORE &&
    (row.return_10d ?? -999) >= 1 &&
    (row.return_20d ?? -999) >= 4 &&
    (row.relative_strength_20d ?? -999) >= 1.5 &&
    (row.volume_ratio ?? 0) >= 0.8 &&
    (
      (row.breakout_20d ?? false) ||
      (row.breakout_10d ?? false) ||
      (row.relative_strength_20d ?? -999) >= 4
    ) &&
    (row.breakout_clearance_pct ?? -999) >= -1 &&
    (row.extension_from_sma20_pct ?? 999) <= 16 &&
    (row.close_in_day_range ?? 0) >= 0.4
  )
}

function isBalancedEligible(row: CandidateHistoryRow) {
  return (
    row.passes_price &&
    row.passes_volume &&
    row.passes_dollar_volume &&
    row.passes_market_cap &&
    row.above_sma_20 &&
    (row.candidate_score ?? 0) >= BALANCED_MIN_SCORE &&
    (row.return_20d ?? -999) >= 2 &&
    (row.relative_strength_20d ?? -999) >= 0.5 &&
    (row.volume_ratio ?? 0) >= 0.75 &&
    (row.extension_from_sma20_pct ?? 999) <= 18
  )
}

function isFallbackEligible(row: CandidateHistoryRow) {
  return (
    row.passes_price &&
    row.passes_volume &&
    row.passes_dollar_volume &&
    row.passes_market_cap &&
    row.above_sma_20 &&
    (row.candidate_score ?? 0) >= FALLBACK_MIN_SCORE &&
    (row.return_20d ?? -999) >= 1 &&
    (row.relative_strength_20d ?? -999) >= 0 &&
    (row.volume_ratio ?? 0) >= 0.65 &&
    (row.extension_from_sma20_pct ?? 999) <= 20
  )
}

function buildPtrSignalMap(rows: RawPtrTradeRow[]) {
  const grouped = new Map<string, RawPtrTradeRow[]>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue

    const action = String(row.action || "").trim().toLowerCase()
    if (action !== "buy" && action !== "purchase" && action !== "purchased") continue

    if (!grouped.has(ticker)) grouped.set(ticker, [])
    grouped.get(ticker)!.push(row)
  }

  const out = new Map<string, PtrSignalSummary>()

  for (const [ticker, tickerRows] of grouped.entries()) {
    const uniqueFilers = new Set(
      tickerRows.map((row) => String(row.filer_name || "").trim()).filter(Boolean)
    ).size

    const recentBuyCount = tickerRows.filter((row) => {
      const age = daysAgo(row.transaction_date)
      return age !== null && age <= PTR_RECENT_DAYS
    }).length

    const totalAmountLow = tickerRows.reduce((sum, row) => sum + Number(row.amount_low || 0), 0)
    const buyTradeCount = tickerRows.length

    let ptrBonus = 0

    if (buyTradeCount >= 1) ptrBonus += 2
    if (buyTradeCount >= 2) ptrBonus += 1
    if (buyTradeCount >= 3) ptrBonus += 1
    if (uniqueFilers >= 2) ptrBonus += 1
    if (uniqueFilers >= 3) ptrBonus += 1
    if (recentBuyCount >= 1) ptrBonus += 1
    if (recentBuyCount >= 2) ptrBonus += 1
    if (totalAmountLow >= 100_001) ptrBonus += 1
    if (totalAmountLow >= 250_001) ptrBonus += 1
    if (totalAmountLow >= 500_001) ptrBonus += 1

    ptrBonus = Math.min(ptrBonus, MAX_PTR_BONUS)

    const summaryParts: string[] = []
    summaryParts.push(`${buyTradeCount} PTR buy${buyTradeCount === 1 ? "" : "s"}`)
    if (uniqueFilers > 0) summaryParts.push(`${uniqueFilers} filer${uniqueFilers === 1 ? "" : "s"}`)
    if (recentBuyCount > 0) summaryParts.push(`${recentBuyCount} recent`)
    if (totalAmountLow > 0) summaryParts.push(`min disclosed $${totalAmountLow.toLocaleString()}`)

    out.set(ticker, {
      ptrBonus,
      buyTradeCount,
      uniqueFilers,
      recentBuyCount,
      totalAmountLow,
      summary: summaryParts.length ? `PTR support: ${summaryParts.join(", ")}` : null,
    })
  }

  return out
}

function getSelectionScore(
  row: CandidateHistoryRow,
  ptr: PtrSignalSummary | null | undefined
) {
  let score = Number(row.candidate_score ?? 0)
  const reasons: string[] = []

  /**
   * Priority stack:
   * PTR > insider / filings > signals / clusters > technical refinement
   */

  if (ptr?.ptrBonus) {
    score += ptr.ptrBonus + 4
    reasons.push(`PTR priority +${ptr.ptrBonus + 4}`)
  }

  if (row.has_insider_trades) {
    score += 4
    reasons.push("insider filing support")
  }

  if ((row.eligibility_reason || "").includes("high_priority_filings")) {
    score += 3
    reasons.push("high-priority filing support")
  }

  if (row.has_clusters) {
    score += 1
    reasons.push("signal support")
  }

  if (row.above_sma_20) {
    score += 2
    reasons.push("above 20dma")
  }

  if ((row.return_10d ?? 0) >= 2) {
    score += 2
    reasons.push("10d momentum")
  } else if ((row.return_10d ?? 0) > 0) {
    score += 1
  }

  if ((row.return_20d ?? 0) >= 5) {
    score += 3
    reasons.push("20d momentum")
  } else if ((row.return_20d ?? 0) >= 2) {
    score += 1.5
  }

  if ((row.relative_strength_20d ?? 0) >= 4) {
    score += 3
    reasons.push("strong relative strength")
  } else if ((row.relative_strength_20d ?? 0) >= 2) {
    score += 2
  } else if ((row.relative_strength_20d ?? 0) >= 0.5) {
    score += 1
  }

  if ((row.volume_ratio ?? 0) >= 1.5) {
    score += 2
    reasons.push("volume expansion")
  } else if ((row.volume_ratio ?? 0) >= 1.0) {
    score += 1
  }

  if (row.breakout_20d) {
    score += 2.5
    reasons.push("20d breakout")
  } else if (row.breakout_10d) {
    score += 1.5
    reasons.push("10d breakout")
  } else if ((row.breakout_clearance_pct ?? -999) >= -0.5) {
    score += 0.75
    reasons.push("near breakout")
  }

  if ((row.close_in_day_range ?? 0) >= 0.65) {
    score += 1.5
    reasons.push("strong close")
  } else if ((row.close_in_day_range ?? 0) >= 0.5) {
    score += 0.5
  }

  if ((row.extension_from_sma20_pct ?? 999) <= 12) {
    score += 1
    reasons.push("not too extended")
  } else if ((row.extension_from_sma20_pct ?? 999) > 18) {
    score -= 2
    reasons.push("too extended")
  }

  if ((row.avg_dollar_volume_20d ?? 0) >= 50_000_000) {
    score += 1.5
    reasons.push("strong liquidity")
  } else if ((row.avg_dollar_volume_20d ?? 0) >= 35_000_000) {
    score += 0.5
  }

  return {
    selectionScore: Math.round(score * 100) / 100,
    reasons,
  }
}

function buildRankedRows(
  rows: CandidateHistoryRow[],
  ptrMap: Map<string, PtrSignalSummary>
): RankedRow[] {
  return rows
    .map((row) => {
      const ptrSummary = ptrMap.get(normalizeTicker(row.ticker)) ?? null
      const { selectionScore, reasons } = getSelectionScore(row, ptrSummary)

      let bucket: "strict" | "balanced" | "fallback" = "fallback"
      if (isStrictEligible(row)) bucket = "strict"
      else if (isBalancedEligible(row)) bucket = "balanced"
      else if (isFallbackEligible(row)) bucket = "fallback"
      else return null

      return {
        row,
        ptrSummary,
        selectionScore,
        bucket,
        reasons,
      }
    })
    .filter((item): item is RankedRow => item !== null)
    .sort((a, b) => {
      const bucketRank = { strict: 3, balanced: 2, fallback: 1 }
      if (bucketRank[b.bucket] !== bucketRank[a.bucket]) {
        return bucketRank[b.bucket] - bucketRank[a.bucket]
      }

      if (b.selectionScore !== a.selectionScore) {
        return b.selectionScore - a.selectionScore
      }

      if ((b.row.candidate_score ?? 0) !== (a.row.candidate_score ?? 0)) {
        return (b.row.candidate_score ?? 0) - (a.row.candidate_score ?? 0)
      }

      if ((b.row.relative_strength_20d ?? 0) !== (a.row.relative_strength_20d ?? 0)) {
        return (b.row.relative_strength_20d ?? 0) - (a.row.relative_strength_20d ?? 0)
      }

      if ((b.row.return_20d ?? 0) !== (a.row.return_20d ?? 0)) {
        return (b.row.return_20d ?? 0) - (a.row.return_20d ?? 0)
      }

      if ((b.row.volume_ratio ?? 0) !== (a.row.volume_ratio ?? 0)) {
        return (b.row.volume_ratio ?? 0) - (a.row.volume_ratio ?? 0)
      }

      return (b.row.market_cap ?? 0) - (a.row.market_cap ?? 0)
    })
}

function selectFinalRows(ranked: RankedRow[]) {
  const strictRows = ranked.filter((item) => item.bucket === "strict")
  const balancedRows = ranked.filter((item) => item.bucket === "balanced")
  const fallbackRows = ranked.filter((item) => item.bucket === "fallback")

  let selected: RankedRow[] = []

  selected.push(...strictRows.slice(0, 12))

  if (selected.length < TARGET_MIN_FINAL_CANDIDATES) {
    const needed = TARGET_MIN_FINAL_CANDIDATES - selected.length
    selected.push(...balancedRows.slice(0, Math.max(needed, 6)))
  } else {
    selected.push(...balancedRows.slice(0, 6))
  }

  if (selected.length < TARGET_MIN_FINAL_CANDIDATES) {
    const stillNeeded = TARGET_MIN_FINAL_CANDIDATES - selected.length
    selected.push(...fallbackRows.slice(0, stillNeeded))
  }

  const seen = new Set<string>()
  const deduped = [...selected, ...strictRows, ...balancedRows, ...fallbackRows].filter((item) => {
    const ticker = normalizeTicker(item.row.ticker)
    if (!ticker || seen.has(ticker)) return false
    seen.add(ticker)
    return true
  })

  return deduped.slice(0, MAX_FINAL_CANDIDATES)
}

function toUniverseRow(
  ranked: RankedRow,
  selectedSource: string
): CandidateUniverseRow {
  const { row, ptrSummary, selectionScore, bucket, reasons } = ranked
  const ptrReason = ptrSummary?.summary ? `; ${ptrSummary.summary}` : ""

  return {
    company_id: row.company_id ?? null,
    ticker: row.ticker,
    cik: row.cik,
    name: row.name,
    is_active: row.is_active ?? true,
    is_eligible: row.is_eligible ?? null,
    has_insider_trades: row.has_insider_trades ?? null,
    has_ptr_forms: row.has_ptr_forms ?? null,
    has_clusters: row.has_clusters ?? null,
    eligibility_reason: row.eligibility_reason ?? null,
    price: row.price,
    market_cap: row.market_cap,
    pe_ratio: row.pe_ratio,
    pe_forward: row.pe_forward,
    pe_type: row.pe_type,
    sector: row.sector,
    industry: row.industry,
    business_description: row.business_description,
    avg_volume_20d: row.avg_volume_20d,
    avg_dollar_volume_20d: row.avg_dollar_volume_20d,
    one_day_return: row.one_day_return,
    return_5d: row.return_5d,
    return_10d: row.return_10d,
    return_20d: row.return_20d,
    relative_strength_20d: row.relative_strength_20d,
    volume_ratio: row.volume_ratio,
    breakout_20d: row.breakout_20d,
    breakout_10d: row.breakout_10d,
    above_sma_20: row.above_sma_20,
    breakout_clearance_pct: row.breakout_clearance_pct,
    extension_from_sma20_pct: row.extension_from_sma20_pct,
    close_in_day_range: row.close_in_day_range,
    catalyst_count: row.catalyst_count,
    passes_price: row.passes_price,
    passes_volume: row.passes_volume,
    passes_dollar_volume: row.passes_dollar_volume,
    passes_market_cap: row.passes_market_cap,
    candidate_score: row.candidate_score,
    included: true,
    screen_reason: `Finalized ${selectedSource} ${bucket} candidate (${row.candidate_score}, selection ${selectionScore}): ${reasons.join(", ")}${ptrReason}`,
    last_screened_at: row.last_screened_at,
    updated_at: new Date().toISOString(),
  }
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const candidateHistoryTable = supabase.from("candidate_screen_history") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any

    const { data: screenedDates, error: screenedDatesError } = await candidateHistoryTable
      .select("screened_on")
      .order("screened_on", { ascending: false })

    if (screenedDatesError) {
      return Response.json({ ok: false, error: screenedDatesError.message }, { status: 500 })
    }

    const orderedDates = uniqueStrings(
      (screenedDates || []).map((row: any) => String(row.screened_on || ""))
    )

    let screenedOn: string | null = null
    let snapshotRows: CandidateHistoryRow[] = []

    for (const candidateDate of orderedDates) {
      const { data: rows, error: rowsError } = await candidateHistoryTable
        .select("*")
        .eq("screened_on", candidateDate)

      if (rowsError) {
        return Response.json({ ok: false, error: rowsError.message }, { status: 500 })
      }

      const typedRows = (rows || []) as CandidateHistoryRow[]
      if (!typedRows.length) continue

      const viableRows = typedRows.filter(
        (row) =>
          (row.candidate_score ?? 0) > 0 &&
          (
            row.passes_price ||
            row.passes_volume ||
            row.passes_dollar_volume ||
            row.passes_market_cap ||
            row.above_sma_20 ||
            row.return_20d !== null ||
            row.relative_strength_20d !== null
          )
      )

      if (viableRows.length >= 20) {
        screenedOn = candidateDate
        snapshotRows = typedRows
        break
      }
    }

    if (!screenedOn || !snapshotRows.length) {
      return Response.json(
        {
          ok: false,
          error: "No viable candidate history snapshot found to finalize",
        },
        { status: 500 }
      )
    }

    const scoredRows = snapshotRows.filter(
      (row) => row.candidate_score !== null && row.candidate_score !== undefined
    )

    const snapshotTickers = uniqueStrings(scoredRows.map((row) => row.ticker))
    const ptrCutoff = new Date()
    ptrCutoff.setDate(ptrCutoff.getDate() - PTR_LOOKBACK_DAYS)
    const ptrCutoffString = toIsoDateString(ptrCutoff)

    let ptrMap = new Map<string, PtrSignalSummary>()
    let ptrDiagnostics: Record<string, any> = {
      loaded: false,
      rows: 0,
      tickersWithPtrSupport: 0,
      error: null as string | null,
    }

    if (snapshotTickers.length > 0) {
      try {
        const { data: ptrRows, error: ptrError } = await supabase
          .from("raw_ptr_trades")
          .select("filer_name, ticker, action, transaction_date, amount_low, amount_high")
          .in("ticker", snapshotTickers)
          .gte("transaction_date", ptrCutoffString)

        if (ptrError) {
          ptrDiagnostics.error = ptrError.message
        } else {
          const normalizedRows = (ptrRows || []) as RawPtrTradeRow[]
          ptrMap = buildPtrSignalMap(normalizedRows)
          ptrDiagnostics = {
            loaded: true,
            rows: normalizedRows.length,
            tickersWithPtrSupport: ptrMap.size,
            error: null,
          }
        }
      } catch (error: any) {
        ptrDiagnostics.error = error?.message || "Unknown PTR lookup error"
      }
    }

    const rankedRows = buildRankedRows(scoredRows, ptrMap)
    const strictCount = rankedRows.filter((item) => item.bucket === "strict").length
    const balancedCount = rankedRows.filter((item) => item.bucket === "balanced").length
    const fallbackCount = rankedRows.filter((item) => item.bucket === "fallback").length

    const selectedRankedRows = selectFinalRows(rankedRows)

    if (!selectedRankedRows.length) {
      return Response.json(
        {
          ok: false,
          error: "Finalize step found zero eligible candidates",
          debug: {
            screenedOn,
            snapshotRowCount: snapshotRows.length,
            scoredRowCount: scoredRows.length,
            strictCount,
            balancedCount,
            fallbackCount,
            ptrDiagnostics,
          },
        },
        { status: 500 }
      )
    }

    const selectedTickers = new Set(selectedRankedRows.map((item) => item.row.ticker))
    const selectedSource =
      strictCount >= TARGET_MIN_FINAL_CANDIDATES
        ? "strict-led"
        : balancedCount > 0
          ? "balanced-led"
          : "fallback-led"

    const universeRows = selectedRankedRows.map((item) =>
      toUniverseRow(item, selectedSource)
    )

    const deleteError = await deleteAllUniverseRows(candidateUniverseTable)
    if (deleteError) {
      return Response.json(
        {
          ok: false,
          error: "Failed clearing candidate universe before finalization",
          debug: { deleteError },
        },
        { status: 500 }
      )
    }

    const universeWrite = await upsertUniverseInChunks(candidateUniverseTable, universeRows)
    if (universeWrite.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing finalized candidate universe",
          debug: {
            errorCount: universeWrite.errorCount,
            errorSamples: universeWrite.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const { error: markFalseError } = await candidateHistoryTable
      .update({ included: false })
      .eq("screened_on", screenedOn)

    if (markFalseError) {
      return Response.json(
        {
          ok: false,
          error: "Failed resetting included flags on candidate history",
          debug: {
            message: markFalseError.message,
          },
        },
        { status: 500 }
      )
    }

    for (const chunk of chunkArray([...selectedTickers], DB_CHUNK_SIZE)) {
      const { error } = await candidateHistoryTable
        .update({ included: true })
        .eq("screened_on", screenedOn)
        .in("ticker", chunk)

      if (error) {
        return Response.json(
          {
            ok: false,
            error: "Failed marking finalized rows in candidate history",
            debug: {
              message: error.message,
            },
          },
          { status: 500 }
        )
      }
    }

    const ptrSelectedCount = selectedRankedRows.filter((item) =>
      ptrMap.has(normalizeTicker(item.row.ticker))
    ).length

    return Response.json({
      ok: true,
      screenedOn,
      snapshotRowCount: snapshotRows.length,
      scoredRowCount: scoredRows.length,
      strictCount,
      balancedCount,
      fallbackCount,
      selectedSource,
      finalizedCount: universeRows.length,
      ptrDiagnostics,
      ptrSelectedCount,
      firstTicker: universeRows[0]?.ticker ?? null,
      lastTicker: universeRows[universeRows.length - 1]?.ticker ?? null,
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown finalization error" },
      { status: 500 }
    )
  }
}