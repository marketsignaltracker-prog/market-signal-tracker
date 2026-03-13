import { createClient } from "@supabase/supabase-js"

type CandidateHistoryRow = {
  ticker: string
  cik: string
  name: string | null
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
  ticker: string
  cik: string
  name: string | null
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

const MAX_FINAL_CANDIDATES = 75
const STRICT_MIN_SCORE = 78
const FALLBACK_MIN_SCORE = 68
const MIN_STRICT_POOL_SIZE = 15
const DB_CHUNK_SIZE = 250

function chunkArray<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
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
    (row.return_10d ?? -999) >= 3 &&
    (row.return_20d ?? -999) >= 8 &&
    (row.relative_strength_20d ?? -999) >= 3 &&
    (row.volume_ratio ?? 0) >= 1.1 &&
    ((row.breakout_20d ?? false) || (row.breakout_10d ?? false)) &&
    (row.breakout_clearance_pct ?? -999) >= 0.15 &&
    (row.extension_from_sma20_pct ?? 999) <= 14 &&
    (row.close_in_day_range ?? 0) >= 0.55
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
    (row.return_20d ?? -999) >= 4 &&
    (row.relative_strength_20d ?? -999) >= 1.5 &&
    (row.volume_ratio ?? 0) >= 1.0 &&
    (row.extension_from_sma20_pct ?? 999) <= 16
  )
}

function compareRows(a: CandidateHistoryRow, b: CandidateHistoryRow) {
  if ((b.candidate_score ?? 0) !== (a.candidate_score ?? 0)) {
    return (b.candidate_score ?? 0) - (a.candidate_score ?? 0)
  }
  if ((b.relative_strength_20d ?? 0) !== (a.relative_strength_20d ?? 0)) {
    return (b.relative_strength_20d ?? 0) - (a.relative_strength_20d ?? 0)
  }
  if ((b.return_20d ?? 0) !== (a.return_20d ?? 0)) {
    return (b.return_20d ?? 0) - (a.return_20d ?? 0)
  }
  if ((b.volume_ratio ?? 0) !== (a.volume_ratio ?? 0)) {
    return (b.volume_ratio ?? 0) - (a.volume_ratio ?? 0)
  }
  if ((b.avg_dollar_volume_20d ?? 0) !== (a.avg_dollar_volume_20d ?? 0)) {
    return (b.avg_dollar_volume_20d ?? 0) - (a.avg_dollar_volume_20d ?? 0)
  }
  return (b.market_cap ?? 0) - (a.market_cap ?? 0)
}

function toUniverseRow(row: CandidateHistoryRow, reason: string): CandidateUniverseRow {
  return {
    ticker: row.ticker,
    cik: row.cik,
    name: row.name,
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
    screen_reason: reason,
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

    const { data: latestRow, error: latestError } = await candidateHistoryTable
      .select("screened_on")
      .order("screened_on", { ascending: false })
      .limit(1)
      .maybeSingle()

    if (latestError) {
      return Response.json({ ok: false, error: latestError.message }, { status: 500 })
    }

    const screenedOn = latestRow?.screened_on ?? null

    if (!screenedOn) {
      return Response.json(
        { ok: false, error: "No candidate history rows available to finalize" },
        { status: 500 }
      )
    }

    const { data: latestRows, error: rowsError } = await candidateHistoryTable
      .select("*")
      .eq("screened_on", screenedOn)

    if (rowsError) {
      return Response.json({ ok: false, error: rowsError.message }, { status: 500 })
    }

    const snapshotRows = (latestRows || []) as CandidateHistoryRow[]

    if (!snapshotRows.length) {
      return Response.json(
        { ok: false, error: "Latest screened snapshot contains no rows" },
        { status: 500 }
      )
    }

    const scoredRows = snapshotRows.filter(
      (row) => row.candidate_score !== null && row.candidate_score !== undefined
    )

    const strictPool = scoredRows.filter(isStrictEligible).sort(compareRows)
    const fallbackPool = scoredRows.filter(isFallbackEligible).sort(compareRows)

    const selectedSource = strictPool.length >= MIN_STRICT_POOL_SIZE ? "strict" : "fallback"
    const selectedPool = selectedSource === "strict" ? strictPool : fallbackPool
    const selectedRows = selectedPool.slice(0, MAX_FINAL_CANDIDATES)

    if (!selectedRows.length) {
      return Response.json(
        {
          ok: false,
          error: "Finalize step found zero eligible candidates",
          debug: {
            screenedOn,
            snapshotRowCount: snapshotRows.length,
            scoredRowCount: scoredRows.length,
            strictEligibleCount: strictPool.length,
            fallbackEligibleCount: fallbackPool.length,
          },
        },
        { status: 500 }
      )
    }

    const selectedTickers = new Set(selectedRows.map((row) => row.ticker))
    const universeRows = selectedRows.map((row) =>
      toUniverseRow(
        row,
        selectedSource === "strict"
          ? `Finalized elite strict candidate (${row.candidate_score})`
          : `Finalized elite fallback candidate (${row.candidate_score})`
      )
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

    return Response.json({
      ok: true,
      screenedOn,
      snapshotRowCount: snapshotRows.length,
      scoredRowCount: scoredRows.length,
      strictEligibleCount: strictPool.length,
      fallbackEligibleCount: fallbackPool.length,
      selectedSource,
      finalizedCount: universeRows.length,
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