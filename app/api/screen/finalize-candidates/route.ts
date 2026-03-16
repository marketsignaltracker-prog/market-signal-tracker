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
  passed?: boolean | null
  screen_reason: string
  last_screened_at: string
  as_of_date?: string | null
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
  passed?: boolean | null
  screen_reason: string
  last_screened_at: string
  as_of_date?: string | null
  updated_at: string
}

type TickerScoreRow = {
  ticker: string
  company_id?: number | null
  filing_signal_score?: number | null
  ptr_signal_score?: number | null
  combined_score?: number | null
  rank?: number | null
  confidence_label?: "low" | "medium" | "high" | null
  as_of_date?: string | null
}

type RankedRow = {
  row: CandidateUniverseRow
  combinedScore: number
  candidateScore: number
  filingSignalScore: number
  ptrSignalScore: number
  confidenceLabel: "low" | "medium" | "high"
  bucket: "high" | "medium" | "fallback"
  reasons: string[]
}

const DB_CHUNK_SIZE = 250

const DEFAULT_FINAL_LIMIT = 30
const MAX_FINAL_LIMIT = 50
const DEFAULT_TARGET_MIN = 12
const MIN_COMBINED_SCORE_HIGH = 72
const MIN_COMBINED_SCORE_MEDIUM = 58
const MIN_COMBINED_SCORE_FALLBACK = 45

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

function parseInteger(value: string | null | undefined, fallback: number) {
  if (!value || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

async function upsertUniverseInChunks(table: any, rows: CandidateUniverseRow[]) {
  let insertedOrUpdated = 0
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(rows, DB_CHUNK_SIZE)) {
    const { error } = await table.upsert(chunk, { onConflict: "ticker" })
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    } else {
      insertedOrUpdated += chunk.length
    }
  }

  return { insertedOrUpdated, errorCount, errors }
}

async function updateUniverseFlagsInChunks(
  table: any,
  tickers: string[],
  values: Partial<CandidateUniverseRow>
) {
  let errorCount = 0
  const errors: string[] = []

  for (const chunk of chunkArray(tickers, DB_CHUNK_SIZE)) {
    const { error } = await table.update(values).in("ticker", chunk)
    if (error) {
      errorCount += chunk.length
      errors.push(error.message)
    }
  }

  return { errorCount, errors }
}

function buildFallbackUniverseMap(rows: CandidateHistoryRow[]) {
  const map = new Map<string, CandidateUniverseRow>()

  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue

    if (!map.has(ticker)) {
      map.set(ticker, {
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
        included: row.included,
        passed: row.passed ?? row.included,
        screen_reason: row.screen_reason,
        last_screened_at: row.last_screened_at,
        as_of_date: row.as_of_date ?? row.last_screened_at,
        updated_at: row.updated_at,
      })
    }
  }

  return map
}

function getBucket(params: {
  combinedScore: number
  candidateScore: number
  filingSignalScore: number
  ptrSignalScore: number
}) {
  const { combinedScore, candidateScore, filingSignalScore, ptrSignalScore } = params

  if (
    combinedScore >= MIN_COMBINED_SCORE_HIGH &&
    candidateScore >= 65 &&
    (filingSignalScore >= 45 || ptrSignalScore >= 45)
  ) {
    return "high" as const
  }

  if (combinedScore >= MIN_COMBINED_SCORE_MEDIUM && candidateScore >= 58) {
    return "medium" as const
  }

  if (combinedScore >= MIN_COMBINED_SCORE_FALLBACK && candidateScore >= 50) {
    return "fallback" as const
  }

  return null
}

function buildRankedRows(params: {
  universeRows: CandidateUniverseRow[]
  tickerScores: TickerScoreRow[]
}) {
  const { universeRows, tickerScores } = params

  const universeByTicker = new Map<string, CandidateUniverseRow>()
  for (const row of universeRows) {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) continue
    universeByTicker.set(ticker, row)
  }

  const ranked: RankedRow[] = []

  for (const scoreRow of tickerScores) {
    const ticker = normalizeTicker(scoreRow.ticker)
    if (!ticker) continue

    const universeRow = universeByTicker.get(ticker)
    if (!universeRow) continue

    const combinedScore = Number(scoreRow.combined_score || 0)
    const candidateScore = Number(universeRow.candidate_score || 0)
    const filingSignalScore = Number(scoreRow.filing_signal_score || 0)
    const ptrSignalScore = Number(scoreRow.ptr_signal_score || 0)
    const confidenceLabel =
      (scoreRow.confidence_label as "low" | "medium" | "high" | null) || "low"

    const bucket = getBucket({
      combinedScore,
      candidateScore,
      filingSignalScore,
      ptrSignalScore,
    })

    if (!bucket) continue

    const reasons = uniqueStrings([
      ptrSignalScore >= 60 ? "strong PTR score" : null,
      ptrSignalScore >= 40 ? "constructive PTR score" : null,
      filingSignalScore >= 60 ? "strong filing score" : null,
      filingSignalScore >= 40 ? "constructive filing score" : null,
      candidateScore >= 75 ? "strong candidate score" : null,
      candidateScore >= 60 ? "passed technical candidate screen" : null,
      universeRow.has_ptr_forms ? "has PTR activity" : null,
      universeRow.has_insider_trades ? "has insider filing activity" : null,
      universeRow.has_clusters ? "has cluster-style evidence" : null,
      universeRow.breakout_20d ? "20d breakout" : null,
      universeRow.breakout_10d ? "10d breakout" : null,
      universeRow.above_sma_20 ? "above 20-day average" : null,
      Number(universeRow.volume_ratio || 0) >= 1.5 ? "elevated volume" : null,
      Number(universeRow.relative_strength_20d || 0) >= 4
        ? "constructive relative strength"
        : null,
      confidenceLabel === "high" ? "high confidence score" : null,
    ])

    ranked.push({
      row: universeRow,
      combinedScore,
      candidateScore,
      filingSignalScore,
      ptrSignalScore,
      confidenceLabel,
      bucket,
      reasons,
    })
  }

  return ranked.sort((a, b) => {
    const bucketRank = { high: 3, medium: 2, fallback: 1 }

    if (bucketRank[b.bucket] !== bucketRank[a.bucket]) {
      return bucketRank[b.bucket] - bucketRank[a.bucket]
    }

    if (b.combinedScore !== a.combinedScore) {
      return b.combinedScore - a.combinedScore
    }

    if (b.ptrSignalScore !== a.ptrSignalScore) {
      return b.ptrSignalScore - a.ptrSignalScore
    }

    if (b.filingSignalScore !== a.filingSignalScore) {
      return b.filingSignalScore - a.filingSignalScore
    }

    if (b.candidateScore !== a.candidateScore) {
      return b.candidateScore - a.candidateScore
    }

    return a.row.ticker.localeCompare(b.row.ticker)
  })
}

function selectFinalRows(ranked: RankedRow[], targetMin: number, finalLimit: number) {
  const highRows = ranked.filter((item) => item.bucket === "high")
  const mediumRows = ranked.filter((item) => item.bucket === "medium")
  const fallbackRows = ranked.filter((item) => item.bucket === "fallback")

  let selected: RankedRow[] = []

  selected.push(...highRows.slice(0, Math.min(14, finalLimit)))

  if (selected.length < targetMin) {
    const needed = targetMin - selected.length
    selected.push(...mediumRows.slice(0, Math.max(needed, 6)))
  } else {
    selected.push(...mediumRows.slice(0, Math.min(6, finalLimit - selected.length)))
  }

  if (selected.length < targetMin) {
    const stillNeeded = targetMin - selected.length
    selected.push(...fallbackRows.slice(0, stillNeeded))
  }

  const seen = new Set<string>()
  const deduped = [...selected, ...highRows, ...mediumRows, ...fallbackRows].filter(
    (item) => {
      const ticker = normalizeTicker(item.row.ticker)
      if (!ticker || seen.has(ticker)) return false
      seen.add(ticker)
      return true
    }
  )

  return deduped.slice(0, finalLimit)
}

function toUniverseRow(
  ranked: RankedRow,
  selectedSource: string,
  updatedAt: string
): CandidateUniverseRow {
  const { row, combinedScore, candidateScore, filingSignalScore, ptrSignalScore, bucket, reasons } =
    ranked

  return {
    ...row,
    included: true,
    passed: true,
    screen_reason: `Finalized ${selectedSource} ${bucket} candidate (combined ${combinedScore}, candidate ${candidateScore}, filing ${filingSignalScore}, ptr ${ptrSignalScore}): ${reasons.join(", ")}`,
    last_screened_at: updatedAt,
    as_of_date: updatedAt,
    updated_at: updatedAt,
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

    const { searchParams } = new URL(request.url)
    const finalLimit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_FINAL_LIMIT)),
      MAX_FINAL_LIMIT
    )
    const targetMin = Math.max(
      1,
      parseInteger(searchParams.get("targetMin"), DEFAULT_TARGET_MIN)
    )
    const includePreview =
      (searchParams.get("includePreview") || "false").toLowerCase() === "true"

    const candidateHistoryTable = supabase.from("candidate_screen_history") as any
    const candidateUniverseTable = supabase.from("candidate_universe") as any
    const tickerScoresTable = supabase.from("ticker_scores_current") as any

    const [{ data: universeRows, error: universeError }, { data: tickerScores, error: tickerScoresError }] =
      await Promise.all([
        candidateUniverseTable.select("*"),
        tickerScoresTable.select("*").order("rank", { ascending: true }),
      ])

    if (universeError) {
      return Response.json({ ok: false, error: universeError.message }, { status: 500 })
    }

    if (tickerScoresError) {
      return Response.json({ ok: false, error: tickerScoresError.message }, { status: 500 })
    }

    let workingUniverseRows = (universeRows || []) as CandidateUniverseRow[]

    if (!workingUniverseRows.length) {
      const { data: screenedDates, error: screenedDatesError } = await candidateHistoryTable
        .select("screened_on")
        .order("screened_on", { ascending: false })

      if (screenedDatesError) {
        return Response.json({ ok: false, error: screenedDatesError.message }, { status: 500 })
      }

      const orderedDates = uniqueStrings(
        (screenedDates || []).map((row: any) => String(row.screened_on || ""))
      )

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
              row.above_sma_20
            )
        )

        if (viableRows.length > 0) {
          snapshotRows = typedRows
          break
        }
      }

      workingUniverseRows = [...buildFallbackUniverseMap(snapshotRows).values()]
    }

    if (!workingUniverseRows.length) {
      return Response.json(
        {
          ok: false,
          error: "No candidate universe rows available to finalize",
        },
        { status: 500 }
      )
    }

    const typedTickerScores = (tickerScores || []) as TickerScoreRow[]
    if (!typedTickerScores.length) {
      return Response.json(
        {
          ok: false,
          error: "No ticker_scores_current rows available to finalize",
        },
        { status: 500 }
      )
    }

    const rankedRows = buildRankedRows({
      universeRows: workingUniverseRows,
      tickerScores: typedTickerScores,
    })

    const highCount = rankedRows.filter((item) => item.bucket === "high").length
    const mediumCount = rankedRows.filter((item) => item.bucket === "medium").length
    const fallbackCount = rankedRows.filter((item) => item.bucket === "fallback").length

    const selectedRankedRows = selectFinalRows(rankedRows, targetMin, finalLimit)

    if (!selectedRankedRows.length) {
      return Response.json(
        {
          ok: false,
          error: "Finalize step found zero final candidates",
          debug: {
            universeRowCount: workingUniverseRows.length,
            tickerScoreCount: typedTickerScores.length,
            highCount,
            mediumCount,
            fallbackCount,
          },
        },
        { status: 500 }
      )
    }

    const updatedAt = new Date().toISOString()
    const selectedTickers = new Set(
      selectedRankedRows.map((item) => normalizeTicker(item.row.ticker))
    )

    const selectedSource =
      highCount >= targetMin
        ? "high-led"
        : mediumCount > 0
          ? "medium-led"
          : "fallback-led"

    const finalizedRows = selectedRankedRows.map((item) =>
      toUniverseRow(item, selectedSource, updatedAt)
    )

    const resetResult = await updateUniverseFlagsInChunks(
      candidateUniverseTable,
      workingUniverseRows.map((row) => normalizeTicker(row.ticker)).filter(Boolean),
      {
        included: false,
        passed: false,
        updated_at: updatedAt,
      }
    )

    if (resetResult.errorCount > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed resetting candidate universe finalization flags",
          debug: {
            errorSamples: resetResult.errors.slice(0, 5),
          },
        },
        { status: 500 }
      )
    }

    const universeWrite = await upsertUniverseInChunks(candidateUniverseTable, finalizedRows)
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

    const { data: latestHistorySnapshot, error: latestHistoryError } = await candidateHistoryTable
      .select("screened_on")
      .order("screened_on", { ascending: false })
      .limit(1)
      .maybeSingle()

    if (latestHistoryError) {
      return Response.json(
        { ok: false, error: latestHistoryError.message },
        { status: 500 }
      )
    }

    const latestScreenedOn = latestHistorySnapshot?.screened_on ?? null

    if (latestScreenedOn) {
      const { error: markFalseError } = await candidateHistoryTable
        .update({ included: false, passed: false })
        .eq("screened_on", latestScreenedOn)

      if (markFalseError) {
        return Response.json(
          {
            ok: false,
            error: "Failed resetting included/passed flags on candidate history",
            debug: {
              message: markFalseError.message,
            },
          },
          { status: 500 }
        )
      }

      for (const chunk of chunkArray([...selectedTickers], DB_CHUNK_SIZE)) {
        const { error } = await candidateHistoryTable
          .update({ included: true, passed: true })
          .eq("screened_on", latestScreenedOn)
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
    }

    return Response.json({
      ok: true,
      stage: "finalize_candidates",
      selectedSource,
      universeRowCount: workingUniverseRows.length,
      tickerScoreCount: typedTickerScores.length,
      highCount,
      mediumCount,
      fallbackCount,
      finalizedCount: finalizedRows.length,
      firstTicker: finalizedRows[0]?.ticker ?? null,
      lastTicker: finalizedRows[finalizedRows.length - 1]?.ticker ?? null,
      preview: includePreview
        ? selectedRankedRows.slice(0, 25).map((item, index) => ({
            rank: index + 1,
            ticker: item.row.ticker,
            bucket: item.bucket,
            combinedScore: item.combinedScore,
            candidateScore: item.candidateScore,
            filingSignalScore: item.filingSignalScore,
            ptrSignalScore: item.ptrSignalScore,
            confidenceLabel: item.confidenceLabel,
            reasons: item.reasons.slice(0, 8),
          }))
        : undefined,
      message:
        "Final candidates were rebuilt from current ticker scores, with candidate universe rows preserved and passed/included flags updated.",
    })
  } catch (error: any) {
    return Response.json(
      { ok: false, error: error.message || "Unknown finalization error" },
      { status: 500 }
    )
  }
}