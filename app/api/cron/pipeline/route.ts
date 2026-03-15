import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const maxDuration = 300
export const dynamic = "force-dynamic"

type StepResult = {
  step: string
  path: string
  ok: boolean
  status: number
  durationMs: number
  data: any
}

type PipelineStage =
  | "idle"
  | "companies"
  | "filings"
  | "signals"
  | "filing_signals"
  | "eligible_universe"
  | "screening"
  | "finalize_candidates"
  | "ptrs"
  | "ticker_scores"
  | "complete"
  | "error"

type PipelineStatus = "idle" | "running" | "success" | "error"

type PipelineStateRow = {
  job_name: string
  stage: PipelineStage
  status: PipelineStatus
  screen_start: number
  screen_batch: number
  screen_total: number | null
  screen_next_start: number | null
  cycle_started_at: string | null
  cycle_completed_at: string | null
  filings_completed_at: string | null
  signals_completed_at: string | null
  last_run_started_at: string | null
  last_run_finished_at: string | null
  last_success_at: string | null
  last_error: string | null
  last_error_at: string | null
  updated_at?: string
}

const PIPELINE_JOB_NAME = "market_signal_pipeline"

/**
 * Screening is now against a reduced eligible universe,
 * so batches can be moderately larger than the old Yahoo-first flow.
 */
const DEFAULT_SCREEN_BATCH = 50
const MAX_SCREEN_BATCH = 100

/**
 * Keep network-heavy stages conservative.
 * PTRs are especially expensive when they are live API / per-ticker.
 */
const DEFAULT_FILINGS_BATCH = 100
const DEFAULT_PTRS_BATCH = 25
const DEFAULT_SIGNALS_LIMIT = 250
const DEFAULT_SIGNALS_LOOKBACK_DAYS = 14
const DEFAULT_FILING_SIGNALS_LIMIT = 200
const DEFAULT_FILING_SIGNALS_LOOKBACK_DAYS = 21
const DEFAULT_ELIGIBLE_UNIVERSE_LOOKBACK_DAYS = 30
const DEFAULT_TICKER_SCORES_LIMIT = 1000

const DEFAULT_STEP_TIMEOUT_MS = 240_000

const MAX_PIPELINE_RUNTIME_MS = 210_000
const RUNTIME_SAFETY_BUFFER_MS = 15_000

/**
 * Screening can checkpoint cleanly, so allow a few batches per run.
 */
const MAX_BATCHES_PER_RUN = 8
const RUN_LOCK_WINDOW_MS = 4 * 60 * 1000

function nowIso() {
  return new Date().toISOString()
}

function getBaseUrl() {
  const appUrl = process.env.APP_URL?.trim()

  if (!appUrl) {
    throw new Error("Missing APP_URL environment variable")
  }

  return appUrl.replace(/\/$/, "")
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

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }

  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function makeUrl(baseUrl: string, path: string) {
  return `${baseUrl}${path}`
}

function withSearchParams(
  path: string,
  params: Record<string, string | number | boolean | null | undefined>
) {
  const url = new URL(path, "https://internal.local")

  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue
    url.searchParams.set(key, String(value))
  }

  return `${url.pathname}${url.search}`
}

function shouldStopForRuntime(runStartedAtMs: number) {
  const elapsed = Date.now() - runStartedAtMs
  return elapsed >= MAX_PIPELINE_RUNTIME_MS - RUNTIME_SAFETY_BUFFER_MS
}

function isRecentRun(dateString: string | null | undefined, windowMs: number) {
  if (!dateString) return false
  const ts = new Date(dateString).getTime()
  if (Number.isNaN(ts)) return false
  return Date.now() - ts < windowMs
}

function clampScreenBatch(batch: number | null | undefined) {
  return Math.min(
    Math.max(1, batch || DEFAULT_SCREEN_BATCH),
    MAX_SCREEN_BATCH
  )
}

async function runStep(
  baseUrl: string,
  path: string,
  timeoutMs: number = DEFAULT_STEP_TIMEOUT_MS
): Promise<StepResult> {
  const pipelineToken = process.env.PIPELINE_TOKEN

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const url = makeUrl(baseUrl, path)
  const startedAt = Date.now()

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)

  let response: Response

  try {
    response = await fetch(url, {
      method: "GET",
      headers: {
        "x-pipeline-token": pipelineToken,
      },
      cache: "no-store",
      signal: controller.signal,
    })
  } catch (error: any) {
    clearTimeout(timeout)

    const durationMs = Date.now() - startedAt
    const message =
      error?.name === "AbortError"
        ? "The operation was aborted due to timeout"
        : error?.message || "Step request failed"

    return {
      step: path.split("?")[0].split("/").filter(Boolean).slice(-2).join("/"),
      path,
      ok: false,
      status: 599,
      durationMs,
      data: {
        ok: false,
        error: message,
      },
    }
  } finally {
    clearTimeout(timeout)
  }

  const durationMs = Date.now() - startedAt

  let data: unknown = null

  try {
    data = await response.json()
  } catch {
    data = { ok: false, error: "Non-JSON response returned by step" }
  }

  return {
    step: path.split("?")[0].split("/").filter(Boolean).slice(-2).join("/"),
    path,
    ok: response.ok,
    status: response.status,
    durationMs,
    data,
  }
}

async function getPipelineState(supabase: any): Promise<PipelineStateRow> {
  const pipelineStateTable = supabase.from("pipeline_state") as any

  const { data, error } = await pipelineStateTable
    .select("*")
    .eq("job_name", PIPELINE_JOB_NAME)
    .maybeSingle()

  if (error) {
    throw new Error(`Failed to load pipeline state: ${JSON.stringify(error)}`)
  }

  if (data) {
    return data as PipelineStateRow
  }

  const seed = {
    job_name: PIPELINE_JOB_NAME,
    stage: "idle",
    status: "idle",
    screen_start: 0,
    screen_batch: DEFAULT_SCREEN_BATCH,
    screen_total: null,
    screen_next_start: 0,
    cycle_started_at: null,
    cycle_completed_at: null,
    filings_completed_at: null,
    signals_completed_at: null,
    last_run_started_at: null,
    last_run_finished_at: null,
    last_success_at: null,
    last_error: null,
    last_error_at: null,
  }

  const { data: inserted, error: insertError } = await pipelineStateTable
    .upsert(seed, { onConflict: "job_name" })
    .select("*")
    .single()

  if (insertError) {
    throw new Error(`Failed to seed pipeline state: ${insertError.message}`)
  }

  return inserted as PipelineStateRow
}

async function patchPipelineState(
  supabase: any,
  patch: Partial<PipelineStateRow>
): Promise<PipelineStateRow> {
  const pipelineStateTable = supabase.from("pipeline_state") as any

  const { data, error } = await pipelineStateTable
    .update(patch)
    .eq("job_name", PIPELINE_JOB_NAME)
    .select("*")
    .single()

  if (error) {
    throw new Error(`Failed to update pipeline state: ${error.message}`)
  }

  return data as PipelineStateRow
}

async function failPipelineForStep(
  supabase: any,
  results: StepResult[],
  stepResult: StepResult,
  errorMessage: string
) {
  const failedAt = nowIso()

  await patchPipelineState(supabase, {
    stage: "error",
    status: "error",
    last_error: errorMessage,
    last_error_at: failedAt,
    last_run_finished_at: failedAt,
  })

  return NextResponse.json(
    {
      ok: false,
      error: errorMessage,
      failedStep: stepResult.path,
      results,
    },
    { status: 500 }
  )
}

async function checkpointStage(
  supabase: any,
  stage: PipelineStage,
  results: StepResult[],
  message: string
) {
  const checkpointAt = nowIso()

  const updated = await patchPipelineState(supabase, {
    stage,
    status: "idle",
    last_run_finished_at: checkpointAt,
  })

  return NextResponse.json({
    ok: true,
    message,
    stage: updated.stage,
    status: updated.status,
    results,
  })
}

async function checkpointScreening(
  supabase: any,
  nextStart: number,
  batchSize: number,
  results: StepResult[],
  message: string,
  batchesThisRun: number
) {
  const checkpointAt = nowIso()

  const updated = await patchPipelineState(supabase, {
    stage: "screening",
    status: "idle",
    screen_start: nextStart,
    screen_next_start: nextStart,
    screen_batch: batchSize,
    last_run_finished_at: checkpointAt,
  })

  return NextResponse.json({
    ok: true,
    message,
    stage: updated.stage,
    status: updated.status,
    nextStart: updated.screen_next_start,
    batchesThisRun,
    batchSize,
    results,
  })
}

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization")
  const cronSecret = process.env.CRON_SECRET

  if (!cronSecret) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing CRON_SECRET environment variable",
        debug: {
          hasAppUrl: Boolean(process.env.APP_URL),
          hasCronSecret: Boolean(process.env.CRON_SECRET),
          hasPipelineToken: Boolean(process.env.PIPELINE_TOKEN),
          hasSupabaseUrl: Boolean(process.env.NEXT_PUBLIC_SUPABASE_URL),
          hasServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY),
        },
      },
      { status: 500 }
    )
  }

  if (authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      {
        ok: false,
        error: "Unauthorized",
        hasAuthorizationHeader: Boolean(authHeader),
      },
      { status: 401 }
    )
  }

  const runStartedAtMs = Date.now()
  const runStartedIso = nowIso()

  try {
    const baseUrl = getBaseUrl()
    const supabase = getSupabaseAdmin()

    let state = await getPipelineState(supabase)

    if (
      state.status === "running" &&
      isRecentRun(state.last_run_started_at, RUN_LOCK_WINDOW_MS)
    ) {
      return NextResponse.json({
        ok: true,
        message: "Skipped because another pipeline run is already in progress.",
        state: {
          stage: state.stage,
          status: state.status,
          screenStart: state.screen_start,
          screenNextStart: state.screen_next_start,
          screenBatch: state.screen_batch,
          screenTotal: state.screen_total,
          lastRunStartedAt: state.last_run_started_at,
          lastRunFinishedAt: state.last_run_finished_at,
        },
      })
    }

    state = await patchPipelineState(supabase, {
      status: "running",
      last_run_started_at: runStartedIso,
      last_run_finished_at: null,
      last_error: null,
      last_error_at: null,
      stage:
        state.stage === "idle" ||
        state.stage === "complete" ||
        state.stage === "error"
          ? "companies"
          : state.stage,
      cycle_started_at: state.cycle_started_at ?? runStartedIso,
      screen_batch: clampScreenBatch(state.screen_batch),
    })

    const results: StepResult[] = []

    if (
      state.stage === "companies" ||
      state.stage === "idle" ||
      state.stage === "complete" ||
      state.stage === "error"
    ) {
      const companiesResult = await runStep(
        baseUrl,
        "/api/ingest/companies",
        DEFAULT_STEP_TIMEOUT_MS
      )
      results.push(companiesResult)

      if (!companiesResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          companiesResult,
          `Companies step failed: ${String(
            (companiesResult.data as any)?.error || companiesResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "filings",
        status: "running",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        screen_total: null,
        filings_completed_at: null,
        signals_completed_at: null,
        cycle_completed_at: null,
      })
    }

    if (state.stage === "filings") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "filings",
          results,
          "Pipeline checkpointed before filings. Continue on next cron run."
        )
      }

      const filingsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/filings", {
          scope: "all",
          start: 0,
          batch: DEFAULT_FILINGS_BATCH,
          onlyActive: true,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(filingsResult)

      if (!filingsResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          filingsResult,
          `Filings step failed: ${String(
            (filingsResult.data as any)?.error || filingsResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "signals",
        status: "running",
        filings_completed_at: nowIso(),
      })
    }

    if (state.stage === "signals") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "signals",
          results,
          "Pipeline checkpointed before signals. Continue on next cron run."
        )
      }

      const signalsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/signals", {
          limit: DEFAULT_SIGNALS_LIMIT,
          lookbackDays: DEFAULT_SIGNALS_LOOKBACK_DAYS,
          runRetention: false,
          includeCounts: false,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(signalsResult)

      if (!signalsResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          signalsResult,
          `Signals step failed: ${String(
            (signalsResult.data as any)?.error || signalsResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "filing_signals",
        status: "running",
        signals_completed_at: nowIso(),
      })
    }

    if (state.stage === "filing_signals") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "filing_signals",
          results,
          "Pipeline checkpointed before filing signals. Continue on next cron run."
        )
      }

      const filingSignalsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/filing-signals", {
          limit: DEFAULT_FILING_SIGNALS_LIMIT,
          lookbackDays: DEFAULT_FILING_SIGNALS_LOOKBACK_DAYS,
          runRetention: false,
          includeCounts: false,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(filingSignalsResult)

      if (!filingSignalsResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          filingSignalsResult,
          `Filing signals step failed: ${String(
            (filingSignalsResult.data as any)?.error || filingSignalsResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "eligible_universe",
        status: "running",
      })
    }

    if (state.stage === "eligible_universe") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "eligible_universe",
          results,
          "Pipeline checkpointed before eligible universe build. Continue on next cron run."
        )
      }

      const eligibleUniverseResult = await runStep(
        baseUrl,
        withSearchParams("/api/screen/eligible-universe", {
          lookbackDays: DEFAULT_ELIGIBLE_UNIVERSE_LOOKBACK_DAYS,
          onlyActive: true,
          includeCounts: true,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(eligibleUniverseResult)

      if (!eligibleUniverseResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          eligibleUniverseResult,
          `Eligible universe step failed: ${String(
            (eligibleUniverseResult.data as any)?.error ||
              eligibleUniverseResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "screening",
        status: "running",
        screen_start: 0,
        screen_next_start: 0,
        screen_total:
          typeof (eligibleUniverseResult.data as any)?.eligibleCount === "number"
            ? Number((eligibleUniverseResult.data as any).eligibleCount)
            : null,
      })
    }

    if (state.stage === "screening") {
      let nextStart = state.screen_next_start ?? state.screen_start ?? 0
      const batchSize = clampScreenBatch(
        parseInteger(String(state.screen_batch), DEFAULT_SCREEN_BATCH)
      )

      let screeningComplete = false
      let batchesThisRun = 0

      while (!screeningComplete) {
        if (shouldStopForRuntime(runStartedAtMs)) {
          return await checkpointScreening(
            supabase,
            nextStart,
            batchSize,
            results,
            "Pipeline checkpointed during screening because runtime was nearly exhausted.",
            batchesThisRun
          )
        }

        if (batchesThisRun >= MAX_BATCHES_PER_RUN) {
          return await checkpointScreening(
            supabase,
            nextStart,
            batchSize,
            results,
            "Batch limit reached for this screening run.",
            batchesThisRun
          )
        }

        const screenPath = withSearchParams("/api/screen/candidates", {
          universe: "eligible",
          start: nextStart,
          batch: batchSize,
          onlyActive: true,
          includeResults: false,
          includeCounts: false,
          runRetention: false,
        })

        const screenResult = await runStep(
          baseUrl,
          screenPath,
          DEFAULT_STEP_TIMEOUT_MS
        )

        results.push(screenResult)
        batchesThisRun += 1

        if (!screenResult.ok) {
          return await failPipelineForStep(
            supabase,
            results,
            screenResult,
            `Screening step failed at start=${nextStart}: ${String(
              (screenResult.data as any)?.error || screenResult.status
            )}`
          )
        }

        const screenData = screenResult.data as any
        const returnedNextStart =
          typeof screenData?.nextStart === "number" ? screenData.nextStart : null
        const returnedTotalCompanies =
          typeof screenData?.totalCompanies === "number"
            ? screenData.totalCompanies
            : state.screen_total

        state = await patchPipelineState(supabase, {
          stage: returnedNextStart === null ? "finalize_candidates" : "screening",
          status: "running",
          screen_start: nextStart,
          screen_batch: batchSize,
          screen_total: returnedTotalCompanies,
          screen_next_start: returnedNextStart,
        })

        if (returnedNextStart === null) {
          screeningComplete = true
        } else {
          nextStart = returnedNextStart
        }
      }
    }

    if (state.stage === "finalize_candidates") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "finalize_candidates",
          results,
          "Pipeline checkpointed before candidate finalization. Continue on next cron run."
        )
      }

      const finalizeResult = await runStep(
        baseUrl,
        "/api/screen/finalize-candidates",
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(finalizeResult)

      if (!finalizeResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          finalizeResult,
          `Finalize candidates step failed: ${String(
            (finalizeResult.data as any)?.error || finalizeResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "ptrs",
        status: "running",
      })
    }

    if (state.stage === "ptrs") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "ptrs",
          results,
          "Pipeline checkpointed before PTR ingest. Continue on next cron run."
        )
      }

      const ptrsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/ptrs", {
          scope: "eligible",
          start: 0,
          batch: DEFAULT_PTRS_BATCH,
          onlyActive: true,
          includeCounts: false,
          limit: 10,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(ptrsResult)

      if (!ptrsResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          ptrsResult,
          `PTR step failed: ${String(
            (ptrsResult.data as any)?.error || ptrsResult.status
          )}`
        )
      }

      state = await patchPipelineState(supabase, {
        stage: "ticker_scores",
        status: "running",
      })
    }

    if (state.stage === "ticker_scores") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointStage(
          supabase,
          "ticker_scores",
          results,
          "Pipeline checkpointed before ticker score rebuild. Continue on next cron run."
        )
      }

      const tickerScoresResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/ticker-scores", {
          limit: DEFAULT_TICKER_SCORES_LIMIT,
          lookbackDays: DEFAULT_FILING_SIGNALS_LOOKBACK_DAYS,
          runRetention: false,
          includeCounts: false,
        }),
        DEFAULT_STEP_TIMEOUT_MS
      )

      results.push(tickerScoresResult)

      if (!tickerScoresResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          tickerScoresResult,
          `Ticker scores step failed: ${String(
            (tickerScoresResult.data as any)?.error || tickerScoresResult.status
          )}`
        )
      }

      const completedAt = nowIso()

      state = await patchPipelineState(supabase, {
        stage: "complete",
        status: "success",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        signals_completed_at: completedAt,
        cycle_completed_at: completedAt,
        last_success_at: completedAt,
        last_run_finished_at: completedAt,
      })
    }

    const responseState = {
      stage: state.stage,
      status: state.status,
      screenStart: state.screen_start,
      screenNextStart: state.screen_next_start,
      screenBatch: state.screen_batch,
      screenTotal: state.screen_total,
      lastSuccessAt: state.last_success_at,
    }

    if (state.stage === "complete") {
      await patchPipelineState(supabase, {
        stage: "idle",
        status: "idle",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        cycle_started_at: null,
      })

      return NextResponse.json({
        ok: true,
        message: "Pipeline cycle completed successfully",
        state: responseState,
        results,
      })
    }

    return NextResponse.json({
      ok: true,
      message: "Pipeline run completed",
      state: responseState,
      results,
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown pipeline error"

    try {
      const supabase = getSupabaseAdmin()
      const currentState = await getPipelineState(supabase)

      if (
        currentState.last_error !== message ||
        currentState.status !== "error"
      ) {
        const failedAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: message,
          last_error_at: failedAt,
          last_run_finished_at: failedAt,
        })
      }
    } catch {
      // ignore secondary failure
    }

    return NextResponse.json(
      {
        ok: false,
        error: message,
        debug: {
          hasAppUrl: Boolean(process.env.APP_URL),
          hasCronSecret: Boolean(process.env.CRON_SECRET),
          hasPipelineToken: Boolean(process.env.PIPELINE_TOKEN),
          hasSupabaseUrl: Boolean(process.env.NEXT_PUBLIC_SUPABASE_URL),
          hasServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY),
        },
      },
      { status: 500 }
    )
  }
}