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
  | "screening"
  | "filings"
  | "signals"
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
  updated_at: string
}

const PIPELINE_JOB_NAME = "market_signal_pipeline"

const DEFAULT_SCREEN_BATCH = 100
const MAX_SCREEN_BATCH = 100
const MIN_SCREEN_BATCH = 25

const DEFAULT_FILINGS_BATCH = 1000
const DEFAULT_SIGNALS_LIMIT = 100
const DEFAULT_SIGNALS_LOOKBACK_DAYS = 30

const MAX_PIPELINE_RUNTIME_MS = 250_000
const RUNTIME_SAFETY_BUFFER_MS = 15_000

const MAX_BATCHES_PER_RUN = 12
const SCREENING_CHECKPOINT_EVERY = 3
const RUN_LOCK_WINDOW_MS = 60 * 1000

const DEFAULT_STEP_TIMEOUT_MS = 60_000
const SCREENING_STEP_TIMEOUT_MS = 90_000

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

function parseInteger(
  value: string | number | null | undefined,
  fallback: number
) {
  if (value === null || value === undefined) {
    return fallback
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? Math.trunc(value) : fallback
  }

  const trimmed = value.trim()
  if (!trimmed) {
    return fallback
  }

  const parsed = Number.parseInt(trimmed, 10)
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
    Math.max(MIN_SCREEN_BATCH, batch || DEFAULT_SCREEN_BATCH),
    MAX_SCREEN_BATCH
  )
}

function reduceScreenBatch(batch: number) {
  return Math.max(MIN_SCREEN_BATCH, Math.floor(batch / 2))
}

function getStepName(path: string) {
  return path.split("?")[0].split("/").filter(Boolean).slice(-2).join("/")
}

function isTimeoutLikeError(message: unknown) {
  if (typeof message !== "string") return false
  const normalized = message.toLowerCase()
  return (
    normalized.includes("aborted due to timeout") ||
    normalized.includes("timeout") ||
    normalized.includes("timed out") ||
    normalized.includes("function_invocation_timeout")
  )
}

async function runStep(
  baseUrl: string,
  path: string,
  timeoutMs = DEFAULT_STEP_TIMEOUT_MS
): Promise<StepResult> {
  const pipelineToken = process.env.PIPELINE_TOKEN

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const startedAt = Date.now()
  const url = makeUrl(baseUrl, path)

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "x-pipeline-token": pipelineToken,
      },
      cache: "no-store",
      signal: AbortSignal.timeout(timeoutMs),
    })

    const durationMs = Date.now() - startedAt

    let data: unknown = null

    try {
      data = await response.json()
    } catch {
      data = { ok: false, error: "Non-JSON response returned by step" }
    }

    return {
      step: getStepName(path),
      path,
      ok: response.ok,
      status: response.status,
      durationMs,
      data,
    }
  } catch (error) {
    return {
      step: getStepName(path),
      path,
      ok: false,
      status: 599,
      durationMs: Date.now() - startedAt,
      data: {
        ok: false,
        error:
          error instanceof Error ? error.message : "Unknown step fetch error",
      },
    }
  }
}

async function getPipelineState(supabase: any): Promise<PipelineStateRow> {
  const { data, error } = await supabase
    .from("pipeline_state")
    .select("*")
    .eq("job_name", PIPELINE_JOB_NAME)
    .limit(1)
    .maybeSingle()

  if (error) {
    throw new Error(`Failed to load pipeline state: ${error.message}`)
  }

  if (data) {
    return data as PipelineStateRow
  }

  const seed = {
    job_name: PIPELINE_JOB_NAME,
    stage: "idle" as PipelineStage,
    status: "idle" as PipelineStatus,
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

  const { error: upsertError } = await supabase
    .from("pipeline_state")
    .upsert(seed, { onConflict: "job_name" })

  if (upsertError) {
    throw new Error(`Failed to seed pipeline state: ${upsertError.message}`)
  }

  const { data: inserted, error: fetchInsertedError } = await supabase
    .from("pipeline_state")
    .select("*")
    .eq("job_name", PIPELINE_JOB_NAME)
    .limit(1)
    .single()

  if (fetchInsertedError) {
    throw new Error(
      `Failed to reload seeded pipeline state: ${fetchInsertedError.message}`
    )
  }

  return inserted as PipelineStateRow
}

async function patchPipelineState(
  supabase: any,
  patch: Partial<PipelineStateRow>
): Promise<PipelineStateRow> {
  const { error: updateError } = await supabase
    .from("pipeline_state")
    .update(patch)
    .eq("job_name", PIPELINE_JOB_NAME)

  if (updateError) {
    throw new Error(`Failed to update pipeline state: ${updateError.message}`)
  }

  const { data, error: readError } = await supabase
    .from("pipeline_state")
    .select("*")
    .eq("job_name", PIPELINE_JOB_NAME)
    .limit(1)
    .single()

  if (readError) {
    throw new Error(`Failed to reload pipeline state: ${readError.message}`)
  }

  return data as PipelineStateRow
}

async function tryAcquireRunLock(
  supabase: any,
  currentState: PipelineStateRow,
  runStartedIso: string
): Promise<{ acquired: boolean; state: PipelineStateRow }> {
  const runStartedRecently = isRecentRun(
    currentState.last_run_started_at,
    RUN_LOCK_WINDOW_MS
  )

  const finishedAfterStart =
    currentState.last_run_started_at &&
    currentState.last_run_finished_at &&
    new Date(currentState.last_run_finished_at).getTime() >=
      new Date(currentState.last_run_started_at).getTime()

  if (
    currentState.status === "running" &&
    runStartedRecently &&
    !finishedAfterStart
  ) {
    return { acquired: false, state: currentState }
  }

  const nextStage: PipelineStage =
    currentState.stage === "idle" ||
    currentState.stage === "complete" ||
    currentState.stage === "error"
      ? "companies"
      : currentState.stage

  const updatedState = await patchPipelineState(supabase, {
    status: "running",
    stage: nextStage,
    last_run_started_at: runStartedIso,
    last_run_finished_at: null,
    last_error: null,
    last_error_at: null,
    cycle_started_at: currentState.cycle_started_at ?? runStartedIso,
    screen_batch: clampScreenBatch(currentState.screen_batch),
  })

  return { acquired: true, state: updatedState }
}

async function failPipelineForStep(
  supabase: any,
  results: StepResult[],
  stepResult: StepResult,
  message: string,
  patch?: Partial<PipelineStateRow>
): Promise<NextResponse> {
  const failedAt = nowIso()

  await patchPipelineState(supabase, {
    stage: "error",
    status: "error",
    last_error: message,
    last_error_at: failedAt,
    last_run_finished_at: failedAt,
    ...patch,
  })

  return NextResponse.json(
    {
      ok: false,
      error: message,
      failedStep: stepResult.path,
      results,
    },
    { status: 500 }
  )
}

async function checkpointPipeline(
  supabase: any,
  patch: Partial<PipelineStateRow>,
  body: Record<string, unknown>
) {
  const state = await patchPipelineState(supabase, {
    ...patch,
    last_run_finished_at: nowIso(),
  })

  return NextResponse.json({
    ok: true,
    ...body,
    state: {
      stage: state.stage,
      status: state.status,
      screenStart: state.screen_start,
      screenNextStart: state.screen_next_start,
      screenBatch: state.screen_batch,
      screenTotal: state.screen_total,
      lastSuccessAt: state.last_success_at,
    },
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

    const currentState = await getPipelineState(supabase)
    const lock = await tryAcquireRunLock(supabase, currentState, runStartedIso)

    if (!lock.acquired) {
      return NextResponse.json({
        ok: true,
        message: "Skipped because another pipeline run is already in progress.",
        state: {
          stage: lock.state.stage,
          status: lock.state.status,
          screenStart: lock.state.screen_start,
          screenNextStart: lock.state.screen_next_start,
          screenBatch: lock.state.screen_batch,
          screenTotal: lock.state.screen_total,
          lastRunStartedAt: lock.state.last_run_started_at,
          lastRunFinishedAt: lock.state.last_run_finished_at,
        },
      })
    }

    let state = lock.state
    const results: StepResult[] = []

    if (
      state.stage === "idle" ||
      state.stage === "complete" ||
      state.stage === "error" ||
      state.stage === "companies"
    ) {
      const companiesResult = await runStep(baseUrl, "/api/ingest/companies")
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
        stage: "screening",
        status: "running",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        screen_total: null,
        filings_completed_at: null,
        signals_completed_at: null,
        cycle_completed_at: null,
        cycle_started_at: state.cycle_started_at ?? nowIso(),
      })
    }

    if (state.stage === "screening") {
      let nextStart = state.screen_next_start ?? state.screen_start ?? 0
      let batchSize = clampScreenBatch(
        parseInteger(state.screen_batch, DEFAULT_SCREEN_BATCH)
      )

      let batchesThisRun = 0

      while (true) {
        if (shouldStopForRuntime(runStartedAtMs)) {
          return await checkpointPipeline(
            supabase,
            {
              stage: "screening",
              status: "running",
              screen_start: nextStart,
              screen_next_start: nextStart,
              screen_batch: batchSize,
            },
            {
              message:
                "Pipeline checkpointed during screening because runtime was nearly exhausted.",
              results,
            }
          )
        }

        if (batchesThisRun >= MAX_BATCHES_PER_RUN) {
          return await checkpointPipeline(
            supabase,
            {
              stage: "screening",
              status: "running",
              screen_start: nextStart,
              screen_next_start: nextStart,
              screen_batch: batchSize,
            },
            {
              message: "Batch limit reached for this run.",
              results,
            }
          )
        }

        const screenPath = withSearchParams("/api/screen/candidates", {
          start: nextStart,
          batch: batchSize,
          onlyActive: true,
        })

        const screenResult = await runStep(
          baseUrl,
          screenPath,
          SCREENING_STEP_TIMEOUT_MS
        )

        results.push(screenResult)
        batchesThisRun += 1

        if (!screenResult.ok) {
          const errorText = String(
            (screenResult.data as any)?.error || screenResult.status
          )

          const nextSuggestedBatch = isTimeoutLikeError(errorText)
            ? reduceScreenBatch(batchSize)
            : batchSize

          return await failPipelineForStep(
            supabase,
            results,
            screenResult,
            `Screening step failed at start=${nextStart}: ${errorText}`,
            {
              screen_start: nextStart,
              screen_next_start: nextStart,
              screen_batch: nextSuggestedBatch,
            }
          )
        }

        const screenData = screenResult.data as any
        const returnedNextStart =
          typeof screenData?.nextStart === "number" ? screenData.nextStart : null
        const returnedTotalCompanies =
          typeof screenData?.totalCompanies === "number"
            ? screenData.totalCompanies
            : state.screen_total

        if (returnedNextStart === null) {
          state = await patchPipelineState(supabase, {
            stage: "filings",
            status: "running",
            screen_start: nextStart,
            screen_next_start: null,
            screen_batch: batchSize,
            screen_total: returnedTotalCompanies,
          })

          break
        }

        nextStart = returnedNextStart

        if (batchesThisRun % SCREENING_CHECKPOINT_EVERY === 0) {
          state = await patchPipelineState(supabase, {
            stage: "screening",
            status: "running",
            screen_start: nextStart,
            screen_next_start: nextStart,
            screen_batch: batchSize,
            screen_total: returnedTotalCompanies,
          })
        }
      }
    }

    if (state.stage === "filings") {
      if (shouldStopForRuntime(runStartedAtMs)) {
        return await checkpointPipeline(
          supabase,
          {
            stage: "filings",
            status: "running",
          },
          {
            message: "Pipeline checkpointed before filings. Continue next run.",
            results,
          }
        )
      }

      const filingsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/filings", {
          scope: "candidates",
          start: 0,
          batch: DEFAULT_FILINGS_BATCH,
        })
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
        return await checkpointPipeline(
          supabase,
          {
            stage: "signals",
            status: "running",
          },
          {
            message: "Pipeline checkpointed before signals. Continue next run.",
            results,
          }
        )
      }

const signalsResult = await runStep(
  baseUrl,
  withSearchParams("/api/ingest/signals", {
    limit: DEFAULT_SIGNALS_LIMIT,
    lookbackDays: DEFAULT_SIGNALS_LOOKBACK_DAYS,
    rebuildTickerScores: true,
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
        state: {
          stage: "complete",
          status: "success",
          screenStart: 0,
          screenNextStart: 0,
          screenBatch: state.screen_batch,
          screenTotal: state.screen_total,
          lastSuccessAt: state.last_success_at,
        },
        results,
      })
    }

    await patchPipelineState(supabase, {
      last_run_finished_at: nowIso(),
    })

    return NextResponse.json({
      ok: true,
      message: "Pipeline run completed",
      state: {
        stage: state.stage,
        status: state.status,
        screenStart: state.screen_start,
        screenNextStart: state.screen_next_start,
        screenBatch: state.screen_batch,
        screenTotal: state.screen_total,
        lastSuccessAt: state.last_success_at,
      },
      results,
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown pipeline error"

    try {
      const supabase = getSupabaseAdmin()
      await patchPipelineState(supabase, {
        stage: "error",
        status: "error",
        last_error: message,
        last_error_at: nowIso(),
        last_run_finished_at: nowIso(),
      })
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