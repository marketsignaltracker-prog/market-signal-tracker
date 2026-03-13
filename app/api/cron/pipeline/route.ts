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

/**
 * Throughput tuning:
 * - Bigger screening batches
 * - More batches per run
 * - Bigger downstream batches
 *
 * Tune these based on actual DB / API capacity.
 */
const DEFAULT_SCREEN_BATCH = 500
const MAX_SCREEN_BATCH = 1000

const DEFAULT_FILINGS_BATCH = 2000
const DEFAULT_SIGNALS_LIMIT = 5000
const DEFAULT_SIGNALS_LOOKBACK_DAYS = 30

/**
 * Leave enough room under the platform max duration so we can checkpoint cleanly.
 */
const MAX_PIPELINE_RUNTIME_MS = 285_000
const RUNTIME_SAFETY_BUFFER_MS = 12_000

/**
 * Old value was 2, which is the main reason this can take hours.
 * 8 to 12 is a much more practical starting point.
 */
const MAX_BATCHES_PER_RUN = 10

/**
 * Checkpoint less often to reduce Supabase writes.
 * Worst case, a crash replays up to CHECKPOINT_EVERY_N_BATCHES - 1 batches.
 */
const CHECKPOINT_EVERY_N_BATCHES = 3

const RUN_LOCK_WINDOW_MS = 4 * 60 * 1000
const STEP_TIMEOUT_MS = 120_000

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

  return createClient(supabaseUrl, serviceRoleKey)
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

  if (value.trim() === "") {
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

function getStepName(path: string) {
  return path.split("?")[0].split("/").filter(Boolean).slice(-2).join("/")
}

async function runStep(baseUrl: string, path: string): Promise<StepResult> {
  const pipelineToken = process.env.PIPELINE_TOKEN

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const url = makeUrl(baseUrl, path)
  const startedAt = Date.now()

  let response: Response

  try {
    response = await fetch(url, {
      method: "GET",
      headers: {
        "x-pipeline-token": pipelineToken,
      },
      cache: "no-store",
      signal: AbortSignal.timeout(STEP_TIMEOUT_MS),
    })
  } catch (error) {
    const durationMs = Date.now() - startedAt

    return {
      step: getStepName(path),
      path,
      ok: false,
      status: 599,
      durationMs,
      data: {
        ok: false,
        error:
          error instanceof Error ? error.message : "Unknown fetch error in step",
      },
    }
  }

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

async function tryAcquireRunLock(
  supabase: any,
  currentState: PipelineStateRow,
  runStartedIso: string
): Promise<{ acquired: boolean; state: PipelineStateRow }> {
  const pipelineStateTable = supabase.from("pipeline_state") as any

  if (
    currentState.status === "running" &&
    isRecentRun(currentState.last_run_started_at, RUN_LOCK_WINDOW_MS)
  ) {
    return { acquired: false, state: currentState }
  }

  const nextStage: PipelineStage =
    currentState.stage === "idle" ||
    currentState.stage === "complete" ||
    currentState.stage === "error"
      ? "companies"
      : currentState.stage

  const { data, error } = await pipelineStateTable
    .update({
      status: "running",
      stage: nextStage,
      last_run_started_at: runStartedIso,
      last_run_finished_at: null,
      last_error: null,
      last_error_at: null,
      cycle_started_at: currentState.cycle_started_at ?? runStartedIso,
      screen_batch: clampScreenBatch(currentState.screen_batch),
    })
    .eq("job_name", PIPELINE_JOB_NAME)
    .or(
      [
        "status.neq.running",
        `last_run_started_at.lt.${new Date(Date.now() - RUN_LOCK_WINDOW_MS).toISOString()}`,
        "last_run_started_at.is.null",
      ].join(",")
    )
    .select("*")
    .maybeSingle()

  if (error) {
    throw new Error(`Failed to acquire pipeline lock: ${error.message}`)
  }

  if (!data) {
    const freshState = await getPipelineState(supabase)
    return { acquired: false, state: freshState }
  }

  return { acquired: true, state: data as PipelineStateRow }
}

async function checkpointScreeningState(
  supabase: any,
  nextStart: number,
  batchSize: number
) {
  return patchPipelineState(supabase, {
    stage: "screening",
    status: "running",
    screen_start: nextStart,
    screen_next_start: nextStart,
    screen_batch: batchSize,
    last_run_finished_at: nowIso(),
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

  const startedAtMs = Date.now()
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
      state.stage === "companies" ||
      state.stage === "idle" ||
      state.stage === "complete" ||
      state.stage === "error"
    ) {
      const companiesResult = await runStep(baseUrl, "/api/ingest/companies")
      results.push(companiesResult)

      if (!companiesResult.ok) {
        const failedAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Companies step failed: ${String(
            (companiesResult.data as any)?.error || companiesResult.status
          )}`,
          last_error_at: failedAt,
          last_run_finished_at: failedAt,
        })

        return NextResponse.json(
          {
            ok: false,
            error: "Pipeline stopped because the companies step failed",
            failedStep: companiesResult.path,
            results,
          },
          { status: 500 }
        )
      }

      const transitionAt = nowIso()

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
        cycle_started_at: state.cycle_started_at ?? transitionAt,
      })
    }

    if (state.stage === "screening") {
      let nextStart = state.screen_next_start ?? state.screen_start ?? 0
      const batchSize = clampScreenBatch(
        parseInteger(state.screen_batch, DEFAULT_SCREEN_BATCH)
      )

      let screeningComplete = false
      let batchesThisRun = 0

      while (!screeningComplete) {
        if (shouldStopForRuntime(startedAtMs)) {
          const updated = await checkpointScreeningState(
            supabase,
            nextStart,
            batchSize
          )

          return NextResponse.json({
            ok: true,
            message:
              "Pipeline checkpointed during screening because runtime was nearly exhausted.",
            stage: updated.stage,
            status: updated.status,
            nextStart: updated.screen_next_start,
            batchesThisRun,
            batchSize,
            results,
          })
        }

        if (batchesThisRun >= MAX_BATCHES_PER_RUN) {
          const updated = await checkpointScreeningState(
            supabase,
            nextStart,
            batchSize
          )

          return NextResponse.json({
            ok: true,
            message: "Batch limit reached for this cron run.",
            stage: updated.stage,
            status: updated.status,
            nextStart: updated.screen_next_start,
            batchesThisRun,
            batchSize,
            results,
          })
        }

        const screenPath = withSearchParams("/api/screen/candidates", {
          start: nextStart,
          batch: batchSize,
          onlyActive: true,
        })

        const screenResult = await runStep(baseUrl, screenPath)
        results.push(screenResult)
        batchesThisRun += 1

        if (!screenResult.ok) {
          const failedAt = nowIso()

          await patchPipelineState(supabase, {
            stage: "error",
            status: "error",
            screen_start: nextStart,
            screen_next_start: nextStart,
            screen_batch: batchSize,
            last_error: `Screening step failed at start=${nextStart}: ${String(
              (screenResult.data as any)?.error || screenResult.status
            )}`,
            last_error_at: failedAt,
            last_run_finished_at: failedAt,
          })

          return NextResponse.json(
            {
              ok: false,
              error: "Pipeline stopped because the screening step failed",
              failedStep: screenResult.path,
              results,
            },
            { status: 500 }
          )
        }

        const screenData = screenResult.data as any
        const returnedNextStart =
          typeof screenData?.nextStart === "number" ? screenData.nextStart : null
        const returnedTotalCompanies =
          typeof screenData?.totalCompanies === "number"
            ? screenData.totalCompanies
            : state.screen_total

        const shouldCheckpointMidLoop =
          batchesThisRun % CHECKPOINT_EVERY_N_BATCHES === 0 &&
          returnedNextStart !== null

        if (returnedNextStart === null) {
          state = await patchPipelineState(supabase, {
            stage: "filings",
            status: "running",
            screen_start: nextStart,
            screen_batch: batchSize,
            screen_total: returnedTotalCompanies,
            screen_next_start: null,
          })

          screeningComplete = true
        } else {
          nextStart = returnedNextStart

          if (shouldCheckpointMidLoop) {
            state = await patchPipelineState(supabase, {
              stage: "screening",
              status: "running",
              screen_start: nextStart,
              screen_batch: batchSize,
              screen_total: returnedTotalCompanies,
              screen_next_start: nextStart,
            })
          }
        }
      }
    }

    if (state.stage === "filings") {
      if (shouldStopForRuntime(startedAtMs)) {
        const checkpointAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "filings",
          status: "running",
          last_run_finished_at: checkpointAt,
        })

        return NextResponse.json({
          ok: true,
          message: "Pipeline checkpointed before filings. Continue on next cron run.",
          stage: "filings",
          results,
        })
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
        const failedAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Filings step failed: ${String(
            (filingsResult.data as any)?.error || filingsResult.status
          )}`,
          last_error_at: failedAt,
          last_run_finished_at: failedAt,
        })

        return NextResponse.json(
          {
            ok: false,
            error: "Pipeline stopped because the filings step failed",
            failedStep: filingsResult.path,
            results,
          },
          { status: 500 }
        )
      }

      const completedAt = nowIso()

      state = await patchPipelineState(supabase, {
        stage: "signals",
        status: "running",
        filings_completed_at: completedAt,
      })
    }

    if (state.stage === "signals") {
      if (shouldStopForRuntime(startedAtMs)) {
        const checkpointAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "signals",
          status: "running",
          last_run_finished_at: checkpointAt,
        })

        return NextResponse.json({
          ok: true,
          message: "Pipeline checkpointed before signals. Continue on next cron run.",
          stage: "signals",
          results,
        })
      }

      const signalsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/signals", {
          limit: DEFAULT_SIGNALS_LIMIT,
          lookbackDays: DEFAULT_SIGNALS_LOOKBACK_DAYS,
        })
      )

      results.push(signalsResult)

      if (!signalsResult.ok) {
        const failedAt = nowIso()

        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Signals step failed: ${String(
            (signalsResult.data as any)?.error || signalsResult.status
          )}`,
          last_error_at: failedAt,
          last_run_finished_at: failedAt,
        })

        return NextResponse.json(
          {
            ok: false,
            error: "Pipeline stopped because the signals step failed",
            failedStep: signalsResult.path,
            results,
          },
          { status: 500 }
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

    await patchPipelineState(supabase, {
      last_run_finished_at: nowIso(),
    })

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

      if (currentState.last_error !== message || currentState.status !== "error") {
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