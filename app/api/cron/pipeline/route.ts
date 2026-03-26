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
  | "ptrs"
  | "screening"
  | "eligible_universe"
  | "signals"
  | "ticker_scores"
  | "finalize_candidates"
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
  companies_completed_at?: string | null
  filings_completed_at: string | null
  signals_completed_at: string | null
  ticker_scores_completed_at?: string | null
  last_run_started_at: string | null
  last_run_finished_at: string | null
  last_success_at: string | null
  last_error: string | null
  last_error_at: string | null
  updated_at?: string
}

const PIPELINE_JOB_NAME = "market_signal_pipeline"

const DEFAULT_SCREEN_BATCH = 500
const MAX_SCREEN_BATCH = 800

const DEFAULT_FILINGS_BATCH = 100
const DEFAULT_FILINGS_LOOKBACK_DAYS = 60
const DEFAULT_PTRS_BATCH = 100
const DEFAULT_PTRS_LOOKBACK_DAYS = 60
const DEFAULT_ELIGIBLE_UNIVERSE_LOOKBACK_DAYS = 30

const DEFAULT_SIGNALS_LIMIT = 1000
const DEFAULT_SIGNALS_LOOKBACK_DAYS = 31
const DEFAULT_SIGNALS_MIN_STRENGTH = 35

const DEFAULT_TICKER_SCORES_LIMIT = 200
const DEFAULT_TICKER_SCORES_PTR_LOOKBACK_DAYS = 60
const DEFAULT_TICKER_SCORES_PTR_RECENT_DAYS = 14
const DEFAULT_TICKER_SCORES_MIN_COMBINED_SCORE = 68

const DEFAULT_FINAL_CANDIDATES_LIMIT = 30
const DEFAULT_FINAL_CANDIDATES_TARGET_MIN = 12

const DEFAULT_STEP_TIMEOUT_MS = 280_000
const FILINGS_STEP_TIMEOUT_MS = 280_000
const PTRS_STEP_TIMEOUT_MS = 280_000
const SEED_ELIGIBLE_STEP_TIMEOUT_MS = 35_000
const SCREENING_STEP_TIMEOUT_MS = 280_000  // screening function has 300s max
const RUN_LOCK_WINDOW_MS = 6 * 60 * 1000
const COMPANIES_REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000
const CHAIN_BUDGET_MS = 250_000  // chain multiple batches within one 300s function call

function nowIso() {
  return new Date().toISOString()
}

function getBaseUrl() {
  // Priority for internal pipeline calls:
  // 1. PIPELINE_BASE_URL — explicit override for self-calls
  // 2. Vercel project production URL (project-level alias, bypasses both
  //    Cloudflare bot protection on custom domains AND Vercel deployment auth
  //    on per-deployment URLs)
  // 3. APP_URL fallback
  const candidates = [
    process.env.PIPELINE_BASE_URL?.trim(),
    process.env.VERCEL_PROJECT_PRODUCTION_URL?.trim(),
    process.env.APP_URL?.trim(),
  ].filter(Boolean)

  for (const raw of candidates) {
    if (!raw) continue
    const url = raw.startsWith("http") ? raw : `https://${raw}`
    return url.replace(/\/$/, "")
  }

  throw new Error("Missing PIPELINE_BASE_URL, VERCEL_PROJECT_PRODUCTION_URL, and APP_URL environment variables")
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

function getStageCursor(state: PipelineStateRow) {
  return state.screen_next_start ?? state.screen_start ?? 0
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

  const headers: Record<string, string> = {
    "x-pipeline-token": pipelineToken,
  }

  // Bypass Vercel Deployment Protection for internal pipeline calls
  const bypassSecret = process.env.VERCEL_AUTOMATION_BYPASS_SECRET?.trim()
  if (bypassSecret) {
    headers["x-vercel-protection-bypass"] = bypassSecret
  }

  try {
    response = await fetch(url, {
      method: "GET",
      headers,
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

  let rawText = ""
  try {
    rawText = await response.text()
  } catch { /* ignore */ }

  try {
    data = rawText ? JSON.parse(rawText) : {}
  } catch {
    const snippet = rawText.slice(0, 300)
    data = {
      ok: false,
      error: `Non-JSON response returned by step (status=${response.status}, url=${url}, snippet=${snippet})`,
    }
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
    companies_completed_at: null,
    filings_completed_at: null,
    signals_completed_at: null,
    ticker_scores_completed_at: null,
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

function extractStepCount(data: any, possibleKeys: string[]) {
  for (const key of possibleKeys) {
    if (typeof data?.[key] === "number") return Number(data[key])
  }
  return null
}

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization")
  const cronSecret = process.env.CRON_SECRET

  if (!cronSecret) {
    return NextResponse.json(
      {
        ok: false,
        error: "Missing CRON_SECRET environment variable",
      },
      { status: 500 }
    )
  }

  if (authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      {
        ok: false,
        error: "Unauthorized",
      },
      { status: 401 }
    )
  }

  const runStartedIso = nowIso()
  const runStartMs = Date.now()

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
      const companiesRecentlyRun = isRecentRun(
        state.companies_completed_at,
        COMPANIES_REFRESH_INTERVAL_MS
      )

      if (companiesRecentlyRun) {
        state = await patchPipelineState(supabase, {
          stage: "screening",
          status: "idle",
          screen_start: 0,
          screen_next_start: 0,
          screen_batch: clampScreenBatch(state.screen_batch),
          screen_total: null,
          filings_completed_at: null,
          signals_completed_at: null,
          ticker_scores_completed_at: null,
          cycle_completed_at: null,
          last_run_finished_at: nowIso(),
        })

        return NextResponse.json({
          ok: true,
          message: "Skipped companies step (ran recently). Moving to screening.",
          nextStage: state.stage,
          results,
        })
      }

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
        stage: "screening",
        status: "idle",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        screen_total: null,
        companies_completed_at: nowIso(),
        filings_completed_at: null,
        signals_completed_at: null,
        ticker_scores_completed_at: null,
        cycle_completed_at: null,
        last_run_finished_at: nowIso(),
      })

      return NextResponse.json({
        ok: true,
        message: "Completed companies step.",
        nextStage: state.stage,
        results,
      })
    }

    if (state.stage === "screening") {
      const screeningStart = getStageCursor(state)
      const batchSize = clampScreenBatch(
        parseInteger(String(state.screen_batch), DEFAULT_SCREEN_BATCH)
      )

      const screenResult = await runStep(
        baseUrl,
        withSearchParams("/api/screen/candidates", {
          universe: "all",
          start: screeningStart,
          batch: batchSize,
          onlyActive: true,
          includeResults: false,
          includeCounts: false,
          runRetention: false,
        }),
        SCREENING_STEP_TIMEOUT_MS
      )

      results.push(screenResult)

      if (!screenResult.ok) {
        return await failPipelineForStep(
          supabase,
          results,
          screenResult,
          `Screening step failed at start=${screeningStart}: ${String(
            (screenResult.data as any)?.error || screenResult.status
          )} | samples: ${JSON.stringify((screenResult.data as any)?.debug?.sampleErrors ?? [])}`
        )
      }

      const screenData = screenResult.data as any
      const returnedNextStart =
        typeof screenData?.nextStart === "number"
          ? Number(screenData.nextStart)
          : null

      const returnedTotalCompanies =
        extractStepCount(screenData, ["totalCompanies"]) ?? state.screen_total

      state = await patchPipelineState(supabase, {
        stage: returnedNextStart === null ? "eligible_universe" : "screening",
        status: "idle",
        screen_start: returnedNextStart ?? 0,
        screen_next_start: returnedNextStart,
        screen_batch: batchSize,
        screen_total: returnedTotalCompanies,
        last_run_finished_at: nowIso(),
      })

      return NextResponse.json({
        ok: true,
        message:
          returnedNextStart === null
            ? "Completed final screening batch."
            : "Completed one screening batch.",
        nextStage: state.stage,
        nextStart: returnedNextStart,
        state: {
          stage: state.stage,
          status: state.status,
          screenStart: state.screen_start,
          screenNextStart: state.screen_next_start,
          screenBatch: state.screen_batch,
          screenTotal: state.screen_total,
        },
        results,
      })
    }

    if (state.stage === "eligible_universe") {
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
        stage: "filings",
        status: "idle",
        screen_start: 0,
        screen_next_start: 0,
        screen_total:
          typeof (eligibleUniverseResult.data as any)?.eligibleCount === "number"
            ? Number((eligibleUniverseResult.data as any).eligibleCount)
            : state.screen_total,
        last_run_finished_at: nowIso(),
      })

      if (Date.now() - runStartMs >= CHAIN_BUDGET_MS) {
        return NextResponse.json({
          ok: true,
          message: "Completed eligible universe step.",
          nextStage: state.stage,
          results,
        })
      }
      // else: fall through to filings stage below
    }

    if (state.stage === "filings") {
      let filingsStart = getStageCursor(state)
      let filingsComplete = false

      // Chain multiple filings batches per cron tick (SEC EDGAR is free/unlimited)
      while (Date.now() - runStartMs < CHAIN_BUDGET_MS) {
        const filingsResult = await runStep(
          baseUrl,
          withSearchParams("/api/ingest/filings", {
            scope: "eligible",
            start: filingsStart,
            batch: DEFAULT_FILINGS_BATCH,
            lookbackDays: DEFAULT_FILINGS_LOOKBACK_DAYS,
            runRetention: true,
          }),
          FILINGS_STEP_TIMEOUT_MS
        )

        results.push(filingsResult)

        if (!filingsResult.ok) {
          await patchPipelineState(supabase, {
            screen_start: filingsStart,
            screen_next_start: filingsStart,
            last_run_finished_at: nowIso(),
          })
          return await failPipelineForStep(
            supabase,
            results,
            filingsResult,
            `Filings step failed at start=${filingsStart}: ${String(
              (filingsResult.data as any)?.error || filingsResult.status
            )}`
          )
        }

        const filingsData = filingsResult.data as any
        const nextFilingsStart =
          typeof filingsData?.nextStart === "number"
            ? Number(filingsData.nextStart)
            : null

        filingsComplete = nextFilingsStart === null

        if (filingsComplete) break
        filingsStart = nextFilingsStart!
      }

      state = await patchPipelineState(supabase, {
        stage: filingsComplete ? "ptrs" : "filings",
        status: "idle",
        screen_start: filingsComplete ? 0 : filingsStart,
        screen_next_start: filingsComplete ? 0 : filingsStart,
        filings_completed_at: filingsComplete ? nowIso() : null,
        last_run_finished_at: nowIso(),
      })

      return NextResponse.json({
        ok: true,
        message: filingsComplete
          ? "Completed all filings batches."
          : `Completed filings batches up to start=${filingsStart}.`,
        nextStage: state.stage,
        batchesRun: results.filter((r) => r.path.includes("/api/ingest/filings")).length,
        state: {
          stage: state.stage,
          status: state.status,
          screenStart: state.screen_start,
          screenNextStart: state.screen_next_start,
        },
        results,
      })
    }

    if (state.stage === "ptrs") {
      const ptrsStart = getStageCursor(state)

      const ptrsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/ptrs", {
          scope: "eligible",
          start: ptrsStart,
          batch: DEFAULT_PTRS_BATCH,
          lookbackDays: DEFAULT_PTRS_LOOKBACK_DAYS,
          onlyActive: true,
          includeCounts: false,
          runRetention: true,
        }),
        PTRS_STEP_TIMEOUT_MS
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

      const ptrsData = ptrsResult.data as any
      const nextPtrsStart =
        typeof ptrsData?.nextStart === "number"
          ? Number(ptrsData.nextStart)
          : null

      const ptrsComplete = nextPtrsStart === null

      state = await patchPipelineState(supabase, {
        stage: ptrsComplete ? "signals" : "ptrs",
        status: "idle",
        screen_start: ptrsComplete ? 0 : nextPtrsStart,
        screen_next_start: ptrsComplete ? 0 : nextPtrsStart,
        last_run_finished_at: nowIso(),
      })

      return NextResponse.json({
        ok: true,
        message: ptrsComplete
          ? "Completed PTR step."
          : "Completed one PTR batch.",
        nextStage: state.stage,
        nextPtrsStart,
        state: {
          stage: state.stage,
          status: state.status,
          screenStart: state.screen_start,
          screenNextStart: state.screen_next_start,
          screenBatch: state.screen_batch,
          screenTotal: state.screen_total,
        },
        results,
      })
    }

    if (state.stage === "signals") {
      const signalsResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/signals", {
          scope: "eligible",
          mode: "all",
          limit: DEFAULT_SIGNALS_LIMIT,
          lookbackDays: DEFAULT_SIGNALS_LOOKBACK_DAYS,
          minSignalStrength: DEFAULT_SIGNALS_MIN_STRENGTH,
          onlyActive: true,
          runRetention: false,
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
        stage: "ticker_scores",
        status: "idle",
        signals_completed_at: nowIso(),
        last_run_finished_at: nowIso(),
      })

      if (Date.now() - runStartMs >= CHAIN_BUDGET_MS) {
        return NextResponse.json({
          ok: true,
          message: "Completed signals step.",
          nextStage: state.stage,
          results,
        })
      }
      // else: fall through to ticker_scores stage below
    }

    if (state.stage === "ticker_scores") {
      const tickerScoresResult = await runStep(
        baseUrl,
        withSearchParams("/api/ingest/ticker-scores", {
          scope: "eligible",
          limit: DEFAULT_TICKER_SCORES_LIMIT,
          lookbackDays: DEFAULT_SIGNALS_LOOKBACK_DAYS,
          ptrLookbackDays: DEFAULT_TICKER_SCORES_PTR_LOOKBACK_DAYS,
          ptrRecentDays: DEFAULT_TICKER_SCORES_PTR_RECENT_DAYS,
          minCombinedScore: DEFAULT_TICKER_SCORES_MIN_COMBINED_SCORE,
          includePreview: false,
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

      state = await patchPipelineState(supabase, {
        stage: "finalize_candidates",
        status: "idle",
        ticker_scores_completed_at: nowIso(),
        last_run_finished_at: nowIso(),
      })

      if (Date.now() - runStartMs >= CHAIN_BUDGET_MS) {
        return NextResponse.json({
          ok: true,
          message: "Completed ticker scores step.",
          nextStage: state.stage,
          results,
        })
      }
      // else: fall through to finalize_candidates stage below
    }

    if (state.stage === "finalize_candidates") {
      const finalizeResult = await runStep(
        baseUrl,
        withSearchParams("/api/screen/finalize-candidates", {
          limit: DEFAULT_FINAL_CANDIDATES_LIMIT,
          targetMin: DEFAULT_FINAL_CANDIDATES_TARGET_MIN,
          includePreview: false,
        }),
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

      const completedAt = nowIso()

      state = await patchPipelineState(supabase, {
        stage: "idle",
        status: "idle",
        screen_start: 0,
        screen_next_start: 0,
        screen_batch: clampScreenBatch(state.screen_batch),
        cycle_started_at: null,
        cycle_completed_at: completedAt,
        last_success_at: completedAt,
        last_run_finished_at: completedAt,
      })

      return NextResponse.json({
        ok: true,
        message: "Pipeline cycle completed successfully",
        nextStage: state.stage,
        results,
      })
    }

    state = await patchPipelineState(supabase, {
      stage: "companies",
      status: "idle",
      screen_start: 0,
      screen_next_start: 0,
      last_run_finished_at: nowIso(),
    })

    return NextResponse.json({
      ok: true,
      message: "Pipeline was reset to companies stage.",
      nextStage: state.stage,
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
      },
      { status: 500 }
    )
  }
}