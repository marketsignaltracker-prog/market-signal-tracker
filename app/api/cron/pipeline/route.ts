import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

type StepResult = {
  step: string
  path: string
  ok: boolean
  status: number
  durationMs: number
  data: any
}

type PipelineStateRow = {
  job_name: string
  stage: "idle" | "companies" | "screening" | "filings" | "signals" | "complete" | "error"
  status: "idle" | "running" | "success" | "error"
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
const MAX_SCREEN_BATCH = 250

const MAX_PIPELINE_RUNTIME_MS = 210_000
const RUNTIME_SAFETY_BUFFER_MS = 15_000

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

async function runStep(baseUrl: string, path: string): Promise<StepResult> {
  const pipelineToken = process.env.PIPELINE_TOKEN

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const url = makeUrl(baseUrl, path)
  const startedAt = Date.now()

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-pipeline-token": pipelineToken,
    },
    cache: "no-store",
  })

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
    throw new Error(`Failed to load pipeline state: ${error.message}`)
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

function nowIso() {
  return new Date().toISOString()
}

function shouldStopForRuntime(runStartedAtMs: number) {
  const elapsed = Date.now() - runStartedAtMs
  return elapsed >= MAX_PIPELINE_RUNTIME_MS - RUNTIME_SAFETY_BUFFER_MS
}

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization")

  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized" },
      { status: 401 }
    )
  }

  const startedAtMs = Date.now()
  const runStartedIso = nowIso()

  try {
    const baseUrl = getBaseUrl()
    const supabase = getSupabaseAdmin()

    let state = await getPipelineState(supabase)

    state = await patchPipelineState(supabase, {
      status: "running",
      last_run_started_at: runStartedIso,
      last_run_finished_at: null,
      last_error: null,
      last_error_at: null,
      stage:
        state.stage === "idle" || state.stage === "complete" || state.stage === "error"
          ? "companies"
          : state.stage,
      cycle_started_at: state.cycle_started_at ?? runStartedIso,
    })

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
        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Companies step failed: ${String(
            (companiesResult.data as any)?.error || companiesResult.status
          )}`,
          last_error_at: nowIso(),
          last_run_finished_at: nowIso(),
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

      state = await patchPipelineState(supabase, {
        stage: "screening",
        status: "running",
        screen_start: 0,
        screen_next_start: 0,
        screen_total: null,
        filings_completed_at: null,
        signals_completed_at: null,
        cycle_completed_at: null,
        cycle_started_at: state.cycle_started_at ?? runStartedIso,
      })
    }

    if (state.stage === "screening") {
      let nextStart = state.screen_next_start ?? state.screen_start ?? 0

      const batchSize = Math.min(
        Math.max(1, parseInteger(String(state.screen_batch), DEFAULT_SCREEN_BATCH)),
        MAX_SCREEN_BATCH
      )

      let screeningComplete = false

      while (!screeningComplete) {
        if (shouldStopForRuntime(startedAtMs)) {
          const updated = await patchPipelineState(supabase, {
            stage: "screening",
            status: "running",
            screen_start: nextStart,
            screen_next_start: nextStart,
            last_run_finished_at: nowIso(),
          })

          return NextResponse.json({
            ok: true,
            message: "Pipeline checkpointed during screening. Continue on next cron run.",
            stage: updated.stage,
            status: updated.status,
            nextStart: updated.screen_next_start,
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

        if (!screenResult.ok) {
          await patchPipelineState(supabase, {
            stage: "error",
            status: "error",
            screen_start: nextStart,
            screen_next_start: nextStart,
            last_error: `Screening step failed at start=${nextStart}: ${String(
              (screenResult.data as any)?.error || screenResult.status
            )}`,
            last_error_at: nowIso(),
            last_run_finished_at: nowIso(),
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
          typeof screenData?.totalCompanies === "number" ? screenData.totalCompanies : null

        state = await patchPipelineState(supabase, {
          stage: returnedNextStart === null ? "filings" : "screening",
          status: "running",
          screen_start: nextStart,
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

    if (state.stage === "filings") {
      if (shouldStopForRuntime(startedAtMs)) {
        await patchPipelineState(supabase, {
          stage: "filings",
          status: "running",
          last_run_finished_at: nowIso(),
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
          batch: 250,
        })
      )

      results.push(filingsResult)

      if (!filingsResult.ok) {
        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Filings step failed: ${String(
            (filingsResult.data as any)?.error || filingsResult.status
          )}`,
          last_error_at: nowIso(),
          last_run_finished_at: nowIso(),
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

      state = await patchPipelineState(supabase, {
        stage: "signals",
        status: "running",
        filings_completed_at: nowIso(),
      })
    }

    if (state.stage === "signals") {
      if (shouldStopForRuntime(startedAtMs)) {
        await patchPipelineState(supabase, {
          stage: "signals",
          status: "running",
          last_run_finished_at: nowIso(),
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
          limit: 150,
          lookbackDays: 14,
        })
      )

      results.push(signalsResult)

      if (!signalsResult.ok) {
        await patchPipelineState(supabase, {
          stage: "error",
          status: "error",
          last_error: `Signals step failed: ${String(
            (signalsResult.data as any)?.error || signalsResult.status
          )}`,
          last_error_at: nowIso(),
          last_run_finished_at: nowIso(),
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

      state = await patchPipelineState(supabase, {
        stage: "complete",
        status: "success",
        screen_start: 0,
        screen_next_start: 0,
        signals_completed_at: nowIso(),
        cycle_completed_at: nowIso(),
        last_success_at: nowIso(),
        last_run_finished_at: nowIso(),
      })
    }

    if (state.stage === "complete") {
      state = await patchPipelineState(supabase, {
        stage: "idle",
        status: "idle",
        screen_start: 0,
        screen_next_start: 0,
        cycle_started_at: null,
      })
    }

    return NextResponse.json({
      ok: true,
      message: "Pipeline cycle completed successfully",
      state: {
        stage: state.stage,
        status: state.status,
        screenStart: state.screen_start,
        screenNextStart: state.screen_next_start,
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
      },
      { status: 500 }
    )
  }
}