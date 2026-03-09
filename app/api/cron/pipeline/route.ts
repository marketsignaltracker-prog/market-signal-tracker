import { NextRequest, NextResponse } from "next/server"

export const runtime = "nodejs"
export const maxDuration = 300

type StepPath =
  | "/api/ingest/companies"
  | "/api/screen/candidates"
  | "/api/ingest/filings"
  | "/api/ingest/signals"

type StepResult = {
  step: string
  path: StepPath
  ok: boolean
  status: number
  durationMs: number
  data: unknown
}

const STEPS: StepPath[] = [
  "/api/ingest/companies",
  "/api/screen/candidates",
  "/api/ingest/filings",
  "/api/ingest/signals",
]

function getBaseUrl(request: NextRequest) {
  const requestOrigin = request.nextUrl.origin?.trim()
  const appUrl = process.env.APP_URL?.trim()

  if (requestOrigin) {
    return requestOrigin.replace(/\/$/, "")
  }

  if (appUrl) {
    return appUrl.replace(/\/$/, "")
  }

  throw new Error("Missing request origin and APP_URL environment variable")
}

function getCronSecret() {
  const cronSecret = process.env.CRON_SECRET?.trim()

  if (!cronSecret) {
    throw new Error("Missing CRON_SECRET environment variable")
  }

  return cronSecret
}

function getPipelineToken() {
  const pipelineToken = process.env.PIPELINE_TOKEN?.trim()

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  return pipelineToken
}

function getStepName(path: string) {
  return path.split("/").filter(Boolean).slice(-2).join("/")
}

async function parseResponse(response: Response) {
  const contentType = response.headers.get("content-type") ?? ""

  if (contentType.includes("application/json")) {
    try {
      return await response.json()
    } catch {
      return { ok: false, error: "Invalid JSON response returned by step" }
    }
  }

  try {
    const text = await response.text()
    return {
      ok: false,
      error: "Non-JSON response returned by step",
      body: text.slice(0, 1000),
    }
  } catch {
    return {
      ok: false,
      error: "Unable to read response body from step",
    }
  }
}

async function runStep(
  baseUrl: string,
  path: StepPath,
  pipelineToken: string
): Promise<StepResult> {
  const startedAt = Date.now()
  const url = `${baseUrl}${path}`

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 120_000)

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "x-pipeline-token": pipelineToken,
        "x-pipeline-step": getStepName(path),
      },
      cache: "no-store",
      signal: controller.signal,
    })

    const data = await parseResponse(response)

    return {
      step: getStepName(path),
      path,
      ok: response.ok,
      status: response.status,
      durationMs: Date.now() - startedAt,
      data,
    }
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown fetch error"

    return {
      step: getStepName(path),
      path,
      ok: false,
      status: 500,
      durationMs: Date.now() - startedAt,
      data: {
        ok: false,
        error: message,
      },
    }
  } finally {
    clearTimeout(timeout)
  }
}

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization")
  const cronSecret = getCronSecret()

  if (authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized" },
      { status: 401 }
    )
  }

  try {
    const baseUrl = getBaseUrl(request)
    const pipelineToken = getPipelineToken()

    const results: StepResult[] = []

    for (const path of STEPS) {
      const result = await runStep(baseUrl, path, pipelineToken)
      results.push(result)

      if (!result.ok) {
        return NextResponse.json(
          {
            ok: false,
            error: "Pipeline stopped because a step failed",
            failedStep: result.path,
            results,
          },
          { status: 500 }
        )
      }
    }

    return NextResponse.json({
      ok: true,
      message: "Pipeline completed successfully",
      results,
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown pipeline error"

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    )
  }
}