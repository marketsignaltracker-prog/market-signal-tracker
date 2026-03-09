import { NextRequest, NextResponse } from "next/server"

type StepResult = {
  step: string
  path: string
  ok: boolean
  status: number
  data: unknown
}

function getBaseUrl() {
  const appUrl = process.env.APP_URL?.trim()

  if (!appUrl) {
    throw new Error("Missing APP_URL environment variable")
  }

  return appUrl.replace(/\/$/, "")
}

async function runStep(baseUrl: string, path: string): Promise<StepResult> {
  const pipelineToken = process.env.PIPELINE_TOKEN

  if (!pipelineToken) {
    throw new Error("Missing PIPELINE_TOKEN environment variable")
  }

  const url = `${baseUrl}${path}`

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-pipeline-token": pipelineToken,
    },
    cache: "no-store",
  })

  let data: unknown = null

  try {
    data = await response.json()
  } catch {
    data = { ok: false, error: "Non-JSON response returned by step" }
  }

  return {
    step: path.split("/").filter(Boolean).slice(-2).join("/"),
    path,
    ok: response.ok,
    status: response.status,
    data,
  }
}

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization")

  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json(
      { ok: false, error: "Unauthorized" },
      { status: 401 }
    )
  }

  try {
    const baseUrl = getBaseUrl()

    const steps = [
      "/api/ingest/companies",
      "/api/screen/candidates",
      "/api/ingest/filings",
      "/api/ingest/signals",
    ]

    const results: StepResult[] = []

    for (const path of steps) {
      const result = await runStep(baseUrl, path)
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