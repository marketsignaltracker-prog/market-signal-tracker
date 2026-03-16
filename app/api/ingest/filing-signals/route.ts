import { NextRequest } from "next/server"

export const dynamic = "force-dynamic"
export const maxDuration = 300

function parseBoolean(value: string | null, defaultValue: boolean) {
  if (value === null || value === undefined || value.trim() === "") {
    return defaultValue
  }

  const normalized = value.trim().toLowerCase()
  if (normalized === "true") return true
  if (normalized === "false") return false
  return defaultValue
}

function parseInteger(value: string | null, fallback: number) {
  if (!value || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

export async function GET(request: NextRequest) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json(
      { ok: false, error: "Unauthorized" },
      { status: 401 }
    )
  }

  try {
    const url = new URL(request.url)

    const scope = (url.searchParams.get("scope") || "eligible").toLowerCase()
    const limit = parseInteger(url.searchParams.get("limit"), 150)
    const lookbackDays = parseInteger(url.searchParams.get("lookbackDays"), 21)
    const minSignalStrength = parseInteger(
      url.searchParams.get("minSignalStrength"),
      20
    )
    const onlyActive = parseBoolean(url.searchParams.get("onlyActive"), true)
    const runRetention = parseBoolean(
      url.searchParams.get("runRetention"),
      false
    )

    const signalsUrl = new URL("/api/ingest/signals", url.origin)
    signalsUrl.searchParams.set("scope", scope)
    signalsUrl.searchParams.set("mode", "filings")
    signalsUrl.searchParams.set("limit", String(limit))
    signalsUrl.searchParams.set("lookbackDays", String(lookbackDays))
    signalsUrl.searchParams.set(
      "minSignalStrength",
      String(minSignalStrength)
    )
    signalsUrl.searchParams.set("onlyActive", String(onlyActive))
    signalsUrl.searchParams.set("runRetention", String(runRetention))

    const upstreamResponse = await fetch(signalsUrl.toString(), {
      method: "GET",
      headers: {
        "x-pipeline-token": suppliedToken,
      },
      cache: "no-store",
    })

    const contentType = upstreamResponse.headers.get("content-type") || ""
    const responseBody = contentType.includes("application/json")
      ? await upstreamResponse.json()
      : await upstreamResponse.text()

    if (!upstreamResponse.ok) {
      return Response.json(
        {
          ok: false,
          error: "Unified signals route returned an error for filing mode.",
          upstreamStatus: upstreamResponse.status,
          upstream: responseBody,
        },
        { status: upstreamResponse.status }
      )
    }

    return Response.json({
      ok: true,
      stage: "filing_signals",
      delegatedTo: "signals",
      delegatedMode: "filings",
      scope,
      limit,
      lookbackDays,
      minSignalStrength,
      onlyActive,
      runRetention,
      upstream: responseBody,
      message:
        "Filing-signals compatibility route delegated successfully to the unified signals route in filings mode.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown filing-signals compatibility error",
      },
      { status: 500 }
    )
  }
}