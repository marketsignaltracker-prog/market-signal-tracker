import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"

const DEFAULT_UPSTREAM_URL =
  "https://api.quiverquant.com/beta/historical/congresstrading.csv"

async function fetchWithTimeout(url: string, timeoutMs: number) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)

  try {
    return await fetch(url, {
      cache: "no-store",
      signal: controller.signal,
      headers: {
        Accept: "text/csv,text/plain,application/json,*/*",
        "User-Agent":
          process.env.PTR_USER_AGENT ||
          "Market Signal Tracker ptr feed marketsignaltracker@gmail.com",
      },
    })
  } finally {
    clearTimeout(timeout)
  }
}

export async function GET() {
  const upstreamUrl = process.env.PTR_UPSTREAM_CSV_URL?.trim() || DEFAULT_UPSTREAM_URL
  const timeoutMs = 15000

  try {
    const res = await fetchWithTimeout(upstreamUrl, timeoutMs)
    const body = await res.text()

    if (!res.ok) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed to fetch upstream congressional trading feed",
          debug: {
            upstreamUrl,
            status: res.status,
            bodyPreview: body.slice(0, 500),
          },
        },
        { status: 500 }
      )
    }

    const trimmed = body.trim()

    if (!trimmed) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upstream congressional trading feed returned an empty body",
          debug: {
            upstreamUrl,
          },
        },
        { status: 500 }
      )
    }

    const looksLikeCsv =
      trimmed.includes(",") &&
      /ticker|symbol|transaction|report|member|filer/i.test(trimmed.slice(0, 1000))

    if (!looksLikeCsv) {
      return NextResponse.json(
        {
          ok: false,
          error: "Upstream response did not look like congressional trading CSV",
          debug: {
            upstreamUrl,
            contentType: res.headers.get("content-type"),
            bodyPreview: trimmed.slice(0, 500),
          },
        },
        { status: 500 }
      )
    }

    return new NextResponse(body, {
      status: 200,
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Cache-Control": "no-store",
      },
    })
  } catch (error: any) {
    return NextResponse.json(
      {
        ok: false,
        error: "Failed to fetch congressional trading feed",
        debug: {
          upstreamUrl,
          message: error?.message || "Unknown fetch error",
        },
      },
      { status: 500 }
    )
  }
}