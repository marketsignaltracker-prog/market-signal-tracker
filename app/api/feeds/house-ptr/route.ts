import { NextResponse } from "next/server"
import { createClient } from "@supabase/supabase-js"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type PtrFeedSourceRow = {
  filer_name: string
  chamber: string | null
  district_or_state: string | null
  report_date: string | null
  transaction_date: string | null
  ticker: string | null
  asset_name: string | null
  asset_type: string | null
  action: string | null
  amount_range: string | null
  owner: string | null
  ptr_url: string | null
  raw_document_url: string | null
  is_active?: boolean | null
}

function getSupabaseAdmin() {
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
  if (!value || value.trim() === "") return fallback
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function csvEscape(value: unknown) {
  if (value === null || value === undefined) return ""
  const s = String(value)

  if (s.includes('"') || s.includes(",") || s.includes("\n") || s.includes("\r")) {
    return `"${s.replace(/"/g, '""')}"`
  }

  return s
}

function toCsv(rows: PtrFeedSourceRow[]) {
  const header = [
    "filer_name",
    "chamber",
    "district_or_state",
    "report_date",
    "transaction_date",
    "ticker",
    "asset_name",
    "asset_type",
    "action",
    "amount_range",
    "owner",
    "ptr_url",
    "raw_document_url",
  ]

  const lines = [header.join(",")]

  for (const row of rows) {
    lines.push(
      [
        csvEscape(row.filer_name),
        csvEscape(row.chamber),
        csvEscape(row.district_or_state),
        csvEscape(row.report_date),
        csvEscape(row.transaction_date),
        csvEscape(row.ticker),
        csvEscape(row.asset_name),
        csvEscape(row.asset_type),
        csvEscape(row.action),
        csvEscape(row.amount_range),
        csvEscape(row.owner),
        csvEscape(row.ptr_url),
        csvEscape(row.raw_document_url),
      ].join(",")
    )
  }

  return lines.join("\n")
}

export async function GET(request: Request) {
  try {
    const supabase = getSupabaseAdmin()
    const { searchParams } = new URL(request.url)

    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), 5000)),
      20000
    )

    const onlyActive =
      (searchParams.get("onlyActive") || "true").toLowerCase() !== "false"

    let query = (supabase.from("ptr_feed_source") as any)
      .select(
        "filer_name, chamber, district_or_state, report_date, transaction_date, ticker, asset_name, asset_type, action, amount_range, owner, ptr_url, raw_document_url, is_active"
      )
      .order("transaction_date", { ascending: false, nullsFirst: false })
      .order("report_date", { ascending: false, nullsFirst: false })
      .limit(limit)

    if (onlyActive) {
      query = query.eq("is_active", true)
    }

    const { data, error } = await query

    if (error) {
      return NextResponse.json(
        {
          ok: false,
          error: "Failed to load PTR feed rows from Supabase",
          debug: {
            message: error.message,
          },
        },
        { status: 500 }
      )
    }

    const rows = (data || []) as PtrFeedSourceRow[]
    const csv = toCsv(rows)

    return new NextResponse(csv, {
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
        error: error?.message || "Unknown feed route error",
      },
      { status: 500 }
    )
  }
}