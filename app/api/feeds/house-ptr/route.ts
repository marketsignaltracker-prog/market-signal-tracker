import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"

export async function GET() {
  const res = await fetch(
    "https://api.quiverquant.com/beta/historical/congresstrading.csv",
    { cache: "no-store" }
  )

  if (!res.ok) {
    return NextResponse.json(
      { error: "Failed to fetch congressional trading feed" },
      { status: 500 }
    )
  }

  const csv = await res.text()

  return new NextResponse(csv, {
    headers: {
      "Content-Type": "text/csv",
    },
  })
}