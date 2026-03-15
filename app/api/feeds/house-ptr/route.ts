import { NextResponse } from "next/server"

export const dynamic = "force-dynamic"

export async function GET() {
  const csv = [
    "filer_name,chamber,district_or_state,report_date,transaction_date,ticker,asset_name,asset_type,action,amount_range,owner,ptr_url,raw_document_url",
    'Jane Doe,House,CA-12,2026-03-01,2026-02-20,AAPL,Apple Inc.,Stock,Purchase,"$15,001 - $50,000",Self,https://example.com/report/1,https://example.com/raw/1',
    'John Smith,House,TX-07,2026-03-02,2026-02-25,NVDA,NVIDIA Corp.,Stock,Sale,"$1,001 - $15,000",Spouse,https://example.com/report/2,https://example.com/raw/2',
  ].join("\n")

  return new NextResponse(csv, {
    status: 200,
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Cache-Control": "no-store",
    },
  })
}