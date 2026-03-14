const { data: screenedDates, error: screenedDatesError } = await candidateHistoryTable
  .select("screened_on")
  .order("screened_on", { ascending: false })

if (screenedDatesError) {
  return Response.json({ ok: false, error: screenedDatesError.message }, { status: 500 })
}

const orderedDates = uniqueStrings(
  (screenedDates || []).map((row: any) => String(row.screened_on || ""))
)

let screenedOn: string | null = null
let snapshotRows: CandidateHistoryRow[] = []

for (const candidateDate of orderedDates) {
  const { data: rows, error: rowsError } = await candidateHistoryTable
    .select("*")
    .eq("screened_on", candidateDate)

  if (rowsError) {
    return Response.json({ ok: false, error: rowsError.message }, { status: 500 })
  }

  const typedRows = (rows || []) as CandidateHistoryRow[]
  if (!typedRows.length) continue

  const viableRows = typedRows.filter(
    (row) =>
      (row.candidate_score ?? 0) > 0 &&
      (
        row.passes_price ||
        row.passes_volume ||
        row.passes_dollar_volume ||
        row.passes_market_cap ||
        row.above_sma_20 ||
        row.return_20d !== null ||
        row.relative_strength_20d !== null
      )
  )

  if (viableRows.length >= 25) {
    screenedOn = candidateDate
    snapshotRows = typedRows
    break
  }
}

if (!screenedOn || !snapshotRows.length) {
  return Response.json(
    {
      ok: false,
      error: "No viable candidate history snapshot found to finalize",
    },
    { status: 500 }
  )
}