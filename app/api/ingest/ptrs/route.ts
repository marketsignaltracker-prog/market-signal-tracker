import { createClient } from "@supabase/supabase-js"
import { XMLParser } from "fast-xml-parser"

export const dynamic = "force-dynamic"
export const maxDuration = 300

type RawPtrTradeRow = {
  disclosure_source: string
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
  amount_low: number | null
  amount_high: number | null
  owner: string | null
  ptr_url: string | null
  raw_document_url: string | null
  trade_key: string
  fetched_at: string
  created_at?: string
  updated_at?: string
}

type ChunkWriteResult = {
  insertedOrUpdated: number
  errors: Array<{
    table: string
    chunkStart: number
    chunkSize: number
    message: string
    details?: string | null
    hint?: string | null
    code?: string | null
    sampleKeys?: string[]
  }>
}

type SourceDiagnostic = {
  url: string
  loadedRows: number
  keptRows: number
  error: string | null
}

const DEFAULT_LOOKBACK_DAYS = 30
const MAX_LOOKBACK_DAYS = 180
const DEFAULT_LIMIT = 1000
const MAX_LIMIT = 10000
const DB_CHUNK_SIZE = 200
const SOURCE_FETCH_CONCURRENCY = 4
const SOURCE_TIMEOUT_MS = 12000

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  parseTagValue: true,
  trimValues: true,
})

function parseInteger(value: string | null | undefined, fallback: number) {
  if (value === null || value === undefined || value.trim() === "") {
    return fallback
  }
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

function normalizeTicker(ticker: string | null | undefined) {
  const t = (ticker || "").trim().toUpperCase()
  if (!t) return null
  if (!/^[A-Z.\-]{1,10}$/.test(t)) return null
  return t
}

function normalizeText(value: unknown) {
  const s = String(value ?? "").trim()
  return s.length ? s : null
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10)
}

function normalizeDate(value: unknown): string | null {
  const raw = String(value ?? "").trim()
  if (!raw) return null

  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (isoMatch) return raw

  const usMatch = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)
  if (usMatch) {
    const [, mm, dd, yyyy] = usMatch
    return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`
  }

  const parsed = new Date(raw)
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10)
  }

  return null
}

function uniqueStrings(values: (string | null | undefined)[]) {
  return Array.from(new Set(values.map((v) => (v ?? "").trim()).filter(Boolean)))
}

function safeNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = Number(String(value).replace(/[$,]/g, "").trim())
  return Number.isFinite(n) ? n : null
}

function toArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return []
  return Array.isArray(value) ? value : [value]
}

function pickFirst(obj: Record<string, any>, keys: string[]) {
  for (const key of keys) {
    const value = obj[key]
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value
    }
  }
  return null
}

function parseAmountRange(range: string | null) {
  if (!range) {
    return {
      amount_low: null,
      amount_high: null,
    }
  }

  const cleaned = range.replace(/\s+/g, " ").trim()

  const commonMap: Record<string, { low: number; high: number | null }> = {
    "$1,001 - $15,000": { low: 1001, high: 15000 },
    "$15,001 - $50,000": { low: 15001, high: 50000 },
    "$50,001 - $100,000": { low: 50001, high: 100000 },
    "$100,001 - $250,000": { low: 100001, high: 250000 },
    "$250,001 - $500,000": { low: 250001, high: 500000 },
    "$500,001 - $1,000,000": { low: 500001, high: 1000000 },
    "$1,000,001 - $5,000,000": { low: 1000001, high: 5000000 },
    "$5,000,001 - $25,000,000": { low: 5000001, high: 25000000 },
    "$25,000,001 - $50,000,000": { low: 25000001, high: 50000000 },
    "$50,000,001+": { low: 50000001, high: null },
  }

  if (commonMap[cleaned]) {
    return {
      amount_low: commonMap[cleaned].low,
      amount_high: commonMap[cleaned].high,
    }
  }

  const rangeMatch = cleaned.match(/\$?([\d,]+)\s*-\s*\$?([\d,]+)/)
  if (rangeMatch) {
    return {
      amount_low: safeNumber(rangeMatch[1]),
      amount_high: safeNumber(rangeMatch[2]),
    }
  }

  const plusMatch = cleaned.match(/\$?([\d,]+)\s*\+/)
  if (plusMatch) {
    return {
      amount_low: safeNumber(plusMatch[1]),
      amount_high: null,
    }
  }

  return {
    amount_low: null,
    amount_high: null,
  }
}

function normalizeAction(value: string | null) {
  const v = (value || "").trim().toLowerCase()
  if (!v) return null

  if (
    v === "purchase" ||
    v === "buy" ||
    v === "purchased" ||
    v === "partial purchase"
  ) {
    return "Buy"
  }

  if (
    v === "sale" ||
    v === "sell" ||
    v === "sold" ||
    v === "partial sale"
  ) {
    return "Sell"
  }

  if (v.includes("exchange")) return "Exchange"
  return value
}

function buildTradeKey(row: {
  disclosure_source: string
  filer_name: string
  transaction_date: string | null
  ticker: string | null
  asset_name: string | null
  action: string | null
  amount_range: string | null
}) {
  return [
    row.disclosure_source.trim().toLowerCase(),
    row.filer_name.trim().toLowerCase(),
    row.transaction_date || "unknown-date",
    (row.ticker || "").trim().toUpperCase() || "unknown-ticker",
    (row.asset_name || "").trim().toLowerCase() || "unknown-asset",
    (row.action || "").trim().toLowerCase() || "unknown-action",
    (row.amount_range || "").trim().toLowerCase() || "unknown-amount",
  ].join("::")
}

function parseCsvLine(line: string) {
  const out: string[] = []
  let current = ""
  let inQuotes = false

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i]
    const next = i + 1 < line.length ? line[i + 1] : ""

    if (ch === '"') {
      if (inQuotes && next === '"') {
        current += '"'
        i += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }

    if (ch === "," && !inQuotes) {
      out.push(current)
      current = ""
      continue
    }

    current += ch
  }

  out.push(current)
  return out.map((v) => v.trim())
}

function parseDelimitedText(text: string, delimiter: "," | "\t" = ",") {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)

  if (!lines.length) return []

  const header =
    delimiter === ","
      ? parseCsvLine(lines[0])
      : lines[0].split("\t").map((v) => v.trim())

  const rows: Record<string, string>[] = []

  for (const line of lines.slice(1)) {
    const values =
      delimiter === ","
        ? parseCsvLine(line)
        : line.split("\t").map((v) => v.trim())

    const row: Record<string, string> = {}
    for (let i = 0; i < header.length; i += 1) {
      row[header[i]] = values[i] ?? ""
    }
    rows.push(row)
  }

  return rows
}

function extractRecordsFromXml(xmlText: string) {
  const parsed = xmlParser.parse(xmlText)

  const candidateCollections = [
    parsed?.records?.record,
    parsed?.rows?.row,
    parsed?.items?.item,
    parsed?.disclosures?.disclosure,
    parsed?.trades?.trade,
    parsed?.data?.row,
    parsed?.data?.record,
  ]

  for (const collection of candidateCollections) {
    const rows = toArray(collection)
    if (rows.length) return rows as Record<string, any>[]
  }

  const flatArrays = Object.values(parsed || {}).filter(Array.isArray)
  for (const arr of flatArrays) {
    if (Array.isArray(arr) && arr.length && typeof arr[0] === "object") {
      return arr as Record<string, any>[]
    }
  }

  return []
}

function normalizePtrRow(
  raw: Record<string, any>,
  sourceLabel: string,
  fetchedAt: string
): RawPtrTradeRow | null {
  const filerFirst = normalizeText(
    pickFirst(raw, ["first_name", "firstname", "filer_first_name", "member_first_name"])
  )
  const filerLast = normalizeText(
    pickFirst(raw, ["last_name", "lastname", "filer_last_name", "member_last_name"])
  )
  const filerName =
    normalizeText(
      pickFirst(raw, [
        "filer_name",
        "member",
        "member_name",
        "name",
        "representative",
        "senator",
        "reporting_individual",
      ])
    ) || [filerFirst, filerLast].filter(Boolean).join(" ").trim() || null

  const ticker = normalizeTicker(
    normalizeText(
      pickFirst(raw, [
        "ticker",
        "symbol",
        "asset_ticker",
        "stock",
        "stock_symbol",
      ])
    )
  )

  const assetName = normalizeText(
    pickFirst(raw, [
      "asset_name",
      "asset",
      "description",
      "description_of_asset",
      "security",
    ])
  )

  const transactionDate = normalizeDate(
    pickFirst(raw, [
      "transaction_date",
      "tx_date",
      "date_of_transaction",
      "trade_date",
    ])
  )

  const reportDate = normalizeDate(
    pickFirst(raw, [
      "report_date",
      "disclosure_date",
      "filed_date",
      "filing_date",
      "date_received",
    ])
  )

  const action = normalizeAction(
    normalizeText(
      pickFirst(raw, [
        "action",
        "transaction_type",
        "type",
        "transaction",
      ])
    )
  )

  const amountRange = normalizeText(
    pickFirst(raw, [
      "amount_range",
      "amount",
      "range",
      "value_range",
    ])
  )

  const amounts = parseAmountRange(amountRange)

  const filer = filerName || null
  if (!filer) return null

  if (!ticker && !assetName) return null

  const ptrUrl = normalizeText(
    pickFirst(raw, ["ptr_url", "report_url", "url", "disclosure_url"])
  )

  const rawDocumentUrl = normalizeText(
    pickFirst(raw, ["raw_document_url", "document_url", "pdf_url", "xml_url"])
  )

  const chamber = normalizeText(
    pickFirst(raw, ["chamber", "body"])
  ) || (sourceLabel.toLowerCase().includes("house") ? "House" : null)

  const districtOrState = normalizeText(
    pickFirst(raw, ["district_or_state", "district", "state", "office"])
  )

  const assetType = normalizeText(
    pickFirst(raw, ["asset_type", "type_of_asset", "holding_type"])
  )

  const owner = normalizeText(
    pickFirst(raw, ["owner", "owner_type", "held_by"])
  )

  const tradeKey = buildTradeKey({
    disclosure_source: sourceLabel,
    filer_name: filer,
    transaction_date: transactionDate,
    ticker,
    asset_name: assetName,
    action,
    amount_range: amountRange,
  })

  return {
    disclosure_source: sourceLabel,
    filer_name: filer,
    chamber,
    district_or_state: districtOrState,
    report_date: reportDate,
    transaction_date: transactionDate,
    ticker,
    asset_name: assetName,
    asset_type: assetType,
    action,
    amount_range: amountRange,
    amount_low: amounts.amount_low,
    amount_high: amounts.amount_high,
    owner,
    ptr_url: ptrUrl,
    raw_document_url: rawDocumentUrl,
    trade_key: tradeKey,
    fetched_at: fetchedAt,
    updated_at: fetchedAt,
  }
}

async function fetchText(url: string) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), SOURCE_TIMEOUT_MS)

  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent":
          process.env.PTR_USER_AGENT ||
          "Market Signal Tracker ptr ingest marketsignaltracker@gmail.com",
        Accept:
          "application/json,text/csv,text/tab-separated-values,application/xml,text/xml,text/plain;q=0.9,*/*;q=0.8",
      },
      cache: "no-store",
      signal: controller.signal,
    })

    if (!res.ok) {
      throw new Error(`Failed fetching PTR source: ${url} (${res.status})`)
    }

    return await res.text()
  } finally {
    clearTimeout(timeout)
  }
}

async function fetchSourceRows(url: string, fetchedAt: string) {
  const text = await fetchText(url)
  const trimmed = text.trim()
  const lowerUrl = url.toLowerCase()

  let records: Record<string, any>[] = []

  if (
    lowerUrl.endsWith(".json") ||
    trimmed.startsWith("[") ||
    trimmed.startsWith("{")
  ) {
    const parsed = JSON.parse(trimmed)

    if (Array.isArray(parsed)) {
      records = parsed as Record<string, any>[]
    } else if (Array.isArray((parsed as any)?.records)) {
      records = (parsed as any).records
    } else if (Array.isArray((parsed as any)?.data)) {
      records = (parsed as any).data
    } else if (Array.isArray((parsed as any)?.items)) {
      records = (parsed as any).items
    } else {
      records = []
    }
  } else if (
    lowerUrl.endsWith(".xml") ||
    trimmed.startsWith("<?xml") ||
    trimmed.startsWith("<")
  ) {
    records = extractRecordsFromXml(trimmed)
  } else {
    const delimiter: "," | "\t" =
      trimmed.includes("\t") && !trimmed.includes(",") ? "\t" : ","
    records = parseDelimitedText(trimmed, delimiter)
  }

  const sourceLabel = lowerUrl.includes("house") ? "House PTR" : "PTR"

  return records
    .map((record) => normalizePtrRow(record, sourceLabel, fetchedAt))
    .filter((row): row is RawPtrTradeRow => row !== null)
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length)
  let nextIndex = 0

  async function runner() {
    while (true) {
      const current = nextIndex
      nextIndex += 1
      if (current >= items.length) return
      results[current] = await worker(items[current], current)
    }
  }

  const runners = Array.from(
    { length: Math.min(concurrency, Math.max(items.length, 1)) },
    () => runner()
  )

  await Promise.all(runners)
  return results
}

async function upsertInChunksDetailed(
  table: any,
  tableName: string,
  rows: any[],
  onConflict: string,
  sampleKeyBuilder?: (row: any) => string
): Promise<ChunkWriteResult> {
  let insertedOrUpdated = 0
  const errors: ChunkWriteResult["errors"] = []

  for (let i = 0; i < rows.length; i += DB_CHUNK_SIZE) {
    const chunk = rows.slice(i, i + DB_CHUNK_SIZE)
    const { error } = await table.upsert(chunk, { onConflict })

    if (error) {
      errors.push({
        table: tableName,
        chunkStart: i,
        chunkSize: chunk.length,
        message: error.message,
        details: (error as any)?.details ?? null,
        hint: (error as any)?.hint ?? null,
        code: (error as any)?.code ?? null,
        sampleKeys: sampleKeyBuilder ? chunk.slice(0, 10).map(sampleKeyBuilder) : undefined,
      })
    } else {
      insertedOrUpdated += chunk.length
    }
  }

  return {
    insertedOrUpdated,
    errors,
  }
}

export async function GET(request: Request) {
  const pipelineToken = process.env.PIPELINE_TOKEN
  const suppliedToken = request.headers.get("x-pipeline-token")

  if (!pipelineToken || suppliedToken !== pipelineToken) {
    return Response.json({ ok: false, error: "Unauthorized" }, { status: 401 })
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: "Missing Supabase environment variables" },
      { status: 500 }
    )
  }

  const houseFeedUrls = uniqueStrings(
    (process.env.PTR_HOUSE_FEED_URLS || "")
      .split(",")
      .map((v) => v.trim())
  )

  if (!houseFeedUrls.length) {
    return Response.json(
      {
        ok: false,
        error: "Missing PTR_HOUSE_FEED_URLS environment variable",
      },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })

    const { searchParams } = new URL(request.url)

    const scopeParam = (searchParams.get("scope") || "all").toLowerCase()
    const start = Math.max(0, parseInteger(searchParams.get("start"), 0))
    const batch = Math.max(1, parseInteger(searchParams.get("batch"), 100))
    const lookbackDays = Math.min(
      Math.max(1, parseInteger(searchParams.get("lookbackDays"), DEFAULT_LOOKBACK_DAYS)),
      MAX_LOOKBACK_DAYS
    )
    const limit = Math.min(
      Math.max(1, parseInteger(searchParams.get("limit"), DEFAULT_LIMIT)),
      MAX_LIMIT
    )
    const runRetention =
      (searchParams.get("runRetention") || "false").toLowerCase() === "true"
    const includeCounts =
      (searchParams.get("includeCounts") || "false").toLowerCase() === "true"

    if (!["all", "eligible", "candidates"].includes(scopeParam)) {
      return Response.json(
        {
          ok: false,
          error: `Invalid scope "${scopeParam}". Expected one of: all, eligible, candidates`,
        },
        { status: 400 }
      )
    }

    const scope = scopeParam as "all" | "eligible" | "candidates"

    const now = new Date()
    const fetchedAt = now.toISOString()

    const cutoffDate = new Date(now)
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays)
    const cutoffDateString = toIsoDateString(cutoffDate)

    const sourceResults = await mapWithConcurrency(
      houseFeedUrls,
      SOURCE_FETCH_CONCURRENCY,
      async (url): Promise<{
        diagnostic: SourceDiagnostic
        rows: RawPtrTradeRow[]
      }> => {
        try {
          const sourceRows = await fetchSourceRows(url, fetchedAt)

          const filtered = sourceRows.filter((row) => {
            const tradeDate = row.transaction_date || row.report_date
            return tradeDate ? tradeDate >= cutoffDateString : true
          })

          return {
            diagnostic: {
              url,
              loadedRows: sourceRows.length,
              keptRows: filtered.length,
              error: null,
            },
            rows: filtered,
          }
        } catch (error: any) {
          return {
            diagnostic: {
              url,
              loadedRows: 0,
              keptRows: 0,
              error: error?.message || "Unknown source error",
            },
            rows: [],
          }
        }
      }
    )

    const allRows: RawPtrTradeRow[] = []
    const sourceDiagnostics: SourceDiagnostic[] = []

    for (const result of sourceResults) {
      sourceDiagnostics.push(result.diagnostic)
      allRows.push(...result.rows)
    }

    const dedupedMap = new Map<string, RawPtrTradeRow>()
    for (const row of allRows) {
      if (!dedupedMap.has(row.trade_key)) {
        dedupedMap.set(row.trade_key, row)
      }
    }

    const effectiveLimit =
      scope === "all"
        ? limit
        : Math.min(limit, Math.max(batch * 10, 200))

    const dedupedRows = Array.from(dedupedMap.values())
      .sort((a, b) => {
        const aDate = a.transaction_date || a.report_date || ""
        const bDate = b.transaction_date || b.report_date || ""
        if (aDate !== bDate) return bDate.localeCompare(aDate)
        return a.trade_key.localeCompare(b.trade_key)
      })
      .slice(0, effectiveLimit)

    const writeResult =
      dedupedRows.length > 0
        ? await upsertInChunksDetailed(
            supabase.from("raw_ptr_trades"),
            "raw_ptr_trades",
            dedupedRows,
            "trade_key",
            (row) => row.trade_key
          )
        : { insertedOrUpdated: 0, errors: [] as ChunkWriteResult["errors"] }

    if (writeResult.errors.length > 0) {
      return Response.json(
        {
          ok: false,
          error: "Failed writing PTR rows",
          debug: {
            errorSamples: writeResult.errors.slice(0, 5),
            sourceDiagnostics,
            request: {
              scope,
              start,
              batch,
              lookbackDays,
              limit,
            },
          },
        },
        { status: 500 }
      )
    }

    let retentionMessage = "skipped"
    if (runRetention) {
      const retentionCutoff = new Date(now)
      retentionCutoff.setDate(retentionCutoff.getDate() - 30)
      const retentionCutoffString = toIsoDateString(retentionCutoff)

      const { error: retentionError } = await supabase
        .from("raw_ptr_trades")
        .delete()
        .or(
          `transaction_date.lt.${retentionCutoffString},and(transaction_date.is.null,report_date.lt.${retentionCutoffString})`
        )

      retentionMessage = retentionError ? retentionError.message : "ok"
    }

    let ptrCount: number | null = null
    if (includeCounts) {
      const { count, error } = await supabase
        .from("raw_ptr_trades")
        .select("*", { count: "exact", head: true })

      ptrCount = error ? null : count ?? 0
    }

    const failedSources = sourceDiagnostics.filter((s) => s.error).length

    return Response.json({
      ok: true,
      scope,
      start,
      batch,
      fetchedSources: houseFeedUrls.length,
      failedSources,
      scannedRows: allRows.length,
      insertedOrUpdated: writeResult.insertedOrUpdated,
      dedupedRows: dedupedRows.length,
      lookbackDays,
      limit: effectiveLimit,
      retentionCleanup: retentionMessage,
      ptrCount,
      sourceDiagnostics,
      message:
        "House PTR trades ingested successfully. This route is feed-based, accepts pipeline scope parameters, and writes deduped rows into raw_ptr_trades.",
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || "Unknown PTR ingest error",
      },
      { status: 500 }
    )
  }
}