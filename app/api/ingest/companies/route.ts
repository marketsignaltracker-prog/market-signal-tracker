import { createClient } from '@supabase/supabase-js'

type SecCompany = {
  cik_str: number
  ticker: string
  title: string
}

type CompanyRow = {
  cik: string
  ticker: string
  name: string
  is_active: boolean
  source: string
  last_seen_at: string
  updated_at: string
}

const SEC_COMPANIES_URL = 'https://www.sec.gov/files/company_tickers.json'
const SEC_USER_AGENT =
  process.env.SEC_USER_AGENT ||
  'Market Signal Tracker marketsignaltracker@gmail.com'

const UPSERT_CHUNK_SIZE = 500

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

function normalizeTicker(ticker: string | null | undefined) {
  return (ticker || '').trim().toUpperCase()
}

function normalizeName(name: string | null | undefined) {
  return (name || '').trim()
}

async function fetchSecCompanies(): Promise<CompanyRow[]> {
  const response = await fetch(SEC_COMPANIES_URL, {
    headers: {
      'User-Agent': SEC_USER_AGENT,
      Accept: 'application/json',
    },
    cache: 'no-store',
  })

  if (!response.ok) {
    throw new Error(`SEC request failed: ${response.status}`)
  }

  const json = await response.json()

  const now = new Date().toISOString()

  const companies = Object.values(json as Record<string, SecCompany>)
    .map((item) => {
      const ticker = normalizeTicker(item.ticker)
      const name = normalizeName(item.title)
      const cik = String(item.cik_str).padStart(10, '0')

      return {
        cik,
        ticker,
        name,
        is_active: true,
        source: 'sec_company_tickers',
        last_seen_at: now,
        updated_at: now,
      } satisfies CompanyRow
    })
    .filter((item) => item.ticker && item.name && item.cik)

  const dedupedByTicker = new Map<string, CompanyRow>()

  for (const company of companies) {
    if (!dedupedByTicker.has(company.ticker)) {
      dedupedByTicker.set(company.ticker, company)
    }
  }

  return Array.from(dedupedByTicker.values()).sort((a, b) =>
    a.ticker.localeCompare(b.ticker)
  )
}

export async function GET() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY

  if (!supabaseUrl || !serviceRoleKey) {
    return Response.json(
      { ok: false, error: 'Missing Supabase environment variables' },
      { status: 500 }
    )
  }

  try {
    const supabase = createClient(supabaseUrl, serviceRoleKey)

    const companies = await fetchSecCompanies()

    if (!companies.length) {
      return Response.json(
        { ok: false, error: 'No companies returned from SEC feed.' },
        { status: 500 }
      )
    }

    const chunks = chunkArray(companies, UPSERT_CHUNK_SIZE)

    let totalUpserted = 0

    for (const chunk of chunks) {
      const { error } = await supabase
        .from('companies')
        .upsert(chunk, { onConflict: 'ticker' })

      if (error) {
        throw new Error(`Supabase upsert failed: ${error.message}`)
      }

      totalUpserted += chunk.length
    }

    const refreshStartedAt = new Date().toISOString()

    const { error: staleError } = await supabase
      .from('companies')
      .update({
        is_active: false,
        updated_at: refreshStartedAt,
      })
      .lt('last_seen_at', refreshStartedAt)

    if (staleError) {
      throw new Error(`Failed marking stale companies inactive: ${staleError.message}`)
    }

    return Response.json({
      ok: true,
      totalFetched: companies.length,
      totalUpserted,
      chunks: chunks.length,
      message: 'All SEC companies imported and synced into Supabase.',
    })
  } catch (error: any) {
    return Response.json(
      {
        ok: false,
        error: error?.message || 'Unknown error',
      },
      { status: 500 }
    )
  }
}