import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const maxDuration = 10;

type Health = "healthy" | "stale" | "error" | "unknown";

export async function GET() {
  try {
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

    if (!supabaseUrl || !supabaseAnonKey) {
      throw new Error("Missing Supabase environment variables");
    }

    const supabase = createClient(supabaseUrl, supabaseAnonKey);

    // Query pipeline_state table
    const { data: pipelineRows, error: pipelineError } = await supabase
      .from("pipeline_state")
      .select("*")
      .limit(1)
      .maybeSingle();

    if (pipelineError) {
      throw new Error(`pipeline_state query failed: ${pipelineError.message}`);
    }

    // Query total ticker count
    const { count: totalTickers, error: totalError } = await supabase
      .from("ticker_scores_current")
      .select("*", { count: "exact", head: true });

    if (totalError) {
      throw new Error(`ticker_scores_current total count query failed: ${totalError.message}`);
    }

    // Query strong buy count (score >= 84)
    const { count: strongBuyCount, error: strongBuyError } = await supabase
      .from("ticker_scores_current")
      .select("*", { count: "exact", head: true })
      .gte("score", 84);

    if (strongBuyError) {
      throw new Error(`ticker_scores_current strong_buy count query failed: ${strongBuyError.message}`);
    }

    // Query elite count (score >= 94)
    const { count: eliteCount, error: eliteError } = await supabase
      .from("ticker_scores_current")
      .select("*", { count: "exact", head: true })
      .gte("score", 94);

    if (eliteError) {
      throw new Error(`ticker_scores_current elite count query failed: ${eliteError.message}`);
    }

    // Query most recent score_updated_at
    const { data: lastUpdatedRow, error: lastUpdatedError } = await supabase
      .from("ticker_scores_current")
      .select("score_updated_at")
      .order("score_updated_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (lastUpdatedError) {
      throw new Error(`ticker_scores_current last_updated query failed: ${lastUpdatedError.message}`);
    }

    const pipeline = pipelineRows ?? null;
    const now = new Date();
    const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);

    // Determine health
    let health: Health = "unknown";

    if (!pipeline && (totalTickers ?? 0) === 0) {
      health = "unknown";
    } else {
      const hasRecentError =
        pipeline?.status === "error" ||
        (pipeline?.last_error_at && new Date(pipeline.last_error_at) > twoHoursAgo);

      if (hasRecentError) {
        health = "error";
      } else {
        const lastSuccess = pipeline?.last_success_at
          ? new Date(pipeline.last_success_at)
          : null;

        const isStale =
          !lastSuccess ||
          lastSuccess < twoHoursAgo ||
          (totalTickers ?? 0) === 0;

        health = isStale ? "stale" : "healthy";
      }
    }

    const screenProgress = pipeline?.screen_progress ?? null;

    return NextResponse.json({
      ok: true,
      pipeline: {
        stage: pipeline?.stage ?? null,
        status: pipeline?.status ?? null,
        cycleStartedAt: pipeline?.cycle_started_at ?? null,
        cycleCompletedAt: pipeline?.cycle_completed_at ?? null,
        lastSuccessAt: pipeline?.last_success_at ?? null,
        lastError: pipeline?.last_error ?? null,
        lastErrorAt: pipeline?.last_error_at ?? null,
        lastRunStartedAt: pipeline?.last_run_started_at ?? null,
        screenProgress: {
          start: screenProgress?.start ?? 0,
          total: screenProgress?.total ?? null,
          batch: screenProgress?.batch ?? 100,
        },
      },
      board: {
        totalTickers: totalTickers ?? 0,
        strongBuyCount: strongBuyCount ?? 0,
        eliteCount: eliteCount ?? 0,
        lastUpdatedAt: lastUpdatedRow?.score_updated_at ?? null,
      },
      health,
      generatedAt: now.toISOString(),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    return NextResponse.json(
      {
        ok: false,
        error: message,
        generatedAt: new Date().toISOString(),
      },
      { status: 500 }
    );
  }
}
