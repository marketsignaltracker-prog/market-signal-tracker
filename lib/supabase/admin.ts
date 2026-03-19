import { createClient } from "@supabase/supabase-js";

// Admin client with service role key — only use server-side (webhooks, etc.)
export function createAdminClient() {
  return createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );
}
