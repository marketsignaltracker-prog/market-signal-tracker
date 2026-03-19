import { createAdminClient } from "@/lib/supabase/admin";
import { stripe } from "@/lib/stripe";
import { NextRequest, NextResponse } from "next/server";
import Stripe from "stripe";

export async function POST(req: NextRequest) {
  const body = await req.text();
  const sig = req.headers.get("stripe-signature")!;

  let event: Stripe.Event;
  try {
    event = stripe.webhooks.constructEvent(
      body,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET!
    );
  } catch (err) {
    console.error("Webhook signature verification failed:", err);
    return NextResponse.json({ error: "Invalid signature" }, { status: 400 });
  }

  const admin = createAdminClient();

  switch (event.type) {
    case "checkout.session.completed": {
      const session = event.data.object as Stripe.Checkout.Session;
      if (session.subscription && session.customer) {
        const subscription = await stripe.subscriptions.retrieve(
          session.subscription as string
        );
        const userId = subscription.metadata.supabase_user_id;

        if (userId) {
          await admin.from("profiles").update({
            subscription_status: "active",
            subscription_id: subscription.id,
            stripe_customer_id: session.customer as string,
            price_id: subscription.items.data[0]?.price.id,
            updated_at: new Date().toISOString(),
          }).eq("id", userId);
        }
      }
      break;
    }

    case "customer.subscription.updated":
    case "customer.subscription.deleted": {
      const subscription = event.data.object as Stripe.Subscription;
      const userId = subscription.metadata.supabase_user_id;

      if (userId) {
        const status = subscription.status === "active" ? "active"
          : subscription.status === "past_due" ? "past_due"
          : "canceled";

        await admin.from("profiles").update({
          subscription_status: status,
          updated_at: new Date().toISOString(),
        }).eq("id", userId);
      }
      break;
    }
  }

  return NextResponse.json({ received: true });
}
