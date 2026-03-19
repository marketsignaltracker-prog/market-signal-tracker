import Stripe from "stripe";

export const stripe = new Stripe(process.env.STRIPE_SECRET_KEY!, {
  apiVersion: "2026-02-25.clover",
  typescript: true,
});

export const PLANS = {
  pro: {
    priceId: "price_1TCpEDDmvNoGaAxP4OKGNPNB",
    name: "Signal Tracker Pro",
    price: 9.99,
    interval: "month" as const,
  },
} as const;
