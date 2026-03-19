import Stripe from "stripe";

export const stripe = new Stripe(process.env.STRIPE_SECRET_KEY!, {
  apiVersion: "2026-02-25.clover",
  typescript: true,
});

export const PLANS = {
  proMonthly: {
    priceId: "price_1TCpEDDmvNoGaAxP4OKGNPNB",
    name: "Signal Tracker Pro",
    price: 9.99,
    interval: "month" as const,
  },
  proYearly: {
    priceId: "price_1TCq9HDmvNoGaAxP6rgDacJv",
    name: "Signal Tracker Pro",
    price: 99.99,
    interval: "year" as const,
  },
} as const;
