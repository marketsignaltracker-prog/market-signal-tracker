import Stripe from "stripe";

export const stripe = new Stripe(process.env.STRIPE_SECRET_KEY!);

export const PLANS = {
  proMonthly: {
    priceId: "price_1TCpEDDmvNoGaAxP4OKGNPNB",
    name: "Market Signal Tracker Pro",
    price: 9.99,
    interval: "month" as const,
  },
  proYearly: {
    priceId: "price_1TCq9HDmvNoGaAxP6rgDacJv",
    name: "Market Signal Tracker Pro",
    price: 99.99,
    interval: "year" as const,
  },
} as const;
