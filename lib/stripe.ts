import Stripe from "stripe";

let _stripe: Stripe | null = null;

export function getStripe(): Stripe {
  if (!_stripe) {
    const key = process.env.STRIPE_SECRET_KEY;
    if (!key) throw new Error("Missing STRIPE_SECRET_KEY environment variable");
    _stripe = new Stripe(key);
  }
  return _stripe;
}

/** @deprecated Use getStripe() instead — kept for backward compat */
export const stripe = new Proxy({} as Stripe, {
  get(_target, prop) {
    return (getStripe() as any)[prop];
  },
});

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
