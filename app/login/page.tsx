"use client";

import { createClient } from "@/lib/supabase/client";
import { useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Turnstile, type TurnstileInstance } from "@marsidev/react-turnstile";

export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [mode, setMode] = useState<"login" | "signup">("login");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [captchaToken, setCaptchaToken] = useState<string | null>(null);
  const captchaRef = useRef<TurnstileInstance>(null);
  const router = useRouter();
  const supabase = createClient();

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!captchaToken) {
      setError("Please complete the captcha verification.");
      return;
    }
    setLoading(true);
    setError("");
    setMessage("");

    if (mode === "signup") {
      const { error } = await supabase.auth.signUp({
        email,
        password,
        options: { captchaToken },
      });
      captchaRef.current?.reset();
      setCaptchaToken(null);
      if (error) {
        setError(error.message);
      } else {
        router.push("/dashboard");
        router.refresh();
      }
    } else {
      const { error } = await supabase.auth.signInWithPassword({
        email,
        password,
        options: { captchaToken },
      });
      captchaRef.current?.reset();
      setCaptchaToken(null);
      if (error) {
        setError(error.message);
      } else {
        router.push("/dashboard");
        router.refresh();
      }
    }
    setLoading(false);
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4"
      style={{
        background:
          "radial-gradient(circle at top, rgba(34, 211, 238, 0.10), transparent 20%), " +
          "radial-gradient(circle at bottom left, rgba(16, 185, 129, 0.08), transparent 24%), " +
          "linear-gradient(to bottom, #080d18, #081122 45%, #080d18)",
      }}
    >
      <div className="w-full max-w-sm">
        {/* Logo */}
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-white">Market Signal Tracker</h1>
          <p className="text-sm text-slate-400 mt-1">
            {mode === "login" ? "Welcome back" : "Create your account"}
          </p>
        </div>

        {/* Card */}
        <div className="bg-white/5 border border-white/10 rounded-2xl p-6 backdrop-blur-sm">
          {/* Email form */}
          <form onSubmit={handleSubmit} className="space-y-3">
            <input
              type="email"
              placeholder="Email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500/50 transition"
            />
            <input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={6}
              className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500/50 transition"
            />

            {/* Turnstile CAPTCHA */}
            <div className="flex justify-center">
              <Turnstile
                ref={captchaRef}
                siteKey="0x4AAAAAACtyjOAQwhbRFOFA"
                onSuccess={(token) => setCaptchaToken(token)}
                onExpire={() => setCaptchaToken(null)}
                onError={() => setCaptchaToken(null)}
                options={{
                  theme: "dark",
                  size: "compact",
                }}
              />
            </div>

            <button
              type="submit"
              disabled={loading || !captchaToken}
              className="w-full bg-cyan-500 hover:bg-cyan-400 text-black font-semibold py-2.5 px-4 rounded-xl transition text-sm disabled:opacity-50"
            >
              {loading ? "..." : mode === "login" ? "Sign In" : "Create Account"}
            </button>
          </form>

          {error && (
            <p className="mt-3 text-xs text-red-400 text-center">{error}</p>
          )}
          {message && (
            <p className="mt-3 text-xs text-emerald-400 text-center">{message}</p>
          )}
        </div>

        {/* Toggle mode */}
        <p className="text-center text-sm text-slate-400 mt-6">
          {mode === "login" ? "Don't have an account?" : "Already have an account?"}{" "}
          <button
            onClick={() => { setMode(mode === "login" ? "signup" : "login"); setError(""); setMessage(""); }}
            className="text-cyan-400 hover:text-cyan-300 transition"
          >
            {mode === "login" ? "Sign up" : "Sign in"}
          </button>
        </p>

        {/* Back */}
        <p className="text-center mt-4">
          <a href="/" className="text-xs text-slate-500 hover:text-slate-400 transition">
            ← Back to home
          </a>
        </p>

        {/* Support */}
        <p className="text-center mt-6 text-[10px] text-slate-600">
          Need help? <a href="mailto:zorvalabs@outlook.com" className="text-slate-500 hover:text-slate-400 transition">zorvalabs@outlook.com</a>
        </p>
      </div>
    </div>
  );
}
