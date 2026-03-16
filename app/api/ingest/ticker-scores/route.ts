Failed to compile.
./app/api/ingest/ticker-scores/route.ts:434:23
Type error: Property 'signal_key' does not exist on type 'SignalRow'.
  432 |       if (filedAtDiff !== 0) return filedAtDiff
  433 |
> 434 |       return String(a.signal_key || "").localeCompare(String(b.signal_key || ""))
      |                       ^
  435 |     })
  436 |
  437 |     const primary = sorted[0]
Next.js build worker exited with code: 1 and signal: null
Error: Command "npm run build" exited with 1