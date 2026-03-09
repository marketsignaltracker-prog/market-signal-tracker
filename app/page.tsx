function FilterChip({
  label,
  value,
  tone,
}: {
  label: string
  value: string
  tone: BoardMode
}) {
  return (
    <span
      className={[
        "inline-flex items-center gap-1 rounded-full border px-3 py-1 text-sm",
        tone === "risk"
          ? "border-rose-400/20 bg-rose-500/10 text-rose-200"
          : "border-emerald-400/20 bg-emerald-500/10 text-emerald-200",
      ].join(" ")}
    >
      <span className="text-slate-300">{label}:</span>
      <span className="font-semibold text-white">{value}</span>
    </span>
  )
}