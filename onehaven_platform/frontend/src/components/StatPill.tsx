import clsx from "clsx";
import { motion } from "framer-motion";

export default function StatPill({
  label,
  value,
  tone = "neutral",
  size = "md",
}: {
  label: string;
  value: string;
  tone?: "good" | "warn" | "bad" | "neutral";
  size?: "sm" | "md";
}) {
  const toneClass =
    tone === "good"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
      : tone === "warn"
        ? "border-amber-300/20 bg-amber-300/10 text-amber-100"
        : tone === "bad"
          ? "border-red-400/20 bg-red-400/10 text-red-100"
          : "border-white/10 bg-white/5 text-white/80";

  const sizeClass =
    size === "sm" ? "px-2.5 py-1 text-[11px]" : "px-3 py-1.5 text-xs";

  return (
    <motion.div
      className={clsx(
        "inline-flex items-center gap-2 rounded-full border",
        toneClass,
        sizeClass,
      )}
      whileHover={{ scale: 1.03 }}
      transition={{ duration: 0.15 }}
    >
      <span className="opacity-70">{label}</span>
      <span className="font-semibold tracking-wide">{value}</span>
    </motion.div>
  );
}
