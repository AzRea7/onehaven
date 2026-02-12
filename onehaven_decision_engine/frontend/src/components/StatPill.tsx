import clsx from "clsx";
import { motion } from "framer-motion";

export default function StatPill({
  label,
  value,
  tone = "neutral",
}: {
  label: string;
  value: string;
  tone?: "good" | "warn" | "bad" | "neutral";
}) {
  const toneClass =
    tone === "good"
      ? "border-green-400/20 bg-green-400/10 text-green-200"
      : tone === "warn"
        ? "border-yellow-300/20 bg-yellow-300/10 text-yellow-100"
        : tone === "bad"
          ? "border-red-400/20 bg-red-400/10 text-red-200"
          : "border-white/10 bg-white/5 text-white/80";

  return (
    <motion.div
      className={clsx(
        "rounded-full border px-3 py-1 text-xs inline-flex items-center gap-2",
        toneClass,
      )}
      whileHover={{ scale: 1.03 }}
      transition={{ duration: 0.15 }}
    >
      <span className="opacity-70">{label}</span>
      <span className="font-semibold tracking-wide">{value}</span>
    </motion.div>
  );
}
