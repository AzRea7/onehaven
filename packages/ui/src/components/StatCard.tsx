import React from "react";
import { motion } from "framer-motion";
import type { LucideIcon } from "lucide-react";

function fmtMoney(n: any) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function fmtNum(n: any, digits = 2) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

export default function StatCard(props: {
  title: string;
  value: string | number | null | undefined;
  hint?: string;
  tone?: "good" | "warn" | "bad" | "neutral";
  icon?: LucideIcon;
  format?: "money" | "num" | "raw";
}) {
  const tone =
    props.tone === "good"
      ? "border-emerald-500/30"
      : props.tone === "warn"
        ? "border-amber-500/30"
        : props.tone === "bad"
          ? "border-red-500/30"
          : "border-zinc-800/80";

  const Icon = props.icon;

  let display = props.value as any;
  if (props.format === "money") display = fmtMoney(display);
  if (props.format === "num") display = fmtNum(display);

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35 }}
      className={`gradient-border rounded-2xl glass glass-hover p-4 ${tone}`}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs text-zinc-400">{props.title}</div>
          <div className="mt-1 text-lg font-semibold tracking-tight">
            {display ?? "—"}
          </div>
          {props.hint && (
            <div className="mt-1 text-xs text-zinc-500">{props.hint}</div>
          )}
        </div>

        {Icon && (
          <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-2">
            <Icon className="h-4 w-4 text-zinc-200" />
          </div>
        )}
      </div>
    </motion.div>
  );
}
