import React from "react";
import type { LucideIcon } from "lucide-react";
import clsx from "clsx";

type Tone = "default" | "success" | "warning" | "danger" | "accent";

const toneClass: Record<Tone, string> = {
  default: "",
  success: "oh-kpi-success",
  warning: "oh-kpi-warning",
  danger: "oh-kpi-danger",
  accent: "oh-kpi-accent",
};

export default function KpiCard({
  title,
  value,
  subtitle,
  icon: Icon,
  tone = "default",
  className,
}: {
  title: string;
  value: React.ReactNode;
  subtitle?: string;
  icon?: LucideIcon;
  tone?: Tone;
  className?: string;
}) {
  return (
    <div className={clsx("oh-kpi-card", toneClass[tone], className)}>
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            {title}
          </div>
          <div className="mt-2 text-2xl font-semibold tracking-tight text-app-0">
            {value}
          </div>
          {subtitle ? (
            <div className="mt-2 text-sm text-app-3">{subtitle}</div>
          ) : null}
        </div>

        {Icon ? (
          <div className="oh-kpi-icon">
            <Icon className="h-4 w-4" />
          </div>
        ) : null}
      </div>
    </div>
  );
}
