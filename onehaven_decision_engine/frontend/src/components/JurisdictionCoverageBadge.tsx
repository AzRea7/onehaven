import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  ShieldAlert,
  ShieldCheck,
} from "lucide-react";

type CoverageLike = {
  completeness_status?: string | null;
  completeness_score?: number | null;
  coverage_confidence?: string | null;
  confidence_label?: string | null;
  production_readiness?: string | null;
  is_stale?: boolean | null;
  stale_warning?: boolean | null;
  stale_reason?: string | null;
  resolved_rule_version?: string | null;
  rule_version?: string | null;
  last_refreshed?: string | null;
  last_refreshed_at?: string | null;
};

function norm(value: unknown) {
  return String(value ?? "")
    .trim()
    .toLowerCase();
}

function pct(value: unknown) {
  const n = Number(value ?? 0);
  if (!Number.isFinite(n)) return "0%";
  return `${Math.round(n * 100)}%`;
}

function formatDate(value: unknown) {
  if (!value) return "—";
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function toneForValue(value: unknown) {
  const v = norm(value);
  if (
    [
      "high",
      "complete",
      "ready",
      "verified",
      "good",
      "pass",
      "fresh",
      "strong",
      "confirmed",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "medium",
      "partial",
      "unknown",
      "attention",
      "pending",
      "conditional",
      "needs_review",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-warn";
  }
  if (
    [
      "low",
      "missing",
      "stale",
      "blocked",
      "not_ready",
      "bad",
      "critical",
      "weak",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

export default function JurisdictionCoverageBadge({
  coverage,
  compact = false,
}: {
  coverage?: CoverageLike | null;
  compact?: boolean;
}) {
  const c = coverage || {};
  const confidence = c.coverage_confidence || c.confidence_label || "unknown";
  const completeness = c.completeness_status || "unknown";
  const readiness = c.production_readiness || "unknown";
  const isStale = Boolean(c.is_stale || c.stale_warning);
  const version = c.resolved_rule_version || c.rule_version || "—";
  const refreshed = c.last_refreshed || c.last_refreshed_at || null;

  if (compact) {
    return (
      <div className="flex flex-wrap items-center gap-2">
        <span className={toneForValue(confidence)}>
          {norm(confidence) === "high" ? (
            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
          ) : norm(confidence) === "low" ? (
            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
          ) : null}
          Confidence: {String(confidence).replace(/_/g, " ")}
        </span>

        <span className={toneForValue(completeness)}>
          <BadgeCheck className="mr-1 h-3.5 w-3.5" />
          {String(completeness).replace(/_/g, " ")}
        </span>

        {isStale ? (
          <span className="oh-pill oh-pill-warn">
            <AlertTriangle className="mr-1 h-3.5 w-3.5" />
            Stale
          </span>
        ) : null}
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className={toneForValue(confidence)}>
          {norm(confidence) === "high" ? (
            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
          ) : norm(confidence) === "low" ? (
            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
          ) : null}
          Coverage confidence: {String(confidence).replace(/_/g, " ")}
        </span>

        <span className={toneForValue(completeness)}>
          <BadgeCheck className="mr-1 h-3.5 w-3.5" />
          Completeness: {String(completeness).replace(/_/g, " ")}
        </span>

        <span className={toneForValue(readiness)}>
          Production: {String(readiness).replace(/_/g, " ")}
        </span>

        {isStale ? (
          <span className="oh-pill oh-pill-warn">
            <AlertTriangle className="mr-1 h-3.5 w-3.5" />
            Stale
          </span>
        ) : (
          <span className="oh-pill oh-pill-good">Fresh</span>
        )}
      </div>

      <div className="mt-3 grid gap-3 md:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Completeness score
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {pct(c.completeness_score)}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Resolved rule version
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">{version}</div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Last refreshed
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {formatDate(refreshed)}
          </div>
        </div>
      </div>

      {isStale && c.stale_reason ? (
        <div className="mt-3 rounded-2xl border border-amber-400/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-50/90">
          <span className="font-semibold text-amber-100">Stale warning:</span>{" "}
          {c.stale_reason}
        </div>
      ) : null}
    </div>
  );
}
