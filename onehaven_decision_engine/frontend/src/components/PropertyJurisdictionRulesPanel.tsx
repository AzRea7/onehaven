import React from "react";
import {
  AlertTriangle,
  FileSearch,
  FolderTree,
  Layers3,
  Link2,
  MapPin,
  ShieldAlert,
  ShieldCheck,
} from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";
import JurisdictionCoverageBadge from "./JurisdictionCoverageBadge";

type RuleLayer = {
  layer?: string | null;
  scope?: string | null;
  label?: string | null;
  source?: string | null;
  version?: string | null;
  matched?: boolean | null;
  applied?: boolean | null;
  authority?: string | null;
  confidence?: string | null;
};

type EvidenceRow = {
  label?: string | null;
  title?: string | null;
  url?: string | null;
  source?: string | null;
  source_name?: string | null;
  excerpt?: string | null;
  fetched_at?: string | null;
  is_authoritative?: boolean | null;
};

type SourceSummaryLike = {
  total?: number;
  authoritative_count?: number;
  freshness_counts?: Record<string, number> | null;
  refresh_state_counts?: Record<string, number> | null;
  validation_state_counts?: Record<string, number> | null;
  next_refresh_due_at?: string | null;
  next_validation_due_at?: string | null;
};

type LockoutLike = {
  lockout_active?: boolean | null;
  lockout_reason?: string | null;
  critical_stale_categories?: string[] | null;
  stale_categories?: string[] | null;
};

type NextActionsLike = {
  next_step?: string | null;
  next_search_retry_due_at?: string | null;
  refresh_state?: string | null;
};

type OperationalStatusLike = {
  health_state?: string | null;
  refresh_state?: string | null;
  refresh_status_reason?: string | null;
  reliability_state?: string | null;
  safe_to_rely_on?: boolean | null;
  trustworthy_for_projection?: boolean | null;
  review_required?: boolean | null;
  reasons?: string[] | null;
  lockout?: LockoutLike | null;
  next_actions?: NextActionsLike | null;
  source_summary?: SourceSummaryLike | null;
};

type ResolvedProfile = {
  scope?: string | null;
  state?: string | null;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
  resolved_rule_version?: string | null;
  rule_version?: string | null;
  last_refreshed?: string | null;
  last_refreshed_at?: string | null;
  completeness_status?: string | null;
  completeness_score?: number | null;
  coverage_confidence?: string | null;
  confidence_label?: string | null;
  production_readiness?: string | null;
  trustworthy_for_projection?: boolean | null;
  is_stale?: boolean | null;
  stale_warning?: boolean | null;
  stale_reason?: string | null;
  required_categories?: string[] | null;
  covered_categories?: string[] | null;
  missing_categories?: string[] | null;
  stale_categories?: string[] | null;
  conflicting_categories?: string[] | null;
  source_evidence?: EvidenceRow[] | null;
  evidence?: EvidenceRow[] | null;
  evidence_rows?: EvidenceRow[] | null;
  layers?: RuleLayer[] | null;
  resolved_layers?: RuleLayer[] | null;
  completeness?: {
    required_categories?: string[] | null;
    covered_categories?: string[] | null;
    missing_categories?: string[] | null;
    stale_categories?: string[] | null;
    conflicting_categories?: string[] | null;
  } | null;
  operational_status?: OperationalStatusLike | null;
  health?: OperationalStatusLike | null;
};

function toArray<T = any>(v: any): T[] {
  return Array.isArray(v) ? v : [];
}

function norm(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function titleize(v: any) {
  return String(v ?? "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function formatDate(value: unknown) {
  if (!value) return "—";
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function rowTone(v: any) {
  const s = norm(v);
  if (
    [
      "applied",
      "matched",
      "yes",
      "true",
      "high",
      "verified",
      "healthy",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "partial",
      "medium",
      "unknown",
      "review",
      "degraded",
      "validating",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-warn";
  }
  if (["missing", "low", "false", "stale", "blocked", "failed"].includes(s)) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

function renderCountChips(
  counts?: Record<string, number> | null,
  tone: "good" | "warn" | "bad" | "neutral" = "neutral",
) {
  const items = Object.entries(counts || {}).filter(
    ([, value]) => Number(value || 0) > 0,
  );
  if (!items.length) return <span className="text-sm text-app-4">None</span>;
  return items.map(([key, value]) => (
    <span
      key={key}
      className={
        tone === "good"
          ? "oh-pill oh-pill-good"
          : tone === "warn"
            ? "oh-pill oh-pill-warn"
            : tone === "bad"
              ? "oh-pill oh-pill-bad"
              : "oh-pill"
      }
    >
      {titleize(key)} · {value}
    </span>
  ));
}

function LayerCard({ row }: { row: RuleLayer }) {
  const layer = row.layer || row.scope || row.label || "layer";
  const applied = row.applied ?? row.matched ?? false;
  return (
    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-app-0">
          {titleize(layer)}
        </div>
        <span className={applied ? "oh-pill oh-pill-good" : "oh-pill"}>
          {applied ? "Applied" : "Available"}
        </span>
      </div>

      <div className="mt-3 grid gap-2 text-sm text-app-2">
        <div>
          <span className="text-app-4">Authority:</span>{" "}
          {row.authority || row.source || "—"}
        </div>
        <div>
          <span className="text-app-4">Version:</span> {row.version || "—"}
        </div>
        <div>
          <span className="text-app-4">Confidence:</span>{" "}
          <span className={rowTone(row.confidence)}>
            {titleize(row.confidence || "unknown")}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function PropertyJurisdictionRulesPanel({
  profile,
}: {
  profile?: ResolvedProfile | null;
}) {
  const p = profile || {};
  const operational = p.operational_status || p.health || null;
  const layers = toArray<RuleLayer>(p.resolved_layers || p.layers);
  const evidence = toArray<EvidenceRow>(
    p.source_evidence || p.evidence || p.evidence_rows,
  );
  const covered = toArray<string>(
    p.covered_categories || p.completeness?.covered_categories,
  );
  const missing = toArray<string>(
    p.missing_categories || p.completeness?.missing_categories,
  );
  const stale = toArray<string>(
    p.stale_categories || p.completeness?.stale_categories,
  );
  const conflicting = toArray<string>(
    p.conflicting_categories || p.completeness?.conflicting_categories,
  );
  const locationBits = [p.city, p.county, p.state].filter(Boolean).join(", ");
  const safeToRely = Boolean(operational?.safe_to_rely_on);
  const lockout = operational?.lockout;
  const sourceSummary = operational?.source_summary;
  const reasons = toArray<string>(operational?.reasons);

  return (
    <Surface
      title="Jurisdiction trust and rule overlays"
      subtitle="Resolved local compliance layers, health state, lockout posture, freshness, validation, and source-backed evidence."
    >
      {!profile ? (
        <EmptyState
          title="No jurisdiction profile loaded"
          description="Select a property or load its compliance brief to see the resolved layered rules and trust posture."
        />
      ) : (
        <div className="space-y-4">
          <JurisdictionCoverageBadge coverage={{
            completeness_status: p.completeness_status,
            completeness_score: p.completeness_score,
            is_stale: p.is_stale,
            stale_reason: p.stale_reason,
          }} />

          {lockout?.lockout_active || reasons.length ? (
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-start gap-2 text-sm text-app-1">
                {safeToRely ? (
                  <ShieldCheck className="mt-0.5 h-4 w-4 shrink-0" />
                ) : (
                  <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0" />
                )}
                <div>
                  <div className="font-semibold text-app-0">
                    {safeToRely
                      ? "Safe to rely on"
                      : "Do not rely on this jurisdiction state yet"}
                  </div>
                  <div className="mt-1 text-app-2">
                    {lockout?.lockout_reason ||
                      reasons[0] ||
                      p.stale_reason ||
                      "This jurisdiction still needs review."}
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <MapPin className="h-4 w-4" />
                Resolved market
              </div>
              <div className="mt-3 space-y-2 text-sm text-app-2">
                <div>
                  <span className="text-app-4">Location:</span>{" "}
                  {locationBits || "—"}
                </div>
                <div>
                  <span className="text-app-4">PHA / overlay:</span>{" "}
                  {p.pha_name || "—"}
                </div>
                <div>
                  <span className="text-app-4">Resolved version:</span>{" "}
                  {p.resolved_rule_version || p.rule_version || "—"}
                </div>
                <div>
                  <span className="text-app-4">Last refreshed:</span>{" "}
                  {formatDate(p.last_refreshed || p.last_refreshed_at)}
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <ShieldCheck className="h-4 w-4" />
                Operational trust
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <span
                  className={rowTone(
                    operational?.health_state || operational?.refresh_state,
                  )}
                >
                  {titleize(
                    operational?.health_state ||
                      operational?.refresh_state ||
                      "unknown",
                  )}
                </span>
                <span
                  className={
                    safeToRely
                      ? "oh-pill oh-pill-good"
                      : rowTone(operational?.reliability_state)
                  }
                >
                  {safeToRely
                    ? "Safe to rely on"
                    : titleize(
                        operational?.reliability_state || "review_required",
                      )}
                </span>
                {lockout?.lockout_active ? (
                  <span className="oh-pill oh-pill-bad">Lockout active</span>
                ) : null}
              </div>
              <div className="mt-3 text-sm text-app-3">
                {operational?.refresh_status_reason ||
                  p.stale_reason ||
                  "No operational status reason returned."}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <FolderTree className="h-4 w-4" />
                Source freshness
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {renderCountChips(sourceSummary?.freshness_counts, "warn")}
              </div>
              <div className="mt-3 text-sm text-app-3">
                Next refresh: {formatDate(sourceSummary?.next_refresh_due_at)}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <FileSearch className="h-4 w-4" />
                Validation and next step
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {renderCountChips(
                  sourceSummary?.validation_state_counts,
                  "warn",
                )}
              </div>
              <div className="mt-3 text-sm text-app-3">
                Next step:{" "}
                {titleize(operational?.next_actions?.next_step || "monitor")}
              </div>
              {operational?.next_actions?.next_search_retry_due_at ? (
                <div className="mt-1 text-xs text-app-4">
                  Retry/search due{" "}
                  {formatDate(
                    operational.next_actions.next_search_retry_due_at,
                  )}
                </div>
              ) : null}
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Covered categories
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {covered.length ? (
                  covered.map((item) => (
                    <span key={item} className="oh-pill oh-pill-good">
                      {titleize(item)}
                    </span>
                  ))
                ) : (
                  <span className="text-sm text-app-4">None yet</span>
                )}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Missing / stale / conflicting
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {missing.map((item) => (
                  <span key={`m-${item}`} className="oh-pill oh-pill-warn">
                    Missing: {titleize(item)}
                  </span>
                ))}
                {stale.map((item) => (
                  <span key={`s-${item}`} className="oh-pill oh-pill-warn">
                    Stale: {titleize(item)}
                  </span>
                ))}
                {conflicting.map((item) => (
                  <span key={`c-${item}`} className="oh-pill oh-pill-bad">
                    Conflict: {titleize(item)}
                  </span>
                ))}
                {!missing.length && !stale.length && !conflicting.length ? (
                  <span className="oh-pill oh-pill-good">
                    No known risk categories
                  </span>
                ) : null}
              </div>
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <Layers3 className="h-4 w-4" />
              Resolved layers
            </div>
            {!layers.length ? (
              <div className="mt-3 text-sm text-app-4">
                No explicit layer rows returned for this profile.
              </div>
            ) : (
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                {layers.map((row, idx) => (
                  <LayerCard
                    key={`${row.layer || row.scope || row.label || "layer"}-${idx}`}
                    row={row}
                  />
                ))}
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <Link2 className="h-4 w-4" />
              Source evidence
            </div>
            {!evidence.length ? (
              <div className="mt-3 text-sm text-app-4">
                No evidence rows returned.
              </div>
            ) : (
              <div className="mt-3 grid gap-3">
                {evidence.map((row, idx) => (
                  <div
                    key={`${row.title || row.label || row.url || "evidence"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {row.title ||
                          row.label ||
                          row.source_name ||
                          "Evidence"}
                      </div>
                      <span
                        className={
                          row.is_authoritative
                            ? "oh-pill oh-pill-good"
                            : "oh-pill"
                        }
                      >
                        {row.is_authoritative ? "Authoritative" : "Supporting"}
                      </span>
                    </div>
                    <div className="mt-2 text-sm text-app-3">
                      {row.source_name || row.source || "Unknown source"}
                    </div>
                    {row.excerpt ? (
                      <div className="mt-3 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-2">
                        {row.excerpt}
                      </div>
                    ) : null}
                    <div className="mt-3 flex flex-wrap items-center gap-3 text-sm text-app-4">
                      <span>Fetched: {formatDate(row.fetched_at)}</span>
                      {row.url ? (
                        <a
                          href={row.url}
                          target="_blank"
                          rel="noreferrer"
                          className="text-cyan-300 hover:text-cyan-200"
                        >
                          Open source
                        </a>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </Surface>
  );
}
