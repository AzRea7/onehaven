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
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";
import JurisdictionCoverageBadge from "products/compliance/frontend/src/components/JurisdictionCoverageBadge";

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
  authority_tier?: string | null;
  authority_use_type?: string | null;
};

type SourceSummaryLike = {
  total?: number;
  authoritative_count?: number;
  freshness_counts?: Record<string, number> | null;
  refresh_state_counts?: Record<string, number> | null;
  validation_state_counts?: Record<string, number> | null;
  next_refresh_due_at?: string | null;
  next_validation_due_at?: string | null;
  source_authority_score?: number | null;
  authority_use_counts?: Record<string, number> | null;
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
  last_validation_at?: string | null;
  next_due_step?: string | null;
  lockout_causing_categories?: string[] | null;
  informational_gap_categories?: string[] | null;
  validation_pending_categories?: string[] | null;
  authority_gap_categories?: string[] | null;
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
  source_authority_score?: number | null;
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
  legally_unsafe?: boolean | null;
  informationally_incomplete?: boolean | null;
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
      "binding",
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
      "supporting",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-warn";
  }
  if (
    [
      "missing",
      "low",
      "false",
      "stale",
      "blocked",
      "failed",
      "weak",
      "conflicting",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

function BoundaryCallout({
  title,
  body,
  tone = "warn",
}: {
  title: string;
  body: React.ReactNode;
  tone?: "warn" | "bad" | "good";
}) {
  const cls =
    tone === "bad"
      ? "border-red-400/25 bg-red-500/10 text-red-100"
      : tone === "good"
        ? "border-emerald-400/25 bg-emerald-500/10 text-emerald-100"
        : "border-amber-300/25 bg-amber-500/10 text-amber-100";
  return (
    <div className={`rounded-2xl border px-4 py-4 ${cls}`}>
      <div className="text-sm font-semibold">{title}</div>
      <div className="mt-2 text-sm leading-6">{body}</div>
    </div>
  );
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

function EvidenceCard({ row }: { row: EvidenceRow }) {
  const authorityLabel =
    row.authority_tier ||
    row.authority_use_type ||
    (row.is_authoritative ? "authoritative" : "supporting");
  return (
    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-app-0">
          {row.title || row.label || "Evidence"}
        </div>
        <span className={rowTone(authorityLabel)}>
          {titleize(authorityLabel)}
        </span>
      </div>
      <div className="mt-2 text-sm text-app-2">
        {row.source_name || row.source || "Unknown source"}
      </div>
      {row.excerpt ? (
        <div className="mt-2 text-sm text-app-3">{row.excerpt}</div>
      ) : null}
      <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-app-4">
        <span>Fetched: {formatDate(row.fetched_at)}</span>
        {row.url ? (
          <a
            href={row.url}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 text-app-1 hover:underline"
          >
            Open source <Link2 className="h-3.5 w-3.5" />
          </a>
        ) : null}
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
  const lockoutCausing = toArray<string>(
    operational?.lockout_causing_categories,
  );
  const informationalGaps = toArray<string>(
    operational?.informational_gap_categories,
  );
  const validationPending = toArray<string>(
    operational?.validation_pending_categories,
  );
  const authorityGaps = toArray<string>(operational?.authority_gap_categories);
  const legallyUnsafe = Boolean(p.legally_unsafe);
  const informationallyIncomplete = Boolean(p.informationally_incomplete);
  const locationBits = [p.city, p.county, p.state].filter(Boolean).join(", ");
  const safeToRely = Boolean(operational?.safe_to_rely_on);
  const lockout = operational?.lockout;
  const sourceSummary = operational?.source_summary;
  const reasons = toArray<string>(operational?.reasons);
  const authoritativeCount = Number(sourceSummary?.authoritative_count || 0);
  const authorityUseCounts = sourceSummary?.authority_use_counts || {};
  const sourceAuthorityScore = Number(
    p.source_authority_score ?? sourceSummary?.source_authority_score ?? 0,
  );

  return (
    <Surface
      title="Jurisdiction trust and rule overlays"
      subtitle="Resolved local compliance layers, missing/conflicting categories, trust posture, freshness, validation, and source-backed authority."
    >
      {!profile ? (
        <EmptyState
          title="No jurisdiction profile loaded"
          description="Select a property or load its compliance brief to see the resolved layered rules and trust posture."
        />
      ) : (
        <div className="space-y-4">
          <BoundaryCallout
            title="Legal and product boundary"
            tone={
              safeToRely
                ? "good"
                : legallyUnsafe || lockout?.lockout_active
                  ? "bad"
                  : "warn"
            }
            body={
              <>
                OneHaven distinguishes between{" "}
                <strong>operational trust</strong> and{" "}
                <strong>legal clearance</strong>. Even when this view is safe to
                rely on operationally, it is not legal advice and it is not a
                legal compliance guarantee.{" "}
                {!safeToRely
                  ? "A human or legal review is still needed before treating this property as cleared."
                  : "You should still verify critical requirements with the authoritative jurisdiction source before final external use."}
              </>
            }
          />

          <JurisdictionCoverageBadge
            coverage={{
              completeness_status: p.completeness_status,
              completeness_score: p.completeness_score,
              is_stale: p.is_stale,
              stale_reason: p.stale_reason,
              operational_status: operational,
              lockout_causing_categories: lockoutCausing,
              informational_gap_categories: informationalGaps,
              validation_pending_categories: validationPending,
              authority_gap_categories: authorityGaps,
              last_validation_at: operational?.last_validation_at,
              next_due_step: operational?.next_due_step,
              last_refreshed: p.last_refreshed,
              last_refreshed_at: p.last_refreshed_at,
              resolved_rule_version: p.resolved_rule_version,
              rule_version: p.rule_version,
              coverage_confidence: p.coverage_confidence,
              confidence_label: p.confidence_label,
              production_readiness: p.production_readiness,
              trustworthy_for_projection: p.trustworthy_for_projection,
              missing_categories: missing,
              conflicting_categories: conflicting,
              source_authority_score: sourceAuthorityScore,
              source_summary: sourceSummary || undefined,
            }}
          />

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
                Trust status
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
                    safeToRely ? "oh-pill oh-pill-good" : "oh-pill oh-pill-bad"
                  }
                >
                  {safeToRely ? "Safe to rely on" : "Review required"}
                </span>
                <span className={rowTone(p.production_readiness || "unknown")}>
                  {titleize(p.production_readiness || "unknown")}
                </span>
              </div>
              {lockout?.lockout_reason || reasons[0] ? (
                <div className="mt-3 text-sm text-app-3">
                  {lockout?.lockout_reason || reasons[0]}
                </div>
              ) : null}
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <FileSearch className="h-4 w-4" />
                Coverage gaps
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {missing.length ? (
                  missing.map((item) => (
                    <span key={item} className="oh-pill oh-pill-warn">
                      {titleize(item)}
                    </span>
                  ))
                ) : (
                  <span className="text-sm text-app-4">
                    No missing categories
                  </span>
                )}
              </div>
              {stale.length ? (
                <div className="mt-3 flex flex-wrap gap-2">
                  {stale.map((item) => (
                    <span key={item} className="oh-pill oh-pill-warn">
                      stale: {titleize(item)}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <AlertTriangle className="h-4 w-4" />
                Conflicts and authority
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                {conflicting.length ? (
                  conflicting.map((item) => (
                    <span key={item} className="oh-pill oh-pill-bad">
                      {titleize(item)}
                    </span>
                  ))
                ) : (
                  <span className="text-sm text-app-4">
                    No category conflicts
                  </span>
                )}
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <span className="oh-pill">
                  Authoritative: {authoritativeCount}
                </span>
                <span className="oh-pill">
                  Binding: {Number(authorityUseCounts["binding"] || 0)}
                </span>
                <span className="oh-pill">
                  Authority: {Math.round(sourceAuthorityScore * 100)}%
                </span>
              </div>
            </div>
          </div>

          {lockoutCausing.length ||
          validationPending.length ||
          authorityGaps.length ? (
            <div className="grid gap-3 md:grid-cols-3">
              <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                <div className="text-sm font-semibold text-app-0">
                  Blocking categories
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {lockoutCausing.length ? (
                    lockoutCausing.map((item) => (
                      <span key={item} className="oh-pill oh-pill-bad">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
              <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                <div className="text-sm font-semibold text-app-0">
                  Validation pending
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {validationPending.length ? (
                    validationPending.map((item) => (
                      <span key={item} className="oh-pill oh-pill-warn">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
              <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                <div className="text-sm font-semibold text-app-0">
                  Authority gaps
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {authorityGaps.length ? (
                    authorityGaps.map((item) => (
                      <span key={item} className="oh-pill oh-pill-bad">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
            </div>
          ) : null}

          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <Layers3 className="h-4 w-4" />
                Rule layers
              </div>
              <div className="mt-3 grid gap-3">
                {layers.length ? (
                  layers.map((row, idx) => (
                    <LayerCard
                      key={`${row.layer || row.label || "layer"}-${idx}`}
                      row={row}
                    />
                  ))
                ) : (
                  <span className="text-sm text-app-4">
                    No resolved layers available.
                  </span>
                )}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <FolderTree className="h-4 w-4" />
                Source authority and evidence
              </div>
              <div className="mt-3 grid gap-3">
                <div className="flex flex-wrap gap-2">
                  <span className="oh-pill">
                    Authoritative sources · {authoritativeCount}
                  </span>
                  <span className="oh-pill">
                    Binding · {Number(authorityUseCounts["binding"] || 0)}
                  </span>
                  <span className="oh-pill">
                    Supporting · {Number(authorityUseCounts["supporting"] || 0)}
                  </span>
                  <span className="oh-pill">
                    Authority score · {Math.round(sourceAuthorityScore * 100)}%
                  </span>
                </div>
                {evidence.length ? (
                  evidence
                    .slice(0, 8)
                    .map((row, idx) => (
                      <EvidenceCard
                        key={`${row.url || row.title || row.label || "evidence"}-${idx}`}
                        row={row}
                      />
                    ))
                ) : (
                  <span className="text-sm text-app-4">
                    No source evidence rows available.
                  </span>
                )}
              </div>
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="text-sm font-semibold text-app-0">
              Category coverage summary
            </div>
            <div className="mt-3 grid gap-3 md:grid-cols-3">
              <div>
                <div className="text-xs uppercase tracking-[0.14em] text-app-4">
                  Covered
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {covered.length ? (
                    covered.map((item) => (
                      <span key={item} className="oh-pill oh-pill-good">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.14em] text-app-4">
                  Missing
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {missing.length ? (
                    missing.map((item) => (
                      <span key={item} className="oh-pill oh-pill-warn">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.14em] text-app-4">
                  Conflicting
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {conflicting.length ? (
                    conflicting.map((item) => (
                      <span key={item} className="oh-pill oh-pill-bad">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </Surface>
  );
}
