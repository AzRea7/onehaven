import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Clock3,
  ShieldAlert,
  ShieldCheck,
} from "lucide-react";

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

type SourceAuthoritySummary = {
  authoritative_count?: number | null;
  authority_use_counts?: Record<string, number> | null;
  source_authority_score?: number | null;
  linked_source_ids?: number[] | null;
} | null;

type OperationalStatusLike = {
  health_state?: string | null;
  refresh_state?: string | null;
  refresh_status_reason?: string | null;
  reliability_state?: string | null;
  safe_to_rely_on?: boolean | null;
  trustworthy_for_projection?: boolean | null;
  review_required?: boolean | null;
  reasons?: string[] | null;
  next_actions?: NextActionsLike | null;
  lockout?: LockoutLike | null;
  source_summary?: SourceAuthoritySummary;
  last_validation_at?: string | null;
  next_due_step?: string | null;
  lockout_causing_categories?: string[] | null;
  informational_gap_categories?: string[] | null;
  validation_pending_categories?: string[] | null;
  authority_gap_categories?: string[] | null;
};

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
  trustworthy_for_projection?: boolean | null;
  missing_categories?: string[] | null;
  conflicting_categories?: string[] | null;
  covered_categories?: string[] | null;
  required_categories?: string[] | null;
  completeness?: {
    completeness_status?: string | null;
    completeness_score?: number | null;
    is_stale?: boolean | null;
    stale_reason?: string | null;
    missing_categories?: string[] | null;
    conflicting_categories?: string[] | null;
  } | null;
  operational_status?: OperationalStatusLike | null;
  health?: OperationalStatusLike | null;
  lockout?: LockoutLike | null;
  next_actions?: NextActionsLike | null;
  lockout_causing_categories?: string[] | null;
  informational_gap_categories?: string[] | null;
  validation_pending_categories?: string[] | null;
  authority_gap_categories?: string[] | null;
  last_validation_at?: string | null;
  next_due_step?: string | null;
  repo_artifact_support_state?: string | null;
  repo_policy_raw_count?: number | null;
  repo_pdf_count?: number | null;
  repo_pdf_names?: string[] | null;
  source_authority_score?: number | null;
  source_summary?: SourceAuthoritySummary;
};

function norm(value: unknown) {
  return String(value ?? "")
    .trim()
    .toLowerCase();
}

function titleize(value: unknown) {
  return String(value ?? "unknown")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
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
      "healthy",
      "ok",
      "safe_to_rely_on",
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
      "review_required",
      "degraded",
      "validating",
      "warning",
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
      "failed",
      "unsafe_to_rely_on",
      "conflicting",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

function deriveOperationalStatus(c: CoverageLike) {
  const operational = c.operational_status || c.health || null;
  const lockout = c.lockout || operational?.lockout || null;
  const nextActions = c.next_actions || operational?.next_actions || null;
  const healthState =
    operational?.health_state || operational?.refresh_state || "unknown";
  const reliabilityState = operational?.reliability_state || "unknown";
  const safeToRely = Boolean(
    operational?.safe_to_rely_on ||
    (c.trustworthy_for_projection && !lockout?.lockout_active),
  );
  const reasons = Array.isArray(operational?.reasons)
    ? operational?.reasons.filter(Boolean)
    : [];
  return {
    operational,
    lockout,
    nextActions,
    healthState,
    reliabilityState,
    safeToRely,
    reasons,
  };
}

function dueNowSummary(
  lockoutCausing: string[],
  informationalGaps: string[],
  validationPending: string[],
  authorityGaps: string[],
  conflicting: string[],
  missing: string[],
  isStale: boolean,
) {
  const parts: string[] = [];
  if (lockoutCausing.length)
    parts.push(
      `${lockoutCausing.length} blocking categor${lockoutCausing.length === 1 ? "y" : "ies"}`,
    );
  if (conflicting.length)
    parts.push(
      `${conflicting.length} conflict${conflicting.length === 1 ? "" : "s"}`,
    );
  if (missing.length)
    parts.push(
      `${missing.length} missing categor${missing.length === 1 ? "y" : "ies"}`,
    );
  if (validationPending.length)
    parts.push(`${validationPending.length} validation pending`);
  if (authorityGaps.length)
    parts.push(
      `${authorityGaps.length} authority gap${authorityGaps.length === 1 ? "" : "s"}`,
    );
  if (informationalGaps.length)
    parts.push(
      `${informationalGaps.length} informational gap${informationalGaps.length === 1 ? "" : "s"}`,
    );
  if (isStale) parts.push("freshness follow-up needed");
  return parts.join(" · ");
}

export default function JurisdictionCoverageBadge({
  coverage,
  compact = false,
  reviewQueueCount,
}: {
  coverage?: CoverageLike | null;
  compact?: boolean;
  reviewQueueCount?: number | null;
}) {
  const c = coverage || {};
  const confidence = c.coverage_confidence || c.confidence_label || "unknown";
  const completeness =
    c.completeness_status || c.completeness?.completeness_status || "unknown";
  const readiness = c.production_readiness || "unknown";
  const isStale = Boolean(
    c.is_stale || c.stale_warning || c.completeness?.is_stale,
  );
  const version = c.resolved_rule_version || c.rule_version || "—";
  const refreshed = c.last_refreshed || c.last_refreshed_at || null;
  const {
    lockout,
    nextActions,
    healthState,
    reliabilityState,
    safeToRely,
    reasons,
    operational,
  } = deriveOperationalStatus(c);

  const lockoutCausing = Array.isArray(c.lockout_causing_categories)
    ? c.lockout_causing_categories
    : Array.isArray(operational?.lockout_causing_categories)
      ? operational.lockout_causing_categories
      : [];
  const informationalGaps = Array.isArray(c.informational_gap_categories)
    ? c.informational_gap_categories
    : Array.isArray(operational?.informational_gap_categories)
      ? operational.informational_gap_categories
      : [];
  const validationPending = Array.isArray(c.validation_pending_categories)
    ? c.validation_pending_categories
    : Array.isArray(operational?.validation_pending_categories)
      ? operational.validation_pending_categories
      : [];
  const authorityGaps = Array.isArray(c.authority_gap_categories)
    ? c.authority_gap_categories
    : Array.isArray(operational?.authority_gap_categories)
      ? operational.authority_gap_categories
      : [];
  const missingCategories = Array.isArray(c.missing_categories)
    ? c.missing_categories
    : Array.isArray(c.completeness?.missing_categories)
      ? c.completeness.missing_categories
      : [];
  const conflictingCategories = Array.isArray(c.conflicting_categories)
    ? c.conflicting_categories
    : Array.isArray(c.completeness?.conflicting_categories)
      ? c.completeness.conflicting_categories
      : [];
  const lastValidationAt =
    c.last_validation_at || operational?.last_validation_at || null;
  const nextDueStep =
    c.next_due_step ||
    operational?.next_due_step ||
    nextActions?.next_step ||
    null;

  const sourceSummary = c.source_summary || operational?.source_summary || null;
  const sourceAuthorityScore = Number(
    c.source_authority_score ?? sourceSummary?.source_authority_score ?? 0,
  );
  const authoritativeCount = Number(sourceSummary?.authoritative_count ?? 0);
  const authorityUseCounts = sourceSummary?.authority_use_counts || {};

  if (compact) {
    return (
      <div className="flex flex-wrap items-center gap-2">
        <span className={toneForValue(healthState)}>
          {norm(healthState) === "healthy" || norm(healthState) === "ok" ? (
            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
          ) : (
            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
          )}
          {titleize(healthState)}
        </span>

        <span className={toneForValue(reliabilityState)}>
          {safeToRely ? (
            <BadgeCheck className="mr-1 h-3.5 w-3.5" />
          ) : (
            <AlertTriangle className="mr-1 h-3.5 w-3.5" />
          )}
          {safeToRely ? "Safe to rely on" : titleize(reliabilityState)}
        </span>

        {Number(reviewQueueCount || 0) > 0 ? (
          <span className="oh-pill oh-pill-warn">
            Review Queue · {reviewQueueCount}
          </span>
        ) : null}

        {!!missingCategories.length ? (
          <span className="oh-pill oh-pill-warn">
            Missing · {missingCategories.length}
          </span>
        ) : null}
        {!!conflictingCategories.length ? (
          <span className="oh-pill oh-pill-bad">
            Conflicts · {conflictingCategories.length}
          </span>
        ) : null}
        {!!authorityGaps.length ? (
          <span className="oh-pill oh-pill-bad">
            Authority gaps · {authorityGaps.length}
          </span>
        ) : null}
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className={toneForValue(healthState)}>
          {norm(healthState) === "healthy" || norm(healthState) === "ok" ? (
            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
          ) : (
            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
          )}
          Health: {titleize(healthState)}
        </span>

        <span className={toneForValue(confidence)}>
          Coverage confidence: {titleize(confidence)}
        </span>

        <span className={toneForValue(completeness)}>
          <BadgeCheck className="mr-1 h-3.5 w-3.5" />
          Completeness: {titleize(completeness)}
        </span>

        <span className={toneForValue(readiness)}>
          Production: {titleize(readiness)}
        </span>

        <span className={toneForValue(reliabilityState)}>
          {safeToRely ? (
            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
          ) : (
            <AlertTriangle className="mr-1 h-3.5 w-3.5" />
          )}
          {safeToRely ? "Safe to rely on" : titleize(reliabilityState)}
        </span>

        {lockout?.lockout_active ? (
          <span className="oh-pill oh-pill-bad">
            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
            Lockout active
          </span>
        ) : isStale ? (
          <span className="oh-pill oh-pill-warn">
            <AlertTriangle className="mr-1 h-3.5 w-3.5" />
            Stale
          </span>
        ) : (
          <span className="oh-pill oh-pill-good">Fresh</span>
        )}
      </div>

      {(lockout?.lockout_reason ||
        reasons[0] ||
        c.stale_reason ||
        c.completeness?.stale_reason) && (
        <div className="mt-3 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-1">
          {lockout?.lockout_reason ||
            reasons[0] ||
            c.stale_reason ||
            c.completeness?.stale_reason}
        </div>
      )}

      {dueNowSummary(
        lockoutCausing,
        informationalGaps,
        validationPending,
        authorityGaps,
        conflictingCategories,
        missingCategories,
        isStale,
      ) ? (
        <div className="mt-3 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-1">
          <div className="font-semibold text-app-0">Why due now</div>
          <div className="mt-1">
            {dueNowSummary(
              lockoutCausing,
              informationalGaps,
              validationPending,
              authorityGaps,
              conflictingCategories,
              missingCategories,
              isStale,
            )}
          </div>
        </div>
      ) : null}

      <div className="mt-3 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Completeness score
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {pct(c.completeness_score ?? c.completeness?.completeness_score)}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Missing
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {missingCategories.length}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Conflicts
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {conflictingCategories.length}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Authoritative sources
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {authoritativeCount}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Source authority
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {pct(sourceAuthorityScore)}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Binding sources
          </div>
          <div className="mt-2 text-sm font-semibold text-app-0">
            {Number(authorityUseCounts["binding"] || 0)}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Rule version
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

      {(missingCategories.length ||
        conflictingCategories.length ||
        authorityGaps.length ||
        validationPending.length) && (
        <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
              Missing categories
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
              {missingCategories.length ? (
                missingCategories.map((item) => (
                  <span key={item} className="oh-pill oh-pill-warn">
                    {titleize(item)}
                  </span>
                ))
              ) : (
                <span className="text-sm text-app-4">None</span>
              )}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
              Conflicting categories
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
              {conflictingCategories.length ? (
                conflictingCategories.map((item) => (
                  <span key={item} className="oh-pill oh-pill-bad">
                    {titleize(item)}
                  </span>
                ))
              ) : (
                <span className="text-sm text-app-4">None</span>
              )}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
              Authority gaps
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
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

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
              Next validation
            </div>
            <div className="mt-2 space-y-1 text-sm text-app-2">
              <div>{formatDate(lastValidationAt)}</div>
              <div className="text-app-4">
                {titleize(nextDueStep || "monitor")}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
