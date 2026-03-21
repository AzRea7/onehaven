import React from "react";
import {
  ShieldCheck,
  TriangleAlert,
  FileCheck2,
  Building2,
  AlertTriangle,
  ClipboardX,
  Wrench,
  ShieldAlert,
  BadgeCheck,
} from "lucide-react";
import { api } from "../lib/api";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

type PropertyLike = {
  id?: number;
  state?: string | null;
  county?: string | null;
  city?: string | null;
  strategy?: string | null;
};

type Brief = {
  ok?: boolean;
  market?: {
    state?: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
  };
  compliance?: {
    market_label?: string;
    registration_required?: string | null;
    inspection_required?: string | null;
    certificate_required_before_occupancy?: string | null;
    pha_specific_workflow?: boolean | null;
    coverage_confidence?: string | null;
    production_readiness?: string | null;
  };
  explanation?: string | null;
  required_actions?: any[];
  blocking_items?: any[];
  coverage?: {
    completeness_status?: string | null;
    completeness_score?: number | null;
    is_stale?: boolean | null;
    stale_reason?: string | null;
    missing_categories?: string[] | null;
    covered_categories?: string[] | null;
    required_categories?: string[] | null;
  };
};

function fmtBoolish(v: any) {
  if (v == null || v === "unknown") return "Unknown";
  if (v === true || String(v).toLowerCase() === "yes") return "Yes";
  if (v === false || String(v).toLowerCase() === "no") return "No";
  return String(v);
}

function badgeTone(v: any) {
  const s = String(v || "").toLowerCase();
  if (
    s === "verified" ||
    s === "yes" ||
    s === "ready" ||
    s === "high" ||
    s === "complete"
  )
    return "oh-pill oh-pill-good";
  if (
    s === "partial" ||
    s === "medium" ||
    s === "unknown" ||
    s === "conditional" ||
    s === "attention" ||
    s === "in_progress"
  )
    return "oh-pill oh-pill-warn";
  if (
    s === "low" ||
    s === "needs_review" ||
    s === "no" ||
    s === "missing" ||
    s === "stale" ||
    s === "blocked" ||
    s === "critical_failures" ||
    s === "needs_remediation" ||
    s === "not_ready"
  )
    return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
        {label}
      </div>
      <div className="mt-2 text-sm font-semibold text-app-0">{value}</div>
    </div>
  );
}

function titleCase(v: any) {
  return String(v || "")
    .replace(/_/g, " ")
    .trim();
}

export default function PropertyCompliancePanel({
  property,
  compliance,
}: {
  property?: PropertyLike;
  compliance?: any;
}) {
  const [brief, setBrief] = React.useState<Brief | null>(compliance || null);
  const [readiness, setReadiness] = React.useState<any | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!property?.id) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    Promise.allSettled([
      compliance
        ? Promise.resolve(compliance)
        : api.compliancePropertyBrief(property.id),
      api.complianceInspectionReadiness(property.id),
    ])
      .then((results) => {
        if (cancelled) return;

        const briefRes = results[0];
        const readinessRes = results[1];

        if (briefRes.status === "fulfilled") {
          setBrief((briefRes.value as any) || null);
        }

        if (readinessRes.status === "fulfilled") {
          setReadiness((readinessRes.value as any) || null);
        }

        if (
          briefRes.status === "rejected" &&
          readinessRes.status === "rejected"
        ) {
          throw briefRes.reason || readinessRes.reason;
        }
      })
      .catch((e: any) => {
        if (cancelled) return;
        setError(String(e?.message || e));
      })
      .finally(() => {
        if (cancelled) return;
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [property?.id, compliance]);

  const c = brief?.compliance || {};
  const m = brief?.market || {};
  const coverage = brief?.coverage || {};
  const requiredActions = Array.isArray(brief?.required_actions)
    ? brief?.required_actions
    : [];
  const blockingItems = Array.isArray(brief?.blocking_items)
    ? brief?.blocking_items
    : [];

  const readinessState = readiness?.readiness || {};
  const readinessSummary = readiness?.readiness_summary || {};
  const readinessMeta = readinessSummary?.readiness || {};
  const completionMeta = readinessSummary?.completion || {};
  const readinessCounts = readiness?.counts || {};
  const readinessBlockingItems = Array.isArray(readiness?.blocking_items)
    ? readiness.blocking_items
    : [];
  const readinessWarningItems = Array.isArray(readiness?.warning_items)
    ? readiness.warning_items
    : [];
  const recommendedActions = Array.isArray(readiness?.recommended_actions)
    ? readiness.recommended_actions
    : [];
  const failureActions = Array.isArray(
    readiness?.inspection_failure_actions?.recommended_actions,
  )
    ? readiness.inspection_failure_actions.recommended_actions
    : [];

  const mergedBlockingItems =
    readinessBlockingItems.length > 0 ? readinessBlockingItems : blockingItems;

  return (
    <Surface
      title="Compliance posture"
      subtitle="Municipal rules, inspection readiness, blockers, and remediation in one place instead of hiding the real answer behind pretty labels."
      actions={
        readiness?.posture ? (
          <span className={badgeTone(readiness.posture)}>
            {titleCase(readiness.posture)}
          </span>
        ) : c.production_readiness ? (
          <span className={badgeTone(c.production_readiness)}>
            {String(c.production_readiness).replace(/_/g, " ")}
          </span>
        ) : null
      }
    >
      {loading ? (
        <div className="grid gap-3">
          <div className="oh-skeleton h-[72px] rounded-2xl" />
          <div className="oh-skeleton h-[72px] rounded-2xl" />
          <div className="oh-skeleton h-[72px] rounded-2xl" />
        </div>
      ) : error ? (
        <EmptyState
          compact
          title="Could not load compliance data"
          description={error}
        />
      ) : !brief && !readiness ? (
        <EmptyState
          compact
          title="No compliance data yet"
          description="Once the market profile and inspection readiness are computed, the property-level compliance view will show up here."
        />
      ) : (
        <div className="space-y-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <Field
              label="Market"
              value={
                <span className="flex items-center gap-2">
                  <Building2 className="h-4 w-4 text-app-4" />
                  {c.market_label ||
                    [m.city, m.county, m.state].filter(Boolean).join(", ") ||
                    "—"}
                </span>
              }
            />
            <Field
              label="Registration"
              value={
                <span className={badgeTone(c.registration_required)}>
                  {fmtBoolish(c.registration_required)}
                </span>
              }
            />
            <Field
              label="Inspection"
              value={
                <span className={badgeTone(c.inspection_required)}>
                  {fmtBoolish(c.inspection_required)}
                </span>
              }
            />
            <Field
              label="Certificate before occupancy"
              value={
                <span
                  className={badgeTone(c.certificate_required_before_occupancy)}
                >
                  {fmtBoolish(c.certificate_required_before_occupancy)}
                </span>
              }
            />
          </div>

          <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
            <Field
              label="Readiness score"
              value={
                readiness?.score_pct != null
                  ? `${Number(readiness.score_pct).toFixed(1)}%`
                  : "—"
              }
            />
            <Field
              label="Completion"
              value={
                readiness?.completion_pct != null
                  ? `${Number(readiness.completion_pct).toFixed(1)}%`
                  : completionMeta?.pct != null
                    ? `${Number(completionMeta.pct).toFixed(1)}%`
                    : "—"
              }
            />
            <Field
              label="Projection"
              value={
                readiness?.completion_projection_pct != null
                  ? `${Number(readiness.completion_projection_pct).toFixed(1)}%`
                  : completionMeta?.projection_pct != null
                    ? `${Number(completionMeta.projection_pct).toFixed(1)}%`
                    : "—"
              }
            />
            <Field
              label="Readiness status"
              value={
                <span
                  className={badgeTone(
                    readinessState?.status || readinessMeta?.status,
                  )}
                >
                  {titleCase(
                    readinessState?.status || readinessMeta?.status || "—",
                  )}
                </span>
              }
            />
            <Field
              label="Result status"
              value={
                <span
                  className={badgeTone(
                    readinessState?.result_status ||
                      readinessMeta?.result_status,
                  )}
                >
                  {titleCase(
                    readinessState?.result_status ||
                      readinessMeta?.result_status ||
                      "—",
                  )}
                </span>
              }
            />
            <Field
              label="Posture"
              value={
                <span
                  className={badgeTone(
                    readiness?.posture || readinessMeta?.posture,
                  )}
                >
                  {titleCase(
                    readiness?.posture || readinessMeta?.posture || "—",
                  )}
                </span>
              }
            />
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <Field
              label="HQS"
              value={
                <span
                  className={badgeTone(
                    readinessState?.hqs_ready ? "ready" : "blocked",
                  )}
                >
                  {readinessState?.hqs_ready == null
                    ? "—"
                    : readinessState.hqs_ready
                      ? "Ready"
                      : "Blocked"}
                </span>
              }
            />
            <Field
              label="Local"
              value={
                <span
                  className={badgeTone(
                    readinessState?.local_ready ? "ready" : "blocked",
                  )}
                >
                  {readinessState?.local_ready == null
                    ? "—"
                    : readinessState.local_ready
                      ? "Ready"
                      : "Blocked"}
                </span>
              }
            />
            <Field
              label="Voucher"
              value={
                <span
                  className={badgeTone(
                    readinessState?.voucher_ready ? "ready" : "blocked",
                  )}
                >
                  {readinessState?.voucher_ready == null
                    ? "—"
                    : readinessState.voucher_ready
                      ? "Ready"
                      : "Blocked"}
                </span>
              }
            />
            <Field
              label="Lease-up"
              value={
                <span
                  className={badgeTone(
                    readinessState?.lease_up_ready ? "ready" : "blocked",
                  )}
                >
                  {readinessState?.lease_up_ready == null
                    ? "—"
                    : readinessState.lease_up_ready
                      ? "Ready"
                      : "Blocked"}
                </span>
              }
            />
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <Field
              label="Completeness status"
              value={
                <span className={badgeTone(coverage.completeness_status)}>
                  {coverage.completeness_status || "—"}
                </span>
              }
            />
            <Field
              label="Completeness score"
              value={
                coverage.completeness_score != null
                  ? Number(coverage.completeness_score).toFixed(2)
                  : "—"
              }
            />
            <Field
              label="Stale"
              value={
                <span
                  className={badgeTone(coverage.is_stale ? "stale" : "ready")}
                >
                  {coverage.is_stale ? "Yes" : "No"}
                </span>
              }
            />
            <Field
              label="Failed / blocked"
              value={`${readinessCounts?.failing ?? 0} / ${readinessCounts?.blocking ?? 0}`}
            />
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <Field
              label="Latest inspection passed"
              value={
                <span
                  className={badgeTone(
                    readinessState?.latest_inspection_passed
                      ? "ready"
                      : "blocked",
                  )}
                >
                  {readinessState?.latest_inspection_passed == null
                    ? "—"
                    : readinessState.latest_inspection_passed
                      ? "Yes"
                      : "No"}
                </span>
              }
            />
            <Field
              label="Inspection failed items"
              value={readinessCounts?.inspection_failed_items ?? "—"}
            />
            <Field
              label="Critical failed items"
              value={readinessCounts?.inspection_failed_critical_items ?? "—"}
            />
          </div>

          {coverage.is_stale ||
          (coverage.missing_categories || []).length > 0 ? (
            <div className="rounded-2xl border border-amber-400/20 bg-amber-500/10 px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-amber-100">
                <AlertTriangle className="h-4 w-4" />
                Jurisdiction warning
              </div>
              <div className="mt-2 text-sm leading-6 text-amber-50/90">
                {coverage.is_stale
                  ? `Jurisdiction data is stale${coverage.stale_reason ? `: ${coverage.stale_reason}` : "."}`
                  : "Jurisdiction data is present but not fully complete."}
              </div>

              {Array.isArray(coverage.missing_categories) &&
              coverage.missing_categories.length > 0 ? (
                <div className="mt-3 flex flex-wrap gap-2">
                  {coverage.missing_categories.map((item) => (
                    <span key={item} className="oh-pill oh-pill-warn">
                      {item}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}

          {brief?.explanation ? (
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <FileCheck2 className="h-4 w-4" />
                Explanation
              </div>
              <div className="mt-2 text-sm leading-6 text-app-3">
                {brief.explanation}
              </div>
            </div>
          ) : null}

          {mergedBlockingItems.length > 0 ? (
            <div className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-red-200">
                <ClipboardX className="h-4 w-4" />
                Blocking findings
              </div>

              <div className="mt-3 space-y-2">
                {mergedBlockingItems
                  .slice(0, 6)
                  .map((item: any, idx: number) => (
                    <div
                      key={`${item?.rule_key || item?.code || item?.key || item?.title || idx}`}
                      className="rounded-2xl border border-red-500/20 bg-red-500/[0.04] px-3 py-3"
                    >
                      <div className="text-sm font-medium text-red-200">
                        {item?.label ||
                          item?.title ||
                          item?.description ||
                          item?.code ||
                          item?.key ||
                          "Untitled blocker"}
                      </div>
                      <div className="mt-1 text-xs text-red-200/70">
                        {(
                          item?.category ||
                          item?.severity ||
                          "uncategorized"
                        ).toString()}
                      </div>
                      {item?.suggested_fix ? (
                        <div className="mt-2 text-sm leading-6 text-red-100/85">
                          {item.suggested_fix}
                        </div>
                      ) : null}
                    </div>
                  ))}
              </div>
            </div>
          ) : null}

          {readinessWarningItems.length > 0 ? (
            <div className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-amber-100">
                <ShieldAlert className="h-4 w-4" />
                Warning findings
              </div>

              <div className="mt-3 space-y-2">
                {readinessWarningItems
                  .slice(0, 4)
                  .map((item: any, idx: number) => (
                    <div
                      key={`${item?.rule_key || item?.code || item?.key || item?.title || idx}`}
                      className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.04] px-3 py-3"
                    >
                      <div className="text-sm font-medium text-amber-100">
                        {item?.label ||
                          item?.title ||
                          item?.description ||
                          item?.code ||
                          item?.key ||
                          "Untitled warning"}
                      </div>
                      <div className="mt-1 text-xs text-amber-100/70">
                        {(
                          item?.category ||
                          item?.severity ||
                          "uncategorized"
                        ).toString()}
                      </div>
                      {item?.suggested_fix ? (
                        <div className="mt-2 text-sm leading-6 text-amber-50/90">
                          {item.suggested_fix}
                        </div>
                      ) : null}
                    </div>
                  ))}
              </div>
            </div>
          ) : null}

          <div className="grid gap-4 xl:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <BadgeCheck className="h-4 w-4" />
                Required actions
              </div>

              {requiredActions.length === 0 ? (
                <div className="mt-3 text-sm text-app-4">
                  No required actions returned.
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {requiredActions.map((item: any, idx: number) => (
                    <div
                      key={`${item?.code || item?.key || item?.title || idx}`}
                      className="rounded-2xl border border-app bg-app-muted px-3 py-3"
                    >
                      <div className="text-sm font-medium text-app-0">
                        {item?.title ||
                          item?.description ||
                          item?.code ||
                          item?.key ||
                          "Untitled action"}
                      </div>
                      <div className="mt-1 text-xs text-app-4">
                        {(
                          item?.category ||
                          item?.severity ||
                          "uncategorized"
                        ).toString()}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <Wrench className="h-4 w-4" />
                Remediation actions
              </div>

              {recommendedActions.length === 0 &&
              failureActions.length === 0 ? (
                <div className="mt-3 text-sm text-app-4">
                  No remediation actions returned.
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {(recommendedActions.length > 0
                    ? recommendedActions
                    : failureActions
                  )
                    .slice(0, 6)
                    .map((item: any, idx: number) => (
                      <div
                        key={`${item?.rule_key || item?.code || item?.title || idx}`}
                        className="rounded-2xl border border-app bg-app-muted px-3 py-3"
                      >
                        <div className="text-sm font-medium text-app-0">
                          {item?.label ||
                            item?.title ||
                            item?.description ||
                            item?.code ||
                            "Untitled remediation"}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {(
                            item?.category ||
                            item?.severity ||
                            item?.priority ||
                            "uncategorized"
                          ).toString()}
                        </div>
                        {item?.suggested_fix || item?.notes ? (
                          <div className="mt-2 text-sm leading-6 text-app-3">
                            {item?.suggested_fix || item?.notes}
                          </div>
                        ) : null}
                      </div>
                    ))}
                </div>
              )}
            </div>
          </div>

          {m?.pha_name ? (
            <div className="flex items-center gap-2 text-xs text-app-4">
              <ShieldCheck className="h-3.5 w-3.5" />
              PHA: {m.pha_name}
            </div>
          ) : (
            <div className="flex items-center gap-2 text-xs text-app-4">
              <TriangleAlert className="h-3.5 w-3.5" />
              No specific PHA override shown in this brief
            </div>
          )}
        </div>
      )}
    </Surface>
  );
}
