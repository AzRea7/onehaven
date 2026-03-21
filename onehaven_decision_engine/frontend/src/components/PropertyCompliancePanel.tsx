import React from "react";
import {
  ShieldCheck,
  TriangleAlert,
  FileCheck2,
  Building2,
  AlertTriangle,
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
  if (s === "verified" || s === "yes" || s === "ready" || s === "high")
    return "oh-pill oh-pill-good";
  if (
    s === "partial" ||
    s === "medium" ||
    s === "unknown" ||
    s === "conditional"
  )
    return "oh-pill oh-pill-warn";
  if (
    s === "low" ||
    s === "needs_review" ||
    s === "no" ||
    s === "missing" ||
    s === "stale"
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

export default function PropertyCompliancePanel({
  property,
  compliance,
}: {
  property?: PropertyLike;
  compliance?: any;
}) {
  const [brief, setBrief] = React.useState<Brief | null>(compliance || null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (compliance) {
      setBrief(compliance);
      return;
    }

    if (!property?.id) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    api
      .compliancePropertyBrief(property.id)
      .then((out: any) => {
        if (cancelled) return;
        setBrief(out || null);
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

  return (
    <Surface
      title="Compliance posture"
      subtitle="Municipal + inspection + program posture, surfaced in one place instead of buried under five tabs and a small tragedy."
      actions={
        c.production_readiness ? (
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
          title="Could not load compliance brief"
          description={error}
        />
      ) : !brief ? (
        <EmptyState
          compact
          title="No compliance brief yet"
          description="Once the market profile is resolved, the property-level compliance brief will show up here."
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

          <div className="grid gap-3 md:grid-cols-3">
            <Field
              label="PHA workflow"
              value={
                <span
                  className={badgeTone(
                    c.pha_specific_workflow ? "yes" : "unknown",
                  )}
                >
                  {c.pha_specific_workflow
                    ? "Required"
                    : "Not specific / unknown"}
                </span>
              }
            />
            <Field
              label="Coverage confidence"
              value={
                <span className={badgeTone(c.coverage_confidence)}>
                  {c.coverage_confidence || "—"}
                </span>
              }
            />
            <Field
              label="Readiness"
              value={
                <span className={badgeTone(c.production_readiness)}>
                  {c.production_readiness || "—"}
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
              label="Missing categories"
              value={
                Array.isArray(coverage.missing_categories)
                  ? coverage.missing_categories.length
                  : 0
              }
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

          <div className="grid gap-4 xl:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
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
              <div className="text-sm font-semibold text-app-0">
                Blocking items
              </div>

              {blockingItems.length === 0 ? (
                <div className="mt-3 text-sm text-app-4">
                  No blockers returned.
                </div>
              ) : (
                <div className="mt-3 space-y-2">
                  {blockingItems.map((item: any, idx: number) => (
                    <div
                      key={`${item?.code || item?.key || item?.title || idx}`}
                      className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-3 py-3"
                    >
                      <div className="text-sm font-medium text-red-200">
                        {item?.title ||
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
