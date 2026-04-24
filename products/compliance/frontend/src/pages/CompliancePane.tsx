import React from "react";
import {
  AlertTriangle,
  CalendarClock,
  ClipboardCheck,
  Eye,
  RefreshCcw,
} from "lucide-react";
import PageShell from "onehaven_onehaven_platform/frontend/src/shell/PageShell";
import PageHero from "onehaven_onehaven_platform/frontend/src/shell/PageHero";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";
import PropertyJurisdictionRulesPanel from "products/compliance/frontend/src/components/PropertyJurisdictionRulesPanel";
import JurisdictionCoverageBadge from "products/compliance/frontend/src/components/JurisdictionCoverageBadge";
import { api } from "@/lib/api";

type ComplianceRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  county?: string;
  urgency?: string;
  blockers?: string[];
  jurisdiction?: {
    completeness_status?: string;
    completeness_score?: number | null;
    is_stale?: boolean;
    coverage_confidence?: string | null;
    confidence_label?: string | null;
    production_readiness?: string | null;
    missing_categories?: string[] | null;
    conflicting_categories?: string[] | null;
    covered_categories?: string[] | null;
    required_categories?: string[] | null;
    safe_to_rely_on?: boolean | null;
    health_state?: string | null;
    reliability_state?: string | null;
    validation_pending_categories?: string[] | null;
    authority_gap_categories?: string[] | null;
    lockout_causing_categories?: string[] | null;
    source_authority_score?: number | null;
    source_summary?: {
      authoritative_count?: number | null;
      authority_use_counts?: Record<string, number> | null;
      source_authority_score?: number | null;
    } | null;
  };
};

type PanePayload = {
  rows?: ComplianceRow[];
  count?: number;
  kpis?: Record<string, any>;
};

type PolicyResolvedRulesPayload = {
  property?: {
    id?: number;
    address?: string | null;
    city?: string | null;
    county?: string | null;
    state?: string | null;
  } | null;
  profile?: any | null;
  brief?: any | null;
  operational_status?: any | null;
  operational_health?: any | null;
  safe_to_rely_on?: boolean | null;
  unsafe_reasons?: string[] | null;
};

function labelize(value?: string | null) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function urgencyTone(urgency?: string | null) {
  const v = String(urgency || "").toLowerCase();
  if (v === "critical" || v === "high") return "oh-pill oh-pill-bad";
  if (v === "warning" || v === "medium") return "oh-pill oh-pill-warn";
  if (v === "info" || v === "low") return "oh-pill oh-pill-good";
  return "oh-pill";
}

function statusTone(value?: string | boolean | null) {
  const v = String(value ?? "").toLowerCase();
  if (["true", "pass", "ready", "confirmed", "ok", "healthy"].includes(v)) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "false",
      "fail",
      "blocked",
      "critical",
      "not_ready",
      "failed",
      "conflicting",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-bad";
  }
  if (
    [
      "attention",
      "warn",
      "warning",
      "pending",
      "unknown",
      "draft",
      "scheduled",
      "stale",
      "partial",
      "review_required",
    ].includes(v)
  ) {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
}

function confidenceTone(value?: string | null) {
  const v = String(value || "").toLowerCase();
  if (["high", "strong", "verified", "info"].includes(v))
    return "oh-pill oh-pill-good";
  if (["medium", "partial", "unknown", "warning"].includes(v))
    return "oh-pill oh-pill-warn";
  if (["low", "weak"].includes(v)) return "oh-pill oh-pill-bad";
  return "oh-pill";
}

export default function CompliancePane() {
  const [data, setData] = React.useState<PanePayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);
  const [selectedId, setSelectedId] = React.useState<number | null>(null);
  const [selectedResolvedRules, setSelectedResolvedRules] =
    React.useState<PolicyResolvedRulesPayload | null>(null);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [detailError, setDetailError] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const out = await api.get<PanePayload>("/compliance/queue?limit=100");
      setData(out);
      setErr(null);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const rows = Array.isArray(data?.rows) ? data.rows : [];

  React.useEffect(() => {
    if (!rows.length) {
      setSelectedId(null);
      return;
    }
    if (
      selectedId == null ||
      !rows.some((row) => row.property_id === selectedId)
    ) {
      setSelectedId(rows[0].property_id);
    }
  }, [rows, selectedId]);

  const selectedRow = React.useMemo(
    () => rows.find((row) => row.property_id === selectedId) || null,
    [rows, selectedId],
  );

  React.useEffect(() => {
    if (!selectedRow?.property_id) {
      setSelectedResolvedRules(null);
      setDetailError(null);
      return;
    }

    let cancelled = false;
    setDetailLoading(true);
    setDetailError(null);

    api
      .get(`/policy/property/${selectedRow.property_id}/resolved-rules`)
      .then((out) => {
        if (!cancelled)
          setSelectedResolvedRules((out as PolicyResolvedRulesPayload) || null);
      })
      .catch((e: any) => {
        if (!cancelled) {
          setSelectedResolvedRules(null);
          setDetailError(String(e?.message || e));
        }
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedRow?.property_id]);

  const weakCoverageCount = rows.filter((row) =>
    ["low", "partial", "medium", "unknown", "warning"].includes(
      String(
        row.jurisdiction?.coverage_confidence ||
          row.jurisdiction?.confidence_label ||
          "",
      ).toLowerCase(),
    ),
  ).length;

  const missingCoverageCount = rows.filter(
    (row) => (row.jurisdiction?.missing_categories || []).length > 0,
  ).length;

  const conflictingCoverageCount = rows.filter(
    (row) => (row.jurisdiction?.conflicting_categories || []).length > 0,
  ).length;

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Lifecycle pane"
          title="Compliance / S8 pane"
          subtitle="Property-scoped compliance, trust status, missing categories, conflicts, source authority, and local rule coverage now live in one pane."
          actions={
            <div className="flex flex-wrap gap-3">
              <button
                onClick={() => void refresh()}
                className="oh-btn oh-btn-secondary"
              >
                <RefreshCcw className="h-4 w-4" />
                Refresh pane
              </button>
            </div>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
          <Surface
            title="Visible properties"
            subtitle="Properties currently routed here"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(
                data?.kpis?.total_properties || data?.count || rows.length || 0,
              )}
            </div>
          </Surface>

          <Surface
            title="Weak coverage"
            subtitle="Low confidence or partial rules"
          >
            <div className="text-3xl font-semibold text-app-0">
              {weakCoverageCount}
            </div>
          </Surface>

          <Surface
            title="Missing categories"
            subtitle="Properties with uncovered requirements"
          >
            <div className="text-3xl font-semibold text-app-0">
              {missingCoverageCount}
            </div>
          </Surface>

          <Surface
            title="Conflicting categories"
            subtitle="Properties needing conflict resolution"
          >
            <div className="text-3xl font-semibold text-app-0">
              {conflictingCoverageCount}
            </div>
          </Surface>

          <Surface
            title="Review blockers"
            subtitle="Trust or authority issues still blocking use"
          >
            <div className="text-3xl font-semibold text-app-0">
              {
                rows.filter(
                  (row) => !Boolean(row.jurisdiction?.safe_to_rely_on),
                ).length
              }
            </div>
          </Surface>
        </div>

        <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <Surface
            title="Compliance queue"
            subtitle="Select a property to inspect trust posture, coverage gaps, conflicts, and source authority."
          >
            {loading ? (
              <div className="grid gap-3">
                {Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="oh-skeleton h-[132px] rounded-3xl" />
                ))}
              </div>
            ) : !rows.length ? (
              <EmptyState
                icon={ClipboardCheck}
                title="No compliance properties"
                description="Nothing is currently routed into the compliance pane."
              />
            ) : (
              <div className="grid gap-4">
                {rows.map((row) => {
                  const isSelected = row.property_id === selectedId;
                  const jurisdiction = row.jurisdiction || {};
                  const confidence =
                    jurisdiction.coverage_confidence ||
                    jurisdiction.confidence_label ||
                    undefined;

                  return (
                    <button
                      key={row.property_id}
                      type="button"
                      onClick={() => setSelectedId(row.property_id)}
                      className={[
                        "w-full rounded-3xl border px-5 py-4 text-left transition",
                        isSelected
                          ? "border-app-strong bg-app-muted"
                          : "border-app bg-app-panel hover:border-app-strong hover:bg-app-muted",
                      ].join(" ")}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-4">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="text-base font-semibold text-app-0">
                              {row.address || `Property #${row.property_id}`}
                            </div>
                            {row.urgency ? (
                              <span className={urgencyTone(row.urgency)}>
                                {labelize(row.urgency)}
                              </span>
                            ) : null}
                            {jurisdiction.health_state ? (
                              <span
                                className={statusTone(
                                  jurisdiction.health_state,
                                )}
                              >
                                {labelize(jurisdiction.health_state)}
                              </span>
                            ) : null}
                            {jurisdiction.safe_to_rely_on === true ? (
                              <span className="oh-pill oh-pill-good">
                                Safe to rely on
                              </span>
                            ) : (
                              <span className="oh-pill oh-pill-bad">
                                Review required
                              </span>
                            )}
                          </div>

                          <div className="mt-1 text-sm text-app-4">
                            {[row.city, row.state].filter(Boolean).join(", ")}
                            {row.county ? ` · ${row.county}` : ""}
                          </div>

                          <div className="mt-3 flex flex-wrap gap-2">
                            {jurisdiction.completeness_status ? (
                              <span
                                className={statusTone(
                                  jurisdiction.completeness_status,
                                )}
                              >
                                {labelize(jurisdiction.completeness_status)}
                              </span>
                            ) : null}
                            {jurisdiction.production_readiness ? (
                              <span
                                className={statusTone(
                                  jurisdiction.production_readiness,
                                )}
                              >
                                {labelize(jurisdiction.production_readiness)}
                              </span>
                            ) : null}
                            {confidence ? (
                              <span className={confidenceTone(confidence)}>
                                Coverage: {labelize(confidence)}
                              </span>
                            ) : null}
                            {jurisdiction.source_summary?.authoritative_count !=
                            null ? (
                              <span className="oh-pill">
                                Authority:{" "}
                                {
                                  jurisdiction.source_summary
                                    .authoritative_count
                                }
                              </span>
                            ) : null}
                            {jurisdiction.source_authority_score != null ? (
                              <span className="oh-pill">
                                Authority score:{" "}
                                {Math.round(
                                  Number(
                                    jurisdiction.source_authority_score || 0,
                                  ) * 100,
                                )}
                                %
                              </span>
                            ) : null}
                          </div>

                          {!!jurisdiction.missing_categories?.length ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {jurisdiction.missing_categories
                                .slice(0, 3)
                                .map((reason) => (
                                  <span
                                    key={reason}
                                    className="oh-pill oh-pill-warn"
                                  >
                                    missing: {labelize(reason)}
                                  </span>
                                ))}
                            </div>
                          ) : null}

                          {!!jurisdiction.conflicting_categories?.length ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {jurisdiction.conflicting_categories
                                .slice(0, 3)
                                .map((reason) => (
                                  <span
                                    key={reason}
                                    className="oh-pill oh-pill-bad"
                                  >
                                    conflict: {labelize(reason)}
                                  </span>
                                ))}
                            </div>
                          ) : null}
                        </div>

                        <div className="shrink-0 text-app-4">
                          <Eye className="h-4 w-4" />
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </Surface>

          <div className="space-y-4">
            {!selectedRow ? (
              <Surface
                title="Selected property"
                subtitle="Choose a property from the compliance queue."
              >
                <EmptyState
                  compact
                  icon={ClipboardCheck}
                  title="No property selected"
                  description="Select a property to inspect trust posture, missing categories, conflicting categories, and source authority."
                />
              </Surface>
            ) : detailError ? (
              <Surface
                tone="danger"
                title="Selected property"
                subtitle="Could not load details"
              >
                <div className="text-sm text-red-300">{detailError}</div>
              </Surface>
            ) : detailLoading && !selectedResolvedRules ? (
              <Surface
                title="Selected property"
                subtitle="Loading property-scoped trust and rule state"
              >
                <div className="grid gap-3">
                  <div className="oh-skeleton h-[120px] rounded-2xl" />
                  <div className="oh-skeleton h-[180px] rounded-2xl" />
                </div>
              </Surface>
            ) : (
              <>
                <Surface
                  title="Selected property trust summary"
                  subtitle="This top summary reflects the current market trust and authority posture."
                >
                  <JurisdictionCoverageBadge
                    coverage={{
                      ...selectedRow.jurisdiction,
                      ...(selectedResolvedRules?.profile || {}),
                      operational_status:
                        selectedResolvedRules?.operational_status ||
                        selectedResolvedRules?.operational_health ||
                        (selectedResolvedRules?.profile || {})
                          .operational_status ||
                        null,
                    }}
                  />
                </Surface>

                <PropertyJurisdictionRulesPanel
                  profile={
                    (selectedResolvedRules?.profile as any) ||
                    ({
                      ...selectedRow.jurisdiction,
                      operational_status:
                        selectedResolvedRules?.operational_status ||
                        selectedResolvedRules?.operational_health ||
                        null,
                    } as any)
                  }
                />
              </>
            )}
          </div>
        </div>
      </div>
    </PageShell>
  );
}
