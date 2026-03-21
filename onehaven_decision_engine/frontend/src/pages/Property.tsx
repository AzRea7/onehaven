import React from "react";
import { useParams } from "react-router-dom";
import {
  Home,
  ShieldCheck,
  AlertTriangle,
  ClipboardList,
  RefreshCcw,
  Wrench,
  FileCheck2,
} from "lucide-react";

import { api } from "../lib/api";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import InspectionReadiness from "../components/InspectionReadiness";
import PropertyCompliancePanel from "../components/PropertyCompliancePanel";
import NextActionsPanel from "../components/NextActionsPanel";

function titleCase(v: any) {
  return String(v || "")
    .replace(/_/g, " ")
    .trim();
}

function pillTone(v: any) {
  const s = String(v || "").toLowerCase();
  if (["ready", "pass", "inspection_ready", "complete", "good"].includes(s)) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "blocked",
      "fail",
      "critical_failures",
      "needs_remediation",
      "not_ready",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-bad";
  }
  if (["attention", "unknown", "in_progress", "needs_work"].includes(s)) {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
}

export default function PropertyPage() {
  const { id: routeId, propertyId: legacyPropertyId } = useParams();
  const rawId = routeId ?? legacyPropertyId;
  const propertyId = Number(rawId);
  const hasValidPropertyId = Number.isFinite(propertyId) && propertyId > 0;

  const [property, setProperty] = React.useState<any>(null);
  const [workflow, setWorkflow] = React.useState<any>(null);
  const [complianceBrief, setComplianceBrief] = React.useState<any>(null);
  const [inspectionReadiness, setInspectionReadiness] =
    React.useState<any>(null);
  const [loading, setLoading] = React.useState(true);
  const [automationBusy, setAutomationBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const load = React.useCallback(async () => {
    if (!hasValidPropertyId) {
      setProperty(null);
      setWorkflow(null);
      setComplianceBrief(null);
      setInspectionReadiness(null);
      setError("Invalid property id.");
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const results = await Promise.allSettled([
        api.property(propertyId),
        api.propertyWorkflow(propertyId),
        api.compliancePropertyBrief(propertyId),
        api.complianceInspectionReadiness(propertyId),
      ]);

      const [propertyRes, workflowRes, briefRes, readinessRes] = results;

      setProperty(
        propertyRes.status === "fulfilled" ? (propertyRes.value ?? null) : null,
      );
      setWorkflow(
        workflowRes.status === "fulfilled" ? (workflowRes.value ?? null) : null,
      );
      setComplianceBrief(
        briefRes.status === "fulfilled" ? (briefRes.value ?? null) : null,
      );
      setInspectionReadiness(
        readinessRes.status === "fulfilled"
          ? (readinessRes.value ?? null)
          : null,
      );

      const failedAll = results.every((r) => r.status === "rejected");
      if (failedAll) {
        const reasons = results
          .filter((r): r is PromiseRejectedResult => r.status === "rejected")
          .map((r) =>
            String(r.reason?.message || r.reason || "Request failed"),
          );
        throw new Error(reasons.join(" | "));
      }
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [hasValidPropertyId, propertyId]);

  React.useEffect(() => {
    load();
  }, [load]);

  async function runComplianceAutomation() {
    if (!hasValidPropertyId) return;

    try {
      setAutomationBusy(true);
      await api.complianceAutomationRun(propertyId, true);
      await load();
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setAutomationBusy(false);
    }
  }

  const p = property || {};
  const readiness = inspectionReadiness || {};
  const readinessState = readiness?.readiness || {};
  const readinessSummary = readiness?.readiness_summary || {};
  const readinessMeta = readinessSummary?.readiness || {};
  const completionMeta = readinessSummary?.completion || {};
  const workflowSummary = workflow || {};

  const nextActions = Array.isArray(workflowSummary?.next_actions)
    ? workflowSummary.next_actions
    : Array.isArray(workflowSummary?.outstanding_tasks?.next_actions)
      ? workflowSummary.outstanding_tasks.next_actions
      : Array.isArray(readiness?.recommended_actions)
        ? readiness.recommended_actions
        : [];

  const blockers =
    readiness?.blocking_items ||
    workflowSummary?.outstanding_tasks?.blockers ||
    [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Property workflow"
          title={
            p?.address || `Property #${hasValidPropertyId ? propertyId : "—"}`
          }
          subtitle="Inspection-grade compliance, workflow gates, and real remediation visibility for this property."
          actions={
            <>
              <button onClick={load} className="oh-btn oh-btn-secondary">
                <RefreshCcw className="h-4 w-4" />
                Refresh
              </button>
              <button
                onClick={runComplianceAutomation}
                disabled={automationBusy || !hasValidPropertyId}
                className="oh-btn oh-btn-primary"
              >
                <ShieldCheck className="h-4 w-4" />
                {automationBusy ? "Running…" : "Run compliance automation"}
              </button>
            </>
          }
        />

        {error ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{error}</div>
          </Surface>
        ) : null}

        {loading ? (
          <div className="grid gap-4">
            <div className="oh-skeleton h-[180px] rounded-3xl" />
            <div className="oh-skeleton h-[220px] rounded-3xl" />
            <div className="oh-skeleton h-[220px] rounded-3xl" />
          </div>
        ) : !property && !inspectionReadiness ? (
          <EmptyState
            title={
              hasValidPropertyId
                ? "Property not found"
                : "Invalid property route"
            }
            description={
              hasValidPropertyId
                ? "This property could not be loaded."
                : "The URL does not contain a valid property id."
            }
          />
        ) : (
          <>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Home className="h-3.5 w-3.5" />
                  Property
                </div>
                <div className="mt-3 text-sm font-semibold text-app-0">
                  {[p?.city, p?.state, p?.zip].filter(Boolean).join(", ") ||
                    "—"}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <ClipboardList className="h-3.5 w-3.5" />
                  Stage
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <span className="oh-pill oh-pill-accent">
                    {titleCase(workflowSummary?.current_stage || "—")}
                  </span>
                  {workflowSummary?.gate_status ? (
                    <span className={pillTone(workflowSummary.gate_status)}>
                      gate {titleCase(workflowSummary.gate_status)}
                    </span>
                  ) : null}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <ShieldCheck className="h-3.5 w-3.5" />
                  Readiness
                </div>
                <div className="mt-3 flex flex-wrap gap-2">
                  <span
                    className={pillTone(
                      readinessMeta?.status || readinessState?.status,
                    )}
                  >
                    {titleCase(
                      readinessMeta?.status || readinessState?.status || "—",
                    )}
                  </span>
                  <span
                    className={pillTone(
                      readiness?.posture || readinessMeta?.posture,
                    )}
                  >
                    {titleCase(
                      readiness?.posture || readinessMeta?.posture || "—",
                    )}
                  </span>
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Failures / blockers
                </div>
                <div className="mt-3 text-base font-semibold text-app-0">
                  {readiness?.counts?.failing ?? 0} /{" "}
                  {readiness?.counts?.blocking ?? 0}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <FileCheck2 className="h-3.5 w-3.5" />
                  Completion
                </div>
                <div className="mt-3 text-base font-semibold text-app-0">
                  {completionMeta?.pct != null
                    ? `${Number(completionMeta.pct).toFixed(1)}%`
                    : readiness?.completion_pct != null
                      ? `${Number(readiness.completion_pct).toFixed(1)}%`
                      : "—"}
                </div>
              </div>
            </div>

            <div className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
              <div className="space-y-6">
                <InspectionReadiness
                  readiness={inspectionReadiness}
                  brief={complianceBrief}
                  status={{
                    passed: readinessState?.latest_inspection_passed,
                    overall_status: readiness?.overall_status,
                  }}
                  summary={{
                    score_pct: readiness?.score_pct,
                    failed: readiness?.counts?.failing,
                    blocked: readiness?.counts?.blocking,
                  }}
                  onRunAutomation={runComplianceAutomation}
                  busy={automationBusy}
                />

                <PropertyCompliancePanel
                  property={{
                    id: propertyId,
                    state: p?.state,
                    county: p?.county,
                    city: p?.city,
                    strategy: p?.strategy,
                  }}
                  compliance={complianceBrief}
                />
              </div>

              <div className="space-y-6">
                <NextActionsPanel actions={nextActions} />

                <Surface
                  title="Workflow visibility"
                  subtitle="Why the property is advancing or blocked right now."
                >
                  {!workflowSummary ? (
                    <EmptyState compact title="No workflow summary yet" />
                  ) : (
                    <div className="space-y-4">
                      <div className="flex flex-wrap gap-2">
                        <span className="oh-pill oh-pill-accent">
                          stage {titleCase(workflowSummary.current_stage)}
                        </span>
                        {workflowSummary.gate_status ? (
                          <span
                            className={pillTone(workflowSummary.gate_status)}
                          >
                            {titleCase(workflowSummary.gate_status)}
                          </span>
                        ) : null}
                        {workflowSummary?.compliance?.posture ? (
                          <span
                            className={pillTone(
                              workflowSummary.compliance.posture,
                            )}
                          >
                            {titleCase(workflowSummary.compliance.posture)}
                          </span>
                        ) : null}
                      </div>

                      <div className="grid gap-3 md:grid-cols-2">
                        <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                            Primary action
                          </div>
                          <div className="mt-2 text-sm font-semibold text-app-0">
                            {workflowSummary?.primary_action?.title || "—"}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                            Open failed items
                          </div>
                          <div className="mt-2 text-sm font-semibold text-app-0">
                            {workflowSummary?.compliance?.open_failed_items ??
                              readiness?.counts?.inspection_failed_items ??
                              "—"}
                          </div>
                        </div>
                      </div>

                      <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                        <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                          <Wrench className="h-4 w-4 text-app-4" />
                          Current blockers
                        </div>

                        {Array.isArray(blockers) && blockers.length > 0 ? (
                          <div className="mt-3 flex flex-wrap gap-2">
                            {blockers
                              .slice(0, 12)
                              .map((b: any, idx: number) => (
                                <span
                                  key={`${String(b)}-${idx}`}
                                  className="oh-pill oh-pill-bad"
                                >
                                  {typeof b === "string"
                                    ? titleCase(b)
                                    : titleCase(
                                        b?.label ||
                                          b?.rule_key ||
                                          b?.code ||
                                          b?.title ||
                                          "blocker",
                                      )}
                                </span>
                              ))}
                          </div>
                        ) : (
                          <div className="mt-3 text-sm text-app-4">
                            No active blockers surfaced.
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </Surface>
              </div>
            </div>
          </>
        )}
      </div>
    </PageShell>
  );
}
