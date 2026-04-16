import React from "react";
import { Link } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  CalendarClock,
  ClipboardCheck,
  Eye,
  RefreshCcw,
  ShieldAlert,
  ShieldCheck,
} from "lucide-react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import PropertyCompliancePanel from "../components/PropertyCompliancePanel";
import InspectionReadiness from "../components/InspectionReadiness";
import InspectionSchedulerModal from "../components/InspectionSchedulerModal";
import InspectionTimelineCard from "../components/InspectionTimelineCard";
import ComplianceReminderPanel from "../components/ComplianceReminderPanel";
import ComplianceDocumentUploader from "../components/ComplianceDocumentUploader";
import ComplianceDocumentStack from "../components/ComplianceDocumentStack";
import PhotoUploader from "../components/PhotoUploader";
import PhotoGallery from "../components/PhotoGallery";
import RehabFromPhotosCTA from "../components/RehabFromPhotosCTA";
import CompliancePhotoFindingsPanel from "../components/CompliancePhotoFindingsPanel";
import PropertyJurisdictionRulesPanel from "../components/PropertyJurisdictionRulesPanel";
import { api } from "../lib/api";

type ComplianceRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  county?: string;
  current_stage?: string;
  current_stage_label?: string;
  current_pane?: string;
  current_pane_label?: string;
  urgency?: string;
  blockers?: string[];
  next_actions?: string[];
  compliance?: {
    completion_pct?: number;
    failed_count?: number;
    blocked_count?: number;
    open_failed_items?: number;
    latest_inspection_passed?: boolean;
    reinspect_required?: boolean;
    readiness_score?: number;
    readiness_status?: string;
    result_status?: string;
  };
  jurisdiction?: {
    completeness_status?: string;
    completeness_score?: number | null;
    is_stale?: boolean;
    gate_ok?: boolean;
    stale_reason?: string | null;
    coverage_confidence?: string | null;
    confidence_label?: string | null;
    production_readiness?: string | null;
    missing_categories?: string[] | null;
    covered_categories?: string[] | null;
    required_categories?: string[] | null;
    resolved_rule_version?: string | null;
    last_refreshed?: string | null;
    last_refreshed_at?: string | null;
    health_state?: string | null;
    reliability_state?: string | null;
    safe_to_rely_on?: boolean | null;
    lockout_causing_categories?: string[] | null;
    informational_gap_categories?: string[] | null;
    validation_pending_categories?: string[] | null;
    authority_gap_categories?: string[] | null;
    last_validation_at?: string | null;
    next_due_step?: string | null;
  };
};

type BlockerRow = {
  blocker?: string;
  count?: number;
  example_property_id?: number;
  example_address?: string;
  example_city?: string;
  urgency?: string;
};

type ActionRow = {
  property_id?: number;
  address?: string;
  city?: string;
  stage?: string;
  pane?: string;
  urgency?: string;
  blocker?: string;
  action?: string;
};

type StaleRow = {
  property_id?: number;
  address?: string;
  city?: string;
  pane?: string;
  stage?: string;
  urgency?: string;
  reasons?: string[];
};

type QueueCounts = {
  total?: number;
  by_stage?: Record<string, number>;
  by_status?: Record<string, number>;
  by_urgency?: Record<string, number>;
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
  coverage_matrix?: any | null;
  operational_status?: any | null;
  operational_health?: any | null;
  safe_to_rely_on?: boolean | null;
  unsafe_reasons?: string[] | null;
};

type PanePayload = {
  allowed_panes?: string[];
  filters?: Record<string, any>;
  kpis?: Record<string, any>;
  blockers?: BlockerRow[];
  recent_actions?: ActionRow[];
  next_actions?: ActionRow[];
  stale_items?: StaleRow[];
  queue_counts?: QueueCounts;
  rows?: ComplianceRow[];
  count?: number;
};

type PropertyLite = {
  id: number;
  address?: string;
  city?: string;
  state?: string;
  county?: string;
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
  if (
    v === "true" ||
    v === "pass" ||
    v === "ready" ||
    v === "confirmed" ||
    v === "ok"
  ) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "false",
      "fail",
      "blocked",
      "critical",
      "needs_work",
      "critical_failures",
      "needs_remediation",
      "reinspection_required",
      "not_ready",
      "failed",
      "canceled",
      "cancelled",
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
  if (["medium", "partial", "unknown", "warning"].includes(v)) {
    return "oh-pill oh-pill-warn";
  }
  if (["low", "weak"].includes(v)) return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function toPropertyLite(row?: ComplianceRow | null): PropertyLite | null {
  if (!row?.property_id) return null;
  return {
    id: row.property_id,
    address: row.address,
    city: row.city,
    state: row.state,
    county: row.county,
  };
}

function formatDate(v: any) {
  if (!v) return "—";
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

function BoundaryNotice({
  title,
  children,
  tone = "warn",
}: {
  title: string;
  children: React.ReactNode;
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
      <div className="mt-2 text-sm leading-6">{children}</div>
    </div>
  );
}

export default function CompliancePane() {
  const [data, setData] = React.useState<PanePayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const [selectedId, setSelectedId] = React.useState<number | null>(null);
  const [selectedBrief, setSelectedBrief] = React.useState<any | null>(null);
  const [selectedReadiness, setSelectedReadiness] = React.useState<any | null>(
    null,
  );
  const [selectedResolvedRules, setSelectedResolvedRules] =
    React.useState<PolicyResolvedRulesPayload | null>(null);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [detailError, setDetailError] = React.useState<string | null>(null);

  const [scheduleSummary, setScheduleSummary] = React.useState<any | null>(
    null,
  );
  const [timelineRows, setTimelineRows] = React.useState<any[]>([]);
  const [reminderRows, setReminderRows] = React.useState<any[]>([]);
  const [documentStack, setDocumentStack] = React.useState<any | null>(null);
  const [photos, setPhotos] = React.useState<any[]>([]);
  const [scheduleLoading, setScheduleLoading] = React.useState(false);
  const [schedulerOpen, setSchedulerOpen] = React.useState(false);
  const [photoAnalysis, setPhotoAnalysis] = React.useState<any | null>(null);
  const [photoBusy, setPhotoBusy] = React.useState(false);
  const [selectedFindingCodes, setSelectedFindingCodes] = React.useState<
    string[]
  >([]);
  const [markPhotoTasksBlocking, setMarkPhotoTasksBlocking] =
    React.useState(false);
  const [manualLifecycleResult, setManualLifecycleResult] = React.useState<
    any | null
  >(null);
  const [manualLifecycleBusy, setManualLifecycleBusy] = React.useState(false);

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

  const refreshPropertyArtifacts = React.useCallback(
    async (propertyId: number) => {
      try {
        const [docs, photoRows] = await Promise.all([
          api.get(`/compliance/properties/${propertyId}/document-stack`),
          api.get(`/photos/${propertyId}`),
        ]);
        setDocumentStack(docs?.documents || docs || null);
        setPhotos(Array.isArray(photoRows) ? photoRows : []);
      } catch {
        setDocumentStack(null);
        setPhotos([]);
      }
    },
    [],
  );

  const refreshScheduling = React.useCallback(async (propertyId: number) => {
    try {
      setScheduleLoading(true);
      const [summary, timeline, reminders] = await Promise.all([
        api.get(`/inspections/property/${propertyId}/schedule-summary`),
        api.get(`/inspections/property/${propertyId}/timeline`),
        api.get("/automation/inspection-reminders/preview"),
      ]);
      setScheduleSummary(summary || null);
      setTimelineRows(
        Array.isArray(timeline?.rows)
          ? timeline.rows
          : Array.isArray(timeline)
            ? timeline
            : [],
      );
      setReminderRows(Array.isArray(reminders?.rows) ? reminders.rows : []);
    } catch {
      setScheduleSummary(null);
      setTimelineRows([]);
      setReminderRows([]);
    } finally {
      setScheduleLoading(false);
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const rows = Array.isArray(data?.rows) ? data.rows : [];
  const blockers = Array.isArray(data?.blockers) ? data.blockers : [];
  const nextActions = Array.isArray(data?.next_actions)
    ? data.next_actions
    : [];
  const staleItems = Array.isArray(data?.stale_items) ? data.stale_items : [];

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
  const selectedProperty = React.useMemo(
    () => toPropertyLite(selectedRow),
    [selectedRow],
  );

  React.useEffect(() => {
    if (!selectedProperty?.id) {
      setSelectedBrief(null);
      setSelectedReadiness(null);
      setSelectedResolvedRules(null);
      setDetailError(null);
      setScheduleSummary(null);
      setTimelineRows([]);
      setReminderRows([]);
      setDocumentStack(null);
      setPhotos([]);
      setPhotoAnalysis(null);
      setSelectedFindingCodes([]);
      setMarkPhotoTasksBlocking(false);
      return;
    }

    let cancelled = false;
    setDetailLoading(true);
    setDetailError(null);

    Promise.allSettled([
      api.get(`/compliance/properties/${selectedProperty.id}/brief`),
      api.get(
        `/compliance/property/${selectedProperty.id}/inspection-readiness`,
      ),
      api.get(`/policy/property/${selectedProperty.id}/resolved-rules`),
    ])
      .then((results) => {
        if (cancelled) return;
        const briefRes = results[0];
        const readinessRes = results[1];
        const rulesRes = results[2];

        if (briefRes.status === "fulfilled") {
          setSelectedBrief((briefRes.value as any) || null);
        } else {
          setSelectedBrief(null);
        }

        if (readinessRes.status === "fulfilled") {
          setSelectedReadiness((readinessRes.value as any) || null);
        } else {
          setSelectedReadiness(null);
        }

        if (rulesRes.status === "fulfilled") {
          setSelectedResolvedRules(
            (rulesRes.value as PolicyResolvedRulesPayload) || null,
          );
        } else {
          setSelectedResolvedRules(null);
        }

        if (
          briefRes.status === "rejected" &&
          readinessRes.status === "rejected" &&
          rulesRes.status === "rejected"
        ) {
          throw briefRes.reason || readinessRes.reason || rulesRes.reason;
        }
      })
      .catch((e: any) => {
        if (!cancelled) setDetailError(String(e?.message || e));
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });

    void refreshScheduling(selectedProperty.id);
    void refreshPropertyArtifacts(selectedProperty.id);

    return () => {
      cancelled = true;
    };
  }, [refreshScheduling, refreshPropertyArtifacts, selectedProperty?.id]);

  const selectedInspectionId =
    scheduleSummary?.appointment?.inspection_id ||
    scheduleSummary?.latest_appointment?.inspection_id ||
    selectedReadiness?.latest_inspection?.id ||
    null;

  const selectedAppointment =
    scheduleSummary?.appointment || scheduleSummary?.latest_appointment || null;
  const selectedJurisdictionProfile =
    selectedResolvedRules?.profile || selectedBrief?.brief?.coverage || null;
  const selectedOperationalStatus =
    selectedResolvedRules?.operational_status ||
    selectedResolvedRules?.operational_health ||
    selectedJurisdictionProfile?.operational_status ||
    null;
  const selectedSafeToRely = Boolean(
    selectedResolvedRules?.safe_to_rely_on ??
    selectedOperationalStatus?.safe_to_rely_on,
  );
  const selectedUnsafeReasons = Array.isArray(
    selectedResolvedRules?.unsafe_reasons,
  )
    ? selectedResolvedRules?.unsafe_reasons
    : Array.isArray(selectedOperationalStatus?.reasons)
      ? selectedOperationalStatus?.reasons
      : [];

  async function runManualPolicyAction(action: string) {
    if (!selectedProperty) return;
    setManualLifecycleBusy(true);
    try {
      const payload = {
        state: selectedProperty.state || "MI",
        county: selectedProperty.county || null,
        city: selectedProperty.city || null,
        focus: "se_mi_extended",
        org_scope: false,
        auto_activate: true,
      };
      const out = await api.post(`/policy/manual/${action}`, payload);
      setManualLifecycleResult(out || null);
      await refresh();
    } catch (e: any) {
      setDetailError(String(e?.message || e));
    } finally {
      setManualLifecycleBusy(false);
    }
  }

  function syncSelectedFindings(analysis: any) {
    setPhotoAnalysis(analysis);
    const codes = (Array.isArray(analysis?.findings) ? analysis.findings : [])
      .map((item: any) =>
        String(item?.code || item?.rule_mapping?.code || "").toUpperCase(),
      )
      .filter(Boolean);
    setSelectedFindingCodes(codes);
  }

  async function previewCompliancePhotoFindings() {
    if (!selectedProperty?.id) return;
    try {
      setPhotoBusy(true);
      const form = new FormData();
      if (selectedInspectionId != null) {
        form.append("inspection_id", String(selectedInspectionId));
      }
      const result = await api.post(
        `/photos/${selectedProperty.id}/compliance-preview`,
        form,
      );
      syncSelectedFindings(result);
    } finally {
      setPhotoBusy(false);
    }
  }

  async function createComplianceTasksFromPhotos() {
    if (!selectedProperty?.id || !selectedFindingCodes.length) return;
    try {
      setPhotoBusy(true);
      const form = new FormData();
      form.append("confirmed_codes", selectedFindingCodes.join(","));
      form.append("mark_blocking", String(markPhotoTasksBlocking));
      if (selectedInspectionId != null) {
        form.append("inspection_id", String(selectedInspectionId));
      }
      const result = await api.post(
        `/photos/${selectedProperty.id}/compliance-tasks`,
        form,
      );
      if (result?.findings) {
        syncSelectedFindings({
          ...(photoAnalysis || {}),
          ...result,
          findings: result.findings,
          issues: result.findings,
        });
      }
      api
        .get(`/compliance/property/${selectedProperty.id}/inspection-readiness`)
        .then(setSelectedReadiness)
        .catch(() => {});
    } finally {
      setPhotoBusy(false);
    }
  }

  async function deletePhoto(photoId: number) {
    await api.delete(`/photos/${photoId}`);
    if (selectedProperty?.id) {
      await refreshPropertyArtifacts(selectedProperty.id);
    }
  }

  const weakCoverageCount = rows.filter((row) =>
    ["low", "partial", "medium", "unknown", "warning"].includes(
      String(
        row.jurisdiction?.coverage_confidence ||
          row.jurisdiction?.confidence_label ||
          "",
      ).toLowerCase(),
    ),
  ).length;

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Lifecycle pane"
          title="Compliance / S8 pane"
          subtitle="Property-scoped compliance, workflow gating, inspection history, scheduling, reminders, local rule coverage, stale alerts, and remediation now flow through one pane."
          actions={
            <div className="flex flex-wrap gap-3">
              {selectedProperty?.id ? (
                <button
                  onClick={() => setSchedulerOpen(true)}
                  className="oh-btn"
                >
                  <CalendarClock className="h-4 w-4" />
                  Schedule inspection
                </button>
              ) : null}
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

        <BoundaryNotice title="Operational trust boundary" tone="warn">
          OneHaven shows whether this workflow is operationally safe to rely on based on the rules, proof, freshness, and validation currently in the system. It does <strong>not</strong> provide legal advice or guarantee legal compliance. Verify critical requirements like registration, inspections, occupancy approval, lead documents, and subsidy paperwork with the authoritative source before acting.
        </BoundaryNotice>

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
            title="With blockers"
            subtitle="Properties needing intervention"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.with_blockers || 0)}
            </div>
          </Surface>

          <Surface
            title="Stale items"
            subtitle="Records needing freshness or follow-up"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.stale_items || staleItems.length || 0)}
            </div>
          </Surface>

          <Surface
            title="Critical items"
            subtitle="Highest urgency compliance workload"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.critical_items || 0)}
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
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface
            title="Queue status"
            subtitle="Shared queue breakdown used across pane dashboards"
          >
            <div className="grid gap-3">
              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Total in queue
                </div>
                <div className="mt-1 text-2xl font-semibold text-app-0">
                  {Number(data?.queue_counts?.total || rows.length || 0)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  By urgency
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(data?.queue_counts?.by_urgency || {})
                    .length ? (
                    Object.entries(data?.queue_counts?.by_urgency || {}).map(
                      ([key, value]) => (
                        <span key={key} className={urgencyTone(key)}>
                          {labelize(key)} · {Number(value || 0)}
                        </span>
                      ),
                    )
                  ) : (
                    <span className="text-sm text-app-4">No urgency data</span>
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  By status
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(data?.queue_counts?.by_status || {})
                    .length ? (
                    Object.entries(data?.queue_counts?.by_status || {}).map(
                      ([key, value]) => (
                        <span key={key} className="oh-pill">
                          {labelize(key)} · {Number(value || 0)}
                        </span>
                      ),
                    )
                  ) : (
                    <span className="text-sm text-app-4">No status data</span>
                  )}
                </div>
              </div>
            </div>
          </Surface>

          <Surface
            title="Top blockers"
            subtitle="Most common reasons properties are not flowing forward"
          >
            {!blockers.length ? (
              <div className="text-sm text-app-4">No active blockers.</div>
            ) : (
              <div className="grid gap-3">
                {blockers.slice(0, 5).map((item, idx) => (
                  <div
                    key={`${item.blocker || "blocker"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-app-0">
                          {labelize(item.blocker)}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          Example: {item.example_address || "Unknown property"}
                          {item.example_city ? ` · ${item.example_city}` : ""}
                        </div>
                      </div>
                      <div className="text-sm font-semibold text-app-1">
                        {Number(item.count || 0)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>

          <Surface
            title="Next actions"
            subtitle="Highest-priority actions the pane says to do next"
          >
            {!nextActions.length ? (
              <div className="text-sm text-app-4">
                No next actions right now.
              </div>
            ) : (
              <div className="grid gap-3">
                {nextActions.slice(0, 5).map((item, idx) => (
                  <div
                    key={`${item.property_id || "action"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                  >
                    <div className="text-sm font-medium text-app-0">
                      {item.action || "No action description"}
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      {item.address || `Property #${item.property_id || "—"}`}
                      {item.city ? ` · ${item.city}` : ""}
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {item.urgency ? (
                        <span className={urgencyTone(item.urgency)}>
                          {labelize(item.urgency)}
                        </span>
                      ) : null}
                      {item.blocker ? (
                        <span className="oh-pill oh-pill-warn">
                          {labelize(item.blocker)}
                        </span>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>

        <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <Surface
            title="Compliance queue"
            subtitle="Select a property to inspect readiness, scheduling state, local rule coverage, projection blockers, document stack, and failure-driven actions."
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
                  const readinessStatus =
                    row.compliance?.result_status ||
                    row.compliance?.readiness_status ||
                    undefined;
                  const reinspectRequired = Boolean(
                    row.compliance?.reinspect_required,
                  );
                  const confidence =
                    row.jurisdiction?.coverage_confidence ||
                    row.jurisdiction?.confidence_label ||
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
                            {readinessStatus ? (
                              <span className={statusTone(readinessStatus)}>
                                {labelize(readinessStatus)}
                              </span>
                            ) : null}
                            {reinspectRequired ? (
                              <span className="oh-pill oh-pill-bad">
                                Reinspection required
                              </span>
                            ) : null}
                          </div>

                          <div className="mt-1 text-sm text-app-4">
                            {[row.city, row.state].filter(Boolean).join(", ")}
                            {row.county ? ` · ${row.county}` : ""}
                          </div>

                          <div className="mt-3 flex flex-wrap gap-2">
                            <span className="oh-pill oh-pill-warn">
                              {row.current_stage_label ||
                                row.current_stage ||
                                "compliance"}
                            </span>

                            {row.compliance?.latest_inspection_passed ===
                            true ? (
                              <span className="oh-pill oh-pill-good">
                                Latest inspection passed
                              </span>
                            ) : row.compliance?.latest_inspection_passed ===
                              false ? (
                              <span className="oh-pill oh-pill-bad">
                                Latest inspection failed
                              </span>
                            ) : null}

                            {row.jurisdiction?.is_stale ? (
                              <span className="oh-pill oh-pill-bad">
                                Jurisdiction stale
                              </span>
                            ) : null}

                            {row.jurisdiction?.health_state ? (
                              <span
                                className={statusTone(
                                  row.jurisdiction.health_state,
                                )}
                              >
                                {labelize(row.jurisdiction.health_state)}
                              </span>
                            ) : null}
                            {row.jurisdiction?.validation_pending_categories
                              ?.length ? (
                              <span className="oh-pill oh-pill-warn">
                                Validation pending
                              </span>
                            ) : null}
                            {row.jurisdiction?.authority_gap_categories
                              ?.length ? (
                              <span className="oh-pill oh-pill-bad">
                                Authority gap
                              </span>
                            ) : null}

                            {row.jurisdiction?.completeness_status &&
                            row.jurisdiction?.completeness_status !==
                              "complete" ? (
                              <span className="oh-pill oh-pill-accent">
                                {labelize(row.jurisdiction.completeness_status)}
                              </span>
                            ) : null}

                            {confidence ? (
                              <span className={confidenceTone(confidence)}>
                                Coverage: {labelize(confidence)}
                              </span>
                            ) : null}

                            {row.blockers?.[0] ? (
                              <span className="oh-pill oh-pill-warn">
                                {labelize(row.blockers[0])}
                              </span>
                            ) : null}
                          </div>

                          {!!row.jurisdiction?.missing_categories?.length ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {row.jurisdiction?.missing_categories
                                ?.slice(0, 3)
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
            {!selectedProperty ? (
              <Surface
                title="Selected property"
                subtitle="Choose a property from the compliance queue."
              >
                <EmptyState
                  compact
                  icon={ClipboardCheck}
                  title="No property selected"
                  description="Select a property to inspect readiness, checklist state, inspection history, scheduling, compliance documents, local rule coverage, and workflow gating."
                />
              </Surface>
            ) : detailError ? (
              <><Surface
                  tone="danger"
                  title="Selected property"
                  subtitle="Could not load details"
                >
                  <div className="text-sm text-red-300">{detailError}</div>
                </Surface><BoundaryNotice
                  title={selectedSafeToRely ? "Operationally usable, not legal advice" : "Human review still needed"}
                  tone={selectedSafeToRely ? "good" : "bad"}
                >
                    {selectedSafeToRely
                      ? "The system currently sees enough governed rule and proof coverage to support operations, but you should still verify critical requirements before representing the property as legally compliant."
                      : "This property should not be represented as cleared. Resolve the blocking proof, trust, freshness, or review issues first, and verify critical requirements with the authoritative jurisdiction source or qualified legal/compliance review."}
                    <div className="mt-2">Any copied, exported, or shared compliance summary should keep this same boundary language.</div>
                  </BoundaryNotice></>

            ) : detailLoading && !selectedReadiness && !selectedBrief ? (
              <Surface
                title="Selected property"
                subtitle="Loading property-scoped compliance state"
              >
                <div className="grid gap-3">
                  <div className="oh-skeleton h-[120px] rounded-2xl" />
                  <div className="oh-skeleton h-[180px] rounded-2xl" />
                  <div className="oh-skeleton h-[240px] rounded-2xl" />
                </div>
              </Surface>
            ) : (
              <>
                <Surface
                  title="Manual jurisdiction lifecycle"
                  subtitle="Run discovery refresh, crawl refresh, validation retry, recompute, and health recompute manually while automation stays disabled."
                >
                  <div className="flex flex-wrap gap-2">
                    <button
                      onClick={() =>
                        void runManualPolicyAction("discovery-refresh")
                      }
                      className="oh-btn oh-btn-secondary"
                      disabled={manualLifecycleBusy || !selectedProperty}
                    >
                      Discovery refresh
                    </button>
                    <button
                      onClick={() =>
                        void runManualPolicyAction("crawl-refresh")
                      }
                      className="oh-btn oh-btn-secondary"
                      disabled={manualLifecycleBusy || !selectedProperty}
                    >
                      Crawl refresh
                    </button>
                    <button
                      onClick={() =>
                        void runManualPolicyAction("validation-retry")
                      }
                      className="oh-btn oh-btn-secondary"
                      disabled={manualLifecycleBusy || !selectedProperty}
                    >
                      Validation retry
                    </button>
                    <button
                      onClick={() => void runManualPolicyAction("recompute")}
                      className="oh-btn oh-btn-secondary"
                      disabled={manualLifecycleBusy || !selectedProperty}
                    >
                      Recompute
                    </button>
                    <button
                      onClick={() =>
                        void runManualPolicyAction("health-recompute")
                      }
                      className="oh-btn oh-btn-secondary"
                      disabled={manualLifecycleBusy || !selectedProperty}
                    >
                      Health recompute
                    </button>
                  </div>
                  {manualLifecycleResult ? (
                    <pre className="mt-3 overflow-auto rounded-2xl border border-app bg-black/20 p-4 text-xs text-app-1">
                      {JSON.stringify(manualLifecycleResult, null, 2)}
                    </pre>
                  ) : null}
                </Surface>

                <Surface
                  title="Selected property"
                  subtitle="Latest inspection, projection blockers, readiness summary, jurisdiction coverage, workflow gate, scheduling state, and evidence uploads for the active property."
                  actions={
                    <div className="flex flex-wrap gap-3">
                      <button
                        onClick={() => setSchedulerOpen(true)}
                        className="oh-btn"
                      >
                        <CalendarClock className="h-4 w-4" />
                        {selectedAppointment
                          ? "Edit schedule"
                          : "Schedule inspection"}
                      </button>
                      <Link
                        to={`/properties/${selectedProperty.id}`}
                        className="oh-btn oh-btn-secondary"
                      >
                        Open property
                      </Link>
                    </div>
                  }
                >
                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Property
                      </div>
                      <div className="mt-2 text-sm font-semibold text-app-0">
                        {selectedProperty.address ||
                          `Property #${selectedProperty.id}`}
                      </div>
                      <div className="mt-1 text-sm text-app-4">
                        {[selectedProperty.city, selectedProperty.state]
                          .filter(Boolean)
                          .join(", ")}
                        {selectedProperty.county
                          ? ` · ${selectedProperty.county}`
                          : ""}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Inspection posture
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {selectedReadiness?.latest_inspection?.passed ===
                        true ? (
                          <span className="oh-pill oh-pill-good">
                            <ShieldCheck className="mr-1 h-3.5 w-3.5" />
                            Passed
                          </span>
                        ) : selectedReadiness?.latest_inspection?.passed ===
                          false ? (
                          <span className="oh-pill oh-pill-bad">
                            <ShieldAlert className="mr-1 h-3.5 w-3.5" />
                            Failed
                          </span>
                        ) : (
                          <span className="oh-pill">Pending</span>
                        )}
                        {selectedReadiness?.latest_inspection
                          ?.reinspect_required ? (
                          <span className="oh-pill oh-pill-bad">
                            Reinspection required
                          </span>
                        ) : null}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Coverage confidence
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        <span
                          className={confidenceTone(
                            selectedBrief?.brief?.coverage
                              ?.coverage_confidence ||
                              selectedBrief?.brief?.coverage
                                ?.confidence_label ||
                              selectedRow?.jurisdiction?.coverage_confidence ||
                              selectedRow?.jurisdiction?.confidence_label ||
                              "unknown",
                          )}
                        >
                          {labelize(
                            selectedBrief?.brief?.coverage
                              ?.coverage_confidence ||
                              selectedBrief?.brief?.coverage
                                ?.confidence_label ||
                              selectedRow?.jurisdiction?.coverage_confidence ||
                              selectedRow?.jurisdiction?.confidence_label ||
                              "unknown",
                          )}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Operational state
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        <span
                          className={statusTone(
                            selectedOperationalStatus?.health_state ||
                              selectedOperationalStatus?.refresh_state ||
                              "unknown",
                          )}
                        >
                          {labelize(
                            selectedOperationalStatus?.health_state ||
                              selectedOperationalStatus?.refresh_state ||
                              "unknown",
                          )}
                        </span>
                        <span
                          className={
                            selectedSafeToRely
                              ? "oh-pill oh-pill-good"
                              : "oh-pill oh-pill-bad"
                          }
                        >
                          {selectedSafeToRely
                            ? "Safe to rely on"
                            : "Do not rely on"}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Rule version
                      </div>
                      <div className="mt-2 text-sm font-semibold text-app-0">
                        {selectedBrief?.brief?.coverage
                          ?.resolved_rule_version ||
                          selectedBrief?.brief?.projection?.rules_version ||
                          selectedRow?.jurisdiction?.resolved_rule_version ||
                          "—"}
                      </div>
                      <div className="mt-1 text-xs text-app-4">
                        Refreshed{" "}
                        {formatDate(
                          selectedBrief?.brief?.coverage?.last_refreshed ||
                            selectedRow?.jurisdiction?.last_refreshed ||
                            selectedRow?.jurisdiction?.last_refreshed_at,
                        )}
                      </div>
                    </div>
                  </div>

                  {selectedBrief?.workflow?.compliance_gate?.blocked_reason ||
                  !selectedSafeToRely ||
                  selectedOperationalStatus?.lockout?.lockout_active ? (
                    <div
                      className={`mt-4 oh-state-banner ${selectedOperationalStatus?.lockout?.lockout_active ? "oh-state-banner-bad" : !selectedSafeToRely ? "oh-state-banner-warn" : "oh-state-banner-good"}`}
                    >
                      <div className="flex items-start gap-2 text-sm text-red-100">
                        <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0" />
                        <div className="space-y-1">
                          {selectedBrief?.workflow?.compliance_gate
                            ?.blocked_reason ? (
                            <div>
                              {
                                selectedBrief.workflow.compliance_gate
                                  .blocked_reason
                              }
                            </div>
                          ) : null}
                          {selectedOperationalStatus?.lockout
                            ?.lockout_reason ? (
                            <div>
                              {selectedOperationalStatus.lockout.lockout_reason}
                            </div>
                          ) : null}
                          {!selectedSafeToRely &&
                          !selectedOperationalStatus?.lockout
                            ?.lockout_reason ? (
                            <div>
                              {selectedUnsafeReasons[0] ||
                                selectedOperationalStatus?.refresh_status_reason ||
                                "This jurisdiction state is not yet safe to rely on for compliance decisions."}
                            </div>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {selectedBrief?.brief?.coverage?.is_stale ||
                  selectedRow?.jurisdiction?.is_stale ? (
                    <div className="mt-4 rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3">
                      <div className="flex items-start gap-2 text-sm text-amber-100">
                        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                        <div>
                          {selectedBrief?.brief?.coverage?.stale_reason ||
                            selectedRow?.jurisdiction?.stale_reason ||
                            "Local rule data is stale and needs review."}
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {selectedOperationalStatus?.next_actions?.next_step ? (
                    <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                      <div className="flex items-start gap-2 text-sm text-app-2">
                        <RefreshCcw className="mt-0.5 h-4 w-4 shrink-0" />
                        <div>
                          Next step:{" "}
                          {labelize(
                            selectedOperationalStatus.next_actions.next_step,
                          )}
                          {selectedOperationalStatus.next_actions
                            .next_search_retry_due_at ? (
                            <span className="ml-2 text-app-4">
                              · due{" "}
                              {formatDate(
                                selectedOperationalStatus.next_actions
                                  .next_search_retry_due_at,
                              )}
                            </span>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {!!selectedBrief?.brief?.coverage ? (
                    <div className="mt-4 grid gap-3 md:grid-cols-3">
                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Completeness
                        </div>
                        <div className="mt-2 text-sm font-semibold text-app-0">
                          {labelize(
                            selectedBrief.brief.coverage.completeness_status ||
                              "unknown",
                          )}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          Score{" "}
                          {Math.round(
                            Number(
                              selectedBrief.brief.coverage.completeness_score ||
                                0,
                            ) * 100,
                          )}
                          %
                        </div>
                      </div>

                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Covered categories
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {(
                            selectedBrief.brief.coverage.covered_categories ||
                            []
                          )
                            .slice(0, 5)
                            .map((item: string) => (
                              <span key={item} className="oh-pill oh-pill-good">
                                {labelize(item)}
                              </span>
                            ))}
                          {!(
                            selectedBrief.brief.coverage.covered_categories ||
                            []
                          ).length ? (
                            <span className="text-sm text-app-4">
                              None listed
                            </span>
                          ) : null}
                        </div>
                      </div>

                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Missing local rule areas
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {(
                            selectedBrief.brief.coverage.missing_categories ||
                            []
                          )
                            .slice(0, 6)
                            .map((item: string) => (
                              <span key={item} className="oh-pill oh-pill-warn">
                                {labelize(item)}
                              </span>
                            ))}
                          {!(
                            selectedBrief.brief.coverage.missing_categories ||
                            []
                          ).length ? (
                            <span className="oh-pill oh-pill-good">
                              No known gaps
                            </span>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  ) : null}
                </Surface>

                <PropertyJurisdictionRulesPanel
                  profile={
                    (selectedJurisdictionProfile ||
                      selectedResolvedRules?.profile ||
                      null) as any
                  }
                />

                <CompliancePhotoFindingsPanel
                  analysis={photoAnalysis}
                  busy={photoBusy}
                  selectedCodes={selectedFindingCodes}
                  onSelectedCodesChange={setSelectedFindingCodes}
                  onPreview={previewCompliancePhotoFindings}
                  onCreateTasks={createComplianceTasksFromPhotos}
                  onMarkBlockingChange={setMarkPhotoTasksBlocking}
                  markTasksBlocking={markPhotoTasksBlocking}
                />

                <PropertyCompliancePanel
                  property={selectedProperty}
                  compliance={selectedBrief}
                  photoAnalysis={photoAnalysis}
                />

                <InspectionReadiness
                  property={selectedProperty}
                  readiness={selectedReadiness}
                />

                <InspectionTimelineCard
                  rows={timelineRows}
                  loading={scheduleLoading}
                  appointment={selectedAppointment}
                  property={selectedProperty}
                />

                <ComplianceReminderPanel
                  property={selectedProperty}
                  rows={reminderRows}
                />

                <Surface
                  title="Compliance documents"
                  subtitle="Documents and document-derived evidence linked to the active property."
                >
                  <div className="grid gap-4">
                    <ComplianceDocumentUploader
                      propertyId={selectedProperty.id}
                      onUploaded={() =>
                        void refreshPropertyArtifacts(selectedProperty.id)
                      }
                    />
                    <ComplianceDocumentStack
                      propertyId={selectedProperty.id}
                      documents={
                        Array.isArray(documentStack?.rows)
                          ? documentStack.rows
                          : Array.isArray(documentStack?.documents)
                            ? documentStack.documents
                            : []
                      }
                      onChanged={() =>
                        void refreshPropertyArtifacts(selectedProperty.id)
                      }
                    />
                  </div>
                </Surface>

                <Surface
                  title="Property photos"
                  subtitle="Operational property photos, deletion, and rehab/compliance follow-up."
                >
                  <div className="grid gap-4">
                    <PhotoUploader
                      propertyId={selectedProperty.id}
                      inspectionId={selectedInspectionId}
                      autoAnalyze
                      onUploaded={() =>
                        void refreshPropertyArtifacts(selectedProperty.id)
                      }
                    />
                    <PhotoGallery photos={photos} onDelete={deletePhoto} />
                  </div>
                </Surface>

                <RehabFromPhotosCTA
                  propertyId={selectedProperty.id}
                  selectedFindingCodes={selectedFindingCodes}
                />
              </>
            )}
          </div>
        </div>

        <Surface
          title="Stale / follow-up items"
          subtitle="Items that likely need freshness, remediation, or policy follow-up"
        >
          {!staleItems.length ? (
            <div className="text-sm text-app-4">No stale items.</div>
          ) : (
            <div className="grid gap-3">
              {staleItems.slice(0, 6).map((item, idx) => (
                <div
                  key={`${item.property_id || "stale"}-${idx}`}
                  className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-medium text-app-0">
                        {item.address || `Property #${item.property_id || "—"}`}
                      </div>
                      <div className="mt-1 text-xs text-app-4">
                        {item.city || "Unknown city"}
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(item.reasons || []).map((reason) => (
                          <span key={reason} className="oh-pill oh-pill-warn">
                            {labelize(reason)}
                          </span>
                        ))}
                      </div>
                    </div>

                    <Link
                      to={`/properties/${item.property_id}`}
                      className="inline-flex items-center gap-1 text-sm text-app-1"
                    >
                      Open
                      <ArrowRight className="h-4 w-4" />
                    </Link>
                  </div>
                </div>
              ))}
            </div>
          )}
        </Surface>

        {(
          selectedReadiness?.inspection_failure_actions?.recommended_actions ||
          []
        ).length > 0 ? (
          <Surface
            title="Failure-driven actions"
            subtitle="Actionable remediation items generated from failed, blocked, or inconclusive inspection findings."
          >
            <div className="grid gap-3">
              {(
                selectedReadiness?.inspection_failure_actions
                  ?.recommended_actions || []
              )
                .slice(0, 8)
                .map((item: any, idx: number) => (
                  <div
                    key={`${item?.code || item?.title || idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {item?.title || item?.code || "Untitled action"}
                      </div>
                      {item?.priority ? (
                        <span className={statusTone(item.priority)}>
                          {labelize(item.priority)}
                        </span>
                      ) : null}
                      {item?.result_status ? (
                        <span className={statusTone(item.result_status)}>
                          {labelize(item.result_status)}
                        </span>
                      ) : null}
                      {item?.requires_reinspection ? (
                        <span className="oh-pill oh-pill-bad">
                          Reinspection
                        </span>
                      ) : null}
                    </div>

                    {item?.notes ? (
                      <div className="mt-2 text-sm leading-6 text-app-3">
                        {item.notes}
                      </div>
                    ) : null}
                  </div>
                ))}
            </div>
          </Surface>
        ) : null}

        {selectedProperty?.id ? (
          <InspectionSchedulerModal
            open={schedulerOpen}
            property={selectedProperty}
            onClose={() => setSchedulerOpen(false)}
            onSaved={async () => {
              setSchedulerOpen(false);
              await Promise.all([
                refreshScheduling(selectedProperty.id),
                refresh(),
              ]);
            }}
          />
        ) : null}
      </div>
    </PageShell>
  );
}
