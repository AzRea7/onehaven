import React from "react";
import {
  AlertTriangle,
  Building2,
  CalendarClock,
  Camera,
  ClipboardList,
  ClipboardX,
  FileCheck2,
  Image as ImageIcon,
  ShieldAlert,
  TriangleAlert,
  Wrench,
} from "lucide-react";
import { api } from "../lib/api";
import Surface from "./Surface";
import EmptyState from "./EmptyState";
import ComplianceDocumentUploader from "./ComplianceDocumentUploader";
import ComplianceDocumentStack from "./ComplianceDocumentStack";
import CompliancePhotoFindingsPanel from "./CompliancePhotoFindingsPanel";

type PropertyLike = {
  id?: number;
  state?: string | null;
  county?: string | null;
  city?: string | null;
  strategy?: string | null;
  address?: string | null;
};

type Projection = {
  id?: number;
  projection_status?: string | null;
  rules_version?: string | null;
  blocking_count?: number | null;
  unknown_count?: number | null;
  stale_count?: number | null;
  conflicting_count?: number | null;
  evidence_gap_count?: number | null;
  confirmed_count?: number | null;
  inferred_count?: number | null;
  failing_count?: number | null;
  readiness_score?: number | null;
  projected_compliance_cost?: number | null;
  projected_days_to_rent?: number | null;
  confidence_score?: number | null;
  impacted_rules?: any[];
  unresolved_evidence_gaps?: any[];
  last_projected_at?: string | null;
};

type Workflow = {
  current_stage?: string | null;
  current_stage_label?: string | null;
  current_pane?: string | null;
  current_pane_label?: string | null;
  compliance_gate?: {
    ok?: boolean;
    severity?: string | null;
    status?: string | null;
    blocked_reason?: string | null;
    warning_reason?: string | null;
    warnings?: string[];
    blockers?: any[];
    readiness_score?: number | null;
    confidence_score?: number | null;
    projected_compliance_cost?: number | null;
    projected_days_to_rent?: number | null;
    blocking_count?: number | null;
    unknown_count?: number | null;
    stale_count?: number | null;
    conflicting_count?: number | null;
    impacted_rules?: any[];
    unresolved_evidence_gaps?: any[];
  };
  pre_close_risk?: {
    active?: boolean;
    status?: string | null;
    severity?: string | null;
    blocking?: boolean;
    warnings?: string[];
    summary?: string | null;
    projected_compliance_cost?: number | null;
    projected_days_to_rent?: number | null;
  };
  post_close_recheck?: {
    active?: boolean;
    status?: string | null;
    needed?: boolean;
    reason?: string | null;
    warnings?: string[];
  };
};

type BriefPayload = {
  ok?: boolean;
  property?: PropertyLike;
  brief?: {
    coverage?: {
      completeness_status?: string | null;
      completeness_score?: number | null;
      confidence_label?: string | null;
      production_readiness?: string | null;
      is_stale?: boolean | null;
      stale_reason?: string | null;
      required_categories?: string[] | null;
      covered_categories?: string[] | null;
      missing_categories?: string[] | null;
      coverage_confidence?: string | null;
      resolved_rule_version?: string | null;
      last_refreshed?: string | null;
      source_evidence?: any[] | null;
      evidence?: any[] | null;
      resolved_layers?: any[] | null;
      layers?: any[] | null;
    };
    required_actions?: any[];
    blocking_items?: any[];
    verified_rules?: any[];
    projection?: Projection | null;
    projection_counts?: Record<string, number> | null;
    source_evidence?: any[];
    resolved_layers?: any[];
  };
  workflow?: Workflow;
  documents?:
    | {
        documents?: any[];
        rows?: any[];
      }
    | any;
};

type InspectionRecord = {
  id?: number;
  inspection_date?: string | null;
  inspector?: string | null;
  jurisdiction?: string | null;
  template_key?: string | null;
  template_version?: string | null;
  passed?: boolean | null;
  result_status?: string | null;
  readiness_status?: string | null;
  readiness_score?: number | null;
  reinspect_required?: boolean | null;
  completion_pct?: number | null;
  counts?: Record<string, any>;
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
    [
      "verified",
      "yes",
      "ready",
      "high",
      "complete",
      "pass",
      "good",
      "confirmed",
      "scheduled",
      "clean",
      "parsed",
      "strong",
      "ok",
      "info",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-good";
  }
  if (
    [
      "partial",
      "medium",
      "unknown",
      "conditional",
      "attention",
      "in_progress",
      "pending",
      "warn",
      "warning",
      "draft",
      "queued",
      "skipped",
      "stale",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-warn";
  }
  if (
    [
      "low",
      "weak",
      "needs_review",
      "no",
      "missing",
      "blocked",
      "critical_failures",
      "needs_remediation",
      "not_ready",
      "fail",
      "critical",
      "reinspection_required",
      "failed",
      "canceled",
      "cancelled",
      "infected",
      "error",
      "conflicting",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

function statusTone(value?: string | boolean | null) {
  const s = String(value ?? "").toLowerCase();
  if (s === "true" || s === "ready" || s === "pass" || s === "ok")
    return "oh-pill oh-pill-good";
  if (
    [
      "false",
      "fail",
      "blocked",
      "critical",
      "critical_failures",
      "needs_remediation",
      "reinspection_required",
      "not_ready",
      "needs_work",
      "failed",
      "canceled",
      "cancelled",
      "conflicting",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-bad";
  }
  if (
    [
      "pending",
      "warn",
      "warning",
      "unknown",
      "attention",
      "inconclusive",
      "draft",
      "scheduled",
      "stale",
      "partial",
    ].includes(s)
  ) {
    return "oh-pill oh-pill-warn";
  }
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

function toArray<T = any>(value: any): T[] {
  return Array.isArray(value) ? value : [];
}

function normalizeInspectionHistory(readiness: any): InspectionRecord[] {
  const candidates = [
    readiness?.inspection_history,
    readiness?.history,
    readiness?.inspections,
    readiness?.latest_inspection_history,
    readiness?.readiness_summary?.history,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate) && candidate.length > 0) {
      return candidate as InspectionRecord[];
    }
  }

  if (readiness?.latest_inspection) {
    return [readiness.latest_inspection as InspectionRecord];
  }

  return [];
}

function normalizeChecklistItems(readiness: any) {
  const preferred = [
    readiness?.results,
    readiness?.checklist_items,
    readiness?.readiness_summary?.items,
    readiness?.template?.items,
  ];

  for (const candidate of preferred) {
    if (Array.isArray(candidate) && candidate.length > 0) {
      return candidate;
    }
  }

  return [];
}

function itemStatus(item: any) {
  return String(
    item?.result_status ||
      item?.status ||
      item?.latest_result_status ||
      item?.readiness_status ||
      "unknown",
  ).toLowerCase();
}

function itemSeverity(item: any) {
  const raw = String(
    item?.severity || item?.severity_label || "",
  ).toLowerCase();
  if (["critical", "fail", "warn", "info"].includes(raw)) return raw;
  const n = Number(item?.severity);
  if (!Number.isNaN(n)) {
    if (n >= 4) return "critical";
    if (n === 3) return "fail";
    if (n === 2) return "warn";
    return "info";
  }
  return raw || "unknown";
}

function itemTitle(item: any) {
  return (
    item?.label ||
    item?.title ||
    item?.description ||
    item?.code ||
    item?.item_code ||
    item?.rule_key ||
    "Untitled item"
  );
}

function itemNotes(item: any) {
  return (
    item?.notes ||
    item?.suggested_fix ||
    item?.fail_reason ||
    item?.remediation_guidance ||
    item?.evidence ||
    null
  );
}

function evidenceList(item: any): any[] {
  if (Array.isArray(item?.evidence)) return item.evidence;
  if (Array.isArray(item?.latest_evidence)) return item.latest_evidence;
  if (Array.isArray(item?.evidence_json)) return item.evidence_json;
  return [];
}

function photoList(item: any): any[] {
  if (Array.isArray(item?.photo_references)) return item.photo_references;
  if (Array.isArray(item?.photo_references_json))
    return item.photo_references_json;
  if (Array.isArray(item?.latest_photos)) return item.latest_photos;
  return [];
}

function formatDate(v: any) {
  if (!v) return "—";
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

function money(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function percent(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return "—";
  return `${Math.round(Number(value))}%`;
}

function FindingCard({ item, tone }: { item: any; tone: "bad" | "warn" }) {
  const outer =
    tone === "bad"
      ? "rounded-2xl border border-red-500/20 bg-red-500/[0.04] px-3 py-3"
      : "rounded-2xl border border-amber-400/20 bg-amber-500/[0.04] px-3 py-3";
  const titleTone = tone === "bad" ? "text-red-200" : "text-amber-100";
  const metaTone = tone === "bad" ? "text-red-200/70" : "text-amber-100/70";
  const detailTone = tone === "bad" ? "text-red-100/85" : "text-amber-50/90";

  return (
    <div className={outer}>
      <div className={`text-sm font-medium ${titleTone}`}>
        {itemTitle(item)}
      </div>
      <div className={`mt-1 text-xs ${metaTone}`}>
        {(item?.category || itemSeverity(item) || "uncategorized").toString()}
      </div>
      {itemNotes(item) ? (
        <div className={`mt-2 text-sm leading-6 ${detailTone}`}>
          {itemNotes(item)}
        </div>
      ) : null}
    </div>
  );
}

function InspectionHistoryCard({
  inspection,
  active,
}: {
  inspection: InspectionRecord;
  active?: boolean;
}) {
  const passed = inspection?.passed;
  const resultStatus =
    inspection?.result_status || inspection?.readiness_status || undefined;

  return (
    <div
      className={[
        "rounded-2xl border px-4 py-4",
        active ? "border-app-strong bg-app-muted" : "border-app bg-app-panel",
      ].join(" ")}
    >
      <div className="flex flex-wrap items-center gap-2">
        <div className="text-sm font-semibold text-app-0">
          {inspection?.inspection_date ||
            `Inspection #${inspection?.id || "—"}`}
        </div>
        {active ? <span className="oh-pill oh-pill-accent">Latest</span> : null}
        {passed === true ? (
          <span className="oh-pill oh-pill-good">Passed</span>
        ) : null}
        {passed === false ? (
          <span className="oh-pill oh-pill-bad">Failed</span>
        ) : null}
        {inspection?.reinspect_required ? (
          <span className="oh-pill oh-pill-bad">Reinspection required</span>
        ) : null}
        {resultStatus ? (
          <span className={statusTone(resultStatus)}>
            {titleCase(resultStatus)}
          </span>
        ) : null}
      </div>

      <div className="mt-3 grid gap-2 text-sm text-app-3 md:grid-cols-2">
        <div>Inspector: {inspection?.inspector || "—"}</div>
        <div>Jurisdiction: {inspection?.jurisdiction || "—"}</div>
        <div>Template: {inspection?.template_key || "—"}</div>
        <div>Version: {inspection?.template_version || "—"}</div>
        <div>
          Readiness score:{" "}
          {inspection?.readiness_score != null
            ? `${Number(inspection.readiness_score).toFixed(1)}%`
            : "—"}
        </div>
        <div>ID: {inspection?.id || "—"}</div>
      </div>
    </div>
  );
}

function ChecklistExecutionCard({ item }: { item: any }) {
  const status = itemStatus(item);
  const severity = itemSeverity(item);
  const notes = itemNotes(item);
  const evidence = evidenceList(item);
  const photos = photoList(item);

  return (
    <div
      className={[
        "rounded-2xl border px-4 py-4",
        severity === "critical" || status === "fail"
          ? "border-red-500/20 bg-red-500/[0.04]"
          : status === "blocked" || status === "inconclusive"
            ? "border-amber-400/20 bg-amber-500/[0.06]"
            : "border-app bg-app-panel",
      ].join(" ")}
    >
      <div className="flex flex-wrap items-center gap-2">
        <div className="text-sm font-semibold text-app-0">
          {itemTitle(item)}
        </div>
        <span className={statusTone(status)}>{titleCase(status)}</span>
        <span className={badgeTone(severity)}>{titleCase(severity)}</span>
        {item?.code || item?.item_code || item?.rule_key ? (
          <span className="oh-pill">
            {item?.code || item?.item_code || item?.rule_key}
          </span>
        ) : null}
      </div>

      {item?.category ? (
        <div className="mt-2 text-xs text-app-4">
          {titleCase(item.category)}
        </div>
      ) : null}
      {notes ? (
        <div className="mt-3 text-sm leading-6 text-app-3">{notes}</div>
      ) : null}

      {evidence.length > 0 || photos.length > 0 ? (
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
              <ClipboardList className="h-3.5 w-3.5" />
              Evidence
            </div>
            {evidence.length ? (
              <div className="mt-2 space-y-1 text-sm text-app-3">
                {evidence.slice(0, 4).map((entry: any, idx: number) => (
                  <div
                    key={`${item?.code || item?.item_code || "evidence"}-${idx}`}
                    className="break-words"
                  >
                    {typeof entry === "string" ? entry : JSON.stringify(entry)}
                  </div>
                ))}
              </div>
            ) : (
              <div className="mt-2 text-sm text-app-4">
                No evidence attached.
              </div>
            )}
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
              <Camera className="h-3.5 w-3.5" />
              Photo references
            </div>
            {photos.length ? (
              <div className="mt-2 space-y-1 text-sm text-app-3">
                {photos.slice(0, 4).map((entry: any, idx: number) => (
                  <div
                    key={`${item?.code || item?.item_code || "photo"}-${idx}`}
                    className="break-words"
                  >
                    {typeof entry === "string" ? entry : JSON.stringify(entry)}
                  </div>
                ))}
              </div>
            ) : (
              <div className="mt-2 text-sm text-app-4">
                No photo references attached.
              </div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function buildCoverage(briefPayload: BriefPayload | null) {
  const brief = briefPayload?.brief || {};
  const coverage = brief?.coverage || {};
  return {
    ...coverage,
    coverage_confidence:
      coverage?.coverage_confidence || coverage?.confidence_label,
    production_readiness: coverage?.production_readiness,
    resolved_rule_version:
      coverage?.resolved_rule_version || brief?.projection?.rules_version,
    last_refreshed: coverage?.last_refreshed,
    source_evidence:
      coverage?.source_evidence ||
      coverage?.evidence ||
      brief?.source_evidence ||
      [],
    resolved_layers:
      coverage?.resolved_layers ||
      coverage?.layers ||
      brief?.resolved_layers ||
      [],
  };
}

export default function PropertyCompliancePanel({
  property,
  compliance,
  photoAnalysis,
}: {
  property?: PropertyLike;
  compliance?: any;
  photoAnalysis?: any;
}) {
  const [briefPayload, setBriefPayload] = React.useState<BriefPayload | null>(
    compliance || null,
  );
  const [readiness, setReadiness] = React.useState<any | null>(null);
  const [workflow, setWorkflow] = React.useState<Workflow | null>(null);
  const [projectionSnapshot, setProjectionSnapshot] = React.useState<
    any | null
  >(null);
  const [scheduleSummary, setScheduleSummary] = React.useState<any | null>(
    null,
  );
  const [documentStack, setDocumentStack] = React.useState<any | null>(null);
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
        : api.get(`/compliance/properties/${property.id}/brief`),
      api.get(`/compliance/property/${property.id}/inspection-readiness`),
      api.get(`/compliance/properties/${property.id}/workflow`),
      api.get(`/compliance/properties/${property.id}/projection`),
      api.get(`/inspections/property/${property.id}/schedule-summary`),
      api.get(`/compliance/properties/${property.id}/document-stack`),
    ])
      .then((results) => {
        if (cancelled) return;

        const briefRes = results[0];
        const readinessRes = results[1];
        const workflowRes = results[2];
        const projectionRes = results[3];
        const scheduleRes = results[4];
        const documentRes = results[5];

        if (briefRes.status === "fulfilled")
          setBriefPayload((briefRes.value as any) || null);
        if (readinessRes.status === "fulfilled")
          setReadiness((readinessRes.value as any) || null);
        if (workflowRes.status === "fulfilled")
          setWorkflow(
            ((workflowRes.value as any)?.workflow || null) as Workflow | null,
          );
        if (projectionRes.status === "fulfilled")
          setProjectionSnapshot((projectionRes.value as any) || null);
        if (scheduleRes.status === "fulfilled")
          setScheduleSummary((scheduleRes.value as any) || null);
        if (documentRes.status === "fulfilled") {
          setDocumentStack(
            (documentRes.value as any)?.documents ||
              (documentRes.value as any) ||
              null,
          );
        }

        if (
          briefRes.status === "rejected" &&
          readinessRes.status === "rejected" &&
          workflowRes.status === "rejected" &&
          projectionRes.status === "rejected" &&
          scheduleRes.status === "rejected" &&
          documentRes.status === "rejected"
        ) {
          throw (
            briefRes.reason ||
            readinessRes.reason ||
            workflowRes.reason ||
            projectionRes.reason ||
            scheduleRes.reason ||
            documentRes.reason
          );
        }
      })
      .catch((e: any) => {
        if (!cancelled) setError(String(e?.message || e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [property?.id, compliance]);

  const brief = briefPayload?.brief || {};
  const projection: Projection | null =
    brief?.projection || projectionSnapshot?.projection || null;
  const coverage = buildCoverage(briefPayload || null);
  const requiredActions = toArray(brief?.required_actions);
  const blockingItems = toArray(brief?.blocking_items);
  const verifiedRules = toArray(brief?.verified_rules);

  const readinessState = readiness?.readiness || {};
  const readinessSummary = readiness?.readiness_summary || {};
  const readinessMeta = readinessSummary?.readiness || {};
  const completionMeta = readinessSummary?.completion || {};
  const readinessBlockingItems = toArray(readiness?.blocking_items);
  const readinessWarningItems = toArray(readiness?.warning_items);
  const recommendedActions = toArray(readiness?.recommended_actions);
  const failureActions = toArray(
    readiness?.inspection_failure_actions?.recommended_actions,
  );
  const inspectionHistory = normalizeInspectionHistory(readiness);
  const checklistItems = normalizeChecklistItems(readiness);

  const latestInspection =
    readiness?.latest_inspection || inspectionHistory[0] || null;
  const mergedBlockingItems =
    readinessBlockingItems.length > 0 ? readinessBlockingItems : blockingItems;
  const criticalChecklistItems = checklistItems.filter(
    (item) => itemSeverity(item) === "critical",
  );
  const failedChecklistItems = checklistItems.filter(
    (item) => itemStatus(item) === "fail",
  );
  const blockedChecklistItems = checklistItems.filter(
    (item) => itemStatus(item) === "blocked",
  );
  const displayedChecklist =
    checklistItems.length > 0 ? checklistItems : mergedBlockingItems;
  const appointment =
    scheduleSummary?.appointment || scheduleSummary?.latest_appointment || null;
  const documents = Array.isArray(documentStack?.rows)
    ? documentStack.rows
    : Array.isArray(documentStack?.documents)
      ? documentStack.documents
      : [];
  const evidenceRows = toArray(
    projectionSnapshot?.evidence || coverage?.source_evidence,
  );
  const layerRows = toArray(coverage?.resolved_layers);
  const impactedRules = toArray(projection?.impacted_rules);
  const unresolvedGaps = toArray(projection?.unresolved_evidence_gaps);
  const complianceGate = workflow?.compliance_gate || {};
  const preCloseRisk = workflow?.pre_close_risk || {};
  const postCloseRecheck = workflow?.post_close_recheck || {};

  async function refreshProjection() {
    if (!property?.id) return;
    try {
      setLoading(true);
      const [briefRes, workflowRes, projectionRes, docsRes] = await Promise.all(
        [
          api.get(
            `/compliance/properties/${property.id}/brief?rebuild_projection=1`,
          ),
          api.get(`/compliance/properties/${property.id}/workflow`),
          api.get(`/compliance/properties/${property.id}/projection?rebuild=1`),
          api.get(`/compliance/properties/${property.id}/document-stack`),
        ],
      );
      setBriefPayload(briefRes || null);
      setWorkflow((workflowRes?.workflow || null) as Workflow | null);
      setProjectionSnapshot(projectionRes || null);
      setDocumentStack(docsRes?.documents || docsRes || null);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  const [photoAnalysisState, setPhotoAnalysisState] = React.useState<
    any | null
  >(photoAnalysis || null);
  const [photoBusy, setPhotoBusy] = React.useState(false);
  const [selectedFindingCodes, setSelectedFindingCodes] = React.useState<
    string[]
  >([]);
  const [markPhotoTasksBlocking, setMarkPhotoTasksBlocking] =
    React.useState(false);

  React.useEffect(() => {
    setPhotoAnalysisState(photoAnalysis || null);
  }, [photoAnalysis]);

  const selectedInspectionId =
    scheduleSummary?.appointment?.inspection_id ||
    scheduleSummary?.latest_appointment?.inspection_id ||
    readiness?.latest_inspection?.id ||
    null;

  function syncSelectedFindings(analysis: any) {
    setPhotoAnalysisState(analysis);
    const codes = (Array.isArray(analysis?.findings) ? analysis.findings : [])
      .map((item: any) =>
        String(item?.code || item?.rule_mapping?.code || "").toUpperCase(),
      )
      .filter(Boolean);
    setSelectedFindingCodes(codes);
  }

  async function previewCompliancePhotoFindings() {
    if (!property?.id) return;
    try {
      setPhotoBusy(true);
      const form = new FormData();
      if (selectedInspectionId != null)
        form.append("inspection_id", String(selectedInspectionId));
      const result = await api.post(
        `/photos/${property.id}/compliance-preview`,
        form,
      );
      syncSelectedFindings(result);
    } finally {
      setPhotoBusy(false);
    }
  }

  async function createComplianceTasksFromPhotos() {
    if (!property?.id || !selectedFindingCodes.length) return;
    try {
      setPhotoBusy(true);
      const form = new FormData();
      form.append("confirmed_codes", selectedFindingCodes.join(","));
      form.append("mark_blocking", String(markPhotoTasksBlocking));
      if (selectedInspectionId != null)
        form.append("inspection_id", String(selectedInspectionId));
      const result = await api.post(
        `/photos/${property.id}/compliance-tasks`,
        form,
      );
      if (result?.findings) {
        syncSelectedFindings({
          ...(photoAnalysisState || {}),
          ...result,
          findings: result.findings,
          issues: result.findings,
        });
      }
      api
        .get(`/compliance/property/${property.id}/inspection-readiness`)
        .then(setReadiness)
        .catch(() => {});
    } finally {
      setPhotoBusy(false);
    }
  }

  return (
    <Surface
      title="Compliance posture"
      subtitle="Property-scoped compliance merges projection, workflow gating, inspection history, checklist execution, local rule coverage, scheduling, and evidence."
      actions={
        <div className="flex flex-wrap gap-2">
          {workflow?.current_stage_label ? (
            <span className={badgeTone(workflow.current_stage_label)}>
              {titleCase(workflow.current_stage_label)}
            </span>
          ) : null}
          {workflow?.current_pane_label ? (
            <span className="oh-pill">
              {titleCase(workflow.current_pane_label)}
            </span>
          ) : null}
          {projection?.projection_status ? (
            <span className={badgeTone(projection.projection_status)}>
              {titleCase(projection.projection_status)}
            </span>
          ) : null}
          <button
            type="button"
            className="oh-btn oh-btn-secondary"
            onClick={() => void refreshProjection()}
            disabled={loading}
          >
            {loading ? "Refreshing..." : "Refresh projection"}
          </button>
        </div>
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
      ) : !briefPayload && !readiness && !projection ? (
        <EmptyState
          compact
          title="No compliance data yet"
          description="Once compliance projection and readiness are computed, the property-level view will appear here."
        />
      ) : (
        <div className="space-y-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <Field
              label="Market"
              value={
                <span className="flex items-center gap-2">
                  <Building2 className="h-4 w-4 text-app-4" />
                  {[property?.address, property?.city, property?.state]
                    .filter(Boolean)
                    .join(" · ") || "—"}
                </span>
              }
            />
            <Field
              label="Readiness"
              value={
                <span className={badgeTone(projection?.projection_status)}>
                  {percent(projection?.readiness_score)}
                </span>
              }
            />
            <Field
              label="Confidence"
              value={
                <span
                  className={badgeTone(
                    coverage?.coverage_confidence ||
                      projection?.confidence_score,
                  )}
                >
                  {projection?.confidence_score != null
                    ? Number(projection.confidence_score).toFixed(2)
                    : fmtBoolish(coverage?.coverage_confidence)}
                </span>
              }
            />
            <Field
              label="Projected cost"
              value={
                <span className="flex items-center gap-2">
                  <Wrench className="h-4 w-4 text-app-4" />
                  {money(projection?.projected_compliance_cost)}
                </span>
              }
            />
          </div>

          <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
            <Field
              label="Blocking"
              value={
                <span
                  className={badgeTone(
                    projection?.blocking_count ? "blocked" : "ok",
                  )}
                >
                  {projection?.blocking_count ?? 0}
                </span>
              }
            />
            <Field
              label="Unknown"
              value={
                <span
                  className={badgeTone(
                    projection?.unknown_count ? "warning" : "ok",
                  )}
                >
                  {projection?.unknown_count ?? 0}
                </span>
              }
            />
            <Field
              label="Stale"
              value={
                <span
                  className={badgeTone(
                    projection?.stale_count ? "stale" : "ok",
                  )}
                >
                  {projection?.stale_count ?? 0}
                </span>
              }
            />
            <Field
              label="Conflicting"
              value={
                <span
                  className={badgeTone(
                    projection?.conflicting_count ? "conflicting" : "ok",
                  )}
                >
                  {projection?.conflicting_count ?? 0}
                </span>
              }
            />
            <Field
              label="Evidence gaps"
              value={
                <span
                  className={badgeTone(
                    projection?.evidence_gap_count ? "warning" : "ok",
                  )}
                >
                  {projection?.evidence_gap_count ?? 0}
                </span>
              }
            />
            <Field
              label="Days to rent impact"
              value={
                <span className="flex items-center gap-2">
                  <CalendarClock className="h-4 w-4 text-app-4" />
                  {projection?.projected_days_to_rent ?? "—"}
                </span>
              }
            />
          </div>

          {complianceGate?.blocked_reason ||
          complianceGate?.warning_reason ||
          preCloseRisk?.summary ||
          postCloseRecheck?.needed ? (
            <div className="grid gap-3">
              {complianceGate?.blocked_reason ? (
                <div className="rounded-2xl border border-red-500/20 bg-red-500/[0.04] px-4 py-3">
                  <div className="flex items-start gap-2 text-sm text-red-100">
                    <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0" />
                    <div>
                      <div className="font-semibold">Workflow blocked</div>
                      <div className="mt-1">
                        {complianceGate.blocked_reason}
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}

              {!complianceGate?.blocked_reason && preCloseRisk?.summary ? (
                <div className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3">
                  <div className="flex items-start gap-2 text-sm text-amber-100">
                    <TriangleAlert className="mt-0.5 h-4 w-4 shrink-0" />
                    <div>
                      <div className="font-semibold">Pre-close risk</div>
                      <div className="mt-1">{preCloseRisk.summary}</div>
                    </div>
                  </div>
                </div>
              ) : null}

              {postCloseRecheck?.needed ? (
                <div className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3">
                  <div className="flex items-start gap-2 text-sm text-amber-100">
                    <CalendarClock className="mt-0.5 h-4 w-4 shrink-0" />
                    <div>
                      <div className="font-semibold">
                        Post-close recheck needed
                      </div>
                      <div className="mt-1">
                        {postCloseRecheck.reason ||
                          "Compliance should be re-evaluated."}
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}

              {(complianceGate?.warnings || []).length ? (
                <div className="grid gap-2">
                  {(complianceGate.warnings || []).map(
                    (warning: string, idx: number) => (
                      <div
                        key={`${warning}-${idx}`}
                        className="rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-2"
                      >
                        {warning}
                      </div>
                    ),
                  )}
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="grid gap-4 xl:grid-cols-2">
            <Surface
              title="Jurisdiction coverage"
              subtitle="Coverage completeness and freshness used in this projection"
            >
              <div className="grid gap-3">
                <div className="flex flex-wrap gap-2">
                  <span className={badgeTone(coverage?.production_readiness)}>
                    {fmtBoolish(coverage?.production_readiness)}
                  </span>
                  <span className={badgeTone(coverage?.completeness_status)}>
                    {fmtBoolish(coverage?.completeness_status)}
                  </span>
                  {coverage?.is_stale ? (
                    <span className="oh-pill oh-pill-warn">Stale</span>
                  ) : null}
                </div>

                <div className="grid gap-3 md:grid-cols-4">
                  <Field
                    label="Completeness score"
                    value={percent(
                      (Number(coverage?.completeness_score || 0) || 0) * 100,
                    )}
                  />
                  <Field
                    label="Coverage confidence"
                    value={
                      <span
                        className={badgeTone(coverage?.coverage_confidence)}
                      >
                        {fmtBoolish(coverage?.coverage_confidence)}
                      </span>
                    }
                  />
                  <Field
                    label="Rule version"
                    value={
                      coverage?.resolved_rule_version ||
                      projection?.rules_version ||
                      "—"
                    }
                  />
                  <Field
                    label="Last refreshed"
                    value={formatDate(coverage?.last_refreshed)}
                  />
                </div>

                {coverage?.stale_reason ? (
                  <div className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3 text-sm text-amber-100">
                    {coverage.stale_reason}
                  </div>
                ) : null}

                <div className="grid gap-3 md:grid-cols-3">
                  <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Covered categories
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {(coverage?.covered_categories || []).length ? (
                        (coverage.covered_categories || []).map(
                          (item: string) => (
                            <span key={item} className="oh-pill oh-pill-good">
                              {titleCase(item)}
                            </span>
                          ),
                        )
                      ) : (
                        <span className="text-sm text-app-4">None listed</span>
                      )}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Missing categories
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {(coverage?.missing_categories || []).length ? (
                        (coverage.missing_categories || []).map(
                          (item: string) => (
                            <span key={item} className="oh-pill oh-pill-warn">
                              {titleCase(item)}
                            </span>
                          ),
                        )
                      ) : (
                        <span className="oh-pill oh-pill-good">
                          No known gaps
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Required categories
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {(coverage?.required_categories || []).length ? (
                        (coverage.required_categories || []).map(
                          (item: string) => (
                            <span key={item} className="oh-pill">
                              {titleCase(item)}
                            </span>
                          ),
                        )
                      ) : (
                        <span className="text-sm text-app-4">None listed</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </Surface>

            <Surface
              title="Projection outcomes"
              subtitle="Rules, blockers, unknowns, and evidence gaps computed for this property"
            >
              <div className="grid gap-3">
                <div className="grid gap-3 md:grid-cols-2">
                  <Field
                    label="Confirmed proofs"
                    value={
                      <span className={badgeTone("confirmed")}>
                        {projection?.confirmed_count ?? 0}
                      </span>
                    }
                  />
                  <Field
                    label="Inferred proofs"
                    value={
                      <span className={badgeTone("partial")}>
                        {projection?.inferred_count ?? 0}
                      </span>
                    }
                  />
                  <Field
                    label="Failing items"
                    value={
                      <span
                        className={badgeTone(
                          projection?.failing_count ? "blocked" : "ok",
                        )}
                      >
                        {projection?.failing_count ?? 0}
                      </span>
                    }
                  />
                  <Field
                    label="Last projected"
                    value={formatDate(projection?.last_projected_at)}
                  />
                </div>

                {unresolvedGaps.length ? (
                  <div>
                    <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-app-0">
                      <AlertTriangle className="h-4 w-4" />
                      Unresolved evidence gaps
                    </div>
                    <div className="grid gap-2">
                      {unresolvedGaps
                        .slice(0, 6)
                        .map((item: any, idx: number) => (
                          <div
                            key={`${item?.rule_key || "gap"}-${idx}`}
                            className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3"
                          >
                            <div className="text-sm font-semibold text-amber-100">
                              {titleCase(
                                item?.rule_key ||
                                  item?.category ||
                                  "Evidence gap",
                              )}
                            </div>
                            <div className="mt-1 text-sm text-amber-50/90">
                              {item?.gap || "Evidence still needed."}
                            </div>
                          </div>
                        ))}
                    </div>
                  </div>
                ) : null}

                {impactedRules.length ? (
                  <div>
                    <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-app-0">
                      <ShieldAlert className="h-4 w-4" />
                      Impacted rules
                    </div>
                    <div className="grid gap-2">
                      {impactedRules
                        .slice(0, 6)
                        .map((item: any, idx: number) => (
                          <div
                            key={`${item?.rule_key || "impact"}-${idx}`}
                            className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <div className="text-sm font-semibold text-app-0">
                                {titleCase(item?.rule_key || "Rule")}
                              </div>
                              {item?.evaluation_status ? (
                                <span
                                  className={statusTone(item.evaluation_status)}
                                >
                                  {titleCase(item.evaluation_status)}
                                </span>
                              ) : null}
                            </div>
                          </div>
                        ))}
                    </div>
                  </div>
                ) : null}
              </div>
            </Surface>
          </div>

          <div className="grid gap-4 xl:grid-cols-[1fr_1fr]">
            <div className="space-y-4">
              <Surface
                title="Required actions"
                subtitle="Actions from compliance brief and readiness projections"
              >
                {!requiredActions.length &&
                !recommendedActions.length &&
                !failureActions.length ? (
                  <EmptyState
                    compact
                    title="No actions generated"
                    description="No required or recommended actions were returned for this property."
                  />
                ) : (
                  <div className="grid gap-3">
                    {[
                      ...requiredActions,
                      ...recommendedActions,
                      ...failureActions,
                    ]
                      .slice(0, 10)
                      .map((item: any, idx: number) => (
                        <div
                          key={`${item?.title || item?.label || idx}`}
                          className="rounded-2xl border border-app bg-app-muted px-4 py-4"
                        >
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="text-sm font-semibold text-app-0">
                              {item?.title ||
                                item?.label ||
                                item?.action ||
                                item?.code ||
                                "Untitled action"}
                            </div>
                            {item?.priority ? (
                              <span className={badgeTone(item.priority)}>
                                {titleCase(item.priority)}
                              </span>
                            ) : null}
                          </div>
                          {item?.notes || item?.detail ? (
                            <div className="mt-2 text-sm text-app-3">
                              {item?.notes || item?.detail}
                            </div>
                          ) : null}
                        </div>
                      ))}
                  </div>
                )}
              </Surface>

              <Surface
                title="Blocking and warning items"
                subtitle="Unresolved blockers from the latest readiness and brief outputs"
              >
                {!mergedBlockingItems.length &&
                !readinessWarningItems.length ? (
                  <EmptyState
                    compact
                    title="No active blockers"
                    description="No blocking or warning items were returned."
                  />
                ) : (
                  <div className="grid gap-3">
                    {mergedBlockingItems
                      .slice(0, 6)
                      .map((item: any, idx: number) => (
                        <FindingCard
                          key={`blocking-${idx}`}
                          item={item}
                          tone="bad"
                        />
                      ))}
                    {readinessWarningItems
                      .slice(0, 4)
                      .map((item: any, idx: number) => (
                        <FindingCard
                          key={`warn-${idx}`}
                          item={item}
                          tone="warn"
                        />
                      ))}
                  </div>
                )}
              </Surface>

              <Surface
                title="Verified rules"
                subtitle="Resolved jurisdiction rules currently applied to this property"
              >
                {!verifiedRules.length ? (
                  <EmptyState
                    compact
                    title="No verified rules"
                    description="No verified rules were returned for this property."
                  />
                ) : (
                  <div className="grid gap-2">
                    {verifiedRules
                      .slice(0, 10)
                      .map((item: any, idx: number) => (
                        <div
                          key={`${item?.rule_key || idx}`}
                          className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                        >
                          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                            <FileCheck2 className="h-4 w-4" />
                            {item?.label || titleCase(item?.rule_key || "Rule")}
                          </div>
                          <div className="mt-2 flex flex-wrap gap-2">
                            {item?.category ? (
                              <span className="oh-pill">
                                {titleCase(item.category)}
                              </span>
                            ) : null}
                            {item?.status ? (
                              <span className={badgeTone(item.status)}>
                                {titleCase(item.status)}
                              </span>
                            ) : null}
                            {item?.source_level ? (
                              <span className="oh-pill">
                                {titleCase(item.source_level)}
                              </span>
                            ) : null}
                          </div>
                        </div>
                      ))}
                  </div>
                )}
              </Surface>
            </div>

            <div className="space-y-4">
              <Surface
                title="Inspection history"
                subtitle="Historical inspection attempts and latest execution context"
              >
                {!inspectionHistory.length ? (
                  <EmptyState
                    compact
                    title="No inspection history yet"
                    description="Historical inspection attempts will show here once inspections are created."
                  />
                ) : (
                  <div className="grid gap-3">
                    {inspectionHistory.map((inspection, idx) => (
                      <InspectionHistoryCard
                        key={`${inspection.id || idx}`}
                        inspection={inspection}
                        active={idx === 0}
                      />
                    ))}
                  </div>
                )}
              </Surface>

              <Surface
                title="Appointment and documents"
                subtitle="Scheduling state plus uploaded document evidence"
              >
                <div className="grid gap-3">
                  <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                    <div className="text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
                      Appointment
                    </div>
                    {appointment ? (
                      <div className="mt-2 space-y-2 text-sm text-app-3">
                        <div>
                          Date:{" "}
                          {appointment?.scheduled_for ||
                            appointment?.inspection_date ||
                            "—"}
                        </div>
                        <div>
                          Inspector:{" "}
                          {appointment?.inspector_name ||
                            appointment?.inspector ||
                            "—"}
                        </div>
                        <div>Status: {appointment?.status || "—"}</div>
                      </div>
                    ) : (
                      <div className="mt-2 text-sm text-app-4">
                        No appointment scheduled.
                      </div>
                    )}
                  </div>

                  <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                    <div className="text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
                      Documents
                    </div>
                    {documents.length ? (
                      <div className="mt-2 space-y-2 text-sm text-app-3">
                        {documents.slice(0, 6).map((doc: any, idx: number) => (
                          <div
                            key={`${doc?.id || idx}`}
                            className="rounded-xl border border-app bg-app-panel px-3 py-3"
                          >
                            {doc?.label ||
                              doc?.filename ||
                              doc?.title ||
                              `Document #${idx + 1}`}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="mt-2 text-sm text-app-4">
                        No compliance documents uploaded.
                      </div>
                    )}
                  </div>
                </div>
              </Surface>

              <Surface
                title="Projection evidence"
                subtitle="Evidence rows currently linked into the compliance projection"
              >
                {!evidenceRows.length ? (
                  <EmptyState
                    compact
                    title="No evidence linked"
                    description="No evidence rows were returned from the projection."
                  />
                ) : (
                  <div className="grid gap-2">
                    {evidenceRows.slice(0, 10).map((row: any, idx: number) => (
                      <div
                        key={`${row?.id || row?.evidence_key || idx}`}
                        className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                      >
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-sm font-semibold text-app-0">
                            {row?.evidence_name ||
                              row?.line_item_label ||
                              row?.rule_key ||
                              "Evidence"}
                          </div>
                          {row?.evidence_status ? (
                            <span className={statusTone(row.evidence_status)}>
                              {titleCase(row.evidence_status)}
                            </span>
                          ) : null}
                          {row?.proof_state ? (
                            <span className={badgeTone(row.proof_state)}>
                              {titleCase(row.proof_state)}
                            </span>
                          ) : null}
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {row?.rule_key ? (
                            <span className="oh-pill">
                              {titleCase(row.rule_key)}
                            </span>
                          ) : null}
                          {row?.document_kind ? (
                            <span className="oh-pill">
                              {titleCase(row.document_kind)}
                            </span>
                          ) : null}
                          {row?.evidence_source_type ? (
                            <span className="oh-pill">
                              {titleCase(row.evidence_source_type)}
                            </span>
                          ) : null}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </Surface>
            </div>
          </div>

          <Surface
            title="Checklist execution"
            subtitle="Latest property-scoped checklist items, results, evidence, and photo references"
          >
            {!displayedChecklist.length ? (
              <EmptyState
                compact
                title="No checklist items"
                description="Checklist execution items will show here once inspection templates are applied to this property."
              />
            ) : (
              <div className="grid gap-3">
                {displayedChecklist
                  .slice(0, 12)
                  .map((item: any, idx: number) => (
                    <ChecklistExecutionCard
                      key={`${item?.code || item?.item_code || idx}`}
                      item={item}
                    />
                  ))}
              </div>
            )}

            {!!criticalChecklistItems.length ||
            !!failedChecklistItems.length ||
            !!blockedChecklistItems.length ? (
              <div className="mt-4 flex flex-wrap gap-2">
                {!!criticalChecklistItems.length ? (
                  <span className="oh-pill oh-pill-bad">
                    Critical: {criticalChecklistItems.length}
                  </span>
                ) : null}
                {!!failedChecklistItems.length ? (
                  <span className="oh-pill oh-pill-bad">
                    Failed: {failedChecklistItems.length}
                  </span>
                ) : null}
                {!!blockedChecklistItems.length ? (
                  <span className="oh-pill oh-pill-warn">
                    Blocked: {blockedChecklistItems.length}
                  </span>
                ) : null}
              </div>
            ) : null}
          </Surface>

          {layerRows.length ? (
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
                Resolved rule layers
              </div>
              <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {layerRows.map((row: any, idx: number) => (
                  <div
                    key={`${row?.layer || row?.scope || "layer"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {titleCase(
                          row?.layer || row?.scope || row?.label || "layer",
                        )}
                      </div>
                      <span
                        className={badgeTone(
                          row?.confidence ||
                            row?.status ||
                            (row?.applied ? "applied" : "available"),
                        )}
                      >
                        {titleCase(
                          row?.confidence ||
                            row?.status ||
                            (row?.applied ? "applied" : "available"),
                        )}
                      </span>
                    </div>
                    <div className="mt-3 space-y-2 text-sm text-app-3">
                      <div>
                        Authority: {row?.authority || row?.source || "—"}
                      </div>
                      <div>Version: {row?.version || "—"}</div>
                      <div>Applied: {row?.applied ? "Yes" : "No"}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="grid gap-4 xl:grid-cols-2">
            <Surface
              title="Compliance documents"
              subtitle="Upload and review compliance packet documents."
            >
              <div className="grid gap-4">
                {property?.id ? (
                  <ComplianceDocumentUploader
                    propertyId={property.id}
                    onUploaded={() => void refreshProjection()}
                  />
                ) : null}
                <ComplianceDocumentStack
                  propertyId={property?.id || 0}
                  documents={documents}
                  onChanged={() => void refreshProjection()}
                />
              </div>
            </Surface>

            <Surface
              title="Photo-driven evidence"
              subtitle="Photo-linked evidence and checklist support."
            >
              <div className="grid gap-3">
                <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                    <ImageIcon className="h-4 w-4" />
                    Photo analysis linked
                  </div>
                  <div className="mt-2 text-sm text-app-3">
                    {photoAnalysisState
                      ? "Photo findings loaded for this property."
                      : "No photo analysis loaded."}
                  </div>
                </div>

                <CompliancePhotoFindingsPanel
                  analysis={photoAnalysisState}
                  busy={photoBusy}
                  selectedCodes={selectedFindingCodes}
                  onSelectedCodesChange={setSelectedFindingCodes}
                  onPreview={previewCompliancePhotoFindings}
                  onCreateTasks={createComplianceTasksFromPhotos}
                  onMarkBlockingChange={setMarkPhotoTasksBlocking}
                  markTasksBlocking={markPhotoTasksBlocking}
                />
              </div>
            </Surface>
          </div>
        </div>
      )}
    </Surface>
  );
}
