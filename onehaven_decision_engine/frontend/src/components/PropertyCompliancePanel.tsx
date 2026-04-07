import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Building2,
  CalendarClock,
  Camera,
  CheckCircle2,
  ClipboardList,
  ClipboardX,
  FileCheck2,
  FileText,
  Image as ImageIcon,
  Mail,
  ShieldAlert,
  ShieldCheck,
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
    resolved_rule_version?: string | null;
    last_refreshed?: string | null;
  };
  explanation?: string | null;
  required_actions?: any[];
  blocking_items?: any[];
  resolved_profile?: any;
  jurisdiction_profile?: any;
  source_evidence?: any[];
  resolved_layers?: any[];
  coverage?: {
    completeness_status?: string | null;
    completeness_score?: number | null;
    is_stale?: boolean | null;
    stale_reason?: string | null;
    missing_categories?: string[] | null;
    covered_categories?: string[] | null;
    required_categories?: string[] | null;
    coverage_confidence?: string | null;
    confidence_label?: string | null;
    production_readiness?: string | null;
    resolved_rule_version?: string | null;
    last_refreshed?: string | null;
    source_evidence?: any[];
    evidence?: any[];
    resolved_layers?: any[];
    layers?: any[];
  };
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
      "stale",
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
    ].includes(s)
  ) {
    return "oh-pill oh-pill-bad";
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

function statusTone(value?: string | boolean | null) {
  const s = String(value ?? "").toLowerCase();
  if (s === "true" || s === "ready" || s === "pass")
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
    ].includes(s)
  ) {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
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

function buildCoverage(brief: Brief | null) {
  const coverage = brief?.coverage || {};
  const compliance = brief?.compliance || {};
  const profile = brief?.resolved_profile || brief?.jurisdiction_profile || {};
  return {
    ...profile,
    ...coverage,
    coverage_confidence:
      coverage?.coverage_confidence ||
      coverage?.confidence_label ||
      compliance?.coverage_confidence,
    production_readiness:
      coverage?.production_readiness || compliance?.production_readiness,
    resolved_rule_version:
      coverage?.resolved_rule_version ||
      profile?.resolved_rule_version ||
      compliance?.resolved_rule_version,
    last_refreshed:
      coverage?.last_refreshed ||
      profile?.last_refreshed ||
      compliance?.last_refreshed,
    source_evidence:
      coverage?.source_evidence ||
      coverage?.evidence ||
      brief?.source_evidence ||
      profile?.source_evidence ||
      [],
    resolved_layers:
      coverage?.resolved_layers ||
      coverage?.layers ||
      brief?.resolved_layers ||
      profile?.resolved_layers ||
      profile?.layers ||
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
  const [brief, setBrief] = React.useState<Brief | null>(compliance || null);
  const [readiness, setReadiness] = React.useState<any | null>(null);
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
        : api.compliancePropertyBrief(property.id),
      api.complianceInspectionReadiness(property.id),
      api.get(`/inspections/property/${property.id}/schedule-summary`),
      api.get(`/compliance/properties/${property.id}/document-stack`),
    ])
      .then((results) => {
        if (cancelled) return;

        const briefRes = results[0];
        const readinessRes = results[1];
        const scheduleRes = results[2];
        const documentRes = results[3];

        if (briefRes.status === "fulfilled")
          setBrief((briefRes.value as any) || null);
        if (readinessRes.status === "fulfilled")
          setReadiness((readinessRes.value as any) || null);
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
          scheduleRes.status === "rejected" &&
          documentRes.status === "rejected"
        ) {
          throw (
            briefRes.reason ||
            readinessRes.reason ||
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

  const c = brief?.compliance || {};
  const m = brief?.market || {};
  const coverage = buildCoverage(brief || null);
  const requiredActions = toArray(brief?.required_actions);
  const blockingItems = toArray(brief?.blocking_items);

  const readinessState = readiness?.readiness || {};
  const readinessSummary = readiness?.readiness_summary || {};
  const readinessMeta = readinessSummary?.readiness || {};
  const completionMeta = readinessSummary?.completion || {};
  const readinessCounts = readiness?.counts || {};
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
    : [];
  const evidenceRows = toArray(coverage?.source_evidence);
  const layerRows = toArray(coverage?.resolved_layers);

  return (
    <Surface
      title="Compliance posture"
      subtitle="Property-scoped compliance merges inspection history, checklist execution state, unresolved failures, layered jurisdiction coverage, scheduling, and evidence documents."
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
                  : readinessMeta?.score != null
                    ? `${Number(readinessMeta.score).toFixed(1)}%`
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
              label="Latest inspection"
              value={
                <span className="flex flex-wrap gap-2">
                  {latestInspection?.passed === true ? (
                    <span className="oh-pill oh-pill-good">Passed</span>
                  ) : latestInspection?.passed === false ? (
                    <span className="oh-pill oh-pill-bad">Failed</span>
                  ) : (
                    <span className="oh-pill">No result</span>
                  )}
                  {latestInspection?.reinspect_required ? (
                    <span className="oh-pill oh-pill-bad">
                      Reinspection required
                    </span>
                  ) : null}
                </span>
              }
            />
            <Field
              label="Inspection date"
              value={latestInspection?.inspection_date || "—"}
            />
            <Field
              label="Inspector"
              value={latestInspection?.inspector || "—"}
            />
            <Field
              label="Template"
              value={
                latestInspection?.template_key
                  ? `${latestInspection.template_key}${latestInspection.template_version ? ` · ${latestInspection.template_version}` : ""}`
                  : "—"
              }
            />
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-sm font-semibold text-app-0">
                Jurisdiction coverage
              </div>
              <div className="flex flex-wrap gap-2">
                <span
                  className={badgeTone(
                    coverage?.coverage_confidence || coverage?.confidence_label,
                  )}
                >
                  {titleCase(
                    coverage?.coverage_confidence ||
                      coverage?.confidence_label ||
                      "unknown",
                  )}
                </span>
                <span className={badgeTone(coverage?.completeness_status)}>
                  {titleCase(coverage?.completeness_status || "unknown")}
                </span>
                {coverage?.production_readiness ? (
                  <span className={badgeTone(coverage.production_readiness)}>
                    {titleCase(coverage.production_readiness)}
                  </span>
                ) : null}
              </div>
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-4">
              <Field
                label="Completeness score"
                value={`${Math.round(Number(coverage?.completeness_score || 0) * 100)}%`}
              />
              <Field
                label="Rule version"
                value={coverage?.resolved_rule_version || "—"}
              />
              <Field
                label="Last refreshed"
                value={formatDate(coverage?.last_refreshed)}
              />
              <Field
                label="PHA / overlay"
                value={m?.pha_name || coverage?.pha_name || "—"}
              />
            </div>

            {coverage?.is_stale ? (
              <div className="mt-3 rounded-2xl border border-amber-400/20 bg-amber-500/[0.08] px-4 py-3 text-sm text-amber-100">
                <div className="flex items-start gap-2">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  <div>
                    {coverage?.stale_reason ||
                      "This rule set is stale and needs review."}
                  </div>
                </div>
              </div>
            ) : null}

            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
                  Covered categories
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {toArray(coverage?.covered_categories).length ? (
                    toArray(coverage?.covered_categories).map(
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

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-xs font-semibold uppercase tracking-[0.16em] text-app-4">
                  Missing local rule areas
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {toArray(coverage?.missing_categories).length ? (
                    toArray(coverage?.missing_categories).map(
                      (item: string) => (
                        <span key={item} className="oh-pill oh-pill-warn">
                          {titleCase(item)}
                        </span>
                      ),
                    )
                  ) : (
                    <span className="oh-pill oh-pill-good">No known gaps</span>
                  )}
                </div>
              </div>
            </div>
          </div>

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

          {evidenceRows.length ? (
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
                Source evidence
              </div>
              <div className="mt-3 grid gap-3">
                {evidenceRows.slice(0, 6).map((row: any, idx: number) => (
                  <div
                    key={`${row?.title || row?.label || row?.url || "evidence"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {row?.title ||
                          row?.label ||
                          row?.source_name ||
                          "Evidence"}
                      </div>
                      <span
                        className={
                          row?.is_authoritative
                            ? "oh-pill oh-pill-good"
                            : "oh-pill"
                        }
                      >
                        {row?.is_authoritative ? "Authoritative" : "Supporting"}
                      </span>
                    </div>
                    <div className="mt-2 text-sm text-app-3">
                      {row?.source_name || row?.source || "Unknown source"}
                    </div>
                    {row?.excerpt ? (
                      <div className="mt-3 rounded-2xl border border-app bg-app-muted px-3 py-3 text-sm text-app-2">
                        {row.excerpt}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {brief?.explanation ? (
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4 text-sm leading-6 text-app-2">
              {brief.explanation}
            </div>
          ) : null}

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

          {photoAnalysis ? (
            <CompliancePhotoFindingsPanel
              analysis={photoAnalysis}
            />
          ) : null}

          {property?.id ? (
            <div className="grid gap-4 xl:grid-cols-2">
              <Surface
                title="Compliance documents"
                subtitle="Upload and review compliance packet documents."
                actions={
                  <ComplianceDocumentUploader
                    propertyId={property.id}
                    onUploaded={() => {}}
                  />
                }
              >
                <ComplianceDocumentStack
                  data={documentStack}
                  onDeleted={() => {}}
                />
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
                      {photoAnalysis
                        ? "Photo findings loaded for this property."
                        : "No photo analysis loaded."}
                    </div>
                  </div>
                </div>
              </Surface>
            </div>
          ) : null}
        </div>
      )}
    </Surface>
  );
}
