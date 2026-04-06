// frontend/src/components/PropertyCompliancePanel.tsx
import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Building2,
  Camera,
  CheckCircle2,
  ClipboardList,
  ClipboardX,
  FileCheck2,
  Image as ImageIcon,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
  Wrench,
  XCircle,
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
    s === "verified" ||
    s === "yes" ||
    s === "ready" ||
    s === "high" ||
    s === "complete" ||
    s === "pass" ||
    s === "good"
  ) {
    return "oh-pill oh-pill-good";
  }

  if (
    s === "partial" ||
    s === "medium" ||
    s === "unknown" ||
    s === "conditional" ||
    s === "attention" ||
    s === "in_progress" ||
    s === "pending" ||
    s === "warn" ||
    s === "warning"
  ) {
    return "oh-pill oh-pill-warn";
  }

  if (
    s === "low" ||
    s === "needs_review" ||
    s === "no" ||
    s === "missing" ||
    s === "stale" ||
    s === "blocked" ||
    s === "critical_failures" ||
    s === "needs_remediation" ||
    s === "not_ready" ||
    s === "fail" ||
    s === "critical" ||
    s === "reinspection_required"
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
        ) : passed === false ? (
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

  return (
    <Surface
      title="Compliance posture"
      subtitle="Property-scoped compliance now merges inspection history, checklist execution state, unresolved failures, and remediation actions."
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

          <div className="grid gap-3 md:grid-cols-4">
            <Field
              label="Failed items"
              value={
                readinessCounts?.inspection_failed_items ??
                failedChecklistItems.length ??
                "—"
              }
            />
            <Field
              label="Blocked items"
              value={
                readinessCounts?.inspection_blocked_items ??
                blockedChecklistItems.length ??
                "—"
              }
            />
            <Field
              label="Critical items"
              value={
                readinessCounts?.inspection_failed_critical_items ??
                criticalChecklistItems.length ??
                "—"
              }
            />
            <Field
              label="History"
              value={`${inspectionHistory.length} inspection${inspectionHistory.length === 1 ? "" : "s"}`}
            />
          </div>

          <div className="grid gap-3 md:grid-cols-4">
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

          <div className="grid gap-3 md:grid-cols-4">
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
              label="Failure-driven actions"
              value={String(
                (recommendedActions.length > 0
                  ? recommendedActions
                  : failureActions
                ).length,
              )}
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

          {inspectionHistory.length > 0 ? (
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <BadgeCheck className="h-4 w-4 text-app-4" />
                Inspection history
              </div>
              <div className="mt-3 grid gap-3">
                {inspectionHistory.map((inspection, idx) => (
                  <InspectionHistoryCard
                    key={`${inspection?.id || inspection?.inspection_date || idx}`}
                    inspection={inspection}
                    active={idx === 0}
                  />
                ))}
              </div>
            </div>
          ) : null}

          {latestInspection?.reinspect_required ? (
            <div className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-red-200">
                <ShieldAlert className="h-4 w-4" />
                Reinspection required
              </div>
              <div className="mt-2 text-sm leading-6 text-red-100/90">
                The latest inspection plus unresolved failed or blocked
                checklist items still prevent this property from reaching a
                ready state.
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
                    <FindingCard
                      key={`${item?.rule_key || item?.code || item?.key || item?.title || idx}`}
                      item={item}
                      tone="bad"
                    />
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
                    <FindingCard
                      key={`${item?.rule_key || item?.code || item?.key || item?.title || idx}`}
                      item={item}
                      tone="warn"
                    />
                  ))}
              </div>
            </div>
          ) : null}

          <div className="grid gap-4 xl:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <CheckCircle2 className="h-4 w-4 text-emerald-400" />
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
                <Wrench className="h-4 w-4 text-app-4" />
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
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-sm font-medium text-app-0">
                            {item?.label ||
                              item?.title ||
                              item?.description ||
                              item?.code ||
                              "Untitled remediation"}
                          </div>
                          {item?.priority ? (
                            <span className={statusTone(item.priority)}>
                              {titleCase(item.priority)}
                            </span>
                          ) : null}
                          {item?.severity ? (
                            <span className={statusTone(item.severity)}>
                              {titleCase(item.severity)}
                            </span>
                          ) : null}
                          {item?.requires_reinspection ? (
                            <span className="oh-pill oh-pill-bad">
                              Reinspection
                            </span>
                          ) : null}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {(
                            item?.rehab_category ||
                            item?.category ||
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

          <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <ClipboardList className="h-4 w-4 text-app-4" />
              Inspection-driven checklist execution
            </div>

            {!displayedChecklist.length ? (
              <div className="mt-3 text-sm text-app-4">
                No checklist execution rows were returned for this property.
              </div>
            ) : (
              <div className="mt-3 grid gap-3">
                {displayedChecklist
                  .slice(0, 12)
                  .map((item: any, idx: number) => (
                    <ChecklistExecutionCard
                      key={`${item?.rule_key || item?.code || item?.item_code || item?.title || idx}`}
                      item={item}
                    />
                  ))}
              </div>
            )}
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <ShieldCheck className="h-4 w-4 text-app-4" />
                Property-scoped compliance
              </div>
              <div className="mt-2 text-sm leading-6 text-app-3">
                This panel now reflects the latest inspection, inspection
                history, checklist execution state, unresolved failures, blocked
                items, and failure-driven remediation actions directly on the
                property.
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              {m?.pha_name ? (
                <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                  <TriangleAlert className="h-4 w-4 text-app-4" />
                  PHA: {m.pha_name}
                </div>
              ) : (
                <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                  <ImageIcon className="h-4 w-4 text-app-4" />
                  No specific PHA override shown in this brief
                </div>
              )}
              <div className="mt-2 text-sm leading-6 text-app-3">
                Inspection notes, evidence, photo references, and remediation
                guidance are treated as execution data, not just template
                defaults.
              </div>
            </div>
          </div>
        </div>
      )}
    </Surface>
  );
}
