// frontend/src/components/InspectionReadiness.tsx
import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Camera,
  ClipboardList,
  ClipboardX,
  PlayCircle,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
  Wrench,
} from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

function toneClass(
  kind: "good" | "warn" | "bad" | "neutral" | "accent" = "neutral",
) {
  if (kind === "good") return "oh-pill oh-pill-good";
  if (kind === "warn") return "oh-pill oh-pill-warn";
  if (kind === "bad") return "oh-pill oh-pill-bad";
  if (kind === "accent") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

function readinessPill(label: string, ok: boolean | undefined) {
  const tone = ok === true ? "good" : ok === false ? "bad" : "neutral";
  return (
    <span key={label} className={toneClass(tone)}>
      {label}: {ok === true ? "ready" : ok === false ? "blocked" : "—"}
    </span>
  );
}

function postureTone(posture?: string) {
  const p = String(posture || "").toLowerCase();
  if (p === "inspection_ready") return "good";
  if (
    [
      "critical_failures",
      "needs_remediation",
      "not_ready",
      "reinspection_required",
    ].includes(p)
  ) {
    return "bad";
  }
  if (["in_progress", "unknown"].includes(p)) return "warn";
  return "neutral";
}

function statusTone(status?: string) {
  const s = String(status || "").toLowerCase();
  if (["pass", "ready"].includes(s)) return "good";
  if (
    [
      "fail",
      "blocked",
      "critical",
      "needs_work",
      "reinspection_required",
    ].includes(s)
  )
    return "bad";
  if (["attention", "unknown", "pending", "inconclusive"].includes(s))
    return "warn";
  return "neutral";
}

function titleCase(v: any) {
  return String(v || "")
    .replace(/_/g, " ")
    .trim();
}

function toArray<T = any>(value: any): T[] {
  return Array.isArray(value) ? value : [];
}

function itemStatus(item: any) {
  return String(
    item?.result_status ||
      item?.status ||
      item?.latest_result_status ||
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

function normalizeInspectionHistory(model: any) {
  const candidates = [
    model?.inspection_history,
    model?.history,
    model?.inspections,
    model?.latest_inspection_history,
    model?.readiness_summary?.history,
  ];
  for (const candidate of candidates) {
    if (Array.isArray(candidate) && candidate.length > 0) return candidate;
  }
  if (model?.latest_inspection) return [model.latest_inspection];
  return [];
}

type ActionRow = {
  rule_key?: string;
  label?: string;
  title?: string;
  source?: string;
  status?: string;
  severity?: string;
  category?: string;
  suggested_fix?: string;
  notes?: string;
  evidence?: string | null;
  blocks_hqs?: boolean;
  blocks_local?: boolean;
  blocks_voucher?: boolean;
  blocks_lease_up?: boolean;
  requires_reinspection?: boolean;
};

function ActionCard({
  item,
  tone = "bad",
}: {
  item: ActionRow;
  tone?: "bad" | "warn";
}) {
  const title =
    item?.label || item?.title || item?.rule_key || "Untitled finding";
  const detail = item?.suggested_fix || item?.notes || item?.evidence || "";

  return (
    <div
      className={
        tone === "bad"
          ? "rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-4"
          : "rounded-2xl border border-amber-400/20 bg-amber-500/10 px-4 py-4"
      }
    >
      <div className="flex flex-wrap items-center gap-2">
        <div
          className={
            tone === "bad"
              ? "text-sm font-semibold text-red-200"
              : "text-sm font-semibold text-amber-100"
          }
        >
          {title}
        </div>

        {item?.severity ? (
          <span
            className={
              tone === "bad" ? "oh-pill oh-pill-bad" : "oh-pill oh-pill-warn"
            }
          >
            {titleCase(item.severity)}
          </span>
        ) : null}

        {item?.category ? (
          <span className="oh-pill">{titleCase(item.category)}</span>
        ) : null}
        {item?.requires_reinspection ? (
          <span className="oh-pill oh-pill-bad">Reinspection</span>
        ) : null}
      </div>

      {detail ? (
        <div
          className={
            tone === "bad"
              ? "mt-2 text-sm leading-6 text-red-100/85"
              : "mt-2 text-sm leading-6 text-amber-50/90"
          }
        >
          {detail}
        </div>
      ) : null}

      <div className="mt-2 flex flex-wrap gap-2 text-xs text-app-4">
        {item?.rule_key ? <span>code: {item.rule_key}</span> : null}
        {item?.source ? <span>source: {item.source}</span> : null}
        {item?.blocks_hqs ? <span>blocks HQS</span> : null}
        {item?.blocks_local ? <span>blocks local</span> : null}
        {item?.blocks_voucher ? <span>blocks voucher</span> : null}
        {item?.blocks_lease_up ? <span>blocks lease-up</span> : null}
      </div>
    </div>
  );
}

function HistoryCard({
  inspection,
  active,
}: {
  inspection: any;
  active?: boolean;
}) {
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
        {inspection?.passed === true ? (
          <span className="oh-pill oh-pill-good">Passed</span>
        ) : inspection?.passed === false ? (
          <span className="oh-pill oh-pill-bad">Failed</span>
        ) : null}
        {inspection?.reinspect_required ? (
          <span className="oh-pill oh-pill-bad">Reinspection required</span>
        ) : null}
        {inspection?.result_status ? (
          <span className={toneClass(statusTone(inspection.result_status))}>
            {titleCase(inspection.result_status)}
          </span>
        ) : null}
      </div>

      <div className="mt-3 grid gap-2 text-sm text-app-3 md:grid-cols-2">
        <div>Inspector: {inspection?.inspector || "—"}</div>
        <div>Jurisdiction: {inspection?.jurisdiction || "—"}</div>
        <div>Template: {inspection?.template_key || "—"}</div>
        <div>Version: {inspection?.template_version || "—"}</div>
      </div>
    </div>
  );
}

function ChecklistCard({ item }: { item: any }) {
  const status = itemStatus(item);
  const severity = itemSeverity(item);
  const evidence = toArray(
    item?.evidence || item?.latest_evidence || item?.evidence_json,
  );
  const photos = toArray(
    item?.photo_references ||
      item?.latest_photos ||
      item?.photo_references_json,
  );
  const notes =
    item?.notes ||
    item?.suggested_fix ||
    item?.fail_reason ||
    item?.remediation_guidance ||
    item?.evidence ||
    null;

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
          {item?.label ||
            item?.title ||
            item?.description ||
            item?.code ||
            item?.item_code ||
            item?.rule_key ||
            "Checklist item"}
        </div>
        <span className={toneClass(statusTone(status))}>
          {titleCase(status)}
        </span>
        <span className={toneClass(statusTone(severity))}>
          {titleCase(severity)}
        </span>
      </div>

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
                {evidence.slice(0, 3).map((entry: any, idx: number) => (
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
              Photos
            </div>
            {photos.length ? (
              <div className="mt-2 space-y-1 text-sm text-app-3">
                {photos.slice(0, 3).map((entry: any, idx: number) => (
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

export default function InspectionReadiness({
  readiness,
  brief,
  status,
  summary,
  onRunAutomation,
  busy,
}: {
  readiness?: any;
  brief?: any;
  status?: any;
  summary?: any;
  onRunAutomation?: () => void;
  busy?: boolean;
}) {
  const model = readiness || brief || null;
  if (!model) return null;

  const readinessState = model?.readiness || {};
  const counts = model?.counts || {};
  const runSummary = model?.run_summary || summary || {};
  const readinessSummary = model?.readiness_summary || {};
  const completion = readinessSummary?.completion || {};
  const readinessMeta = readinessSummary?.readiness || {};
  const blockingItems = toArray(model?.blocking_items);
  const warningItems = toArray(model?.warning_items);
  const recommendedActions = toArray(model?.recommended_actions);
  const failureActions = toArray(
    model?.inspection_failure_actions?.recommended_actions,
  );
  const checklistItems = toArray(
    model?.results || model?.checklist_items || readinessSummary?.items,
  );
  const inspectionHistory = normalizeInspectionHistory(model);
  const latestInspection =
    model?.latest_inspection || inspectionHistory[0] || status || null;

  const pills = [
    readinessPill("HQS", readinessState?.hqs_ready),
    readinessPill("Local", readinessState?.local_ready),
    readinessPill("Voucher", readinessState?.voucher_ready),
    readinessPill("Lease-up", readinessState?.lease_up_ready),
  ];

  const readinessScore =
    model?.score_pct ?? readinessMeta?.score ?? summary?.score_pct ?? null;
  const completionPct = model?.completion_pct ?? completion?.pct ?? null;
  const projectionPct =
    model?.completion_projection_pct ?? completion?.projection_pct ?? null;
  const failedCount =
    counts?.inspection_failed_items ??
    counts?.failing ??
    counts?.failed_items ??
    summary?.failed ??
    null;
  const blockedCount =
    counts?.inspection_blocked_items ??
    counts?.blocking ??
    counts?.blocked_items ??
    null;
  const criticalCount =
    counts?.inspection_failed_critical_items ??
    counts?.failed_critical_items ??
    counts?.unresolved_critical_count ??
    null;

  const overallStatus =
    model?.overall_status ||
    status?.overall_status ||
    readinessMeta?.status ||
    "unknown";
  const posture = model?.posture || readinessMeta?.posture || "unknown";
  const latestPassed =
    readinessState?.latest_inspection_passed ??
    status?.passed ??
    latestInspection?.passed ??
    null;
  const reinspectRequired =
    readinessState?.reinspect_required ??
    readinessMeta?.reinspect_required ??
    latestInspection?.reinspect_required ??
    false;
  const explanation =
    brief?.explanation || model?.policy_brief?.explanation || null;

  const failedOrBlockedChecklist = checklistItems.filter((item: any) =>
    ["fail", "blocked", "inconclusive"].includes(itemStatus(item)),
  );

  return (
    <Surface
      title="Inspection readiness"
      subtitle="Inspection-driven readiness combines the latest inspection with unresolved failures, blocked items, critical issues, and remediation guidance."
      actions={
        onRunAutomation ? (
          <button
            onClick={onRunAutomation}
            disabled={busy}
            className="oh-btn oh-btn-secondary"
          >
            <PlayCircle className="h-4 w-4" />
            {busy ? "running…" : "run automation"}
          </button>
        ) : null
      }
    >
      <div className="flex flex-wrap gap-2">{pills}</div>

      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-6">
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Readiness score
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {readinessScore != null
              ? `${Number(readinessScore).toFixed(1)}%`
              : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Completion
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {completionPct != null
              ? `${Number(completionPct).toFixed(1)}%`
              : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Projection
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {projectionPct != null
              ? `${Number(projectionPct).toFixed(1)}%`
              : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Failed
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {failedCount ?? "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Blocked
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {blockedCount ?? "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Critical
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {criticalCount ?? "—"}
          </div>
        </div>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Overall status
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            <span className={toneClass(statusTone(overallStatus))}>
              {titleCase(overallStatus) || "—"}
            </span>
            <span className={toneClass(postureTone(posture))}>
              {titleCase(posture) || "—"}
            </span>
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Latest inspection
          </div>
          <div className="mt-2 text-sm text-app-3">
            {latestInspection?.inspection_date || latestInspection?.id ? (
              <>
                <div>
                  {latestInspection?.inspection_date ||
                    `Inspection #${latestInspection?.id}`}
                </div>
                <div className="mt-1 text-xs text-app-4">
                  {latestInspection?.inspector || "Unknown inspector"}
                  {latestInspection?.jurisdiction
                    ? ` · ${latestInspection.jurisdiction}`
                    : ""}
                </div>
              </>
            ) : (
              "No recorded inspection yet."
            )}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Latest result
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            {latestPassed == null ? (
              <span className="oh-pill">—</span>
            ) : latestPassed ? (
              <span className="oh-pill oh-pill-good">Passed</span>
            ) : (
              <span className="oh-pill oh-pill-bad">Failed</span>
            )}
            {latestInspection?.result_status ? (
              <span
                className={toneClass(
                  statusTone(latestInspection.result_status),
                )}
              >
                {titleCase(latestInspection.result_status)}
              </span>
            ) : null}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Reinspection
          </div>
          <div className="mt-2">
            <span
              className={
                reinspectRequired
                  ? "oh-pill oh-pill-bad"
                  : "oh-pill oh-pill-good"
              }
            >
              {reinspectRequired ? "Required" : "Not required"}
            </span>
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Checklist issues
          </div>
          <div className="mt-2 text-sm text-app-3">
            {failedOrBlockedChecklist.length} active failure-like item(s)
          </div>
        </div>
      </div>

      {inspectionHistory.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <BadgeCheck className="h-4 w-4 text-app-4" />
            Inspection history
          </div>
          <div className="mt-3 grid gap-3">
            {inspectionHistory.map((inspection: any, idx: number) => (
              <HistoryCard
                key={`${inspection?.id || inspection?.inspection_date || idx}`}
                inspection={inspection}
                active={idx === 0}
              />
            ))}
          </div>
        </div>
      ) : null}

      {reinspectRequired ? (
        <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-red-200">
            <ShieldAlert className="h-4 w-4" />
            Reinspection workflow still open
          </div>
          <div className="mt-2 text-sm leading-6 text-red-100/90">
            Latest readiness still includes unresolved failures, blocked items,
            or critical issues. Resolve the failure-driven actions below and
            rerun the inspection when remediation is complete.
          </div>
        </div>
      ) : null}

      {explanation ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            {latestPassed ? (
              <ShieldCheck className="h-4 w-4 text-app-4" />
            ) : (
              <TriangleAlert className="h-4 w-4 text-app-4" />
            )}
            Readiness notes
          </div>
          <div className="mt-2 text-sm leading-6 text-app-3">{explanation}</div>
        </div>
      ) : null}

      {blockingItems.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/[0.04] px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-red-200">
            <ShieldAlert className="h-4 w-4" />
            Blocking findings
          </div>
          <div className="mt-3 space-y-3">
            {blockingItems.slice(0, 6).map((item: any, idx: number) => (
              <ActionCard
                key={`${item?.rule_key || item?.label || idx}`}
                item={item}
                tone="bad"
              />
            ))}
          </div>
        </div>
      ) : null}

      {warningItems.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-amber-100">
            <TriangleAlert className="h-4 w-4" />
            Warning items
          </div>
          <div className="mt-3 space-y-3">
            {warningItems.slice(0, 4).map((item: any, idx: number) => (
              <ActionCard
                key={`${item?.rule_key || item?.label || idx}`}
                item={item}
                tone="warn"
              />
            ))}
          </div>
        </div>
      ) : null}

      {checklistItems.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <ClipboardList className="h-4 w-4 text-app-4" />
            Inspection-driven checklist
          </div>
          <div className="mt-3 grid gap-3">
            {checklistItems.slice(0, 8).map((item: any, idx: number) => (
              <ChecklistCard
                key={`${item?.rule_key || item?.code || item?.item_code || idx}`}
                item={item}
              />
            ))}
          </div>
        </div>
      ) : null}

      {recommendedActions.length > 0 || failureActions.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <Wrench className="h-4 w-4 text-app-4" />
            Remediation guidance
          </div>

          <div className="mt-3 space-y-3">
            {recommendedActions.length > 0
              ? recommendedActions.slice(0, 6).map((item: any, idx: number) => (
                  <div
                    key={`${item?.rule_key || item?.label || idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {item?.label ||
                          item?.title ||
                          item?.rule_key ||
                          "Untitled action"}
                      </div>
                      {item?.severity ? (
                        <span className={toneClass(statusTone(item.severity))}>
                          {titleCase(item.severity)}
                        </span>
                      ) : null}
                      {item?.category ? (
                        <span className="oh-pill">
                          {titleCase(item.category)}
                        </span>
                      ) : null}
                    </div>

                    {item?.suggested_fix ? (
                      <div className="mt-2 text-sm leading-6 text-app-3">
                        {item.suggested_fix}
                      </div>
                    ) : null}

                    {item?.evidence ? (
                      <div className="mt-2 text-xs text-app-4">
                        evidence: {item.evidence}
                      </div>
                    ) : null}
                  </div>
                ))
              : failureActions.slice(0, 6).map((item: any, idx: number) => (
                  <div
                    key={`${item?.code || item?.title || idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {item?.title || item?.code || "Untitled action"}
                      </div>
                      {item?.priority ? (
                        <span className={toneClass(statusTone(item.priority))}>
                          {titleCase(item.priority)}
                        </span>
                      ) : null}
                      {item?.category ? (
                        <span className="oh-pill">
                          {titleCase(item.category)}
                        </span>
                      ) : null}
                      {item?.requires_reinspection ? (
                        <span className="oh-pill oh-pill-bad">
                          Reinspection
                        </span>
                      ) : null}
                    </div>

                    {item?.notes ? (
                      <div className="mt-2 text-sm leading-6 text-app-3 whitespace-pre-wrap">
                        {item.notes}
                      </div>
                    ) : null}
                  </div>
                ))}
          </div>
        </div>
      ) : null}

      {!explanation &&
      blockingItems.length === 0 &&
      warningItems.length === 0 &&
      recommendedActions.length === 0 &&
      failureActions.length === 0 &&
      checklistItems.length === 0 &&
      failedCount == null &&
      latestPassed == null ? (
        <div className="mt-4">
          <EmptyState compact title="No detailed readiness notes yet" />
        </div>
      ) : null}

      {failedCount != null || blockedCount != null || criticalCount != null ? (
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <ClipboardX className="h-4 w-4 text-app-4" />
              Failure posture
            </div>
            <div className="mt-2 text-sm text-app-3">
              Failed: {failedCount ?? 0} · Blocked: {blockedCount ?? 0} ·
              Critical: {criticalCount ?? 0}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <AlertTriangle className="h-4 w-4 text-app-4" />
              Result status
            </div>
            <div className="mt-2 text-sm text-app-3">
              {titleCase(
                readinessState?.result_status ||
                  readinessMeta?.result_status ||
                  overallStatus ||
                  "unknown",
              )}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
              <ShieldCheck className="h-4 w-4 text-app-4" />
              Posture
            </div>
            <div className="mt-2 text-sm text-app-3">
              {titleCase(posture || "unknown")}
            </div>
          </div>
        </div>
      ) : null}
    </Surface>
  );
}
