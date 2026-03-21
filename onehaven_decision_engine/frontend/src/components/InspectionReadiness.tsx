import React from "react";
import {
  ShieldCheck,
  TriangleAlert,
  PlayCircle,
  ClipboardX,
  ShieldAlert,
  Wrench,
  BadgeCheck,
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
  if (["critical_failures", "needs_remediation", "not_ready"].includes(p)) {
    return "bad";
  }
  if (["in_progress", "unknown"].includes(p)) return "warn";
  return "neutral";
}

function statusTone(status?: string) {
  const s = String(status || "").toLowerCase();
  if (["pass", "ready"].includes(s)) return "good";
  if (["fail", "blocked", "critical", "needs_work"].includes(s)) return "bad";
  if (["attention", "unknown", "pending"].includes(s)) return "warn";
  return "neutral";
}

function titleCase(v: any) {
  return String(v || "")
    .replace(/_/g, " ")
    .trim();
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
  const blockingItems = Array.isArray(model?.blocking_items)
    ? model.blocking_items
    : [];
  const warningItems = Array.isArray(model?.warning_items)
    ? model.warning_items
    : [];
  const recommendedActions = Array.isArray(model?.recommended_actions)
    ? model.recommended_actions
    : [];
  const failureActions = Array.isArray(
    model?.inspection_failure_actions?.recommended_actions,
  )
    ? model.inspection_failure_actions.recommended_actions
    : [];

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
    counts?.failing ??
    counts?.inspection_failed_items ??
    summary?.failed ??
    null;

  const blockedCount =
    counts?.blocking ?? counts?.inspection_blocked_items ?? null;

  const overallStatus =
    model?.overall_status ||
    status?.overall_status ||
    readinessMeta?.status ||
    "unknown";

  const posture = model?.posture || readinessMeta?.posture || "unknown";

  const latestPassed =
    readinessState?.latest_inspection_passed ?? status?.passed ?? null;

  const explanation =
    brief?.explanation || model?.policy_brief?.explanation || null;

  return (
    <Surface
      title="Inspection readiness"
      subtitle="Inspection-grade readiness using actual pass/fail posture, blockers, and remediation—not just optimistic checklist vibes."
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

      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
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
            Failures / blockers
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {failedCount != null || blockedCount != null
              ? `${failedCount ?? 0} / ${blockedCount ?? 0}`
              : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
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
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <BadgeCheck className="h-4 w-4 text-app-4" />
            Latest inspection
          </div>
          <div className="mt-2 text-sm text-app-3">
            {latestPassed == null
              ? "No recorded result yet."
              : latestPassed
                ? "Passed"
                : "Did not pass"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <ClipboardX className="h-4 w-4 text-app-4" />
            Blocking items
          </div>
          <div className="mt-2 text-sm text-app-3">
            {blockingItems.length || counts?.blocking || 0}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <Wrench className="h-4 w-4 text-app-4" />
            Remediation actions
          </div>
          <div className="mt-2 text-sm text-app-3">
            {recommendedActions.length || failureActions.length || 0}
          </div>
        </div>
      </div>

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

      {recommendedActions.length > 0 || failureActions.length > 0 ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <Wrench className="h-4 w-4 text-app-4" />
            Remediation guidance
          </div>

          <div className="mt-3 space-y-3">
            {recommendedActions.slice(0, 6).map((item: any, idx: number) => (
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
                    <span className="oh-pill">{titleCase(item.category)}</span>
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
            ))}

            {recommendedActions.length === 0 &&
              failureActions.slice(0, 6).map((item: any, idx: number) => (
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
                  </div>

                  {item?.notes ? (
                    <div className="mt-2 text-sm leading-6 text-app-3">
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
      failedCount == null &&
      latestPassed == null ? (
        <div className="mt-4">
          <EmptyState compact title="No detailed readiness notes yet" />
        </div>
      ) : null}
    </Surface>
  );
}
