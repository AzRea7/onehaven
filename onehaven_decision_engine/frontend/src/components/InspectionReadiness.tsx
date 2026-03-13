import React from "react";

type Readiness = {
  hqs_ready?: boolean;
  local_ready?: boolean;
  voucher_ready?: boolean;
  lease_up_ready?: boolean;
};

function toneClass(kind: "good" | "warn" | "bad" | "neutral" = "neutral") {
  if (kind === "good")
    return "border-green-400/20 bg-green-400/10 text-green-200";
  if (kind === "warn")
    return "border-yellow-300/20 bg-yellow-300/10 text-yellow-100";
  if (kind === "bad") return "border-red-400/20 bg-red-400/10 text-red-200";
  return "border-white/10 bg-white/[0.03] text-white/80";
}

function readinessPill(label: string, ok: boolean | undefined) {
  const tone = ok === true ? "good" : ok === false ? "bad" : "neutral";
  return (
    <span
      key={label}
      className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(tone)}`}
    >
      {label}: {ok === true ? "ready" : ok === false ? "blocked" : "—"}
    </span>
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

  const jurisdiction = model?.jurisdiction || brief?.jurisdiction || {};
  const coverage = model?.coverage || brief?.coverage || {};
  const blockingItems = Array.isArray(model?.blocking_items)
    ? model.blocking_items
    : Array.isArray(brief?.blocking_items)
      ? brief.blocking_items
      : [];
  const requiredActions = Array.isArray(model?.recommended_actions)
    ? model.recommended_actions
    : Array.isArray(model?.required_actions)
      ? model.required_actions
      : Array.isArray(brief?.required_actions)
        ? brief.required_actions
        : [];
  const evidenceLinks = Array.isArray(model?.policy_brief?.evidence_links)
    ? model.policy_brief.evidence_links
    : Array.isArray(brief?.evidence_links)
      ? brief.evidence_links
      : [];

  const readinessFlags: Readiness = model?.readiness || status?.readiness || {};

  const scorePct =
    model?.score_pct ?? summary?.score_pct ?? status?.score_pct ?? null;

  const overallStatus =
    model?.overall_status || status?.overall_status || "unknown";

  const readinessTone =
    blockingItems.length > 0
      ? "bad"
      : requiredActions.length > 0
        ? "warn"
        : "good";

  return (
    <div className="oh-panel p-5">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <div className="text-sm font-semibold text-white">
            Inspection Readiness
          </div>
          <div className="text-xs text-white/50 mt-1">
            Policy-driven HQS + local compliance automation
          </div>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <span
            className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(readinessTone as any)}`}
          >
            {blockingItems.length > 0
              ? `${blockingItems.length} blockers`
              : requiredActions.length > 0
                ? `${requiredActions.length} actions`
                : "ready to inspect"}
          </span>

          {scorePct != null ? (
            <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/[0.03] text-white/80">
              score: {scorePct}%
            </span>
          ) : null}

          <span
            className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(
              overallStatus === "ready"
                ? "good"
                : overallStatus === "attention"
                  ? "warn"
                  : overallStatus === "blocked"
                    ? "bad"
                    : "neutral",
            )}`}
          >
            {overallStatus}
          </span>

          {onRunAutomation ? (
            <button
              onClick={onRunAutomation}
              disabled={!!busy}
              className="oh-btn oh-btn-primary cursor-pointer"
            >
              {busy ? "running…" : "run automation"}
            </button>
          ) : null}
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        {readinessPill("HQS", readinessFlags?.hqs_ready)}
        {readinessPill("Local", readinessFlags?.local_ready)}
        {readinessPill("Voucher", readinessFlags?.voucher_ready)}
        {readinessPill("Lease-up", readinessFlags?.lease_up_ready)}
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-4 gap-3">
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-[11px] uppercase tracking-wider text-white/45">
            Match
          </div>
          <div className="mt-2 text-sm text-white/85">
            {jurisdiction?.match_level || "—"}
          </div>
          <div className="mt-1 text-xs text-white/50">
            Scope: {jurisdiction?.scope || "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-[11px] uppercase tracking-wider text-white/45">
            Coverage
          </div>
          <div className="mt-2 text-sm text-white/85">
            {coverage?.coverage_status || "—"}
          </div>
          <div className="mt-1 text-xs text-white/50">
            Confidence: {coverage?.confidence_label || "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-[11px] uppercase tracking-wider text-white/45">
            Friction
          </div>
          <div className="mt-2 text-sm text-white/85">
            x{jurisdiction?.friction_multiplier ?? "1.0"}
          </div>
          <div className="mt-1 text-xs text-white/50">
            Profile: {jurisdiction?.profile_id ?? "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-[11px] uppercase tracking-wider text-white/45">
            Rules
          </div>
          <div className="mt-2 text-sm text-white/85">
            Blockers: {blockingItems.length}
          </div>
          <div className="mt-1 text-xs text-white/50">
            Actions: {requiredActions.length}
          </div>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 xl:grid-cols-2 gap-4">
        <div className="rounded-2xl border border-red-400/15 bg-red-400/[0.04] p-4">
          <div className="text-sm font-semibold text-white">Blocking Items</div>
          <div className="mt-3 space-y-2">
            {blockingItems.length === 0 ? (
              <div className="text-sm text-white/55">
                No blocking items found.
              </div>
            ) : (
              blockingItems.map((item: any, idx: number) => (
                <div
                  key={`${item?.rule_key || item?.code || item?.title || "blocker"}-${idx}`}
                  className="rounded-xl border border-red-400/15 bg-black/20 p-3"
                >
                  <div className="text-sm text-white font-medium">
                    {item?.label ||
                      item?.title ||
                      item?.description ||
                      item?.code ||
                      "Unnamed blocker"}
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {item?.category || "Policy"}
                    {item?.severity != null ? ` · ${item.severity}` : ""}
                  </div>
                  {item?.suggested_fix ? (
                    <div className="text-xs text-white/70 mt-2">
                      Fix: {item.suggested_fix}
                    </div>
                  ) : null}
                </div>
              ))
            )}
          </div>
        </div>

        <div className="rounded-2xl border border-yellow-300/15 bg-yellow-300/[0.04] p-4">
          <div className="text-sm font-semibold text-white">
            Recommended Actions
          </div>
          <div className="mt-3 space-y-2">
            {requiredActions.length === 0 ? (
              <div className="text-sm text-white/55">
                No required actions found.
              </div>
            ) : (
              requiredActions.map((item: any, idx: number) => (
                <div
                  key={`${item?.rule_key || item?.code || item?.title || "action"}-${idx}`}
                  className="rounded-xl border border-yellow-300/15 bg-black/20 p-3"
                >
                  <div className="text-sm text-white font-medium">
                    {item?.label ||
                      item?.title ||
                      item?.description ||
                      item?.code ||
                      "Unnamed action"}
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {item?.category || "Policy"}
                    {item?.severity != null ? ` · ${item.severity}` : ""}
                  </div>
                  {item?.suggested_fix ? (
                    <div className="text-xs text-white/70 mt-2">
                      Fix: {item.suggested_fix}
                    </div>
                  ) : null}
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {evidenceLinks.length > 0 && (
        <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-sm font-semibold text-white">Evidence Links</div>
          <div className="mt-3 space-y-2">
            {evidenceLinks.slice(0, 8).map((x: any, idx: number) => {
              const href = x?.url || x?.href;
              const label =
                x?.title || x?.label || href || `Evidence ${idx + 1}`;
              if (!href) return null;
              return (
                <a
                  key={`${href}-${idx}`}
                  href={href}
                  target="_blank"
                  rel="noreferrer"
                  className="block text-sm underline text-white/80"
                >
                  {label}
                </a>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
