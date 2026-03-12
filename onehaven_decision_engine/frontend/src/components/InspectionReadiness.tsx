import React from "react";

function toneClass(kind: "good" | "warn" | "bad" | "neutral" = "neutral") {
  if (kind === "good")
    return "border-green-400/20 bg-green-400/10 text-green-200";
  if (kind === "warn")
    return "border-yellow-300/20 bg-yellow-300/10 text-yellow-100";
  if (kind === "bad") return "border-red-400/20 bg-red-400/10 text-red-200";
  return "border-white/10 bg-white/[0.03] text-white/80";
}

export default function InspectionReadiness({ brief }: { brief: any }) {
  if (!brief) return null;

  const jurisdiction = brief?.jurisdiction || {};
  const coverage = brief?.coverage || {};
  const blockingItems = Array.isArray(brief?.blocking_items)
    ? brief.blocking_items
    : [];
  const requiredActions = Array.isArray(brief?.required_actions)
    ? brief.required_actions
    : [];
  const evidenceLinks = Array.isArray(brief?.evidence_links)
    ? brief.evidence_links
    : [];

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
            Jurisdiction-aware compliance summary
          </div>
        </div>

        <span
          className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(readinessTone as any)}`}
        >
          {blockingItems.length > 0
            ? `${blockingItems.length} blockers`
            : requiredActions.length > 0
              ? `${requiredActions.length} required actions`
              : "ready to inspect"}
        </span>
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
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
            Status: {coverage?.coverage_status || "—"}
          </div>
          <div className="mt-1 text-xs text-white/50">
            Confidence: {coverage?.confidence_label || "—"}
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
                  key={`${item?.code || item?.title || "blocker"}-${idx}`}
                  className="rounded-xl border border-red-400/15 bg-black/20 p-3"
                >
                  <div className="text-sm text-white font-medium">
                    {item?.title ||
                      item?.description ||
                      item?.code ||
                      "Unnamed blocker"}
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {item?.category || "Policy"}
                    {item?.severity != null
                      ? ` · severity ${item.severity}`
                      : ""}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="rounded-2xl border border-yellow-300/15 bg-yellow-300/[0.04] p-4">
          <div className="text-sm font-semibold text-white">
            Required Actions
          </div>
          <div className="mt-3 space-y-2">
            {requiredActions.length === 0 ? (
              <div className="text-sm text-white/55">
                No required actions found.
              </div>
            ) : (
              requiredActions.map((item: any, idx: number) => (
                <div
                  key={`${item?.code || item?.title || "action"}-${idx}`}
                  className="rounded-xl border border-yellow-300/15 bg-black/20 p-3"
                >
                  <div className="text-sm text-white font-medium">
                    {item?.title ||
                      item?.description ||
                      item?.code ||
                      "Unnamed action"}
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {item?.category || "Policy"}
                    {item?.severity != null
                      ? ` · severity ${item.severity}`
                      : ""}
                  </div>
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
            {evidenceLinks.slice(0, 6).map((x: any, idx: number) => {
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
