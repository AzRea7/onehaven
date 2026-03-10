import React from "react";

type StageRow = {
  key: string;
  label: string;
  description?: string;
  status?: string;
};

const DEFAULT_STAGES: StageRow[] = [
  { key: "import", label: "Import" },
  { key: "deal", label: "Deal" },
  { key: "decision", label: "Decision" },
  { key: "acquisition", label: "Acquisition" },
  { key: "rehab_plan", label: "Rehab Plan" },
  { key: "rehab_exec", label: "Rehab Exec" },
  { key: "compliance", label: "Compliance" },
  { key: "tenant", label: "Tenant" },
  { key: "lease", label: "Lease" },
  { key: "cash", label: "Cash" },
  { key: "equity", label: "Equity" },
];

function stageIndex(stage: string | null | undefined) {
  const key = String(stage || "").toLowerCase();
  const idx = DEFAULT_STAGES.findIndex((s) => s.key === key);
  return idx >= 0 ? idx : 0;
}

export default function StageProgress({
  workflow,
  currentStage,
  currentStageLabel,
  onAdvance,
  busy,
}: {
  workflow?: any;
  currentStage?: string | null;
  currentStageLabel?: string | null;
  onAdvance?: (() => void | Promise<void>) | null;
  busy?: boolean;
}) {
  const wfStages: StageRow[] = Array.isArray(workflow?.stages)
    ? workflow.stages
    : [];

  const stages = wfStages.length
    ? wfStages
    : DEFAULT_STAGES.map((s, idx) => {
        const activeIdx = stageIndex(currentStage);
        return {
          ...s,
          status:
            idx < activeIdx
              ? "completed"
              : idx === activeIdx
                ? "current"
                : idx === activeIdx + 1
                  ? "next"
                  : "locked",
        };
      });

  const activeIdx = Math.max(
    0,
    stages.findIndex((s) => s.status === "current"),
  );
  const pct =
    stages.length <= 1
      ? 0
      : Math.round((activeIdx / (stages.length - 1)) * 100);

  const primaryAction = workflow?.primary_action;
  const gate = workflow?.gate || {};

  return (
    <div className="oh-panel p-5">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <div className="text-sm font-semibold text-white">
            Workflow Progress
          </div>
          <div className="mt-1 text-xs text-white/55">
            Current stage:{" "}
            {currentStageLabel || workflow?.current_stage_label || "—"}
          </div>
        </div>

        {primaryAction?.kind === "advance" && onAdvance ? (
          <button
            onClick={() => onAdvance()}
            disabled={!!busy}
            className="oh-btn oh-btn-primary cursor-pointer"
          >
            {busy ? "advancing…" : primaryAction?.title || "advance"}
          </button>
        ) : null}
      </div>

      <div className="mt-4">
        <div className="h-2 rounded-full bg-white/10 overflow-hidden">
          <div className="h-2 bg-white/70" style={{ width: `${pct}%` }} />
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-3 xl:grid-cols-6 gap-2">
        {stages.map((s) => {
          const status = String(s.status || "").toLowerCase();
          const cls =
            status === "completed"
              ? "border-green-400/20 bg-green-400/8"
              : status === "current"
                ? "border-white/25 bg-white/10"
                : status === "next"
                  ? "border-yellow-300/20 bg-yellow-300/8"
                  : "border-white/10 bg-white/[0.03]";

          return (
            <div
              key={s.key}
              className={`rounded-2xl border p-3 ${cls}`}
              style={{ contain: "layout paint" }}
            >
              <div className="text-[11px] uppercase tracking-wider text-white/45">
                {status || "locked"}
              </div>
              <div className="mt-1 text-sm font-semibold text-white">
                {s.label}
              </div>
              {s.description ? (
                <div className="mt-1 text-xs text-white/55 leading-relaxed">
                  {s.description}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>

      <div className="mt-4 grid grid-cols-1 lg:grid-cols-[1.1fr_.9fr] gap-3">
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-xs text-white/45">Primary next move</div>
          <div className="mt-2 text-sm font-semibold text-white">
            {primaryAction?.title || "No action required"}
          </div>
          <div className="mt-2 text-xs text-white/55">
            {workflow?.next_stage_label
              ? `Next stage: ${workflow.next_stage_label}`
              : "No further stage information yet."}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-xs text-white/45">Transition gate</div>
          <div className="mt-2 text-sm text-white/80">
            {gate?.ok
              ? `Ready to move into ${gate?.allowed_next_stage_label || gate?.allowed_next_stage || "next stage"}.`
              : gate?.blocked_reason || "Not ready yet."}
          </div>
        </div>
      </div>
    </div>
  );
}
