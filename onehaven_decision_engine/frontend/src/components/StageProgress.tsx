import React from "react";
import { ArrowRight, Lock } from "lucide-react";
import Surface from "./Surface";

const ORDER = [
  "import",
  "deal",
  "decision",
  "acquisition",
  "rehab_plan",
  "rehab_exec",
  "compliance",
  "tenant",
  "lease",
  "cash",
  "equity",
];

function labelize(stage: string) {
  return stage.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function stageIndex(stage?: string | null) {
  const idx = ORDER.indexOf(String(stage || "").toLowerCase());
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
  onAdvance?: () => void | Promise<void>;
  busy?: boolean;
}) {
  const current = String(
    currentStage || workflow?.current_stage || "deal",
  ).toLowerCase();
  const idx = stageIndex(current);
  const primaryAction = workflow?.primary_action || null;
  const gate = workflow?.transition_gate || null;

  return (
    <Surface
      title="Workflow progress"
      subtitle={`Current stage: ${currentStageLabel || labelize(current)}`}
      actions={
        primaryAction?.kind === "advance" && onAdvance ? (
          <button
            onClick={onAdvance}
            disabled={busy}
            className="oh-btn oh-btn-primary"
          >
            {busy ? "advancing…" : "advance"}
            <ArrowRight className="h-4 w-4" />
          </button>
        ) : null
      }
    >
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {ORDER.map((stage, i) => {
          const done = i < idx;
          const active = i === idx;
          const locked = i > idx;

          return (
            <div
              key={stage}
              className={[
                "rounded-2xl border px-4 py-3",
                active
                  ? "border-app-strong bg-app-panel"
                  : done
                    ? "border-app bg-app-muted"
                    : "border-app bg-transparent opacity-75",
              ].join(" ")}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  {i + 1}
                </div>
                {locked ? <Lock className="h-3.5 w-3.5 text-app-4" /> : null}
              </div>
              <div className="mt-2 text-sm font-semibold text-app-0">
                {labelize(stage)}
              </div>
              <div className="mt-1 text-xs text-app-4">
                {active ? "Current" : done ? "Completed / passed" : "Locked"}
              </div>
            </div>
          );
        })}
      </div>

      {gate ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted p-4">
          <div className="text-xs text-app-4">Transition gate</div>
          <div className="mt-2 text-sm text-app-2">
            {gate?.ok
              ? `Ready to move into ${gate?.allowed_next_stage_label || gate?.allowed_next_stage || "next stage"}.`
              : gate?.blocked_reason || "Not ready yet."}
          </div>
        </div>
      ) : null}
    </Surface>
  );
}
