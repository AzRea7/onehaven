import React from "react";
import { ArrowRight, CheckCircle2, Lock } from "lucide-react";
import Surface from "./Surface";

const ORDER = [
  "deal",
  "rehab",
  "compliance",
  "tenant",
  "lease",
  "cash_equity",
] as const;

function normalizeStage(stage?: string | null) {
  const x = String(stage || "")
    .trim()
    .toLowerCase();

  if (
    [
      "import",
      "intake",
      "deal",
      "decision",
      "acquisition",
      "procurement",
      "underwriting",
    ].includes(x)
  ) {
    return "deal";
  }

  if (
    [
      "rehab",
      "rehab_plan",
      "rehab_exec",
      "renovation",
      "construction",
    ].includes(x)
  ) {
    return "rehab";
  }

  if (["compliance", "inspection", "licensing"].includes(x)) {
    return "compliance";
  }

  if (["tenant", "voucher"].includes(x)) {
    return "tenant";
  }

  if (["lease", "leasing", "management", "ops"].includes(x)) {
    return "lease";
  }

  if (["cash", "cashflow", "equity", "portfolio", "cash_equity"].includes(x)) {
    return "cash_equity";
  }

  return "deal";
}

function labelize(stage: string) {
  const s = normalizeStage(stage);
  if (s === "deal") return "Deal / Procurement";
  if (s === "rehab") return "Rehab";
  if (s === "compliance") return "Compliance";
  if (s === "tenant") return "Tenant Placement";
  if (s === "lease") return "Lease / Management";
  return "Cashflow / Equity";
}

function stageIndex(stage?: string | null) {
  const idx = ORDER.indexOf(normalizeStage(stage) as (typeof ORDER)[number]);
  return idx >= 0 ? idx : 0;
}

function getStageHelp(stage: string) {
  const s = normalizeStage(stage);

  if (s === "deal") {
    return "Create the deal, confirm underwriting, and decide whether this property deserves more time and money.";
  }
  if (s === "rehab") {
    return "Define scope, generate tasks, estimate cost, and move the property toward physical readiness.";
  }
  if (s === "compliance") {
    return "Complete checklist items, inspections, and jurisdiction requirements before tenant placement.";
  }
  if (s === "tenant") {
    return "Move from compliance-ready unit to approved tenant and placement readiness.";
  }
  if (s === "lease") {
    return "Activate lease execution, lease start, and management handoff.";
  }
  return "Track live cashflow, operating performance, and long-term equity once the property is functioning as an income asset.";
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
  const current = normalizeStage(
    currentStage || workflow?.current_stage || "deal",
  );
  const idx = stageIndex(current);
  const primaryAction = workflow?.primary_action || null;
  const gate = workflow?.transition_gate || null;

  const normalizedCurrentLabel =
    currentStageLabel && String(currentStageLabel).trim()
      ? String(currentStageLabel)
      : labelize(current);

  const nextStageLabel = labelize(
    gate?.allowed_next_stage || gate?.allowed_next_stage_label || "rehab",
  );

  return (
    <Surface
      title="Workflow progress"
      subtitle={`Current stage: ${normalizedCurrentLabel}`}
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
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {ORDER.map((stage, i) => {
          const done = i < idx;
          const active = i === idx;
          const locked = i > idx;

          return (
            <div
              key={stage}
              className={[
                "rounded-2xl border px-4 py-4",
                active
                  ? "border-app-strong bg-app-panel shadow-soft"
                  : done
                    ? "border-app bg-app-muted"
                    : "border-app bg-transparent opacity-80",
              ].join(" ")}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Step {i + 1}
                </div>

                {done ? (
                  <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                ) : locked ? (
                  <Lock className="h-3.5 w-3.5 text-app-4" />
                ) : null}
              </div>

              <div className="mt-2 text-sm font-semibold text-app-0">
                {labelize(stage)}
              </div>

              <div className="mt-2 text-xs leading-relaxed text-app-4">
                {getStageHelp(stage)}
              </div>

              <div className="mt-3 text-xs">
                {active ? (
                  <span className="oh-pill oh-pill-accent">Current</span>
                ) : done ? (
                  <span className="oh-pill oh-pill-good">Completed</span>
                ) : (
                  <span className="oh-pill">Locked</span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-4 rounded-2xl border border-app bg-app-muted p-4">
        <div className="text-xs text-app-4">Transition gate</div>
        <div className="mt-2 text-sm text-app-2">
          {gate
            ? gate?.ok
              ? `Ready to move into ${nextStageLabel}.`
              : gate?.blocked_reason || "Not ready yet."
            : `This property is currently gated at ${labelize(
                current,
              )}. Complete the required work in this stage before moving forward.`}
        </div>
      </div>
    </Surface>
  );
}
