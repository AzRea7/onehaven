import React from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ShieldAlert,
  Sparkles,
  Wrench,
} from "lucide-react";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";

type Finding = {
  code?: string;
  observed_issue?: string;
  probable_failed_inspection_item?: string;
  severity?: string;
  confidence?: number;
  recommended_fix?: string;
  requires_reinspection?: boolean;
  hard_blocker_candidate?: boolean;
  human_review_required?: boolean;
  rule_mapping?: {
    code?: string;
    standard_label?: string;
    standard_citation?: string;
    template_key?: string;
    template_version?: string;
  };
  evidence_photo_ids?: number[];
};

function toneForSeverity(severity?: string | null) {
  const value = String(severity || "").toLowerCase();
  if (value === "critical") return "oh-pill oh-pill-bad";
  if (value === "high" || value === "fail") return "oh-pill oh-pill-warn";
  if (value === "medium" || value === "warn") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

export default function CompliancePhotoFindingsPanel({
  analysis,
  busy,
  selectedCodes,
  markTasksBlocking,
  onSelectedCodesChange,
  onPreview,
  onCreateTasks,
  onMarkBlockingChange,
}: {
  analysis?: any;
  busy?: boolean;
  selectedCodes?: string[];
  markTasksBlocking?: boolean;
  onSelectedCodesChange?: (codes: string[]) => void;
  onPreview?: () => void | Promise<void>;
  onCreateTasks?: () => void | Promise<void>;
  onMarkBlockingChange?: (value: boolean) => void;
}) {
  const findings: Finding[] = Array.isArray(analysis?.findings)
    ? analysis.findings
    : Array.isArray(analysis?.issues)
      ? analysis.issues
      : [];
  const selected = new Set(
    (selectedCodes || []).map((x) => String(x).toUpperCase()),
  );

  function toggleCode(code: string) {
    const upper = String(code).toUpperCase();
    const next = selected.has(upper)
      ? (selectedCodes || []).filter(
          (item) => String(item).toUpperCase() !== upper,
        )
      : [...(selectedCodes || []), upper];
    onSelectedCodesChange?.(next);
  }

  function selectAll() {
    const next = findings.map((finding, idx) =>
      String(
        finding.code || finding.rule_mapping?.code || `FINDING_${idx + 1}`,
      ).toUpperCase(),
    );
    onSelectedCodesChange?.(next);
  }

  function clearAll() {
    onSelectedCodesChange?.([]);
  }

  return (
    <Surface
      title="AI compliance findings"
      subtitle="Review each suggested fail point before it becomes a real task or blocker."
      actions={
        findings.length ? (
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => void onPreview?.()}
              className="oh-btn oh-btn-secondary"
              disabled={busy}
            >
              {busy ? "Working..." : "Preview"}
            </button>
            <button
              onClick={selectAll}
              className="oh-btn oh-btn-secondary"
              disabled={busy}
            >
              Select all
            </button>
            <button
              onClick={clearAll}
              className="oh-btn oh-btn-secondary"
              disabled={busy}
            >
              Clear
            </button>
            <button
              onClick={() => void onCreateTasks?.()}
              className="oh-btn"
              disabled={busy || !selected.size}
            >
              {busy
                ? "Working..."
                : `Create ${selected.size} task${selected.size === 1 ? "" : "s"}`}
            </button>
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => void onPreview?.()}
              className="oh-btn oh-btn-secondary"
              disabled={busy}
            >
              {busy ? "Working..." : "Preview"}
            </button>
          </div>
        )
      }
    >
      {!analysis ? (
        <EmptyState
          compact
          icon={Sparkles}
          title="No findings loaded"
          description="Run a preview to see likely inspection failures mapped from the current photo set."
        />
      ) : !findings.length ? (
        <EmptyState
          compact
          icon={CheckCircle2}
          title="No findings returned"
          description="The analysis completed but did not return any reviewable photo findings."
        />
      ) : (
        <div className="space-y-4">
          <label className="inline-flex items-center gap-2 text-sm text-app-2">
            <input
              type="checkbox"
              checked={Boolean(markTasksBlocking)}
              onChange={(e) => onMarkBlockingChange?.(e.target.checked)}
              className="h-4 w-4 rounded border-app bg-app-panel"
            />
            Mark critical confirmed findings as blocking tasks
          </label>

          <div className="space-y-3">
            {findings.map((finding, idx) => {
              const code = String(
                finding.code ||
                  finding.rule_mapping?.code ||
                  `FINDING_${idx + 1}`,
              ).toUpperCase();
              const checked = selected.has(code);

              return (
                <div
                  key={`${code}-${idx}`}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <label className="flex min-w-0 flex-1 items-start gap-3">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleCode(code)}
                        className="mt-1 h-4 w-4 rounded border-app bg-app-panel"
                      />
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-sm font-semibold text-app-0">
                            {finding.probable_failed_inspection_item ||
                              finding.observed_issue ||
                              `Finding ${idx + 1}`}
                          </div>
                          <span className={toneForSeverity(finding.severity)}>
                            {finding.severity || "unknown"}
                          </span>
                          {finding.requires_reinspection ? (
                            <span className="oh-pill oh-pill-bad">
                              Reinspect likely
                            </span>
                          ) : null}
                          {finding.hard_blocker_candidate ? (
                            <span className="oh-pill oh-pill-warn">
                              Critical candidate
                            </span>
                          ) : null}
                        </div>

                        {finding.observed_issue ? (
                          <div className="mt-2 text-sm text-app-3">
                            Observed issue: {finding.observed_issue}
                          </div>
                        ) : null}

                        {finding.recommended_fix ? (
                          <div className="mt-2 flex items-start gap-2 text-sm text-app-2">
                            <Wrench className="mt-0.5 h-4 w-4 text-app-4" />
                            <span>{finding.recommended_fix}</span>
                          </div>
                        ) : null}

                        <div className="mt-3 flex flex-wrap gap-2 text-xs text-app-4">
                          {finding.confidence != null ? (
                            <span className="oh-pill">
                              Confidence {finding.confidence}
                            </span>
                          ) : null}
                          {finding.rule_mapping?.standard_label ? (
                            <span className="oh-pill">
                              {finding.rule_mapping.standard_label}
                            </span>
                          ) : null}
                          {finding.rule_mapping?.template_key ? (
                            <span className="oh-pill">
                              {finding.rule_mapping.template_key}
                              {finding.rule_mapping.template_version
                                ? ` · ${finding.rule_mapping.template_version}`
                                : ""}
                            </span>
                          ) : null}
                          {Array.isArray(finding.evidence_photo_ids) &&
                          finding.evidence_photo_ids.length ? (
                            <span className="oh-pill">
                              Photo refs {finding.evidence_photo_ids.join(", ")}
                            </span>
                          ) : null}
                        </div>

                        {finding.rule_mapping?.standard_citation ? (
                          <div className="mt-2 text-xs text-app-4">
                            Citation: {finding.rule_mapping.standard_citation}
                          </div>
                        ) : null}
                      </div>
                    </label>

                    <div className="flex items-center gap-2 text-xs text-app-4">
                      {finding.human_review_required ? (
                        <span className="inline-flex items-center gap-1">
                          <ShieldAlert className="h-3.5 w-3.5" />
                          Review required
                        </span>
                      ) : null}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          {!selected.size ? (
            <div className="flex items-start gap-2 rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-3 text-sm text-amber-50/90">
              <AlertTriangle className="mt-0.5 h-4 w-4 text-amber-200" />
              Select at least one reviewed finding before creating tasks.
            </div>
          ) : null}
        </div>
      )}
    </Surface>
  );
}
