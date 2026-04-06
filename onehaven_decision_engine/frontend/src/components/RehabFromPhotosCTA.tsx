import React from "react";
import { Camera, ShieldAlert, Sparkles } from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

export default function RehabFromPhotosCTA({
  busy,
  analysis,
  selectedCount = 0,
  onPreview,
  onGenerate,
}: {
  busy?: boolean;
  analysis?: any;
  selectedCount?: number;
  onPreview?: () => void | Promise<void>;
  onGenerate?: () => void | Promise<void>;
}) {
  const findings = Array.isArray(analysis?.issues)
    ? analysis.issues
    : Array.isArray(analysis?.findings)
      ? analysis.findings
      : [];

  return (
    <Surface
      title="Inspection findings from photos"
      subtitle="Run photo analysis first, then confirm the likely HQS and local inspection fail points you actually want converted into tasks."
      actions={
        <div className="flex gap-2">
          <button
            onClick={() => onPreview?.()}
            disabled={busy}
            className="oh-btn oh-btn-secondary"
          >
            {busy ? "working…" : "preview findings"}
          </button>
          <button
            onClick={() => onGenerate?.()}
            disabled={busy || !selectedCount}
            className="oh-btn oh-btn-primary"
          >
            {busy
              ? "working…"
              : `create ${selectedCount || ""} task${selectedCount === 1 ? "" : "s"}`}
          </button>
        </div>
      }
    >
      {!analysis ? (
        <EmptyState
          compact
          icon={Sparkles}
          title="No compliance photo analysis yet"
          description="Upload room or exterior photos, then preview likely fail points before you turn them into actionable inspection tasks."
        />
      ) : (
        <div className="space-y-3">
          <div className="grid gap-3 md:grid-cols-4">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Findings
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {findings.length}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Photos scanned
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {analysis?.photo_count ?? "—"}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Estimated blockers
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {analysis?.estimated_blockers ?? "—"}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Selected for tasks
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {selectedCount}
              </div>
            </div>
          </div>

          {findings.length ? (
            <div className="grid gap-2 md:grid-cols-2">
              {findings.slice(0, 4).map((issue: any, i: number) => (
                <div
                  key={i}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                >
                  <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                    <ShieldAlert className="h-4 w-4 text-app-4" />
                    {issue?.probable_failed_inspection_item ||
                      issue?.title ||
                      `Finding ${i + 1}`}
                  </div>
                  {issue?.observed_issue ? (
                    <div className="mt-2 text-sm text-app-3">
                      {issue.observed_issue}
                    </div>
                  ) : null}
                  <div className="mt-2 text-xs text-app-4">
                    {issue?.severity
                      ? `Severity ${issue.severity}`
                      : "Severity unknown"}
                    {issue?.confidence != null
                      ? ` · confidence ${issue.confidence}`
                      : ""}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState
              compact
              icon={Camera}
              title="No findings returned"
              description="The preview request succeeded but did not return any compliance-specific finding rows."
            />
          )}
        </div>
      )}
    </Surface>
  );
}
