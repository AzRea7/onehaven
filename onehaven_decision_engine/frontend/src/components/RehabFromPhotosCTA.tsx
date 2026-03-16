import React from "react";
import { Sparkles, Wrench } from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

export default function RehabFromPhotosCTA({
  busy,
  analysis,
  onPreview,
  onGenerate,
}: {
  busy?: boolean;
  analysis?: any;
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
      title="Rehab from photos"
      subtitle="Turn photo evidence into structured issues and then into rehab tasks."
      actions={
        <div className="flex gap-2">
          <button
            onClick={() => onPreview?.()}
            disabled={busy}
            className="oh-btn oh-btn-secondary"
          >
            {busy ? "working…" : "preview"}
          </button>
          <button
            onClick={() => onGenerate?.()}
            disabled={busy}
            className="oh-btn oh-btn-primary"
          >
            {busy ? "working…" : "generate tasks"}
          </button>
        </div>
      }
    >
      {!analysis ? (
        <EmptyState
          compact
          icon={Sparkles}
          title="No photo analysis yet"
          description="Preview first to inspect extracted issues. Generate only when the output looks sane and not like a caffeinated raccoon guessed at drywall."
        />
      ) : (
        <div className="space-y-3">
          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Issues
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {findings.length}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Estimated total
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {analysis?.estimated_total_cost != null
                  ? `$${Math.round(Number(analysis.estimated_total_cost)).toLocaleString()}`
                  : "—"}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Status
              </div>
              <div className="mt-2 text-xl font-semibold text-app-0">
                {analysis?.created_count != null
                  ? `${analysis.created_count} task${analysis.created_count === 1 ? "" : "s"}`
                  : "preview"}
              </div>
            </div>
          </div>

          {findings.length ? (
            <div className="space-y-2">
              {findings.slice(0, 8).map((issue: any, i: number) => (
                <div
                  key={i}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                >
                  <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                    <Wrench className="h-4 w-4 text-app-4" />
                    {issue?.title || issue?.issue || `Issue ${i + 1}`}
                  </div>
                  {issue?.notes || issue?.description ? (
                    <div className="mt-2 text-sm text-app-3">
                      {issue?.notes || issue?.description}
                    </div>
                  ) : null}
                  <div className="mt-2 text-xs text-app-4">
                    {issue?.estimated_cost != null
                      ? `Est. $${Math.round(Number(issue.estimated_cost)).toLocaleString()}`
                      : "No cost estimate"}
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
              title="No issues returned"
              description="The analysis payload came back but did not include issue rows."
            />
          )}
        </div>
      )}
    </Surface>
  );
}
