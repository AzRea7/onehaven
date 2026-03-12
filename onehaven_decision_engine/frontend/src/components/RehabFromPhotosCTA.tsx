// frontend/src/components/RehabFromPhotosCTA.tsx
import React from "react";
import type { RehabPhotoAnalysis } from "../lib/api";

type Props = {
  busy: boolean;
  analysis: RehabPhotoAnalysis | null;
  onPreview: () => Promise<void>;
  onGenerate: () => Promise<void>;
};

function severityTone(value: string) {
  const v = (value || "").toLowerCase();
  if (v === "critical") return "text-red-200 bg-red-400/10 border-red-400/20";
  if (v === "high")
    return "text-orange-200 bg-orange-400/10 border-orange-400/20";
  if (v === "medium")
    return "text-yellow-100 bg-yellow-300/10 border-yellow-300/20";
  return "text-white/80 bg-white/5 border-white/10";
}

export default function RehabFromPhotosCTA({
  busy,
  analysis,
  onPreview,
  onGenerate,
}: Props) {
  return (
    <div className="oh-panel p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">
            Rehab from photos
          </div>
          <div className="mt-1 text-xs text-white/55">
            Analyze Zillow interior/exterior photos and generate rehab tasks.
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            className="oh-btn cursor-pointer"
            disabled={busy}
            onClick={onPreview}
          >
            {busy ? "Running..." : "Preview analysis"}
          </button>
          <button
            className="oh-btn oh-btn-primary cursor-pointer"
            disabled={busy}
            onClick={onGenerate}
          >
            Generate rehab tasks
          </button>
        </div>
      </div>

      {analysis ? (
        <div className="mt-4 space-y-4">
          <div className="grid gap-3 sm:grid-cols-4">
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
              <div className="text-[11px] text-white/50">Photos</div>
              <div className="mt-1 text-lg font-semibold text-white">
                {analysis.photo_count}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
              <div className="text-[11px] text-white/50">Interior</div>
              <div className="mt-1 text-lg font-semibold text-white">
                {analysis.summary?.interior ?? 0}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
              <div className="text-[11px] text-white/50">Exterior</div>
              <div className="mt-1 text-lg font-semibold text-white">
                {analysis.summary?.exterior ?? 0}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-3">
              <div className="text-[11px] text-white/50">Issues</div>
              <div className="mt-1 text-lg font-semibold text-white">
                {analysis.issues?.length ?? 0}
              </div>
            </div>
          </div>

          <div className="space-y-3">
            {analysis.issues?.map((issue, idx) => (
              <div
                key={`${issue.title}-${idx}`}
                className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <div className="text-sm font-semibold text-white">
                    {issue.title}
                  </div>
                  <span
                    className={`rounded-full border px-2 py-1 text-[10px] ${severityTone(issue.severity)}`}
                  >
                    {issue.severity}
                  </span>
                  {issue.blocker ? (
                    <span className="rounded-full border border-red-400/20 bg-red-400/10 px-2 py-1 text-[10px] text-red-200">
                      blocker
                    </span>
                  ) : null}
                </div>

                <div className="mt-2 text-xs text-white/55">
                  {issue.category} · est. cost{" "}
                  {issue.estimated_cost != null
                    ? `$${Math.round(issue.estimated_cost).toLocaleString()}`
                    : "—"}
                </div>

                {issue.notes ? (
                  <div className="mt-2 text-sm text-white/75">
                    {issue.notes}
                  </div>
                ) : null}
              </div>
            ))}
          </div>

          {analysis.created != null ? (
            <div className="rounded-2xl border border-green-400/20 bg-green-400/10 p-3 text-sm text-green-100">
              Created {analysis.created} rehab task
              {analysis.created === 1 ? "" : "s"}.
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
