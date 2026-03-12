import React from "react";

type RunRow = {
  id: number;
  property_id?: number | null;
  agent_key: string;
  status: string;
  runtime_health?: string;
  approval_status?: string;
  attempts?: number;
  duration_ms?: number | null;
  created_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  last_error?: string | null;
};

type Props = {
  rows: RunRow[];
  selectedRunIds: number[];
  onToggleSelect: (runId: number) => void;
  onOpenRun: (runId: number) => void;
  loading?: boolean;
};

function tone(status: string) {
  const s = (status || "").toLowerCase();
  if (s === "done")
    return "border-emerald-500/30 bg-emerald-500/10 text-emerald-200";
  if (s === "failed" || s === "timed_out")
    return "border-red-500/30 bg-red-500/10 text-red-200";
  if (s === "blocked")
    return "border-amber-500/30 bg-amber-500/10 text-amber-200";
  if (s === "running") return "border-sky-500/30 bg-sky-500/10 text-sky-200";
  if (s === "queued")
    return "border-yellow-400/30 bg-yellow-400/10 text-yellow-100";
  return "border-white/10 bg-white/5 text-zinc-200";
}

function fmtMs(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (v < 1000) return `${v}ms`;
  const sec = v / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${Math.round(sec % 60)}s`;
}

export default function AgentRunHistory({
  rows,
  selectedRunIds,
  onToggleSelect,
  onOpenRun,
  loading,
}: Props) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-zinc-100">Run history</div>
          <div className="text-xs text-zinc-400 mt-1">
            Pick up to 4 runs to compare. Click a row to inspect the full trace.
          </div>
        </div>
        <div className="text-xs text-zinc-400">{rows.length} rows</div>
      </div>

      <div className="mt-4 space-y-2">
        {loading ? (
          <div className="text-sm text-zinc-400">Loading history…</div>
        ) : rows.length === 0 ? (
          <div className="text-sm text-zinc-400">
            No runs found for the current filters.
          </div>
        ) : (
          rows.map((row) => {
            const selected = selectedRunIds.includes(row.id);
            return (
              <div
                key={row.id}
                className={`rounded-2xl border p-3 transition ${selected ? "border-fuchsia-400/40 bg-fuchsia-400/10" : "border-white/10 bg-black/20 hover:bg-white/5"}`}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <button
                    className="text-left flex-1 min-w-[220px]"
                    onClick={() => onOpenRun(row.id)}
                  >
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-semibold text-zinc-100">
                        {row.agent_key}
                      </span>
                      <span
                        className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] ${tone(row.status)}`}
                      >
                        {row.status}
                      </span>
                      {row.runtime_health ? (
                        <span className="inline-flex rounded-full border border-white/10 px-2 py-0.5 text-[11px] text-zinc-300">
                          {row.runtime_health}
                        </span>
                      ) : null}
                    </div>
                    <div className="mt-2 text-xs text-zinc-400 flex flex-wrap gap-x-4 gap-y-1">
                      <span>run #{row.id}</span>
                      <span>property #{row.property_id ?? "—"}</span>
                      <span>
                        approval: {row.approval_status || "not_required"}
                      </span>
                      <span>attempts: {row.attempts ?? 0}</span>
                      <span>duration: {fmtMs(row.duration_ms)}</span>
                    </div>
                    {row.last_error ? (
                      <div className="mt-2 text-xs text-red-200/90 line-clamp-2">
                        {row.last_error}
                      </div>
                    ) : null}
                  </button>

                  <button
                    className={`rounded-xl border px-3 py-2 text-xs ${selected ? "border-fuchsia-400/40 bg-fuchsia-400/10 text-fuchsia-100" : "border-white/10 bg-white/5 text-zinc-200 hover:bg-white/10"}`}
                    onClick={() => onToggleSelect(row.id)}
                  >
                    {selected ? "selected" : "compare"}
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
