import React from "react";
import { CheckSquare, History, SquareTerminal } from "lucide-react";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";

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
  if (s === "done") return "oh-pill oh-pill-good";
  if (s === "failed" || s === "timed_out") return "oh-pill oh-pill-bad";
  if (s === "blocked") return "oh-pill oh-pill-warn";
  if (s === "running") return "oh-pill oh-pill-accent";
  if (s === "queued") return "oh-pill oh-pill-warn";
  return "oh-pill";
}

function fmtMs(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (v < 1000) return `${v}ms`;
  const sec = v / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${Math.round(sec % 60)}s`;
}

function fmtDate(v?: string | null) {
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

export default function AgentRunHistory({
  rows,
  selectedRunIds,
  onToggleSelect,
  onOpenRun,
  loading,
}: Props) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-app-0">Run history</div>
          <div className="text-xs text-app-4 mt-1">
            Pick up to 4 runs to compare. Click a row to inspect the full trace.
          </div>
        </div>
        <div className="text-xs text-app-4">{rows.length} rows</div>
      </div>

      {loading ? (
        <div className="grid gap-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="oh-skeleton h-[102px] rounded-2xl" />
          ))}
        </div>
      ) : rows.length === 0 ? (
        <EmptyState
          compact
          icon={History}
          title="No runs found"
          description="No agent runs matched the current property and filter selection."
        />
      ) : (
        <div className="space-y-2">
          {rows.map((row) => {
            const selected = selectedRunIds.includes(row.id);

            return (
              <div
                key={row.id}
                className={`rounded-2xl border p-4 transition ${
                  selected
                    ? "border-fuchsia-400/40 bg-fuchsia-400/10"
                    : "border-app bg-app-panel hover:border-app-strong hover:bg-app-muted"
                }`}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <button
                    className="text-left flex-1 min-w-[220px]"
                    onClick={() => onOpenRun(row.id)}
                  >
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-semibold text-app-0">
                        {row.agent_key}
                      </span>
                      <span className={tone(row.status)}>{row.status}</span>
                      {row.runtime_health ? (
                        <span className="oh-pill">{row.runtime_health}</span>
                      ) : null}
                    </div>

                    <div className="mt-2 text-xs text-app-4 flex flex-wrap gap-x-4 gap-y-1">
                      <span>run #{row.id}</span>
                      <span>property #{row.property_id ?? "—"}</span>
                      <span>
                        approval: {row.approval_status || "not_required"}
                      </span>
                      <span>attempts: {row.attempts ?? 0}</span>
                      <span>duration: {fmtMs(row.duration_ms)}</span>
                    </div>

                    <div className="mt-2 text-[11px] text-app-4 flex flex-wrap gap-x-4 gap-y-1">
                      <span>created: {fmtDate(row.created_at)}</span>
                      <span>started: {fmtDate(row.started_at)}</span>
                      <span>finished: {fmtDate(row.finished_at)}</span>
                    </div>

                    {row.last_error ? (
                      <div className="mt-2 text-xs text-red-300/90 line-clamp-2">
                        {row.last_error}
                      </div>
                    ) : null}
                  </button>

                  <div className="flex shrink-0 gap-2">
                    <button
                      className="oh-btn oh-btn-secondary"
                      onClick={() => onOpenRun(row.id)}
                    >
                      <SquareTerminal className="h-4 w-4" />
                      Inspect
                    </button>

                    <button
                      className={`oh-btn ${
                        selected ? "oh-btn-primary" : "oh-btn-secondary"
                      }`}
                      onClick={() => onToggleSelect(row.id)}
                    >
                      <CheckSquare className="h-4 w-4" />
                      {selected ? "Selected" : "Compare"}
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
