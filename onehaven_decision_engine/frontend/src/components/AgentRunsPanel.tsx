import React from "react";
import { Bot, RefreshCw, RotateCcw } from "lucide-react";
import { api } from "../lib/api";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

function fmtDate(v?: string | null) {
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

function statusTone(status: string) {
  const s = (status || "").toLowerCase();
  if (s === "done") return "oh-pill oh-pill-good";
  if (s === "failed" || s === "timed_out") return "oh-pill oh-pill-bad";
  if (s === "blocked") return "oh-pill oh-pill-warn";
  if (s === "running") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

export default function AgentRunsPanel({ propertyId }: { propertyId: number }) {
  const [rows, setRows] = React.useState<any[]>([]);
  const [busyId, setBusyId] = React.useState<number | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  async function load() {
    setLoading(true);
    setErr(null);
    try {
      const r = await api.get(
        `/api/agents/runs?property_id=${propertyId}&limit=50`,
      );
      setRows(r?.items || r?.rows || r || []);
    } catch (e: any) {
      setErr(String(e?.message || e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load().catch(() => {});
  }, [propertyId]);

  async function rerun(runId: number) {
    setBusyId(runId);
    setErr(null);
    try {
      await api.post(`/api/agents/${runId}/retry?dispatch=true`);
      await load();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusyId(null);
    }
  }

  return (
    <Surface
      title="Recent agent runs"
      subtitle={`Latest runs for property #${propertyId}. Retry, inspect status, and watch the little automation goblins work.`}
      actions={
        <button
          className="oh-btn oh-btn-secondary"
          onClick={() => load()}
          disabled={loading}
        >
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          {loading ? "Refreshing…" : "Refresh"}
        </button>
      }
    >
      {err ? (
        <div className="mb-4 rounded-2xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-300">
          {err}
        </div>
      ) : null}

      {loading && rows.length === 0 ? (
        <div className="grid gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="oh-skeleton h-[88px] rounded-2xl" />
          ))}
        </div>
      ) : rows.length === 0 ? (
        <EmptyState
          compact
          icon={Bot}
          title="No runs yet"
          description="This property has not produced any recent agent runs."
        />
      ) : (
        <div className="space-y-2">
          {rows.map((r) => (
            <div
              key={r.id}
              className="rounded-2xl border border-app bg-app-panel p-4"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <div className="text-sm font-semibold text-app-0">
                      {r.agent_key}
                    </div>
                    <span className={statusTone(r.status)}>{r.status}</span>
                  </div>

                  <div className="mt-2 text-xs text-app-4 flex flex-wrap gap-x-4 gap-y-1">
                    <span>started: {fmtDate(r.started_at)}</span>
                    <span>finished: {fmtDate(r.finished_at)}</span>
                  </div>
                </div>

                <button
                  className="oh-btn oh-btn-secondary"
                  onClick={() => rerun(Number(r.id))}
                  disabled={busyId === Number(r.id)}
                >
                  <RotateCcw className="h-4 w-4" />
                  {busyId === Number(r.id) ? "Working…" : "Rerun"}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </Surface>
  );
}
