// onehaven_decision_engine/frontend/src/components/AgentRunsPanel.tsx
import React from "react";
import { api } from "../lib/api";

export default function AgentRunsPanel({ propertyId }: { propertyId: number }) {
  const [rows, setRows] = React.useState<any[]>([]);
  const [busyId, setBusyId] = React.useState<number | null>(null);

  async function load() {
    const r = await api.get(
      `/api/agents/runs?property_id=${propertyId}&limit=50`,
    );
    setRows(r?.items || r?.rows || r || []);
  }

  React.useEffect(() => {
    load().catch(() => {});
  }, [propertyId]);

  async function rerun(runId: number) {
    setBusyId(runId);
    try {
      await api.post(`/api/agents/${runId}/retry?dispatch=true`);
      await load();
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-black/30 p-4">
      <div className="text-white/80 font-semibold mb-3">Recent agent runs</div>
      <div className="space-y-2">
        {rows.map((r) => (
          <div
            key={r.id}
            className="rounded-xl border border-white/10 bg-white/5 p-3 flex items-center justify-between"
          >
            <div>
              <div className="text-sm text-white/90">{r.agent_key}</div>
              <div className="text-xs text-white/60">
                status: {r.status} · started: {r.started_at || "—"} · finished:{" "}
                {r.finished_at || "—"}
              </div>
            </div>
            <button
              className="rounded-xl border border-white/10 bg-white/10 px-3 py-2 text-sm text-white/90 hover:bg-white/15 cursor-pointer"
              onClick={() => rerun(Number(r.id))}
              disabled={busyId === Number(r.id)}
            >
              {busyId === Number(r.id) ? "..." : "Rerun"}
            </button>
          </div>
        ))}
        {rows.length === 0 && (
          <div className="text-sm text-white/50">No runs yet.</div>
        )}
      </div>
    </div>
  );
}
