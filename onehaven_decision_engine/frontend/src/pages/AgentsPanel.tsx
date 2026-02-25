// frontend/src/pages/AgentsPanel.tsx
import React from "react";
import { api } from "../lib/api";

type TraceEvent = {
  id: number;
  created_at: string;
  agent_key: string | null;
  event_type: string | null;
  payload: any;
  event: any;
};

function safeJson(v: any, fallback: any) {
  try {
    if (v == null) return fallback;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return fallback;
      return JSON.parse(s);
    }
    return v;
  } catch {
    return fallback;
  }
}

export default function AgentsPanel({ propertyId }: { propertyId: number }) {
  const [runs, setRuns] = React.useState<any[]>([]);
  const [selectedRunId, setSelectedRunId] = React.useState<number | null>(null);
  const [trace, setTrace] = React.useState<TraceEvent[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  async function refreshRuns(keepSelection = true) {
    setErr(null);
    setBusy(true);
    try {
      // ✅ api.agentRunsList now supports both signatures safely
      const out = await api.agentRunsList(propertyId);
      setRuns(out);

      if (!keepSelection) {
        setSelectedRunId(out.length ? out[0].id : null);
        return;
      }

      if (!selectedRunId && out.length) setSelectedRunId(out[0].id);
      if (selectedRunId && !out.some((r: any) => r.id === selectedRunId)) {
        setSelectedRunId(out.length ? out[0].id : null);
      }
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  React.useEffect(() => {
    refreshRuns(false).catch((e) => setErr(String(e?.message || e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId]);

  React.useEffect(() => {
    if (!selectedRunId) return;

    setTrace([]);
    setErr(null);

    // ✅ now exists on api and is typed
    const es = api.agentRunsStream(selectedRunId);

    const onTrace = (ev: any) => {
      try {
        const data = JSON.parse(ev.data);
        setTrace((prev) => {
          const next = [...prev, data];
          return next.slice(-1000);
        });
      } catch {}
    };

    es.addEventListener("trace", onTrace);

    es.addEventListener("error", () => {
      // SSE auto reconnects; keep quiet
    });

    return () => {
      try {
        es.removeEventListener("trace", onTrace as any);
      } catch {}
      es.close();
    };
  }, [selectedRunId]);

  async function approve(runId: number) {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsApprove(runId);
      await refreshRuns(true);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function apply(runId: number) {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsApply(runId);
      await refreshRuns(true);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  const selected = runs.find((r) => r.id === selectedRunId);

  const proposedActions =
    selected?.proposed_actions_json != null
      ? safeJson(selected.proposed_actions_json, selected.proposed_actions_json)
      : [];

  return (
    <div className="grid grid-cols-12 gap-4">
      <div className="col-span-12 md:col-span-4 oh-panel p-4 space-y-2">
        <div className="flex items-center justify-between">
          <div className="font-semibold">Agent Runs</div>
          <button
            className="oh-btn"
            disabled={busy}
            onClick={() => refreshRuns(true)}
          >
            Refresh
          </button>
        </div>

        <div className="space-y-2">
          {runs.map((r) => {
            const active = r.id === selectedRunId;
            return (
              <button
                key={r.id}
                onClick={() => setSelectedRunId(r.id)}
                className={`w-full text-left rounded-xl border px-3 py-3 transition ${
                  active
                    ? "bg-zinc-950/60 border-zinc-700 text-zinc-100"
                    : "bg-zinc-950/20 border-zinc-800 text-zinc-200 hover:bg-zinc-950/40"
                }`}
              >
                <div className="text-sm font-medium">{r.agent_key}</div>
                <div className="text-xs text-zinc-400 mt-1">
                  #{r.id} • {r.status} • {r.approval_status}
                </div>
              </button>
            );
          })}
          {!runs.length ? (
            <div className="text-sm text-zinc-400">No runs yet.</div>
          ) : null}
        </div>

        {err ? <div className="mt-2 text-red-200 text-sm">{err}</div> : null}
      </div>

      <div className="col-span-12 md:col-span-8 oh-panel p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="font-semibold">Run Details</div>
          {selected ? (
            <div className="flex gap-2">
              <button
                className="oh-btn"
                disabled={busy}
                onClick={() => approve(selected.id)}
              >
                Approve
              </button>
              <button
                className="oh-btn-primary"
                disabled={busy}
                onClick={() => apply(selected.id)}
              >
                Apply
              </button>
            </div>
          ) : null}
        </div>

        {selected ? (
          <div className="space-y-3">
            <div className="text-sm text-zinc-300">
              <span className="text-zinc-500">Status:</span> {selected.status} /{" "}
              {selected.approval_status}
            </div>

            {selected.last_error ? (
              <div className="oh-panel-solid p-3 border-red-900/60 bg-red-950/30 text-red-200 text-xs">
                <span className="font-semibold">Error:</span>{" "}
                {selected.last_error}
              </div>
            ) : null}

            <div>
              <div className="text-xs text-zinc-500 mb-2">Proposed Actions</div>
              <pre className="text-xs text-zinc-200 bg-zinc-950/60 border border-zinc-800 rounded-xl p-3 overflow-auto max-h-56">
                {typeof proposedActions === "string"
                  ? proposedActions
                  : JSON.stringify(proposedActions ?? [], null, 2)}
              </pre>
            </div>

            <div>
              <div className="text-xs text-zinc-500 mb-2">
                Trace Timeline (live)
              </div>
              <div className="border border-zinc-800 rounded-xl p-3 max-h-[28rem] overflow-auto space-y-2 bg-zinc-950/30">
                {trace.map((t) => (
                  <div
                    key={t.id}
                    className="text-xs border-b border-zinc-800 pb-2"
                  >
                    <div className="text-zinc-500">
                      #{t.id} • {t.created_at} • {t.agent_key} • {t.event_type}
                    </div>
                    <pre className="whitespace-pre-wrap text-zinc-200">
                      {JSON.stringify(t.payload ?? t.event ?? {}, null, 2)}
                    </pre>
                  </div>
                ))}
                {!trace.length ? (
                  <div className="text-sm text-zinc-500">
                    No trace events yet.
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        ) : (
          <div className="text-sm text-zinc-400">Select a run…</div>
        )}
      </div>
    </div>
  );
}
