// frontend/src/pages/Agents.tsx
import React from "react";
import { api } from "../lib/api";

function pretty(v: any) {
  try {
    if (v == null) return "";
    if (typeof v === "string") {
      // output_json / proposed_actions_json are often JSON strings
      const parsed = JSON.parse(v);
      return JSON.stringify(parsed, null, 2);
    }
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v ?? "");
  }
}

export default function Agents() {
  const [agents, setAgents] = React.useState<any[]>([]);
  const [runs, setRuns] = React.useState<any[]>([]);
  const [plan, setPlan] = React.useState<any[]>([]);
  const [propertyId, setPropertyId] = React.useState<number>(1);

  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  async function loadAgents() {
    setErr(null);
    const a = await api.agents();
    setAgents(Array.isArray(a) ? a : []);
  }

  async function loadRuns(pid = propertyId) {
    setErr(null);
    const r = await api.agentRunsList(pid);
    setRuns(Array.isArray(r) ? r : []);
  }

  async function doPlan() {
    setBusy(true);
    setErr(null);
    try {
      const p = await api.agentRunsPlan(propertyId);
      setPlan(Array.isArray(p) ? p : []);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doEnqueue() {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsEnqueue(propertyId, true);
      await loadRuns(propertyId);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doApprove(runId: number) {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsApprove(runId);
      await loadRuns(propertyId);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doReject(runId: number) {
    const reason =
      prompt("Reject reason?", "rejected_by_owner") || "rejected_by_owner";
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsReject(runId, reason);
      await loadRuns(propertyId);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doApply(runId: number) {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsApply(runId);
      await loadRuns(propertyId);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  React.useEffect(() => {
    (async () => {
      try {
        setBusy(true);
        await loadAgents();
        await loadRuns(propertyId);
      } catch (e: any) {
        setErr(String(e.message || e));
      } finally {
        setBusy(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    // refresh runs when property changes
    loadRuns(propertyId).catch((e: any) => setErr(String(e.message || e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <div className="text-2xl font-semibold tracking-tight">Agents</div>
          <div className="text-sm text-zinc-400 mt-1">
            Workflow slots (humans now, automation later). Each agent is a
            contract: inputs → outputs → audit trail.
          </div>
        </div>
      </div>

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {err}
        </div>
      )}

      {/* Run Console */}
      <div className="oh-panel p-5">
        <div className="flex flex-wrap items-center gap-3">
          <div>
            <div className="text-lg font-semibold tracking-tight">
              Run Console
            </div>
            <div className="text-sm text-zinc-400 mt-1">
              Plan → enqueue → review output → approve/reject → apply
              (owner-only).
            </div>
          </div>

          <div className="ml-auto flex flex-wrap items-center gap-2">
            <div className="text-xs text-zinc-500">Property ID</div>
            <input
              className="w-24 px-3 py-2 rounded-xl bg-zinc-950/60 border border-zinc-800 text-sm text-zinc-200"
              value={propertyId}
              onChange={(e) =>
                setPropertyId(parseInt(e.target.value || "0", 10))
              }
            />
            <button
              className="oh-btn"
              onClick={() => loadRuns(propertyId)}
              disabled={busy}
              title="Refresh run list"
            >
              Refresh
            </button>
            <button className="oh-btn" onClick={doPlan} disabled={busy}>
              Plan
            </button>
            <button
              className="oh-btn-primary"
              onClick={doEnqueue}
              disabled={busy}
            >
              Enqueue
            </button>
          </div>
        </div>

        {plan.length > 0 && (
          <div className="mt-4">
            <div className="text-xs text-zinc-500 mb-2">Planned runs</div>
            <pre className="text-xs text-zinc-200 bg-zinc-950/60 border border-zinc-800 rounded-xl p-3 overflow-auto">
              {pretty(plan)}
            </pre>
          </div>
        )}

        <div className="mt-5 grid grid-cols-1 gap-3">
          {runs.map((r) => (
            <div key={r.id} className="oh-panel p-5">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-lg font-semibold tracking-tight">
                    {r.agent_key}
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    run: <span className="oh-kbd">{r.id}</span> status:{" "}
                    <span className="oh-kbd">{r.status}</span>{" "}
                    {r.approval_status ? (
                      <>
                        approval:{" "}
                        <span className="oh-kbd">{r.approval_status}</span>
                      </>
                    ) : null}
                  </div>
                </div>
                <span className="oh-badge">run</span>
              </div>

              {r.last_error ? (
                <div className="mt-3 oh-panel-solid p-3 border-red-900/60 bg-red-950/30 text-red-200 text-xs">
                  {r.last_error}
                </div>
              ) : null}

              <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <div className="text-xs text-zinc-500 mb-2">Output</div>
                  <pre className="text-xs text-zinc-200 bg-zinc-950/60 border border-zinc-800 rounded-xl p-3 overflow-auto">
                    {pretty(r.output_json)}
                  </pre>
                </div>
                <div>
                  <div className="text-xs text-zinc-500 mb-2">
                    Proposed Actions
                  </div>
                  <pre className="text-xs text-zinc-200 bg-zinc-950/60 border border-zinc-800 rounded-xl p-3 overflow-auto">
                    {pretty(r.proposed_actions_json)}
                  </pre>
                </div>
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                <button
                  className="oh-btn"
                  onClick={() => doApprove(r.id)}
                  disabled={busy}
                >
                  Approve
                </button>
                <button
                  className="oh-btn"
                  onClick={() => doReject(r.id)}
                  disabled={busy}
                >
                  Reject
                </button>
                <button
                  className="oh-btn-primary"
                  onClick={() => doApply(r.id)}
                  disabled={busy}
                >
                  Apply
                </button>
              </div>
            </div>
          ))}

          {!runs.length ? (
            <div className="text-sm text-zinc-400">
              No runs yet for this property.
            </div>
          ) : null}
        </div>
      </div>

      {/* Agent Specs */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {agents.map((a) => (
          <div key={a.agent_key ?? a.key} className="oh-panel p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold tracking-tight">
                  {a.title ?? a.name}
                </div>
                <div className="text-xs text-zinc-400 mt-1">
                  key: <span className="oh-kbd">{a.agent_key ?? a.key}</span>
                </div>
              </div>
              <span className="oh-badge">spec</span>
            </div>

            <div className="mt-3 text-sm text-zinc-300 leading-relaxed">
              {a.description}
            </div>

            <div className="mt-4">
              <div className="text-xs text-zinc-500 mb-2">Default payload</div>
              <pre className="text-xs text-zinc-200 bg-zinc-950/60 border border-zinc-800 rounded-xl p-3 overflow-auto">
                {JSON.stringify(a.default_payload_schema ?? {}, null, 2)}
              </pre>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
