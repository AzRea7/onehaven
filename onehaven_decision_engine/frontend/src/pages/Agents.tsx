// frontend/src/pages/Agents.tsx
import React from "react";
import AgentRunCompare from "../components/AgentRunCompare";
import AgentRunHistory from "../components/AgentRunHistory";
import AgentSlots from "../components/AgentSlots";
import { api } from "../lib/api";

type Summary = {
  total: number;
  pending_approval: number;
  stale_running: number;
  failures: number;
  average_duration_ms?: number | null;
  by_status: Record<string, number>;
  by_agent: Array<Record<string, any>>;
};

type RunDetail = Record<string, any> | null;

type RunRow = {
  id: number;
  agent_key: string;
  status: string;
  [key: string]: any;
};

function pretty(v: any) {
  try {
    if (v == null) return "";
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return "";
      const parsed = JSON.parse(s);
      return JSON.stringify(parsed, null, 2);
    }
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v ?? "");
  }
}

function Stat({
  label,
  value,
  sub,
}: {
  label: string;
  value: React.ReactNode;
  sub?: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="text-xs uppercase tracking-[0.18em] text-zinc-400">
        {label}
      </div>
      <div className="mt-2 text-2xl font-semibold text-zinc-100">{value}</div>
      {sub ? <div className="mt-1 text-xs text-zinc-400">{sub}</div> : null}
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const s = (status || "").toLowerCase();
  const cls =
    s === "done"
      ? "bg-emerald-900/30 border-emerald-700/50 text-emerald-200"
      : s === "failed" || s === "timed_out"
        ? "bg-red-900/30 border-red-700/50 text-red-200"
        : s === "blocked"
          ? "bg-amber-900/30 border-amber-700/50 text-amber-200"
          : s === "running"
            ? "bg-sky-900/30 border-sky-700/50 text-sky-200"
            : "bg-zinc-900/30 border-zinc-700/50 text-zinc-200";

  return (
    <span
      className={`inline-flex items-center px-2 py-1 text-xs rounded-lg border ${cls}`}
    >
      {status}
    </span>
  );
}

function fmtMs(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (v < 1000) return `${v}ms`;
  const sec = v / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${Math.round(sec % 60)}s`;
}

export default function Agents() {
  const [agents, setAgents] = React.useState<any[]>([]);
  const [summary, setSummary] = React.useState<Summary | null>(null);
  const [historyRows, setHistoryRows] = React.useState<RunRow[]>([]);
  const [selectedRunIds, setSelectedRunIds] = React.useState<number[]>([]);
  const [compareRows, setCompareRows] = React.useState<RunRow[]>([]);
  const [runDetail, setRunDetail] = React.useState<RunDetail>(null);
  const [traceRows, setTraceRows] = React.useState<any[]>([]);
  const [propertyId, setPropertyId] = React.useState<number>(1);
  const [propertyIdRaw, setPropertyIdRaw] = React.useState<string>("1");
  const [statusFilter, setStatusFilter] = React.useState<string>("");
  const [agentKeyFilter, setAgentKeyFilter] = React.useState<string>("");
  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [loadingHistory, setLoadingHistory] = React.useState(false);

  const loadAgents = React.useCallback(async () => {
    const a = await api.agents();
    setAgents(Array.isArray(a) ? a : []);
  }, []);

  const loadSummaryAndHistory = React.useCallback(async () => {
    setLoadingHistory(true);
    try {
      const params: Record<string, any> = {
        property_id: propertyId,
        limit: 100,
      };
      if (statusFilter) params.status = statusFilter;
      if (agentKeyFilter) params.agent_key = agentKeyFilter;

      const [sum, history] = await Promise.all([
        api.agentRunsSummary(params),
        api.agentRunsHistory(params),
      ]);

      setSummary(sum);
      setHistoryRows(Array.isArray(history?.rows) ? history.rows : []);
    } finally {
      setLoadingHistory(false);
    }
  }, [propertyId, statusFilter, agentKeyFilter]);

  const loadRunDetail = React.useCallback(async (runId: number) => {
    const [detail, trace] = await Promise.all([
      api.agentRunGet(runId),
      api.agentRunTrace(runId),
    ]);
    setRunDetail(detail);
    setTraceRows(Array.isArray(trace?.rows) ? trace.rows : []);
  }, []);

  const loadCompare = React.useCallback(async (ids: number[]) => {
    if (ids.length < 2) {
      setCompareRows([]);
      return;
    }
    const res = await api.agentRunsCompare(ids);
    setCompareRows(Array.isArray(res?.rows) ? res.rows : []);
  }, []);

  React.useEffect(() => {
    (async () => {
      try {
        setBusy(true);
        setErr(null);
        await Promise.all([loadAgents(), loadSummaryAndHistory()]);
      } catch (e: any) {
        setErr(String(e?.message || e));
      } finally {
        setBusy(false);
      }
    })();
  }, [loadAgents, loadSummaryAndHistory]);

  React.useEffect(() => {
    loadCompare(selectedRunIds).catch((e: any) =>
      setErr(String(e?.message || e)),
    );
  }, [selectedRunIds, loadCompare]);

  function syncPropertyIdFromRaw() {
    const n = parseInt((propertyIdRaw || "").trim(), 10);
    if (!Number.isFinite(n) || n <= 0) return;
    setPropertyId(n);
  }

  function toggleCompare(runId: number) {
    setSelectedRunIds((prev) => {
      if (prev.includes(runId)) return prev.filter((id) => id !== runId);
      if (prev.length >= 4) return [...prev.slice(1), runId];
      return [...prev, runId];
    });
  }

  async function doPlan() {
    setBusy(true);
    setErr(null);
    try {
      const res = await api.agentRunsPlan(propertyId);
      alert(`Planned ${Array.isArray(res) ? res.length : 0} run(s).`);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doEnqueue() {
    setBusy(true);
    setErr(null);
    try {
      await api.agentRunsEnqueue(propertyId, true);
      await loadSummaryAndHistory();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function actionRefresh() {
    setBusy(true);
    setErr(null);
    try {
      await loadSummaryAndHistory();
      if (runDetail?.id) await loadRunDetail(runDetail.id);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doApprove(runId: number) {
    setBusy(true);
    try {
      await api.agentRunsApprove(runId);
      await actionRefresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doReject(runId: number) {
    const reason =
      prompt("Reject reason?", "rejected_by_owner") || "rejected_by_owner";
    setBusy(true);
    try {
      await api.agentRunsReject(runId, reason);
      await actionRefresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doApply(runId: number) {
    setBusy(true);
    try {
      await api.agentRunsApply(runId);
      await actionRefresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function doRetry(runId: number) {
    setBusy(true);
    try {
      await api.agentRunsRetry(runId, true);
      await actionRefresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4 flex-wrap">
        <div>
          <div className="text-2xl font-semibold tracking-tight text-zinc-100">
            Agents
          </div>
          <div className="text-sm text-zinc-400 mt-1 max-w-3xl">
            This turns the agent area into an actual operating console:
            lifecycle summary, history, compare, trace, and slot ownership.
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="text-xs text-zinc-500">Property ID</div>
          <input
            className="w-28 px-3 py-2 rounded-xl bg-zinc-950/60 border border-zinc-800 text-sm text-zinc-200"
            value={propertyIdRaw}
            inputMode="numeric"
            onChange={(e) => setPropertyIdRaw(e.target.value)}
            onBlur={syncPropertyIdFromRaw}
            onKeyDown={(e) => e.key === "Enter" && syncPropertyIdFromRaw()}
          />
          <select
            className="px-3 py-2 rounded-xl bg-zinc-950/60 border border-zinc-800 text-sm text-zinc-200"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="">all statuses</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="blocked">blocked</option>
            <option value="done">done</option>
            <option value="failed">failed</option>
            <option value="timed_out">timed_out</option>
          </select>
          <select
            className="px-3 py-2 rounded-xl bg-zinc-950/60 border border-zinc-800 text-sm text-zinc-200"
            value={agentKeyFilter}
            onChange={(e) => setAgentKeyFilter(e.target.value)}
          >
            <option value="">all agents</option>
            {agents.map((a) => (
              <option key={a.agent_key ?? a.key} value={a.agent_key ?? a.key}>
                {a.title ?? a.agent_key ?? a.key}
              </option>
            ))}
          </select>
          <button className="oh-btn" onClick={actionRefresh} disabled={busy}>
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

      {err ? (
        <div className="rounded-2xl border border-red-900/60 bg-red-950/30 p-4 text-red-200">
          {err}
        </div>
      ) : null}

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Stat
          label="Runs in view"
          value={summary?.total ?? 0}
          sub={`property #${propertyId}`}
        />
        <Stat label="Pending approval" value={summary?.pending_approval ?? 0} />
        <Stat
          label="Failures"
          value={summary?.failures ?? 0}
          sub={`stale: ${summary?.stale_running ?? 0}`}
        />
        <Stat
          label="Average duration"
          value={fmtMs(summary?.average_duration_ms)}
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-6">
        <div className="space-y-6">
          <AgentRunHistory
            rows={historyRows}
            selectedRunIds={selectedRunIds}
            onToggleSelect={toggleCompare}
            onOpenRun={(runId) =>
              loadRunDetail(runId).catch((e: any) =>
                setErr(String(e?.message || e)),
              )
            }
            loading={loadingHistory}
          />
          <AgentRunCompare rows={compareRows} />
        </div>

        <div className="space-y-6">
          <AgentSlots propertyId={propertyId} propertyOnly />

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="text-sm font-semibold text-zinc-100">
              Selected run
            </div>
            <div className="text-xs text-zinc-400 mt-1">
              Trace-driven detail view for one run at a time.
            </div>

            {!runDetail ? (
              <div className="mt-4 text-sm text-zinc-400">
                Select a run from history.
              </div>
            ) : (
              <div className="mt-4 space-y-4">
                <div className="flex items-start justify-between gap-3 flex-wrap">
                  <div>
                    <div className="flex items-center gap-2 flex-wrap">
                      <div className="text-lg font-semibold text-zinc-100">
                        {runDetail.agent_key}
                      </div>
                      <StatusPill status={runDetail.status} />
                      {runDetail.runtime_health ? (
                        <span className="inline-flex items-center px-2 py-1 text-xs rounded-lg border border-white/10 text-zinc-300">
                          {runDetail.runtime_health}
                        </span>
                      ) : null}
                    </div>
                    <div className="mt-2 text-xs text-zinc-400 flex flex-wrap gap-x-4 gap-y-1">
                      <span>run #{runDetail.id}</span>
                      <span>property #{runDetail.property_id ?? "—"}</span>
                      <span>
                        approval: {runDetail.approval_status || "not_required"}
                      </span>
                      <span>attempts: {runDetail.attempts ?? 0}</span>
                      <span>trace: {runDetail.trace_count ?? 0}</span>
                      <span>messages: {runDetail.message_count ?? 0}</span>
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <button
                      className="oh-btn"
                      onClick={() => doRetry(runDetail.id)}
                      disabled={busy}
                    >
                      Retry
                    </button>
                    <button
                      className="oh-btn"
                      onClick={() => doApprove(runDetail.id)}
                      disabled={busy || runDetail.approval_status !== "pending"}
                    >
                      Approve
                    </button>
                    <button
                      className="oh-btn"
                      onClick={() => doReject(runDetail.id)}
                      disabled={busy}
                    >
                      Reject
                    </button>
                    <button
                      className="oh-btn-primary"
                      onClick={() => doApply(runDetail.id)}
                      disabled={
                        busy || runDetail.approval_status !== "approved"
                      }
                    >
                      Apply
                    </button>
                  </div>
                </div>

                {runDetail.last_error ? (
                  <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-200">
                    {runDetail.last_error}
                  </div>
                ) : null}

                <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                  <div>
                    <div className="text-xs text-zinc-500 mb-2">Output</div>
                    <pre className="max-h-[280px] overflow-auto rounded-xl border border-zinc-800 bg-zinc-950/60 p-3 text-xs text-zinc-200">
                      {pretty(runDetail.output)}
                    </pre>
                  </div>
                  <div>
                    <div className="text-xs text-zinc-500 mb-2">
                      Proposed Actions
                    </div>
                    <pre className="max-h-[280px] overflow-auto rounded-xl border border-zinc-800 bg-zinc-950/60 p-3 text-xs text-zinc-200">
                      {pretty(runDetail.proposed)}
                    </pre>
                  </div>
                </div>

                <div>
                  <div className="text-xs text-zinc-500 mb-2">
                    Trace Timeline
                  </div>
                  <div className="max-h-[320px] overflow-auto space-y-2 rounded-xl border border-zinc-800 bg-zinc-950/40 p-3">
                    {traceRows.length === 0 ? (
                      <div className="text-sm text-zinc-400">
                        No trace events for this run yet.
                      </div>
                    ) : (
                      traceRows.map((row: any) => (
                        <div
                          key={row.id}
                          className="rounded-xl border border-white/5 bg-white/5 p-3"
                        >
                          <div className="flex items-center justify-between gap-3 flex-wrap">
                            <div className="text-sm font-medium text-zinc-100">
                              {row.event_type}
                            </div>
                            <div className="text-[11px] text-zinc-400">
                              #{row.id}
                            </div>
                          </div>
                          <div className="mt-1 text-[11px] text-zinc-400">
                            {row.created_at || row.ts || ""}
                          </div>
                          <pre className="mt-2 whitespace-pre-wrap break-words text-xs text-zinc-200">
                            {pretty(row.payload || row.event)}
                          </pre>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
