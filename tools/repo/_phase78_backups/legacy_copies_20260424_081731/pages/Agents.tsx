import React from "react";
import {
  Bot,
  CheckCircle2,
  Clock3,
  GitCompare,
  PlayCircle,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";
import AgentRunCompare from "onehaven_onehaven_platform/frontend/src/components/AgentRunCompare";
import AgentRunHistory from "onehaven_onehaven_platform/frontend/src/components/AgentRunHistory";
import AgentSlots from "onehaven_onehaven_platform/frontend/src/components/AgentSlots";
import EmptyState from "onehaven_onehaven_platform/frontend/src/components/EmptyState";
import PageHero from "onehaven_onehaven_platform/frontend/src/components/PageHero";
import PageShell from "onehaven_onehaven_platform/frontend/src/components/PageShell";
import Surface from "onehaven_onehaven_platform/frontend/src/components/Surface";
import KpiCard from "onehaven_onehaven_platform/frontend/src/components/KpiCard";
import Golem from "onehaven_onehaven_platform/frontend/src/components/Golem";
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

function StatusPill({ status }: { status: string }) {
  const s = (status || "").toLowerCase();
  const cls =
    s === "done"
      ? "oh-pill oh-pill-good"
      : s === "failed" || s === "timed_out"
        ? "oh-pill oh-pill-bad"
        : s === "blocked"
          ? "oh-pill oh-pill-warn"
          : s === "running"
            ? "oh-pill oh-pill-accent"
            : "oh-pill";

  return <span className={cls}>{status}</span>;
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
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Automation console"
          title="Agents"
          subtitle="Lifecycle summary, run history, compare, trace review, and slot ownership in one serious operator view."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[240px] w-[240px] md:h-[270px] md:w-[270px] translate-y-[-6px] opacity-95">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <div className="flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2">
                <span className="text-xs text-app-4">Property ID</span>
                <input
                  className="w-20 bg-transparent text-sm text-app-1 outline-none"
                  value={propertyIdRaw}
                  inputMode="numeric"
                  onChange={(e) => setPropertyIdRaw(e.target.value)}
                  onBlur={syncPropertyIdFromRaw}
                  onKeyDown={(e) =>
                    e.key === "Enter" && syncPropertyIdFromRaw()
                  }
                />
              </div>

              <select
                className="oh-input !w-auto min-w-[160px]"
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
                className="oh-input !w-auto min-w-[170px]"
                value={agentKeyFilter}
                onChange={(e) => setAgentKeyFilter(e.target.value)}
              >
                <option value="">all agents</option>
                {agents.map((a) => (
                  <option
                    key={a.agent_key ?? a.key}
                    value={a.agent_key ?? a.key}
                  >
                    {a.title ?? a.agent_key ?? a.key}
                  </option>
                ))}
              </select>

              <button
                className="oh-btn oh-btn-secondary"
                onClick={actionRefresh}
                disabled={busy}
              >
                <RefreshCw
                  className={`h-4 w-4 ${busy ? "animate-spin" : ""}`}
                />
                Refresh
              </button>

              <button
                className="oh-btn oh-btn-secondary"
                onClick={doPlan}
                disabled={busy}
              >
                Plan
              </button>

              <button
                className="oh-btn oh-btn-primary"
                onClick={doEnqueue}
                disabled={busy}
              >
                Enqueue
              </button>
            </>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          <KpiCard
            title="Runs in view"
            value={summary?.total ?? 0}
            subtitle={`property #${propertyId}`}
            icon={Bot}
            tone="accent"
          />
          <KpiCard
            title="Pending approval"
            value={summary?.pending_approval ?? 0}
            subtitle="human gate"
            icon={ShieldCheck}
            tone="warning"
          />
          <KpiCard
            title="Failures"
            value={summary?.failures ?? 0}
            subtitle={`stale running ${summary?.stale_running ?? 0}`}
            icon={CheckCircle2}
            tone={(summary?.failures ?? 0) > 0 ? "danger" : "success"}
          />
          <KpiCard
            title="Average duration"
            value={fmtMs(summary?.average_duration_ms)}
            subtitle="recent visible runs"
            icon={Clock3}
          />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-6">
          <div className="space-y-6">
            <Surface
              title="Run history"
              subtitle="Select runs to inspect or compare."
              actions={
                selectedRunIds.length ? (
                  <span className="oh-pill oh-pill-accent">
                    <GitCompare className="h-3.5 w-3.5" />
                    {selectedRunIds.length} selected
                  </span>
                ) : null
              }
            >
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
            </Surface>

            <Surface
              title="Compare selected runs"
              subtitle="Compare up to 4 runs side by side."
            >
              {compareRows.length === 0 ? (
                <EmptyState
                  compact
                  title="No compare rows yet"
                  description="Select at least two runs from the history list."
                />
              ) : (
                <AgentRunCompare rows={compareRows} selectedRunIds={[]} onToggleSelect={function (runId: number): void {
                    throw new Error("Function not implemented.");
                  } } onOpenRun={function (runId: number): void {
                    throw new Error("Function not implemented.");
                  } } />
              )}
            </Surface>
          </div>

          <div className="space-y-6">
            <AgentSlots propertyId={propertyId} propertyOnly />

            <Surface
              title="Selected run"
              subtitle="Trace-driven detail view for one run at a time."
              actions={
                runDetail?.status ? (
                  <StatusPill status={runDetail.status} />
                ) : null
              }
            >
              {!runDetail ? (
                <EmptyState
                  compact
                  title="No run selected"
                  description="Pick a run from history to inspect output, proposed actions, and trace events."
                />
              ) : (
                <div className="space-y-4">
                  <div className="flex items-start justify-between gap-3 flex-wrap">
                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <div className="text-lg font-semibold text-app-0">
                          {runDetail.agent_key}
                        </div>
                        <StatusPill status={runDetail.status} />
                        {runDetail.runtime_health ? (
                          <span className="oh-pill">
                            {runDetail.runtime_health}
                          </span>
                        ) : null}
                      </div>
                      <div className="mt-2 text-xs text-app-4 flex flex-wrap gap-x-4 gap-y-1">
                        <span>run #{runDetail.id}</span>
                        <span>property #{runDetail.property_id ?? "—"}</span>
                        <span>
                          approval:{" "}
                          {runDetail.approval_status || "not_required"}
                        </span>
                        <span>attempts: {runDetail.attempts ?? 0}</span>
                        <span>trace: {runDetail.trace_count ?? 0}</span>
                        <span>messages: {runDetail.message_count ?? 0}</span>
                      </div>
                    </div>

                    <div className="flex flex-wrap gap-2">
                      <button
                        className="oh-btn oh-btn-secondary"
                        onClick={() => doRetry(runDetail.id)}
                        disabled={busy}
                      >
                        Retry
                      </button>
                      <button
                        className="oh-btn oh-btn-secondary"
                        onClick={() => doApprove(runDetail.id)}
                        disabled={
                          busy || runDetail.approval_status !== "pending"
                        }
                      >
                        Approve
                      </button>
                      <button
                        className="oh-btn oh-btn-secondary"
                        onClick={() => doReject(runDetail.id)}
                        disabled={busy}
                      >
                        Reject
                      </button>
                      <button
                        className="oh-btn oh-btn-primary"
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
                    <div className="rounded-2xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-300">
                      {runDetail.last_error}
                    </div>
                  ) : null}

                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <div>
                      <div className="text-xs text-app-4 mb-2">Output</div>
                      <pre className="max-h-[280px] overflow-auto rounded-2xl border border-app bg-app-muted p-3 text-xs text-app-1">
                        {pretty(runDetail.output)}
                      </pre>
                    </div>
                    <div>
                      <div className="text-xs text-app-4 mb-2">
                        Proposed Actions
                      </div>
                      <pre className="max-h-[280px] overflow-auto rounded-2xl border border-app bg-app-muted p-3 text-xs text-app-1">
                        {pretty(runDetail.proposed)}
                      </pre>
                    </div>
                  </div>

                  <div>
                    <div className="text-xs text-app-4 mb-2">
                      Trace Timeline
                    </div>
                    <div className="max-h-[320px] overflow-auto space-y-2 rounded-2xl border border-app bg-app-muted p-3">
                      {traceRows.length === 0 ? (
                        <div className="text-sm text-app-4">
                          No trace events for this run yet.
                        </div>
                      ) : (
                        traceRows.map((row: any) => (
                          <div
                            key={row.id}
                            className="rounded-2xl border border-app bg-app-panel p-3"
                          >
                            <div className="flex items-center justify-between gap-3 flex-wrap">
                              <div className="text-sm font-medium text-app-0">
                                {row.event_type}
                              </div>
                              <div className="text-[11px] text-app-4">
                                #{row.id}
                              </div>
                            </div>
                            <div className="mt-1 text-[11px] text-app-4">
                              {row.created_at || row.ts || ""}
                            </div>
                            <pre className="mt-2 whitespace-pre-wrap break-words text-xs text-app-1">
                              {pretty(row.payload || row.event)}
                            </pre>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              )}
            </Surface>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
