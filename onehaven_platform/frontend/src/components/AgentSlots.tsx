import React from "react";
import { motion, useReducedMotion } from "framer-motion";
import { Activity, Bot, Clock3, RefreshCw, UserCog } from "lucide-react";
import { api } from "../lib/api";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";

type SlotSpec = {
  slot_key: string;
  title: string;
  description: string;
  owner_type: "human" | "ai" | "hybrid" | string;
  default_status: string;
};

type SlotAssign = {
  id: number;
  slot_key: string;
  property_id: number | null;
  owner_type: string;
  assignee: string | null;
  status: string;
  notes: string | null;
  updated_at?: string;
};

type RunLite = {
  id: number;
  agent_key: string;
  status: string;
  runtime_health?: string;
  approval_status?: string;
  attempts?: number;
  duration_ms?: number | null;
  property_id?: number | null;
};

type Props = {
  propertyId?: number;
  propertyOnly?: boolean;
};

function statusTone(status: string) {
  const s = (status || "idle").toLowerCase();
  if (s === "done") return "oh-pill oh-pill-good";
  if (s === "blocked") return "oh-pill oh-pill-warn";
  if (s === "running") return "oh-pill oh-pill-accent";
  if (s === "queued") return "oh-pill oh-pill-warn";
  if (s === "failed" || s === "timed_out") return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function ownerTone(ownerType: string) {
  const s = (ownerType || "").toLowerCase();
  if (s === "ai") return "oh-pill oh-pill-accent";
  if (s === "human") return "oh-pill oh-pill-good";
  if (s === "hybrid") return "oh-pill oh-pill-warn";
  return "oh-pill";
}

function healthTone(runtimeHealth?: string) {
  const s = (runtimeHealth || "").toLowerCase();
  if (s === "healthy") return "text-green-300";
  if (s === "degraded") return "text-yellow-200";
  if (s === "unhealthy") return "text-red-300";
  return "text-app-4";
}

function asArray<T = any>(x: any): T[] {
  if (Array.isArray(x)) return x;
  if (x && Array.isArray(x.items)) return x.items;
  if (x && Array.isArray(x.rows)) return x.rows;
  if (x && Array.isArray(x.data)) return x.data;
  return [];
}

function fmtMs(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (v < 1000) return `${v}ms`;
  const sec = v / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${Math.round(sec % 60)}s`;
}

function fmtDate(v?: string) {
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <div className="text-app-4">{label}</div>
      <div className="text-app-1 font-medium text-right">{value}</div>
    </div>
  );
}

export default function AgentSlots({ propertyId, propertyOnly }: Props) {
  const prefersReducedMotion = useReducedMotion();

  const [specs, setSpecs] = React.useState<SlotSpec[]>([]);
  const [assignments, setAssignments] = React.useState<SlotAssign[]>([]);
  const [latestRuns, setLatestRuns] = React.useState<RunLite[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [busyKey, setBusyKey] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(false);

  const abortRef = React.useRef<AbortController | null>(null);

  const [editing, setEditing] = React.useState<null | {
    slot_key: string;
    status: string;
    assignee: string;
    notes: string;
    owner_type: string;
  }>(null);

  const shouldFetchGlobalAssignments = React.useMemo(() => {
    if (propertyId != null) return !propertyOnly;
    return true;
  }, [propertyId, propertyOnly]);

  async function refresh() {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);

      const tasks: Promise<any>[] = [api.slotSpecs(ac.signal)];

      if (shouldFetchGlobalAssignments) {
        tasks.push(api.slotAssignments(undefined, ac.signal));
      } else {
        tasks.push(Promise.resolve([]));
      }

      if (propertyId != null) {
        tasks.push(api.slotAssignments(propertyId, ac.signal));
      } else {
        tasks.push(Promise.resolve([]));
      }

      if (propertyId != null) {
        tasks.push(api.agentRunsHistory({ property_id: propertyId, limit: 30 }, ac.signal));
      } else {
        tasks.push(api.agentRunsHistory({ limit: 30 }, ac.signal));
      }

      const [specsOut, globalAssignOut, propertyAssignOut, runsOut] =
        await Promise.all(tasks);

      const specRows = asArray<SlotSpec>(specsOut);
      const globalAssign = asArray<SlotAssign>(globalAssignOut);
      const propertyAssign = asArray<SlotAssign>(propertyAssignOut);
      const runRows = asArray<RunLite>(runsOut);

      const mergedAssignments =
        propertyId != null
          ? [
              ...globalAssign.filter((x) => x.property_id == null),
              ...propertyAssign.filter((x) => x.property_id === propertyId),
            ]
          : globalAssign;

      setSpecs(specRows);
      setAssignments(mergedAssignments);
      setLatestRuns(runRows);
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    refresh();
    return () => abortRef.current?.abort();
  }, [propertyId, propertyOnly]);

  const bySlot = React.useMemo(() => {
    const m = new Map<string, SlotAssign>();
    for (const a of assignments) {
      if (!m.has(a.slot_key)) m.set(a.slot_key, a);
      else {
        const existing = m.get(a.slot_key)!;
        const existingIsGlobal = existing.property_id == null;
        const nextIsProperty = a.property_id != null;
        if (existingIsGlobal && nextIsProperty) m.set(a.slot_key, a);
      }
    }
    return m;
  }, [assignments]);

  const runsByAgent = React.useMemo(() => {
    const m = new Map<string, RunLite>();
    for (const r of latestRuns) {
      if (!m.has(r.agent_key)) m.set(r.agent_key, r);
    }
    return m;
  }, [latestRuns]);

  async function saveEdit() {
    if (!editing) return;

    try {
      setBusyKey(editing.slot_key);
      setErr(null);

      await api.upsertSlotAssignment({
        slot_key: editing.slot_key,
        property_id: propertyId ?? null,
        owner_type: editing.owner_type,
        assignee: editing.assignee.trim() || null,
        status: editing.status,
        notes: editing.notes.trim() || null,
      });

      setEditing(null);
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusyKey(null);
    }
  }

  async function resetSlot(slotKey: string) {
    try {
      setBusyKey(slotKey);
      setErr(null);

      await api.upsertSlotAssignment({
        slot_key: slotKey,
        property_id: propertyId ?? null,
        owner_type: "hybrid",
        assignee: null,
        status: "idle",
        notes: null,
      });

      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <Surface
      title={propertyId != null ? "Property agent slots" : "Agent slots"}
      subtitle={
        propertyId != null
          ? propertyOnly
            ? "Property-specific slot assignments only."
            : "Property-specific assignments layered over global defaults."
          : "Global slot assignments across the operating system."
      }
      actions={
        <button
          onClick={refresh}
          className="oh-btn oh-btn-secondary cursor-pointer"
          disabled={loading}
        >
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          {loading ? "syncing…" : "sync"}
        </button>
      }
    >
      {err ? (
        <div className="mb-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
          {err}
        </div>
      ) : null}

      {loading && specs.length === 0 ? (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="oh-skeleton h-[260px] rounded-2xl" />
          ))}
        </div>
      ) : null}

      {!loading && specs.length === 0 ? (
        <EmptyState
          compact
          title="No slot specs found"
          description="No agent slot definitions were returned by the API."
        />
      ) : null}

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        {specs.map((slot, idx) => {
          const assign = bySlot.get(slot.slot_key);
          const run = runsByAgent.get(slot.slot_key);
          const isEditing = editing?.slot_key === slot.slot_key;
          const isBusy = busyKey === slot.slot_key;

          const Card = prefersReducedMotion ? "div" : motion.div;
          const motionProps = prefersReducedMotion
            ? {}
            : {
                initial: { opacity: 0, y: 10 },
                animate: { opacity: 1, y: 0 },
                transition: { duration: 0.18, delay: idx * 0.02 },
              };

          return (
            <Card
              key={slot.slot_key}
              {...motionProps}
              className="rounded-2xl border border-app bg-app-panel p-4 shadow-soft"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <div className="text-sm font-semibold text-app-0">{slot.title}</div>
                    <span className={ownerTone(assign?.owner_type || slot.owner_type)}>
                      {assign?.owner_type || slot.owner_type}
                    </span>
                    <span className={statusTone(assign?.status || slot.default_status)}>
                      {assign?.status || slot.default_status}
                    </span>
                  </div>

                  <div className="text-xs text-app-4 mt-2 leading-relaxed">
                    {slot.description}
                  </div>
                </div>

                <div className="text-right shrink-0">
                  <div className="text-[11px] uppercase tracking-widest text-app-4">
                    run health
                  </div>
                  <div className={`text-xs mt-1 font-medium ${healthTone(run?.runtime_health)}`}>
                    {run?.runtime_health || "—"}
                  </div>
                </div>
              </div>

              <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="rounded-2xl border border-app bg-app-muted p-3">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <UserCog className="h-3.5 w-3.5" />
                    Assignment
                  </div>
                  <div className="mt-2 text-sm text-app-1 font-medium">
                    {assign?.assignee || "Unassigned"}
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    updated {fmtDate(assign?.updated_at)}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-muted p-3">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <Activity className="h-3.5 w-3.5" />
                    Latest run
                  </div>
                  <div className="mt-2 text-sm text-app-1 font-medium">
                    {run?.status || "—"}
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    attempts {run?.attempts ?? "—"} · {fmtMs(run?.duration_ms)}
                  </div>
                </div>
              </div>

              {assign?.notes ? (
                <div className="mt-3 rounded-2xl border border-app bg-app-muted p-3">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Notes
                  </div>
                  <div className="mt-2 text-sm text-app-2">{assign.notes}</div>
                </div>
              ) : null}

              {!isEditing ? (
                <div className="mt-4 flex items-center gap-2 flex-wrap">
                  <button
                    className="oh-btn oh-btn-secondary cursor-pointer"
                    onClick={() =>
                      setEditing({
                        slot_key: slot.slot_key,
                        status: assign?.status || slot.default_status || "idle",
                        assignee: assign?.assignee || "",
                        notes: assign?.notes || "",
                        owner_type:
                          assign?.owner_type || slot.owner_type || "hybrid",
                      })
                    }
                    disabled={isBusy}
                  >
                    edit
                  </button>

                  <button
                    className="oh-btn oh-btn-secondary cursor-pointer"
                    onClick={() => resetSlot(slot.slot_key)}
                    disabled={isBusy}
                  >
                    {isBusy ? "..." : "reset"}
                  </button>
                </div>
              ) : (
                <div className="mt-4 grid grid-cols-1 gap-3">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <label className="block">
                      <div className="oh-field-label">owner type</div>
                      <select
                        value={editing.owner_type}
                        onChange={(e) =>
                          setEditing((prev) =>
                            prev ? { ...prev, owner_type: e.target.value } : prev,
                          )
                        }
                        className="oh-input"
                      >
                        <option value="human">human</option>
                        <option value="ai">ai</option>
                        <option value="hybrid">hybrid</option>
                      </select>
                    </label>

                    <label className="block">
                      <div className="oh-field-label">status</div>
                      <select
                        value={editing.status}
                        onChange={(e) =>
                          setEditing((prev) =>
                            prev ? { ...prev, status: e.target.value } : prev,
                          )
                        }
                        className="oh-input"
                      >
                        <option value="idle">idle</option>
                        <option value="queued">queued</option>
                        <option value="running">running</option>
                        <option value="blocked">blocked</option>
                        <option value="done">done</option>
                        <option value="failed">failed</option>
                      </select>
                    </label>

                    <label className="block">
                      <div className="oh-field-label">assignee</div>
                      <input
                        value={editing.assignee}
                        onChange={(e) =>
                          setEditing((prev) =>
                            prev ? { ...prev, assignee: e.target.value } : prev,
                          )
                        }
                        className="oh-input"
                        placeholder="name or role"
                      />
                    </label>
                  </div>

                  <label className="block">
                    <div className="oh-field-label">notes</div>
                    <textarea
                      value={editing.notes}
                      onChange={(e) =>
                        setEditing((prev) =>
                          prev ? { ...prev, notes: e.target.value } : prev,
                        )
                      }
                      className="oh-textarea"
                      placeholder="slot-specific notes"
                    />
                  </label>

                  <div className="rounded-2xl border border-app bg-app-muted p-3">
                    <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                      <Clock3 className="h-3.5 w-3.5" />
                      Current slot context
                    </div>
                    <div className="mt-3 space-y-2">
                      <InfoRow label="Slot key" value={slot.slot_key} />
                      <InfoRow label="Property scope" value={propertyId ?? "global"} />
                      <InfoRow label="Current run" value={run?.status || "—"} />
                    </div>
                  </div>

                  <div className="flex items-center gap-2">
                    <button
                      className="oh-btn oh-btn-primary cursor-pointer"
                      onClick={saveEdit}
                      disabled={isBusy}
                    >
                      {isBusy ? "saving…" : "save"}
                    </button>
                    <button
                      className="oh-btn oh-btn-secondary cursor-pointer"
                      onClick={() => setEditing(null)}
                      disabled={isBusy}
                    >
                      cancel
                    </button>
                  </div>
                </div>
              )}
            </Card>
          );
        })}
      </div>

      {!loading && specs.length > 0 ? (
        <div className="mt-4 flex items-center gap-2 text-xs text-app-4">
          <Bot className="h-3.5 w-3.5" />
          slots merge global defaults with property overrides when available
        </div>
      ) : null}
    </Surface>
  );
}