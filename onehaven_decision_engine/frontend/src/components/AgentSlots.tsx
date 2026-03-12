import React from "react";
import { motion, useReducedMotion } from "framer-motion";
import { api } from "../lib/api";

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
  if (s === "done") return "bg-green-400/10 border-green-400/20 text-green-200";
  if (s === "blocked")
    return "bg-amber-400/10 border-amber-400/20 text-amber-100";
  if (s === "running") return "bg-blue-400/10 border-blue-400/20 text-blue-200";
  if (s === "queued")
    return "bg-yellow-300/10 border-yellow-300/20 text-yellow-100";
  if (s === "failed" || s === "timed_out")
    return "bg-red-400/10 border-red-400/20 text-red-200";
  return "bg-white/5 border-white/10 text-white/70";
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
      if (shouldFetchGlobalAssignments)
        tasks.push(api.slotAssignments(undefined, ac.signal));
      else tasks.push(Promise.resolve([]));
      if (propertyId != null)
        tasks.push(api.slotAssignments(propertyId, ac.signal));
      else tasks.push(Promise.resolve([]));
      if (propertyId != null)
        tasks.push(
          api.agentRunsHistory(
            { property_id: propertyId, limit: 30 },
            ac.signal,
          ),
        );
      else tasks.push(api.agentRunsHistory({ limit: 30 }, ac.signal));

      const [sRaw, aGlobalRaw, aPropRaw, historyRaw] = await Promise.all(tasks);

      const s = asArray<SlotSpec>(sRaw);
      const aGlobal = asArray<SlotAssign>(aGlobalRaw);
      const aProp = asArray<SlotAssign>(aPropRaw);
      const merged = [...aProp, ...aGlobal].filter(Boolean);

      const seen = new Set<number>();
      const dedup = merged.filter((x) => {
        const id = Number(x?.id);
        if (!Number.isFinite(id) || id <= 0) return true;
        if (seen.has(id)) return false;
        seen.add(id);
        return true;
      });

      const historyRows = asArray<RunLite>(historyRaw?.rows ?? historyRaw);
      const latestMap = new Map<string, RunLite>();
      for (const row of historyRows) {
        if (!row?.agent_key) continue;
        if (!latestMap.has(row.agent_key)) latestMap.set(row.agent_key, row);
      }

      setSpecs(s);
      setAssignments(dedup);
      setLatestRuns(Array.from(latestMap.values()));
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

  const byKey = React.useMemo(() => {
    const m = new Map<string, SlotAssign>();
    for (const a of asArray<SlotAssign>(assignments)) {
      if (!a?.slot_key) continue;
      const existing = m.get(a.slot_key);
      const aIsProp = propertyId != null && a.property_id === propertyId;
      const aIsGlobal = a.property_id == null;
      if (propertyOnly && propertyId != null && !aIsProp) continue;
      if (!existing) {
        m.set(a.slot_key, a);
        continue;
      }
      const eIsProp = propertyId != null && existing.property_id === propertyId;
      const eIsGlobal = existing.property_id == null;
      if (eIsProp) continue;
      if (aIsProp) {
        m.set(a.slot_key, a);
        continue;
      }
      if (!eIsGlobal && aIsGlobal) m.set(a.slot_key, a);
    }
    return m;
  }, [assignments, propertyId, propertyOnly]);

  const runByAgentKey = React.useMemo(() => {
    const m = new Map<string, RunLite>();
    latestRuns.forEach((r) => m.set(String(r.agent_key), r));
    return m;
  }, [latestRuns]);

  function openEdit(spec: SlotSpec) {
    const a = byKey.get(spec.slot_key);
    const status = a?.status ?? spec.default_status ?? "idle";
    const assignee =
      a?.assignee ?? (spec.owner_type === "ai" ? "system" : "unassigned");
    const notes = a?.notes ?? "";
    const owner_type = a?.owner_type ?? spec.owner_type ?? "human";
    setEditing({
      slot_key: spec.slot_key,
      status,
      assignee,
      notes,
      owner_type,
    });
  }

  async function saveEdit() {
    if (!editing) return;
    try {
      setErr(null);
      setBusyKey(editing.slot_key);
      await api.upsertSlotAssignment({
        slot_key: editing.slot_key,
        property_id: propertyId ?? null,
        owner_type: editing.owner_type ?? "human",
        assignee:
          (editing.assignee || "").trim().toLowerCase() === "unassigned"
            ? null
            : (editing.assignee || "").trim(),
        status: (editing.status || "idle").trim(),
        notes: (editing.notes || "").trim() || null,
      });
      setEditing(null);
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusyKey(null);
    }
  }

  return (
    <div className="gradient-border rounded-2xl glass p-4">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <div className="text-sm font-semibold tracking-wide text-zinc-200">
            Agent Slots
          </div>
          <div className="mt-1 text-[11px] text-zinc-400 leading-relaxed">
            Slot ownership plus latest observed run for each workflow agent.
          </div>
        </div>
        <div className="flex items-center gap-2">
          {propertyId != null ? (
            <span className="text-[10px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 text-zinc-300">
              property #{propertyId}
            </span>
          ) : null}
          <button
            onClick={refresh}
            className="text-[11px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 hover:bg-white/10"
            disabled={loading}
          >
            {loading ? "syncing…" : "sync"}
          </button>
        </div>
      </div>

      {err ? (
        <div className="mt-3 text-[11px] p-2 rounded-lg border border-red-900/50 bg-red-950/30 text-red-200">
          {err}
        </div>
      ) : null}

      <div className="mt-4 space-y-3">
        {specs.length === 0 && !loading ? (
          <div className="text-[11px] text-zinc-400">
            No slot specs found yet.
          </div>
        ) : null}
        {specs.map((s) => {
          const a = byKey.get(s.slot_key);
          const latest = runByAgentKey.get(s.slot_key);
          const status =
            a?.status ?? latest?.status ?? s.default_status ?? "idle";
          const assignee =
            a?.assignee ?? (s.owner_type === "ai" ? "system" : "unassigned");
          const motionProps = prefersReducedMotion
            ? {}
            : { whileHover: { y: -2 } };
          return (
            <motion.div
              key={s.slot_key}
              {...motionProps}
              className="rounded-2xl border border-white/10 bg-black/20 p-3"
            >
              <div className="flex items-start justify-between gap-3 flex-wrap">
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <div className="text-sm font-semibold text-zinc-100">
                      {s.title}
                    </div>
                    <span
                      className={`rounded-full border px-2 py-0.5 text-[11px] ${statusTone(status)}`}
                    >
                      {status}
                    </span>
                    {latest?.runtime_health ? (
                      <span className="rounded-full border border-white/10 px-2 py-0.5 text-[11px] text-zinc-300">
                        {latest.runtime_health}
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-1 text-xs text-zinc-400">
                    {s.description}
                  </div>
                  <div className="mt-2 text-[11px] text-zinc-500 flex flex-wrap gap-x-4 gap-y-1">
                    <span>owner: {a?.owner_type ?? s.owner_type}</span>
                    <span>assignee: {assignee}</span>
                    {latest ? <span>run #{latest.id}</span> : null}
                    {latest?.approval_status ? (
                      <span>approval: {latest.approval_status}</span>
                    ) : null}
                    {latest?.duration_ms != null ? (
                      <span>duration: {fmtMs(latest.duration_ms)}</span>
                    ) : null}
                  </div>
                </div>

                <button
                  className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-zinc-200 hover:bg-white/10"
                  onClick={() => openEdit(s)}
                  disabled={busyKey === s.slot_key}
                >
                  {busyKey === s.slot_key ? "saving…" : "edit"}
                </button>
              </div>

              {a?.notes ? (
                <div className="mt-3 text-xs text-zinc-300">{a.notes}</div>
              ) : null}
            </motion.div>
          );
        })}
      </div>

      {editing ? (
        <div className="mt-4 rounded-2xl border border-white/10 bg-zinc-950/70 p-4">
          <div className="text-sm font-semibold text-zinc-100">
            Edit slot: {editing.slot_key}
          </div>
          <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
            <select
              className="rounded-xl border border-zinc-800 bg-zinc-950/60 px-3 py-2 text-sm text-zinc-200"
              value={editing.status}
              onChange={(e) =>
                setEditing({ ...editing, status: e.target.value })
              }
            >
              <option value="idle">idle</option>
              <option value="queued">queued</option>
              <option value="running">running</option>
              <option value="blocked">blocked</option>
              <option value="done">done</option>
              <option value="failed">failed</option>
            </select>
            <input
              className="rounded-xl border border-zinc-800 bg-zinc-950/60 px-3 py-2 text-sm text-zinc-200"
              value={editing.assignee}
              onChange={(e) =>
                setEditing({ ...editing, assignee: e.target.value })
              }
              placeholder="assignee"
            />
            <select
              className="rounded-xl border border-zinc-800 bg-zinc-950/60 px-3 py-2 text-sm text-zinc-200"
              value={editing.owner_type}
              onChange={(e) =>
                setEditing({ ...editing, owner_type: e.target.value })
              }
            >
              <option value="human">human</option>
              <option value="ai">ai</option>
              <option value="hybrid">hybrid</option>
            </select>
            <input
              className="rounded-xl border border-zinc-800 bg-zinc-950/60 px-3 py-2 text-sm text-zinc-200"
              value={editing.notes}
              onChange={(e) =>
                setEditing({ ...editing, notes: e.target.value })
              }
              placeholder="notes"
            />
          </div>
          <div className="mt-3 flex gap-2 justify-end">
            <button
              className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-xs text-zinc-200 hover:bg-white/10"
              onClick={() => setEditing(null)}
            >
              Cancel
            </button>
            <button
              className="rounded-xl border border-fuchsia-500/30 bg-fuchsia-500/20 px-3 py-2 text-xs text-fuchsia-100 hover:bg-fuchsia-500/30"
              onClick={saveEdit}
            >
              Save
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
