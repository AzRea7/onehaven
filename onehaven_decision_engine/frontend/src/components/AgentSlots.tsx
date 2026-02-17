// frontend/src/components/AgentSlots.tsx
import React from "react";
import { api } from "../lib/api";
import { motion, useReducedMotion } from "framer-motion";

function statusTone(status: string) {
  const s = (status || "idle").toLowerCase();
  if (s === "done") return "bg-green-400/10 border-green-400/20 text-green-200";
  if (s === "blocked") return "bg-red-400/10 border-red-400/20 text-red-200";
  if (s === "in_progress")
    return "bg-blue-400/10 border-blue-400/20 text-blue-200";
  if (s === "queued")
    return "bg-yellow-300/10 border-yellow-300/20 text-yellow-100";
  return "bg-white/5 border-white/10 text-white/70";
}

type SlotSpec = {
  slot_key: string;
  title: string;
  description: string;
  owner_type: "human" | "ai" | "hybrid";
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

type Props = {
  /** If provided, prefer per-property assignments. Also allow editing for that property. */
  propertyId?: number;
  /** If true, hide global-only slots when propertyId is provided */
  propertyOnly?: boolean;
};

function asArray<T = any>(x: any): T[] {
  if (Array.isArray(x)) return x;
  if (x && Array.isArray(x.items)) return x.items;
  if (x && Array.isArray(x.rows)) return x.rows;
  if (x && Array.isArray(x.data)) return x.data;
  return [];
}

/**
 * Performance goals:
 * - Avoid “n.map is not a function” by normalizing all list responses.
 * - Avoid UI freezing by:
 *   - removing per-row stagger animations
 *   - aborting in-flight requests on refresh/unmount
 *   - not fetching global+property assignments redundantly unless needed
 *   - memoizing derived maps
 */
export default function AgentSlots({ propertyId, propertyOnly }: Props) {
  const prefersReducedMotion = useReducedMotion();

  const [specs, setSpecs] = React.useState<SlotSpec[]>([]);
  const [assignments, setAssignments] = React.useState<SlotAssign[]>([]);
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
    // If you’re in a property view, you still want to show “effective”
    // assignments (property overrides global). So fetch both unless propertyOnly=true.
    if (propertyId != null) return !propertyOnly;
    // If no propertyId, you’re basically in “global board” mode.
    return true;
  }, [propertyId, propertyOnly]);

  async function refresh() {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);

      const tasks: Promise<any>[] = [];
      tasks.push(api.slotSpecs()); // cached in api.ts already

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

      const [sRaw, aGlobalRaw, aPropRaw] = await Promise.all(tasks);

      const s = asArray<SlotSpec>(sRaw);
      const aGlobal = asArray<SlotAssign>(aGlobalRaw);
      const aProp = asArray<SlotAssign>(aPropRaw);

      // Merge; property assignment should win when we compute "effective"
      const merged = [...aProp, ...aGlobal].filter(Boolean);

      // De-dupe by id (stable)
      const seen = new Set<number>();
      const dedup = merged.filter((x) => {
        const id = Number(x?.id);
        if (!Number.isFinite(id) || id <= 0) return true; // keep weird rows
        if (seen.has(id)) return false;
        seen.add(id);
        return true;
      });

      setSpecs(s);
      setAssignments(dedup);
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId, propertyOnly]);

  // Effective assignment per slot_key:
  // property assignment wins; otherwise global (property_id == null).
  const byKey = React.useMemo(() => {
    const m = new Map<string, SlotAssign>();

    for (const a of asArray<SlotAssign>(assignments)) {
      if (!a?.slot_key) continue;

      const existing = m.get(a.slot_key);

      const aIsProp = propertyId != null && a.property_id === propertyId;
      const aIsGlobal = a.property_id == null;

      // If propertyOnly, ignore globals entirely when in property view
      if (propertyOnly && propertyId != null && !aIsProp) continue;

      if (!existing) {
        m.set(a.slot_key, a);
        continue;
      }

      const eIsProp = propertyId != null && existing.property_id === propertyId;
      const eIsGlobal = existing.property_id == null;

      // If we already have property assignment, keep it.
      if (eIsProp) continue;

      // Prefer property assignment over global
      if (aIsProp) {
        m.set(a.slot_key, a);
        continue;
      }

      // Otherwise keep existing (stable)
      if (!eIsGlobal && aIsGlobal) {
        m.set(a.slot_key, a);
      }
    }

    return m;
  }, [assignments, propertyId, propertyOnly]);

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

      const payload = {
        slot_key: editing.slot_key,
        property_id: propertyId ?? null,
        owner_type: editing.owner_type ?? "human",
        assignee:
          (editing.assignee || "").trim().toLowerCase() === "unassigned"
            ? null
            : (editing.assignee || "").trim(),
        status: (editing.status || "idle").trim(),
        notes: (editing.notes || "").trim() || null,
      };

      await api.upsertSlotAssignment(payload);

      setEditing(null);
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusyKey(null);
    }
  }

  const safeSpecs = asArray<SlotSpec>(specs);

  return (
    <div className="gradient-border rounded-2xl glass p-3">
      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold tracking-wide text-zinc-200">
          Agent Slots
        </div>

        <div className="flex items-center gap-2">
          {propertyId != null && (
            <span className="text-[10px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 text-zinc-300">
              property #{propertyId}
            </span>
          )}

          <button
            onClick={refresh}
            className="text-[11px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 hover:bg-white/10"
            title="Refresh"
            disabled={loading}
          >
            {loading ? "syncing…" : "sync"}
          </button>
        </div>
      </div>

      <div className="mt-2 text-[11px] text-zinc-400 leading-relaxed">
        Humans now. Automation later. This is your operational truth layer.
      </div>

      {err && (
        <div className="mt-3 text-[11px] p-2 rounded-lg border border-red-900/50 bg-red-950/30 text-red-200">
          {err}
        </div>
      )}

      <div className="mt-3 space-y-2">
        {safeSpecs.length === 0 && !loading && (
          <div className="text-[11px] text-zinc-400">
            No slot specs found yet.
          </div>
        )}

        {safeSpecs.map((s) => {
          const a = byKey.get(s.slot_key);
          const status = a?.status ?? s.default_status ?? "idle";
          const assignee =
            a?.assignee ?? (s.owner_type === "ai" ? "system" : "unassigned");

          // If propertyOnly, hide slots without a property assignment
          if (
            propertyOnly &&
            propertyId != null &&
            a?.property_id !== propertyId
          ) {
            return null;
          }

          const RowWrap: any = prefersReducedMotion ? "div" : motion.div;
          const rowProps = prefersReducedMotion
            ? {}
            : {
                initial: { opacity: 0, y: 4 },
                animate: { opacity: 1, y: 0 },
                transition: { duration: 0.18 },
              };

          return (
            <RowWrap
              key={s.slot_key}
              {...rowProps}
              className="rounded-xl border border-white/10 bg-white/[0.03] p-2"
            >
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="text-xs font-semibold text-zinc-100 leading-tight">
                    {s.title}
                  </div>
                  <div className="mt-1 text-[11px] text-zinc-400 line-clamp-2">
                    {s.description}
                  </div>

                  <div className="mt-2 flex items-center gap-2">
                    <span className="text-[10px] px-2 py-0.5 rounded-full border border-white/10 bg-white/5 text-zinc-300">
                      {s.owner_type}
                    </span>

                    {propertyId != null && (
                      <button
                        onClick={() => openEdit(s)}
                        className="text-[10px] px-2 py-0.5 rounded-full border border-white/10 bg-white/5 hover:bg-white/10 text-zinc-200"
                        disabled={busyKey === s.slot_key}
                      >
                        {busyKey === s.slot_key ? "…" : "edit"}
                      </button>
                    )}
                  </div>
                </div>

                <div className="flex flex-col items-end gap-1">
                  <span
                    className={`text-[10px] px-2 py-0.5 rounded-full border ${statusTone(
                      status,
                    )}`}
                  >
                    {status}
                  </span>
                  <span className="text-[10px] text-zinc-400">{assignee}</span>
                </div>
              </div>
            </RowWrap>
          );
        })}
      </div>

      {editing && (
        <div className="mt-3 rounded-xl border border-white/10 bg-black/20 p-3">
          <div className="flex items-center justify-between">
            <div className="text-xs font-semibold text-zinc-100">
              Edit: {editing.slot_key}
            </div>
            <button
              onClick={() => setEditing(null)}
              className="text-[11px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 hover:bg-white/10 text-zinc-200"
            >
              close
            </button>
          </div>

          <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-2">
            <label className="text-[11px] text-zinc-400">
              status
              <select
                className="mt-1 w-full text-sm rounded-lg border border-white/10 bg-white/5 px-2 py-2 text-zinc-100"
                value={editing.status}
                onChange={(e) =>
                  setEditing((x) => (x ? { ...x, status: e.target.value } : x))
                }
              >
                <option value="idle">idle</option>
                <option value="queued">queued</option>
                <option value="in_progress">in_progress</option>
                <option value="blocked">blocked</option>
                <option value="done">done</option>
              </select>
            </label>

            <label className="text-[11px] text-zinc-400">
              assignee
              <input
                className="mt-1 w-full text-sm rounded-lg border border-white/10 bg-white/5 px-2 py-2 text-zinc-100"
                value={editing.assignee}
                onChange={(e) =>
                  setEditing((x) =>
                    x ? { ...x, assignee: e.target.value } : x,
                  )
                }
                placeholder="unassigned | name/email"
              />
            </label>

            <label className="text-[11px] text-zinc-400 md:col-span-2">
              notes
              <textarea
                className="mt-1 w-full text-sm rounded-lg border border-white/10 bg-white/5 px-2 py-2 text-zinc-100"
                value={editing.notes}
                onChange={(e) =>
                  setEditing((x) => (x ? { ...x, notes: e.target.value } : x))
                }
                rows={3}
                placeholder="What’s blocking this slot? What’s next?"
              />
            </label>
          </div>

          <div className="mt-3 flex justify-end gap-2">
            <button
              onClick={() => setEditing(null)}
              className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10 text-zinc-200"
            >
              cancel
            </button>
            <button
              onClick={saveEdit}
              className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/10 hover:bg-white/15 text-zinc-100"
              disabled={busyKey === editing.slot_key}
            >
              {busyKey === editing.slot_key ? "saving…" : "save"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
