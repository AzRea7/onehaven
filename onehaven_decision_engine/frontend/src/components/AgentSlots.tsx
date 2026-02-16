import React from "react";
import { api } from "../lib/api";
import { motion } from "framer-motion";

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

export default function AgentSlots({ propertyId, propertyOnly }: Props) {
  const [specs, setSpecs] = React.useState<SlotSpec[]>([]);
  const [assignments, setAssignments] = React.useState<SlotAssign[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [busyKey, setBusyKey] = React.useState<string | null>(null);

  const [editing, setEditing] = React.useState<null | {
    slot_key: string;
    status: string;
    assignee: string;
    notes: string;
    owner_type: string;
  }>(null);

  async function refresh() {
    try {
      setErr(null);
      const [s, aGlobal, aProp] = await Promise.all([
        api.slotSpecs(),
        api.slotAssignments(undefined), // global + any property if backend returns all
        propertyId != null
          ? api.slotAssignments(propertyId)
          : Promise.resolve([]),
      ]);

      // Merge; keep both so we can prefer property assignment over global.
      // De-dupe by id.
      const merged = [...(aProp as any[]), ...(aGlobal as any[])].filter(
        Boolean,
      );
      const seen = new Set<number>();
      const dedup = merged.filter((x) => {
        if (!x?.id) return true;
        if (seen.has(x.id)) return false;
        seen.add(x.id);
        return true;
      });

      setSpecs(s);
      setAssignments(dedup as SlotAssign[]);
    } catch (e: any) {
      setErr(String(e.message || e));
    }
  }

  React.useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId]);

  // Choose the “effective” assignment per slot_key:
  // property assignment wins; otherwise global (property_id == null).
  const byKey = new Map<string, SlotAssign>();
  for (const a of assignments) {
    if (!a?.slot_key) continue;
    const existing = byKey.get(a.slot_key);

    const aIsProp = propertyId != null && a.property_id === propertyId;
    const aIsGlobal = a.property_id == null;

    if (!existing) {
      if (propertyOnly && propertyId != null && !aIsProp) continue;
      byKey.set(a.slot_key, a);
      continue;
    }

    const eIsProp = propertyId != null && existing.property_id === propertyId;
    const eIsGlobal = existing.property_id == null;

    // If we already have property assignment, keep it.
    if (eIsProp) continue;

    // Prefer property assignment over global
    if (aIsProp) {
      byKey.set(a.slot_key, a);
      continue;
    }

    // Otherwise keep existing (stable)
    if (!eIsGlobal && aIsGlobal) {
      byKey.set(a.slot_key, a);
    }
  }

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
      setErr(String(e.message || e));
    } finally {
      setBusyKey(null);
    }
  }

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
          >
            sync
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
        {specs.map((s, idx) => {
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

          return (
            <motion.div
              key={s.slot_key}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.25, delay: idx * 0.02 }}
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
            </motion.div>
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
