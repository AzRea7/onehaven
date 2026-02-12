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
};

export default function AgentSlots() {
  const [specs, setSpecs] = React.useState<SlotSpec[]>([]);
  const [assignments, setAssignments] = React.useState<SlotAssign[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  async function refresh() {
    try {
      setErr(null);
      const [s, a] = await Promise.all([
        api.slotSpecs(),
        api.slotAssignments(),
      ]);
      setSpecs(s);
      setAssignments(a);
    } catch (e: any) {
      setErr(String(e.message || e));
    }
  }

  React.useEffect(() => {
    refresh();
  }, []);

  const byKey = new Map<string, SlotAssign>();
  for (const a of assignments) {
    // keep the most recently updated per slot key (global sidebar)
    if (!byKey.has(a.slot_key)) byKey.set(a.slot_key, a);
  }

  return (
    <div className="gradient-border rounded-2xl glass p-3">
      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold tracking-wide text-zinc-200">
          Agent Slots
        </div>
        <button
          onClick={refresh}
          className="text-[11px] px-2 py-1 rounded-lg border border-white/10 bg-white/5 hover:bg-white/10"
          title="Refresh"
        >
          sync
        </button>
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
    </div>
  );
}
