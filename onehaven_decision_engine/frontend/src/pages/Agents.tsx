import React from "react";
import { api } from "../lib/api";

export default function Agents() {
  const [agents, setAgents] = React.useState<any[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    api
      .agents()
      .then(setAgents)
      .catch((e) => setErr(String(e.message || e)));
  }, []);

  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold">Agents</div>
        <div className="text-sm text-zinc-400">
          These are workflow slots. Today they’re humans. Later they’re
          cooperating agents using the same operating principles.
        </div>
      </div>

      {err && (
        <div className="p-3 rounded-lg bg-red-950/40 border border-red-800 text-red-200">
          {err}
        </div>
      )}

      <div className="grid grid-cols-2 gap-3">
        {agents.map((a) => (
          <div
            key={a.key}
            className="p-4 rounded-xl border border-zinc-800 bg-zinc-900/30"
          >
            <div className="font-semibold">{a.name}</div>
            <div className="text-xs text-zinc-400">{a.key}</div>
            <div className="mt-2 text-sm text-zinc-300">{a.description}</div>
            <div className="mt-3 text-xs text-zinc-400">
              default payload schema: {JSON.stringify(a.default_payload_schema)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
