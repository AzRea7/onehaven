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
    <div className="space-y-5">
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
