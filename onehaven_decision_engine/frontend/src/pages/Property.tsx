import React from "react";
import { api } from "../lib/api";
import { Link } from "react-router-dom";

export default function Properties() {
  const [rows, setRows] = React.useState<any[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    api
      .dashboardProperties({ limit: 50 })
      .then(setRows)
      .catch((e) => setErr(String(e.message || e)));
  }, []);

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <div className="text-2xl font-semibold tracking-tight">
            Properties
          </div>
          <div className="text-sm text-zinc-400 mt-1">
            “One source of truth” views (Deal → Compliance → Cash → Equity).
          </div>
        </div>
      </div>

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {err}
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {rows.map((v) => {
          const p = v.property;
          const d = v.deal;
          const r = v.last_underwriting_result;
          return (
            <Link
              key={p.id}
              to={`/properties/${p.id}`}
              className="oh-panel p-5 hover:border-white/20 transition"
            >
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-lg font-semibold tracking-tight">
                    {p.address}
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    {p.city}, {p.state} {p.zip} · {p.bedrooms}bd
                  </div>
                </div>
                <span className="oh-badge">
                  {(d?.strategy || "section8").toUpperCase()}
                </span>
              </div>

              <div className="mt-4 grid grid-cols-3 gap-2 text-sm">
                <div className="p-3 rounded-xl border border-white/10 bg-black/20">
                  <div className="text-xs text-zinc-500">Decision</div>
                  <div className="font-semibold">{r?.decision ?? "—"}</div>
                </div>
                <div className="p-3 rounded-xl border border-white/10 bg-black/20">
                  <div className="text-xs text-zinc-500">DSCR</div>
                  <div className="font-semibold">
                    {r?.dscr?.toFixed?.(2) ?? "—"}
                  </div>
                </div>
                <div className="p-3 rounded-xl border border-white/10 bg-black/20">
                  <div className="text-xs text-zinc-500">Cash Flow</div>
                  <div className="font-semibold">
                    {r?.cash_flow != null ? `$${Math.round(r.cash_flow)}` : "—"}
                  </div>
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
