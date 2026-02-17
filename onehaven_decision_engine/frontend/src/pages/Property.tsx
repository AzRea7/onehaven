// frontend/src/pages/Property.tsx
import React from "react";
import { api } from "../lib/api";
import { Link } from "react-router-dom";
import PageHero from "../components/PageHero";
import BrickBuilder from "../components/BrickBuilder";

export default function Properties() {
  const [rows, setRows] = React.useState<any[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);

  async function refresh(background = false) {
    try {
      setErr(null);
      if (!background) setLoading(true);
      const out = await api.dashboardProperties({ limit: 50 });
      setRows(Array.isArray(out) ? out : []);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      if (!background) setLoading(false);
    }
  }

  React.useEffect(() => {
    refresh(false);
  }, []);

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Portfolio"
        title="Properties are the substrate."
        subtitle="Each card is a single-pane view into Deal → Underwrite → Compliance → Cash → Equity. Make the machine obvious."
        right={
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="h-[200px] w-[200px] md:h-[230px] md:w-[230px] opacity-95">
              <BrickBuilder className="h-full w-full" />
            </div>
          </div>
        }
        actions={
          <button
            onClick={() => refresh(false)}
            className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
            title="Refresh"
          >
            sync
          </button>
        }
      />

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {err}
        </div>
      )}

      {loading ? (
        <div className="oh-panel p-5">
          <div className="text-sm text-zinc-300">Loading properties…</div>
        </div>
      ) : rows.length === 0 ? (
        <div className="oh-panel p-5">
          <div className="text-sm text-zinc-300">No properties yet.</div>
          <div className="text-xs text-zinc-500 mt-2">
            Import a snapshot or create a property + deal, then this becomes
            your operating dashboard.
          </div>
        </div>
      ) : (
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
                      {r?.cash_flow != null
                        ? `$${Math.round(r.cash_flow)}`
                        : "—"}
                    </div>
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}
