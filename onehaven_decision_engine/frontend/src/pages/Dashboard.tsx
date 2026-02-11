import React from "react";
import { api } from "../lib/api";
import { Link } from "react-router-dom";

export default function Dashboard() {
  const [rows, setRows] = React.useState<any[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    api
      .dashboard()
      .then(setRows)
      .catch((e) => setErr(String(e.message || e)));
  }, []);

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between">
        <div>
          <div className="text-xl font-semibold">Dashboard</div>
          <div className="text-sm text-zinc-400">
            Property “one screen truth” list
          </div>
        </div>
      </div>

      {err && (
        <div className="p-3 rounded-lg bg-red-950/40 border border-red-800 text-red-200">
          {err}
        </div>
      )}

      <div className="grid grid-cols-1 gap-3">
        {rows.map((v, idx) => (
          <Link
            key={idx}
            to={`/property/${v.property?.id}`}
            className="p-4 rounded-xl border border-zinc-800 bg-zinc-900/30 hover:bg-zinc-900/50 transition"
          >
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="font-medium">{v.property?.address}</div>
                <div className="text-xs text-zinc-400">
                  {v.property?.city}, {v.property?.state} {v.property?.zip} •{" "}
                  {v.property?.bedrooms}bd
                </div>
              </div>
              <div className="text-right">
                <div className="text-xs text-zinc-400">Decision</div>
                <div className="font-semibold">
                  {v.last_underwriting_result?.decision ?? "—"}
                </div>
              </div>
            </div>
            <div className="mt-3 grid grid-cols-4 gap-2 text-xs text-zinc-300">
              <div>
                DSCR: {v.last_underwriting_result?.dscr?.toFixed?.(2) ?? "—"}
              </div>
              <div>
                Cashflow:{" "}
                {v.last_underwriting_result?.cash_flow?.toFixed?.(0) ?? "—"}
              </div>
              <div>Rent Used: {v.rent_explain?.rent_used ?? "—"}</div>
              <div>Friction: {v.jurisdiction_friction?.multiplier ?? "—"}</div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
