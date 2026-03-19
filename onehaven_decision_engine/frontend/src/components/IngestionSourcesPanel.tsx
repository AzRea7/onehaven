import React from "react";
import GlassCard from "./GlassCard";
import Spinner from "./Spinner";
import { ingestionClient, IngestionSource } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  onChanged?: () => void;
};

function fmt(dt?: string | null) {
  if (!dt) return "—";
  try {
    return new Date(dt).toLocaleString();
  } catch {
    return dt;
  }
}

function regionLabel(row: IngestionSource) {
  const cfg = row.config_json || {};
  const parts = [
    cfg.city || null,
    cfg.county || null,
    cfg.state || null,
  ].filter(Boolean);
  return parts.length ? parts.join(", ") : "Default market";
}

export default function IngestionSourcesPanel({ refreshKey }: Props) {
  const [rows, setRows] = React.useState<IngestionSource[]>([]);
  const [loading, setLoading] = React.useState(true);

  async function load() {
    setLoading(true);
    try {
      setRows(await ingestionClient.listSources());
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
  }, [refreshKey]);

  if (loading) {
    return (
      <GlassCard className="p-4">
        <Spinner />
      </GlassCard>
    );
  }

  return (
    <GlassCard className="p-4">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div>
          <h3 className="text-lg font-semibold text-white">
            Daily sync coverage
          </h3>
          <p className="mt-1 text-sm text-neutral-400">
            Default warm markets that keep your property database current.
          </p>
        </div>
        <button
          onClick={() => load()}
          className="rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
        >
          Refresh
        </button>
      </div>

      <div className="space-y-3">
        {rows.map((row) => (
          <div
            key={row.id}
            className="rounded-2xl border border-white/10 bg-white/5 p-4"
          >
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="font-medium text-white">{row.display_name}</div>
                <div className="mt-1 text-sm text-neutral-400">
                  {regionLabel(row)}
                </div>
              </div>

              <span
                className={[
                  "rounded-full border px-3 py-1 text-xs",
                  row.status === "connected"
                    ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
                    : row.status === "error"
                      ? "border-red-400/20 bg-red-400/10 text-red-100"
                      : "border-amber-400/20 bg-amber-400/10 text-amber-100",
                ].join(" ")}
              >
                {row.status}
              </span>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3 text-sm text-neutral-300 md:grid-cols-3">
              <div>
                <div className="text-neutral-500">Last sync</div>
                <div>{fmt(row.last_synced_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Next scheduled run</div>
                <div>{fmt(row.next_scheduled_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Sync cadence</div>
                <div>
                  {row.sync_interval_minutes
                    ? `Every ${row.sync_interval_minutes} min`
                    : "—"}
                </div>
              </div>
            </div>

            {row.last_error_summary ? (
              <div className="mt-3 rounded-xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-200">
                {row.last_error_summary}
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </GlassCard>
  );
}
