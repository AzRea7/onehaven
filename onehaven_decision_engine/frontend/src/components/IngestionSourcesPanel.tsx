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
  return parts.length ? parts.join(", ") : "No default market";
}

export default function IngestionSourcesPanel({
  refreshKey,
  onChanged,
}: Props) {
  const [rows, setRows] = React.useState<IngestionSource[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [busyId, setBusyId] = React.useState<number | null>(null);

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

  React.useEffect(() => {
    const id = window.setInterval(() => {
      load().catch(() => undefined);
    }, 10000);
    return () => window.clearInterval(id);
  }, [refreshKey]);

  async function onToggle(row: IngestionSource) {
    setBusyId(row.id);
    try {
      await ingestionClient.updateSource(row.id, {
        is_enabled: !row.is_enabled,
      });
      await load();
      onChanged?.();
    } finally {
      setBusyId(null);
    }
  }

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
          <h3 className="text-lg font-semibold text-white">Ingestion source</h3>
          <p className="mt-1 text-sm text-neutral-400">
            Source health and defaults.
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
                  {row.provider} · {row.source_type} · {row.slug}
                </div>

                <div className="mt-3 flex flex-wrap gap-2 text-xs text-neutral-300">
                  <span className="rounded-full border border-white/10 bg-black/20 px-3 py-1">
                    Default market: {regionLabel(row)}
                  </span>
                  <span
                    className={[
                      "rounded-full border px-3 py-1",
                      row.status === "connected"
                        ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
                        : "border-amber-400/20 bg-amber-400/10 text-amber-100",
                    ].join(" ")}
                  >
                    {row.status}
                  </span>
                </div>
              </div>

              <button
                className="rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
                onClick={() => onToggle(row)}
                disabled={busyId === row.id}
              >
                {row.is_enabled ? "Disable" : "Enable"}
              </button>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3 text-sm text-neutral-300 md:grid-cols-4">
              <div>
                <div className="text-neutral-500">Last sync</div>
                <div>{fmt(row.last_synced_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Last success</div>
                <div>{fmt(row.last_success_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Last failure</div>
                <div>{fmt(row.last_failure_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Next scheduled run</div>
                <div>{fmt(row.next_scheduled_at)}</div>
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
