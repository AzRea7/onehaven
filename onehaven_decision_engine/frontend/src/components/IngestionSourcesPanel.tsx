import React from "react";
import  GlassCard  from "./GlassCard";
import  Spinner  from "./Spinner";
import {
  IngestionSource, api } from "../lib/api";

function fmt(dt?: string | null) {
  if (!dt) return "—";
  try {
    return new Date(dt).toLocaleString();
  } catch {
    return dt;
  }
}

export default function IngestionSourcesPanel() {
  const [rows, setRows] = React.useState<IngestionSource[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [busyId, setBusyId] = React.useState<number | null>(null);

  async function load() {
    setLoading(true);
    try {
      setRows(await api.listIngestionSources());
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
  }, []);

  async function onSync(id: number) {
    setBusyId(id);
    try {
      await api.syncIngestionSource(id);
      await load();
    } finally {
      setBusyId(null);
    }
  }

  async function onToggle(row: IngestionSource) {
    setBusyId(row.id);
    try {
      await api.updateIngestionSource(row.id, { is_enabled: !row.is_enabled });
      await load();
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
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-semibold">Ingestion Sources</h3>
        <span className="text-sm text-neutral-400">
          {rows.length} configured
        </span>
      </div>

      <div className="space-y-3">
        {rows.map((row) => (
          <div
            key={row.id}
            className="rounded-2xl border border-white/10 bg-white/5 p-4"
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="font-medium">{row.display_name}</div>
                <div className="text-sm text-neutral-400">
                  {row.provider} · {row.source_type} · {row.slug}
                </div>
              </div>

              <div className="flex items-center gap-2">
                <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                  {row.status}
                </span>
                <button
                  className="rounded-xl border border-white/10 px-3 py-2 text-sm"
                  onClick={() => onToggle(row)}
                  disabled={busyId === row.id}
                >
                  {row.is_enabled ? "Disable" : "Enable"}
                </button>
                <button
                  className="rounded-xl bg-white/10 px-3 py-2 text-sm"
                  onClick={() => onSync(row.id)}
                  disabled={busyId === row.id}
                >
                  {busyId === row.id ? "Syncing..." : "Sync now"}
                </button>
              </div>
            </div>

            <div className="mt-3 grid grid-cols-1 gap-3 text-sm text-neutral-300 md:grid-cols-4">
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
