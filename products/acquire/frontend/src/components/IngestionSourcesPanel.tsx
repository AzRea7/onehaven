import React from "react";
import {
  CalendarClock,
  Database,
  RefreshCcw,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import GlassCard from "packages/ui/onehaven_onehaven_platform/frontend/src/components/GlassCard";
import Spinner from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Spinner";
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

function cadenceLabel(minutes?: number | null) {
  if (!minutes || !Number.isFinite(Number(minutes))) return "—";
  const n = Number(minutes);
  if (n < 60) return `Every ${n} min`;
  if (n % 60 === 0) return `Every ${n / 60} hr`;
  return `Every ${n} min`;
}

function statusTone(status?: string) {
  const v = String(status || "").toLowerCase();
  if (v === "connected" || v === "healthy" || v === "ready") {
    return "border-emerald-400/20 bg-emerald-400/10 text-emerald-100";
  }
  if (v === "error" || v === "failed") {
    return "border-red-400/20 bg-red-400/10 text-red-100";
  }
  if (v === "paused" || v === "warning") {
    return "border-amber-400/20 bg-amber-400/10 text-amber-100";
  }
  return "border-white/10 bg-white/5 text-neutral-200";
}

export default function IngestionSourcesPanel({
  refreshKey,
  onChanged,
}: Props) {
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

  function handleRefresh() {
    load();
    onChanged?.();
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
          <h3 className="text-lg font-semibold text-white">
            Daily sync coverage
          </h3>
          <p className="mt-1 text-sm text-neutral-400">
            This is a monitoring panel, not part of the main investor intake
            flow. It exists to show warm-market coverage and source health.
          </p>
        </div>
        <button
          onClick={handleRefresh}
          className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
        >
          <RefreshCcw className="h-4 w-4" />
          Refresh
        </button>
      </div>

      {!rows.length ? (
        <div className="rounded-2xl border border-white/10 bg-white/5 p-5 text-sm text-neutral-300">
          No ingestion sources are configured yet.
        </div>
      ) : (
        <div className="space-y-3">
          {rows.map((row) => (
            <div
              key={row.id}
              className="rounded-2xl border border-white/10 bg-white/5 p-4"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="font-medium text-white">
                      {row.display_name}
                    </div>
                    <span
                      className={`rounded-full border px-3 py-1 text-xs ${statusTone(
                        row.status,
                      )}`}
                    >
                      {row.status}
                    </span>
                  </div>

                  <div className="mt-1 text-sm text-neutral-400">
                    {regionLabel(row)}
                  </div>

                  <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-neutral-400">
                    <span className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-black/20 px-2.5 py-1">
                      <Database className="h-3.5 w-3.5" />
                      {row.provider}
                    </span>
                    <span className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-black/20 px-2.5 py-1">
                      <ShieldCheck className="h-3.5 w-3.5" />
                      {row.source_type}
                    </span>
                  </div>
                </div>
              </div>

              <div className="mt-4 grid grid-cols-1 gap-3 text-sm text-neutral-300 md:grid-cols-3">
                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="flex items-center gap-2 text-neutral-500">
                    <RefreshCcw className="h-3.5 w-3.5" />
                    Last sync
                  </div>
                  <div className="mt-1">{fmt(row.last_synced_at)}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="flex items-center gap-2 text-neutral-500">
                    <CalendarClock className="h-3.5 w-3.5" />
                    Next scheduled run
                  </div>
                  <div className="mt-1">{fmt(row.next_scheduled_at)}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="text-neutral-500">Sync cadence</div>
                  <div className="mt-1">
                    {cadenceLabel(row.sync_interval_minutes)}
                  </div>
                </div>
              </div>

              {row.last_error_summary ? (
                <div className="mt-3 rounded-xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-200">
                  <div className="mb-1 flex items-center gap-2 font-medium">
                    <TriangleAlert className="h-4 w-4" />
                    Last error
                  </div>
                  <div>{row.last_error_summary}</div>
                </div>
              ) : null}
            </div>
          ))}
        </div>
      )}
    </GlassCard>
  );
}
