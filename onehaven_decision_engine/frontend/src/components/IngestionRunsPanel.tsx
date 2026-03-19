import React from "react";
import {
  ArrowRight,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCcw,
} from "lucide-react";
import GlassCard from "./GlassCard";
import Spinner from "./Spinner";
import { ingestionClient, IngestionRun } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  onSelectRun?: (runId: number) => void;
};

function fmt(dt?: string | null) {
  if (!dt) return "—";
  try {
    return new Date(dt).toLocaleString();
  } catch {
    return dt;
  }
}

function statusClass(status: string) {
  const v = String(status || "").toLowerCase();
  if (v === "success") {
    return "border-emerald-400/20 bg-emerald-400/10 text-emerald-100";
  }
  if (v === "failed") {
    return "border-red-400/20 bg-red-400/10 text-red-100";
  }
  if (v === "running" || v === "queued") {
    return "border-sky-400/20 bg-sky-400/10 text-sky-100";
  }
  return "border-white/10 bg-white/5 text-neutral-200";
}

export default function IngestionRunsPanel({ refreshKey, onSelectRun }: Props) {
  const [rows, setRows] = React.useState<IngestionRun[]>([]);
  const [loading, setLoading] = React.useState(true);

  async function load() {
    setLoading(true);
    try {
      setRows(await ingestionClient.listRuns(40));
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
            Recent intake runs
          </h3>
          <p className="mt-1 text-sm text-neutral-400">
            Manual refresh only, so the page stays stable while you work.
          </p>
        </div>
        <button
          onClick={() => load()}
          className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
        >
          <RefreshCcw className="h-4 w-4" />
          Refresh
        </button>
      </div>

      {!rows.length ? (
        <div className="rounded-2xl border border-white/10 bg-white/5 p-5 text-sm text-neutral-300">
          No recent intake runs yet.
        </div>
      ) : (
        <div className="max-h-[760px] space-y-3 overflow-y-auto pr-1">
          {rows.map((row) => (
            <button
              key={row.id}
              onClick={() => onSelectRun?.(row.id)}
              className="w-full rounded-2xl border border-white/10 bg-white/5 p-4 text-left transition hover:bg-white/7"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="font-medium text-white">
                      {row.source_label}
                    </div>
                    <span
                      className={`rounded-full border px-3 py-1 text-xs font-medium ${statusClass(
                        row.status,
                      )}`}
                    >
                      {row.status}
                    </span>
                  </div>
                  <div className="mt-1 text-sm text-neutral-400">
                    {row.provider} · {row.trigger_type}
                  </div>
                </div>

                <div className="flex items-center gap-2 text-neutral-400">
                  <ArrowRight className="h-4 w-4" />
                </div>
              </div>

              <div className="mt-4 grid grid-cols-2 gap-3 text-sm text-neutral-300 md:grid-cols-6">
                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="flex items-center gap-2 text-neutral-500">
                    <Clock3 className="h-3.5 w-3.5" />
                    Started
                  </div>
                  <div className="mt-1">{fmt(row.started_at)}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="flex items-center gap-2 text-neutral-500">
                    <DatabaseZap className="h-3.5 w-3.5" />
                    Imported
                  </div>
                  <div className="mt-1">{row.records_imported}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="text-neutral-500">Created</div>
                  <div className="mt-1">{row.properties_created ?? "—"}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="text-neutral-500">Updated</div>
                  <div className="mt-1">{row.properties_updated ?? "—"}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="text-neutral-500">Duplicates</div>
                  <div className="mt-1">{row.duplicates_skipped}</div>
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                  <div className="text-neutral-500">Invalid</div>
                  <div className="mt-1">{row.invalid_rows}</div>
                </div>
              </div>

              {row.error_summary ? (
                <div className="mt-3 rounded-xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-200">
                  <div className="mb-1 flex items-center gap-2 font-medium">
                    <CircleAlert className="h-4 w-4" />
                    Error summary
                  </div>
                  <div>{row.error_summary}</div>
                </div>
              ) : null}
            </button>
          ))}
        </div>
      )}
    </GlassCard>
  );
}
