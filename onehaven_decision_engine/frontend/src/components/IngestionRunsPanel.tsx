import React from "react";
import GlassCard from "./GlassCard";
import Spinner  from "./Spinner";
import { IngestionRun, api } from "../lib/api";

type Props = {
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

export default function IngestionRunsPanel({ onSelectRun }: Props) {
  const [rows, setRows] = React.useState<IngestionRun[]>([]);
  const [loading, setLoading] = React.useState(true);

  async function load() {
    setLoading(true);
    try {
      setRows(await api.listIngestionRuns());
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
  }, []);

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
        <h3 className="text-lg font-semibold">Recent Ingestion Runs</h3>
        <span className="text-sm text-neutral-400">{rows.length} runs</span>
      </div>

      <div className="space-y-3">
        {rows.map((row) => (
          <button
            key={row.id}
            onClick={() => onSelectRun?.(row.id)}
            className="w-full rounded-2xl border border-white/10 bg-white/5 p-4 text-left"
          >
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="font-medium">{row.source_label}</div>
                <div className="text-sm text-neutral-400">
                  {row.provider} · {row.trigger_type}
                </div>
              </div>
              <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                {row.status}
              </span>
            </div>

            <div className="mt-3 grid grid-cols-2 gap-3 text-sm md:grid-cols-5">
              <div>
                <div className="text-neutral-500">Started</div>
                <div>{fmt(row.started_at)}</div>
              </div>
              <div>
                <div className="text-neutral-500">Imported</div>
                <div>{row.records_imported}</div>
              </div>
              <div>
                <div className="text-neutral-500">Seen</div>
                <div>{row.records_seen}</div>
              </div>
              <div>
                <div className="text-neutral-500">Duplicates</div>
                <div>{row.duplicates_skipped}</div>
              </div>
              <div>
                <div className="text-neutral-500">Invalid</div>
                <div>{row.invalid_rows}</div>
              </div>
            </div>

            {row.error_summary ? (
              <div className="mt-3 text-sm text-red-300">
                {row.error_summary}
              </div>
            ) : null}
          </button>
        ))}
      </div>
    </GlassCard>
  );
}
