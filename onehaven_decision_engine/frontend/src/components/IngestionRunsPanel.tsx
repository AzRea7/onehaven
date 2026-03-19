import React from "react";
import {
  ArrowRight,
  CircleAlert,
  Clock3,
  DatabaseZap,
  RefreshCcw,
  Search,
  SlidersHorizontal,
  Workflow,
} from "lucide-react";
import GlassCard from "./GlassCard";
import Spinner from "./Spinner";
import { ingestionClient, IngestionRun } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  onSelectRun?: (runId: number) => void;
};

type RunDetail = {
  id: number;
  source_id: number;
  trigger_type: string;
  status: string;
  started_at: string;
  finished_at?: string | null;
  records_seen: number;
  records_imported: number;
  properties_created: number;
  properties_updated: number;
  deals_created: number;
  deals_updated: number;
  rent_rows_upserted: number;
  photos_upserted: number;
  duplicates_skipped: number;
  invalid_rows: number;
  retry_count: number;
  error_summary?: string | null;
  error_json?: Record<string, any> | null;
  summary_json?: Record<string, any> | null;
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

function compactNumber(value: any) {
  if (value === undefined || value === null || value === "") return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n.toLocaleString();
}

function launchLabel(summary?: Record<string, any> | null) {
  const launch = summary?.launch || {};
  const city = launch.city;
  const county = launch.county;
  const state = launch.state;
  return [city, county, state].filter(Boolean).join(", ") || "Launch config";
}

function zipSummary(summary?: Record<string, any> | null) {
  const launch = summary?.launch || {};
  const zips = Array.isArray(launch.zip_codes) ? launch.zip_codes : [];
  if (!zips.length) return "City-wide";
  if (zips.length <= 3) return zips.join(", ");
  return `${zips.slice(0, 3).join(", ")} +${zips.length - 3}`;
}

function priceBucketSummary(summary?: Record<string, any> | null) {
  const launch = summary?.launch || {};
  const buckets = Array.isArray(launch.price_buckets)
    ? launch.price_buckets
    : [];
  if (!buckets.length) return "Auto / none";
  return buckets
    .map((b: any) =>
      Array.isArray(b) && b.length === 2
        ? `$${compactNumber(b[0])}–$${compactNumber(b[1])}`
        : null,
    )
    .filter(Boolean)
    .join(" · ");
}

function filterReasonSummary(summary?: Record<string, any> | null) {
  const counts = summary?.filter_reason_counts || {};
  const entries = Object.entries(counts)
    .filter(([, value]) => Number(value) > 0)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .slice(0, 3);

  if (!entries.length) return "—";

  return entries
    .map(([key, value]) => `${key}: ${compactNumber(value)}`)
    .join(" · ");
}

export default function IngestionRunsPanel({ refreshKey, onSelectRun }: Props) {
  const [rows, setRows] = React.useState<IngestionRun[]>([]);
  const [details, setDetails] = React.useState<Record<number, RunDetail>>({});
  const [loading, setLoading] = React.useState(true);

  async function load() {
    setLoading(true);
    try {
      const baseRows = await ingestionClient.listRuns(40);
      setRows(baseRows);

      const detailEntries = await Promise.all(
        baseRows.map(async (row) => {
          try {
            const detail = await ingestionClient.runDetail(row.id);
            return [row.id, detail as RunDetail] as const;
          } catch {
            return [row.id, undefined] as const;
          }
        }),
      );

      const nextDetails: Record<number, RunDetail> = {};
      for (const [runId, detail] of detailEntries) {
        if (detail) nextDetails[runId] = detail;
      }
      setDetails(nextDetails);
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
          {rows.map((row) => {
            const detail = details[row.id];
            const summary = detail?.summary_json || {};
            const launch = summary?.launch || {};

            return (
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
                    <div className="mt-2 text-xs text-neutral-500">
                      {launchLabel(summary)}
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

                <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
                  <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-sm text-neutral-300">
                    <div className="mb-2 flex items-center gap-2 text-neutral-500">
                      <Search className="h-3.5 w-3.5" />
                      Search targeting
                    </div>
                    <div className="space-y-1 text-xs">
                      <div>
                        <span className="text-neutral-500">ZIPs:</span>{" "}
                        <span className="text-neutral-200">
                          {zipSummary(summary)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Price buckets:</span>{" "}
                        <span className="text-neutral-200">
                          {priceBucketSummary(summary)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">
                          Pages per shard:
                        </span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(launch.pages_per_shard ?? 1)}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-sm text-neutral-300">
                    <div className="mb-2 flex items-center gap-2 text-neutral-500">
                      <SlidersHorizontal className="h-3.5 w-3.5" />
                      Scan stats
                    </div>
                    <div className="space-y-1 text-xs">
                      <div>
                        <span className="text-neutral-500">Seen:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(
                            summary.records_seen ?? row.records_seen,
                          )}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Filtered:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.filtered_out)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Pages scanned:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.provider_pages_scanned)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">
                          Provider page size:
                        </span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.provider_fetch_limit)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">
                          Top filter misses:
                        </span>{" "}
                        <span className="text-neutral-200">
                          {filterReasonSummary(summary)}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3 text-sm text-neutral-300">
                    <div className="mb-2 flex items-center gap-2 text-neutral-500">
                      <Workflow className="h-3.5 w-3.5" />
                      Post-import pipeline
                    </div>
                    <div className="space-y-1 text-xs">
                      <div>
                        <span className="text-neutral-500">Attempted:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(
                            summary.post_import_pipeline_attempted,
                          )}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Geo:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.geo_enriched)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Rent:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.rent_refreshed)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Evaluated:</span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.evaluated)}
                        </span>
                      </div>
                      <div>
                        <span className="text-neutral-500">
                          Workflow synced:
                        </span>{" "}
                        <span className="text-neutral-200">
                          {compactNumber(summary.workflow_synced)}
                        </span>
                      </div>
                    </div>
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
            );
          })}
        </div>
      )}
    </GlassCard>
  );
}
