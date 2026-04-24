import React from "react";
import {
  ArrowRight,
  ChevronDown,
  ChevronUp,
  CircleAlert,
  Clock3,
  DatabaseZap,
  FileSearch,
  RefreshCcw,
  Route,
  ScanSearch,
  Workflow,
  X,
} from "lucide-react";
import GlassCard from "packages/ui/onehaven_onehaven_platform/frontend/src/components/GlassCard";
import Spinner from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Spinner";
import { ingestionClient, IngestionRun } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  open?: boolean;
  onClose?: () => void;
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
  if (v === "success" || v === "completed") {
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

function prettifyMarketSlug(value: any) {
  const raw = String(value || "").trim();
  if (!raw) return "—";
  return raw
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" / ");
}

function compactCursor(summary?: Record<string, any> | null) {
  const cursor = summary?.cursor_advanced_to || {};
  const page = cursor?.page;
  const shard = cursor?.shard;
  const sortMode = cursor?.sort_mode;

  const parts = [];
  if (page != null) parts.push(`page ${page}`);
  if (shard != null) parts.push(`shard ${shard}`);
  if (sortMode) parts.push(String(sortMode));

  return parts.length ? parts.join(" · ") : "—";
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

function pageStatsSummary(summary?: Record<string, any> | null) {
  const rows = Array.isArray(summary?.page_stats) ? summary?.page_stats : [];
  if (!rows.length) return "—";

  return rows
    .slice(0, 3)
    .map((item: any) => {
      const page = item?.page_scanned ?? item?.page_number ?? "—";
      const imported =
        item?.new_listings_imported ??
        item?.new_records_imported ??
        item?.imported ??
        0;
      const raw = item?.raw_count ?? 0;
      return `p${page}: ${compactNumber(imported)} new / ${compactNumber(raw)} seen`;
    })
    .join(" · ");
}

export default function IngestionRunsPanel({
  refreshKey,
  open = true,
  onClose,
  onSelectRun,
}: Props) {
  const [rows, setRows] = React.useState<IngestionRun[]>([]);
  const [details, setDetails] = React.useState<Record<number, RunDetail>>({});
  const [expanded, setExpanded] = React.useState<Record<number, boolean>>({});
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
    if (!open) return;
    load();
  }, [refreshKey, open]);

  if (!open) return null;

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
          <h3 className="text-lg font-semibold text-white">Recent sync runs</h3>
          <p className="mt-1 text-sm text-neutral-400">
            See whether a sync found new inventory or mostly rechecked existing
            pages.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => load()}
            className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
          >
            <RefreshCcw className="h-4 w-4" />
            Refresh
          </button>

          {onClose ? (
            <button
              onClick={onClose}
              className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
            >
              <X className="h-4 w-4" />
              Close
            </button>
          ) : null}
        </div>
      </div>

      {!rows.length ? (
        <div className="rounded-2xl border border-white/10 bg-white/5 p-5 text-sm text-neutral-300">
          No recent sync runs yet.
        </div>
      ) : (
        <div className="max-h-[760px] space-y-3 overflow-y-auto pr-1">
          {rows.map((row) => {
            const detail = details[row.id];
            const summary =
              detail?.summary_json || (row as any)?.summary_json || {};
            const isExpanded = Boolean(expanded[row.id]);

            const marketSlug =
              summary?.market_slug ||
              (row as any)?.market_slug ||
              summary?.cursor_advanced_to?.market_slug;

            const syncMode =
              summary?.sync_mode || (row as any)?.sync_mode || "refresh";

            const newListingsImported =
              summary?.new_listings_imported ??
              summary?.new_records_imported ??
              (row as any)?.new_listings_imported ??
              row.records_imported;

            const alreadySeenSkipped =
              summary?.already_seen_skipped ??
              (row as any)?.already_seen_skipped ??
              0;

            const providerPagesScanned =
              summary?.provider_pages_scanned ??
              (row as any)?.provider_pages_scanned ??
              0;

            const marketExhausted = Boolean(
              summary?.market_exhausted ??
              (row as any)?.market_exhausted ??
              false,
            );

            const cursorAdvancedTo = compactCursor(summary);

            return (
              <div
                key={row.id}
                className="rounded-2xl border border-white/10 bg-white/5 p-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <button
                    onClick={() =>
                      setExpanded((prev) => ({
                        ...prev,
                        [row.id]: !prev[row.id],
                      }))
                    }
                    className="min-w-0 flex-1 text-left"
                  >
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
                      <span className="rounded-full border border-white/10 bg-black/20 px-3 py-1 text-xs text-neutral-300">
                        {syncMode}
                      </span>
                    </div>

                    <div className="mt-1 text-sm text-neutral-400">
                      {row.provider} · {row.trigger_type}
                    </div>

                    <div className="mt-2 flex flex-wrap gap-2 text-xs text-neutral-500">
                      <span>Market: {prettifyMarketSlug(marketSlug)}</span>
                      <span>•</span>
                      <span>Started: {fmt(row.started_at)}</span>
                    </div>
                  </button>

                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => onSelectRun?.(row.id)}
                      className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
                    >
                      <ArrowRight className="h-4 w-4" />
                      Details
                    </button>

                    <button
                      onClick={() =>
                        setExpanded((prev) => ({
                          ...prev,
                          [row.id]: !prev[row.id],
                        }))
                      }
                      className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2 text-sm text-white transition hover:bg-white/5"
                    >
                      {isExpanded ? (
                        <ChevronUp className="h-4 w-4" />
                      ) : (
                        <ChevronDown className="h-4 w-4" />
                      )}
                    </button>
                  </div>
                </div>

                <div className="mt-4 grid grid-cols-2 gap-3 text-sm text-neutral-300 md:grid-cols-6">
                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="flex items-center gap-2 text-neutral-500">
                      <DatabaseZap className="h-3.5 w-3.5" />
                      New listings
                    </div>
                    <div className="mt-1">
                      {compactNumber(newListingsImported)}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="flex items-center gap-2 text-neutral-500">
                      <ScanSearch className="h-3.5 w-3.5" />
                      Already seen
                    </div>
                    <div className="mt-1">
                      {compactNumber(alreadySeenSkipped)}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="flex items-center gap-2 text-neutral-500">
                      <Route className="h-3.5 w-3.5" />
                      Pages scanned
                    </div>
                    <div className="mt-1">
                      {compactNumber(providerPagesScanned)}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="flex items-center gap-2 text-neutral-500">
                      <Workflow className="h-3.5 w-3.5" />
                      Cursor advanced
                    </div>
                    <div className="mt-1">{cursorAdvancedTo}</div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="text-neutral-500">Market exhausted</div>
                    <div className="mt-1">{marketExhausted ? "Yes" : "No"}</div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/20 p-3">
                    <div className="flex items-center gap-2 text-neutral-500">
                      <Clock3 className="h-3.5 w-3.5" />
                      Finished
                    </div>
                    <div className="mt-1">{fmt(row.finished_at)}</div>
                  </div>
                </div>

                {isExpanded ? (
                  <div className="mt-4 grid gap-3 md:grid-cols-2">
                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                      <div className="mb-3 text-sm font-medium text-white">
                        Run summary
                      </div>
                      <div className="space-y-2 text-sm text-neutral-300">
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">Records seen</span>
                          <span>
                            {compactNumber(
                              summary?.records_seen ?? row.records_seen,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">Imported</span>
                          <span>
                            {compactNumber(
                              summary?.records_imported ?? row.records_imported,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">
                            Properties created
                          </span>
                          <span>
                            {compactNumber(
                              summary?.properties_created ??
                                row.properties_created,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">
                            Properties updated
                          </span>
                          <span>
                            {compactNumber(
                              summary?.properties_updated ??
                                row.properties_updated,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">
                            Duplicates skipped
                          </span>
                          <span>
                            {compactNumber(
                              summary?.duplicates_skipped ??
                                row.duplicates_skipped,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">Invalid rows</span>
                          <span>
                            {compactNumber(
                              summary?.invalid_rows ?? row.invalid_rows,
                            )}
                          </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                          <span className="text-neutral-500">Stop reason</span>
                          <span>{summary?.stop_reason || "—"}</span>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                      <div className="mb-3 text-sm font-medium text-white">
                        Scan behavior
                      </div>
                      <div className="space-y-3 text-sm text-neutral-300">
                        <div className="flex items-start gap-2">
                          <FileSearch className="mt-0.5 h-4 w-4 text-neutral-500" />
                          <div>
                            <div className="text-neutral-500">
                              Top page results
                            </div>
                            <div className="mt-1">
                              {pageStatsSummary(summary)}
                            </div>
                          </div>
                        </div>

                        <div className="flex items-start gap-2">
                          <Workflow className="mt-0.5 h-4 w-4 text-neutral-500" />
                          <div>
                            <div className="text-neutral-500">
                              Filter reasons
                            </div>
                            <div className="mt-1">
                              {filterReasonSummary(summary)}
                            </div>
                          </div>
                        </div>

                        {detail?.error_summary ? (
                          <div className="flex items-start gap-2 text-red-200">
                            <CircleAlert className="mt-0.5 h-4 w-4" />
                            <div>
                              <div className="text-neutral-500">Error</div>
                              <div className="mt-1">{detail.error_summary}</div>
                            </div>
                          </div>
                        ) : null}

                        {summary?.post_import_failures ? (
                          <div className="flex items-start gap-2 text-amber-200">
                            <CircleAlert className="mt-0.5 h-4 w-4" />
                            <div>
                              <div className="text-neutral-500">
                                Pipeline issues
                              </div>
                              <div className="mt-1">
                                {compactNumber(summary?.post_import_failures)}{" "}
                                failures
                              </div>
                            </div>
                          </div>
                        ) : null}
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </GlassCard>
  );
}
