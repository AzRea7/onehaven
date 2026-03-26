import React from "react";
import {
  Calendar,
  Loader2,
  MapPinned,
  Play,
  RefreshCcw,
  SearchCheck,
} from "lucide-react";
import GlassCard from "./GlassCard";
import { ingestionClient, type SupportedMarket } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  selectedMarketSlug?: string | null;
  onMarketChange?: (market: SupportedMarket | null) => void;
  onQueued?: () => void;
};

function toneForTier(tier?: string) {
  const value = String(tier || "").toLowerCase();
  if (value === "hot") return "oh-pill oh-pill-good";
  if (value === "warm") return "oh-pill oh-pill-warn";
  return "oh-pill";
}

function marketLabel(market: SupportedMarket) {
  return market.label || market.city || market.slug;
}

function marketSubLabel(market?: SupportedMarket | null) {
  if (!market) return "Select a supported market";
  return [market.city, market.county, market.state].filter(Boolean).join(" • ");
}

function relativeTime(raw?: string | null) {
  if (!raw) return "Never";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return "Unknown";

  const diffMs = Date.now() - date.getTime();
  const mins = Math.floor(diffMs / 60000);
  const hours = Math.floor(diffMs / 3600000);
  const days = Math.floor(diffMs / 86400000);

  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  if (hours < 24) return `${hours}h ago`;
  return `${days}d ago`;
}

export default function IngestionLaunchCard({
  refreshKey,
  selectedMarketSlug,
  onMarketChange,
  onQueued,
}: Props) {
  const [markets, setMarkets] = React.useState<SupportedMarket[]>([]);
  const [selectedSlug, setSelectedSlug] = React.useState<string>(
    selectedMarketSlug || "",
  );
  const [overviewLastSyncAt, setOverviewLastSyncAt] = React.useState<
    string | null | undefined
  >(null);

  const [loading, setLoading] = React.useState(true);
  const [syncing, setSyncing] = React.useState(false);
  const [refreshingOverview, setRefreshingOverview] = React.useState(false);

  const [message, setMessage] = React.useState<string | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const selectedMarket = React.useMemo(
    () => markets.find((m) => m.slug === selectedSlug) || null,
    [markets, selectedSlug],
  );

  const load = React.useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const [marketRows, overview] = await Promise.all([
        ingestionClient.listSupportedMarkets(),
        ingestionClient.overview(),
      ]);

      setMarkets(marketRows || []);
      setOverviewLastSyncAt(overview?.last_sync_at);

      const nextSlug =
        selectedMarketSlug || selectedSlug || marketRows?.[0]?.slug || "";

      setSelectedSlug(nextSlug);

      const nextMarket =
        marketRows.find((m) => m.slug === nextSlug) || marketRows?.[0] || null;

      if ((nextMarket?.slug || null) !== (selectedMarketSlug || null)) {
        onMarketChange?.(nextMarket);
      }
    } catch (err: any) {
      setError(err?.message || "Could not load supported markets.");
    } finally {
      setLoading(false);
    }
  }, [selectedMarketSlug, selectedSlug, onMarketChange]);

  React.useEffect(() => {
    load();
  }, [refreshKey, selectedMarketSlug]);

  React.useEffect(() => {
    if (!selectedMarketSlug) return;
    setSelectedSlug(selectedMarketSlug);
  }, [selectedMarketSlug]);

  async function handleSync() {
    if (!selectedMarket?.slug) {
      setError("Select a supported market first.");
      return;
    }

    setSyncing(true);
    setError(null);
    setMessage(null);

    try {
      const res = await ingestionClient.syncMarket({
        market_slug: selectedMarket.slug,
      });

      if (!res?.covered) {
        throw new Error("That market is not in supported coverage.");
      }

      setMessage(
        `Sync queued for ${marketLabel(selectedMarket)}${
          typeof res?.queued_count === "number"
            ? ` (${res.queued_count} source${res.queued_count === 1 ? "" : "s"})`
            : ""
        }.`,
      );

      onQueued?.();

      setRefreshingOverview(true);
      try {
        const overview = await ingestionClient.overview();
        setOverviewLastSyncAt(overview?.last_sync_at);
      } finally {
        setRefreshingOverview(false);
      }
    } catch (err: any) {
      setError(err?.message || "Failed to queue supported-market sync.");
    } finally {
      setSyncing(false);
    }
  }

  function handleSelectChange(nextSlug: string) {
    setSelectedSlug(nextSlug);
    const nextMarket = markets.find((m) => m.slug === nextSlug) || null;
    onMarketChange?.(nextMarket);
  }

  return (
    <GlassCard className="p-4">
      <div className="space-y-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-app-0">
              Supported market sync
            </div>
            <div className="mt-1 text-sm text-app-4">
              Search your own covered inventory first. “Sync now” only refreshes
              the currently selected supported market.
            </div>
          </div>

          <button
            type="button"
            onClick={load}
            disabled={loading}
            className="oh-btn oh-btn-secondary"
          >
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCcw className="h-4 w-4" />
            )}
            Reload
          </button>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.4fr_1fr_1fr]">
          <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
              <MapPinned className="h-3.5 w-3.5" />
              Supported market
            </div>

            <select
              value={selectedSlug}
              onChange={(e) => handleSelectChange(e.target.value)}
              className="w-full bg-transparent text-sm text-app-0 outline-none"
              disabled={loading || syncing}
            >
              {markets.length === 0 ? (
                <option value="">No supported markets</option>
              ) : null}
              {markets.map((market) => (
                <option key={market.slug} value={market.slug}>
                  {marketLabel(market)}
                </option>
              ))}
            </select>

            <div className="mt-2 text-xs text-app-4">
              {marketSubLabel(selectedMarket)}
            </div>
          </label>

          <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
              <SearchCheck className="h-3.5 w-3.5" />
              Coverage
            </div>
            <div className="flex flex-wrap gap-2">
              <span className={toneForTier(selectedMarket?.coverage_tier)}>
                {selectedMarket?.coverage_tier || "unknown"} tier
              </span>
              {selectedMarket?.max_price ? (
                <span className="oh-pill">
                  max price ${Number(selectedMarket.max_price).toLocaleString()}
                </span>
              ) : null}
              {selectedMarket?.property_types?.length ? (
                <span className="oh-pill">
                  {selectedMarket.property_types.join(", ")}
                </span>
              ) : null}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
              <Calendar className="h-3.5 w-3.5" />
              Inventory freshness
            </div>
            <div className="text-sm text-app-0">
              Last platform sync: {relativeTime(overviewLastSyncAt)}
            </div>
            <div className="mt-1 text-xs text-app-4">
              {refreshingOverview
                ? "Refreshing freshness..."
                : "Based on ingestion overview."}
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={handleSync}
            disabled={loading || syncing || !selectedMarket}
            className="oh-btn"
          >
            {syncing ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            Sync now
          </button>

          {selectedMarket?.sync_limit ? (
            <span className="text-xs text-app-4">
              bounded refresh limit: {selectedMarket.sync_limit} listings
            </span>
          ) : null}
        </div>

        {message ? (
          <div className="rounded-2xl border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-200">
            {message}
          </div>
        ) : null}

        {error ? (
          <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-200">
            {error}
          </div>
        ) : null}
      </div>
    </GlassCard>
  );
}
