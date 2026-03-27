import React from "react";
import {
  Calendar,
  Loader2,
  MapPinned,
  Play,
  RefreshCcw,
  SearchCheck,
  Settings2,
} from "lucide-react";
import GlassCard from "./GlassCard";
import { ingestionClient, type SupportedMarket } from "../lib/ingestionClient";

type Props = {
  market: SupportedMarket | null;
  onRunQueued?: () => void;
  onManageSources?: () => void;
};

function toneForTier(tier?: string | null) {
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
  market,
  onRunQueued,
  onManageSources,
}: Props) {
  const [overviewLastSyncAt, setOverviewLastSyncAt] = React.useState<
    string | null | undefined
  >(null);
  const [syncing, setSyncing] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function loadOverview() {
    try {
      const overview = await ingestionClient.overview();
      setOverviewLastSyncAt(overview?.last_sync_at ?? null);
    } catch {
      setOverviewLastSyncAt(null);
    }
  }

  React.useEffect(() => {
    loadOverview();
  }, []);

  async function handleSync() {
    if (!market?.slug) return;

    setSyncing(true);
    setError(null);

    try {
      await ingestionClient.syncSupportedMarket({
        market_slug: market.slug,
      });
      await loadOverview();
      onRunQueued?.();
    } catch (e: any) {
      const detail = e?.message || "Failed to queue supported-market sync.";
      setError(detail);
    } finally {
      setSyncing(false);
    }
  }

  return (
    <GlassCard className="p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <SearchCheck className="h-3.5 w-3.5" />
            Supported-market sync
          </div>

          <div className="mt-2 flex flex-wrap items-center gap-2">
            <div className="truncate text-base font-semibold text-app-0">
              {market ? marketLabel(market) : "No market selected"}
            </div>
            {market?.coverage_tier ? (
              <span className={toneForTier(market.coverage_tier)}>
                {market.coverage_tier}
              </span>
            ) : null}
          </div>

          <div className="mt-1 text-sm text-app-4">
            {marketSubLabel(market)}
          </div>
        </div>

        <button
          type="button"
          className="oh-btn oh-btn-secondary"
          onClick={() => onManageSources?.()}
          disabled={!market}
        >
          <Settings2 className="h-4 w-4" />
          Sources
        </button>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <MapPinned className="h-3.5 w-3.5" />
            Market
          </div>
          <div className="mt-2 text-sm font-medium text-app-0">
            {market?.slug || "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <Calendar className="h-3.5 w-3.5" />
            Last overall sync
          </div>
          <div className="mt-2 text-sm font-medium text-app-0">
            {relativeTime(overviewLastSyncAt)}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <RefreshCcw className="h-3.5 w-3.5" />
            Mode
          </div>
          <div className="mt-2 text-sm font-medium text-app-0">refresh</div>
        </div>
      </div>

      {error ? (
        <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-3 py-2 text-sm text-red-100">
          {error}
        </div>
      ) : null}

      <div className="mt-4 flex flex-wrap items-center gap-3">
        <button
          type="button"
          className="oh-btn"
          onClick={handleSync}
          disabled={!market || syncing}
        >
          {syncing ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" />
              Queueing…
            </>
          ) : (
            <>
              <Play className="h-4 w-4" />
              Sync now
            </>
          )}
        </button>

        <div className="text-xs text-app-4">
          Refreshes only the selected supported market.
        </div>
      </div>
    </GlassCard>
  );
}
