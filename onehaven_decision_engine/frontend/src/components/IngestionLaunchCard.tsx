import React from "react";
import {
  Bath,
  BedDouble,
  Building2,
  Calendar,
  Filter,
  MapPinned,
  Play,
  SearchCheck,
} from "lucide-react";
import GlassCard from "./GlassCard";
import { ingestionClient, IngestionSource } from "../lib/ingestionClient";

type Props = {
  refreshKey?: number;
  onQueued?: () => void;
};

const COUNTY_TO_CITIES: Record<string, string[]> = {
  wayne: [
    "Detroit",
    "Dearborn",
    "Dearborn Heights",
    "Inkster",
    "Livonia",
    "Redford",
    "Romulus",
    "Southgate",
    "Taylor",
    "Westland",
    "Wyandotte",
  ],
  oakland: [
    "Pontiac",
    "Southfield",
    "Oak Park",
    "Ferndale",
    "Royal Oak",
    "Madison Heights",
    "Troy",
    "Farmington Hills",
  ],
  macomb: [
    "Warren",
    "Sterling Heights",
    "Clinton Township",
    "Roseville",
    "Eastpointe",
    "St. Clair Shores",
    "Mount Clemens",
  ],
};

const PROPERTY_TYPES = [
  { value: "", label: "Any type" },
  { value: "single_family", label: "Single-family" },
  { value: "multi_family", label: "Multi-family" },
  { value: "condo", label: "Condo" },
  { value: "townhouse", label: "Townhouse" },
];

function normalizeLimitInput(value: string) {
  return value.replace(/[^\d]/g, "").slice(0, 4);
}

function parseLimit(value: string) {
  const parsed = Number(value || 100);
  if (!Number.isFinite(parsed) || parsed <= 0) return 100;
  return Math.round(parsed);
}

function toNumberOrUndefined(value: string) {
  if (!value.trim()) return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function findBestSource(
  rows: IngestionSource[],
  county: string,
  city: string,
): IngestionSource | undefined {
  const enabled = rows.filter((r) => r.is_enabled);

  const normalizedCounty = county.trim().toLowerCase();
  const normalizedCity = city.trim().toLowerCase();

  if (normalizedCounty) {
    const byCounty = enabled.find((row) => {
      const cfg = row.config_json || {};
      return (
        String(cfg.county || "")
          .trim()
          .toLowerCase() === normalizedCounty
      );
    });
    if (byCounty) return byCounty;
  }

  if (normalizedCity) {
    const byCity = enabled.find((row) => {
      const cfg = row.config_json || {};
      return (
        String(cfg.city || "")
          .trim()
          .toLowerCase() === normalizedCity
      );
    });
    if (byCity) return byCity;
  }

  return enabled[0] || rows[0];
}

function statusTone(status?: string) {
  const v = String(status || "").toLowerCase();
  if (v === "connected" || v === "healthy" || v === "ready") {
    return "border-emerald-400/20 bg-emerald-400/10 text-emerald-100";
  }
  if (v === "error" || v === "failed") {
    return "border-red-400/20 bg-red-400/10 text-red-100";
  }
  return "border-amber-400/20 bg-amber-400/10 text-amber-100";
}

export default function IngestionLaunchCard({ refreshKey, onQueued }: Props) {
  const [sources, setSources] = React.useState<IngestionSource[]>([]);
  const [loadingSources, setLoadingSources] = React.useState(true);
  const [submitting, setSubmitting] = React.useState(false);
  const [dailyRefreshing, setDailyRefreshing] = React.useState(false);
  const [message, setMessage] = React.useState<string | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const [state, setState] = React.useState("MI");
  const [county, setCounty] = React.useState("wayne");
  const [city, setCity] = React.useState("Detroit");
  const [minPrice, setMinPrice] = React.useState("");
  const [maxPrice, setMaxPrice] = React.useState("");
  const [minBedrooms, setMinBedrooms] = React.useState("");
  const [minBathrooms, setMinBathrooms] = React.useState("");
  const [propertyType, setPropertyType] = React.useState("");
  const [limit, setLimit] = React.useState("100");

  async function loadSources() {
    setLoadingSources(true);
    try {
      const rows = await ingestionClient.listSources();
      setSources(rows);
      setError(null);
    } catch (err: any) {
      setError(err?.message || "Could not load intake sources");
    } finally {
      setLoadingSources(false);
    }
  }

  React.useEffect(() => {
    loadSources();
  }, [refreshKey]);

  React.useEffect(() => {
    const cityOptions = COUNTY_TO_CITIES[county.toLowerCase()] || [];
    if (cityOptions.length > 0 && !cityOptions.includes(city)) {
      setCity(cityOptions[0] || "");
    }
  }, [county, city]);

  const cityOptions = COUNTY_TO_CITIES[county.toLowerCase()] || [];
  const selectedSource = React.useMemo(
    () => findBestSource(sources, county, city),
    [sources, county, city],
  );

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    setMessage(null);

    try {
      if (!selectedSource?.id) {
        throw new Error("No intake source is ready for this market yet.");
      }

      const payload = {
        trigger_type: "manual",
        state: state.trim() || "MI",
        county: county.trim() || undefined,
        city: city.trim() || undefined,
        min_price: toNumberOrUndefined(minPrice),
        max_price: toNumberOrUndefined(maxPrice),
        min_bedrooms: toNumberOrUndefined(minBedrooms),
        min_bathrooms: toNumberOrUndefined(minBathrooms),
        property_type: propertyType || undefined,
        limit: parseLimit(limit),
      };

      await ingestionClient.syncSource(Number(selectedSource.id), payload);

      setMessage(
        `Queued intake for ${[payload.city, payload.county, payload.state]
          .filter(Boolean)
          .join(", ")}.`,
      );
      onQueued?.();
    } catch (err: any) {
      setError(err?.message || "Failed to queue intake run");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDailyRefresh() {
    setDailyRefreshing(true);
    setError(null);
    setMessage(null);

    try {
      await ingestionClient.queueDailyRefresh();
      setMessage("Queued a full daily market refresh.");
      onQueued?.();
    } catch (err: any) {
      setError(err?.message || "Failed to queue daily refresh");
    } finally {
      setDailyRefreshing(false);
    }
  }

  return (
    <GlassCard className="p-4">
      <form onSubmit={handleSubmit} className="space-y-5">
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1fr_1fr]">
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="mb-3 flex items-center gap-2 text-sm font-medium text-white">
              <MapPinned className="h-4 w-4 text-neutral-300" />
              Intake region
            </div>

            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  State
                </div>
                <input
                  value={state}
                  onChange={(e) => setState(e.target.value.toUpperCase())}
                  maxLength={2}
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                  placeholder="MI"
                />
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  County
                </div>
                <select
                  value={county}
                  onChange={(e) => setCounty(e.target.value)}
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                >
                  <option value="wayne">Wayne County</option>
                  <option value="oakland">Oakland County</option>
                  <option value="macomb">Macomb County</option>
                </select>
              </label>

              <label className="block md:col-span-2">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  City
                </div>
                {cityOptions.length > 0 ? (
                  <select
                    value={city}
                    onChange={(e) => setCity(e.target.value)}
                    disabled={submitting || dailyRefreshing}
                    className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                  >
                    {cityOptions.map((item) => (
                      <option key={item} value={item}>
                        {item}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    value={city}
                    onChange={(e) => setCity(e.target.value)}
                    disabled={submitting || dailyRefreshing}
                    className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                    placeholder="Detroit"
                  />
                )}
              </label>
            </div>
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="mb-3 flex items-center gap-2 text-sm font-medium text-white">
              <Filter className="h-4 w-4 text-neutral-300" />
              Light listing filters
            </div>

            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Min price
                </div>
                <input
                  value={minPrice}
                  onChange={(e) => setMinPrice(e.target.value)}
                  inputMode="numeric"
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                  placeholder="60000"
                />
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Max price
                </div>
                <input
                  value={maxPrice}
                  onChange={(e) => setMaxPrice(e.target.value)}
                  inputMode="numeric"
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                  placeholder="150000"
                />
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Min bedrooms
                </div>
                <div className="relative">
                  <BedDouble className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-400" />
                  <input
                    value={minBedrooms}
                    onChange={(e) => setMinBedrooms(e.target.value)}
                    inputMode="numeric"
                    disabled={submitting || dailyRefreshing}
                    className="w-full rounded-xl border border-white/10 bg-black/20 py-2.5 pl-10 pr-3 text-sm text-white outline-none transition focus:border-white/20"
                    placeholder="2"
                  />
                </div>
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Min bathrooms
                </div>
                <div className="relative">
                  <Bath className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-400" />
                  <input
                    value={minBathrooms}
                    onChange={(e) => setMinBathrooms(e.target.value)}
                    inputMode="decimal"
                    disabled={submitting || dailyRefreshing}
                    className="w-full rounded-xl border border-white/10 bg-black/20 py-2.5 pl-10 pr-3 text-sm text-white outline-none transition focus:border-white/20"
                    placeholder="1"
                  />
                </div>
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Property type
                </div>
                <select
                  value={propertyType}
                  onChange={(e) => setPropertyType(e.target.value)}
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                >
                  {PROPERTY_TYPES.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="block">
                <div className="mb-1.5 text-xs uppercase tracking-[0.12em] text-neutral-400">
                  Results target
                </div>
                <input
                  value={limit}
                  onChange={(e) =>
                    setLimit(normalizeLimitInput(e.target.value))
                  }
                  onBlur={() => setLimit(String(parseLimit(limit)))}
                  inputMode="numeric"
                  disabled={submitting || dailyRefreshing}
                  className="w-full rounded-xl border border-white/10 bg-black/20 px-3 py-2.5 text-sm text-white outline-none transition focus:border-white/20"
                  placeholder="100"
                />
              </label>
            </div>
          </div>
        </div>

        {selectedSource ? (
          <div className="flex flex-wrap items-center gap-2 text-xs text-neutral-300">
            <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">
              Market source: {selectedSource.display_name}
            </span>
            <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">
              Provider: {selectedSource.provider}
            </span>
            <span
              className={`rounded-full border px-3 py-1 ${statusTone(
                selectedSource.status,
              )}`}
            >
              {selectedSource.status}
            </span>
          </div>
        ) : null}

        {message ? (
          <div className="rounded-2xl border border-emerald-400/20 bg-emerald-400/10 px-4 py-3 text-sm text-emerald-100">
            {message}
          </div>
        ) : null}

        {error ? (
          <div className="rounded-2xl border border-red-400/20 bg-red-400/10 px-4 py-3 text-sm text-red-100">
            {error}
          </div>
        ) : null}

        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 pt-4">
          <div className="flex items-center gap-2 text-sm text-neutral-400">
            <SearchCheck className="h-4 w-4" />
            New listings only. Existing external matches are skipped
            automatically.
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={handleDailyRefresh}
              disabled={dailyRefreshing || submitting || loadingSources}
              className="inline-flex h-12 items-center justify-center gap-2 rounded-2xl border border-white/15 bg-white/5 px-5 text-sm font-semibold text-white transition hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <Calendar className="h-4 w-4" />
              {dailyRefreshing ? "Queueing refresh..." : "Daily refresh"}
            </button>

            <button
              type="submit"
              disabled={submitting || loadingSources || !selectedSource?.id}
              className="inline-flex h-12 items-center justify-center gap-2 rounded-2xl border border-emerald-500/30 bg-emerald-500/15 px-5 text-sm font-semibold text-emerald-50 shadow-lg shadow-emerald-900/20 transition hover:border-emerald-400/40 hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <Play className="h-4 w-4" />
              {submitting ? "Queueing intake..." : "Run intake"}
            </button>
          </div>
        </div>
      </form>
    </GlassCard>
  );
}
