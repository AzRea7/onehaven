import React from "react";
import { Link } from "react-router-dom";
import {
  Search,
  RefreshCcw,
  SlidersHorizontal,
  MapPin,
  BedDouble,
  Bath,
  Ruler,
  Wallet,
  Banknote,
  Landmark,
  ShieldAlert,
  ArrowUpRight,
  ImageOff,
  Loader2,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api, type SupportedMarket } from "../lib/api";

type Row = any;
type MarketRow = SupportedMarket;

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR" | "UNKNOWN";
type CompletenessFilter = "ALL" | "COMPLETE" | "PARTIAL" | "MISSING";
type SortKey =
  | "BEST_CASHFLOW"
  | "LOWEST_PRICE"
  | "HIGHEST_PRICE"
  | "BEST_DSCR"
  | "NEWEST";

const INITIAL_LIMIT = 1000;
const PAGE_SIZE = 25;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function numberOrNull(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeDecision(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toUpperCase();

  if (["PASS", "GOOD_DEAL", "GOOD", "APPROVED", "APPROVE"].includes(x)) {
    return "GOOD_DEAL";
  }
  if (["REJECT", "FAIL", "FAILED", "NO_GO"].includes(x)) {
    return "REJECT";
  }
  return "REVIEW";
}

function decisionPillClass(raw?: string) {
  const d = normalizeDecision(raw);
  if (d === "GOOD_DEAL") return "oh-pill oh-pill-good";
  if (d === "REVIEW") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function resolvePropertyId(r: any) {
  const candidates = [
    r?.id,
    r?.property_id,
    r?.property?.id,
    r?.propertyId,
    r?.property?.property_id,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }

  return null;
}

function inferProperty(r: any) {
  return r?.property || r || {};
}

function inferAskingPrice(r: any) {
  return (
    numberOrNull(r?.asking_price) ??
    numberOrNull(r?.deal?.asking_price) ??
    numberOrNull(r?.deal?.price) ??
    numberOrNull(r?.property?.price) ??
    null
  );
}

function inferMarketRent(r: any) {
  return (
    numberOrNull(r?.market_rent_estimate) ??
    numberOrNull(r?.rent_assumption?.market_rent_estimate) ??
    numberOrNull(r?.monthly_rent_estimate) ??
    numberOrNull(r?.estimated_rent) ??
    null
  );
}

function inferMortgage(r: any) {
  return (
    numberOrNull(r?.estimated_mortgage) ??
    numberOrNull(r?.mortgage_estimate) ??
    numberOrNull(r?.monthly_mortgage_payment) ??
    numberOrNull(r?.last_underwriting_result?.monthly_debt_service) ??
    numberOrNull(r?.last_underwriting_result?.mortgage_payment) ??
    null
  );
}

function inferCashflow(r: any) {
  const direct =
    r?.cashflow_estimate ??
    r?.projected_monthly_cashflow ??
    r?.last_underwriting_result?.cash_flow ??
    r?.last_underwriting_result?.cashflow ??
    r?.property_net_cash_window ??
    r?.metrics?.cashflow_estimate;

  const n = numberOrNull(direct);
  if (n != null) return n;

  const rent = inferMarketRent(r) ?? 0;
  const mortgage = inferMortgage(r) ?? 0;
  const taxes = numberOrNull(r?.monthly_tax_estimate) ?? 0;
  const insurance = numberOrNull(r?.monthly_insurance_estimate) ?? 0;

  if (rent > 0) return rent - mortgage - taxes - insurance;
  return null;
}

function inferDscr(r: any) {
  return (
    numberOrNull(r?.dscr) ??
    numberOrNull(r?.last_underwriting_result?.dscr) ??
    null
  );
}

function inferCrime(r: any) {
  return (
    numberOrNull(r?.crime_score) ??
    numberOrNull(r?.property?.crime_score) ??
    null
  );
}

function inferCounty(r: any) {
  return r?.county || r?.property?.county || null;
}

function inferLocationConfidence(r: any) {
  return (
    numberOrNull(r?.geocode_confidence) ??
    numberOrNull(r?.property?.geocode_confidence) ??
    null
  );
}

function inferNormalizedAddress(r: any) {
  return r?.normalized_address || r?.property?.normalized_address || null;
}

function inferLat(r: any) {
  return numberOrNull(r?.lat) ?? numberOrNull(r?.property?.lat) ?? null;
}

function inferLng(r: any) {
  return numberOrNull(r?.lng) ?? numberOrNull(r?.property?.lng) ?? null;
}

function getFinancingType(price?: number | null) {
  if (price == null || !Number.isFinite(Number(price))) return "Unknown";
  if (Number(price) < 75000) return "Cash";
  return "DSCR";
}

function inferCompleteness(r: any): "COMPLETE" | "PARTIAL" | "MISSING" {
  const price = inferAskingPrice(r);
  const rent = inferMarketRent(r);
  const cashflow = inferCashflow(r);
  const dscr = inferDscr(r);
  const normalizedAddress = inferNormalizedAddress(r);
  const lat = inferLat(r);
  const lng = inferLng(r);

  const strong =
    price != null &&
    rent != null &&
    cashflow != null &&
    dscr != null &&
    normalizedAddress &&
    lat != null &&
    lng != null;

  if (strong) return "COMPLETE";

  const partialSignals = [
    price != null,
    rent != null,
    cashflow != null,
    dscr != null,
    Boolean(normalizedAddress),
    lat != null && lng != null,
  ].filter(Boolean).length;

  if (partialSignals >= 3) return "PARTIAL";
  return "MISSING";
}

function completenessPillClass(v: "COMPLETE" | "PARTIAL" | "MISSING") {
  if (v === "COMPLETE") return "oh-pill oh-pill-good";
  if (v === "PARTIAL") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function completenessLabel(v: "COMPLETE" | "PARTIAL" | "MISSING") {
  if (v === "COMPLETE") return "Enriched";
  if (v === "PARTIAL") return "Partial";
  return "Missing data";
}

function inferPhotoUrl(r: any) {
  const candidates = [
    r?.photo_url,
    r?.thumbnail_url,
    r?.cover_photo_url,
    r?.hero_photo_url,
    r?.property?.photo_url,
    r?.property?.thumbnail_url,
    r?.property?.cover_photo_url,
    r?.property?.hero_photo_url,
    Array.isArray(r?.photo_urls) ? r.photo_urls[0] : null,
    Array.isArray(r?.property?.photo_urls) ? r.property.photo_urls[0] : null,
    Array.isArray(r?.photos) ? r.photos[0] : null,
    Array.isArray(r?.property?.photos) ? r.property.photos[0] : null,
  ];

  for (const value of candidates) {
    if (typeof value === "string" && value.trim()) return value;
    if (value && typeof value?.url === "string" && value.url.trim()) {
      return value.url;
    }
  }

  return null;
}

function metricTone(value: number | null | undefined) {
  if (value == null) return "text-app-0";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-red-300";
  return "text-app-0";
}

function safeDate(raw: any) {
  const d = raw ? new Date(raw) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
}

function inferUpdatedAt(r: any) {
  return (
    r?.updated_at ||
    r?.last_synced_at ||
    r?.last_enriched_at ||
    r?.created_at ||
    r?.property?.updated_at ||
    null
  );
}

function relativeTime(raw: any) {
  const date = safeDate(raw);
  if (!date) return "Unknown";

  const diffMs = Date.now() - date.getTime();
  const mins = Math.floor(diffMs / 60000);
  const hours = Math.floor(diffMs / 3600000);
  const days = Math.floor(diffMs / 86400000);

  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  if (hours < 24) return `${hours}h ago`;
  return `${days}d ago`;
}

function inferTags(r: any): string[] {
  const tags = new Set<string>();

  const financing = getFinancingType(inferAskingPrice(r));
  const cashflow = inferCashflow(r);
  const dscr = inferDscr(r);
  const completeness = inferCompleteness(r);
  const crime = inferCrime(r);

  if (
    normalizeDecision(r?.normalized_decision || r?.classification) ===
    "GOOD_DEAL"
  ) {
    tags.add("Good deal");
  }
  if (financing === "Cash") tags.add("Cash");
  if (financing === "DSCR") tags.add("DSCR");
  if (cashflow != null && cashflow > 0) tags.add("Cash flow positive");
  if (dscr != null && dscr >= 1.2) tags.add("Strong DSCR");
  if (completeness === "COMPLETE") tags.add("Fully enriched");
  if (crime != null && crime <= 30) tags.add("Lower crime");

  const p = inferProperty(r);
  if (p?.bedrooms != null && Number(p.bedrooms) >= 3) tags.add("3+ beds");
  if (inferCounty(r)) tags.add(String(inferCounty(r)));

  return Array.from(tags).slice(0, 5);
}

function sortRows(rows: Row[], sort: SortKey) {
  const copy = [...rows];

  copy.sort((a, b) => {
    if (sort === "BEST_CASHFLOW") {
      return (inferCashflow(b) ?? -Infinity) - (inferCashflow(a) ?? -Infinity);
    }
    if (sort === "LOWEST_PRICE") {
      return (
        (inferAskingPrice(a) ?? Infinity) - (inferAskingPrice(b) ?? Infinity)
      );
    }
    if (sort === "HIGHEST_PRICE") {
      return (
        (inferAskingPrice(b) ?? -Infinity) - (inferAskingPrice(a) ?? -Infinity)
      );
    }
    if (sort === "BEST_DSCR") {
      return (inferDscr(b) ?? -Infinity) - (inferDscr(a) ?? -Infinity);
    }

    return (
      (safeDate(inferUpdatedAt(b))?.getTime() ?? 0) -
      (safeDate(inferUpdatedAt(a))?.getTime() ?? 0)
    );
  });

  return copy;
}

function Photo({ url, alt }: { url: string | null; alt: string }) {
  const [failed, setFailed] = React.useState(false);

  if (!url || failed) {
    return (
      <div className="flex h-full w-full items-center justify-center bg-app-muted text-app-4">
        <div className="flex flex-col items-center gap-2">
          <ImageOff className="h-6 w-6" />
          <span className="text-xs">No photo</span>
        </div>
      </div>
    );
  }

  return (
    <img
      src={url}
      alt={alt}
      onError={() => setFailed(true)}
      className="h-full w-full object-cover"
      loading="lazy"
    />
  );
}

function formatApiError(e: any, fallback: string) {
  const status = e?.response?.status;
  const requestId =
    e?.response?.headers?.["x-request-id"] ||
    e?.response?.headers?.["X-Request-ID"] ||
    e?.response?.data?.request_id;
  const detail =
    e?.response?.data?.detail ||
    e?.response?.data?.message ||
    e?.message ||
    fallback;

  const detailText =
    typeof detail === "string" ? detail : JSON.stringify(detail);

  return `${status ? `(${status}) ` : ""}${detailText}${
    requestId ? ` [request ${requestId}]` : ""
  }`;
}

function uniqueCities(rows: Row[]) {
  const values = new Set<string>();
  for (const r of rows) {
    const p = inferProperty(r);
    const city = String(p?.city || r?.city || "").trim();
    if (city) values.add(city);
  }
  return Array.from(values).sort((a, b) => a.localeCompare(b));
}

function buildPagination(currentPage: number, totalPages: number) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, i) => i + 1);
  }

  const pages: (number | string)[] = [1];

  if (currentPage > 3) {
    pages.push("...");
  }

  const start = Math.max(2, currentPage - 1);
  const end = Math.min(totalPages - 1, currentPage + 1);

  for (let i = start; i <= end; i += 1) {
    pages.push(i);
  }

  if (currentPage < totalPages - 2) {
    pages.push("...");
  }

  pages.push(totalPages);
  return pages;
}

export default function InvestorPane() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [markets, setMarkets] = React.useState<MarketRow[]>([]);
  const [inventoryErr, setInventoryErr] = React.useState<string | null>(null);
  const [marketsErr, setMarketsErr] = React.useState<string | null>(null);

  const [inventoryLoading, setInventoryLoading] = React.useState(true);
  const [marketsLoading, setMarketsLoading] = React.useState(false);
  const [refreshing, setRefreshing] = React.useState(false);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<DecisionFilter>("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");
  const [completeness, setCompleteness] =
    React.useState<CompletenessFilter>("ALL");
  const [sort, setSort] = React.useState<SortKey>("BEST_CASHFLOW");
  const [selectedCity, setSelectedCity] = React.useState<string>("ALL");
  const [currentPage, setCurrentPage] = React.useState(1);

  const loadInventory = React.useCallback(async () => {
    setInventoryLoading(true);
    setInventoryErr(null);

    try {
      const propertiesRes = await api.get<any>("/properties", {
        params: { limit: INITIAL_LIMIT },
      });

      const propertyItems =
        propertiesRes?.items || propertiesRes?.rows || propertiesRes || [];
      const normalized = Array.isArray(propertyItems) ? propertyItems : [];

      setRows(normalized);
    } catch (e: any) {
      setInventoryErr(formatApiError(e, "Failed to load investor inventory."));
    } finally {
      setInventoryLoading(false);
    }
  }, []);

  const loadMarkets = React.useCallback(async () => {
    setMarketsLoading(true);
    setMarketsErr(null);

    try {
      const marketsRes = await api.supportedMarkets();
      setMarkets(Array.isArray(marketsRes) ? marketsRes : []);
    } catch (e: any) {
      setMarketsErr(formatApiError(e, "Failed to load markets."));
      setMarkets([]);
    } finally {
      setMarketsLoading(false);
    }
  }, []);

  const load = React.useCallback(async () => {
    setRefreshing(true);
    await Promise.allSettled([loadInventory(), loadMarkets()]);
    setRefreshing(false);
  }, [loadInventory, loadMarkets]);

  React.useEffect(() => {
    load();
  }, [load]);

  const filtered = React.useMemo(() => {
    const normalizedQuery = String(deferredQ || "")
      .trim()
      .toLowerCase();

    let next = rows.filter((r) => {
      const property = inferProperty(r);
      const price = inferAskingPrice(r);
      const financingType = getFinancingType(price);
      const rowDecision = normalizeDecision(
        r?.normalized_decision || r?.classification || r?.decision,
      );
      const rowCompleteness = inferCompleteness(r);
      const city = String(property?.city || r?.city || "").trim();

      if (
        selectedCity !== "ALL" &&
        city.toLowerCase() !== selectedCity.toLowerCase()
      ) {
        return false;
      }

      if (decision !== "ALL" && rowDecision !== decision) return false;
      if (financing !== "ALL" && financingType.toUpperCase() !== financing) {
        return false;
      }
      if (completeness !== "ALL" && rowCompleteness !== completeness) {
        return false;
      }

      if (!normalizedQuery) return true;

      const haystack = [
        property?.address,
        property?.city,
        property?.state,
        property?.zip,
        property?.county,
        r?.classification,
        r?.decision,
        ...(inferTags(r) || []),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      return haystack.includes(normalizedQuery);
    });

    next = sortRows(next, sort);
    return next;
  }, [rows, deferredQ, decision, financing, completeness, sort, selectedCity]);

  React.useEffect(() => {
    setCurrentPage(1);
  }, [deferredQ, decision, financing, completeness, sort, selectedCity]);

  const cityOptions = React.useMemo(() => {
    const marketCities = markets
      .map((m) => String(m?.city || "").trim())
      .filter(Boolean);

    const rowCities = uniqueCities(rows);
    return Array.from(new Set([...marketCities, ...rowCities])).sort((a, b) =>
      a.localeCompare(b),
    );
  }, [markets, rows]);

  const completenessCounts = React.useMemo(() => {
    let complete = 0;
    let partial = 0;
    let missing = 0;

    for (const row of rows) {
      const value = inferCompleteness(row);
      if (value === "COMPLETE") complete += 1;
      else if (value === "PARTIAL") partial += 1;
      else missing += 1;
    }

    return {
      complete,
      partial,
      missing,
      total: rows.length,
    };
  }, [rows]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const startIndex = (safeCurrentPage - 1) * PAGE_SIZE;
  const endIndex = startIndex + PAGE_SIZE;
  const pagedRows = filtered.slice(startIndex, endIndex);
  const pageNumbers = buildPagination(safeCurrentPage, totalPages);

  const handleRefresh = React.useCallback(async () => {
    await load();
  }, [load]);

  const handlePageChange = React.useCallback((page: number) => {
    setCurrentPage(page);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, []);

  return (
    <PageShell>
      <PageHero title="Investor Inventory" />

      <div className="grid gap-4">
        <Surface className="p-4">
          <div className="grid gap-4 lg:grid-cols-[1.4fr_1.4fr_auto]">
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search address, city, county, tags..."
                className="w-full rounded-2xl border border-app bg-app-panel pl-10 pr-4 py-3 text-sm text-app-0 outline-none"
              />
            </div>

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              <select
                value={selectedCity}
                onChange={(e) => setSelectedCity(e.target.value)}
                className="rounded-2xl border border-app bg-app-panel px-3 py-3 text-sm text-app-0"
              >
                <option value="ALL">All cities</option>
                {cityOptions.map((city) => (
                  <option key={city} value={city}>
                    {city}
                  </option>
                ))}
              </select>

              <select
                value={decision}
                onChange={(e) => setDecision(e.target.value as DecisionFilter)}
                className="rounded-2xl border border-app bg-app-panel px-3 py-3 text-sm text-app-0"
              >
                <option value="ALL">All decisions</option>
                <option value="GOOD_DEAL">Good deal</option>
                <option value="REVIEW">Review</option>
                <option value="REJECT">Reject</option>
              </select>

              <select
                value={financing}
                onChange={(e) =>
                  setFinancing(e.target.value as FinancingFilter)
                }
                className="rounded-2xl border border-app bg-app-panel px-3 py-3 text-sm text-app-0"
              >
                <option value="ALL">All financing</option>
                <option value="CASH">Cash</option>
                <option value="DSCR">DSCR</option>
                <option value="UNKNOWN">Unknown</option>
              </select>

              <select
                value={completeness}
                onChange={(e) =>
                  setCompleteness(e.target.value as CompletenessFilter)
                }
                className="rounded-2xl border border-app bg-app-panel px-3 py-3 text-sm text-app-0"
              >
                <option value="ALL">All completeness</option>
                <option value="COMPLETE">Enriched</option>
                <option value="PARTIAL">Partial</option>
                <option value="MISSING">Missing</option>
              </select>

              <select
                value={sort}
                onChange={(e) => setSort(e.target.value as SortKey)}
                className="rounded-2xl border border-app bg-app-panel px-3 py-3 text-sm text-app-0"
              >
                <option value="BEST_CASHFLOW">Best cashflow</option>
                <option value="LOWEST_PRICE">Lowest price</option>
                <option value="HIGHEST_PRICE">Highest price</option>
                <option value="BEST_DSCR">Best DSCR</option>
                <option value="NEWEST">Newest</option>
              </select>
            </div>

            <button
              onClick={handleRefresh}
              disabled={refreshing}
              className="inline-flex items-center justify-center gap-2 rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-0 disabled:opacity-60"
            >
              {refreshing ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCcw className="h-4 w-4" />
              )}
              Refresh
            </button>
          </div>

          <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
              <SlidersHorizontal className="h-3.5 w-3.5" />
              Live inventory filters
            </div>

            <div className="flex flex-wrap gap-2 text-xs text-app-4">
              <span className="oh-pill">{completenessCounts.total} loaded</span>
              <span className="oh-pill">
                {completenessCounts.complete} enriched
              </span>
              <span className="oh-pill">
                {completenessCounts.partial} partial
              </span>
              <span className="oh-pill">
                {completenessCounts.missing} missing
              </span>
            </div>
          </div>

          {marketsErr && (
            <div className="mt-3 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
              Market list unavailable: {marketsErr}
            </div>
          )}
        </Surface>

        <Surface className="p-4">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-app-0">
                Available investment inventory
              </div>
              <div className="text-xs text-app-4">
                Showing 25 properties per page.
              </div>
            </div>
            <div className="text-xs text-app-4">
              {filtered.length.toLocaleString()} results
            </div>
          </div>

          {inventoryLoading ? (
            <div className="py-16 text-center text-app-4">
              Loading inventory…
            </div>
          ) : inventoryErr ? (
            <EmptyState
              title="Investor page failed to load"
              description={inventoryErr}
            />
          ) : filtered.length === 0 ? (
            <EmptyState
              title="No matching inventory"
              description="Try changing filters."
            />
          ) : (
            <>
              <div className="mb-4 flex items-center justify-between gap-3 text-xs text-app-4">
                <div>
                  Showing {startIndex + 1}-{Math.min(endIndex, filtered.length)}{" "}
                  of {filtered.length}
                </div>
                <div>
                  Page {safeCurrentPage} of {totalPages}
                </div>
              </div>

              <div className="grid gap-4 xl:grid-cols-2">
                {pagedRows.map((r, index) => {
                  const property = inferProperty(r);
                  const propertyId = resolvePropertyId(r);
                  const price = inferAskingPrice(r);
                  const rent = inferMarketRent(r);
                  const mortgage = inferMortgage(r);
                  const cashflow = inferCashflow(r);
                  const dscr = inferDscr(r);
                  const completenessValue = inferCompleteness(r);
                  const photoUrl = inferPhotoUrl(r);
                  const tags = inferTags(r);

                  return (
                    <div
                      key={`${propertyId || property?.address || "row"}-${startIndex + index}`}
                      className="overflow-hidden rounded-3xl border border-app bg-app-panel"
                    >
                      <div className="grid md:grid-cols-[240px_1fr]">
                        <div className="h-56 md:h-full">
                          <Photo
                            url={photoUrl}
                            alt={property?.address || "Property photo"}
                          />
                        </div>

                        <div className="p-5">
                          <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                              <div className="text-lg font-semibold text-app-0">
                                {property?.address || "Unknown address"}
                              </div>
                              <div className="mt-1 flex items-center gap-1 text-sm text-app-4">
                                <MapPin className="h-4 w-4" />
                                {[
                                  property?.city,
                                  property?.state,
                                  property?.zip,
                                ]
                                  .filter(Boolean)
                                  .join(", ")}
                              </div>
                            </div>

                            <div className="flex flex-wrap gap-2">
                              <div
                                className={decisionPillClass(
                                  r?.normalized_decision || r?.classification,
                                )}
                              >
                                {normalizeDecision(
                                  r?.normalized_decision || r?.classification,
                                ).replace("_", " ")}
                              </div>
                              <div
                                className={completenessPillClass(
                                  completenessValue,
                                )}
                              >
                                {completenessLabel(completenessValue)}
                              </div>
                            </div>
                          </div>

                          <div className="mt-4 grid gap-3 sm:grid-cols-3">
                            <div className="rounded-2xl border border-app bg-app px-3 py-3">
                              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                <Landmark className="h-3.5 w-3.5" />
                                Price
                              </div>
                              <div className="mt-2 text-base font-semibold text-app-0">
                                {money(price)}
                              </div>
                            </div>

                            <div className="rounded-2xl border border-app bg-app px-3 py-3">
                              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                <Wallet className="h-3.5 w-3.5" />
                                Cashflow
                              </div>
                              <div
                                className={`mt-2 text-base font-semibold ${metricTone(cashflow)}`}
                              >
                                {money(cashflow)}
                              </div>
                            </div>

                            <div className="rounded-2xl border border-app bg-app px-3 py-3">
                              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                <Banknote className="h-3.5 w-3.5" />
                                DSCR
                              </div>
                              <div className="mt-2 text-base font-semibold text-app-0">
                                {dscr != null ? dscr.toFixed(2) : "—"}
                              </div>
                            </div>
                          </div>

                          <div className="mt-4 grid gap-3 sm:grid-cols-4">
                            <div className="flex items-center gap-2 text-sm text-app-3">
                              <BedDouble className="h-4 w-4" />
                              {property?.bedrooms ?? "—"} bd
                            </div>
                            <div className="flex items-center gap-2 text-sm text-app-3">
                              <Bath className="h-4 w-4" />
                              {property?.bathrooms ?? "—"} ba
                            </div>
                            <div className="flex items-center gap-2 text-sm text-app-3">
                              <Ruler className="h-4 w-4" />
                              {property?.square_feet
                                ? Number(property.square_feet).toLocaleString()
                                : "—"}{" "}
                              sqft
                            </div>
                            <div className="flex items-center gap-2 text-sm text-app-3">
                              <ShieldAlert className="h-4 w-4" />
                              Crime {inferCrime(r) ?? "—"}
                            </div>
                          </div>

                          <div className="mt-4 grid gap-3 sm:grid-cols-3 text-sm text-app-3">
                            <div>Mortgage: {money(mortgage)}</div>
                            <div>Market rent: {money(rent)}</div>
                            <div>
                              Updated: {relativeTime(inferUpdatedAt(r))}
                            </div>
                          </div>

                          {tags.length > 0 && (
                            <div className="mt-4 flex flex-wrap gap-2">
                              {tags.map((tag) => (
                                <span key={tag} className="oh-pill">
                                  {tag}
                                </span>
                              ))}
                            </div>
                          )}

                          <div className="mt-5 flex items-center justify-between gap-3">
                            <div className="text-xs text-app-4">
                              Location confidence:{" "}
                              {inferLocationConfidence(r) ?? "—"}
                            </div>

                            {propertyId ? (
                              <Link
                                to={`/property/${propertyId}`}
                                className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app px-3 py-2 text-sm text-app-0"
                              >
                                Open property
                                <ArrowUpRight className="h-4 w-4" />
                              </Link>
                            ) : (
                              <div className="text-xs text-red-300">
                                Missing property id
                              </div>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="mt-8 flex flex-col items-center gap-4">
                <div className="flex items-center gap-2">
                  <button
                    onClick={() =>
                      handlePageChange(Math.max(1, safeCurrentPage - 1))
                    }
                    disabled={safeCurrentPage === 1}
                    className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 disabled:opacity-50"
                  >
                    <ChevronLeft className="h-4 w-4" />
                    Previous
                  </button>

                  <div className="flex flex-wrap items-center justify-center gap-2">
                    {pageNumbers.map((page, idx) =>
                      page === "..." ? (
                        <span
                          key={`ellipsis-${idx}`}
                          className="px-2 py-2 text-sm text-app-4"
                        >
                          ...
                        </span>
                      ) : (
                        <button
                          key={`page-${page}`}
                          onClick={() => handlePageChange(Number(page))}
                          className={`min-w-[40px] rounded-2xl border px-3 py-2 text-sm ${
                            safeCurrentPage === page
                              ? "border-app bg-app text-app-0"
                              : "border-app bg-app-panel text-app-3"
                          }`}
                        >
                          {page}
                        </button>
                      ),
                    )}
                  </div>

                  <button
                    onClick={() =>
                      handlePageChange(
                        Math.min(totalPages, safeCurrentPage + 1),
                      )
                    }
                    disabled={safeCurrentPage === totalPages}
                    className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 disabled:opacity-50"
                  >
                    Next
                    <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              </div>
            </>
          )}
        </Surface>
      </div>
    </PageShell>
  );
}
