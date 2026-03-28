import React from "react";
import { Link } from "react-router-dom";
import {
  Search,
  RefreshCcw,
  SlidersHorizontal,
  MapPin,
  BedDouble,
  Bath,
  Wallet,
  Landmark,
  ShieldAlert,
  ArrowUpRight,
  ImageOff,
  Loader2,
  ChevronLeft,
  ChevronRight,
  BriefcaseBusiness,
  GitBranch,
  Clock3,
  CheckCircle2,
  Settings2,
  PanelRightOpen,
} from "lucide-react";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import PaneSwitcher from "../components/PaneSwitcher";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import MarketSourcePackModal from "../components/MarketSourcePackModal";
import IngestionLaunchCard from "../components/IngestionLaunchCard";
import { api } from "../lib/api";
import type { SupportedMarket } from "../lib/ingestionClient";

type Row = any;
type MarketRow = SupportedMarket;
// type AcquisitionQueueRow = any; // kept out of active use; old queue behavior preserved below in comments

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR" | "UNKNOWN";
type CompletenessFilter = "ALL" | "COMPLETE" | "PARTIAL" | "MISSING";

type DealScopeFilter = "CANDIDATES" | "INCLUDE_SUPPRESSED";
type HiddenReasonFilter =
  | "ALL"
  | "INACTIVE"
  | "LOW_SCORE"
  | "BAD_RISK"
  | "WEAK_CASHFLOW"
  | "WEAK_DSCR";

// type AcquisitionWaitFilter =
//   | "ALL"
//   | "LENDER"
//   | "TITLE"
//   | "OPERATOR"
//   | "SELLER"
//   | "DOCUMENT";
// type AcquisitionStatusFilter = "ALL" | "OVERDUE" | "DUE_SOON" | "BLOCKED";

type SortKey =
  | "RELEVANCE"
  | "BEST_CASHFLOW"
  | "LOWEST_PRICE"
  | "HIGHEST_PRICE"
  | "BEST_DSCR"
  | "NEWEST";

const INITIAL_LIMIT = 500;
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
  if (price == null || !Number.isFinite(Number(price))) return "UNKNOWN";
  if (Number(price) < 75000) return "CASH";
  return "DSCR";
}

function inferDealFilterStatus(r: any) {
  return (
    String(r?.deal_filter_status || "")
      .trim()
      .toLowerCase() || "candidate"
  );
}

function inferIsDealCandidate(r: any) {
  return Boolean(r?.is_deal_candidate);
}

function inferHiddenReason(r: any) {
  return (
    String(r?.hidden_reason || "")
      .trim()
      .toLowerCase() || null
  );
}

function inferCompleteness(r: any): "COMPLETE" | "PARTIAL" | "MISSING" {
  const explicit = String(r?.completeness || "")
    .trim()
    .toUpperCase();
  if (["COMPLETE", "PARTIAL", "MISSING"].includes(explicit)) {
    return explicit as "COMPLETE" | "PARTIAL" | "MISSING";
  }

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
    r?.source_updated_at ||
    r?.acquisition_last_seen_at ||
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
  const hiddenReason = inferHiddenReason(r);
  const isCandidate = inferIsDealCandidate(r);

  if (isCandidate) tags.add("Deal candidate");
  if (hiddenReason === "bad_risk") tags.add("Risk suppressed");
  if (hiddenReason === "weak_cashflow") tags.add("Weak cashflow");
  if (hiddenReason === "weak_dscr") tags.add("Weak DSCR");
  if (hiddenReason === "low_score") tags.add("Low score");

  if (
    normalizeDecision(r?.normalized_decision || r?.classification) ===
    "GOOD_DEAL"
  ) {
    tags.add("Good deal");
  }
  if (financing === "CASH") tags.add("Cash");
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

function buildPagination(currentPage: number, totalPages: number) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, i) => i + 1);
  }

  const pages: (number | string)[] = [1];

  if (currentPage > 3) pages.push("...");

  const start = Math.max(2, currentPage - 1);
  const end = Math.min(totalPages - 1, currentPage + 1);

  for (let i = start; i <= end; i += 1) {
    pages.push(i);
  }

  if (currentPage < totalPages - 2) pages.push("...");

  pages.push(totalPages);
  return pages;
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

function marketDisplayName(market: any) {
  return (
    market?.label ||
    market?.city ||
    [market?.county, market?.state].filter(Boolean).join(", ") ||
    market?.state ||
    "Market"
  );
}

function marketSubLabel(market: any) {
  return [market?.city, market?.county, market?.state]
    .filter(Boolean)
    .join(" • ");
}

export default function InvestorPane() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [markets, setMarkets] = React.useState<MarketRow[]>([]);

  const [inventoryErr, setInventoryErr] = React.useState<string | null>(null);
  const [marketsErr, setMarketsErr] = React.useState<string | null>(null);

  const [inventoryLoading, setInventoryLoading] = React.useState(true);
  const [marketsLoading, setMarketsLoading] = React.useState(true);
  const [refreshing, setRefreshing] = React.useState(false);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<DecisionFilter>("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");
  const [completeness, setCompleteness] =
    React.useState<CompletenessFilter>("ALL");
  const [sort, setSort] = React.useState<SortKey>("RELEVANCE");
  const [currentPage, setCurrentPage] = React.useState(1);

  const [dealScope, setDealScope] =
      React.useState<DealScopeFilter>("CANDIDATES");
  const [hiddenReason, setHiddenReason] =
      React.useState<HiddenReasonFilter>("ALL");
  // OLD CITY-ONLY FILTER KEPT FOR REFERENCE
  // const [selectedCity, setSelectedCity] = React.useState<string>("ALL");

  const [activeRunId, setActiveRunId] = React.useState<number | null>(null);
  const [runsOpen, setRunsOpen] = React.useState(false);
  const [sourcePackMarket, setSourcePackMarket] =
    React.useState<SupportedMarket | null>(null);
  const [selectedMarket, setSelectedMarket] =
    React.useState<SupportedMarket | null>(null);
  const [marketRefreshNonce, setMarketRefreshNonce] = React.useState(0);

  // OLD ACQUISITION QUEUE STATE KEPT FOR REFERENCE
  // const [acquisitionQueue, setAcquisitionQueue] = React.useState<AcquisitionQueueRow[]>([]);
  // const [acquisitionIds, setAcquisitionIds] = React.useState<Set<number>>(new Set());
  // const [queueErr, setQueueErr] = React.useState<string | null>(null);
  // const [queueLoading, setQueueLoading] = React.useState(false);
  // const [queueSearch, setQueueSearch] = React.useState("");
  // const [waitFilter, setWaitFilter] =
  //   React.useState<AcquisitionWaitFilter>("ALL");
  // const [queueStatusFilter, setQueueStatusFilter] =
  //   React.useState<AcquisitionStatusFilter>("ALL");

  const loadInventory = React.useCallback(async () => {
    setInventoryLoading(true);
    setInventoryErr(null);

    try {
      const params: Record<string, any> = {
        limit: INITIAL_LIMIT,
        deals_only: "true",
        include_suppressed:
          dealScope === "INCLUDE_SUPPRESSED" ? "true" : undefined,
            };

      if (selectedMarket?.state) params.state = selectedMarket.state;
      if (selectedMarket?.county) params.county = selectedMarket.county;
      if (selectedMarket?.city) params.city = selectedMarket.city;
      if (deferredQ.trim()) params.q = deferredQ.trim();

      if (sort === "RELEVANCE") params.sort = "relevance";
      if (sort === "BEST_CASHFLOW") params.sort = "best_cashflow";
      if (sort === "LOWEST_PRICE") params.sort = "lowest_price";
      if (sort === "HIGHEST_PRICE") params.sort = "highest_price";
      if (sort === "BEST_DSCR") params.sort = "best_dscr";
      if (sort === "NEWEST") params.sort = "newest";

      if (hiddenReason === "INACTIVE") params.hidden_reason = "inactive_listing";
      if (hiddenReason === "LOW_SCORE") params.hidden_reason = "low_score";
      if (hiddenReason === "BAD_RISK") params.hidden_reason = "bad_risk";
      if (hiddenReason === "WEAK_CASHFLOW") params.hidden_reason = "weak_cashflow";
      if (hiddenReason === "WEAK_DSCR") params.hidden_reason = "weak_dscr";
      
      const propertiesRes = await api.get<any>("/properties", { params });
      const propertyItems =
        propertiesRes?.items || propertiesRes?.rows || propertiesRes || [];
      const normalized = Array.isArray(propertyItems) ? propertyItems : [];
      setRows(normalized);
    } catch (e: any) {
      setInventoryErr(formatApiError(e, "Failed to load investor inventory."));
      setRows([]);
    } finally {
      setInventoryLoading(false);
    }
  }, [deferredQ, selectedMarket, sort, dealScope, hiddenReason]);

  const loadMarkets = React.useCallback(async () => {
    setMarketsLoading(true);
    setMarketsErr(null);

    try {
      const marketsRes = await api.supportedMarkets();

      const normalized: SupportedMarket[] = Array.isArray(marketsRes)
        ? marketsRes.map(
            (m: any): SupportedMarket => ({
              slug: String(m?.slug || ""),
              label: String(m?.label || m?.city || m?.slug || ""),
              state: String(m?.state || "MI"),
              county: m?.county ?? null,
              city: m?.city ?? null,
              zip_codes: Array.isArray(m?.zip_codes)
                ? m.zip_codes.filter((z: any) => typeof z === "string")
                : [],
              coverage_tier: m?.coverage_tier ?? null,
              priority:
                typeof m?.priority === "number" ? m.priority : undefined,
              is_active:
                typeof m?.is_active === "boolean" ? m.is_active : undefined,
              sync_limit:
                typeof m?.sync_limit === "number" ? m.sync_limit : undefined,
              sync_every_hours:
                typeof m?.sync_every_hours === "number"
                  ? m.sync_every_hours
                  : undefined,
              min_price: typeof m?.min_price === "number" ? m.min_price : null,
              max_price: typeof m?.max_price === "number" ? m.max_price : null,
              property_types: Array.isArray(m?.property_types)
                ? m.property_types.filter((p: any) => typeof p === "string")
                : [],
              max_units: typeof m?.max_units === "number" ? m.max_units : null,
              notes: typeof m?.notes === "string" ? m.notes : null,
            }),
          )
        : [];

      setMarkets(normalized);

      setSelectedMarket((prev): SupportedMarket | null => {
        if (prev?.slug) {
          return (
            normalized.find((m) => m.slug === prev.slug) ??
            normalized[0] ??
            null
          );
        }
        return normalized[0] ?? null;
      });
    } catch (e: any) {
      setMarketsErr(formatApiError(e, "Failed to load markets."));
      setMarkets([]);
      setSelectedMarket(null);
    } finally {
      setMarketsLoading(false);
    }
  }, []);

  // OLD QUEUE LOAD PATH KEPT FOR REFERENCE
  // const loadQueue = React.useCallback(async () => {
  //   setQueueLoading(true);
  //   setQueueErr(null);
  //
  //   try {
  //     const queueRes = await api.get<any>("/acquisition/queue", {
  //       params: { limit: 1000 },
  //     });
  //
  //     const items = Array.isArray(queueRes?.items) ? queueRes.items : [];
  //     setAcquisitionQueue(items);
  //
  //     const propertyIds = new Set<number>(
  //       items
  //         .map((x: any) => Number(x?.property_id))
  //         .filter((n: number) => Number.isFinite(n) && n > 0),
  //     );
  //     setAcquisitionIds(propertyIds);
  //   } catch (e: any) {
  //     setQueueErr(formatApiError(e, "Failed to load acquisition queue."));
  //     setAcquisitionQueue([]);
  //     setAcquisitionIds(new Set());
  //   } finally {
  //     setQueueLoading(false);
  //   }
  // }, []);

  const load = React.useCallback(async () => {
    await Promise.all([loadMarkets(), loadInventory()]);
  }, [loadInventory, loadMarkets]);

  React.useEffect(() => {
    loadMarkets();
  }, [loadMarkets]);

  React.useEffect(() => {
    loadInventory();
  }, [loadInventory, marketRefreshNonce]);

    const filteredRows = React.useMemo(() => {
      return rows
        .filter((row) => {
          const normalizedDecision = normalizeDecision(
            row?.normalized_decision || row?.classification,
          );
          if (decision !== "ALL" && normalizedDecision !== decision)
            return false;

          const financingType = getFinancingType(inferAskingPrice(row));
          if (financing !== "ALL" && financingType !== financing) return false;

          const completenessValue = inferCompleteness(row);
          if (completeness !== "ALL" && completenessValue !== completeness) {
            return false;
          }

          if (dealScope === "CANDIDATES" && !inferIsDealCandidate(row)) {
            return false;
          }

          if (hiddenReason !== "ALL") {
            const reason = inferHiddenReason(row);
            if (hiddenReason === "INACTIVE" && reason !== "inactive_listing")
              return false;
            if (hiddenReason === "LOW_SCORE" && reason !== "low_score")
              return false;
            if (hiddenReason === "BAD_RISK" && reason !== "bad_risk")
              return false;
            if (hiddenReason === "WEAK_CASHFLOW" && reason !== "weak_cashflow")
              return false;
            if (hiddenReason === "WEAK_DSCR" && reason !== "weak_dscr")
              return false;
          }

          return true;
        })
        .sort((a, b) => {
          const aCandidate = inferIsDealCandidate(a) ? 1 : 0;
          const bCandidate = inferIsDealCandidate(b) ? 1 : 0;

          if (sort === "BEST_CASHFLOW") {
            return (
              (inferCashflow(b) ?? Number.NEGATIVE_INFINITY) -
              (inferCashflow(a) ?? Number.NEGATIVE_INFINITY)
            );
          }

          if (sort === "BEST_DSCR") {
            return (
              (inferDscr(b) ?? Number.NEGATIVE_INFINITY) -
              (inferDscr(a) ?? Number.NEGATIVE_INFINITY)
            );
          }

          if (sort === "LOWEST_PRICE") {
            return (
              (inferAskingPrice(a) ?? Number.POSITIVE_INFINITY) -
              (inferAskingPrice(b) ?? Number.POSITIVE_INFINITY)
            );
          }

          if (sort === "HIGHEST_PRICE") {
            return (
              (inferAskingPrice(b) ?? Number.NEGATIVE_INFINITY) -
              (inferAskingPrice(a) ?? Number.NEGATIVE_INFINITY)
            );
          }

          if (sort === "NEWEST") {
            return (
              new Date(inferUpdatedAt(b) || 0).getTime() -
              new Date(inferUpdatedAt(a) || 0).getTime()
            );
          }

          // relevance: deal candidates first, then cashflow, then dscr, then freshness
          if (bCandidate !== aCandidate) return bCandidate - aCandidate;

          const cashflowDelta =
            (inferCashflow(b) ?? Number.NEGATIVE_INFINITY) -
            (inferCashflow(a) ?? Number.NEGATIVE_INFINITY);
          if (cashflowDelta !== 0) return cashflowDelta;

          const dscrDelta =
            (inferDscr(b) ?? Number.NEGATIVE_INFINITY) -
            (inferDscr(a) ?? Number.NEGATIVE_INFINITY);
          if (dscrDelta !== 0) return dscrDelta;

          return (
            new Date(inferUpdatedAt(b) || 0).getTime() -
            new Date(inferUpdatedAt(a) || 0).getTime()
          );
        });
    }, [
      rows,
      decision,
      financing,
      completeness,
      dealScope,
      hiddenReason,
      sort,
    ]);

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / PAGE_SIZE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const pageRows = React.useMemo(() => {
    const start = (safeCurrentPage - 1) * PAGE_SIZE;
    return filteredRows.slice(start, start + PAGE_SIZE);
  }, [filteredRows, safeCurrentPage]);

  React.useEffect(() => {
    setCurrentPage(1);
  }, [
    deferredQ,
    decision,
    financing,
    completeness,
    dealScope,
    hiddenReason,
    sort,
    deferredQ,
    selectedMarket?.slug,
  ]);

  const pagination = buildPagination(safeCurrentPage, totalPages);

  async function refreshSelectedMarket() {
    if (!selectedMarket?.slug) return;
    setRefreshing(true);
    setInventoryErr(null);
    try {
      await api.post("/ingestion/sync-market", {
        market_slug: selectedMarket.slug,
      });
      setMarketRefreshNonce((v) => v + 1);
      setRunsOpen(true);
    } catch (e: any) {
      setInventoryErr(formatApiError(e, "Failed to sync selected market."));
    } finally {
      setRefreshing(false);
    }
  }

  return (
    <PageShell>
      <PageHero
        eyebrow="Investor pane"
        title="Curated inventory across supported markets"
        subtitle="Search your covered inventory, refresh the selected market, and inspect sync behavior without dropping into raw ingestion details."
      />

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
        <div className="space-y-6">
          <Surface
            title="Inventory controls"
            subtitle="Scope the investor catalog to a supported market and sort the results that matter."
          >
            <div className="grid gap-4 lg:grid-cols-[1.4fr_1fr_auto]">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <Search className="h-4 w-4" />
                  Search inventory
                </div>
                <div className="mt-2 flex items-center gap-3">
                  <Search className="h-4 w-4 text-app-4" />
                  <input
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="Address, city, ZIP, county"
                    className="w-full bg-transparent text-sm text-app-0 outline-none placeholder:text-app-4"
                  />
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <MapPin className="h-4 w-4" />
                  Supported market
                </div>
                <div className="mt-2">
                  <select
                    value={selectedMarket?.slug || ""}
                    onChange={(e) =>
                      setSelectedMarket(
                        markets.find((m) => m.slug === e.target.value) || null,
                      )
                    }
                    className="w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                  >
                    {markets.map((market) => (
                      <option key={market.slug} value={market.slug}>
                        {marketDisplayName(market)}
                      </option>
                    ))}
                  </select>
                  <div className="mt-2 text-xs text-app-4">
                    {selectedMarket
                      ? marketSubLabel(selectedMarket)
                      : "No market selected"}
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <BriefcaseBusiness className="h-4 w-4" />
                  Deal scope
                </div>
                <select
                  value={dealScope}
                  onChange={(e) =>
                    setDealScope(e.target.value as DealScopeFilter)
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="CANDIDATES">Deal candidates only</option>
                  <option value="INCLUDE_SUPPRESSED">Include suppressed</option>
                </select>
              </div>

              <div className="flex items-stretch gap-3">
                <button
                  type="button"
                  className="oh-btn"
                  disabled={!selectedMarket || refreshing}
                  onClick={refreshSelectedMarket}
                >
                  {refreshing ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Syncing…
                    </>
                  ) : (
                    <>
                      <RefreshCcw className="h-4 w-4" />
                      Sync now
                    </>
                  )}
                </button>

                <button
                  type="button"
                  className="oh-btn oh-btn-secondary"
                  onClick={() => setRunsOpen((v) => !v)}
                >
                  <PanelRightOpen className="h-4 w-4" />
                  {runsOpen ? "Hide runs" : "View runs"}
                </button>
              </div>
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-4">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <SlidersHorizontal className="h-4 w-4" />
                  Decision
                </div>
                <select
                  value={decision}
                  onChange={(e) =>
                    setDecision(e.target.value as DecisionFilter)
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="ALL">All</option>
                  <option value="GOOD_DEAL">Good deal</option>
                  <option value="REVIEW">Review</option>
                  <option value="REJECT">Reject</option>
                </select>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <Landmark className="h-4 w-4" />
                  Financing
                </div>
                <select
                  value={financing}
                  onChange={(e) =>
                    setFinancing(e.target.value as FinancingFilter)
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="ALL">All</option>
                  <option value="CASH">Cash</option>
                  <option value="DSCR">DSCR</option>
                  <option value="UNKNOWN">Unknown</option>
                </select>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <CheckCircle2 className="h-4 w-4" />
                  Completeness
                </div>
                <select
                  value={completeness}
                  onChange={(e) =>
                    setCompleteness(e.target.value as CompletenessFilter)
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="ALL">All</option>
                  <option value="COMPLETE">Complete</option>
                  <option value="PARTIAL">Partial</option>
                  <option value="MISSING">Missing</option>
                </select>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <ShieldAlert className="h-4 w-4" />
                  Suppression reason
                </div>
                <select
                  value={hiddenReason}
                  onChange={(e) =>
                    setHiddenReason(e.target.value as HiddenReasonFilter)
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="ALL">All</option>
                  <option value="BAD_RISK">Bad risk</option>
                  <option value="WEAK_CASHFLOW">Weak cashflow</option>
                  <option value="WEAK_DSCR">Weak DSCR</option>
                  <option value="LOW_SCORE">Low score</option>
                  <option value="INACTIVE">Inactive listing</option>
                </select>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <Settings2 className="h-4 w-4" />
                  Sort
                </div>
                <select
                  value={sort}
                  onChange={(e) => setSort(e.target.value as SortKey)}
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="RELEVANCE">Relevance</option>
                  <option value="BEST_CASHFLOW">Best cashflow</option>
                  <option value="LOWEST_PRICE">Lowest price</option>
                  <option value="HIGHEST_PRICE">Highest price</option>
                  <option value="BEST_DSCR">Best DSCR</option>
                  <option value="NEWEST">Newest</option>
                </select>
              </div>
            </div>

            {inventoryErr ? (
              <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {inventoryErr}
              </div>
            ) : null}

            {marketsErr ? (
              <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {marketsErr}
              </div>
            ) : null}
          </Surface>

          <Surface
            title="Investor inventory"
            subtitle="Your local supported-market catalog, sorted for action instead of raw provider order."
          >
            {inventoryLoading || marketsLoading ? (
              <div className="flex items-center justify-center py-16 text-app-4">
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            ) : !pageRows.length ? (
              <EmptyState
                title="No properties match the current filters"
                description="Try a different market, search term, or decision filter."
              />
            ) : (
              <>
                <div className="grid gap-4">
                  {pageRows.map((row) => {
                    const property = inferProperty(row);
                    const propertyId = resolvePropertyId(row);
                    const askingPrice = inferAskingPrice(row);
                    const marketRent = inferMarketRent(row);
                    const cashflow = inferCashflow(row);
                    const dscr = inferDscr(row);
                    const completenessValue = inferCompleteness(row);
                    const photoUrl = inferPhotoUrl(row);
                    const tags = inferTags(row);
                    const updatedAt = inferUpdatedAt(row);

                    return (
                      <div
                        key={propertyId || property?.id || Math.random()}
                        className="overflow-hidden rounded-3xl border border-app bg-app-panel"
                      >
                        <div className="grid gap-0 md:grid-cols-[280px_minmax(0,1fr)]">
                          <div className="h-[220px] bg-app-muted">
                            <Photo
                              url={photoUrl}
                              alt={property?.address || "Property"}
                            />
                          </div>

                          <div className="p-5">
                            <div className="flex flex-wrap items-start justify-between gap-3">
                              <div className="min-w-0">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span
                                    className={decisionPillClass(
                                      row?.normalized_decision,
                                    )}
                                  >
                                    {normalizeDecision(
                                      row?.normalized_decision,
                                    )}
                                  </span>
                                  <span
                                    className={completenessPillClass(
                                      completenessValue,
                                    )}
                                  >
                                    {completenessLabel(completenessValue)}
                                  </span>

                                  {inferIsDealCandidate(row) ? (
                                        <span className="oh-pill oh-pill-good">Deal candidate</span>
                                      ) : (
                                        <span className="oh-pill oh-pill-warn">
                                          Suppressed{inferHiddenReason(row) ? ` • ${inferHiddenReason(row)}` : ""}
                                        </span>
                                      )}
                                </div>

                                <div className="mt-3 text-xl font-semibold text-app-0">
                                  {property?.address || "Unknown address"}
                                </div>

                                <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-app-4">
                                  <span className="inline-flex items-center gap-1">
                                    <MapPin className="h-4 w-4" />
                                    {[
                                      property?.city,
                                      property?.state,
                                      property?.zip,
                                    ]
                                      .filter(Boolean)
                                      .join(", ")}
                                  </span>
                                  <span>•</span>
                                  <span>{relativeTime(updatedAt)}</span>
                                </div>
                              </div>

                              <div className="text-right">
                                <div className="text-xs uppercase tracking-[0.18em] text-app-4">
                                  Asking
                                </div>
                                <div className="mt-1 text-2xl font-semibold text-app-0">
                                  {money(askingPrice)}
                                </div>
                              </div>
                            </div>

                            <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <Wallet className="h-4 w-4" />
                                  Cashflow
                                </div>
                                <div
                                  className={`mt-2 text-lg font-semibold ${metricTone(cashflow)}`}
                                >
                                  {money(cashflow)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <Landmark className="h-4 w-4" />
                                  DSCR
                                </div>
                                <div className="mt-2 text-lg font-semibold text-app-0">
                                  {dscr != null ? dscr.toFixed(2) : "—"}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <BriefcaseBusiness className="h-4 w-4" />
                                  Rent est.
                                </div>
                                <div className="mt-2 text-lg font-semibold text-app-0">
                                  {money(marketRent)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <ShieldAlert className="h-4 w-4" />
                                  Crime
                                </div>
                                <div className="mt-2 text-lg font-semibold text-app-0">
                                  {numberOrNull(inferCrime(row)) ?? "—"}
                                </div>
                              </div>
                            </div>

                            <div className="mt-4 flex flex-wrap gap-2">
                              {tags.map((tag) => (
                                <span
                                  key={tag}
                                  className="oh-pill oh-pill-secondary"
                                >
                                  {tag}
                                </span>
                              ))}
                            </div>

                            <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
                              <div className="flex flex-wrap items-center gap-4 text-sm text-app-4">
                                <span className="inline-flex items-center gap-1">
                                  <BedDouble className="h-4 w-4" />
                                  {property?.bedrooms ?? "—"} beds
                                </span>
                                <span className="inline-flex items-center gap-1">
                                  <Bath className="h-4 w-4" />
                                  {property?.bathrooms ?? "—"} baths
                                </span>
                                <span className="inline-flex items-center gap-1">
                                  <GitBranch className="h-4 w-4" />
                                  {inferCounty(row) || "Unknown county"}
                                </span>
                              </div>

                              <div className="flex items-center gap-2">
                                {propertyId ? (
                                  <Link
                                    to={`/properties/${propertyId}`}
                                    className="oh-btn"
                                  >
                                    View property
                                    <ArrowUpRight className="h-4 w-4" />
                                  </Link>
                                ) : (
                                  <button
                                    type="button"
                                    className="oh-btn oh-btn-secondary"
                                    disabled
                                  >
                                    Missing property id
                                  </button>
                                )}
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                <div className="mt-5 flex items-center justify-between gap-3">
                  <button
                    type="button"
                    className="oh-btn oh-btn-secondary"
                    disabled={safeCurrentPage <= 1}
                    onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                  >
                    <ChevronLeft className="h-4 w-4" />
                    Previous
                  </button>

                  <div className="flex flex-wrap items-center gap-2">
                    {pagination.map((token, index) =>
                      typeof token === "string" ? (
                        <span
                          key={`ellipsis-${index}`}
                          className="px-2 text-sm text-app-4"
                        >
                          {token}
                        </span>
                      ) : (
                        <button
                          key={token}
                          type="button"
                          className={
                            token === safeCurrentPage
                              ? "oh-btn"
                              : "oh-btn oh-btn-secondary"
                          }
                          onClick={() => setCurrentPage(token)}
                        >
                          {token}
                        </button>
                      ),
                    )}
                  </div>

                  <button
                    type="button"
                    className="oh-btn oh-btn-secondary"
                    disabled={safeCurrentPage >= totalPages}
                    onClick={() =>
                      setCurrentPage((p) => Math.min(totalPages, p + 1))
                    }
                  >
                    Next
                    <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              </>
            )}
          </Surface>
        </div>

        <div className="space-y-6">
          <Surface
            title="Supported market operations"
            subtitle="Source packs and refresh controls for the currently selected supported region."
          >
            <div className="space-y-4">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="text-xs uppercase tracking-[0.18em] text-app-4">
                  Current market
                </div>
                <div className="mt-2 text-base font-semibold text-app-0">
                  {selectedMarket
                    ? marketDisplayName(selectedMarket)
                    : "No market"}
                </div>
                <div className="mt-1 text-sm text-app-4">
                  {selectedMarket
                    ? marketSubLabel(selectedMarket)
                    : "Select a supported market."}
                </div>
              </div>

              <IngestionLaunchCard
                market={selectedMarket}
                onRunQueued={() => {
                  setMarketRefreshNonce((v) => v + 1);
                  setRunsOpen(true);
                }}
                onManageSources={() => setSourcePackMarket(selectedMarket)}
              />

              <button
                type="button"
                className="oh-btn oh-btn-secondary w-full"
                disabled={!selectedMarket}
                onClick={() => setSourcePackMarket(selectedMarket)}
              >
                <Settings2 className="h-4 w-4" />
                Manage market sources
              </button>
            </div>
          </Surface>

          {runsOpen ? (
            <IngestionRunsPanel
              open={runsOpen}
              refreshKey={marketRefreshNonce}
              onClose={() => setRunsOpen(false)}
              onSelectRun={(runId) => setActiveRunId(runId)}
            />
          ) : null}
        </div>
      </div>

      <IngestionErrorsDrawer
        runId={activeRunId}
        onClose={() => setActiveRunId(null)}
      />

      <MarketSourcePackModal
        open={Boolean(sourcePackMarket)}
        market={sourcePackMarket}
        onClose={() => setSourcePackMarket(null)}
        onChanged={load}
      />
    </PageShell>
  );
}
