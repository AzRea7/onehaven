import React from "react";
import { Link, useLocation } from "react-router-dom";
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
  BriefcaseBusiness,
  GitBranch,
  Clock3,
  AlertTriangle,
  FileWarning,
  CheckCircle2,
  Users,
  Building2,
  Settings2,
  Bug,
} from "lucide-react";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import PaneSwitcher from "../components/PaneSwitcher";
import { api, type SupportedMarket } from "../lib/api";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import MarketSourcePackModal from "../components/MarketSourcePackModal";

type Row = any;
type MarketRow = SupportedMarket;
type AcquisitionQueueRow = any;

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR" | "UNKNOWN";
type CompletenessFilter = "ALL" | "COMPLETE" | "PARTIAL" | "MISSING";
type AcquisitionWaitFilter =
  | "ALL"
  | "LENDER"
  | "TITLE"
  | "OPERATOR"
  | "SELLER"
  | "DOCUMENT";
type AcquisitionStatusFilter = "ALL" | "OVERDUE" | "DUE_SOON" | "BLOCKED";

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

function waitingOnLabel(raw: any) {
  const text = String(raw || "").trim();
  return text || "Unassigned";
}

function waitingOnCategory(
  raw: any,
): "LENDER" | "TITLE" | "OPERATOR" | "SELLER" | "DOCUMENT" | "OTHER" {
  const text = String(raw || "")
    .trim()
    .toLowerCase();

  if (!text) return "OTHER";
  if (
    text.includes("lender") ||
    text.includes("loan") ||
    text.includes("finance")
  ) {
    return "LENDER";
  }
  if (text.includes("title") || text.includes("escrow")) {
    return "TITLE";
  }
  if (text.includes("seller")) {
    return "SELLER";
  }
  if (
    text.includes("document") ||
    text.includes("doc") ||
    text.includes("agreement") ||
    text.includes("inspection") ||
    text.includes("binder")
  ) {
    return "DOCUMENT";
  }
  if (
    text.includes("operator") ||
    text.includes("internal") ||
    text.includes("review") ||
    text.includes("team")
  ) {
    return "OPERATOR";
  }
  return "OTHER";
}

function queueUrgency(row: any): "OVERDUE" | "DUE_SOON" | "BLOCKED" | "NORMAL" {
  const days = numberOrNull(row?.days_to_close);
  const waiting = String(row?.waiting_on || "").toLowerCase();
  const status = String(row?.status || "").toLowerCase();
  const nextStep = String(row?.next_step || "").toLowerCase();

  if (
    status.includes("blocked") ||
    waiting.includes("blocked") ||
    nextStep.includes("blocked")
  ) {
    return "BLOCKED";
  }
  if (days != null && days < 0) return "OVERDUE";
  if (days != null && days <= 7) return "DUE_SOON";
  return "NORMAL";
}

function urgencyPillClass(v: ReturnType<typeof queueUrgency>) {
  if (v === "OVERDUE") return "oh-pill oh-pill-bad";
  if (v === "DUE_SOON") return "oh-pill oh-pill-warn";
  if (v === "BLOCKED") return "oh-pill oh-pill-bad";
  return "oh-pill oh-pill-good";
}

function queueReadinessScore(row: any) {
  let score = 0;

  const documentCount = numberOrNull(row?.document_count) ?? 0;
  const days = numberOrNull(row?.days_to_close);
  const urgency = queueUrgency(row);
  const waiting = waitingOnCategory(row?.waiting_on);
  const status = String(row?.status || "").toLowerCase();

  score += Math.min(documentCount * 12, 48);

  if (status.includes("under_contract")) score += 18;
  if (status.includes("closing")) score += 22;
  if (status.includes("review")) score += 8;

  if (days != null) {
    if (days > 14) score += 18;
    else if (days >= 7) score += 12;
    else if (days >= 0) score += 6;
    else score -= 10;
  }

  if (urgency === "BLOCKED") score -= 20;
  if (urgency === "OVERDUE") score -= 18;

  if (waiting === "DOCUMENT") score -= 8;
  if (waiting === "LENDER") score -= 5;

  return Math.max(0, Math.min(100, Math.round(score)));
}

function readinessTone(score: number) {
  if (score >= 75) return "text-emerald-300";
  if (score >= 45) return "text-amber-300";
  return "text-red-300";
}

function nextRequiredDocument(row: any) {
  const text = String(row?.waiting_on || "").toLowerCase();
  if (text.includes("inspection")) return "Inspection report";
  if (text.includes("insurance")) return "Insurance binder";
  if (text.includes("title")) return "Title / escrow";
  if (text.includes("loan") || text.includes("lender")) return "Loan documents";
  if (text.includes("closing")) return "Closing disclosure";
  if (text.includes("purchase") || text.includes("agreement")) {
    return "Purchase agreement";
  }
  if (text.includes("document") || text.includes("doc")) {
    return "Required document";
  }
  return "Review document stack";
}

function marketDisplayName(market: any) {
  return (
    market?.city ||
    market?.label ||
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
  const [acquisitionQueue, setAcquisitionQueue] = React.useState<
    AcquisitionQueueRow[]
  >([]);
  const [acquisitionIds, setAcquisitionIds] = React.useState<Set<number>>(
    new Set(),
  );

  const [inventoryErr, setInventoryErr] = React.useState<string | null>(null);
  const [marketsErr, setMarketsErr] = React.useState<string | null>(null);
  const [queueErr, setQueueErr] = React.useState<string | null>(null);

  const [inventoryLoading, setInventoryLoading] = React.useState(true);
  const [marketsLoading, setMarketsLoading] = React.useState(false);
  const [queueLoading, setQueueLoading] = React.useState(true);
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

  const [activeRunId, setActiveRunId] = React.useState<number | null>(null);
  const [sourcePackMarket, setSourcePackMarket] =
    React.useState<SupportedMarket | null>(null);

  const location = useLocation();

  const [queueSearch, setQueueSearch] = React.useState("");
  const [waitFilter, setWaitFilter] =
    React.useState<AcquisitionWaitFilter>("ALL");
  const [queueStatusFilter, setQueueStatusFilter] =
    React.useState<AcquisitionStatusFilter>("ALL");

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

  const loadQueue = React.useCallback(async () => {
    setQueueLoading(true);
    setQueueErr(null);

    try {
      const queueRes = await api.get<any>("/acquisition/queue", {
        params: { limit: 1000 },
      });

      const items = Array.isArray(queueRes?.items) ? queueRes.items : [];
      setAcquisitionQueue(items);

      const propertyIds = new Set<number>(
        items
          .map((x: any) => Number(x?.property_id))
          .filter((n: number) => Number.isFinite(n) && n > 0),
      );
      setAcquisitionIds(propertyIds);
    } catch (e: any) {
      setQueueErr(formatApiError(e, "Failed to load acquisition queue."));
      setAcquisitionQueue([]);
      setAcquisitionIds(new Set());
    } finally {
      setQueueLoading(false);
    }
  }, []);

  const load = React.useCallback(async () => {
    setRefreshing(true);
    await Promise.allSettled([loadInventory(), loadMarkets(), loadQueue()]);
    setRefreshing(false);
  }, [loadInventory, loadMarkets, loadQueue]);

  React.useEffect(() => {
    load();
  }, [load]);

  React.useEffect(() => {
    setActiveRunId(null);
    setSourcePackMarket(null);
  }, [location.pathname]);

  const baseRows = React.useMemo(() => {
    return rows.filter((r) => {
      const propertyId = resolvePropertyId(r);
      if (!propertyId) return true;
      return !acquisitionIds.has(propertyId);
    });
  }, [rows, acquisitionIds]);

  const filtered = React.useMemo(() => {
    const normalizedQuery = String(deferredQ || "")
      .trim()
      .toLowerCase();

    let next = baseRows.filter((r) => {
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
  }, [
    baseRows,
    deferredQ,
    selectedCity,
    decision,
    financing,
    completeness,
    sort,
  ]);

  const filteredQueue = React.useMemo(() => {
    const normalized = String(queueSearch || "")
      .trim()
      .toLowerCase();

    return acquisitionQueue.filter((row) => {
      const wait = waitingOnCategory(row?.waiting_on);
      const urgency = queueUrgency(row);
      const haystack = [
        row?.address,
        row?.city,
        row?.state,
        row?.zip,
        row?.county,
        row?.status,
        row?.waiting_on,
        row?.next_step,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      if (waitFilter !== "ALL" && wait !== waitFilter) return false;
      if (queueStatusFilter !== "ALL" && urgency !== queueStatusFilter) {
        return false;
      }
      if (normalized && !haystack.includes(normalized)) return false;

      return true;
    });
  }, [acquisitionQueue, queueSearch, waitFilter, queueStatusFilter]);

  React.useEffect(() => {
    setCurrentPage(1);
  }, [deferredQ, selectedCity, decision, financing, completeness, sort]);

  const cityOptions = React.useMemo(() => uniqueCities(baseRows), [baseRows]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const startIndex = (safeCurrentPage - 1) * PAGE_SIZE;
  const endIndex = startIndex + PAGE_SIZE;
  const pagedRows = filtered.slice(startIndex, endIndex);
  const pagination = buildPagination(safeCurrentPage, totalPages);

  const counts = React.useMemo(() => {
    const c: Record<"GOOD_DEAL" | "REVIEW" | "REJECT", number> = {
      GOOD_DEAL: 0,
      REVIEW: 0,
      REJECT: 0,
    };
    for (const r of baseRows || []) {
      const d = normalizeDecision(
        r?.normalized_decision || r?.classification || r?.decision,
      ) as "GOOD_DEAL" | "REVIEW" | "REJECT";
      c[d] += 1;
    }
    return c;
  }, [baseRows]);

  const enrichedCount = React.useMemo(
    () => baseRows.filter((r) => inferCompleteness(r) === "COMPLETE").length,
    [baseRows],
  );

  const positiveCashflowCount = React.useMemo(
    () => baseRows.filter((r) => (inferCashflow(r) ?? 0) > 0).length,
    [baseRows],
  );

  const avgCashflow = React.useMemo(() => {
    const values = baseRows
      .map((r) => inferCashflow(r))
      .filter((v): v is number => typeof v === "number");
    if (!values.length) return null;
    return values.reduce((sum, v) => sum + v, 0) / values.length;
  }, [baseRows]);

  const overdueCount = React.useMemo(
    () => acquisitionQueue.filter((r) => queueUrgency(r) === "OVERDUE").length,
    [acquisitionQueue],
  );

  const dueSoonCount = React.useMemo(
    () => acquisitionQueue.filter((r) => queueUrgency(r) === "DUE_SOON").length,
    [acquisitionQueue],
  );

  const blockedCount = React.useMemo(
    () => acquisitionQueue.filter((r) => queueUrgency(r) === "BLOCKED").length,
    [acquisitionQueue],
  );

  const lenderWaitCount = React.useMemo(
    () =>
      acquisitionQueue.filter(
        (r) => waitingOnCategory(r?.waiting_on) === "LENDER",
      ).length,
    [acquisitionQueue],
  );

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Pane 1"
          title="Investor pane"
          subtitle="Discover properties, move strong candidates into acquisition, and monitor what the active deal queue is still waiting on."
          actions={
            <>
              <button onClick={load} className="oh-btn oh-btn-secondary">
                {refreshing ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCcw className="h-4 w-4" />
                )}
                Refresh inventory
              </button>
              <Link to="/dashboard" className="oh-btn oh-btn-secondary">
                Portfolio dashboard
              </Link>
            </>
          }
        />

        <PaneSwitcher activePane="investor" />

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
          <Surface
            title="Investor inventory"
            subtitle="Visible acquisition candidates"
          >
            <div className="text-3xl font-semibold text-app-0">
              {baseRows.length.toLocaleString()}
            </div>
          </Surface>
          <Surface
            title="Good deals"
            subtitle="Underwriting-positive candidates"
          >
            <div className="text-3xl font-semibold text-app-0">
              {counts.GOOD_DEAL.toLocaleString()}
            </div>
          </Surface>
          <Surface title="Fully enriched" subtitle="Ready for deeper review">
            <div className="text-3xl font-semibold text-app-0">
              {enrichedCount.toLocaleString()}
            </div>
          </Surface>
          <Surface
            title="Cashflow positive"
            subtitle="Monthly upside candidates"
          >
            <div className="text-3xl font-semibold text-app-0">
              {positiveCashflowCount.toLocaleString()}
            </div>
          </Surface>
        </div>

        <Surface
          title="Acquisition pressure"
          subtitle="This is the operational handoff view: what is in motion, what is blocked, and what needs action now."
        >
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Clock3 className="h-3.5 w-3.5" />
                Due soon
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {dueSoonCount}
              </div>
            </div>
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <AlertTriangle className="h-3.5 w-3.5" />
                Overdue
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {overdueCount}
              </div>
            </div>
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <FileWarning className="h-3.5 w-3.5" />
                Blocked
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {blockedCount}
              </div>
            </div>
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Building2 className="h-3.5 w-3.5" />
                Waiting on lender
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {lenderWaitCount}
              </div>
            </div>
          </div>
        </Surface>

        <Surface
          title="Lifecycle handoff"
          subtitle="This pane exists to decide which properties should move into acquisition, while still keeping a small operational view of the active deal queue."
        >
          <div className="flex flex-wrap gap-2">
            <span className="oh-pill">
              current stage discovery / underwriting
            </span>
            <span className="oh-pill oh-pill-accent">
              next stage acquisition
            </span>
            <span className="oh-pill oh-pill-warn">
              blocker incomplete underwriting or enrichment
            </span>
            <span className="oh-pill">
              {acquisitionIds.size} already moved to acquisition
            </span>
          </div>

          {marketsErr ? (
            <div className="mt-3 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
              Market list unavailable: {marketsErr}
            </div>
          ) : null}
        </Surface>

        <Surface
          title="Acquisition queue preview"
          subtitle="What are we waiting on right now?"
        >
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Search className="h-3.5 w-3.5" />
                Search queue
              </div>
              <input
                value={queueSearch}
                onChange={(e) => setQueueSearch(e.target.value)}
                placeholder="address, city, waiting on..."
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              />
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Users className="h-3.5 w-3.5" />
                Waiting on
              </div>
              <select
                value={waitFilter}
                onChange={(e) =>
                  setWaitFilter(e.target.value as AcquisitionWaitFilter)
                }
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All owners</option>
                <option value="LENDER">Lender</option>
                <option value="TITLE">Title</option>
                <option value="OPERATOR">Operator</option>
                <option value="SELLER">Seller</option>
                <option value="DOCUMENT">Document</option>
              </select>
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <AlertTriangle className="h-3.5 w-3.5" />
                Queue status
              </div>
              <select
                value={queueStatusFilter}
                onChange={(e) =>
                  setQueueStatusFilter(
                    e.target.value as AcquisitionStatusFilter,
                  )
                }
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All</option>
                <option value="OVERDUE">Overdue</option>
                <option value="DUE_SOON">Due soon</option>
                <option value="BLOCKED">Blocked</option>
              </select>
            </label>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <CheckCircle2 className="h-3.5 w-3.5" />
                Deals in queue
              </div>
              <div className="text-2xl font-semibold text-app-0">
                {filteredQueue.length}
              </div>
            </div>
          </div>

          {queueLoading ? (
            <div className="py-10 text-center text-app-4">Loading queue…</div>
          ) : queueErr ? (
            <EmptyState
              compact
              title="Queue unavailable"
              description={queueErr}
            />
          ) : filteredQueue.length === 0 ? (
            <EmptyState
              compact
              title="No active queue rows"
              description="Nothing in acquisition matches the current queue filters."
            />
          ) : (
            <div className="mt-4 grid gap-4 xl:grid-cols-2">
              {filteredQueue.slice(0, 6).map((row, index) => {
                const propertyId = Number(row?.property_id || 0) || null;
                const urgency = queueUrgency(row);
                const readiness = queueReadinessScore(row);
                const waitOwner = waitingOnCategory(row?.waiting_on);
                const runId =
                  numberOrNull(row?.run_id) ??
                  numberOrNull(row?.ingestion_run_id) ??
                  null;

                return (
                  <div
                    key={`${propertyId || "queue"}-${index}`}
                    className="rounded-3xl border border-app bg-app-panel p-5"
                  >
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <div className="text-lg font-semibold text-app-0">
                          {row?.address || "Unknown address"}
                        </div>
                        <div className="mt-1 text-sm text-app-4">
                          {[row?.city, row?.state, row?.zip]
                            .filter(Boolean)
                            .join(", ")}
                        </div>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <span className={urgencyPillClass(urgency)}>
                          {urgency === "DUE_SOON"
                            ? "due soon"
                            : urgency.toLowerCase()}
                        </span>
                        <span className="oh-pill">
                          waiting on {waitOwner.toLowerCase()}
                        </span>
                      </div>
                    </div>

                    <div className="mt-4 grid gap-3 md:grid-cols-3">
                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Waiting on
                        </div>
                        <div className="mt-2 text-sm font-semibold text-app-0">
                          {waitingOnLabel(row?.waiting_on)}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Next required document
                        </div>
                        <div className="mt-2 text-sm font-semibold text-app-0">
                          {nextRequiredDocument(row)}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Close readiness
                        </div>
                        <div
                          className={`mt-2 text-sm font-semibold ${readinessTone(readiness)}`}
                        >
                          {readiness}%
                        </div>
                      </div>
                    </div>

                    <div className="mt-4 flex flex-wrap gap-2">
                      {row?.status ? (
                        <span className="oh-pill">{row.status}</span>
                      ) : null}
                      {numberOrNull(row?.days_to_close) != null ? (
                        <span className="oh-pill">
                          {Number(row.days_to_close) < 0
                            ? `${Math.abs(Number(row.days_to_close))}d overdue`
                            : `${Number(row.days_to_close)}d to close`}
                        </span>
                      ) : null}
                      {numberOrNull(row?.document_count) != null ? (
                        <span className="oh-pill">
                          {Number(row.document_count)} docs
                        </span>
                      ) : null}
                    </div>

                    {row?.next_step ? (
                      <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Next action
                        </div>
                        <div className="mt-2 text-sm text-app-1">
                          {row.next_step}
                        </div>
                      </div>
                    ) : null}

                    <div className="mt-4 flex flex-wrap gap-2">
                      {propertyId ? (
                        <Link
                          to={`/properties/${propertyId}`}
                          className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-3 py-2 text-sm font-medium text-app-0 transition hover:bg-app-panel"
                        >
                          Open property
                          <ArrowUpRight className="h-4 w-4" />
                        </Link>
                      ) : null}

                      {runId ? (
                        <button
                          type="button"
                          onClick={() => setActiveRunId(runId)}
                          className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-3 py-2 text-sm font-medium text-app-0 transition hover:bg-app-panel"
                        >
                          Run errors
                          <Bug className="h-4 w-4" />
                        </button>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </Surface>

        <Surface
          title="Filters"
          subtitle="Investor pane-specific shortlist controls"
        >
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Search className="h-3.5 w-3.5" />
                Search
              </div>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="address, city, county, tag…"
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              />
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <ShieldAlert className="h-3.5 w-3.5" />
                Decision
              </div>
              <select
                value={decision}
                onChange={(e) => setDecision(e.target.value as DecisionFilter)}
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All</option>
                <option value="GOOD_DEAL">Good deal</option>
                <option value="REVIEW">Review</option>
                <option value="REJECT">Reject</option>
              </select>
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Banknote className="h-3.5 w-3.5" />
                Financing
              </div>
              <select
                value={financing}
                onChange={(e) =>
                  setFinancing(e.target.value as FinancingFilter)
                }
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All</option>
                <option value="CASH">Cash</option>
                <option value="DSCR">DSCR</option>
                <option value="UNKNOWN">Unknown</option>
              </select>
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <SlidersHorizontal className="h-3.5 w-3.5" />
                Completeness
              </div>
              <select
                value={completeness}
                onChange={(e) =>
                  setCompleteness(e.target.value as CompletenessFilter)
                }
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All</option>
                <option value="COMPLETE">Enriched</option>
                <option value="PARTIAL">Partial</option>
                <option value="MISSING">Missing</option>
              </select>
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Landmark className="h-3.5 w-3.5" />
                City
              </div>
              <select
                value={selectedCity}
                onChange={(e) => setSelectedCity(e.target.value)}
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="ALL">All cities</option>
                {cityOptions.map((city) => (
                  <option key={city} value={city}>
                    {city}
                  </option>
                ))}
              </select>
            </label>

            <label className="rounded-2xl border border-app bg-app-panel px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <GitBranch className="h-3.5 w-3.5" />
                Sort
              </div>
              <select
                value={sort}
                onChange={(e) => setSort(e.target.value as SortKey)}
                className="w-full bg-transparent text-sm text-app-0 outline-none"
              >
                <option value="BEST_CASHFLOW">Best cashflow</option>
                <option value="LOWEST_PRICE">Lowest price</option>
                <option value="HIGHEST_PRICE">Highest price</option>
                <option value="BEST_DSCR">Best DSCR</option>
                <option value="NEWEST">Newest</option>
              </select>
            </label>
          </div>
        </Surface>

        <Surface className="p-4">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-app-0">
                Available investment inventory
              </div>
              <div className="text-xs text-app-4">
                Showing 25 properties per page. Active acquisition files are
                excluded from this list.
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
                              <span
                                className={decisionPillClass(
                                  r?.normalized_decision || r?.classification,
                                )}
                              >
                                {normalizeDecision(
                                  r?.normalized_decision || r?.classification,
                                ).replace("_", " ")}
                              </span>
                              <span
                                className={completenessPillClass(
                                  completenessValue,
                                )}
                              >
                                {completenessLabel(completenessValue)}
                              </span>
                            </div>
                          </div>

                          <div className="mt-4 grid grid-cols-2 gap-3 xl:grid-cols-4">
                            <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
                              <div className="flex items-center gap-2 text-xs text-app-4">
                                <Wallet className="h-3.5 w-3.5" />
                                Price
                              </div>
                              <div className="mt-2 text-sm font-semibold text-app-0">
                                {money(price)}
                              </div>
                            </div>

                            <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
                              <div className="flex items-center gap-2 text-xs text-app-4">
                                <Banknote className="h-3.5 w-3.5" />
                                Rent
                              </div>
                              <div className="mt-2 text-sm font-semibold text-app-0">
                                {money(rent)}
                              </div>
                            </div>

                            <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
                              <div className="flex items-center gap-2 text-xs text-app-4">
                                <Wallet className="h-3.5 w-3.5" />
                                Cashflow
                              </div>
                              <div
                                className={`mt-2 text-sm font-semibold ${metricTone(cashflow)}`}
                              >
                                {money(cashflow)}
                              </div>
                            </div>

                            <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
                              <div className="flex items-center gap-2 text-xs text-app-4">
                                <Landmark className="h-3.5 w-3.5" />
                                DSCR
                              </div>
                              <div className="mt-2 text-sm font-semibold text-app-0">
                                {dscr != null ? dscr.toFixed(2) : "—"}
                              </div>
                            </div>
                          </div>

                          <div className="mt-4 flex flex-wrap gap-2">
                            {property?.bedrooms != null ? (
                              <span className="oh-pill">
                                <BedDouble className="mr-1 h-3.5 w-3.5" />
                                {property.bedrooms} bd
                              </span>
                            ) : null}
                            {property?.bathrooms != null ? (
                              <span className="oh-pill">
                                <Bath className="mr-1 h-3.5 w-3.5" />
                                {property.bathrooms} ba
                              </span>
                            ) : null}
                            {property?.square_feet != null ? (
                              <span className="oh-pill">
                                <Ruler className="mr-1 h-3.5 w-3.5" />
                                {Number(
                                  property.square_feet,
                                ).toLocaleString()}{" "}
                                sf
                              </span>
                            ) : null}
                            <span className="oh-pill">
                              {getFinancingType(price)}
                            </span>
                            {inferCrime(r) != null ? (
                              <span className="oh-pill">
                                crime {inferCrime(r)}
                              </span>
                            ) : null}
                          </div>

                          {tags.length ? (
                            <div className="mt-3 flex flex-wrap gap-2">
                              {tags.map((tag) => (
                                <span key={tag} className="oh-pill">
                                  {tag}
                                </span>
                              ))}
                            </div>
                          ) : null}

                          <div className="mt-4 flex items-center justify-between gap-3 text-xs text-app-4">
                            <div>
                              updated {relativeTime(inferUpdatedAt(r))}
                              {inferLocationConfidence(r) != null
                                ? ` · geocode ${inferLocationConfidence(r)?.toFixed(2)}`
                                : ""}
                            </div>

                            {propertyId ? (
                              <Link
                                to={`/properties/${propertyId}`}
                                className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-3 py-2 text-sm font-medium text-app-0 transition hover:bg-app-panel"
                              >
                                Open lifecycle
                                <ArrowUpRight className="h-4 w-4" />
                              </Link>
                            ) : null}
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="mt-6 flex flex-wrap items-center justify-between gap-3">
                <button
                  type="button"
                  onClick={() =>
                    setCurrentPage((prev) => Math.max(1, prev - 1))
                  }
                  disabled={safeCurrentPage <= 1}
                  className="oh-btn oh-btn-secondary disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <ChevronLeft className="h-4 w-4" />
                  Previous
                </button>

                <div className="flex flex-wrap items-center gap-2">
                  {pagination.map((page, idx) =>
                    typeof page === "string" ? (
                      <span
                        key={`${page}-${idx}`}
                        className="px-2 text-sm text-app-4"
                      >
                        …
                      </span>
                    ) : (
                      <button
                        key={page}
                        type="button"
                        onClick={() => setCurrentPage(page)}
                        className={`inline-flex h-10 min-w-10 items-center justify-center rounded-2xl border px-3 text-sm ${
                          page === safeCurrentPage
                            ? "border-app-strong bg-app-panel text-app-0"
                            : "border-app bg-app-muted text-app-3"
                        }`}
                      >
                        {page}
                      </button>
                    ),
                  )}
                </div>

                <button
                  type="button"
                  onClick={() =>
                    setCurrentPage((prev) => Math.min(totalPages, prev + 1))
                  }
                  disabled={safeCurrentPage >= totalPages}
                  className="oh-btn oh-btn-secondary disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Next
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>
            </>
          )}
        </Surface>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface title="Market coverage" subtitle="Supported intake markets">
            {marketsLoading ? (
              <div className="text-sm text-app-4">Loading markets…</div>
            ) : markets.length ? (
              <div className="grid gap-3">
                {markets.slice(0, 8).map((market: any, idx) => (
                  <div
                    key={`${market?.city || market?.label || idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-app-0">
                          {marketDisplayName(market)}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {marketSubLabel(market) || "Supported market"}
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={() => setSourcePackMarket(market)}
                        className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-3 py-2 text-xs font-medium text-app-0 transition hover:bg-app-panel"
                      >
                        <Settings2 className="h-4 w-4" />
                        Source pack
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState compact title="No markets loaded" />
            )}
          </Surface>

          <Surface
            title="Average cashflow"
            subtitle="Across visible investor inventory"
          >
            <div
              className={`text-3xl font-semibold ${metricTone(avgCashflow)}`}
            >
              {money(avgCashflow)}
            </div>
          </Surface>

          <Surface
            title="Next lifecycle move"
            subtitle="What this pane should do"
          >
            <div className="space-y-3 text-sm text-app-2">
              <div className="flex items-center gap-2">
                <BriefcaseBusiness className="h-4 w-4" />
                Move shortlisted and reviewable assets into acquisition.
              </div>
              <div className="flex items-center gap-2">
                <GitBranch className="h-4 w-4" />
                The main blocker here is missing enrichment or weak
                underwriting.
              </div>
            </div>
          </Surface>
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
