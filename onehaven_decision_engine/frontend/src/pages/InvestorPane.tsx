import React from "react";
import { Link } from "react-router-dom";
import {
  ArrowUpRight,
  Bath,
  BedDouble,
  BriefcaseBusiness,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Clock3,
  GitBranch,
  ImageOff,
  Landmark,
  Loader2,
  MapPin,
  PanelRightOpen,
  RefreshCcw,
  Search,
  Settings2,
  ShieldAlert,
  Sparkles,
  TrendingUp,
  Wallet,
} from "lucide-react";

import EmptyState from "../components/EmptyState";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import IngestionLaunchCard from "../components/IngestionLaunchCard";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import MarketSourcePackModal from "../components/MarketSourcePackModal";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import PaneSwitcher from "../components/PaneSwitcher";
import RiskBadges from "../components/RiskBadges";
import StatPill from "../components/StatPill";
import Surface from "../components/Surface";
import { api } from "../lib/api";
import type { SupportedMarket } from "../lib/ingestionClient";

type Row = any;
type MarketRow = SupportedMarket;

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR" | "UNKNOWN";
type CompletenessFilter = "ALL" | "COMPLETE" | "PARTIAL" | "MISSING";
type SortKey =
  | "RELEVANCE"
  | "BEST_CASHFLOW"
  | "BEST_DSCR"
  | "BEST_RENT_GAP"
  | "LOWEST_RISK"
  | "NEWEST"
  | "LOWEST_PRICE"
  | "HIGHEST_PRICE";

const INITIAL_LIMIT = 500;
const PAGE_SIZE = 25;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function decimal(v: any, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function numberOrNull(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function percentText(v: any, digits = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}%`;
}

function normalizeDecision(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toUpperCase();

  if (["PASS", "GOOD_DEAL", "GOOD", "APPROVED", "APPROVE", "BUY"].includes(x)) {
    return "GOOD_DEAL";
  }
  if (["REJECT", "FAIL", "FAILED", "NO_GO", "PASS_ON_IT"].includes(x)) {
    return "REJECT";
  }
  return "REVIEW";
}

function decisionLabel(raw?: string) {
  const normalized = normalizeDecision(raw);
  if (normalized === "GOOD_DEAL") return "Good deal";
  if (normalized === "REJECT") return "Reject";
  return "Review";
}

function decisionPillClass(raw?: string) {
  const d = normalizeDecision(raw);
  if (d === "GOOD_DEAL") return "oh-pill oh-pill-good";
  if (d === "REVIEW") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
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
    numberOrNull(r?.last_underwriting_result?.market_rent_estimate) ??
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

function inferRentGap(r: any) {
  const rent = inferMarketRent(r);
  const mortgage = inferMortgage(r);
  if (rent == null && mortgage == null) return null;
  return (rent ?? 0) - (mortgage ?? 0);
}

function inferRiskScore(r: any) {
  const explicit =
    numberOrNull(r?.risk_score) ??
    numberOrNull(r?.last_underwriting_result?.risk_score) ??
    numberOrNull(r?.metrics?.risk_score);

  if (explicit != null) return explicit;

  const crime = inferCrime(r) ?? 0;
  const offenders = inferOffenderCount(r) ?? 0;
  const redZonePenalty = inferIsRedZone(r) ? 25 : 0;
  const completenessPenalty =
    inferCompleteness(r) === "MISSING"
      ? 15
      : inferCompleteness(r) === "PARTIAL"
        ? 7
        : 0;

  return Math.min(
    100,
    crime + offenders * 8 + redZonePenalty + completenessPenalty,
  );
}

function inferCrime(r: any) {
  return (
    numberOrNull(r?.crime_score) ??
    numberOrNull(r?.property?.crime_score) ??
    null
  );
}

function inferOffenderCount(r: any) {
  return (
    numberOrNull(r?.offender_count) ??
    numberOrNull(r?.property?.offender_count) ??
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

function inferGeocodeSource(r: any) {
  return r?.geocode_source || r?.property?.geocode_source || null;
}

function inferGeocodeConfidence(r: any) {
  return (
    numberOrNull(r?.geocode_confidence) ??
    numberOrNull(r?.property?.geocode_confidence) ??
    null
  );
}

function inferIsRedZone(r: any) {
  return Boolean(r?.is_red_zone ?? r?.property?.is_red_zone);
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
  const lat = inferLat(r);
  const lng = inferLng(r);

  const score = [price, rent, lat, lng].filter((x) => x != null).length;
  if (score >= 4) return "COMPLETE";
  if (score >= 2) return "PARTIAL";
  return "MISSING";
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

function metricTone(
  value: number | null | undefined,
  opts?: { inverse?: boolean },
) {
  if (value == null) return "text-app-0";
  if (opts?.inverse) {
    if (value <= 25) return "text-emerald-300";
    if (value <= 50) return "text-amber-200";
    return "text-red-300";
  }
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
  const risk = inferRiskScore(r);
  const isCandidate = inferIsDealCandidate(r);

  if (isCandidate) tags.add("Deal candidate");
  if (
    normalizeDecision(r?.normalized_decision || r?.classification) ===
    "GOOD_DEAL"
  ) {
    tags.add("High conviction");
  }
  if (financing === "CASH") tags.add("Cash");
  if (financing === "DSCR") tags.add("DSCR");
  if (cashflow != null && cashflow > 0) tags.add("Positive cash flow");
  if (dscr != null && dscr >= 1.2) tags.add("Strong DSCR");
  if (completeness === "COMPLETE") tags.add("Fully enriched");
  if (crime != null && crime <= 30) tags.add("Lower crime");
  if (risk != null && risk <= 25) tags.add("Lower risk");

  const p = inferProperty(r);
  if (p?.bedrooms != null && Number(p.bedrooms) >= 3) tags.add("3+ beds");
  if (inferCounty(r)) tags.add(String(inferCounty(r)));

  return Array.from(tags).slice(0, 5);
}

function rankingReason(row: any, sort: SortKey, rank: number) {
  const cashflow = inferCashflow(row);
  const dscr = inferDscr(row);
  const rentGap = inferRentGap(row);
  const risk = inferRiskScore(row);
  const reasons: string[] = [];

  if (rank <= 3) reasons.push(`ranked #${rank} in this view`);

  if (sort === "BEST_CASHFLOW" && cashflow != null) {
    reasons.push(`monthly cashflow is ${money(cashflow)}`);
  } else if (sort === "BEST_DSCR" && dscr != null) {
    reasons.push(`DSCR is ${decimal(dscr, 2)}`);
  } else if (sort === "BEST_RENT_GAP" && rentGap != null) {
    reasons.push(`rent gap is ${money(rentGap)}`);
  } else if (sort === "LOWEST_RISK" && risk != null) {
    reasons.push(`risk score is ${decimal(risk, 0)} and lower is better`);
  } else {
    if (cashflow != null && cashflow > 0)
      reasons.push(`${money(cashflow)} cashflow`);
    if (dscr != null && dscr >= 1.2) reasons.push(`${decimal(dscr, 2)} DSCR`);
    if (risk != null && risk <= 30)
      reasons.push(`${decimal(risk, 0)} risk score`);
    if (rentGap != null && rentGap > 0)
      reasons.push(`${money(rentGap)} rent gap`);
  }

  if (inferCompleteness(row) === "COMPLETE") {
    reasons.push("fully enriched data");
  }

  return reasons.slice(0, 3);
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

function sortToApiValue(sort: SortKey) {
  if (sort === "BEST_CASHFLOW") return "best_cashflow";
  if (sort === "BEST_DSCR") return "best_dscr";
  if (sort === "BEST_RENT_GAP") return "best_rent_gap";
  if (sort === "LOWEST_RISK") return "lowest_risk";
  if (sort === "LOWEST_PRICE") return "lowest_price";
  if (sort === "HIGHEST_PRICE") return "highest_price";
  if (sort === "NEWEST") return "newest";
  return "relevance";
}

function compareRows(a: any, b: any, sort: SortKey) {
  const aCashflow = inferCashflow(a);
  const bCashflow = inferCashflow(b);
  const aDscr = inferDscr(a);
  const bDscr = inferDscr(b);
  const aRentGap = inferRentGap(a);
  const bRentGap = inferRentGap(b);
  const aRisk = inferRiskScore(a);
  const bRisk = inferRiskScore(b);

  if (sort === "BEST_CASHFLOW") {
    return (
      (bCashflow ?? Number.NEGATIVE_INFINITY) -
      (aCashflow ?? Number.NEGATIVE_INFINITY)
    );
  }

  if (sort === "BEST_DSCR") {
    return (
      (bDscr ?? Number.NEGATIVE_INFINITY) - (aDscr ?? Number.NEGATIVE_INFINITY)
    );
  }

  if (sort === "BEST_RENT_GAP") {
    return (
      (bRentGap ?? Number.NEGATIVE_INFINITY) -
      (aRentGap ?? Number.NEGATIVE_INFINITY)
    );
  }

  if (sort === "LOWEST_RISK") {
    return (
      (aRisk ?? Number.POSITIVE_INFINITY) - (bRisk ?? Number.POSITIVE_INFINITY)
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

  const candidateDelta =
    Number(inferIsDealCandidate(b)) - Number(inferIsDealCandidate(a));
  if (candidateDelta !== 0) return candidateDelta;

  const cashflowDelta =
    (bCashflow ?? Number.NEGATIVE_INFINITY) -
    (aCashflow ?? Number.NEGATIVE_INFINITY);
  if (cashflowDelta !== 0) return cashflowDelta;

  const dscrDelta =
    (bDscr ?? Number.NEGATIVE_INFINITY) - (aDscr ?? Number.NEGATIVE_INFINITY);
  if (dscrDelta !== 0) return dscrDelta;

  const riskDelta =
    (aRisk ?? Number.POSITIVE_INFINITY) - (bRisk ?? Number.POSITIVE_INFINITY);
  if (riskDelta !== 0) return riskDelta;

  return (
    new Date(inferUpdatedAt(b) || 0).getTime() -
    new Date(inferUpdatedAt(a) || 0).getTime()
  );
}

function TopDealCard({
  row,
  index,
  sort,
}: {
  row: any;
  index: number;
  sort: SortKey;
}) {
  const property = inferProperty(row);
  const propertyId = resolvePropertyId(row);
  const cashflow = inferCashflow(row);
  const dscr = inferDscr(row);
  const rentGap = inferRentGap(row);
  const risk = inferRiskScore(row);
  const photoUrl = inferPhotoUrl(row);
  const reasons = rankingReason(row, sort, index + 1);

  return (
    <div className="overflow-hidden rounded-3xl border border-app bg-app-panel">
      <div className="h-44 bg-app-muted">
        <Photo url={photoUrl} alt={property?.address || "Property"} />
      </div>

      <div className="p-5">
        <div className="flex items-center justify-between gap-3">
          <div className="oh-pill oh-pill-good">
            <Sparkles className="h-3.5 w-3.5" />
            Top {index + 1}
          </div>
          <span
            className={decisionPillClass(
              row?.normalized_decision || row?.classification,
            )}
          >
            {decisionLabel(row?.normalized_decision || row?.classification)}
          </span>
        </div>

        <div className="mt-3 line-clamp-2 text-base font-semibold text-app-0">
          {property?.address || "Unknown address"}
        </div>

        <div className="mt-1 text-sm text-app-4">
          {[property?.city, property?.state, property?.zip]
            .filter(Boolean)
            .join(", ")}
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <StatPill
            label="Cashflow"
            value={money(cashflow)}
            tone={cashflow != null && cashflow > 0 ? "good" : "warn"}
          />
          <StatPill
            label="DSCR"
            value={decimal(dscr, 2)}
            tone={dscr != null && dscr >= 1.2 ? "good" : "warn"}
          />
          <StatPill
            label="Rent gap"
            value={money(rentGap)}
            tone={rentGap != null && rentGap > 0 ? "good" : "warn"}
          />
          <StatPill
            label="Risk"
            value={decimal(risk, 0)}
            tone={
              risk != null && risk <= 25
                ? "good"
                : risk != null && risk <= 50
                  ? "warn"
                  : "bad"
            }
          />
        </div>

        <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Why this ranks highly
          </div>
          <ul className="mt-2 space-y-1 text-sm text-app-2">
            {reasons.map((reason) => (
              <li key={reason} className="flex items-start gap-2">
                <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" />
                <span>{reason}</span>
              </li>
            ))}
          </ul>
        </div>

        {propertyId ? (
          <Link
            to={`/properties/${propertyId}`}
            className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-app-1 hover:text-app-0"
          >
            Open property
            <ArrowUpRight className="h-4 w-4" />
          </Link>
        ) : null}
      </div>
    </div>
  );
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

  const [activeRunId, setActiveRunId] = React.useState<number | null>(null);
  const [runsOpen, setRunsOpen] = React.useState(false);
  const [sourcePackMarket, setSourcePackMarket] =
    React.useState<SupportedMarket | null>(null);
  const [selectedMarket, setSelectedMarket] =
    React.useState<SupportedMarket | null>(null);
  const [marketRefreshNonce, setMarketRefreshNonce] = React.useState(0);

  const loadInventory = React.useCallback(async () => {
    setInventoryLoading(true);
    setInventoryErr(null);

    try {
      const params: Record<string, any> = {
        limit: INITIAL_LIMIT,
        deals_only: "true",
        include_suppressed: undefined,
        sort: sortToApiValue(sort),
      };

      if (selectedMarket?.state) params.state = selectedMarket.state;
      if (selectedMarket?.county) params.county = selectedMarket.county;
      if (selectedMarket?.city) params.city = selectedMarket.city;
      if (deferredQ.trim()) params.q = deferredQ.trim();

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
  }, [deferredQ, selectedMarket, sort]);

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
      setSelectedMarket((prev) => {
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

  React.useEffect(() => {
    loadMarkets();
  }, [loadMarkets]);

  React.useEffect(() => {
    loadInventory();
  }, [loadInventory, marketRefreshNonce]);

  const filteredRows = React.useMemo(() => {
    return rows
      .filter((row) => {
        const hiddenReason = inferHiddenReason(row);
        const status = inferDealFilterStatus(row);

        if (hiddenReason === "inactive_listing") return false;
        if (!inferIsDealCandidate(row)) return false;
        if (status === "suppressed" || status === "hidden") return false;

        const normalizedDecision = normalizeDecision(
          row?.normalized_decision || row?.classification,
        );
        if (decision !== "ALL" && normalizedDecision !== decision) return false;

        const financingType = getFinancingType(inferAskingPrice(row));
        if (financing !== "ALL" && financingType !== financing) return false;

        const completenessValue = inferCompleteness(row);
        if (completeness !== "ALL" && completenessValue !== completeness) {
          return false;
        }

        if (deferredQ.trim()) {
          const haystack = [
            row?.address,
            row?.normalized_address,
            row?.city,
            row?.state,
            row?.zip,
            row?.county,
            row?.property?.address,
            row?.property?.city,
            row?.property?.state,
            row?.property?.zip,
            row?.property?.county,
          ]
            .filter(Boolean)
            .join(" ")
            .toLowerCase();

          if (!haystack.includes(deferredQ.trim().toLowerCase())) return false;
        }

        return true;
      })
      .sort((a, b) => compareRows(a, b, sort));
  }, [rows, decision, financing, completeness, deferredQ, sort]);

  const topDeals = React.useMemo(
    () => filteredRows.slice(0, 3),
    [filteredRows],
  );

  const stats = React.useMemo(() => {
    const totals = filteredRows.reduce(
      (acc, row) => {
        const cashflow = inferCashflow(row);
        const dscr = inferDscr(row);
        const rentGap = inferRentGap(row);
        const risk = inferRiskScore(row);

        if (cashflow != null) {
          acc.cashflowSum += cashflow;
          acc.cashflowCount += 1;
        }
        if (dscr != null) {
          acc.dscrSum += dscr;
          acc.dscrCount += 1;
        }
        if (rentGap != null) {
          acc.rentGapSum += rentGap;
          acc.rentGapCount += 1;
        }
        if (risk != null) {
          acc.riskSum += risk;
          acc.riskCount += 1;
        }
        return acc;
      },
      {
        cashflowSum: 0,
        cashflowCount: 0,
        dscrSum: 0,
        dscrCount: 0,
        rentGapSum: 0,
        rentGapCount: 0,
        riskSum: 0,
        riskCount: 0,
      },
    );

    return {
      visible: filteredRows.length,
      avgCashflow:
        totals.cashflowCount > 0
          ? totals.cashflowSum / totals.cashflowCount
          : null,
      avgDscr: totals.dscrCount > 0 ? totals.dscrSum / totals.dscrCount : null,
      avgRentGap:
        totals.rentGapCount > 0
          ? totals.rentGapSum / totals.rentGapCount
          : null,
      avgRisk: totals.riskCount > 0 ? totals.riskSum / totals.riskCount : null,
    };
  }, [filteredRows]);

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
    sort,
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
        title="Ranked deal inventory"
        subtitle="This view stays deal-focused. Inactive and suppressed inventory are kept out, top candidates are surfaced first, and every row shows why it ranks where it does."
        right={<PaneSwitcher activePane="investor" />}
      />

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
        <div className="space-y-6">
          <Surface
            title="Ranking controls"
            subtitle="Search one supported market at a time, then sort by the deal signal you care about."
          >
            <div className="grid gap-4 lg:grid-cols-[1.5fr_1fr_auto]">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <BriefcaseBusiness className="h-4 w-4" />
                  Market
                </div>

                <select
                  value={selectedMarket?.slug || ""}
                  onChange={(e) =>
                    setSelectedMarket(
                      markets.find((m) => m.slug === e.target.value) || null,
                    )
                  }
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  {markets.map((market) => (
                    <option key={market.slug} value={market.slug}>
                      {marketDisplayName(market)}
                    </option>
                  ))}
                </select>

                {selectedMarket ? (
                  <div className="mt-2 text-xs text-app-4">
                    {marketSubLabel(selectedMarket)}
                  </div>
                ) : null}
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <Search className="h-4 w-4" />
                  Search
                </div>
                <input
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Address, city, zip, county"
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                />
              </div>

              <button
                type="button"
                onClick={refreshSelectedMarket}
                disabled={refreshing || !selectedMarket?.slug}
                className="inline-flex h-full min-h-[88px] items-center justify-center gap-2 rounded-2xl border border-app bg-app-panel px-5 py-3 text-sm font-medium text-app-0 transition hover:bg-app-muted disabled:cursor-not-allowed disabled:opacity-60"
              >
                {refreshing ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCcw className="h-4 w-4" />
                )}
                Sync now
              </button>
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-5">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <GitBranch className="h-4 w-4" />
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

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3 md:col-span-2">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                  <Settings2 className="h-4 w-4" />
                  Sort ranked deals by
                </div>
                <select
                  value={sort}
                  onChange={(e) => setSort(e.target.value as SortKey)}
                  className="mt-2 w-full rounded-xl border border-app bg-app-muted px-3 py-2 text-sm text-app-0 outline-none"
                >
                  <option value="RELEVANCE">Relevance</option>
                  <option value="BEST_CASHFLOW">Best cashflow</option>
                  <option value="BEST_DSCR">Best DSCR</option>
                  <option value="BEST_RENT_GAP">Best rent gap</option>
                  <option value="LOWEST_RISK">Lowest risk</option>
                  <option value="NEWEST">Newest</option>
                  <option value="LOWEST_PRICE">Lowest price</option>
                  <option value="HIGHEST_PRICE">Highest price</option>
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

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <Surface
              title="Visible deals"
              subtitle="Filtered active ranked deals only"
            >
              <div className="text-3xl font-semibold text-app-0">
                {stats.visible}
              </div>
            </Surface>

            <Surface title="Average cashflow">
              <div
                className={`text-3xl font-semibold ${metricTone(stats.avgCashflow)}`}
              >
                {money(stats.avgCashflow)}
              </div>
            </Surface>

            <Surface title="Average DSCR">
              <div
                className={`text-3xl font-semibold ${metricTone(stats.avgDscr)}`}
              >
                {decimal(stats.avgDscr, 2)}
              </div>
            </Surface>

            <Surface title="Average risk">
              <div
                className={`text-3xl font-semibold ${metricTone(stats.avgRisk, { inverse: true })}`}
              >
                {decimal(stats.avgRisk, 0)}
              </div>
            </Surface>
          </div>

          <Surface
            title="Top ranked deals"
            subtitle="The three strongest candidates in the current filtered view."
          >
            {inventoryLoading || marketsLoading ? (
              <div className="flex items-center justify-center py-16 text-app-4">
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            ) : !topDeals.length ? (
              <EmptyState
                title="No ranked deals yet"
                description="Try a different supported market or relax one of the filters."
              />
            ) : (
              <div className="grid gap-4 xl:grid-cols-3">
                {topDeals.map((row, index) => (
                  <TopDealCard
                    key={`${resolvePropertyId(row) || index}-${index}`}
                    row={row}
                    index={index}
                    sort={sort}
                  />
                ))}
              </div>
            )}
          </Surface>

          <Surface
            title="Ranked deal list"
            subtitle="Every row shows the main financial signals and the reason the property is floating to the top."
          >
            {inventoryLoading || marketsLoading ? (
              <div className="flex items-center justify-center py-16 text-app-4">
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            ) : !pageRows.length ? (
              <EmptyState
                title="No properties match the current filters"
                description="Try a different market, search term, or ranking sort."
              />
            ) : (
              <>
                <div className="grid gap-4">
                  {pageRows.map((row, index) => {
                    const property = inferProperty(row);
                    const propertyId = resolvePropertyId(row);
                    const askingPrice = inferAskingPrice(row);
                    const marketRent = inferMarketRent(row);
                    const cashflow = inferCashflow(row);
                    const dscr = inferDscr(row);
                    const rentGap = inferRentGap(row);
                    const risk = inferRiskScore(row);
                    const completenessValue = inferCompleteness(row);
                    const photoUrl = inferPhotoUrl(row);
                    const tags = inferTags(row);
                    const updatedAt = inferUpdatedAt(row);
                    const absoluteRank =
                      (safeCurrentPage - 1) * PAGE_SIZE + index + 1;
                    const reasons = rankingReason(row, sort, absoluteRank);

                    return (
                      <div
                        key={
                          propertyId ||
                          `${property?.address || "property"}-${absoluteRank}`
                        }
                        className="overflow-hidden rounded-3xl border border-app bg-app-panel"
                      >
                        <div className="grid gap-0 xl:grid-cols-[260px_minmax(0,1fr)]">
                          <div className="h-[220px] bg-app-muted xl:h-full">
                            <Photo
                              url={photoUrl}
                              alt={property?.address || "Property"}
                            />
                          </div>

                          <div className="p-5">
                            <div className="flex flex-wrap items-start justify-between gap-4">
                              <div className="min-w-0">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="oh-pill">
                                    <TrendingUp className="h-3.5 w-3.5" />
                                    Rank #{absoluteRank}
                                  </span>
                                  <span
                                    className={decisionPillClass(
                                      row?.normalized_decision ||
                                        row?.classification,
                                    )}
                                  >
                                    {decisionLabel(
                                      row?.normalized_decision ||
                                        row?.classification,
                                    )}
                                  </span>
                                  <span
                                    className={completenessPillClass(
                                      completenessValue,
                                    )}
                                  >
                                    {completenessLabel(completenessValue)}
                                  </span>
                                  <span className="oh-pill oh-pill-good">
                                    Active deal
                                  </span>
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

                            <div className="mt-4 flex flex-wrap gap-2">
                              <StatPill
                                label="Cashflow"
                                value={money(cashflow)}
                                tone={
                                  cashflow != null && cashflow > 0
                                    ? "good"
                                    : cashflow != null && cashflow < 0
                                      ? "bad"
                                      : "neutral"
                                }
                              />
                              <StatPill
                                label="DSCR"
                                value={decimal(dscr, 2)}
                                tone={
                                  dscr != null && dscr >= 1.2
                                    ? "good"
                                    : dscr != null && dscr < 1
                                      ? "bad"
                                      : "warn"
                                }
                              />
                              <StatPill
                                label="Rent gap"
                                value={money(rentGap)}
                                tone={
                                  rentGap != null && rentGap > 0
                                    ? "good"
                                    : rentGap != null && rentGap < 0
                                      ? "bad"
                                      : "neutral"
                                }
                              />
                              <StatPill
                                label="Risk"
                                value={decimal(risk, 0)}
                                tone={
                                  risk != null && risk <= 25
                                    ? "good"
                                    : risk != null && risk <= 50
                                      ? "warn"
                                      : "bad"
                                }
                              />
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
                                <div
                                  className={`mt-2 text-lg font-semibold ${metricTone(dscr)}`}
                                >
                                  {decimal(dscr, 2)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <TrendingUp className="h-4 w-4" />
                                  Rent gap
                                </div>
                                <div
                                  className={`mt-2 text-lg font-semibold ${metricTone(rentGap)}`}
                                >
                                  {money(rentGap)}
                                </div>
                                <div className="mt-1 text-xs text-app-4">
                                  Rent {money(marketRent)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                                  <ShieldAlert className="h-4 w-4" />
                                  Risk
                                </div>
                                <div
                                  className={`mt-2 text-lg font-semibold ${metricTone(risk, { inverse: true })}`}
                                >
                                  {decimal(risk, 0)}
                                </div>
                                <div className="mt-1 text-xs text-app-4">
                                  Lower is better
                                </div>
                              </div>
                            </div>

                            <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                                Why this deal is ranking highly
                              </div>
                              <div className="mt-2 flex flex-wrap gap-2">
                                {reasons.map((reason) => (
                                  <span key={reason} className="oh-pill">
                                    {reason}
                                  </span>
                                ))}
                              </div>
                            </div>

                            <div className="mt-4">
                              <RiskBadges
                                compact
                                county={inferCounty(row)}
                                isRedZone={inferIsRedZone(row)}
                                crimeScore={inferCrime(row)}
                                offenderCount={inferOffenderCount(row)}
                                lat={inferLat(row)}
                                lng={inferLng(row)}
                                normalizedAddress={inferNormalizedAddress(row)}
                                geocodeSource={inferGeocodeSource(row)}
                                geocodeConfidence={inferGeocodeConfidence(row)}
                              />
                            </div>

                            <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
                              <div className="flex flex-wrap gap-2">
                                {tags.map((tag) => (
                                  <span key={tag} className="oh-pill">
                                    {tag}
                                  </span>
                                ))}
                              </div>

                              <div className="flex items-center gap-3">
                                <div className="text-xs text-app-4">
                                  {property?.bedrooms != null ? (
                                    <span className="inline-flex items-center gap-1">
                                      <BedDouble className="h-4 w-4" />
                                      {property.bedrooms} bd
                                    </span>
                                  ) : null}
                                  {property?.bathrooms != null ? (
                                    <span className="ml-3 inline-flex items-center gap-1">
                                      <Bath className="h-4 w-4" />
                                      {property.bathrooms} ba
                                    </span>
                                  ) : null}
                                </div>

                                {propertyId ? (
                                  <Link
                                    to={`/properties/${propertyId}`}
                                    className="inline-flex items-center gap-2 rounded-xl border border-app bg-app-muted px-3 py-2 text-sm font-medium text-app-0 transition hover:bg-app-panel"
                                  >
                                    Open
                                    <ArrowUpRight className="h-4 w-4" />
                                  </Link>
                                ) : null}
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {totalPages > 1 ? (
                  <div className="mt-6 flex flex-wrap items-center justify-between gap-3">
                    <div className="text-sm text-app-4">
                      Showing {(safeCurrentPage - 1) * PAGE_SIZE + 1}–
                      {Math.min(
                        safeCurrentPage * PAGE_SIZE,
                        filteredRows.length,
                      )}{" "}
                      of {filteredRows.length}
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setCurrentPage((p) => Math.max(1, p - 1))
                        }
                        disabled={safeCurrentPage <= 1}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-app bg-app-panel text-app-0 disabled:opacity-50"
                      >
                        <ChevronLeft className="h-4 w-4" />
                      </button>

                      {pagination.map((item, index) =>
                        typeof item === "string" ? (
                          <span
                            key={`${item}-${index}`}
                            className="px-2 text-app-4"
                          >
                            {item}
                          </span>
                        ) : (
                          <button
                            key={item}
                            type="button"
                            onClick={() => setCurrentPage(item)}
                            className={`inline-flex h-9 min-w-9 items-center justify-center rounded-xl border px-3 text-sm ${
                              item === safeCurrentPage
                                ? "border-app-strong bg-app-muted text-app-0"
                                : "border-app bg-app-panel text-app-3"
                            }`}
                          >
                            {item}
                          </button>
                        ),
                      )}

                      <button
                        type="button"
                        onClick={() =>
                          setCurrentPage((p) => Math.min(totalPages, p + 1))
                        }
                        disabled={safeCurrentPage >= totalPages}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-app bg-app-panel text-app-0 disabled:opacity-50"
                      >
                        <ChevronRight className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                ) : null}
              </>
            )}
          </Surface>
        </div>

        <div className="space-y-6">
          <Surface
            title="Current market"
            subtitle="Selected supported market and current ranking mode."
          >
            {selectedMarket ? (
              <div className="space-y-3">
                <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                  <div className="text-sm font-semibold text-app-0">
                    {marketDisplayName(selectedMarket)}
                  </div>
                  <div className="mt-1 text-sm text-app-4">
                    {marketSubLabel(selectedMarket)}
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Active sort
                    </div>
                    <div className="mt-2 text-sm font-medium text-app-0">
                      {sort === "RELEVANCE"
                        ? "Relevance"
                        : sort === "BEST_CASHFLOW"
                          ? "Best cashflow"
                          : sort === "BEST_DSCR"
                            ? "Best DSCR"
                            : sort === "BEST_RENT_GAP"
                              ? "Best rent gap"
                              : sort === "LOWEST_RISK"
                                ? "Lowest risk"
                                : sort === "LOWEST_PRICE"
                                  ? "Lowest price"
                                  : sort === "HIGHEST_PRICE"
                                    ? "Highest price"
                                    : "Newest"}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Coverage tier
                    </div>
                    <div className="mt-2 text-sm font-medium text-app-0">
                      {selectedMarket.coverage_tier || "—"}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Price band
                    </div>
                    <div className="mt-2 text-sm font-medium text-app-0">
                      {money(selectedMarket.min_price)} to{" "}
                      {money(selectedMarket.max_price)}
                    </div>
                  </div>
                </div>

                <button
                  type="button"
                  onClick={() => setSourcePackMarket(selectedMarket)}
                  className="inline-flex w-full items-center justify-center gap-2 rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm font-medium text-app-0 hover:bg-app-muted"
                >
                  <PanelRightOpen className="h-4 w-4" />
                  View market source pack
                </button>
              </div>
            ) : (
              <EmptyState title="No supported market selected" />
            )}
          </Surface>

          <IngestionLaunchCard
            market={selectedMarket}
            onRunQueued={() => {
              setMarketRefreshNonce((v) => v + 1);
              setRunsOpen(true);
            }}
            onManageSources={() => setSourcePackMarket(selectedMarket)}
          />

          <Surface
            title="Recent sync activity"
            subtitle="Keep sync feedback close to the investor pane."
          >
            <button
              type="button"
              onClick={() => setRunsOpen((v) => !v)}
              className="inline-flex w-full items-center justify-between rounded-2xl border border-app bg-app-panel px-4 py-3 text-left text-sm text-app-0 hover:bg-app-muted"
            >
              <span className="inline-flex items-center gap-2">
                <Clock3 className="h-4 w-4" />
                {runsOpen ? "Hide run history" : "Show run history"}
              </span>
              <ChevronRight
                className={`h-4 w-4 transition ${runsOpen ? "rotate-90" : ""}`}
              />
            </button>

            {runsOpen ? (
              <div className="mt-4">
                <IngestionRunsPanel
                  open={runsOpen}
                  refreshKey={marketRefreshNonce}
                  onClose={() => setRunsOpen(false)}
                  onSelectRun={(runId) => setActiveRunId(runId)}
                />
              </div>
            ) : null}
          </Surface>

          <IngestionErrorsDrawer
            runId={activeRunId}
            onClose={() => setActiveRunId(null)}
          />
        </div>
      </div>

      <MarketSourcePackModal
        open={Boolean(sourcePackMarket)}
        market={sourcePackMarket}
        onClose={() => setSourcePackMarket(null)}
      />
    </PageShell>
  );
}

