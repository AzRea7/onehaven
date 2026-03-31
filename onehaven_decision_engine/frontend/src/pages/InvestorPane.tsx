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
import AppSelect from "../components/AppSelect";

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

const INITIAL_LIMIT = 250;
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
  if (normalized === "REJECT") return "Not a deal";
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
    numberOrNull(r?.market_reference_rent) ??
    numberOrNull(r?.market_rent_estimate) ??
    numberOrNull(r?.inventory_snapshot?.market_reference_rent) ??
    numberOrNull(r?.inventory_snapshot?.market_rent_estimate) ??
    numberOrNull(r?.rent_assumption?.market_rent_estimate) ??
    numberOrNull(r?.monthly_rent_estimate) ??
    numberOrNull(r?.estimated_rent) ??
    numberOrNull(r?.last_underwriting_result?.market_rent_estimate) ??
    null
  );
}

function inferRentUsed(r: any) {
  return (
    numberOrNull(r?.rent_used) ??
    numberOrNull(r?.inventory_snapshot?.rent_used) ??
    numberOrNull(r?.rent_assumption?.rent_used) ??
    null
  );
}

function inferMortgage(r: any) {
  return (
    numberOrNull(r?.monthly_debt_service) ??
    numberOrNull(r?.inventory_snapshot?.monthly_debt_service) ??
    numberOrNull(r?.last_underwriting_result?.monthly_debt_service) ??
    numberOrNull(r?.last_underwriting_result?.mortgage_payment) ??
    numberOrNull(r?.estimated_mortgage) ??
    numberOrNull(r?.mortgage_estimate) ??
    numberOrNull(r?.monthly_mortgage_payment) ??
    null
  );
}

function inferMonthlyTaxes(r: any) {
  return (
    numberOrNull(r?.monthly_taxes) ??
    numberOrNull(r?.inventory_snapshot?.monthly_taxes) ??
    numberOrNull(r?.last_underwriting_result?.monthly_taxes) ??
    numberOrNull(r?.monthly_tax_estimate) ??
    null
  );
}

function inferTaxAnnual(r: any) {
  return (
    numberOrNull(r?.property_tax_annual) ??
    numberOrNull(r?.inventory_snapshot?.property_tax_annual) ??
    null
  );
}

function inferTaxSource(r: any) {
  return (
    r?.property_tax_source || r?.inventory_snapshot?.property_tax_source || null
  );
}

function inferMonthlyInsurance(r: any) {
  return (
    numberOrNull(r?.monthly_insurance) ??
    numberOrNull(r?.inventory_snapshot?.monthly_insurance) ??
    numberOrNull(r?.last_underwriting_result?.monthly_insurance) ??
    numberOrNull(r?.monthly_insurance_estimate) ??
    null
  );
}

function inferInsuranceAnnual(r: any) {
  return (
    numberOrNull(r?.insurance_annual) ??
    numberOrNull(r?.inventory_snapshot?.insurance_annual) ??
    null
  );
}

function inferInsuranceSource(r: any) {
  return r?.insurance_source || r?.inventory_snapshot?.insurance_source || null;
}

function inferMonthlyHousingCost(r: any) {
  return (
    numberOrNull(r?.monthly_housing_cost) ??
    numberOrNull(r?.inventory_snapshot?.monthly_housing_cost) ??
    (() => {
      const mortgage = inferMortgage(r);
      const taxes = inferMonthlyTaxes(r);
      const insurance = inferMonthlyInsurance(r);

      if (mortgage == null && taxes == null && insurance == null) return null;
      return (mortgage ?? 0) + (taxes ?? 0) + (insurance ?? 0);
    })()
  );
}

function inferCashflow(r: any) {
  const direct =
    numberOrNull(r?.projected_monthly_cashflow) ??
    numberOrNull(r?.inventory_snapshot?.projected_monthly_cashflow) ??
    numberOrNull(r?.cashflow_estimate) ??
    numberOrNull(r?.last_underwriting_result?.cash_flow) ??
    numberOrNull(r?.last_underwriting_result?.cashflow) ??
    numberOrNull(r?.property_net_cash_window) ??
    numberOrNull(r?.metrics?.cashflow_estimate);

  if (direct != null) return direct;

  const rent = inferRentUsed(r) ?? inferMarketRent(r);
  const housing = inferMonthlyHousingCost(r);

  if (rent == null || housing == null) return null;
  return rent - housing;
}

function inferRentGap(r: any) {
  return (
    numberOrNull(r?.rent_gap) ??
    numberOrNull(r?.inventory_snapshot?.rent_gap) ??
    (() => {
      const rent = inferMarketRent(r);
      const housing = inferMonthlyHousingCost(r);
      if (rent == null || housing == null) return null;
      return rent - housing;
    })()
  );
}

function inferDscr(r: any) {
  return (
    numberOrNull(r?.dscr) ??
    numberOrNull(r?.inventory_snapshot?.dscr) ??
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
    String(r?.hidden_reason || r?.listing_hidden_reason || "")
      .trim()
      .toLowerCase() || null
  );
}

function inferCompleteness(r: any): "COMPLETE" | "PARTIAL" | "MISSING" {
  const explicit = String(
    r?.completeness || r?.snapshot_completeness || r?.inventory_status || "",
  )
    .trim()
    .toUpperCase();

  if (["COMPLETE", "PARTIAL", "MISSING"].includes(explicit)) {
    return explicit as "COMPLETE" | "PARTIAL" | "MISSING";
  }

  const price = inferAskingPrice(r);
  const rent = inferRentUsed(r) ?? inferMarketRent(r);
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
  else tags.add("Not deal");

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
  const mortgage = inferMortgage(row);
  const monthlyTaxes = inferMonthlyTaxes(row);
  const monthlyInsurance = inferMonthlyInsurance(row);
  const monthlyHousingCost = inferMonthlyHousingCost(row);

  const content = (
    <div className="overflow-visible rounded-3xl border border-app bg-app-panel transition hover:border-app-strong hover:bg-app-muted/30">
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

        <div className="mt-3 grid grid-cols-2 gap-2 xl:grid-cols-4">
          <StatPill label="Mortgage" value={money(mortgage)} tone="neutral" />
          <StatPill label="Taxes" value={money(monthlyTaxes)} tone="neutral" />
          <StatPill
            label="Insurance"
            value={money(monthlyInsurance)}
            tone="neutral"
          />
          <StatPill
            label="Housing total"
            value={money(monthlyHousingCost)}
            tone="neutral"
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
          <div className="mt-4 inline-flex items-center gap-2 text-sm font-medium text-app-1">
            Open property
            <ArrowUpRight className="h-4 w-4" />
          </div>
        ) : null}
      </div>
    </div>
  );

  if (!propertyId) return content;

  return (
    <Link to={`/properties/${propertyId}`} className="block">
      {content}
    </Link>
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
  const [dealsOnly, setDealsOnly] = React.useState(false);
  const [includeSuppressed, setIncludeSuppressed] = React.useState(false);

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
        deals_only: dealsOnly ? "true" : "false",
        include_suppressed: includeSuppressed ? "true" : "false",
      };

      if (selectedMarket?.state) params.state = selectedMarket.state;
      if (selectedMarket?.county) params.county = selectedMarket.county;
      if (selectedMarket?.city) params.city = selectedMarket.city;
      if (deferredQ.trim()) params.q = deferredQ.trim();

      const paneRes = await api.get<any>("/dashboard/panes/investor", {
        params,
      });
      const paneRows = paneRes?.rows || [];
      const normalized = Array.isArray(paneRows) ? paneRows : [];
      setRows(normalized);
    } catch (e: any) {
      setInventoryErr(formatApiError(e, "Failed to load investor inventory."));
      setRows([]);
    } finally {
      setInventoryLoading(false);
    }
  }, [deferredQ, selectedMarket, dealsOnly, includeSuppressed]);

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

  React.useEffect(() => {
    setCurrentPage(1);
  }, [
    deferredQ,
    decision,
    financing,
    completeness,
    sort,
    selectedMarket?.slug,
    dealsOnly,
    includeSuppressed,
  ]);

  const filteredRows = React.useMemo(() => {
    return rows
      .filter((row) => {
        const hiddenReason = inferHiddenReason(row);
        const status = inferDealFilterStatus(row);

        if (hiddenReason === "inactive_listing") return false;
        if (
          !includeSuppressed &&
          (status === "suppressed" || status === "hidden")
        ) {
          return false;
        }

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

        if (dealsOnly && !inferIsDealCandidate(row)) return false;

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
  }, [
    rows,
    decision,
    financing,
    completeness,
    deferredQ,
    sort,
    dealsOnly,
    includeSuppressed,
  ]);

  const topDeals = React.useMemo(
    () => filteredRows.filter((row) => inferIsDealCandidate(row)).slice(0, 3),
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
        if (inferIsDealCandidate(row)) acc.dealCount += 1;
        else acc.notDealCount += 1;

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
        dealCount: 0,
        notDealCount: 0,
      },
    );

    return {
      visible: filteredRows.length,
      dealCount: totals.dealCount,
      notDealCount: totals.notDealCount,
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
      <div className="space-y-6">
        <PageHero
          eyebrow="Investor"
          title=""          
        />

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div className="space-y-6">
            <Surface
              title="Ranking controls"
              subtitle="Search one supported market at a time, then sort and filter the list view."
            >
              <div className="grid gap-4 lg:grid-cols-[1.5fr_1fr_auto]">
                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <BriefcaseBusiness className="h-4 w-4" />
                    Market
                  </div>
                  <div className="mt-2">
                    <AppSelect
                      value={selectedMarket?.slug || ""}
                      onChange={(value) =>
                        setSelectedMarket(
                          markets.find((m) => m.slug === value) || null,
                        )
                      }
                      options={markets.map((market) => ({
                        value: market.slug,
                        label: marketDisplayName(market),
                      }))}
                    />
                  </div>

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

              <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-6">
                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <GitBranch className="h-4 w-4" />
                    Decision
                  </div>

                  <div className="mt-2">
                    <AppSelect
                      value={decision}
                      onChange={(value) => setDecision(value as DecisionFilter)}
                      options={[
                        { value: "ALL", label: "All" },
                        { value: "GOOD_DEAL", label: "Good deal" },
                        { value: "REVIEW", label: "Review" },
                        { value: "REJECT", label: "Reject" },
                      ]}
                    />
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <Landmark className="h-4 w-4" />
                    Financing
                  </div>
                  <div className="mt-2">
                    <AppSelect
                      value={financing}
                      onChange={(value) =>
                        setFinancing(value as FinancingFilter)
                      }
                      options={[
                        { value: "ALL", label: "All" },
                        { value: "CASH", label: "Cash" },
                        { value: "DSCR", label: "DSCR" },
                        { value: "UNKNOWN", label: "Unknown" },
                      ]}
                    />
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <CheckCircle2 className="h-4 w-4" />
                    Completeness
                  </div>
                  <div className="mt-2">
                    <AppSelect
                      value={completeness}
                      onChange={(value) =>
                        setCompleteness(value as CompletenessFilter)
                      }
                      options={[
                        { value: "ALL", label: "All" },
                        { value: "COMPLETE", label: "Complete" },
                        { value: "PARTIAL", label: "Partial" },
                        { value: "MISSING", label: "Missing" },
                      ]}
                    />
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <Wallet className="h-4 w-4" />
                    Deals only
                  </div>
                  <label className="oh-toggle mt-2 text-sm">
                    <input
                      type="checkbox"
                      checked={dealsOnly}
                      onChange={(e) => setDealsOnly(e.target.checked)}
                    />
                    <span>
                      {dealsOnly ? "Showing only deals" : "Showing all rows"}
                    </span>
                  </label>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <ShieldAlert className="h-4 w-4" />
                    Suppressed
                  </div>
                  <label className="oh-toggle mt-2 text-sm">
                    <input
                      type="checkbox"
                      checked={includeSuppressed}
                      onChange={(e) => setIncludeSuppressed(e.target.checked)}
                    />
                    <span>{includeSuppressed ? "Included" : "Hidden"}</span>
                  </label>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-app-4">
                    <Settings2 className="h-4 w-4" />
                    Sort
                  </div>
                  <div className="mt-2">
                    <AppSelect
                      value={sort}
                      onChange={(value) => setSort(value as SortKey)}
                      options={[
                        { value: "RELEVANCE", label: "Relevance" },
                        { value: "BEST_CASHFLOW", label: "Best cashflow" },
                        { value: "BEST_DSCR", label: "Best DSCR" },
                        { value: "BEST_RENT_GAP", label: "Best rent gap" },
                        { value: "LOWEST_RISK", label: "Lowest risk" },
                        { value: "NEWEST", label: "Newest" },
                        { value: "LOWEST_PRICE", label: "Lowest price" },
                        { value: "HIGHEST_PRICE", label: "Highest price" },
                      ]}
                    />
                  </div>
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
              title="Top ranked deals"
              subtitle="The strongest deal candidates in the current filtered view."
            >
              {inventoryLoading || marketsLoading ? (
                <div className="flex items-center justify-center py-16 text-app-4">
                  <Loader2 className="h-5 w-5 animate-spin" />
                </div>
              ) : !topDeals.length ? (
                <EmptyState
                  title="No ranked deals yet"
                  description="Turn Deals only off or relax one of the filters."
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
              title="House list view"
              subtitle="This list shows whether each property is a deal or not a deal. Click anywhere on a card to open the property page."
            >
              {inventoryLoading || marketsLoading ? (
                <div className="flex items-center justify-center py-16 text-app-4">
                  <Loader2 className="h-5 w-5 animate-spin" />
                </div>
              ) : !pageRows.length ? (
                <EmptyState
                  title="No properties match the current filters"
                  description="Try a different market, search term, or turn Deals only off."
                />
              ) : (
                <>
                  <div className="grid gap-4">
                    {pageRows.map((row, index) => {
                      const property = inferProperty(row);
                      const propertyId = resolvePropertyId(row);
                      const askingPrice = inferAskingPrice(row);
                      const marketRent = inferMarketRent(row);
                      const rentUsed = inferRentUsed(row);
                      const cashflow = inferCashflow(row);
                      const dscr = inferDscr(row);
                      const rentGap = inferRentGap(row);
                      const risk = inferRiskScore(row);
                      const completenessValue = inferCompleteness(row);
                      const photoUrl = inferPhotoUrl(row);
                      const tags = inferTags(row);
                      const updatedAt = inferUpdatedAt(row);
                      const mortgage = inferMortgage(row);
                      const monthlyTaxes = inferMonthlyTaxes(row);
                      const monthlyInsurance = inferMonthlyInsurance(row);
                      const monthlyHousingCost = inferMonthlyHousingCost(row);
                      const taxAnnual = inferTaxAnnual(row);
                      const taxSource = inferTaxSource(row);
                      const insuranceAnnual = inferInsuranceAnnual(row);
                      const insuranceSource = inferInsuranceSource(row);
                      const absoluteRank =
                        (safeCurrentPage - 1) * PAGE_SIZE + index + 1;
                      const reasons = rankingReason(row, sort, absoluteRank);
                      const isDeal = inferIsDealCandidate(row);

                      const content = (
                        <div className="overflow-visible rounded-3xl border border-app bg-app-panel transition hover:border-app-strong hover:bg-app-muted/30">
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
                                    <span
                                      className={
                                        isDeal
                                          ? "oh-pill oh-pill-good"
                                          : "oh-pill oh-pill-warn"
                                      }
                                    >
                                      {isDeal ? "Deal" : "Not deal / review"}
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
                                  label="Rent"
                                  value={money(rentUsed ?? marketRent)}
                                  tone="neutral"
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

                              <div className="mt-3 grid grid-cols-2 gap-2 xl:grid-cols-4">
                                <StatPill
                                  label="Mortgage"
                                  value={money(mortgage)}
                                  tone="neutral"
                                />
                                <StatPill
                                  label="Taxes"
                                  value={money(monthlyTaxes)}
                                  tone="neutral"
                                />
                                <StatPill
                                  label="Insurance"
                                  value={money(monthlyInsurance)}
                                  tone="neutral"
                                />
                                <StatPill
                                  label="Housing total"
                                  value={money(monthlyHousingCost)}
                                  tone="neutral"
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
                                    Rent {money(rentUsed ?? marketRent)}
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
                                  Why this row ranks here
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
                                  normalizedAddress={inferNormalizedAddress(
                                    row,
                                  )}
                                  geocodeSource={inferGeocodeSource(row)}
                                  geocodeConfidence={inferGeocodeConfidence(
                                    row,
                                  )}
                                />
                              </div>

                              {taxAnnual != null || insuranceAnnual != null ? (
                                <div className="mt-4 text-xs text-app-4">
                                  {taxAnnual != null ? (
                                    <div>
                                      Property tax annual {money(taxAnnual)}
                                      {taxSource ? ` • ${taxSource}` : ""}
                                    </div>
                                  ) : null}
                                  {insuranceAnnual != null ? (
                                    <div>
                                      Insurance annual {money(insuranceAnnual)}
                                      {insuranceSource
                                        ? ` • ${insuranceSource}`
                                        : ""}
                                    </div>
                                  ) : null}
                                </div>
                              ) : null}

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
                                    <div className="inline-flex items-center gap-2 rounded-xl border border-app bg-app-muted px-3 py-2 text-sm font-medium text-app-0">
                                      Open
                                      <ArrowUpRight className="h-4 w-4" />
                                    </div>
                                  ) : null}
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      );

                      if (!propertyId) {
                        return (
                          <div
                            key={
                              propertyId ||
                              `${property?.address || "property"}-${absoluteRank}`
                            }
                          >
                            {content}
                          </div>
                        );
                      }

                      return (
                        <Link
                          key={propertyId}
                          to={`/properties/${propertyId}`}
                          className="block"
                        >
                          {content}
                        </Link>
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
      </div>

      <MarketSourcePackModal
        open={Boolean(sourcePackMarket)}
        market={sourcePackMarket}
        onClose={() => setSourcePackMarket(null)}
      />
    </PageShell>
  );
}
