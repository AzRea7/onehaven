import React from "react";
import { Link, useParams } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  BadgeDollarSign,
  Bath,
  BedDouble,
  Building2,
  CheckCheck,
  CheckCircle2,
  ChevronRight,
  ClipboardCheck,
  Clock3,
  ExternalLink,
  FileText,
  FileWarning,
  GitBranch,
  Home,
  Landmark,
  LocateFixed,
  MapPinned,
  Phone,
  RefreshCcw,
  Ruler,
  ShieldAlert,
  Users,
  Wallet,
  Mail,
  CalendarDays,
  House,
} from "lucide-react";

import EmptyState from "../components/EmptyState";
import Golem from "../components/Golem";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import RiskBadges from "../components/RiskBadges";
import StatPill from "../components/StatPill";
import Surface from "../components/Surface";
import { nextPaneKey, paneLabel } from "../components/PaneSwitcher";
import { api } from "../lib/api";

type PropertyPayload = {
  id?: number;
  property_id?: number;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  county?: string;

  bedrooms?: number | null;
  bathrooms?: number | null;
  square_feet?: number | null;
  year_built?: number | null;
  property_type?: string | null;

  current_stage?: string;
  current_stage_label?: string;
  current_pane?: string;
  current_pane_label?: string;
  suggested_pane?: string;
  route_reason?: string;
  normalized_decision?: string;
  gate_status?: string;

  asking_price?: number | null;
  projected_monthly_cashflow?: number | null;
  dscr?: number | null;
  loan_amount?: number | null;
  monthly_debt_service?: number | null;
  monthly_taxes?: number | null;
  monthly_insurance?: number | null;
  monthly_housing_cost?: number | null;
  property_tax_annual?: number | null;
  property_tax_rate_annual?: number | null;
  property_tax_source?: string | null;
  property_tax_confidence?: number | null;
  property_tax_year?: number | null;
  insurance_annual?: number | null;
  insurance_source?: string | null;
  insurance_confidence?: number | null;

  market_rent_estimate?: number | null;
  market_reference_rent?: number | null;
  rent_reasonableness_comp?: number | null;
  rent_used?: number | null;
  rent_gap?: number | null;
  effective_gross_income?: number | null;
  variable_operating_expenses?: number | null;
  fixed_operating_expenses?: number | null;
  operating_expenses?: number | null;
  noi?: number | null;
  utilities_monthly?: number | null;
  underwriting_result_cash_flow?: number | null;
  underwriting_result_dscr?: number | null;

  blockers?: string[];
  next_actions?: string[];

  lat?: number | null;
  lng?: number | null;
  normalized_address?: string | null;
  geocode_source?: string | null;
  geocode_confidence?: number | null;
  crime_score?: number | null;
  offender_count?: number | null;
  is_red_zone?: boolean | null;
  risk_score?: number | null;
  risk_band?: string | null;
  risk_confidence?: number | null;
  crime_band?: string | null;
  crime_source?: string | null;
  crime_radius_miles?: number | null;
  crime_incident_count?: number | null;
  crime_confidence?: number | null;
  investment_area_band?: string | null;
  offender_band?: string | null;
  offender_source?: string | null;

  listing_status?: string | null;
  listing_hidden?: boolean;
  listing_hidden_reason?: string | null;
  listing_last_seen_at?: string | null;
  listing_removed_at?: string | null;
  listing_listed_at?: string | null;
  listing_created_at?: string | null;
  listing_days_on_market?: number | null;
  listing_price?: number | null;
  listing_mls_name?: string | null;
  listing_mls_number?: string | null;
  listing_type?: string | null;
  listing_zillow_url?: string | null;

  listing_agent_name?: string | null;
  listing_agent_phone?: string | null;
  listing_agent_email?: string | null;
  listing_agent_website?: string | null;

  listing_office_name?: string | null;
  listing_office_phone?: string | null;
  listing_office_email?: string | null;

  source_updated_at?: string | null;
  updated_at?: string | null;
  created_at?: string | null;

  rent_assumption?: {
    market_rent_estimate?: number | null;
    section8_fmr?: number | null;
    approved_rent_ceiling?: number | null;
    rent_reasonableness_comp?: number | null;
    rent_used?: number | null;
  } | null;

  rent_explain?: {
    market_rent_estimate?: number | null;
    section8_fmr?: number | null;
    approved_rent_ceiling?: number | null;
    rent_reasonableness_comp?: number | null;
    rent_used?: number | null;
    fmr_adjusted?: number | null;
    cap_reason?: string | null;
    strategy?: string | null;
  } | null;

  last_underwriting_result?: {
    cash_flow?: number | null;
    dscr?: number | null;
    gross_rent_used?: number | null;
    mortgage_payment?: number | null;
    operating_expenses?: number | null;
    noi?: number | null;
    decision?: string | null;
    reasons?: string[] | null;
  } | null;

  jurisdiction?: {
    completeness_status?: string;
    is_stale?: boolean;
  };

  compliance?: {
    completion_pct?: number;
    failed_count?: number;
    blocked_count?: number;
    open_failed_items?: number;
  };

  inventory_snapshot?: Record<string, any> | null;
};

type PropertyViewResponse = {
  property?: PropertyPayload;
  deal?: {
    asking_price?: number | null;
    strategy?: string | null;
  } | null;
  rent_explain?: PropertyPayload["rent_explain"];
  last_underwriting_result?: PropertyPayload["last_underwriting_result"];
  inventory_snapshot?: Record<string, any> | null;
};

type BundleResponse = {
  view?: PropertyViewResponse;
  inventory_snapshot?: Record<string, any> | null;
  gallery?: {
    cover_url?: string | null;
    photos?: string[];
  } | null;
};

type AcquisitionDetail = {
  property?: any;
  acquisition?: any;
  documents?: any[];
  required_documents?: Array<{
    kind?: string;
    label?: string;
    present?: boolean;
  }>;
  summary?: {
    days_to_close?: number | null;
    document_count?: number;
    required_documents_total?: number;
    required_documents_present?: number;
  };
};

type AcquisitionTagsPayload = {
  property_id?: number;
  tags?: string[];
  rows?: Array<{ tag?: string }>;
};

type PromoteFormState = {
  status: string;
  waiting_on: string;
  next_step: string;
  target_close_date: string;
  purchase_price: string;
  loan_type: string;
  loan_amount: string;
  cash_to_close: string;
  title_company: string;
  escrow_officer: string;
  notes: string;
};

type PromoteResponse = {
  ok?: boolean;
  property_id?: number;
  tags?: string[];
  state?: {
    current_stage?: string;
    current_pane?: string;
    suggested_pane?: string;
    decision_bucket?: string;
  };
  detail?: AcquisitionDetail;
};

function money(v?: number | null) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function num(v?: number | null, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function intText(v?: number | null) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return Math.round(n).toLocaleString();
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

function panePillClass(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toLowerCase();
  if (x === "management") return "oh-pill oh-pill-good";
  if (x === "tenants") return "oh-pill oh-pill-accent";
  if (x === "compliance") return "oh-pill oh-pill-warn";
  if (x === "acquisition") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

function waitingOnLabel(raw?: string) {
  const value = String(raw || "").trim();
  return value || "Nothing assigned";
}

function waitingOnCategory(
  raw?: string,
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

function daysToCloseTone(v?: number | null) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "text-app-0";
  if (n < 0) return "text-red-300";
  if (n <= 7) return "text-amber-300";
  return "text-emerald-300";
}

function urgencyLabel(days?: number | null, waitingOn?: string) {
  const n = Number(days);
  const text = String(waitingOn || "").toLowerCase();
  if (text.includes("blocked")) return "blocked";
  if (!Number.isFinite(n)) return "active";
  if (n < 0) return "overdue";
  if (n <= 7) return "due soon";
  return "on track";
}

function urgencyPillClass(label: string) {
  if (label === "overdue") return "oh-pill oh-pill-bad";
  if (label === "due soon") return "oh-pill oh-pill-warn";
  if (label === "blocked") return "oh-pill oh-pill-bad";
  return "oh-pill oh-pill-good";
}

function requiredDocsMissing(detail: AcquisitionDetail | null) {
  const rows = Array.isArray(detail?.required_documents)
    ? detail.required_documents
    : [];
  return rows.filter((x) => !x?.present);
}

function nextRequiredDocument(detail: AcquisitionDetail | null) {
  const missing = requiredDocsMissing(detail);
  if (missing.length) return missing[0]?.label || "Required document";
  return "No missing required documents";
}

function closeReadiness(detail: AcquisitionDetail | null) {
  const total = Number(detail?.summary?.required_documents_total || 0);
  const present = Number(detail?.summary?.required_documents_present || 0);
  const days = Number(detail?.summary?.days_to_close);
  const waiting = String(detail?.acquisition?.waiting_on || "").toLowerCase();

  let score = 0;

  if (total > 0) score += Math.round((present / total) * 55);
  if (detail?.summary?.document_count) {
    score += Math.min(Number(detail.summary.document_count) * 4, 20);
  }
  if (Number.isFinite(days)) {
    if (days > 14) score += 20;
    else if (days >= 7) score += 14;
    else if (days >= 0) score += 8;
    else score -= 12;
  }
  if (waiting.includes("document")) score -= 8;
  if (waiting.includes("blocked")) score -= 15;

  return Math.max(0, Math.min(100, score));
}

function readinessTone(score: number) {
  if (score >= 75) return "text-emerald-300";
  if (score >= 45) return "text-amber-300";
  return "text-red-300";
}

function collectConflicts(detail: AcquisitionDetail | null) {
  const documents = Array.isArray(detail?.documents) ? detail.documents : [];
  const fieldMap = new Map<
    string,
    Array<{ value: any; documentId: any; documentName: string }>
  >();

  for (const doc of documents) {
    const fields = doc?.extracted_fields || {};
    for (const [key, rawValue] of Object.entries(fields)) {
      const value = typeof rawValue === "string" ? rawValue.trim() : rawValue;
      if (value == null || value === "") continue;

      const arr = fieldMap.get(key) || [];
      arr.push({
        value,
        documentId: doc?.id,
        documentName:
          doc?.name || doc?.original_filename || `Document #${doc?.id ?? "?"}`,
      });
      fieldMap.set(key, arr);
    }
  }

  const conflicts: Array<{
    field: string;
    values: Array<{ value: any; documentId: any; documentName: string }>;
  }> = [];

  for (const [field, values] of fieldMap.entries()) {
    const normalized = new Set(
      values.map((x) => String(x.value).trim().toLowerCase()),
    );
    if (normalized.size > 1) {
      conflicts.push({ field, values });
    }
  }

  return conflicts;
}

function participantRows(detail: AcquisitionDetail | null) {
  const acquisition = detail?.acquisition || {};
  const contacts = Array.isArray(acquisition?.contacts)
    ? acquisition.contacts
    : [];
  return contacts;
}

function findParticipantContact(
  detail: AcquisitionDetail | null,
  matchers: string[],
) {
  const rows = participantRows(detail);
  const lowered = matchers.map((x) => x.toLowerCase());
  return (
    rows.find((person: any) => {
      const haystack = [person?.role, person?.name, person?.company]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return lowered.some((needle) => haystack.includes(needle));
    }) || null
  );
}

function extractTags(
  payload: AcquisitionTagsPayload | null | undefined,
): string[] {
  if (!payload) return [];

  if (Array.isArray(payload.tags)) {
    return payload.tags.map((tag) => String(tag || "").trim()).filter(Boolean);
  }

  if (Array.isArray(payload.rows)) {
    return payload.rows
      .map((row) => String(row?.tag || "").trim())
      .filter(Boolean);
  }

  return [];
}

function buildPromoteDraft(
  property: PropertyPayload | null,
  detail: AcquisitionDetail | null,
): PromoteFormState {
  const acq = detail?.acquisition || {};

  return {
    status: String(acq.status || "active"),
    waiting_on: String(acq.waiting_on || "Purchase agreement"),
    next_step: String(acq.next_step || "Open acquisition execution"),
    target_close_date: String(acq.target_close_date || ""),
    purchase_price:
      acq.purchase_price != null
        ? String(acq.purchase_price)
        : property?.asking_price != null
          ? String(property.asking_price)
          : "",
    loan_type: String(acq.loan_type || "dscr"),
    loan_amount: acq.loan_amount != null ? String(acq.loan_amount) : "",
    cash_to_close: acq.cash_to_close != null ? String(acq.cash_to_close) : "",
    title_company: String(acq.title_company || ""),
    escrow_officer: String(acq.escrow_officer || ""),
    notes: String(acq.notes || ""),
  };
}

function detailMessage(error: any, fallback: string) {
  const raw = error?.message || error?.response?.data?.detail || fallback;

  if (typeof raw === "string") return raw;

  if (raw && typeof raw === "object") {
    if (typeof raw.message === "string") return raw.message;
    if (Array.isArray(raw.missing_fields) && raw.missing_fields.length) {
      return `Missing required fields: ${raw.missing_fields.join(", ")}`;
    }
  }

  return fallback;
}

function fmtDate(raw?: string | null) {
  if (!raw) return "—";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleDateString();
}

function fmtDateTime(raw?: string | null) {
  if (!raw) return "—";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleString();
}

function deriveZillowUrl(property: PropertyPayload | null) {
  const stored = String(property?.listing_zillow_url || "").trim();
  if (stored) return stored;

  const parts = [
    property?.address,
    property?.city,
    property?.state,
    property?.zip,
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);

  if (!parts.length) return null;

  return `https://www.zillow.com/homes/${encodeURIComponent(parts.join(", "))}_rb/`;
}

function inferSection8Rent(property: PropertyPayload | null) {
  return (
    property?.rent_used ??
    property?.rent_explain?.rent_used ??
    property?.rent_assumption?.rent_used ??
    property?.rent_explain?.approved_rent_ceiling ??
    property?.rent_assumption?.approved_rent_ceiling ??
    property?.rent_explain?.fmr_adjusted ??
    property?.rent_explain?.section8_fmr ??
    property?.rent_assumption?.section8_fmr ??
    null
  );
}

function inferMarketRent(property: PropertyPayload | null) {
  return (
    property?.market_reference_rent ??
    property?.market_rent_estimate ??
    property?.rent_explain?.market_rent_estimate ??
    property?.rent_assumption?.market_rent_estimate ??
    property?.last_underwriting_result?.gross_rent_used ??
    null
  );
}

function inferHousingOnlyCashflow(property: PropertyPayload | null) {
  const rent =
    property?.rent_used ??
    property?.rent_explain?.rent_used ??
    property?.rent_assumption?.rent_used ??
    property?.market_reference_rent ??
    property?.market_rent_estimate ??
    property?.rent_explain?.market_rent_estimate ??
    property?.rent_assumption?.market_rent_estimate ??
    null;
  const housing = inferMonthlyHousingCost(property);
  if (rent == null || housing == null) return null;
  return rent - housing;
}

function inferCashflow(property: PropertyPayload | null) {
  const housingOnly = inferHousingOnlyCashflow(property);
  if (housingOnly != null) return housingOnly;
  return (
    property?.projected_monthly_cashflow ??
    property?.inventory_snapshot?.projected_monthly_cashflow ??
    property?.last_underwriting_result?.cash_flow ??
    null
  );
}

function inferMortgage(property: PropertyPayload | null) {
  return (
    property?.monthly_debt_service ??
    property?.inventory_snapshot?.monthly_debt_service ??
    property?.last_underwriting_result?.mortgage_payment ??
    null
  );
}

function inferMonthlyTaxes(property: PropertyPayload | null) {
  return (
    property?.monthly_taxes ??
    property?.inventory_snapshot?.monthly_taxes ??
    null
  );
}

function inferMonthlyInsurance(property: PropertyPayload | null) {
  return (
    property?.monthly_insurance ??
    property?.inventory_snapshot?.monthly_insurance ??
    null
  );
}

function inferMonthlyHousingCost(property: PropertyPayload | null) {
  return (
    property?.monthly_housing_cost ??
    property?.inventory_snapshot?.monthly_housing_cost ??
    (() => {
      const mortgage = inferMortgage(property);
      const taxes = inferMonthlyTaxes(property);
      const insurance = inferMonthlyInsurance(property);
      if (mortgage == null && taxes == null && insurance == null) return null;
      return (mortgage ?? 0) + (taxes ?? 0) + (insurance ?? 0);
    })()
  );
}

function inferTaxAnnual(property: PropertyPayload | null) {
  return (
    property?.property_tax_annual ??
    property?.inventory_snapshot?.property_tax_annual ??
    null
  );
}

function inferInsuranceAnnual(property: PropertyPayload | null) {
  return (
    property?.insurance_annual ??
    property?.inventory_snapshot?.insurance_annual ??
    null
  );
}

function inferDscr(property: PropertyPayload | null) {
  return (
    property?.dscr ??
    property?.inventory_snapshot?.dscr ??
    property?.last_underwriting_result?.dscr ??
    null
  );
}

function inferPropertyTypeLabel(raw?: string | null) {
  const v = String(raw || "")
    .trim()
    .toLowerCase();
  if (!v) return "—";
  return v
    .split("_")
    .map((x) => x.charAt(0).toUpperCase() + x.slice(1))
    .join(" ");
}

function firstFiniteNumber(
  ...values: Array<number | string | null | undefined>
) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function firstText(...values: Array<string | null | undefined>) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) return value;
  }
  return null;
}

function mergePropertyPayloadSources(
  base: PropertyPayload,
  view?: PropertyViewResponse | null,
  bundle?: BundleResponse | null,
): PropertyPayload {
  const property = view?.property || bundle?.view?.property || {};
  const inventorySnapshot =
    view?.inventory_snapshot ||
    bundle?.inventory_snapshot ||
    property?.inventory_snapshot ||
    base.inventory_snapshot ||
    null;
  const deal = view?.deal || bundle?.view?.deal || null;

  const merged: PropertyPayload = {
    ...base,
    ...property,
    inventory_snapshot: inventorySnapshot,
    rent_explain:
      view?.rent_explain ||
      bundle?.view?.rent_explain ||
      property?.rent_explain ||
      base.rent_explain ||
      null,
    last_underwriting_result:
      view?.last_underwriting_result ||
      bundle?.view?.last_underwriting_result ||
      property?.last_underwriting_result ||
      base.last_underwriting_result ||
      null,
  };

  merged.asking_price = firstFiniteNumber(
    base.asking_price,
    property?.asking_price,
    deal?.asking_price,
    inventorySnapshot?.asking_price,
    property?.listing_price,
    inventorySnapshot?.listing_price,
  );
  merged.listing_price = firstFiniteNumber(
    property?.listing_price,
    inventorySnapshot?.listing_price,
    base.listing_price,
    deal?.asking_price,
    base.asking_price,
  );
  merged.listing_status = firstText(
    property?.listing_status,
    inventorySnapshot?.listing_status,
    base.listing_status,
  );
  merged.listing_listed_at = firstText(
    property?.listing_listed_at,
    inventorySnapshot?.listing_listed_at,
    base.listing_listed_at,
  );
  merged.listing_last_seen_at = firstText(
    property?.listing_last_seen_at,
    inventorySnapshot?.listing_last_seen_at,
    base.listing_last_seen_at,
  );
  merged.listing_removed_at = firstText(
    property?.listing_removed_at,
    inventorySnapshot?.listing_removed_at,
    base.listing_removed_at,
  );
  merged.listing_created_at = firstText(
    property?.listing_created_at,
    inventorySnapshot?.listing_created_at,
    base.listing_created_at,
  );
  merged.listing_days_on_market = firstFiniteNumber(
    property?.listing_days_on_market,
    inventorySnapshot?.listing_days_on_market,
    base.listing_days_on_market,
  );
  merged.listing_type = firstText(
    property?.listing_type,
    inventorySnapshot?.listing_type,
    base.listing_type,
  );
  merged.listing_mls_name = firstText(
    property?.listing_mls_name,
    inventorySnapshot?.listing_mls_name,
    base.listing_mls_name,
  );
  merged.listing_mls_number = firstText(
    property?.listing_mls_number,
    inventorySnapshot?.listing_mls_number,
    base.listing_mls_number,
  );
  merged.listing_zillow_url = firstText(
    property?.listing_zillow_url,
    inventorySnapshot?.listing_zillow_url,
    base.listing_zillow_url,
  );
  merged.listing_agent_name = firstText(
    property?.listing_agent_name,
    inventorySnapshot?.listing_agent_name,
    base.listing_agent_name,
  );
  merged.listing_agent_phone = firstText(
    property?.listing_agent_phone,
    inventorySnapshot?.listing_agent_phone,
    base.listing_agent_phone,
  );
  merged.listing_agent_email = firstText(
    property?.listing_agent_email,
    inventorySnapshot?.listing_agent_email,
    base.listing_agent_email,
  );
  merged.listing_agent_website = firstText(
    property?.listing_agent_website,
    inventorySnapshot?.listing_agent_website,
    base.listing_agent_website,
  );
  merged.listing_office_name = firstText(
    property?.listing_office_name,
    inventorySnapshot?.listing_office_name,
    base.listing_office_name,
  );
  merged.listing_office_phone = firstText(
    property?.listing_office_phone,
    inventorySnapshot?.listing_office_phone,
    base.listing_office_phone,
  );
  merged.listing_office_email = firstText(
    property?.listing_office_email,
    inventorySnapshot?.listing_office_email,
    base.listing_office_email,
  );

  merged.monthly_debt_service = firstFiniteNumber(
    property?.monthly_debt_service,
    inventorySnapshot?.monthly_debt_service,
    base.monthly_debt_service,
    merged.last_underwriting_result?.mortgage_payment,
  );
  merged.monthly_taxes = firstFiniteNumber(
    property?.monthly_taxes,
    inventorySnapshot?.monthly_taxes,
    base.monthly_taxes,
  );
  merged.monthly_insurance = firstFiniteNumber(
    property?.monthly_insurance,
    inventorySnapshot?.monthly_insurance,
    base.monthly_insurance,
  );
  merged.monthly_housing_cost = firstFiniteNumber(
    property?.monthly_housing_cost,
    inventorySnapshot?.monthly_housing_cost,
    base.monthly_housing_cost,
  );
  merged.projected_monthly_cashflow = firstFiniteNumber(
    property?.projected_monthly_cashflow,
    inventorySnapshot?.projected_monthly_cashflow,
    base.projected_monthly_cashflow,
    merged.last_underwriting_result?.cash_flow,
  );
  merged.dscr = firstFiniteNumber(
    property?.dscr,
    inventorySnapshot?.dscr,
    base.dscr,
    merged.last_underwriting_result?.dscr,
  );
  merged.rent_gap = firstFiniteNumber(
    property?.rent_gap,
    inventorySnapshot?.rent_gap,
    base.rent_gap,
  );
  merged.rent_used = firstFiniteNumber(
    property?.rent_used,
    inventorySnapshot?.rent_used,
    base.rent_used,
    merged.rent_explain?.rent_used,
    property?.rent_assumption?.rent_used,
  );
  merged.market_rent_estimate = firstFiniteNumber(
    property?.market_rent_estimate,
    inventorySnapshot?.market_rent_estimate,
    base.market_rent_estimate,
    merged.rent_explain?.market_rent_estimate,
    property?.rent_assumption?.market_rent_estimate,
  );
  merged.market_reference_rent = firstFiniteNumber(
    property?.market_reference_rent,
    inventorySnapshot?.market_reference_rent,
    base.market_reference_rent,
    merged.market_rent_estimate,
  );
  merged.rent_reasonableness_comp = firstFiniteNumber(
    property?.rent_reasonableness_comp,
    inventorySnapshot?.rent_reasonableness_comp,
    base.rent_reasonableness_comp,
    merged.rent_explain?.rent_reasonableness_comp,
  );
  merged.effective_gross_income = firstFiniteNumber(
    property?.effective_gross_income,
    inventorySnapshot?.effective_gross_income,
    base.effective_gross_income,
  );
  merged.variable_operating_expenses = firstFiniteNumber(
    property?.variable_operating_expenses,
    inventorySnapshot?.variable_operating_expenses,
    base.variable_operating_expenses,
  );
  merged.fixed_operating_expenses = firstFiniteNumber(
    property?.fixed_operating_expenses,
    inventorySnapshot?.fixed_operating_expenses,
    base.fixed_operating_expenses,
  );
  merged.operating_expenses = firstFiniteNumber(
    property?.operating_expenses,
    inventorySnapshot?.operating_expenses,
    base.operating_expenses,
    merged.last_underwriting_result?.operating_expenses,
  );
  merged.noi = firstFiniteNumber(
    property?.noi,
    inventorySnapshot?.noi,
    base.noi,
    merged.last_underwriting_result?.noi,
  );
  merged.property_tax_annual = firstFiniteNumber(
    property?.property_tax_annual,
    inventorySnapshot?.property_tax_annual,
    base.property_tax_annual,
  );
  merged.insurance_annual = firstFiniteNumber(
    property?.insurance_annual,
    inventorySnapshot?.insurance_annual,
    base.insurance_annual,
  );
  merged.property_tax_source = firstText(
    property?.property_tax_source,
    inventorySnapshot?.property_tax_source,
    base.property_tax_source,
  );
  merged.insurance_source = firstText(
    property?.insurance_source,
    inventorySnapshot?.insurance_source,
    base.insurance_source,
  );

  return merged;
}

function normalizePropertyPayloadFromView(
  view: PropertyViewResponse,
): PropertyPayload {
  const property = view?.property || {};
  return mergePropertyPayloadSources(
    {
      ...property,
      rent_explain: view?.rent_explain || property?.rent_explain || null,
      last_underwriting_result:
        view?.last_underwriting_result ||
        property?.last_underwriting_result ||
        null,
      inventory_snapshot:
        view?.inventory_snapshot || property?.inventory_snapshot || null,
    },
    view,
    null,
  );
}

function normalizePropertyPayloadFromBundle(
  bundle: BundleResponse,
): PropertyPayload | null {
  const view = bundle?.view;
  if (!view) return null;
  const normalized = normalizePropertyPayloadFromView(view);
  return mergePropertyPayloadSources(
    {
      ...normalized,
      inventory_snapshot:
        bundle?.inventory_snapshot || normalized.inventory_snapshot || null,
    },
    view,
    bundle,
  );
}

function KpiCard({
  icon,
  label,
  value,
  subvalue,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  subvalue?: string;
}) {
  return (
    <div className="rounded-3xl border border-app bg-app-panel p-5">
      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
        {icon}
        {label}
      </div>
      <div className="mt-3 text-2xl font-semibold text-app-0">{value}</div>
      {subvalue ? (
        <div className="mt-1 text-sm text-app-4">{subvalue}</div>
      ) : null}
    </div>
  );
}

export default function Property() {
  const { id } = useParams();
  const [data, setData] = React.useState<PropertyPayload | null>(null);
  const [acquisition, setAcquisition] =
    React.useState<AcquisitionDetail | null>(null);
  const [acquisitionTags, setAcquisitionTags] =
    React.useState<AcquisitionTagsPayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);
  const [showPromoteModal, setShowPromoteModal] = React.useState(false);
  const [promoteSaving, setPromoteSaving] = React.useState(false);
  const [promoteError, setPromoteError] = React.useState<string | null>(null);
  const [promoteForm, setPromoteForm] = React.useState<PromoteFormState>({
    status: "active",
    waiting_on: "Purchase agreement",
    next_step: "Open acquisition execution",
    target_close_date: "",
    purchase_price: "",
    loan_type: "dscr",
    loan_amount: "",
    cash_to_close: "",
    title_company: "",
    escrow_officer: "",
    notes: "",
  });

  const refresh = React.useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);

      let propertyPayload: PropertyPayload | null = null;

      try {
        const bundle = await api.get<BundleResponse>(
          `/properties/${id}/bundle`,
        );
        propertyPayload = normalizePropertyPayloadFromBundle(bundle);
      } catch {
        try {
          const dashboardPayload = await api.get<PropertyPayload>(
            `/dashboard/properties/${id}`,
          );
          propertyPayload = mergePropertyPayloadSources(
            dashboardPayload,
            null,
            null,
          );
        } catch {
          const view = await api.get<PropertyViewResponse>(
            `/properties/${id}/view`,
          );
          propertyPayload = normalizePropertyPayloadFromView(view);
        }
      }

      const [acquisitionDetailRes, tagsRes] = await Promise.allSettled([
        api.get<AcquisitionDetail>(`/acquisition/properties/${id}`),
        api.get<AcquisitionTagsPayload>(`/properties/${id}/acquisition-tags`),
      ]);

      setData(propertyPayload);

      if (acquisitionDetailRes.status === "fulfilled") {
        setAcquisition(acquisitionDetailRes.value);
      } else {
        setAcquisition(null);
      }

      if (tagsRes.status === "fulfilled") {
        setAcquisitionTags(tagsRes.value);
      } else {
        setAcquisitionTags(null);
      }

      setErr(null);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [id]);

  const openPromoteModal = React.useCallback(() => {
    setPromoteForm(buildPromoteDraft(data, acquisition));
    setPromoteError(null);
    setShowPromoteModal(true);
  }, [data, acquisition]);

  const closePromoteModal = React.useCallback(() => {
    if (promoteSaving) return;
    setShowPromoteModal(false);
    setPromoteError(null);
  }, [promoteSaving]);

  async function handlePromoteToAcquisition() {
    if (!id) return;

    setPromoteSaving(true);
    setPromoteError(null);

    try {
      const payload = {
        status: promoteForm.status || "active",
        waiting_on: promoteForm.waiting_on,
        next_step: promoteForm.next_step,
        target_close_date: promoteForm.target_close_date,
        purchase_price:
          promoteForm.purchase_price === ""
            ? null
            : Number(promoteForm.purchase_price),
        loan_type: promoteForm.loan_type || null,
        loan_amount:
          promoteForm.loan_amount === ""
            ? null
            : Number(promoteForm.loan_amount),
        cash_to_close:
          promoteForm.cash_to_close === ""
            ? null
            : Number(promoteForm.cash_to_close),
        title_company: promoteForm.title_company || null,
        escrow_officer: promoteForm.escrow_officer || null,
        notes: promoteForm.notes || null,
      };

      const out = await api.post<PromoteResponse>(
        `/acquisition/properties/${id}/promote`,
        payload,
      );

      if (out?.detail) {
        setAcquisition(out.detail);
      }

      await refresh();
      setShowPromoteModal(false);
    } catch (error: any) {
      setPromoteError(
        detailMessage(error, "Failed to move property into acquisition."),
      );
    } finally {
      setPromoteSaving(false);
    }
  }

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  if (loading) {
    return (
      <PageShell>
        <div className="space-y-6">
          <div className="oh-skeleton h-[220px] rounded-[32px]" />
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="oh-skeleton h-[140px] rounded-3xl" />
            ))}
          </div>
        </div>
      </PageShell>
    );
  }

  if (err || !data) {
    return (
      <PageShell>
        <EmptyState
          icon={FileWarning}
          title="Property failed to load"
          description={err || "Property data is unavailable."}
        />
      </PageShell>
    );
  }

  const currentPane = String(data.current_pane || "investor").toLowerCase();
  const suggestedPane = String(
    data.suggested_pane || data.current_pane || "investor",
  ).toLowerCase();
  const nextStagePane = nextPaneKey(currentPane);

  const cashflow = inferCashflow(data);
  const dscr = inferDscr(data);
  const mortgage = inferMortgage(data);
  const monthlyTaxes = inferMonthlyTaxes(data);
  const monthlyInsurance = inferMonthlyInsurance(data);
  const monthlyHousingCost = inferMonthlyHousingCost(data);
  const taxAnnual = inferTaxAnnual(data);
  const insuranceAnnual = inferInsuranceAnnual(data);
  const section8Rent = inferSection8Rent(data);
  const marketRent = inferMarketRent(data);
  const housingOnlyCashflow = inferHousingOnlyCashflow(data);
  const utilitiesMonthly =
    data.utilities_monthly ??
    data.inventory_snapshot?.utilities_monthly ??
    null;
  const variableOperatingExpenses =
    data.variable_operating_expenses ??
    data.inventory_snapshot?.variable_operating_expenses ??
    null;
  const fixedOperatingExpenses =
    data.fixed_operating_expenses ??
    data.inventory_snapshot?.fixed_operating_expenses ??
    null;
  const fullOperatingExpenses =
    data.operating_expenses ??
    data.inventory_snapshot?.operating_expenses ??
    null;
  const spreadsheetTotalMonthlyCost =
    (monthlyHousingCost ?? 0) +
    (fullOperatingExpenses ?? variableOperatingExpenses ?? 0);
  const fullCycleCashflow =
    data.noi != null && mortgage != null
      ? data.noi - mortgage
      : (data.underwriting_result_cash_flow ??
        data.last_underwriting_result?.cash_flow ??
        data.inventory_snapshot?.underwriting_result_cash_flow ??
        null);

  const listingAgentFallback = findParticipantContact(acquisition, [
    "listing agent",
    "seller agent",
    "agent",
    "broker",
    "realtor",
  ]);
  const listingOfficeFallback = findParticipantContact(acquisition, [
    "brokerage",
    "office",
    "listing office",
    "title",
  ]);

  const listingAgentName =
    data.listing_agent_name || listingAgentFallback?.name || "No agent name";
  const listingAgentPhone =
    data.listing_agent_phone || listingAgentFallback?.phone || "No phone";
  const listingAgentEmail =
    data.listing_agent_email || listingAgentFallback?.email || "No email";
  const listingAgentWebsite =
    data.listing_agent_website || listingAgentFallback?.website || null;
  const listingOfficeName =
    data.listing_office_name ||
    listingOfficeFallback?.company ||
    listingOfficeFallback?.name ||
    "No office name";
  const listingOfficePhone =
    data.listing_office_phone || listingOfficeFallback?.phone || "No phone";
  const listingOfficeEmail =
    data.listing_office_email || listingOfficeFallback?.email || "No email";

  const zillowUrl = deriveZillowUrl(data);

  const acquisitionUrgency = urgencyLabel(
    acquisition?.summary?.days_to_close,
    acquisition?.acquisition?.waiting_on,
  );
  const acquisitionReadiness = closeReadiness(acquisition);
  const conflicts = collectConflicts(acquisition);
  const tags = extractTags(acquisitionTags);

  return (
    <PageShell>
      <PageHero
        eyebrow="Single property view"
        title={data.address || "Property"}
        subtitle={[
          data.city,
          data.state,
          data.zip,
          data.county ? `• ${data.county}` : null,
        ]
          .filter(Boolean)
          .join(" ")}
        right={
          <div className="flex flex-wrap items-center gap-2">
            <span className={decisionPillClass(data.normalized_decision)}>
              {normalizeDecision(data.normalized_decision).replace("_", " ")}
            </span>
            <span className={panePillClass(currentPane)}>
              {paneLabel(currentPane)}
            </span>
            <button
              type="button"
              className="oh-btn oh-btn-secondary"
              onClick={refresh}
            >
              <RefreshCcw className="h-4 w-4" />
              Refresh
            </button>
            {zillowUrl ? (
              <a
                href={zillowUrl}
                target="_blank"
                rel="noreferrer"
                className="oh-btn"
              >
                <ExternalLink className="h-4 w-4" />
                Zillow
              </a>
            ) : null}
          </div>
        }
      />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
        <KpiCard
          icon={<Wallet className="h-4 w-4" />}
          label="Projected cashflow"
          value={money(cashflow)}
          subvalue="monthly"
        />
        <KpiCard
          icon={<Landmark className="h-4 w-4" />}
          label="DSCR"
          value={num(dscr)}
          subvalue="debt service coverage"
        />
        <KpiCard
          icon={<BadgeDollarSign className="h-4 w-4" />}
          label="Section 8 assumption"
          value={money(section8Rent)}
          subvalue={`market rent ${money(marketRent)}`}
        />
        <KpiCard
          icon={<Clock3 className="h-4 w-4" />}
          label="Days on market"
          value={intText(data.listing_days_on_market)}
          subvalue={data.listing_status || "listing status unavailable"}
        />
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,1fr)_380px]">
        <div className="space-y-6">
          <Surface
            title="Deal profile"
            subtitle="Acquisition-facing snapshot of the property, listing, and rent assumptions."
          >
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Home className="h-4 w-4" />
                  Asking / listing price
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {money(data.asking_price ?? data.listing_price)}
                </div>
                <div className="mt-1 text-xs text-app-4">
                  {data.asking_price != null && data.listing_price != null
                    ? `Deal ${money(data.asking_price)} • Listing ${money(data.listing_price)}`
                    : data.listing_price != null
                      ? `Listing ${money(data.listing_price)}`
                      : data.asking_price != null
                        ? `Deal ${money(data.asking_price)}`
                        : "No price available"}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <House className="h-4 w-4" />
                  Property type
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {inferPropertyTypeLabel(data.property_type)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <CalendarDays className="h-4 w-4" />
                  Year built
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {intText(data.year_built)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Ruler className="h-4 w-4" />
                  Sqft
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {intText(data.square_feet)}
                </div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              {data.bedrooms != null ? (
                <StatPill
                  label="Beds"
                  value={String(data.bedrooms)}
                  tone="neutral"
                />
              ) : null}
              {data.bathrooms != null ? (
                <StatPill
                  label="Baths"
                  value={num(data.bathrooms, 1)}
                  tone="neutral"
                />
              ) : null}
              <StatPill
                label="Listing status"
                value={String(data.listing_status || "Unknown")}
                tone={
                  String(data.listing_status || "").toLowerCase() === "active"
                    ? "good"
                    : String(data.listing_status || "").toLowerCase() ===
                        "inactive"
                      ? "bad"
                      : "warn"
                }
              />
              <StatPill
                label="Current pane"
                value={paneLabel(currentPane)}
                tone="neutral"
              />
              {suggestedPane ? (
                <StatPill
                  label="Suggested pane"
                  value={paneLabel(suggestedPane)}
                  tone="warn"
                />
              ) : null}
            </div>
          </Surface>
          <Surface
            title="Investor cashflow spreadsheet"
            subtitle="Headline investor cashflow only counts rent minus mortgage, taxes, and insurance. Utilities and broader operating expenses are shown separately below."
          >
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <StatPill
                label="Investor cashflow"
                value={money(housingOnlyCashflow ?? cashflow)}
                tone={
                  (housingOnlyCashflow ?? cashflow) != null &&
                  Number(housingOnlyCashflow ?? cashflow) > 0
                    ? "good"
                    : (housingOnlyCashflow ?? cashflow) != null &&
                        Number(housingOnlyCashflow ?? cashflow) < 0
                      ? "bad"
                      : "neutral"
                }
              />
              <StatPill
                label="DSCR"
                value={num(dscr)}
                tone={dscr != null && dscr >= 1.2 ? "good" : "warn"}
              />
              <StatPill
                label="Rent used"
                value={money(data.rent_used ?? section8Rent)}
                tone="neutral"
              />
              <StatPill
                label="Housing total"
                value={money(monthlyHousingCost)}
                tone="neutral"
              />
            </div>

            <div className="mt-5 overflow-x-auto rounded-3xl border border-app bg-app-panel">
              <table className="min-w-full text-left text-sm text-app-2">
                <thead className="border-b border-app bg-app-muted/60 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <tr>
                    <th className="px-4 py-3 font-medium">Line item</th>
                    <th className="px-4 py-3 font-medium">Monthly</th>
                    <th className="px-4 py-3 font-medium">Notes</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Rent used
                    </td>
                    <td className="px-4 py-3">
                      {money(data.rent_used ?? section8Rent ?? marketRent)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Section 8 / comp-capped rent input used by underwriting.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Mortgage
                    </td>
                    <td className="px-4 py-3">{money(mortgage)}</td>
                    <td className="px-4 py-3 text-app-4">
                      Monthly debt service.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Property taxes
                    </td>
                    <td className="px-4 py-3">{money(monthlyTaxes)}</td>
                    <td className="px-4 py-3 text-app-4">
                      {data.property_tax_source ||
                        "Property tax enrichment source unavailable."}
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Insurance
                    </td>
                    <td className="px-4 py-3">{money(monthlyInsurance)}</td>
                    <td className="px-4 py-3 text-app-4">
                      {data.insurance_source ||
                        "Insurance enrichment source unavailable."}
                    </td>
                  </tr>
                  <tr className="border-b border-app/70 bg-app-muted/30">
                    <td className="px-4 py-3 font-semibold text-app-0">
                      Investor cashflow (headline)
                    </td>
                    <td className="px-4 py-3 font-semibold text-app-0">
                      {money(housingOnlyCashflow ?? cashflow)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Strict rent minus mortgage, taxes, and insurance only.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Utilities
                    </td>
                    <td className="px-4 py-3">{money(utilitiesMonthly)}</td>
                    <td className="px-4 py-3 text-app-4">
                      Displayed for visibility but excluded from the headline
                      investor cashflow because the tenant lease structure
                      handles utilities.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Variable operating expenses
                    </td>
                    <td className="px-4 py-3">
                      {money(variableOperatingExpenses)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Shown separately for full operating visibility.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Fixed operating expenses
                    </td>
                    <td className="px-4 py-3">
                      {money(fixedOperatingExpenses)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Shown separately for full operating visibility.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Full operating expenses
                    </td>
                    <td className="px-4 py-3">
                      {money(fullOperatingExpenses)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Broader property operating expense bucket from
                      underwriting snapshot.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">NOI</td>
                    <td className="px-4 py-3">{money(data.noi)}</td>
                    <td className="px-4 py-3 text-app-4">
                      Net operating income before debt service.
                    </td>
                  </tr>
                  <tr className="border-b border-app/70">
                    <td className="px-4 py-3 font-medium text-app-0">
                      Spreadsheet total monthly cost
                    </td>
                    <td className="px-4 py-3">
                      {money(spreadsheetTotalMonthlyCost)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Housing cost plus the broader operating bucket for full
                      view.
                    </td>
                  </tr>
                  <tr className="bg-app-muted/30">
                    <td className="px-4 py-3 font-semibold text-app-0">
                      Full-cycle cashflow
                    </td>
                    <td className="px-4 py-3 font-semibold text-app-0">
                      {money(fullCycleCashflow)}
                    </td>
                    <td className="px-4 py-3 text-app-4">
                      Expanded underwriting view after broader operating costs.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            {taxAnnual != null || insuranceAnnual != null ? (
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Property tax details
                  </div>
                  <div className="mt-3 text-lg font-semibold text-app-0">
                    {money(taxAnnual)}
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    {data.property_tax_source || "source unavailable"}
                    {data.property_tax_year
                      ? ` • ${data.property_tax_year}`
                      : ""}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Insurance details
                  </div>
                  <div className="mt-3 text-lg font-semibold text-app-0">
                    {money(insuranceAnnual)}
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    {data.insurance_source || "source unavailable"}
                  </div>
                </div>
              </div>
            ) : null}
          </Surface>

          <Surface
            title="Listing enrichment"
            subtitle="All listing lifecycle and contact details available for this property."
          >
            <div className="grid gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Listing timeline
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex justify-between gap-3">
                    <span>Listed date</span>
                    <span className="font-medium text-app-0">
                      {fmtDate(data.listing_listed_at)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Last seen</span>
                    <span className="font-medium text-app-0">
                      {fmtDateTime(data.listing_last_seen_at)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Removed date</span>
                    <span className="font-medium text-app-0">
                      {fmtDate(data.listing_removed_at)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Created date</span>
                    <span className="font-medium text-app-0">
                      {fmtDate(data.listing_created_at)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Days on market</span>
                    <span className="font-medium text-app-0">
                      {intText(data.listing_days_on_market)}
                    </span>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Listing identifiers
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex justify-between gap-3">
                    <span>Status</span>
                    <span className="font-medium text-app-0">
                      {data.listing_status || "—"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Listing price</span>
                    <span className="font-medium text-app-0">
                      {money(data.listing_price)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Fallback asking price</span>
                    <span className="font-medium text-app-0">
                      {money(data.asking_price)}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Type</span>
                    <span className="font-medium text-app-0">
                      {data.listing_type || "—"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>MLS</span>
                    <span className="font-medium text-app-0">
                      {data.listing_mls_name || "—"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>MLS number</span>
                    <span className="font-medium text-app-0">
                      {data.listing_mls_number || "—"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Hidden</span>
                    <span className="font-medium text-app-0">
                      {data.listing_hidden ? "Yes" : "No"}
                    </span>
                  </div>
                  <div className="flex justify-between gap-3">
                    <span>Hidden reason</span>
                    <span className="font-medium text-app-0">
                      {data.listing_hidden_reason || "—"}
                    </span>
                  </div>
                </div>

                {zillowUrl ? (
                  <div className="mt-4">
                    <a
                      href={zillowUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm font-medium text-app-0 hover:bg-app-panel"
                    >
                      <ExternalLink className="h-4 w-4" />
                      Open Zillow listing
                    </a>
                  </div>
                ) : null}
              </div>
            </div>
          </Surface>

          <Surface
            title="Rent assumptions"
            subtitle="Section 8 and underwriting-facing rent inputs."
          >
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Section 8 rent used
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {money(
                    data.rent_used ??
                      data.rent_explain?.rent_used ??
                      data.rent_assumption?.rent_used,
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Approved ceiling
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {money(
                    data.rent_explain?.approved_rent_ceiling ??
                      data.rent_assumption?.approved_rent_ceiling,
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Section 8 FMR
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {money(
                    data.rent_explain?.section8_fmr ??
                      data.rent_assumption?.section8_fmr,
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Cap reason
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {data.rent_explain?.cap_reason || "—"}
                </div>
              </div>
            </div>
          </Surface>

          <Surface
            title="Contacts"
            subtitle="Agent and brokerage details carried through listing enrichment."
          >
            <div className="grid gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Users className="h-4 w-4" />
                  Listing agent
                </div>
                <div className="mt-3 text-lg font-semibold text-app-0">
                  {listingAgentName}
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex items-center gap-2">
                    <Phone className="h-4 w-4 text-app-4" />
                    <span>{listingAgentPhone}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Mail className="h-4 w-4 text-app-4" />
                    <span>{listingAgentEmail}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <ExternalLink className="h-4 w-4 text-app-4" />
                    {listingAgentWebsite ? (
                      <a
                        href={listingAgentWebsite}
                        target="_blank"
                        rel="noreferrer"
                        className="text-app-1 hover:text-app-0"
                      >
                        Agent website
                      </a>
                    ) : (
                      <span>No website</span>
                    )}
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Building2 className="h-4 w-4" />
                  Brokerage / office
                </div>
                <div className="mt-3 text-lg font-semibold text-app-0">
                  {listingOfficeName}
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex items-center gap-2">
                    <Phone className="h-4 w-4 text-app-4" />
                    <span>{listingOfficePhone}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Mail className="h-4 w-4 text-app-4" />
                    <span>{listingOfficeEmail}</span>
                  </div>
                </div>
              </div>
            </div>
          </Surface>

          <Surface
            title="Location and risk"
            subtitle="Risk context, geocode quality, and locality signals."
          >
            <RiskBadges
              county={data.county}
              isRedZone={data.is_red_zone}
              crimeScore={data.crime_score}
              crimeBand={data.crime_band}
              crimeSource={data.crime_source}
              crimeRadiusMiles={data.crime_radius_miles}
              crimeIncidentCount={data.crime_incident_count}
              crimeConfidence={data.crime_confidence}
              investmentAreaBand={data.investment_area_band}
              offenderCount={data.offender_count}
              offenderBand={data.offender_band}
              offenderSource={data.offender_source}
              riskScore={data.risk_score}
              riskBand={data.risk_band}
              riskConfidence={data.risk_confidence}
              lat={data.lat}
              lng={data.lng}
              normalizedAddress={data.normalized_address}
              geocodeSource={data.geocode_source}
              geocodeConfidence={data.geocode_confidence}
            />
          </Surface>
        </div>

        <div className="space-y-6">
          <Surface
            title="Workflow"
            subtitle="Current pane, stage, and routing posture."
          >
            <div className="space-y-3">
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Current pane
                </div>
                <div className="mt-2 text-base font-semibold text-app-0">
                  {paneLabel(currentPane)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Current stage
                </div>
                <div className="mt-2 text-base font-semibold text-app-0">
                  {data.current_stage_label || data.current_stage || "—"}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Suggested next pane
                </div>
                <div className="mt-2 text-base font-semibold text-app-0">
                  {paneLabel(suggestedPane)}
                </div>
              </div>

              {nextStagePane ? (
                <Link
                  to={`/panes/${nextStagePane}`}
                  className="inline-flex w-full items-center justify-between rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-0 hover:bg-app-muted"
                >
                  Move to {paneLabel(nextStagePane)}
                  <ChevronRight className="h-4 w-4" />
                </Link>
              ) : null}
            </div>
          </Surface>

          <Surface
            title="Acquisition posture"
            subtitle="What the team is waiting on and how close this is to execution."
          >
            <div className="space-y-4">
              <div className="flex flex-wrap gap-2">
                <span className={urgencyPillClass(acquisitionUrgency)}>
                  {acquisitionUrgency}
                </span>
                <span className="oh-pill">
                  waiting on{" "}
                  {waitingOnLabel(acquisition?.acquisition?.waiting_on)}
                </span>
                <span className="oh-pill">
                  {waitingOnCategory(acquisition?.acquisition?.waiting_on)}
                </span>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Estimated close readiness
                </div>
                <div
                  className={`mt-2 text-2xl font-semibold ${readinessTone(acquisitionReadiness)}`}
                >
                  {acquisitionReadiness}%
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Days to close
                </div>
                <div
                  className={`mt-2 text-2xl font-semibold ${daysToCloseTone(acquisition?.summary?.days_to_close)}`}
                >
                  {intText(acquisition?.summary?.days_to_close ?? null)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Next required document
                </div>
                <div className="mt-2 text-base font-semibold text-app-0">
                  {nextRequiredDocument(acquisition)}
                </div>
              </div>

              <button
                type="button"
                className="oh-btn w-full"
                onClick={openPromoteModal}
              >
                <ArrowRight className="h-4 w-4" />
                Promote / update acquisition
              </button>
            </div>
          </Surface>

          {tags.length ? (
            <Surface title="Acquisition tags">
              <div className="flex flex-wrap gap-2">
                {tags.map((tag) => (
                  <span key={tag} className="oh-pill">
                    {tag}
                  </span>
                ))}
              </div>
            </Surface>
          ) : null}

          {conflicts.length ? (
            <Surface
              title="Document conflicts"
              subtitle="Parsed values that disagree across uploaded acquisition docs."
            >
              <div className="space-y-3">
                {conflicts.map((conflict) => (
                  <div
                    key={conflict.field}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                      <AlertTriangle className="h-4 w-4 text-amber-300" />
                      {conflict.field}
                    </div>
                    <div className="mt-2 space-y-1 text-sm text-app-3">
                      {conflict.values.map((v, idx) => (
                        <div key={`${conflict.field}-${idx}`}>
                          {v.documentName}:{" "}
                          <span className="text-app-0">{String(v.value)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </Surface>
          ) : null}

          {participantRows(acquisition).length ? (
            <Surface
              title="Participants"
              subtitle="Contacts tied to acquisition execution."
            >
              <div className="space-y-3">
                {participantRows(acquisition).map(
                  (person: any, idx: number) => (
                    <div
                      key={`${person?.role || "participant"}-${idx}`}
                      className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                    >
                      <div className="text-sm font-semibold text-app-0">
                        {person?.name || person?.role || "Participant"}
                      </div>
                      <div className="mt-1 text-sm text-app-4">
                        {[person?.role, person?.company]
                          .filter(Boolean)
                          .join(" • ")}
                      </div>
                      <div className="mt-2 space-y-1 text-sm text-app-2">
                        {person?.email ? <div>{person.email}</div> : null}
                        {person?.phone ? <div>{person.phone}</div> : null}
                      </div>
                    </div>
                  ),
                )}
              </div>
            </Surface>
          ) : null}
        </div>
      </div>

      {showPromoteModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-[32px] border border-app bg-app px-6 py-6 shadow-2xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold text-app-0">
                  Promote to acquisition
                </div>
                <div className="mt-1 text-sm text-app-4">
                  Set the acquisition-ready details for this property.
                </div>
              </div>

              <button
                type="button"
                className="rounded-xl border border-app p-2 text-app-3 hover:bg-app-panel"
                onClick={closePromoteModal}
                disabled={promoteSaving}
              >
                ✕
              </button>
            </div>

            {promoteError ? (
              <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {promoteError}
              </div>
            ) : null}

            <div className="mt-5 grid gap-4 md:grid-cols-2">
              <label className="block">
                <span className="oh-field-label">Status</span>
                <input
                  className="oh-input"
                  value={promoteForm.status}
                  onChange={(e) =>
                    setPromoteForm((s) => ({ ...s, status: e.target.value }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Waiting on</span>
                <input
                  className="oh-input"
                  value={promoteForm.waiting_on}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      waiting_on: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block md:col-span-2">
                <span className="oh-field-label">Next step</span>
                <input
                  className="oh-input"
                  value={promoteForm.next_step}
                  onChange={(e) =>
                    setPromoteForm((s) => ({ ...s, next_step: e.target.value }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Target close date</span>
                <input
                  type="date"
                  className="oh-input"
                  value={promoteForm.target_close_date}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      target_close_date: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Purchase price</span>
                <input
                  className="oh-input"
                  value={promoteForm.purchase_price}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      purchase_price: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Loan type</span>
                <input
                  className="oh-input"
                  value={promoteForm.loan_type}
                  onChange={(e) =>
                    setPromoteForm((s) => ({ ...s, loan_type: e.target.value }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Loan amount</span>
                <input
                  className="oh-input"
                  value={promoteForm.loan_amount}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      loan_amount: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Cash to close</span>
                <input
                  className="oh-input"
                  value={promoteForm.cash_to_close}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      cash_to_close: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Title company</span>
                <input
                  className="oh-input"
                  value={promoteForm.title_company}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      title_company: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Escrow officer</span>
                <input
                  className="oh-input"
                  value={promoteForm.escrow_officer}
                  onChange={(e) =>
                    setPromoteForm((s) => ({
                      ...s,
                      escrow_officer: e.target.value,
                    }))
                  }
                />
              </label>

              <label className="block md:col-span-2">
                <span className="oh-field-label">Notes</span>
                <textarea
                  className="oh-input min-h-[120px]"
                  value={promoteForm.notes}
                  onChange={(e) =>
                    setPromoteForm((s) => ({ ...s, notes: e.target.value }))
                  }
                />
              </label>
            </div>

            <div className="mt-6 flex items-center justify-end gap-3">
              <button
                type="button"
                className="oh-btn oh-btn-secondary"
                onClick={closePromoteModal}
                disabled={promoteSaving}
              >
                Cancel
              </button>
              <button
                type="button"
                className="oh-btn"
                onClick={handlePromoteToAcquisition}
                disabled={promoteSaving}
              >
                {promoteSaving ? "Saving..." : "Save acquisition setup"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </PageShell>
  );
}
