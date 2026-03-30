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
import { nextPaneKey, paneLabel, paneStep } from "../components/PaneSwitcher";
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
    property?.rent_explain?.market_rent_estimate ??
    property?.rent_assumption?.market_rent_estimate ??
    property?.last_underwriting_result?.gross_rent_used ??
    null
  );
}

function inferCashflow(property: PropertyPayload | null) {
  return (
    property?.projected_monthly_cashflow ??
    property?.last_underwriting_result?.cash_flow ??
    null
  );
}

function inferDscr(property: PropertyPayload | null) {
  return property?.dscr ?? property?.last_underwriting_result?.dscr ?? null;
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

function normalizePropertyPayloadFromView(
  view: PropertyViewResponse,
): PropertyPayload {
  const property = view?.property || {};
  return {
    ...property,
    rent_explain: view?.rent_explain || property?.rent_explain || null,
    last_underwriting_result:
      view?.last_underwriting_result ||
      property?.last_underwriting_result ||
      null,
    inventory_snapshot:
      view?.inventory_snapshot || property?.inventory_snapshot || null,
  };
}

function normalizePropertyPayloadFromBundle(
  bundle: BundleResponse,
): PropertyPayload | null {
  const view = bundle?.view;
  if (!view) return null;
  const normalized = normalizePropertyPayloadFromView(view);
  return {
    ...normalized,
    inventory_snapshot:
      bundle?.inventory_snapshot || normalized.inventory_snapshot || null,
  };
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
          propertyPayload = await api.get<PropertyPayload>(
            `/dashboard/properties/${id}`,
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
  const section8Rent = inferSection8Rent(data);
  const marketRent = inferMarketRent(data);

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
                  Asking price
                </div>
                <div className="mt-3 text-xl font-semibold text-app-0">
                  {money(data.asking_price ?? data.listing_price)}
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
                  {data.listing_agent_name || "No agent name"}
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex items-center gap-2">
                    <Phone className="h-4 w-4 text-app-4" />
                    <span>{data.listing_agent_phone || "No phone"}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Mail className="h-4 w-4 text-app-4" />
                    <span>{data.listing_agent_email || "No email"}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <ExternalLink className="h-4 w-4 text-app-4" />
                    {data.listing_agent_website ? (
                      <a
                        href={data.listing_agent_website}
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
                  {data.listing_office_name || "No office name"}
                </div>
                <div className="mt-3 space-y-2 text-sm text-app-2">
                  <div className="flex items-center gap-2">
                    <Phone className="h-4 w-4 text-app-4" />
                    <span>{data.listing_office_phone || "No phone"}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Mail className="h-4 w-4 text-app-4" />
                    <span>{data.listing_office_email || "No email"}</span>
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
              offenderCount={data.offender_count}
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
