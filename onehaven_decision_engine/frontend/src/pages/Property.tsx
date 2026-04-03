import React from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
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
  Loader2,
  Ruler,
  ShieldAlert,
  Users,
  Wallet,
  Mail,
  CalendarDays,
  House,
  Upload,
  Eye,
  Trash2,
  RotateCcw,
} from "lucide-react";

import AcquisitionParticipantsPanel from "../components/AcquisitionParticipantsPanel";
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
  document_contact_guide?: Record<string, any> | null;
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

type RemoveFromAcquireResponse = {
  ok?: boolean;
  property_id?: number;
  preserved_tags?: string[];
  state?: {
    current_stage?: string;
    current_pane?: string;
    suggested_pane?: string;
    decision_bucket?: string;
  };
  detail?: AcquisitionDetail;
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
  if (v == null) return "—";
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

const ACQUIRE_STAGES = new Set([
  "pursuing",
  "offer_prep",
  "offer_ready",
  "offer_submitted",
  "negotiating",
  "under_contract",
  "due_diligence",
  "closing",
  "owned",
]);

function isAcquireStage(raw?: string | null) {
  return ACQUIRE_STAGES.has(
    String(raw || "")
      .trim()
      .toLowerCase(),
  );
}

function isAcquirePane(raw?: string | null) {
  return (
    String(raw || "")
      .trim()
      .toLowerCase() === "acquisition"
  );
}

function isInAcquireFlow(property: PropertyPayload | null) {
  if (!property) return false;
  return (
    isAcquirePane(property.current_pane) ||
    isAcquirePane(property.suggested_pane) ||
    isAcquireStage(property.current_stage)
  );
}

function hasLiveAcquisitionRecord(detail: AcquisitionDetail | null) {
  return Boolean(
    detail?.acquisition &&
    (detail?.acquisition?.id != null ||
      detail?.acquisition?.status ||
      detail?.acquisition?.waiting_on ||
      detail?.acquisition?.next_step),
  );
}

function isAcquireFlowResolved(
  property: PropertyPayload | null,
  detail: AcquisitionDetail | null,
) {
  return isInAcquireFlow(property) || hasLiveAcquisitionRecord(detail);
}

function hasAcquisitionWorkspace(detail: AcquisitionDetail | null) {
  return Boolean(
    detail?.acquisition &&
    (detail?.acquisition?.id != null ||
      detail?.acquisition?.status ||
      detail?.acquisition?.waiting_on ||
      detail?.acquisition?.next_step),
  );
}

function acquireEntryLabel(
  property: PropertyPayload | null,
  detail: AcquisitionDetail | null,
) {
  return isInAcquireFlow(property) || hasAcquisitionWorkspace(detail)
    ? "Update acquisition"
    : "Start acquisition";
}

function deriveAcquireBlockers(property: PropertyPayload | null) {
  const allowed = new Set([
    "decision_review",
    "decision_reject",
    "not_marked_for_acquisition",
  ]);

  const blockers = Array.isArray(property?.blockers)
    ? property!.blockers
        .map((item) => String(item || "").trim())
        .filter((item) => allowed.has(item))
    : [];

  const gateStatus = String(property?.gate_status || "")
    .trim()
    .toLowerCase();

  if (gateStatus.includes("blocked") && !blockers.length) {
    blockers.push("Workflow gate is currently blocked.");
  }

  return blockers;
}

function canStartAcquisition(
  property: PropertyPayload | null,
  tags: string[] = [],
) {
  if (!property) return false;

  if (
    isAcquirePane(property.current_pane) ||
    isAcquireStage(property.current_stage)
  ) {
    return true;
  }

  const blockers = deriveAcquireBlockers(property);
  if (blockers.length) return false;

  const gateStatus = String(property.gate_status || "")
    .trim()
    .toLowerCase();

  if (
    gateStatus.includes("blocked") ||
    gateStatus.includes("denied") ||
    gateStatus.includes("fail")
  ) {
    return false;
  }

  const normalizedTags = new Set(
    tags.map((x) =>
      String(x || "")
        .trim()
        .toLowerCase(),
    ),
  );
  const investorMarked =
    normalizedTags.has("saved") ||
    normalizedTags.has("shortlisted") ||
    normalizedTags.has("offer_candidate") ||
    normalizedTags.has("favorite");

  return investorMarked;
}

function acquisitionReadinessLabel(
  property: PropertyPayload | null,
  tags: string[] = [],
) {
  return canStartAcquisition(property, tags) ? "ready" : "blocked";
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
  const acquisitionStatus = String(detail?.acquisition?.status || "")
    .trim()
    .toLowerCase();

  const suppressKinds = new Set<string>();
  if (acquisitionStatus === "pursuing" || acquisitionStatus === "offer_prep") {
    suppressKinds.add("purchase_agreement");
  }

  return rows.filter(
    (x) => !x?.present && !suppressKinds.has(String(x?.kind || "").trim()),
  );
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

function documentRoleLabel(role?: string | null) {
  const normalized = String(role || "")
    .trim()
    .toLowerCase();
  const labels: Record<string, string> = {
    listing_agent: "Listing agent",
    buyer_agent: "Buyer agent",
    seller_agent: "Seller agent",
    listing_office: "Listing office",
    lender: "Lender",
    loan_officer: "Loan officer",
    insurance_agent: "Insurance agent",
    insurance_agency: "Insurance agency",
    inspector: "Inspector",
    inspection_company: "Inspection company",
    title_company: "Title company",
    escrow_officer: "Escrow officer",
  };
  return labels[normalized] || normalized.replace(/_/g, " ") || "Contact";
}

function documentContactCardFor(doc: any, detail: AcquisitionDetail | null) {
  if (doc?.document_contact_card) return doc.document_contact_card;
  const guide =
    detail?.document_contact_guide ||
    detail?.acquisition?.document_contact_guide ||
    {};
  return (
    guide[
      String(doc?.kind || "")
        .trim()
        .toLowerCase()
    ] || null
  );
}

function renderDocumentWhoToCallCard(
  doc: any,
  detail: AcquisitionDetail | null,
) {
  const card = documentContactCardFor(doc, detail);
  if (!card) return null;

  const primary = card?.primary_contact_for_document_kind || null;
  const fallbacks = Array.isArray(card?.fallback_contacts_for_document_kind)
    ? card.fallback_contacts_for_document_kind
    : [];
  const missingRoles = Array.isArray(card?.missing_contact_roles)
    ? card.missing_contact_roles
    : [];

  return (
    <div className="mt-3 rounded-2xl border border-app bg-app-muted/40 px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Who to call now
          </div>
          <div className="mt-1 text-sm font-semibold text-app-0">
            {acquisitionDocKindLabel(doc?.kind)}
          </div>
        </div>
        <Users className="h-4 w-4 text-app-4" />
      </div>

      {primary ? (
        <div className="mt-3 rounded-2xl border border-app bg-app-panel px-4 py-3">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <div className="text-sm font-semibold text-app-0">
                {primary?.name || "Unnamed contact"}
              </div>
              <div className="mt-1 text-xs text-app-4">
                {[
                  documentRoleLabel(primary?.role_label || primary?.role),
                  primary?.company,
                ]
                  .filter(Boolean)
                  .join(" • ")}
              </div>
            </div>
            <span className="oh-pill oh-pill-accent">primary</span>
          </div>
          <div className="mt-3 grid gap-2 text-sm text-app-2 md:grid-cols-2">
            <div className="flex items-center gap-2">
              <Phone className="h-4 w-4 text-app-4" />
              <span>{primary?.phone || "No phone"}</span>
            </div>
            <div className="flex items-center gap-2">
              <Mail className="h-4 w-4 text-app-4" />
              <span>{primary?.email || "No email"}</span>
            </div>
          </div>
          {primary?.why_relevant ? (
            <div className="mt-3 rounded-2xl border border-app bg-app px-3 py-2 text-xs text-app-3">
              {primary.why_relevant}
            </div>
          ) : null}
        </div>
      ) : (
        <div className="mt-3 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
          No primary contact is saved yet for this document kind.
        </div>
      )}

      {fallbacks.length ? (
        <div className="mt-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Fallback contacts
          </div>
          <div className="mt-2 grid gap-3 lg:grid-cols-2">
            {fallbacks.slice(0, 2).map((contact: any, idx: number) => (
              <div
                key={`${contact?.id || contact?.role || "fallback"}-${idx}`}
                className="rounded-2xl border border-app bg-app-panel px-4 py-3"
              >
                <div className="text-sm font-medium text-app-0">
                  {contact?.name || "Unnamed contact"}
                </div>
                <div className="mt-1 text-xs text-app-4">
                  {[
                    documentRoleLabel(contact?.role_label || contact?.role),
                    contact?.company,
                  ]
                    .filter(Boolean)
                    .join(" • ")}
                </div>
                <div className="mt-2 space-y-1 text-sm text-app-3">
                  <div>{contact?.phone || "No phone"}</div>
                  <div>{contact?.email || "No email"}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {missingRoles.length ? (
        <div className="mt-3 rounded-2xl border border-app bg-app-panel px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Missing contact roles
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            {missingRoles.map((role: string) => (
              <span key={role} className="oh-pill oh-pill-warn">
                {role}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </div>
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
    status: String(acq.status || "pursuing"),
    waiting_on: String(
      acq.waiting_on || "seller response / access / diligence kickoff",
    ),
    next_step: String(acq.next_step || "Start pre-offer acquisition work"),
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

const acquisitionActionButtonClass =
  "oh-btn w-full transition-all duration-200 hover:scale-[1.01] hover:border-violet-400/60 hover:bg-violet-500/20 hover:text-violet-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-violet-400/60";

const acquisitionPrimaryButtonClass =
  "oh-btn transition-all duration-200 hover:border-violet-400/60 hover:bg-violet-500/20 hover:text-violet-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-violet-400/60 disabled:hover:scale-100";

const acquisitionSelectWrapClass =
  "oh-select-wrap rounded-2xl border border-app bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.2)]";
const acquisitionSelectClass =
  "oh-select w-full rounded-2xl border-0 bg-transparent text-app-0 focus:ring-0 focus:outline-none";

type AcquisitionDocKindOption = {
  value: string;
  label: string;
  extensions?: string[];
};

const ACQUISITION_DOCUMENT_KIND_OPTIONS: AcquisitionDocKindOption[] = [
  {
    value: "purchase_agreement",
    label: "Purchase agreement",
    extensions: ["pdf", "doc", "docx"],
  },
  { value: "loan_estimate", label: "Loan estimate", extensions: ["pdf"] },
  {
    value: "loan_documents",
    label: "Loan documents",
    extensions: ["pdf", "doc", "docx"],
  },
  {
    value: "closing_disclosure",
    label: "Closing disclosure",
    extensions: ["pdf"],
  },
  {
    value: "title_documents",
    label: "Title documents",
    extensions: ["pdf", "doc", "docx"],
  },
  { value: "insurance_binder", label: "Insurance binder", extensions: ["pdf"] },
  {
    value: "inspection_report",
    label: "Inspection report",
    extensions: ["pdf", "doc", "docx"],
  },
];

function acquisitionDocKindLabel(kind?: string | null) {
  const key = String(kind || "")
    .trim()
    .toLowerCase();
  return (
    ACQUISITION_DOCUMENT_KIND_OPTIONS.find((item) => item.value === key)
      ?.label || (key ? key.replace(/_/g, " ") : "Document")
  );
}

function suggestAcquisitionDocKind(filename?: string | null) {
  const lower = String(filename || "")
    .trim()
    .toLowerCase();
  if (!lower) return "inspection_report";
  if (lower.includes("purchase") && lower.includes("agreement"))
    return "purchase_agreement";
  if (lower.includes("loan") && lower.includes("estimate"))
    return "loan_estimate";
  if (lower.includes("closing") && lower.includes("disclosure"))
    return "closing_disclosure";
  if (
    lower.includes("title") ||
    lower.includes("commitment") ||
    lower.includes("deed")
  )
    return "title_documents";
  if (lower.includes("insurance") || lower.includes("binder"))
    return "insurance_binder";
  if (lower.includes("inspection") || lower.includes("report"))
    return "inspection_report";
  if (
    lower.includes("loan") ||
    lower.includes("mortgage") ||
    lower.includes("underwriting")
  )
    return "loan_documents";
  const ext = lower.split(".").pop() || "";
  const byExt = ACQUISITION_DOCUMENT_KIND_OPTIONS.find((item) =>
    (item.extensions || []).includes(ext),
  );
  return byExt?.value || "inspection_report";
}

function duplicateUploadMessage(error: any) {
  const detail = error?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const existing =
      detail.existing_document ||
      detail.duplicate_document ||
      detail.document ||
      null;
    const name =
      existing?.name ||
      existing?.original_filename ||
      (existing?.id != null ? `Document #${existing.id}` : "existing document");
    const kind = acquisitionDocKindLabel(existing?.kind);
    if (
      detail.error === "duplicate_document" ||
      detail.reason === "duplicate_document" ||
      error?.response?.status === 409
    ) {
      return `This exact file is already attached to this property as ${name} (${kind}). Choose that document in the replace dropdown if this upload should supersede it.`;
    }
    if (typeof detail.message === "string") return detail.message;
  }
  return detailMessage(error, "Upload failed.");
}

export default function Property() {
  const { id } = useParams();
  const navigate = useNavigate();
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
  const [promoteSuccessPulse, setPromoteSuccessPulse] = React.useState(false);
  const [showRemoveAcquireModal, setShowRemoveAcquireModal] =
    React.useState(false);
  const [removeAcquireSaving, setRemoveAcquireSaving] = React.useState(false);
  const [removeAcquireError, setRemoveAcquireError] = React.useState<
    string | null
  >(null);
  const [removePreserveSaved, setRemovePreserveSaved] = React.useState(true);
  const [removePreserveShortlisted, setRemovePreserveShortlisted] =
    React.useState(true);

  const [uploadKind, setUploadKind] =
    React.useState<string>("inspection_report");
  const [uploadReplaceDocumentId, setUploadReplaceDocumentId] =
    React.useState<string>("");
  const [uploadingDocument, setUploadingDocument] = React.useState(false);
  const [deletingDocumentId, setDeletingDocumentId] = React.useState<
    number | null
  >(null);
  const [documentActionError, setDocumentActionError] = React.useState<
    string | null
  >(null);
  const [promoteForm, setPromoteForm] = React.useState<PromoteFormState>({
    status: "pursuing",
    waiting_on: "seller response / access / diligence kickoff",
    next_step: "Start pre-offer acquisition work",
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

  const openRemoveAcquireModal = React.useCallback(() => {
    setRemoveAcquireError(null);
    setRemovePreserveSaved(true);
    setRemovePreserveShortlisted(true);
    setShowRemoveAcquireModal(true);
  }, []);

  const closeRemoveAcquireModal = React.useCallback(() => {
    if (removeAcquireSaving) return;
    setShowRemoveAcquireModal(false);
    setRemoveAcquireError(null);
  }, [removeAcquireSaving]);

  async function handlePromoteToAcquisition() {
    if (!id) return;

    setPromoteSaving(true);
    setPromoteError(null);

    try {
      const payload = {
        status: promoteForm.status || "active",
        waiting_on: promoteForm.waiting_on || null,
        next_step: promoteForm.next_step || null,
        target_close_date: promoteForm.target_close_date || null,
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

      const hasAcquisitionRecord = Boolean(
        acquisition?.acquisition &&
        (acquisition?.acquisition?.id != null ||
          acquisition?.acquisition?.status ||
          acquisition?.acquisition?.waiting_on ||
          acquisition?.acquisition?.next_step),
      );

      if (hasAcquisitionRecord) {
        const out = await api.put<{ ok?: boolean; acquisition?: any }>(
          `/acquisition/properties/${id}`,
          payload,
        );

        if (out?.acquisition) {
          setAcquisition((prev) => ({
            ...(prev || {}),
            acquisition: out.acquisition,
          }));
        }
      } else {
        const out = await api.post<PromoteResponse>(
          `/acquisition/properties/${id}/promote`,
          payload,
        );

        if (out?.detail) {
          setAcquisition(out.detail);
        }
      }

      await refresh();
      setShowPromoteModal(false);
    } catch (error: any) {
      setPromoteError(
        detailMessage(error, "Failed to save acquisition details."),
      );
    } finally {
      setPromoteSaving(false);
    }
  }

  async function onUploadAcquisitionDocument(
    event: React.ChangeEvent<HTMLInputElement>,
  ) {
    const file = event.target.files?.[0];
    if (!file || !id) return;

    const resolvedKind = uploadKind || suggestAcquisitionDocKind(file.name);
    setUploadingDocument(true);
    setDocumentActionError(null);

    try {
      const form = new FormData();
      form.append("kind", resolvedKind);
      form.append("file", file);
      form.append("name", file.name);
      if (uploadReplaceDocumentId) {
        form.append("replace_document_id", uploadReplaceDocumentId);
      }

      await api.post(`/acquisition/properties/${id}/documents/upload`, form);
      setUploadReplaceDocumentId("");
      await refresh();
    } catch (error: any) {
      setDocumentActionError(duplicateUploadMessage(error));
    } finally {
      setUploadingDocument(false);
      event.target.value = "";
    }
  }

  async function onDeleteAcquisitionDocument(documentId: number) {
    if (!id) return;
    const confirmed = window.confirm(
      "Delete this document from the stack and remove the uploaded file too?",
    );
    if (!confirmed) return;

    setDeletingDocumentId(documentId);
    setDocumentActionError(null);
    try {
      await api.delete(
        `/acquisition/properties/${id}/documents/${documentId}`,
        {
          params: { hard_delete_file: true },
        },
      );
      if (uploadReplaceDocumentId === String(documentId)) {
        setUploadReplaceDocumentId("");
      }
      await refresh();
    } catch (error: any) {
      setDocumentActionError(
        detailMessage(error, "Failed to delete document."),
      );
    } finally {
      setDeletingDocumentId(null);
    }
  }

  async function handleRemoveFromAcquire() {
    if (!id) return;

    setRemoveAcquireSaving(true);
    setRemoveAcquireError(null);

    try {
      const preserve_tags = [
        removePreserveSaved ? "saved" : null,
        removePreserveShortlisted ? "shortlisted" : null,
      ].filter(Boolean);

      const out = await api.post<RemoveFromAcquireResponse>(
        `/acquisition/properties/${id}/remove`,
        {
          delete_documents: true,
          delete_deadlines: true,
          delete_field_reviews: true,
          delete_contacts: true,
          hard_delete_files: true,
          preserve_tags,
        },
      );

      if (out?.detail) {
        setAcquisition(out.detail);
      }
      await refresh();
      setShowRemoveAcquireModal(false);
    } catch (error: any) {
      setRemoveAcquireError(
        detailMessage(error, "Failed to remove property from Acquire."),
      );
    } finally {
      setRemoveAcquireSaving(false);
    }
  }
  React.useEffect(() => {
    refresh();
  }, [refresh]);

  const acquisitionDocuments = Array.isArray(acquisition?.documents)
    ? acquisition.documents
    : [];

  React.useEffect(() => {
    if (!acquisitionDocuments.length) {
      setUploadReplaceDocumentId("");
    }
  }, [acquisitionDocuments.length]);

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
  const hasAcquisitionRecord = Boolean(
    acquisition?.acquisition &&
    (acquisition?.acquisition?.id != null ||
      acquisition?.acquisition?.status ||
      acquisition?.acquisition?.waiting_on ||
      acquisition?.acquisition?.next_step),
  );
  const effectivePane = hasAcquisitionRecord ? "acquisition" : currentPane;
  const effectiveSuggestedPane = hasAcquisitionRecord
    ? "acquisition"
    : suggestedPane;
  const nextStagePane = nextPaneKey(effectivePane);

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
  const acquireBlockers = deriveAcquireBlockers(data);
  const isAcquireFlow = isAcquireFlowResolved(data, acquisition);
  const canEnterAcquisition = canStartAcquisition(data, tags);
  const acquireActionLabel = acquireEntryLabel(data, acquisition);
  const acquireReadinessState = acquisitionReadinessLabel(data, tags);
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
            <span className={panePillClass(effectivePane)}>
              {paneLabel(effectivePane)}
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
                value={paneLabel(effectivePane)}
                tone="neutral"
              />
              {effectiveSuggestedPane ? (
                <StatPill
                  label="Suggested pane"
                  value={paneLabel(effectiveSuggestedPane)}
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
                  {paneLabel(effectivePane)}
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
                  {paneLabel(effectiveSuggestedPane)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Acquisition entry readiness
                </div>
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  <span
                    className={
                      acquireReadinessState === "ready"
                        ? "oh-pill oh-pill-good"
                        : "oh-pill oh-pill-bad"
                    }
                  >
                    {acquireReadinessState}
                  </span>
                  <span className="text-sm text-app-3">
                    {isAcquireFlow
                      ? "This property is already in the acquisition workflow."
                      : canEnterAcquisition
                        ? "Pre-offer criteria are satisfied and acquisition can be started."
                        : "This property still has blockers before acquisition can be started."}
                  </span>
                </div>

                {data.route_reason ? (
                  <div className="mt-3 text-sm text-app-4">
                    Route reason: {data.route_reason}
                  </div>
                ) : null}

                {acquireBlockers.length ? (
                  <div className="mt-3 space-y-2 text-sm text-app-3">
                    {acquireBlockers.map((blocker) => (
                      <div key={blocker} className="flex items-start gap-2">
                        <AlertTriangle className="mt-0.5 h-4 w-4 text-amber-300" />
                        <span>{blocker}</span>
                      </div>
                    ))}
                  </div>
                ) : null}

                {!acquireBlockers.length &&
                Array.isArray(data.next_actions) &&
                data.next_actions.length ? (
                  <div className="mt-3 space-y-2 text-sm text-app-3">
                    {data.next_actions.slice(0, 3).map((action) => (
                      <div key={action} className="flex items-start gap-2">
                        <CheckCircle2 className="mt-0.5 h-4 w-4 text-emerald-300" />
                        <span>{action}</span>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>

              {effectivePane !== "acquisition" ? (
                <button
                  type="button"
                  className={acquisitionActionButtonClass}
                  onClick={openPromoteModal}
                  disabled={!canEnterAcquisition}
                  title={
                    canEnterAcquisition
                      ? "Start acquisition from investor readiness"
                      : acquireBlockers[0] ||
                        "This property is not ready for acquisition yet."
                  }
                >
                  <ArrowRight className="h-4 w-4" />
                  {acquireActionLabel}
                </button>
              ) : null}

              {nextStagePane && nextStagePane !== effectivePane ? (
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
            subtitle="Pre-offer pursuit through close: what the team is waiting on, what is missing, and how close this deal is to execution."
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

              <div className="grid gap-3">
                <button
                  type="button"
                  className={acquisitionActionButtonClass}
                  onClick={openPromoteModal}
                  disabled={!isAcquireFlow && !canEnterAcquisition}
                  title={
                    !isAcquireFlow && !canEnterAcquisition
                      ? acquireBlockers[0] ||
                        "This property is not ready for acquisition yet."
                      : undefined
                  }
                >
                  <ArrowRight className="h-4 w-4" />
                  {acquireActionLabel}
                </button>

                {isAcquireFlowResolved(data, acquisition) ? (
                  <button
                    type="button"
                    className="oh-btn oh-btn-secondary w-full border-red-500/30 text-red-100 hover:border-red-400/50 hover:bg-red-500/10"
                    onClick={openRemoveAcquireModal}
                  >
                    <RotateCcw className="h-4 w-4" />
                    Remove from Acquire
                  </button>
                ) : null}
              </div>
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

          <Surface
            title="Document stack"
            subtitle="Upload only allowed acquisition workflow documents, choose the correct kind, and delete files directly from the stack."
          >
            <div className="space-y-4">
              {documentActionError ? (
                <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                  {documentActionError}
                </div>
              ) : null}

              <div className="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)_auto]">
                <label className="block">
                  <span className="oh-field-label">Document kind</span>
                  <div className={acquisitionSelectWrapClass}>
                    <select
                      className={acquisitionSelectClass}
                      value={uploadKind}
                      onChange={(e) => setUploadKind(e.target.value)}
                    >
                      {ACQUISITION_DOCUMENT_KIND_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </label>

                <label className="block">
                  <span className="oh-field-label">
                    Replace existing document
                  </span>
                  <div className={acquisitionSelectWrapClass}>
                    <select
                      className={acquisitionSelectClass}
                      value={uploadReplaceDocumentId}
                      onChange={(e) =>
                        setUploadReplaceDocumentId(e.target.value)
                      }
                    >
                      <option value="">Do not replace</option>
                      {acquisitionDocuments.map((doc: any) => (
                        <option key={doc.id} value={String(doc.id)}>
                          {acquisitionDocKindLabel(doc.kind)} —{" "}
                          {doc.name ||
                            doc.original_filename ||
                            `Document #${doc.id}`}
                        </option>
                      ))}
                    </select>
                  </div>
                </label>

                <div className="flex items-end">
                  <label className="oh-btn oh-btn-secondary w-full cursor-pointer justify-center">
                    <Upload className="h-4 w-4" />
                    {uploadingDocument ? "Uploading..." : "Upload document"}
                    <input
                      type="file"
                      className="hidden"
                      onChange={onUploadAcquisitionDocument}
                      disabled={uploadingDocument}
                    />
                  </label>
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-panel px-4 py-4 text-sm text-app-4">
                Allowed document kinds:{" "}
                {ACQUISITION_DOCUMENT_KIND_OPTIONS.map(
                  (option) => option.label,
                ).join(", ")}
                . Duplicate files are blocked unless you intentionally upload
                them as a replacement for an existing document.
              </div>

              {acquisitionDocuments.length ? (
                <div className="space-y-3">
                  {acquisitionDocuments.map((doc: any) => (
                    <div
                      key={doc.id}
                      className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="text-sm font-semibold text-app-0">
                              {doc.name ||
                                doc.original_filename ||
                                `Document #${doc.id}`}
                            </div>
                            <span className="oh-pill oh-pill-accent">
                              {acquisitionDocKindLabel(doc.kind)}
                            </span>
                          </div>
                          <div className="mt-1 text-xs text-app-4">
                            {[
                              doc.parse_status,
                              doc.scan_status,
                              doc.content_type,
                            ]
                              .filter(Boolean)
                              .join(" • ")}
                          </div>
                        </div>

                        <div className="flex flex-wrap gap-2">
                          {doc.file_size_bytes != null ? (
                            <span className="oh-pill">
                              {Math.round(Number(doc.file_size_bytes) / 1024)}{" "}
                              KB
                            </span>
                          ) : null}
                          {doc.created_at ? (
                            <span className="oh-pill">
                              {fmtDate(doc.created_at)}
                            </span>
                          ) : null}
                        </div>
                      </div>

                      {doc.preview_text ? (
                        <div className="mt-3 rounded-2xl border border-app bg-app-muted/40 px-4 py-3 text-sm text-app-2">
                          {String(doc.preview_text).slice(0, 280)}
                        </div>
                      ) : null}

                      {renderDocumentWhoToCallCard(doc, acquisition)}

                      <div className="mt-3 flex flex-wrap gap-2">
                        <a
                          href={`/api/acquisition/properties/${id}/documents/${doc.id}/preview`}
                          target="_blank"
                          rel="noreferrer"
                          className="oh-btn oh-btn-secondary"
                        >
                          <Eye className="h-4 w-4" />
                          Preview
                        </a>
                        <a
                          href={`/api/acquisition/properties/${id}/documents/${doc.id}/download`}
                          target="_blank"
                          rel="noreferrer"
                          className="oh-btn oh-btn-secondary"
                        >
                          <FileText className="h-4 w-4" />
                          Download
                        </a>
                        <button
                          type="button"
                          className="oh-btn oh-btn-secondary"
                          onClick={() => {
                            setUploadKind(
                              String(
                                doc.kind || uploadKind || "inspection_report",
                              ),
                            );
                            setUploadReplaceDocumentId(String(doc.id));
                          }}
                        >
                          <RefreshCcw className="h-4 w-4" />
                          Replace from uploader
                        </button>
                        <button
                          type="button"
                          className="oh-btn oh-btn-secondary"
                          onClick={() =>
                            onDeleteAcquisitionDocument(Number(doc.id))
                          }
                          disabled={deletingDocumentId === Number(doc.id)}
                        >
                          <Trash2 className="h-4 w-4" />
                          {deletingDocumentId === Number(doc.id)
                            ? "Deleting..."
                            : "Delete file"}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <EmptyState title="No acquisition documents uploaded yet" />
              )}
            </div>
          </Surface>

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
            <AcquisitionParticipantsPanel
              participants={participantRows(acquisition)}
              waitingOn={acquisition?.acquisition?.waiting_on}
            />
          ) : null}
        </div>
      </div>

      {showRemoveAcquireModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 backdrop-blur-sm p-4">
          <div className="w-full max-w-2xl rounded-[32px] border border-red-500/20 bg-app-panel px-6 py-6 shadow-[0_24px_90px_rgba(0,0,0,0.55)]">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold text-red-100">
                  Remove property from Acquire
                </div>
                <div className="mt-1 text-sm text-app-4">
                  This will move the property back to Investor posture and purge
                  the current acquisition workspace.
                </div>
              </div>

              <button
                type="button"
                className="rounded-xl border border-app p-2 text-app-3 hover:bg-app-panel"
                onClick={closeRemoveAcquireModal}
                disabled={removeAcquireSaving}
              >
                ✕
              </button>
            </div>

            {removeAcquireError ? (
              <div className="mt-4 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {removeAcquireError}
              </div>
            ) : null}

            <div className="mt-5 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-4 text-sm text-red-50">
              <div className="font-semibold">This cannot be undone.</div>
              <ul className="mt-3 list-disc space-y-1 pl-5">
                <li>
                  Property will leave the Acquire workflow and route back to
                  Investor.
                </li>
                <li>
                  All acquisition documents will be deleted from the workspace.
                </li>
                <li>
                  Parsed field reviews, deadline rows, and acquisition contacts
                  will be cleared.
                </li>
                <li>
                  Acquisition posture tags such as offer_candidate will be
                  removed.
                </li>
              </ul>
            </div>

            <div className="mt-5 grid gap-3 md:grid-cols-2">
              <label className="flex items-start gap-3 rounded-2xl border border-app bg-app-muted/40 px-4 py-3 text-sm text-app-1">
                <input
                  type="checkbox"
                  checked={removePreserveSaved}
                  onChange={(e) => setRemovePreserveSaved(e.target.checked)}
                  className="mt-1"
                />
                <span>
                  Preserve <span className="font-semibold">saved</span> tag when
                  routed back to Investor.
                </span>
              </label>

              <label className="flex items-start gap-3 rounded-2xl border border-app bg-app-muted/40 px-4 py-3 text-sm text-app-1">
                <input
                  type="checkbox"
                  checked={removePreserveShortlisted}
                  onChange={(e) =>
                    setRemovePreserveShortlisted(e.target.checked)
                  }
                  className="mt-1"
                />
                <span>
                  Preserve <span className="font-semibold">shortlisted</span>{" "}
                  tag when routed back to Investor.
                </span>
              </label>
            </div>

            <div className="mt-6 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                className="oh-btn oh-btn-secondary"
                onClick={closeRemoveAcquireModal}
                disabled={removeAcquireSaving}
              >
                Cancel
              </button>
              <button
                type="button"
                className="oh-btn border-red-500/30 bg-red-500/15 text-red-100 hover:bg-red-500/20"
                onClick={handleRemoveFromAcquire}
                disabled={removeAcquireSaving}
              >
                {removeAcquireSaving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RotateCcw className="h-4 w-4" />
                )}
                Confirm destructive rollback
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {showPromoteModal ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
          <div className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-[32px] border border-app bg-app-panel px-6 py-6 shadow-[0_24px_90px_rgba(0,0,0,0.55)]">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold text-app-0">
                  {isAcquireFlow ? "Update acquisition" : "Start acquisition"}
                </div>
                <div className="mt-1 text-sm text-app-4">
                  Capture the pre-offer pursuit details that justify moving this
                  deal from Investor into Acquire.
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

            {!isAcquireFlow && acquireBlockers.length ? (
              <div className="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                <div className="font-semibold">
                  Acquisition is still blocked.
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5">
                  {acquireBlockers.map((blocker) => (
                    <li key={blocker}>{blocker}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            <div className="mt-5 grid gap-4 md:grid-cols-2">
              <label className="block">
                <span className="oh-field-label">Acquire stage</span>
                <div className={acquisitionSelectWrapClass}>
                  <select
                    className={acquisitionSelectClass}
                    value={promoteForm.status}
                    onChange={(e) =>
                      setPromoteForm((s) => ({ ...s, status: e.target.value }))
                    }
                  >
                    {[
                      "pursuing",
                      "offer_prep",
                      "offer_ready",
                      "offer_submitted",
                      "negotiating",
                      "under_contract",
                      "due_diligence",
                      "closing",
                      "owned",
                    ].map((stage) => (
                      <option key={stage} value={stage}>
                        {stage.replace(/_/g, " ")}
                      </option>
                    ))}
                  </select>
                </div>
              </label>

              <label className="block">
                <span className="oh-field-label">Waiting on</span>
                <div className={acquisitionSelectWrapClass}>
                  <select
                    className={acquisitionSelectClass}
                    value={promoteForm.waiting_on}
                    onChange={(e) =>
                      setPromoteForm((s) => ({
                        ...s,
                        waiting_on: e.target.value,
                      }))
                    }
                  >
                    {[
                      "seller response / access / diligence kickoff",
                      "buyer underwriting review",
                      "lender quote",
                      "title / escrow setup",
                      "inspection scheduling",
                      "document collection",
                    ].map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </div>
              </label>

              <label className="block md:col-span-2">
                <span className="oh-field-label">
                  Immediate acquisition next step
                </span>
                <input
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
                  value={promoteForm.loan_type}
                  onChange={(e) =>
                    setPromoteForm((s) => ({ ...s, loan_type: e.target.value }))
                  }
                />
              </label>

              <label className="block">
                <span className="oh-field-label">Loan amount</span>
                <input
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                  className="oh-input min-h-[120px] bg-app-panel/95 shadow-[0_10px_30px_rgba(0,0,0,0.16)]"
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
                className={acquisitionPrimaryButtonClass}
                onClick={handlePromoteToAcquisition}
                disabled={
                  promoteSaving || (!isAcquireFlow && !canEnterAcquisition)
                }
              >
                {promoteSaving
                  ? "Saving..."
                  : isAcquireFlow
                    ? "Save acquisition update"
                    : "Start acquisition"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </PageShell>
  );
}
