import React from "react";
import {
  AlertTriangle,
  Building2,
  CheckCircle2,
  Clock3,
  ExternalLink,
  FileText,
  GitCompareArrows,
  Loader2,
  Mail,
  Phone,
  RefreshCcw,
  ShieldAlert,
  Upload,
  Users,
  Trash2,
  RotateCcw,
} from "lucide-react";

import AcquisitionDeadlinePanel, {
  type AcquisitionDeadline,
} from "products/acquire/frontend/src/components/AcquisitionDeadlinePanel";
import AcquisitionFilters, {
  type AcquisitionQueueFiltersValue,
} from "products/acquire/frontend/src/components/AcquisitionFilters";
import AcquisitionParticipantsPanel, {
  type AcquisitionParticipant,
} from "products/acquire/frontend/src/components/AcquisitionParticipantsPanel";
import AcquisitionTagBar from "products/acquire/frontend/src/components/AcquisitionTagBar";
import DocumentFieldReviewPanel, {
  type FieldValueRow,
} from "products/acquire/frontend/src/components/DocumentFieldReviewPanel";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";
import Golem from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Golem";
import PageHero from "onehaven_onehaven_platform/frontend/src/shell/PageHero";
import PageShell from "onehaven_onehaven_platform/frontend/src/shell/PageShell";
import ShortlistBoard from "products/intelligence/frontend/src/components/ShortlistBoard";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import { api } from "@/lib/api";

type QueueItem = {
  property_id: number;
  address?: string | null;
  city?: string | null;
  state?: string | null;
  zip?: string | null;
  county?: string | null;
  current_stage?: string | null;
  status?: string | null;
  waiting_on?: string | null;
  next_step?: string | null;
  contract_date?: string | null;
  target_close_date?: string | null;
  closing_date?: string | null;
  purchase_price?: number | null;
  loan_amount?: number | null;
  cash_to_close?: number | null;
  closing_costs?: number | null;
  acquisition_updated_at?: string | null;
  document_count?: number | null;
  days_to_close?: number | null;
  acquisition_tags?: string[];
  missing_document_groups?: Array<{ kind?: string; label?: string }>;
  conflict_count?: number | null;
  estimated_close_readiness?: number | null;

  listing_status?: string | null;
  listing_days_on_market?: number | null;
  listing_zillow_url?: string | null;
  listing_agent_name?: string | null;
  listing_agent_phone?: string | null;
  listing_agent_email?: string | null;
  listing_office_name?: string | null;
  listing_office_phone?: string | null;
  listing_office_email?: string | null;
  listing_contacts?: Array<{
    role?: string | null;
    name?: string | null;
    email?: string | null;
    phone?: string | null;
    company?: string | null;
    source_type?: string | null;
  }>;
  participant_count?: number | null;
  suggested_field_count?: number | null;
};

type AcquisitionDocument = {
  id: number;
  property_id?: number;
  kind?: string | null;
  name?: string | null;
  original_filename?: string | null;
  content_type?: string | null;
  file_size_bytes?: number | null;
  upload_status?: string | null;
  scan_status?: string | null;
  scan_result?: string | null;
  parse_status?: string | null;
  parser_version?: string | null;
  preview_text?: string | null;
  status?: string | null;
  source_url?: string | null;
  extracted_text?: string | null;
  extracted_fields?: Record<string, any>;
  actionable_intelligence?: {
    recommended_next_actions?: string[];
    who_to_contact_next?: Array<Record<string, any>>;
    deadline_candidates?: Array<Record<string, any>>;
    risk_flags?: Array<Record<string, any>>;
    mismatch_indicators?: Array<Record<string, any>>;
  } | null;
  notes?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type RequiredDocument = {
  kind?: string | null;
  label?: string | null;
  present?: boolean;
};

type AcquisitionDetail = {
  property?: {
    property_id?: number;
    address?: string | null;
    city?: string | null;
    state?: string | null;
    zip?: string | null;
    county?: string | null;
    bedrooms?: number | null;
    bathrooms?: number | null;
    square_feet?: number | null;
    year_built?: number | null;
    property_type?: string | null;
    current_stage?: string | null;

    listing_status?: string | null;
    listing_days_on_market?: number | null;
    listing_listed_at?: string | null;
    listing_last_seen_at?: string | null;
    listing_removed_at?: string | null;
    listing_zillow_url?: string | null;

    listing_agent_name?: string | null;
    listing_agent_phone?: string | null;
    listing_agent_email?: string | null;
    listing_agent_website?: string | null;

    listing_office_name?: string | null;
    listing_office_phone?: string | null;
    listing_office_email?: string | null;
  };
  acquisition?: {
    status?: string | null;
    waiting_on?: string | null;
    next_step?: string | null;
    contract_date?: string | null;
    target_close_date?: string | null;
    closing_date?: string | null;
    purchase_price?: number | null;
    earnest_money?: number | null;
    loan_amount?: number | null;
    loan_type?: string | null;
    interest_rate?: number | null;
    cash_to_close?: number | null;
    closing_costs?: number | null;
    seller_credits?: number | null;
    title_company?: string | null;
    escrow_officer?: string | null;
    notes?: string | null;
    contacts?: any[];
    listing_contacts?: any[];
    milestones?: any[];
    days_to_close?: number | null;
    deadlines?: any[];
    field_values?: any[];
  };
  documents?: AcquisitionDocument[];
  participants?: AcquisitionParticipant[];
  required_documents?: RequiredDocument[];
  summary?: {
    days_to_close?: number | null;
    document_count?: number | null;
    required_documents_total?: number | null;
    required_documents_present?: number | null;
  };
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

type PropertyTagsPayload = {
  property_id?: number;
  tags?: string[];
  rows?: Array<{ tag?: string }>;
};

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

function extractDetailMessage(error: any, fallback: string) {
  const detail = error?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    if (typeof detail.message === "string") return detail.message;
    const existing =
      detail.existing_document || detail.duplicate_document || detail.document;
    if (existing) {
      const name =
        existing?.name ||
        existing?.original_filename ||
        (existing?.id != null
          ? `Document #${existing.id}`
          : "existing document");
      return `This exact file already exists on this property as ${name}. Pick it in Replace existing document if you want the new upload to supersede it.`;
    }
  }
  return error?.message || fallback;
}

const DEFAULT_FILTERS: AcquisitionQueueFiltersValue = {
  search: "",
  waitingOn: "ALL",
  urgency: "ALL",
  status: "ALL",
  missingDocsOnly: false,
  conflictsOnly: false,
  blockedOnly: false,
};

function formatMoney(value: any) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function formatPercent(value: any) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(0)}%`;
}

function waitingOnBucket(raw: string | null | undefined): string {
  const text = String(raw || "")
    .trim()
    .toLowerCase();
  if (!text) return "other";
  if (
    text.includes("lender") ||
    text.includes("loan") ||
    text.includes("finance")
  ) {
    return "lender";
  }
  if (text.includes("title") || text.includes("escrow")) return "title";
  if (text.includes("seller")) return "seller";
  if (
    text.includes("document") ||
    text.includes("doc") ||
    text.includes("inspection") ||
    text.includes("binder") ||
    text.includes("agreement")
  ) {
    return "document";
  }
  if (
    text.includes("operator") ||
    text.includes("review") ||
    text.includes("internal") ||
    text.includes("team")
  ) {
    return "operator";
  }
  return "other";
}

function urgencyBucket(
  item: Pick<
    QueueItem,
    "days_to_close" | "status" | "waiting_on" | "next_step"
  >,
): string {
  const days = Number(item.days_to_close);
  const status = String(item.status || "").toLowerCase();
  const waitingOn = String(item.waiting_on || "").toLowerCase();
  const nextStep = String(item.next_step || "").toLowerCase();

  if (
    status.includes("blocked") ||
    waitingOn.includes("blocked") ||
    nextStep.includes("blocked")
  ) {
    return "BLOCKED";
  }
  if (Number.isFinite(days) && days < 0) return "OVERDUE";
  if (Number.isFinite(days) && days <= 7) return "DUE_SOON";
  return "ON_TRACK";
}

function urgencyPillClass(label: string) {
  if (label === "OVERDUE") return "oh-pill oh-pill-bad";
  if (label === "DUE_SOON") return "oh-pill oh-pill-warn";
  if (label === "BLOCKED") return "oh-pill oh-pill-bad";
  return "oh-pill oh-pill-good";
}

function safeArray<T = any>(value: any): T[] {
  return Array.isArray(value) ? value : [];
}

function normalizeTags(
  payload: PropertyTagsPayload | null | undefined,
): string[] {
  if (!payload) return [];
  if (Array.isArray(payload.tags)) {
    return payload.tags.filter(Boolean).map(String);
  }
  if (Array.isArray(payload.rows)) {
    return payload.rows
      .map((row) => row?.tag)
      .filter(Boolean)
      .map(String);
  }
  return [];
}

function buildFieldValues(detail: AcquisitionDetail | null): FieldValueRow[] {
  const explicitValues = safeArray(detail?.acquisition?.field_values);
  if (explicitValues.length) {
    return explicitValues.map((row: any) => ({
      id: row?.id,
      field_name: row?.field_name ?? row?.key ?? row?.field,
      value_text:
        row?.value_text ?? (typeof row?.value === "string" ? row.value : null),
      value_number:
        row?.value_number ??
        (typeof row?.value === "number" ? row.value : null),
      review_state: row?.review_state ?? "suggested",
      confidence: row?.confidence ?? null,
      extraction_version: row?.extraction_version ?? null,
      manually_overridden: row?.manually_overridden ?? false,
      source_document_id: row?.source_document_id ?? null,
      source_document_name: row?.source_document_name ?? null,
    }));
  }

  const docs = safeArray<AcquisitionDocument>(detail?.documents);
  const rows: FieldValueRow[] = [];

  for (const doc of docs) {
    const fields = doc?.extracted_fields || {};
    for (const [key, value] of Object.entries(fields)) {
      if (value == null || value === "") continue;
      rows.push({
        id: undefined,
        field_name: key,
        value_text: typeof value === "string" ? value : null,
        value_number: typeof value === "number" ? value : null,
        review_state: "suggested",
        confidence: 0.8,
        extraction_version: doc?.parser_version ?? null,
        manually_overridden: false,
        source_document_id: doc?.id ?? null,
        source_document_name:
          doc?.name || doc?.original_filename || `Document #${doc?.id}`,
      });
    }
  }

  return rows;
}

function buildDeadlines(
  detail: AcquisitionDetail | null,
): AcquisitionDeadline[] {
  const explicit = safeArray(detail?.acquisition?.deadlines);
  if (explicit.length) {
    return explicit.map((row: any) => ({
      id: row?.id,
      kind: row?.kind,
      label: row?.label,
      due_at: row?.due_at,
      status: row?.status,
      waiting_on: row?.waiting_on,
      notes: row?.notes,
      days_remaining: row?.days_remaining,
    }));
  }

  const acq = detail?.acquisition || {};
  const synthetic: AcquisitionDeadline[] = [];

  if (acq.target_close_date) {
    synthetic.push({
      kind: "closing_datetime",
      label: "Closing",
      due_at: acq.target_close_date,
      status: "active",
      waiting_on: acq.waiting_on || null,
    });
  }

  return synthetic;
}

function buildParticipants(
  detail: AcquisitionDetail | null,
): AcquisitionParticipant[] {
  const explicit = safeArray(detail?.participants);
  if (explicit.length) {
    return explicit.map((row: any, idx: number) => ({
      id: row?.id ?? idx + 1,
      role: row?.role,
      name: row?.name,
      company: row?.company ?? null,
      email: row?.email ?? null,
      phone: row?.phone ?? null,
      is_primary: row?.is_primary ?? false,
      waiting_on: row?.waiting_on ?? false,
      source_type: row?.source_type ?? null,
      notes: row?.notes ?? null,
    }));
  }

  return safeArray(detail?.acquisition?.contacts).map(
    (row: any, idx: number) => ({
      id: row?.id ?? idx + 1,
      role: row?.role,
      name: row?.name,
      company: row?.company ?? null,
      email: row?.email ?? null,
      phone: row?.phone ?? null,
      is_primary: row?.is_primary ?? false,
      waiting_on: row?.waiting_on ?? false,
      source_type: row?.source_type ?? null,
      notes: row?.notes ?? null,
    }),
  );
}

function countConflicts(values: FieldValueRow[]) {
  const grouped = values.reduce<Record<string, string[]>>((acc, row) => {
    const key = String(row.field_name || "unknown");
    const value =
      row.value_text != null && row.value_text !== ""
        ? String(row.value_text)
        : row.value_number != null
          ? String(row.value_number)
          : "";
    if (!acc[key]) acc[key] = [];
    if (value) acc[key].push(value.trim().toLowerCase());
    return acc;
  }, {});

  return Object.values(grouped).filter((items) => new Set(items).size > 1)
    .length;
}

function missingDocumentGroups(detail: AcquisitionDetail | null) {
  return safeArray(detail?.required_documents).filter((doc) => !doc?.present);
}

function nextRequiredDocument(detail: AcquisitionDetail | null) {
  const missing = missingDocumentGroups(detail);
  if (missing.length) {
    return missing[0]?.label || missing[0]?.kind || "Required document";
  }
  return "No missing required documents";
}

function estimatedCloseReadiness(
  detail: AcquisitionDetail | null,
  fieldValues: FieldValueRow[],
): number {
  const summary = detail?.summary;
  const acquisition = detail?.acquisition;
  const total = Number(summary?.required_documents_total || 0);
  const present = Number(summary?.required_documents_present || 0);
  const documentCount = Number(summary?.document_count || 0);
  const days = Number(summary?.days_to_close);
  const waitingOn = String(acquisition?.waiting_on || "").toLowerCase();
  const conflicts = countConflicts(fieldValues);

  let score = 0;

  if (total > 0) score += Math.round((present / total) * 50);
  score += Math.min(documentCount * 4, 20);

  if (Number.isFinite(days)) {
    if (days > 14) score += 20;
    else if (days >= 7) score += 14;
    else if (days >= 0) score += 8;
    else score -= 10;
  }

  if (waitingOn.includes("document")) score -= 10;
  if (waitingOn.includes("blocked")) score -= 15;
  if (waitingOn.includes("seller")) score -= 5;
  if (conflicts > 0) score -= Math.min(conflicts * 8, 20);

  return Math.max(0, Math.min(100, score));
}

function readinessTone(score: number) {
  if (score >= 75) return "text-emerald-300";
  if (score >= 45) return "text-amber-300";
  return "text-red-300";
}

function documentHighlights(doc: AcquisitionDocument | null | undefined) {
  const raw = (doc as any)?.highlights;
  return Array.isArray(raw) ? raw : [];
}

export default function AcquisitionQueue() {
  const [filters, setFilters] =
    React.useState<AcquisitionQueueFiltersValue>(DEFAULT_FILTERS);
  const [queueLoading, setQueueLoading] = React.useState(true);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [savingRecord, setSavingRecord] = React.useState(false);
  const [uploadingDocument, setUploadingDocument] = React.useState(false);

  const [queueError, setQueueError] = React.useState<string | null>(null);
  const [detailError, setDetailError] = React.useState<string | null>(null);

  const [previewDocument, setPreviewDocument] =
    React.useState<AcquisitionDocument | null>(null);
  const [deletingDocumentId, setDeletingDocumentId] = React.useState<
    number | null
  >(null);
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

  const [queue, setQueue] = React.useState<QueueItem[]>([]);
  const [selectedPropertyId, setSelectedPropertyId] = React.useState<
    number | null
  >(null);
  const [detail, setDetail] = React.useState<AcquisitionDetail | null>(null);
  const [tags, setTags] = React.useState<PropertyTagsPayload | null>(null);

  const [recordDraft, setRecordDraft] = React.useState({
    status: "",
    waiting_on: "",
    next_step: "",
    contract_date: "",
    target_close_date: "",
    closing_date: "",
    purchase_price: "",
    earnest_money: "",
    loan_amount: "",
    loan_type: "",
    interest_rate: "",
    cash_to_close: "",
    closing_costs: "",
    seller_credits: "",
    title_company: "",
    escrow_officer: "",
    notes: "",
  });

  const loadQueue = React.useCallback(async () => {
    setQueueLoading(true);
    setQueueError(null);

    try {
      const res = await api.get<{ items?: QueueItem[]; count?: number }>(
        "/acquisition/queue",
        { params: { limit: 1000 } },
      );

      const items = Array.isArray(res?.items) ? res.items : [];
      setQueue(items);

      if (!selectedPropertyId && items.length > 0) {
        setSelectedPropertyId(Number(items[0].property_id));
      } else if (
        selectedPropertyId &&
        !items.some(
          (item) => Number(item.property_id) === Number(selectedPropertyId),
        )
      ) {
        setSelectedPropertyId(
          items.length ? Number(items[0].property_id) : null,
        );
      }
    } catch (error: any) {
      setQueueError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to load queue.",
      );
      setQueue([]);
    } finally {
      setQueueLoading(false);
    }
  }, [selectedPropertyId]);

  const loadDetail = React.useCallback(async (propertyId: number) => {
    setDetailLoading(true);
    setDetailError(null);

    try {
      const [detailRes, tagsRes] = await Promise.allSettled([
        api.get<AcquisitionDetail>(`/acquisition/properties/${propertyId}`),
        api.get<PropertyTagsPayload>(
          `/properties/${propertyId}/acquisition-tags`,
        ),
      ]);

      if (detailRes.status === "fulfilled") {
        const payload = detailRes.value;
        setDetail(payload);

        const acq = payload?.acquisition || {};
        setRecordDraft({
          status: acq.status || "",
          waiting_on: acq.waiting_on || "",
          next_step: acq.next_step || "",
          contract_date: acq.contract_date || "",
          target_close_date: acq.target_close_date || "",
          closing_date: acq.closing_date || "",
          purchase_price:
            acq.purchase_price != null ? String(acq.purchase_price) : "",
          earnest_money:
            acq.earnest_money != null ? String(acq.earnest_money) : "",
          loan_amount: acq.loan_amount != null ? String(acq.loan_amount) : "",
          loan_type: acq.loan_type || "",
          interest_rate:
            acq.interest_rate != null ? String(acq.interest_rate) : "",
          cash_to_close:
            acq.cash_to_close != null ? String(acq.cash_to_close) : "",
          closing_costs:
            acq.closing_costs != null ? String(acq.closing_costs) : "",
          seller_credits:
            acq.seller_credits != null ? String(acq.seller_credits) : "",
          title_company: acq.title_company || "",
          escrow_officer: acq.escrow_officer || "",
          notes: acq.notes || "",
        });
      } else {
        setDetail(null);
        setDetailError("Failed to load acquisition detail.");
      }

      if (tagsRes.status === "fulfilled") {
        setTags(tagsRes.value);
      } else {
        setTags(null);
      }
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to load acquisition detail.",
      );
      setDetail(null);
      setTags(null);
    } finally {
      setDetailLoading(false);
    }
  }, []);

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

  async function handleRemoveFromAcquire() {
    if (!selectedPropertyId) return;
    setRemoveAcquireSaving(true);
    setRemoveAcquireError(null);

    try {
      const preserve_tags = [
        removePreserveSaved ? "saved" : null,
        removePreserveShortlisted ? "shortlisted" : null,
      ].filter(Boolean);

      await api.post<RemoveFromAcquireResponse>(
        `/acquisition/properties/${selectedPropertyId}/remove`,
        {
          delete_documents: true,
          delete_deadlines: true,
          delete_field_reviews: true,
          delete_contacts: true,
          hard_delete_files: true,
          preserve_tags,
        },
      );

      setShowRemoveAcquireModal(false);
      await loadQueue();
    } catch (error: any) {
      setRemoveAcquireError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to remove property from Acquire.",
      );
    } finally {
      setRemoveAcquireSaving(false);
    }
  }

  React.useEffect(() => {
    loadQueue();
  }, [loadQueue]);

  React.useEffect(() => {
    if (selectedPropertyId) {
      loadDetail(selectedPropertyId);
    } else {
      setDetail(null);
      setTags(null);
    }
  }, [selectedPropertyId, loadDetail]);

  const fieldValues = React.useMemo(() => buildFieldValues(detail), [detail]);
  const deadlines = React.useMemo(() => buildDeadlines(detail), [detail]);
  const participants = React.useMemo(() => buildParticipants(detail), [detail]);
  const currentTags = React.useMemo(() => normalizeTags(tags), [tags]);
  const readiness = React.useMemo(
    () => estimatedCloseReadiness(detail, fieldValues),
    [detail, fieldValues],
  );
  const conflictCount = React.useMemo(
    () => countConflicts(fieldValues),
    [fieldValues],
  );
  const missingDocs = React.useMemo(
    () => missingDocumentGroups(detail),
    [detail],
  );
  const nextDoc = React.useMemo(() => nextRequiredDocument(detail), [detail]);

  const filteredQueue = React.useMemo(() => {
    const query = filters.search.trim().toLowerCase();

    return queue.filter((item) => {
      const haystack = [
        item.address,
        item.city,
        item.state,
        item.zip,
        item.county,
        item.waiting_on,
        item.next_step,
        item.listing_agent_name,
        item.listing_office_name,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      if (query && !haystack.includes(query)) return false;

      if (
        filters.waitingOn !== "ALL" &&
        waitingOnBucket(item.waiting_on) !== filters.waitingOn.toLowerCase()
      ) {
        return false;
      }

      const urgency = urgencyBucket(item);
      if (filters.urgency !== "ALL" && urgency !== filters.urgency) {
        return false;
      }

      const status = String(item.status || "").toUpperCase();
      if (filters.status !== "ALL" && status !== filters.status) {
        return false;
      }

      if (
        filters.missingDocsOnly &&
        !safeArray(item.missing_document_groups).length
      ) {
        return false;
      }

      if (filters.conflictsOnly && Number(item.conflict_count || 0) <= 0) {
        return false;
      }

      if (
        filters.blockedOnly &&
        urgencyBucket(item) !== "BLOCKED" &&
        !String(item.waiting_on || "")
          .toLowerCase()
          .includes("blocked")
      ) {
        return false;
      }

      return true;
    });
  }, [queue, filters]);

  const selectedQueueItem = React.useMemo(
    () =>
      filteredQueue.find(
        (item) => Number(item.property_id) === Number(selectedPropertyId),
      ) ||
      queue.find(
        (item) => Number(item.property_id) === Number(selectedPropertyId),
      ) ||
      null,
    [filteredQueue, queue, selectedPropertyId],
  );

  async function saveRecord() {
    if (!selectedPropertyId) return;
    setSavingRecord(true);
    setDetailError(null);

    try {
      const payload = {
        status: recordDraft.status || null,
        waiting_on: recordDraft.waiting_on || null,
        next_step: recordDraft.next_step || null,
        contract_date: recordDraft.contract_date || null,
        target_close_date: recordDraft.target_close_date || null,
        closing_date: recordDraft.closing_date || null,
        purchase_price:
          recordDraft.purchase_price === ""
            ? null
            : Number(recordDraft.purchase_price),
        earnest_money:
          recordDraft.earnest_money === ""
            ? null
            : Number(recordDraft.earnest_money),
        loan_amount:
          recordDraft.loan_amount === ""
            ? null
            : Number(recordDraft.loan_amount),
        loan_type: recordDraft.loan_type || null,
        interest_rate:
          recordDraft.interest_rate === ""
            ? null
            : Number(recordDraft.interest_rate),
        cash_to_close:
          recordDraft.cash_to_close === ""
            ? null
            : Number(recordDraft.cash_to_close),
        closing_costs:
          recordDraft.closing_costs === ""
            ? null
            : Number(recordDraft.closing_costs),
        seller_credits:
          recordDraft.seller_credits === ""
            ? null
            : Number(recordDraft.seller_credits),
        title_company: recordDraft.title_company || null,
        escrow_officer: recordDraft.escrow_officer || null,
        notes: recordDraft.notes || null,
      };

      await api.put(`/acquisition/properties/${selectedPropertyId}`, payload);
      await Promise.all([loadQueue(), loadDetail(selectedPropertyId)]);
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to save acquisition record.",
      );
    } finally {
      setSavingRecord(false);
    }
  }

  async function seedListingParticipants() {
    if (!selectedPropertyId) return;
    setDetailLoading(true);
    setDetailError(null);
    try {
      await api.post(
        `/acquisition/properties/${selectedPropertyId}/participants/seed-listing`,
      );
      await loadDetail(selectedPropertyId);
      await loadQueue();
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to seed listing contacts into acquisition.",
      );
    } finally {
      setDetailLoading(false);
    }
  }

  async function onUploadDocument(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file || !selectedPropertyId) return;

    setUploadingDocument(true);
    setDetailError(null);

    try {
      const form = new FormData();
      form.append("kind", uploadKind || suggestAcquisitionDocKind(file.name));
      form.append("file", file);
      form.append("name", file.name);
      if (uploadReplaceDocumentId) {
        form.append("replace_document_id", uploadReplaceDocumentId);
      }

      await api.post(
        `/acquisition/properties/${selectedPropertyId}/documents/upload`,
        form,
      );

      setUploadReplaceDocumentId("");
      await loadDetail(selectedPropertyId);
      await loadQueue();
    } catch (error: any) {
      const status = error?.response?.status;

      if (status === 413) {
        setDetailError(
          "Upload failed because the file is too large for the server/proxy limit.",
        );
      } else {
        setDetailError(
          extractDetailMessage(error, "Failed to upload document."),
        );
      }
    } finally {
      setUploadingDocument(false);
      event.target.value = "";
    }
  }

  function openPreview(doc: AcquisitionDocument) {
    setPreviewDocument(doc);
  }

  async function onDeleteDocument(documentId: number) {
    if (!selectedPropertyId) return;

    const confirmed = window.confirm("Delete this document?");
    if (!confirmed) return;

    setDeletingDocumentId(documentId);
    setDetailError(null);

    try {
      await api.delete(
        `/acquisition/properties/${selectedPropertyId}/documents/${documentId}`,
        { params: { hard_delete_file: true } },
      );
      if (previewDocument?.id === documentId) {
        setPreviewDocument(null);
      }
      await loadDetail(selectedPropertyId);
      await loadQueue();
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to delete document.",
      );
    } finally {
      setDeletingDocumentId(null);
    }
  }

  return (
    <PageShell>
      <PageHero
        eyebrow="Acquisition pane"
        title="What am I waiting on?"
        subtitle="The queue now carries listing-agent and office contacts directly into acquisition so follow-up can start immediately from ingested listing data."
        right={
          <button
            type="button"
            className="oh-btn oh-btn-secondary"
            onClick={loadQueue}
          >
            <RefreshCcw className="h-4 w-4" />
            Refresh queue
          </button>
        }
      />

      <div className="space-y-6">
        <AcquisitionFilters value={filters} onChange={setFilters} />

        {queueError ? (
          <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
            {queueError}
          </div>
        ) : null}

        <div className="grid grid-cols-1 gap-6 xl:grid-cols-[400px_minmax(0,1fr)]">
          <Surface
            title="Acquisition queue"
            subtitle="Operational queue with waiting-on, readiness, and imported listing contact context."
          >
            {queueLoading ? (
              <div className="flex items-center justify-center py-16 text-app-4">
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            ) : !filteredQueue.length ? (
              <EmptyState title="No acquisition rows match the current filters" />
            ) : (
              <div className="space-y-3">
                {filteredQueue.map((item) => {
                  const selected =
                    Number(item.property_id) === Number(selectedPropertyId);
                  const urgency = urgencyBucket(item);

                  return (
                    <button
                      key={item.property_id}
                      type="button"
                      onClick={() =>
                        setSelectedPropertyId(Number(item.property_id))
                      }
                      className={`w-full rounded-2xl border px-4 py-4 text-left transition ${
                        selected
                          ? "border-app-strong bg-app-muted"
                          : "border-app bg-app-panel hover:bg-app-muted"
                      }`}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-app-0">
                            {item.address || "Unknown address"}
                          </div>
                          <div className="mt-1 text-xs text-app-4">
                            {[item.city, item.state, item.zip]
                              .filter(Boolean)
                              .join(", ")}
                          </div>
                        </div>

                        <div className="flex flex-wrap gap-2">
                          <span className={urgencyPillClass(urgency)}>
                            {urgency}
                          </span>
                          {item.listing_status ? (
                            <span className="oh-pill">
                              {item.listing_status}
                            </span>
                          ) : null}
                        </div>
                      </div>

                      <div className="mt-3 flex flex-wrap gap-2">
                        <span className="oh-pill">
                          readiness{" "}
                          {formatPercent(item.estimated_close_readiness)}
                        </span>
                        <span className="oh-pill">
                          docs {Number(item.document_count || 0)}
                        </span>
                        <span className="oh-pill">
                          conflicts {Number(item.conflict_count || 0)}
                        </span>
                        <span className="oh-pill">
                          participants {Number(item.participant_count || 0)}
                        </span>
                        {item.listing_days_on_market != null ? (
                          <span className="oh-pill">
                            DOM {Number(item.listing_days_on_market)}
                          </span>
                        ) : null}
                      </div>

                      <div className="mt-3 text-sm text-app-3">
                        waiting on{" "}
                        <span className="text-app-0">
                          {item.waiting_on || "—"}
                        </span>
                      </div>

                      {item.listing_agent_name || item.listing_office_name ? (
                        <div className="mt-3 rounded-2xl border border-app bg-app-muted px-3 py-3">
                          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                            Listing handoff contacts
                          </div>
                          <div className="mt-2 space-y-1 text-sm text-app-2">
                            {item.listing_agent_name ? (
                              <div>
                                Agent:{" "}
                                <span className="font-medium text-app-0">
                                  {item.listing_agent_name}
                                </span>
                              </div>
                            ) : null}
                            {item.listing_office_name ? (
                              <div>
                                Office:{" "}
                                <span className="font-medium text-app-0">
                                  {item.listing_office_name}
                                </span>
                              </div>
                            ) : null}
                          </div>
                        </div>
                      ) : null}
                    </button>
                  );
                })}
              </div>
            )}
          </Surface>

          <div className="space-y-6">
            {detailError ? (
              <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {detailError}
              </div>
            ) : null}

            {!selectedPropertyId ? (
              <EmptyState title="Select an acquisition row" />
            ) : detailLoading ? (
              <Surface>
                <div className="flex items-center justify-center py-16 text-app-4">
                  <Loader2 className="h-5 w-5 animate-spin" />
                </div>
              </Surface>
            ) : !detail ? (
              <EmptyState title="Detail unavailable" />
            ) : (
              <>
                <Surface
                  title="Selected deal"
                  subtitle="Core deal state, listing context, and immediate follow-up path."
                >
                  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Purchase price
                      </div>
                      <div className="mt-2 text-xl font-semibold text-app-0">
                        {formatMoney(detail?.acquisition?.purchase_price)}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Readiness
                      </div>
                      <div
                        className={`mt-2 text-xl font-semibold ${readinessTone(readiness)}`}
                      >
                        {formatPercent(readiness)}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Next required doc
                      </div>
                      <div className="mt-2 text-sm font-semibold text-app-0">
                        {nextDoc}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Waiting on
                      </div>
                      <div className="mt-2 text-sm font-semibold text-app-0">
                        {detail?.acquisition?.waiting_on || "—"}
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 flex flex-wrap gap-2">
                    {currentTags.map((tag) => (
                      <span key={tag} className="oh-pill">
                        {tag}
                      </span>
                    ))}
                  </div>
                </Surface>

                <Surface
                  title="Listing handoff"
                  subtitle="Imported listing-agent and office contacts available directly inside acquisition."
                  actions={
                    <div className="flex flex-wrap gap-2">
                      <button
                        type="button"
                        className="oh-btn oh-btn-secondary oh-btn-sm"
                        onClick={seedListingParticipants}
                      >
                        <Users className="h-4 w-4" />
                        Refresh listing contacts
                      </button>

                      {detail?.property?.listing_zillow_url ? (
                        <a
                          href={detail.property.listing_zillow_url}
                          target="_blank"
                          rel="noreferrer"
                          className="oh-btn oh-btn-secondary oh-btn-sm"
                        >
                          <ExternalLink className="h-4 w-4" />
                          Zillow
                        </a>
                      ) : null}
                    </div>
                  }
                >
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <Users className="h-4 w-4" />
                        Listing agent
                      </div>
                      <div className="mt-3 text-base font-semibold text-app-0">
                        {detail?.property?.listing_agent_name ||
                          "No agent name"}
                      </div>
                      <div className="mt-3 space-y-2 text-sm text-app-2">
                        <div className="flex items-center gap-2">
                          <Phone className="h-4 w-4 text-app-4" />
                          <span>
                            {detail?.property?.listing_agent_phone ||
                              "No phone"}
                          </span>
                        </div>
                        <div className="flex items-center gap-2">
                          <Mail className="h-4 w-4 text-app-4" />
                          <span>
                            {detail?.property?.listing_agent_email ||
                              "No email"}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <Building2 className="h-4 w-4" />
                        Listing office
                      </div>
                      <div className="mt-3 text-base font-semibold text-app-0">
                        {detail?.property?.listing_office_name ||
                          "No office name"}
                      </div>
                      <div className="mt-3 space-y-2 text-sm text-app-2">
                        <div className="flex items-center gap-2">
                          <Phone className="h-4 w-4 text-app-4" />
                          <span>
                            {detail?.property?.listing_office_phone ||
                              "No phone"}
                          </span>
                        </div>
                        <div className="flex items-center gap-2">
                          <Mail className="h-4 w-4 text-app-4" />
                          <span>
                            {detail?.property?.listing_office_email ||
                              "No email"}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 rounded-2xl border border-emerald-500/20 bg-emerald-500/[0.06] px-4 py-3 text-sm text-emerald-100">
                    Imported listing contacts are now part of the acquisition
                    workflow, so operator follow-up can happen without manual
                    re-entry.
                  </div>
                </Surface>

                <AcquisitionParticipantsPanel
                  participants={participants}
                  waitingOn={detail?.acquisition?.waiting_on}
                />

                <div className="grid gap-6 xl:grid-cols-2">
                  <AcquisitionDeadlinePanel
                    deadlines={deadlines}
                    waitingOn={detail?.acquisition?.waiting_on}
                  />

                  <DocumentFieldReviewPanel
                    propertyId={Number(selectedPropertyId)}
                    items={fieldValues}
                    documents={safeArray(detail?.documents).map((doc) => ({
                      id: doc.id,
                      name: doc.name,
                      kind: doc.kind,
                      parse_status: doc.parse_status,
                      scan_status: doc.scan_status,
                      preview_text: doc.preview_text,
                      actionable_intelligence: doc.actionable_intelligence,
                    }))}
                    missingDocumentGroups={missingDocs.map((doc) => ({
                      kind: doc.kind || "",
                      label: doc.label || "",
                    }))}
                    nextRequiredDocument={nextDoc}
                    estimatedCloseReadiness={readiness}
                    onAction={async () => {
                      if (selectedPropertyId) {
                        await loadDetail(selectedPropertyId);
                        await loadQueue();
                      }
                    }}
                  />
                </div>

                <Surface
                  title="Acquisition record"
                  subtitle="Keep the live acquisition fields updated without leaving the queue."
                  actions={
                    <div className="flex w-full flex-col gap-2 lg:w-auto lg:flex-row lg:flex-wrap lg:items-end">
                      <label className="block min-w-[220px]">
                        <span className="oh-field-label">Document kind</span>
                        <div className="oh-select-wrap">
                          <select
                            className="oh-select w-full"
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

                      <label className="block min-w-[240px]">
                        <span className="oh-field-label">
                          Replace existing document
                        </span>
                        <div className="oh-select-wrap">
                          <select
                            className="oh-select w-full"
                            value={uploadReplaceDocumentId}
                            onChange={(e) =>
                              setUploadReplaceDocumentId(e.target.value)
                            }
                          >
                            <option value="">Do not replace</option>
                            {safeArray(detail?.documents).map((doc) => (
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

                      <label className="oh-btn oh-btn-secondary oh-btn-sm cursor-pointer">
                        <Upload className="h-4 w-4" />
                        {uploadingDocument ? "Uploading..." : "Upload doc"}
                        <input
                          type="file"
                          className="hidden"
                          onChange={(event) => {
                            const file = event.target.files?.[0];
                            if (file) {
                              const suggested = suggestAcquisitionDocKind(
                                file.name,
                              );
                              if (!uploadKind) setUploadKind(suggested);
                            }
                            onUploadDocument(event);
                          }}
                          disabled={uploadingDocument}
                        />
                      </label>

                      <button
                        type="button"
                        className="oh-btn oh-btn-sm"
                        onClick={saveRecord}
                        disabled={savingRecord}
                      >
                        {savingRecord ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <CheckCircle2 className="h-4 w-4" />
                        )}
                        Save
                      </button>

                      {selectedPropertyId ? (
                        <button
                          type="button"
                          className="oh-btn oh-btn-secondary oh-btn-sm border-red-500/30 text-red-100 hover:border-red-400/50 hover:bg-red-500/10"
                          onClick={openRemoveAcquireModal}
                        >
                          <RotateCcw className="h-4 w-4" />
                          Remove from Acquire
                        </button>
                      ) : null}
                    </div>
                  }
                >
                  {safeArray(detail?.documents).length ? (
                    <div className="mb-6 space-y-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                        Uploaded documents
                      </div>

                      <div className="rounded-2xl border border-app bg-app-muted/40 px-4 py-3 text-sm text-app-4">
                        Allowed kinds:{" "}
                        {ACQUISITION_DOCUMENT_KIND_OPTIONS.map(
                          (option) => option.label,
                        ).join(", ")}
                        . The exact same file is blocked unless you
                        intentionally replace an existing document.
                      </div>

                      {safeArray(detail?.documents).map((doc) => (
                        <div
                          key={doc.id}
                          className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                        >
                          <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                              <div className="text-sm font-semibold text-app-0">
                                {doc.name ||
                                  doc.original_filename ||
                                  `Document #${doc.id}`}
                              </div>
                              <div className="mt-1 text-xs text-app-4">
                                {[
                                  acquisitionDocKindLabel(doc.kind),
                                  doc.parse_status,
                                  doc.content_type,
                                ]
                                  .filter(Boolean)
                                  .join(" • ")}
                              </div>
                            </div>

                            <div className="flex flex-wrap gap-2">
                              {doc.file_size_bytes != null ? (
                                <span className="oh-pill">
                                  {Math.round(
                                    Number(doc.file_size_bytes) / 1024,
                                  )}{" "}
                                  KB
                                </span>
                              ) : null}
                              {doc.created_at ? (
                                <span className="oh-pill">
                                  {new Date(
                                    doc.created_at,
                                  ).toLocaleDateString()}
                                </span>
                              ) : null}
                            </div>
                          </div>

                          {documentHighlights(doc).length ? (
                            <div className="mt-3 space-y-2">
                              {documentHighlights(doc)
                                .slice(0, 3)
                                .map((item: any, idx: number) => (
                                  <div
                                    key={`${doc.id}-${item.code}-${idx}`}
                                    className="rounded-xl border border-app bg-app-muted/40 px-3 py-3"
                                  >
                                    <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
                                      {String(item.code || "").replace(
                                        /_/g,
                                        " ",
                                      )}
                                    </div>
                                    <div className="mt-1 text-sm text-app-1">
                                      {item.excerpt}
                                    </div>
                                  </div>
                                ))}
                            </div>
                          ) : null}

                          <div className="mt-3 flex flex-wrap gap-2">
                            <button
                              type="button"
                              className="oh-btn oh-btn-secondary"
                              onClick={() => openPreview(doc)}
                            >
                              Preview
                            </button>

                            <a
                              href={`/api/acquisition/properties/${selectedPropertyId}/documents/${doc.id}/preview`}
                              target="_blank"
                              rel="noreferrer"
                              className="oh-btn oh-btn-secondary"
                            >
                              Open raw file
                            </a>

                            <button
                              type="button"
                              className="oh-btn oh-btn-secondary"
                              onClick={() => {
                                setUploadKind(
                                  String(
                                    doc.kind ||
                                      uploadKind ||
                                      "inspection_report",
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
                              onClick={() => onDeleteDocument(Number(doc.id))}
                              disabled={deletingDocumentId === Number(doc.id)}
                            >
                              {deletingDocumentId === Number(doc.id)
                                ? "Deleting..."
                                : "Delete file"}
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : null}
                  <div className="grid gap-4 md:grid-cols-2">
                    <label className="block">
                      <span className="oh-field-label">Status</span>
                      <input
                        className="oh-input"
                        value={recordDraft.status}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            status: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Waiting on</span>
                      <input
                        className="oh-input"
                        value={recordDraft.waiting_on}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
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
                        value={recordDraft.next_step}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            next_step: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Target close date</span>
                      <input
                        type="date"
                        className="oh-input"
                        value={recordDraft.target_close_date}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            target_close_date: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Closing date</span>
                      <input
                        type="date"
                        className="oh-input"
                        value={recordDraft.closing_date}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            closing_date: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Purchase price</span>
                      <input
                        className="oh-input"
                        value={recordDraft.purchase_price}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            purchase_price: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Loan amount</span>
                      <input
                        className="oh-input"
                        value={recordDraft.loan_amount}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
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
                        value={recordDraft.cash_to_close}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            cash_to_close: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Closing costs</span>
                      <input
                        className="oh-input"
                        value={recordDraft.closing_costs}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            closing_costs: e.target.value,
                          }))
                        }
                      />
                    </label>

                    <label className="block">
                      <span className="oh-field-label">Title company</span>
                      <input
                        className="oh-input"
                        value={recordDraft.title_company}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
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
                        value={recordDraft.escrow_officer}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
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
                        value={recordDraft.notes}
                        onChange={(e) =>
                          setRecordDraft((s) => ({
                            ...s,
                            notes: e.target.value,
                          }))
                        }
                      />
                    </label>
                  </div>
                </Surface>
              </>
            )}
          </div>
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
                  This destructive rollback deletes the acquisition workspace
                  and sends the property back to Investor.
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
                  All acquisition documents will be deleted from the workspace.
                </li>
                <li>
                  Parsed field review rows, deadline rows, and acquisition
                  contacts will be cleared.
                </li>
                <li>
                  The acquisition record will be removed and the property will
                  route back to Investor.
                </li>
                <li>offer_candidate will be removed automatically.</li>
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
                <span>Preserve saved tag.</span>
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
                <span>Preserve shortlisted tag.</span>
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

      {previewDocument ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/55 p-4">
          <div className="max-h-[90vh] w-full max-w-5xl overflow-y-auto rounded-[32px] border border-app bg-app px-6 py-6 shadow-2xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold text-app-0">
                  {previewDocument?.name ||
                    previewDocument?.original_filename ||
                    "Document preview"}
                </div>
                <div className="mt-1 text-sm text-app-4">
                  {acquisitionDocKindLabel(previewDocument?.kind)}
                </div>
              </div>

              <button
                type="button"
                className="rounded-xl border border-app p-2 text-app-3 hover:bg-app-panel"
                onClick={() => setPreviewDocument(null)}
              >
                ✕
              </button>
            </div>

            {documentHighlights(previewDocument).length ? (
              <div className="mt-6 rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Important highlights
                </div>
                <div className="mt-3 space-y-3">
                  {documentHighlights(previewDocument).map(
                    (item: any, idx: number) => (
                      <div
                        key={`${item.code}-${idx}`}
                        className="rounded-xl border border-app bg-app-muted/40 px-3 py-3"
                      >
                        <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
                          {String(item.code || "").replace(/_/g, " ")}
                        </div>
                        <div className="mt-1 text-sm text-app-1">
                          {item.excerpt}
                        </div>
                      </div>
                    ),
                  )}
                </div>
              </div>
            ) : null}

            {previewDocument?.actionable_intelligence ? (
              <div className="mt-6 grid gap-4 lg:grid-cols-2">
                {Array.isArray(
                  previewDocument.actionable_intelligence
                    .recommended_next_actions,
                ) &&
                previewDocument.actionable_intelligence.recommended_next_actions
                  .length ? (
                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      Actionable next steps
                    </div>
                    <div className="mt-3 space-y-2 text-sm text-app-1">
                      {previewDocument.actionable_intelligence.recommended_next_actions
                        .slice(0, 6)
                        .map((item: string, idx: number) => (
                          <div key={`action-${idx}`}>• {item}</div>
                        ))}
                    </div>
                  </div>
                ) : null}
                {Array.isArray(
                  previewDocument.actionable_intelligence.risk_flags,
                ) &&
                previewDocument.actionable_intelligence.risk_flags.length ? (
                  <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-4 py-4">
                    <div className="text-[11px] uppercase tracking-[0.18em] text-red-200">
                      Risk / warning flags
                    </div>
                    <div className="mt-3 space-y-2 text-sm text-red-100">
                      {previewDocument.actionable_intelligence.risk_flags
                        .slice(0, 6)
                        .map((item: any, idx: number) => (
                          <div key={`risk-${idx}`}>
                            {item?.label || item?.code || "warning"}
                          </div>
                        ))}
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}

            {previewDocument?.preview_text ? (
              <div className="mt-6 rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Preview text
                </div>
                <pre className="mt-3 whitespace-pre-wrap text-sm text-app-1">
                  {previewDocument.preview_text}
                </pre>
              </div>
            ) : null}

            {previewDocument?.extracted_fields &&
            Object.keys(previewDocument.extracted_fields).length ? (
              <div className="mt-6 rounded-2xl border border-app bg-app-panel px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Extracted fields
                </div>
                <div className="mt-3 grid gap-3 md:grid-cols-2">
                  {Object.entries(previewDocument.extracted_fields).map(
                    ([key, value]) => (
                      <div
                        key={key}
                        className="rounded-xl border border-app bg-app-muted/40 px-3 py-3"
                      >
                        <div className="text-xs uppercase tracking-[0.14em] text-app-4">
                          {key.replace(/_/g, " ")}
                        </div>
                        <div className="mt-1 text-sm text-app-0">
                          {String(value)}
                        </div>
                      </div>
                    ),
                  )}
                </div>
              </div>
            ) : null}

            <div className="mt-6 flex flex-wrap gap-3">
              <a
                href={`/api/acquisition/properties/${selectedPropertyId}/documents/${previewDocument.id}/preview`}
                target="_blank"
                rel="noreferrer"
                className="oh-btn oh-btn-secondary"
              >
                Open raw file
              </a>

              <button
                type="button"
                className="oh-btn"
                onClick={() => onDeleteDocument(Number(previewDocument.id))}
                disabled={deletingDocumentId === Number(previewDocument.id)}
              >
                {deletingDocumentId === Number(previewDocument.id)
                  ? "Deleting..."
                  : "Delete file"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </PageShell>
  );
}
