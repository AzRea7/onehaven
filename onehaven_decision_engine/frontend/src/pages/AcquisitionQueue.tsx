import React from "react";
import {
  CheckCircle2,
  Clock3,
  FileText,
  Loader2,
  RefreshCcw,
  Upload,
  AlertTriangle,
  GitCompareArrows,
  ShieldAlert,
  Users,
} from "lucide-react";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import Golem from "../components/Golem";
import AcquisitionFilters, {
  type AcquisitionQueueFiltersValue,
} from "../components/AcquisitionFilters";
import ShortlistBoard from "../components/ShortlistBoard";
import AcquisitionTagBar from "../components/AcquisitionTagBar";
import AcquisitionDeadlinePanel, {
  type AcquisitionDeadline,
} from "../components/AcquisitionDeadlinePanel";
import AcquisitionParticipantsPanel, {
  type AcquisitionParticipant,
} from "../components/AcquisitionParticipantsPanel";
import DocumentFieldReviewPanel, {
  type FieldValueRow,
} from "../components/DocumentFieldReviewPanel";
import { api } from "../lib/api";

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
    milestones?: any[];
    days_to_close?: number | null;
    deadlines?: any[];
    field_values?: any[];
  };
  documents?: AcquisitionDocument[];
  required_documents?: RequiredDocument[];
  summary?: {
    days_to_close?: number | null;
    document_count?: number | null;
    required_documents_total?: number | null;
    required_documents_present?: number | null;
  };
};

type PropertyTagsPayload = {
  property_id?: number;
  tags?: string[];
  rows?: Array<{ tag?: string }>;
};

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

export default function AcquisitionQueue() {
  const [filters, setFilters] =
    React.useState<AcquisitionQueueFiltersValue>(DEFAULT_FILTERS);
  const [queueLoading, setQueueLoading] = React.useState(true);
  const [detailLoading, setDetailLoading] = React.useState(false);
  const [savingRecord, setSavingRecord] = React.useState(false);
  const [uploadingDocument, setUploadingDocument] = React.useState(false);

  const [queueError, setQueueError] = React.useState<string | null>(null);
  const [detailError, setDetailError] = React.useState<string | null>(null);

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
      const waitBucket = waitingOnBucket(item.waiting_on);
      const urgency = urgencyBucket(item);
      const status = String(item.status || "").toLowerCase();

      const haystack = [
        item.address,
        item.city,
        item.state,
        item.zip,
        item.county,
        item.status,
        item.waiting_on,
        item.next_step,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      if (filters.waitingOn !== "ALL" && waitBucket !== filters.waitingOn)
        return false;
      if (filters.urgency !== "ALL" && urgency !== filters.urgency)
        return false;
      if (
        filters.status !== "ALL" &&
        !status.includes(filters.status.toLowerCase())
      ) {
        return false;
      }
      if (filters.blockedOnly && urgency !== "BLOCKED") return false;
      if (query && !haystack.includes(query)) return false;

      if (filters.missingDocsOnly) {
        const missingCount =
          safeArray(item.missing_document_groups).length > 0
            ? safeArray(item.missing_document_groups).length
            : Number(item.document_count || 0) <= 0
              ? 1
              : 0;
        if (missingCount <= 0) return false;
      }

      if (filters.conflictsOnly) {
        if (Number(item.conflict_count || 0) <= 0) return false;
      }

      return true;
    });
  }, [filters, queue]);

  const filteredFieldValues = React.useMemo(() => {
    if (!filters.conflictsOnly) return fieldValues;

    const grouped = fieldValues.reduce<Record<string, FieldValueRow[]>>(
      (acc, row) => {
        const key = String(row.field_name || "unknown");
        if (!acc[key]) acc[key] = [];
        acc[key].push(row);
        return acc;
      },
      {},
    );

    const conflictFields = new Set(
      Object.entries(grouped)
        .filter(([, rows]) => {
          const values = rows
            .map((row) =>
              row.value_text != null && row.value_text !== ""
                ? String(row.value_text).trim().toLowerCase()
                : row.value_number != null
                  ? String(row.value_number).trim().toLowerCase()
                  : "",
            )
            .filter(Boolean);
          return new Set(values).size > 1;
        })
        .map(([field]) => field),
    );

    return fieldValues.filter((row) =>
      conflictFields.has(String(row.field_name || "unknown")),
    );
  }, [fieldValues, filters.conflictsOnly]);

  const urgencyCounts = React.useMemo(() => {
    return queue.reduce(
      (acc, item) => {
        const urgency = urgencyBucket(item);
        if (urgency === "OVERDUE") acc.overdue += 1;
        if (urgency === "DUE_SOON") acc.dueSoon += 1;
        if (urgency === "BLOCKED") acc.blocked += 1;
        return acc;
      },
      { overdue: 0, dueSoon: 0, blocked: 0 },
    );
  }, [queue]);

  async function handleSaveRecord() {
    if (!selectedPropertyId) return;

    setSavingRecord(true);
    setDetailError(null);

    try {
      const payload = {
        ...recordDraft,
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

  async function handleUploadDocument(
    event: React.ChangeEvent<HTMLInputElement>,
  ) {
    if (!selectedPropertyId) return;
    const file = event.target.files?.[0];
    if (!file) return;

    setUploadingDocument(true);
    setDetailError(null);

    try {
      const formData = new FormData();
      formData.append("kind", "other");
      formData.append("name", file.name);
      formData.append("file", file);

      await api.post(
        `/acquisition/properties/${selectedPropertyId}/documents/upload`,
        formData,
      );
      await Promise.all([loadQueue(), loadDetail(selectedPropertyId)]);
      event.target.value = "";
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to upload document.",
      );
    } finally {
      setUploadingDocument(false);
    }
  }

  async function handleTagsChange(nextTags: string[]) {
    if (!selectedPropertyId) return;

    try {
      const payload = await api.put<PropertyTagsPayload>(
        `/properties/${selectedPropertyId}/acquisition-tags`,
        { tags: nextTags },
      );
      setTags(payload);
      await loadQueue();
    } catch (error: any) {
      setDetailError(
        error?.response?.data?.detail ||
          error?.message ||
          "Failed to update tags.",
      );
    }
  }

  async function handleFieldValuesChanged() {
    if (!selectedPropertyId) return;
    await loadDetail(selectedPropertyId);
  }

  const selectedUrgency = urgencyBucket({
    days_to_close: detail?.summary?.days_to_close ?? null,
    status: detail?.acquisition?.status ?? null,
    waiting_on: detail?.acquisition?.waiting_on ?? null,
    next_step: detail?.acquisition?.next_step ?? null,
  });

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Pane 2"
          title="Acquisition queue"
          subtitle="Operate the deal. See what you are waiting on, what is overdue, what documents are missing, where parser results disagree, and how close each file is to closing."
          actions={
            <button
              onClick={loadQueue}
              className="oh-btn oh-btn-secondary"
              type="button"
            >
              {queueLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCcw className="h-4 w-4" />
              )}
              Refresh queue
            </button>
          }
          right={
            <div className="pointer-events-none absolute right-0 top-0 hidden h-full w-[240px] items-end justify-end overflow-hidden xl:flex">
              <div className="translate-x-6 translate-y-4 opacity-90">
                <Golem className="h-[210px] w-[210px]" />
              </div>
            </div>
          }
        />

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-5">
          <Surface title="Active files" subtitle="Deals in acquisition">
            <div className="text-3xl font-semibold text-app-0">
              {queue.length}
            </div>
          </Surface>
          <Surface title="Overdue" subtitle="Past close timing">
            <div className="text-3xl font-semibold text-red-300">
              {urgencyCounts.overdue}
            </div>
          </Surface>
          <Surface title="Due soon" subtitle="Seven days or less">
            <div className="text-3xl font-semibold text-amber-300">
              {urgencyCounts.dueSoon}
            </div>
          </Surface>
          <Surface title="Blocked" subtitle="Action cannot move">
            <div className="text-3xl font-semibold text-red-300">
              {urgencyCounts.blocked}
            </div>
          </Surface>
          <Surface title="Conflicts" subtitle="Disagreeing parsed values">
            <div className="text-3xl font-semibold text-app-0">
              {conflictCount}
            </div>
          </Surface>
        </div>

        <ShortlistBoard rows={filteredQueue} />

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[380px_minmax(0,1fr)]">
          <div className="space-y-4">
            <Surface
              title="Queue filters"
              subtitle="Filter by owner, urgency, status, missing docs, and conflicts."
            >
              <AcquisitionFilters value={filters} onChange={setFilters} />
            </Surface>

            <Surface
              title="Queue"
              subtitle="Select a file to open the workspace."
            >
              {queueLoading ? (
                <div className="flex items-center gap-2 py-12 text-sm text-app-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading acquisition queue…
                </div>
              ) : queueError ? (
                <EmptyState
                  compact
                  title="Queue unavailable"
                  description={queueError}
                />
              ) : filteredQueue.length === 0 ? (
                <EmptyState
                  compact
                  title="No deals match these filters"
                  description="Clear filters or refresh the queue."
                />
              ) : (
                <div className="space-y-3">
                  {filteredQueue.map((item) => {
                    const isSelected =
                      Number(selectedPropertyId) === Number(item.property_id);
                    const urgency = urgencyBucket(item);
                    const missingCount = safeArray(
                      item.missing_document_groups,
                    ).length;
                    const readinessScore = Number(
                      item.estimated_close_readiness,
                    );

                    return (
                      <button
                        key={item.property_id}
                        type="button"
                        onClick={() =>
                          setSelectedPropertyId(Number(item.property_id))
                        }
                        className={`w-full rounded-2xl border p-4 text-left transition ${
                          isSelected
                            ? "border-app-strong bg-app-muted"
                            : "border-app bg-app-panel hover:bg-app-muted"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-3">
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
                          <span className={urgencyPillClass(urgency)}>
                            {urgency === "ON_TRACK"
                              ? "on track"
                              : urgency.toLowerCase()}
                          </span>
                        </div>

                        <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-app-4">
                          <div className="rounded-xl border border-app bg-app-muted px-3 py-2">
                            waiting on: {item.waiting_on || "—"}
                          </div>
                          <div className="rounded-xl border border-app bg-app-muted px-3 py-2">
                            next: {item.next_step || "—"}
                          </div>
                          <div className="rounded-xl border border-app bg-app-muted px-3 py-2">
                            missing docs: {missingCount}
                          </div>
                          <div className="rounded-xl border border-app bg-app-muted px-3 py-2">
                            conflicts: {Number(item.conflict_count || 0)}
                          </div>
                        </div>

                        <div className="mt-2 flex flex-wrap gap-2 text-xs">
                          <span className="oh-pill">
                            {Number.isFinite(Number(item.days_to_close))
                              ? `${Number(item.days_to_close)}d to close`
                              : "no close date"}
                          </span>
                          <span className="oh-pill">
                            docs {Number(item.document_count || 0)}
                          </span>
                          {Number.isFinite(readinessScore) ? (
                            <span className="oh-pill">
                              readiness {Math.round(readinessScore)}%
                            </span>
                          ) : null}
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}
            </Surface>
          </div>

          <div className="space-y-4">
            {!selectedPropertyId ? (
              <Surface>
                <EmptyState
                  title="No acquisition file selected"
                  description="Choose a property from the queue to open its workspace."
                />
              </Surface>
            ) : detailLoading ? (
              <Surface>
                <div className="flex items-center gap-2 py-16 text-sm text-app-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading acquisition workspace…
                </div>
              </Surface>
            ) : detailError ? (
              <Surface>
                <EmptyState
                  title="Workspace unavailable"
                  description={detailError}
                />
              </Surface>
            ) : !detail ? (
              <Surface>
                <EmptyState
                  title="No acquisition detail returned"
                  description="Refresh the queue or select another file."
                />
              </Surface>
            ) : (
              <>
                <Surface
                  title={detail.property?.address || "Acquisition workspace"}
                  subtitle={[
                    detail.property?.city,
                    detail.property?.state,
                    detail.property?.zip,
                  ]
                    .filter(Boolean)
                    .join(", ")}
                >
                  <div className="grid gap-4 xl:grid-cols-[1.35fr_1fr]">
                    <div className="space-y-4">
                      <div className="rounded-3xl border border-app bg-app-panel p-5">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                              What am I waiting on?
                            </div>
                            <div className="mt-2 text-2xl font-semibold text-app-0">
                              {detail.acquisition?.waiting_on ||
                                "Nothing assigned"}
                            </div>
                            <div className="mt-2 text-sm text-app-4">
                              {detail.acquisition?.next_step ||
                                "No next step set."}
                            </div>
                          </div>

                          <div className="flex flex-wrap gap-2">
                            <span className={urgencyPillClass(selectedUrgency)}>
                              {selectedUrgency === "ON_TRACK"
                                ? "on track"
                                : selectedUrgency.toLowerCase()}
                            </span>
                            {detail.acquisition?.status ? (
                              <span className="oh-pill">
                                {detail.acquisition.status}
                              </span>
                            ) : null}
                          </div>
                        </div>

                        <div className="mt-4 grid gap-3 md:grid-cols-4">
                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <Users className="h-3.5 w-3.5" />
                              Waiting owner
                            </div>
                            <div className="mt-2 text-lg font-semibold text-app-0">
                              {waitingOnBucket(
                                detail.acquisition?.waiting_on,
                              ).toLowerCase()}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <Clock3 className="h-3.5 w-3.5" />
                              Days to close
                            </div>
                            <div className="mt-2 text-lg font-semibold text-app-0">
                              {Number.isFinite(
                                Number(detail.summary?.days_to_close),
                              )
                                ? Number(detail.summary?.days_to_close)
                                : "—"}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <FileText className="h-3.5 w-3.5" />
                              Missing groups
                            </div>
                            <div className="mt-2 text-lg font-semibold text-app-0">
                              {missingDocs.length}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <CheckCircle2 className="h-3.5 w-3.5" />
                              Readiness
                            </div>
                            <div
                              className={`mt-2 text-lg font-semibold ${readinessTone(readiness)}`}
                            >
                              {readiness}%
                            </div>
                          </div>
                        </div>

                        <div className="mt-4 grid gap-3 md:grid-cols-3">
                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <ShieldAlert className="h-3.5 w-3.5" />
                              Next required document
                            </div>
                            <div className="mt-2 text-sm font-semibold text-app-0">
                              {nextDoc}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <GitCompareArrows className="h-3.5 w-3.5" />
                              Parsed disagreements
                            </div>
                            <div className="mt-2 text-sm font-semibold text-app-0">
                              {conflictCount}
                            </div>
                          </div>

                          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                              <AlertTriangle className="h-3.5 w-3.5" />
                              Status
                            </div>
                            <div className="mt-2 text-sm font-semibold text-app-0">
                              {detail.acquisition?.status || "—"}
                            </div>
                          </div>
                        </div>

                        <div className="mt-4">
                          <AcquisitionTagBar
                            propertyId={selectedPropertyId}
                            value={currentTags}
                            onChange={handleTagsChange}
                          />
                        </div>
                      </div>

                      <Surface
                        title="Edit acquisition record"
                        subtitle="Keep the canonical deal state current."
                      >
                        <div className="grid gap-4 md:grid-cols-2">
                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Status
                            </div>
                            <input
                              value={recordDraft.status}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  status: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                              placeholder="in_progress / waiting_on_docs / blocked / ready_to_close"
                            />
                          </label>

                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Target close date
                            </div>
                            <input
                              type="date"
                              value={recordDraft.target_close_date}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  target_close_date: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>

                          <label className="block md:col-span-2">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Waiting on
                            </div>
                            <input
                              value={recordDraft.waiting_on}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  waiting_on: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                              placeholder="Lender conditions, title commitment, signed addendum…"
                            />
                          </label>

                          <label className="block md:col-span-2">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Next step
                            </div>
                            <input
                              value={recordDraft.next_step}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  next_step: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                              placeholder="Order appraisal, clear title issue, finalize insurance…"
                            />
                          </label>

                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Purchase price
                            </div>
                            <input
                              value={recordDraft.purchase_price}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  purchase_price: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>

                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Loan amount
                            </div>
                            <input
                              value={recordDraft.loan_amount}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  loan_amount: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>

                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Cash to close
                            </div>
                            <input
                              value={recordDraft.cash_to_close}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  cash_to_close: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>

                          <label className="block">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Closing costs
                            </div>
                            <input
                              value={recordDraft.closing_costs}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  closing_costs: e.target.value,
                                }))
                              }
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>

                          <label className="block md:col-span-2">
                            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                              Notes
                            </div>
                            <textarea
                              value={recordDraft.notes}
                              onChange={(e) =>
                                setRecordDraft((prev) => ({
                                  ...prev,
                                  notes: e.target.value,
                                }))
                              }
                              rows={5}
                              className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                            />
                          </label>
                        </div>

                        <div className="mt-4">
                          <button
                            onClick={handleSaveRecord}
                            disabled={savingRecord}
                            className="oh-btn oh-btn-secondary"
                            type="button"
                          >
                            {savingRecord ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <RefreshCcw className="h-4 w-4" />
                            )}
                            Save acquisition record
                          </button>
                        </div>
                      </Surface>
                    </div>

                    <div className="space-y-4">
                      <Surface
                        title="Upload supporting document"
                        subtitle="Add new files into the acquisition stack."
                      >
                        <label className="inline-flex cursor-pointer items-center gap-2 rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0">
                          {uploadingDocument ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <Upload className="h-4 w-4" />
                          )}
                          Upload file
                          <input
                            type="file"
                            className="hidden"
                            onChange={handleUploadDocument}
                          />
                        </label>
                      </Surface>

                      <AcquisitionDeadlinePanel deadlines={deadlines} />

                      <AcquisitionParticipantsPanel
                        participants={participants}
                        waitingOn={detail.acquisition?.waiting_on}
                      />
                    </div>
                  </div>
                </Surface>

                <DocumentFieldReviewPanel
                  propertyId={selectedPropertyId}
                  values={filteredFieldValues}
                  onChanged={handleFieldValuesChanged}
                />
              </>
            )}
          </div>
        </div>
      </div>
    </PageShell>
  );
}
