import React from "react";
import {
  AlertCircle,
  ArrowUpRight,
  CalendarClock,
  CheckCircle2,
  Clock3,
  DollarSign,
  Download,
  Eye,
  FileText,
  Loader2,
  MapPin,
  Phone,
  Search,
  Upload,
  User2,
  Paperclip,
  AlertTriangle,
  GitCompareArrows,
  ShieldAlert,
  Users,
} from "lucide-react";
import { Link } from "react-router-dom";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";

type QueueRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  county?: string;
  current_stage?: string;
  status?: string;
  waiting_on?: string;
  next_step?: string;
  contract_date?: string | null;
  target_close_date?: string | null;
  closing_date?: string | null;
  purchase_price?: number | null;
  loan_amount?: number | null;
  cash_to_close?: number | null;
  closing_costs?: number | null;
  document_count?: number;
  days_to_close?: number | null;
};

type AcquisitionDetail = {
  property: any;
  acquisition: any;
  documents: any[];
  required_documents: { kind: string; label: string; present: boolean }[];
  summary: {
    days_to_close?: number | null;
    document_count: number;
    required_documents_total: number;
    required_documents_present: number;
  };
};

type DocKind =
  | "purchase_agreement"
  | "loan_documents"
  | "loan_estimate"
  | "closing_disclosure"
  | "title_documents"
  | "insurance_binder"
  | "inspection_report"
  | "other";

const DOC_KIND_OPTIONS: { value: DocKind; label: string }[] = [
  { value: "purchase_agreement", label: "Purchase agreement" },
  { value: "loan_documents", label: "Loan documents" },
  { value: "loan_estimate", label: "Loan estimate" },
  { value: "closing_disclosure", label: "Closing disclosure" },
  { value: "title_documents", label: "Title / escrow" },
  { value: "insurance_binder", label: "Insurance binder" },
  { value: "inspection_report", label: "Inspection report" },
  { value: "other", label: "Other" },
];

const CLIENT_ALLOWED_EXTENSIONS = [
  ".pdf",
  ".docx",
  ".txt",
  ".png",
  ".jpg",
  ".jpeg",
];
const CLIENT_MAX_UPLOAD_BYTES = 15 * 1024 * 1024;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function textValue(v: any, fallback = "—") {
  const s = String(v ?? "").trim();
  return s || fallback;
}

function daysLabel(v: number | null | undefined) {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  if (v < 0) return `${Math.abs(v)}d past`;
  if (v === 0) return "Today";
  return `${v}d`;
}

function toneForDays(v: number | null | undefined) {
  if (v == null || !Number.isFinite(Number(v))) return "text-app-2";
  if (v <= 3) return "text-red-300";
  if (v <= 10) return "text-amber-300";
  return "text-emerald-300";
}

function formatDate(v?: string | null) {
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return v;
  return d.toLocaleDateString();
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

function validateUploadFile(file: File | null): string | null {
  if (!file) return "Choose a file first.";

  const lower = file.name.toLowerCase();
  const hasAllowedExt = CLIENT_ALLOWED_EXTENSIONS.some((ext) =>
    lower.endsWith(ext),
  );
  if (!hasAllowedExt) {
    return `Unsupported file type. Allowed: ${CLIENT_ALLOWED_EXTENSIONS.join(", ")}`;
  }

  if (file.size > CLIENT_MAX_UPLOAD_BYTES) {
    return "File is too large. Max 15 MB.";
  }

  return null;
}

function StatusPill({ status }: { status?: string | null }) {
  const normalized = String(status || "needs_setup").toLowerCase();

  let cls = "border-app bg-app-panel text-app-2";
  if (["clear_to_close", "ready_to_close", "complete"].includes(normalized)) {
    cls = "border-emerald-500/30 bg-emerald-500/10 text-emerald-200";
  } else if (
    ["blocked", "issue", "needs_attention", "at_risk"].includes(normalized)
  ) {
    cls = "border-red-500/30 bg-red-500/10 text-red-200";
  } else if (
    [
      "waiting_on_docs",
      "waiting_on_lender",
      "waiting_on_title",
      "in_progress",
    ].includes(normalized)
  ) {
    cls = "border-amber-500/30 bg-amber-500/10 text-amber-200";
  }

  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${cls}`}
    >
      {normalized.replace(/_/g, " ")}
    </span>
  );
}

function MetricCard({
  label,
  value,
  icon,
  tone = "",
}: {
  label: string;
  value: React.ReactNode;
  icon: React.ReactNode;
  tone?: string;
}) {
  return (
    <div className="rounded-3xl border border-app bg-app px-4 py-4">
      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
        {icon}
        {label}
      </div>
      <div className={`mt-2 text-lg font-semibold ${tone || "text-app-0"}`}>
        {value}
      </div>
    </div>
  );
}

function QueueRowCard({
  row,
  active,
  onClick,
}: {
  row: QueueRow;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full rounded-3xl border p-4 text-left transition ${
        active
          ? "border-app-strong bg-app-panel shadow-[0_0_0_1px_rgba(255,255,255,0.08)]"
          : "border-app bg-app"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-app-0">
            {row.address || "Unknown address"}
          </div>
          <div className="mt-1 flex items-center gap-1 text-xs text-app-4">
            <MapPin className="h-3.5 w-3.5" />
            {[row.city, row.state, row.zip].filter(Boolean).join(", ")}
          </div>
        </div>
        <StatusPill status={row.status} />
      </div>

      <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
        <div className="rounded-2xl border border-app bg-app-panel px-3 py-2">
          <div className="text-app-4">Waiting on</div>
          <div className="mt-1 font-medium text-app-1">
            {textValue(row.waiting_on)}
          </div>
        </div>
        <div className="rounded-2xl border border-app bg-app-panel px-3 py-2">
          <div className="text-app-4">Days to close</div>
          <div className={`mt-1 font-medium ${toneForDays(row.days_to_close)}`}>
            {daysLabel(row.days_to_close)}
          </div>
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between gap-2 text-xs text-app-4">
        <span>Docs {row.document_count ?? 0}</span>
        <span>{formatDate(row.target_close_date || row.closing_date)}</span>
      </div>
    </button>
  );
}

function waitingOnCategory(raw?: string) {
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
  if (text.includes("title") || text.includes("escrow")) return "TITLE";
  if (text.includes("seller")) return "SELLER";
  if (
    text.includes("document") ||
    text.includes("doc") ||
    text.includes("inspection") ||
    text.includes("binder") ||
    text.includes("agreement")
  ) {
    return "DOCUMENT";
  }
  if (
    text.includes("operator") ||
    text.includes("review") ||
    text.includes("internal") ||
    text.includes("team")
  ) {
    return "OPERATOR";
  }
  return "OTHER";
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
    ? detail?.required_documents
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

export default function AcquisitionPane() {
  const [q, setQ] = React.useState("");
  const [queue, setQueue] = React.useState<QueueRow[]>([]);
  const [selectedId, setSelectedId] = React.useState<number | null>(null);
  const [detail, setDetail] = React.useState<AcquisitionDetail | null>(null);

  const [loadingQueue, setLoadingQueue] = React.useState(true);
  const [loadingDetail, setLoadingDetail] = React.useState(false);
  const [savingRecord, setSavingRecord] = React.useState(false);
  const [importingDoc, setImportingDoc] = React.useState(false);
  const [uploadingFile, setUploadingFile] = React.useState(false);

  const [queueErr, setQueueErr] = React.useState<string | null>(null);
  const [detailErr, setDetailErr] = React.useState<string | null>(null);
  const [uploadErr, setUploadErr] = React.useState<string | null>(null);

  const [editStatus, setEditStatus] = React.useState("");
  const [editWaitingOn, setEditWaitingOn] = React.useState("");
  const [editNextStep, setEditNextStep] = React.useState("");
  const [editTargetCloseDate, setEditTargetCloseDate] = React.useState("");
  const [editPurchasePrice, setEditPurchasePrice] = React.useState("");
  const [editLoanAmount, setEditLoanAmount] = React.useState("");
  const [editCashToClose, setEditCashToClose] = React.useState("");
  const [editClosingCosts, setEditClosingCosts] = React.useState("");
  const [editNotes, setEditNotes] = React.useState("");

  const [docKind, setDocKind] = React.useState<DocKind>("purchase_agreement");
  const [docName, setDocName] = React.useState("");
  const [docSourceUrl, setDocSourceUrl] = React.useState("");
  const [docExtractedText, setDocExtractedText] = React.useState("");
  const [docNotes, setDocNotes] = React.useState("");

  const [uploadKind, setUploadKind] =
    React.useState<DocKind>("purchase_agreement");
  const [uploadName, setUploadName] = React.useState("");
  const [uploadNotes, setUploadNotes] = React.useState("");
  const [uploadFile, setUploadFile] = React.useState<File | null>(null);

  const fileInputRef = React.useRef<HTMLInputElement | null>(null);

  const loadQueue = React.useCallback(async () => {
    setLoadingQueue(true);
    setQueueErr(null);
    try {
      const res = await api.get<any>("/acquisition/queue", {
        params: { q: q || undefined, limit: 500 },
      });
      const items = Array.isArray(res?.items) ? res.items : [];
      setQueue(items);

      if (!items.length) {
        setSelectedId(null);
        setDetail(null);
        return;
      }

      setSelectedId((current) => {
        if (
          current &&
          items.some((x: QueueRow) => Number(x.property_id) === current)
        ) {
          return current;
        }
        return Number(items[0].property_id);
      });
    } catch (e: any) {
      setQueueErr(formatApiError(e, "Failed to load acquisition queue."));
      setQueue([]);
      setSelectedId(null);
      setDetail(null);
    } finally {
      setLoadingQueue(false);
    }
  }, [q]);

  const loadDetail = React.useCallback(async (propertyId: number) => {
    if (!propertyId) return;
    setLoadingDetail(true);
    setDetailErr(null);
    try {
      const res = await api.get<AcquisitionDetail>(
        `/acquisition/properties/${propertyId}`,
      );
      setDetail(res);

      const acq = res?.acquisition || {};
      setEditStatus(String(acq?.status || ""));
      setEditWaitingOn(String(acq?.waiting_on || ""));
      setEditNextStep(String(acq?.next_step || ""));
      setEditTargetCloseDate(String(acq?.target_close_date || "").slice(0, 10));
      setEditPurchasePrice(
        acq?.purchase_price != null ? String(acq.purchase_price) : "",
      );
      setEditLoanAmount(
        acq?.loan_amount != null ? String(acq.loan_amount) : "",
      );
      setEditCashToClose(
        acq?.cash_to_close != null ? String(acq.cash_to_close) : "",
      );
      setEditClosingCosts(
        acq?.closing_costs != null ? String(acq.closing_costs) : "",
      );
      setEditNotes(String(acq?.notes || ""));
    } catch (e: any) {
      setDetailErr(formatApiError(e, "Failed to load acquisition property."));
      setDetail(null);
    } finally {
      setLoadingDetail(false);
    }
  }, []);

  React.useEffect(() => {
    loadQueue();
  }, [loadQueue]);

  React.useEffect(() => {
    if (selectedId) {
      loadDetail(selectedId);
    }
  }, [selectedId, loadDetail]);

  const handleSaveRecord = React.useCallback(async () => {
    if (!selectedId) return;
    setSavingRecord(true);
    try {
      await api.put(`/acquisition/properties/${selectedId}`, {
        status: editStatus || null,
        waiting_on: editWaitingOn || null,
        next_step: editNextStep || null,
        target_close_date: editTargetCloseDate || null,
        purchase_price: editPurchasePrice ? Number(editPurchasePrice) : null,
        loan_amount: editLoanAmount ? Number(editLoanAmount) : null,
        cash_to_close: editCashToClose ? Number(editCashToClose) : null,
        closing_costs: editClosingCosts ? Number(editClosingCosts) : null,
        notes: editNotes || null,
      });

      await Promise.all([loadQueue(), loadDetail(selectedId)]);
    } catch (e: any) {
      setDetailErr(formatApiError(e, "Failed to save acquisition record."));
    } finally {
      setSavingRecord(false);
    }
  }, [
    selectedId,
    editStatus,
    editWaitingOn,
    editNextStep,
    editTargetCloseDate,
    editPurchasePrice,
    editLoanAmount,
    editCashToClose,
    editClosingCosts,
    editNotes,
    loadQueue,
    loadDetail,
  ]);

  const handleImportDoc = React.useCallback(async () => {
    if (!selectedId) return;
    setImportingDoc(true);
    try {
      await api.post(`/acquisition/properties/${selectedId}/documents`, {
        kind:
          docKind ||
          DOC_KIND_OPTIONS.find((x) => x.value === docKind)?.label ||
          "Imported document",
        name:
          docName ||
          DOC_KIND_OPTIONS.find((x) => x.value === docKind)?.label ||
          "Imported document",
        source_url: docSourceUrl || null,
        extracted_text: docExtractedText || null,
        notes: docNotes || null,
        status: "received",
      });

      setDocName("");
      setDocSourceUrl("");
      setDocExtractedText("");
      setDocNotes("");

      await Promise.all([loadQueue(), loadDetail(selectedId)]);
    } catch (e: any) {
      setDetailErr(formatApiError(e, "Failed to import acquisition document."));
    } finally {
      setImportingDoc(false);
    }
  }, [
    selectedId,
    docKind,
    docName,
    docSourceUrl,
    docExtractedText,
    docNotes,
    loadQueue,
    loadDetail,
  ]);

  const handlePreviewDocument = React.useCallback(
    async (documentId: number, filename?: string) => {
      if (!selectedId) return;

      try {
        const blob = await api.get<Blob>(
          `/acquisition/properties/${selectedId}/documents/${documentId}/preview`,
          { responseType: "blob" },
        );

        const url = URL.createObjectURL(blob);
        window.open(url, "_blank", "noopener,noreferrer");

        setTimeout(() => {
          URL.revokeObjectURL(url);
        }, 60_000);
      } catch (e: any) {
        setDetailErr(formatApiError(e, "Failed to preview document."));
      }
    },
    [selectedId],
  );

  const handleDownloadDocument = React.useCallback(
    async (documentId: number, filename?: string) => {
      if (!selectedId) return;

      try {
        const blob = await api.get<Blob>(
          `/acquisition/properties/${selectedId}/documents/${documentId}/download`,
          { responseType: "blob" },
        );

        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename || "document";
        document.body.appendChild(a);
        a.click();
        a.remove();

        setTimeout(() => {
          URL.revokeObjectURL(url);
        }, 60_000);
      } catch (e: any) {
        setDetailErr(formatApiError(e, "Failed to download document."));
      }
    },
    [selectedId],
  );

  const handleUploadFile = React.useCallback(async () => {
    if (!selectedId) return;

    const validation = validateUploadFile(uploadFile);
    if (validation) {
      setUploadErr(validation);
      return;
    }

    setUploadErr(null);
    setUploadingFile(true);

    try {
      const fd = new FormData();
      fd.append("kind", uploadKind);
      fd.append("name", uploadName || uploadFile?.name || "Uploaded document");
      fd.append("notes", uploadNotes || "");
      if (uploadFile) {
        fd.append("file", uploadFile);
      }

      await api.post(
        `/acquisition/properties/${selectedId}/documents/upload`,
        fd,
      );

      setUploadName("");
      setUploadNotes("");
      setUploadFile(null);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }

      await Promise.all([loadQueue(), loadDetail(selectedId)]);
    } catch (e: any) {
      setUploadErr(formatApiError(e, "Failed to upload file."));
    } finally {
      setUploadingFile(false);
    }
  }, [
    selectedId,
    uploadKind,
    uploadName,
    uploadNotes,
    uploadFile,
    loadQueue,
    loadDetail,
  ]);

  const activeQueueRow = React.useMemo(
    () =>
      queue.find((x) => Number(x.property_id) === Number(selectedId)) || null,
    [queue, selectedId],
  );

  const property = detail?.property || {};
  const acquisition = detail?.acquisition || {};
  const contacts = Array.isArray(acquisition?.contacts)
    ? acquisition.contacts
    : [];
  const documents = Array.isArray(detail?.documents) ? detail.documents : [];
  const requiredDocuments = Array.isArray(detail?.required_documents)
    ? detail.required_documents
    : [];

  const waitingOn = textValue(acquisition?.waiting_on, "Nothing assigned");
  const waitingCategory = waitingOnCategory(acquisition?.waiting_on);
  const urgency = urgencyLabel(
    detail?.summary?.days_to_close,
    acquisition?.waiting_on,
  );
  const missingDocs = requiredDocsMissing(detail);
  const conflicts = collectConflicts(detail);
  const readiness = closeReadiness(detail);

  return (
    <PageShell>
      <PageHero
        eyebrow="Pane 2"
        title="Acquisition Command Center"
        subtitle="Operate the file. Surface waiting owner, urgency, missing documents, parsed disagreements, and close readiness."
      />

      <div className="grid gap-4 xl:grid-cols-[340px_minmax(0,1fr)]">
        <Surface className="p-4 xl:sticky xl:top-4 xl:h-[calc(100vh-160px)] xl:overflow-hidden">
          <div className="flex h-full flex-col">
            <div className="mb-4">
              <div className="text-sm font-semibold text-app-0">
                Properties in acquisition
              </div>
              <div className="mt-1 text-xs text-app-4">
                Clean queue view for contract-to-close work.
              </div>
            </div>

            <div className="relative mb-4">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search address, city, waiting on..."
                className="w-full rounded-2xl border border-app bg-app-panel py-3 pl-10 pr-4 text-sm text-app-0 outline-none"
              />
            </div>

            <button
              onClick={loadQueue}
              className="mb-4 rounded-2xl border border-app bg-app px-3 py-2 text-sm text-app-1"
            >
              Refresh queue
            </button>

            {queueErr ? (
              <EmptyState title="Queue failed to load" description={queueErr} />
            ) : loadingQueue ? (
              <div className="flex flex-1 items-center justify-center text-app-4">
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Loading acquisition queue…
              </div>
            ) : queue.length === 0 ? (
              <EmptyState
                title="No properties in acquisition"
                description="Once a property moves into offer / contract / acquisition, it will show here."
              />
            ) : (
              <div className="flex-1 space-y-3 overflow-y-auto pr-1">
                {queue.map((row) => (
                  <QueueRowCard
                    key={row.property_id}
                    row={row}
                    active={Number(selectedId) === Number(row.property_id)}
                    onClick={() => setSelectedId(Number(row.property_id))}
                  />
                ))}
              </div>
            )}
          </div>
        </Surface>

        <div className="grid gap-4">
          {!selectedId ? (
            <Surface className="p-8">
              <EmptyState
                title="Select a property"
                description="Choose a property from the acquisition queue."
              />
            </Surface>
          ) : loadingDetail ? (
            <Surface className="p-8">
              <div className="flex items-center justify-center text-app-4">
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Loading acquisition workspace…
              </div>
            </Surface>
          ) : detailErr ? (
            <Surface className="p-8">
              <EmptyState
                title="Acquisition detail failed to load"
                description={detailErr}
              />
            </Surface>
          ) : !detail ? (
            <Surface className="p-8">
              <EmptyState
                title="No acquisition detail"
                description="The property could not be loaded."
              />
            </Surface>
          ) : (
            <>
              <Surface className="p-5">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div>
                    <div className="text-xl font-semibold text-app-0">
                      {property?.address || "Unknown address"}
                    </div>
                    <div className="mt-1 flex items-center gap-1 text-sm text-app-4">
                      <MapPin className="h-4 w-4" />
                      {[property?.city, property?.state, property?.zip]
                        .filter(Boolean)
                        .join(", ")}
                    </div>
                    <div className="mt-3 flex flex-wrap gap-2">
                      <StatusPill
                        status={acquisition?.status || activeQueueRow?.status}
                      />
                      <span className="oh-pill">
                        Stage{" "}
                        {textValue(property?.current_stage, "acquisition")}
                      </span>
                    </div>
                  </div>

                  <div className="flex items-center gap-3">
                    <Link
                      to={`/properties/${property?.property_id}`}
                      className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app px-4 py-2 text-sm text-app-0"
                    >
                      Open property
                      <ArrowUpRight className="h-4 w-4" />
                    </Link>
                  </div>
                </div>

                <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <MetricCard
                    label="Days to close"
                    value={daysLabel(detail?.summary?.days_to_close)}
                    icon={<Clock3 className="h-3.5 w-3.5" />}
                    tone={toneForDays(detail?.summary?.days_to_close)}
                  />
                  <MetricCard
                    label="Closing costs"
                    value={money(acquisition?.closing_costs)}
                    icon={<DollarSign className="h-3.5 w-3.5" />}
                  />
                  <MetricCard
                    label="Cash to close"
                    value={money(acquisition?.cash_to_close)}
                    icon={<DollarSign className="h-3.5 w-3.5" />}
                  />
                  <MetricCard
                    label="Docs complete"
                    value={`${detail?.summary?.required_documents_present ?? 0}/${detail?.summary?.required_documents_total ?? 0}`}
                    icon={<FileText className="h-3.5 w-3.5" />}
                  />
                </div>

                <div className="mt-5">
                  <div className="grid gap-4 xl:grid-cols-4">
                    <div className="rounded-3xl border border-app bg-app-panel p-5">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <Users className="h-3.5 w-3.5" />
                        Waiting on
                      </div>
                      <div className="mt-3 text-lg font-semibold text-app-0">
                        {waitingOn}
                      </div>
                      <div className="mt-2">
                        <span className="oh-pill">
                          {waitingCategory.toLowerCase()}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-3xl border border-app bg-app-panel p-5">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <Clock3 className="h-3.5 w-3.5" />
                        Close timing
                      </div>
                      <div
                        className={`mt-3 text-lg font-semibold ${
                          Number.isFinite(
                            Number(detail?.summary?.days_to_close),
                          )
                            ? toneForDays(detail?.summary?.days_to_close)
                            : "text-app-0"
                        }`}
                      >
                        {Number.isFinite(Number(detail?.summary?.days_to_close))
                          ? Number(detail!.summary.days_to_close) < 0
                            ? `${Math.abs(Number(detail!.summary.days_to_close))} days overdue`
                            : `${Number(detail!.summary.days_to_close)} days remaining`
                          : "No target close date"}
                      </div>
                      <div className="mt-2">
                        <span className={urgencyPillClass(urgency)}>
                          {urgency}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-3xl border border-app bg-app-panel p-5">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <ShieldAlert className="h-3.5 w-3.5" />
                        Next required document
                      </div>
                      <div className="mt-3 text-lg font-semibold text-app-0">
                        {nextRequiredDocument(detail)}
                      </div>
                      <div className="mt-2">
                        <span className="oh-pill">
                          {missingDocs.length} missing groups
                        </span>
                      </div>
                    </div>

                    <div className="rounded-3xl border border-app bg-app-panel p-5">
                      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                        <CheckCircle2 className="h-3.5 w-3.5" />
                        Estimated close readiness
                      </div>
                      <div
                        className={`mt-3 text-lg font-semibold ${readinessTone(readiness)}`}
                      >
                        {readiness}%
                      </div>
                      <div className="mt-2">
                        <span
                          className={
                            conflicts.length
                              ? "oh-pill oh-pill-warn"
                              : "oh-pill"
                          }
                        >
                          {conflicts.length} parsed disagreement
                          {conflicts.length === 1 ? "" : "s"}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </Surface>

              <div className="grid gap-4 2xl:grid-cols-[1.15fr_0.85fr]">
                <Surface className="p-5">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-sm font-semibold text-app-0">
                        Acquisition status + close economics
                      </div>
                      <div className="mt-1 text-xs text-app-4">
                        Update current state, next blocker, and close numbers.
                      </div>
                    </div>
                    <button
                      onClick={handleSaveRecord}
                      disabled={savingRecord}
                      className="rounded-2xl border border-app bg-app px-4 py-2 text-sm text-app-0 disabled:opacity-60"
                    >
                      {savingRecord ? "Saving…" : "Save"}
                    </button>
                  </div>

                  <div className="mt-5 grid gap-4 md:grid-cols-2">
                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Status
                      </div>
                      <input
                        value={editStatus}
                        onChange={(e) => setEditStatus(e.target.value)}
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
                        value={editTargetCloseDate}
                        onChange={(e) => setEditTargetCloseDate(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block md:col-span-2">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Waiting on
                      </div>
                      <input
                        value={editWaitingOn}
                        onChange={(e) => setEditWaitingOn(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                        placeholder="Lender conditions, title commitment, signed addendum…"
                      />
                    </label>

                    <label className="block md:col-span-2">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Next step
                      </div>
                      <input
                        value={editNextStep}
                        onChange={(e) => setEditNextStep(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                        placeholder="Order appraisal, clear title issue, finalize insurance…"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Purchase price
                      </div>
                      <input
                        value={editPurchasePrice}
                        onChange={(e) => setEditPurchasePrice(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Loan amount
                      </div>
                      <input
                        value={editLoanAmount}
                        onChange={(e) => setEditLoanAmount(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Cash to close
                      </div>
                      <input
                        value={editCashToClose}
                        onChange={(e) => setEditCashToClose(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Closing costs
                      </div>
                      <input
                        value={editClosingCosts}
                        onChange={(e) => setEditClosingCosts(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block md:col-span-2">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Notes
                      </div>
                      <textarea
                        value={editNotes}
                        onChange={(e) => setEditNotes(e.target.value)}
                        rows={5}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                        placeholder="Key open items, negotiation details, title issues, lender feedback…"
                      />
                    </label>
                  </div>
                </Surface>

                <div className="grid gap-4">
                  <Surface className="p-5">
                    <div className="text-sm font-semibold text-app-0">
                      Required document coverage
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      Immediate scan of what the file stack still needs.
                    </div>

                    <div className="mt-4 space-y-2">
                      {requiredDocuments.map((doc) => (
                        <div
                          key={doc.kind}
                          className="flex items-center justify-between rounded-2xl border border-app bg-app px-3 py-3"
                        >
                          <div className="text-sm text-app-1">{doc.label}</div>
                          {doc.present ? (
                            <span className="inline-flex items-center gap-1 text-xs text-emerald-300">
                              <CheckCircle2 className="h-4 w-4" />
                              Present
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-xs text-amber-300">
                              <AlertCircle className="h-4 w-4" />
                              Missing
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  </Surface>

                  <Surface className="p-5">
                    <div className="text-sm font-semibold text-app-0">
                      Parsed field disagreements
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      Values extracted from multiple documents that do not
                      match.
                    </div>

                    {!conflicts.length ? (
                      <div className="mt-4 rounded-2xl border border-app bg-app px-4 py-4 text-sm text-app-4">
                        No parsed conflicts found.
                      </div>
                    ) : (
                      <div className="mt-4 space-y-3">
                        {conflicts.map((conflict, idx) => (
                          <div
                            key={`${conflict.field}-${idx}`}
                            className="rounded-2xl border border-app bg-app px-4 py-4"
                          >
                            <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                              <GitCompareArrows className="h-4 w-4" />
                              {conflict.field.replace(/_/g, " ")}
                            </div>
                            <div className="mt-3 space-y-2">
                              {conflict.values.map((value, valueIdx) => (
                                <div
                                  key={`${value.documentId}-${valueIdx}`}
                                  className="rounded-xl border border-app bg-app-panel px-3 py-2 text-sm text-app-2"
                                >
                                  <div className="font-medium text-app-0">
                                    {String(value.value)}
                                  </div>
                                  <div className="mt-1 text-xs text-app-4">
                                    {value.documentName}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </Surface>
                </div>
              </div>

              <div className="grid gap-4 2xl:grid-cols-[1fr_1fr]">
                <Surface className="p-5">
                  <div className="flex items-center gap-2">
                    <Upload className="h-4 w-4 text-app-4" />
                    <div className="text-sm font-semibold text-app-0">
                      Secure file upload
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    Allowed: PDF, DOCX, TXT, PNG, JPG. Max 15 MB. Macro-enabled
                    or suspicious files are blocked.
                  </div>

                  <div className="mt-5 grid gap-4">
                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Document kind
                      </div>
                      <select
                        value={uploadKind}
                        onChange={(e) =>
                          setUploadKind(e.target.value as DocKind)
                        }
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      >
                        {DOC_KIND_OPTIONS.map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Display name
                      </div>
                      <input
                        value={uploadName}
                        onChange={(e) => setUploadName(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                        placeholder="Signed purchase agreement"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Upload file
                      </div>
                      <input
                        ref={fileInputRef}
                        type="file"
                        accept=".pdf,.docx,.txt,.png,.jpg,.jpeg"
                        onChange={(e) =>
                          setUploadFile(e.target.files?.[0] || null)
                        }
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Notes
                      </div>
                      <textarea
                        value={uploadNotes}
                        onChange={(e) => setUploadNotes(e.target.value)}
                        rows={3}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                        placeholder="Optional upload notes"
                      />
                    </label>

                    {uploadErr ? (
                      <div className="text-xs text-red-300">{uploadErr}</div>
                    ) : null}

                    <button
                      type="button"
                      onClick={handleUploadFile}
                      disabled={uploadingFile}
                      className="oh-btn oh-btn-secondary"
                    >
                      {uploadingFile ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Paperclip className="h-4 w-4" />
                      )}
                      Upload supporting file
                    </button>
                  </div>
                </Surface>

                <Surface className="p-5">
                  <div className="flex items-center gap-2">
                    <FileText className="h-4 w-4 text-app-4" />
                    <div className="text-sm font-semibold text-app-0">
                      Add Exisiting document record
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    Register a document by URL or extracted text when it
                    originated outside the upload flow.
                  </div>

                  <div className="mt-5 grid gap-4">
                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Document kind
                      </div>
                      <select
                        value={docKind}
                        onChange={(e) => setDocKind(e.target.value as DocKind)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      >
                        {DOC_KIND_OPTIONS.map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Name
                      </div>
                      <input
                        value={docName}
                        onChange={(e) => setDocName(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Source URL
                      </div>
                      <input
                        value={docSourceUrl}
                        onChange={(e) => setDocSourceUrl(e.target.value)}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Extracted text
                      </div>
                      <textarea
                        value={docExtractedText}
                        onChange={(e) => setDocExtractedText(e.target.value)}
                        rows={4}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <label className="block">
                      <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
                        Notes
                      </div>
                      <textarea
                        value={docNotes}
                        onChange={(e) => setDocNotes(e.target.value)}
                        rows={3}
                        className="w-full rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-0 outline-none"
                      />
                    </label>

                    <button
                      type="button"
                      onClick={handleImportDoc}
                      disabled={importingDoc}
                      className="oh-btn oh-btn-secondary"
                    >
                      {importingDoc ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Upload className="h-4 w-4" />
                      )}
                      Import document
                    </button>
                  </div>
                </Surface>
              </div>

              <Surface className="p-5">
                <div className="text-sm font-semibold text-app-0">
                  Document stack
                </div>
                <div className="mt-1 text-xs text-app-4">
                  Review current documents, preview them, and download copies.
                </div>

                {!documents.length ? (
                  <div className="mt-4 rounded-2xl border border-app bg-app px-4 py-4 text-sm text-app-4">
                    No documents attached yet.
                  </div>
                ) : (
                  <div className="mt-4 space-y-3">
                    {documents.map((doc: any) => (
                      <div
                        key={doc.id}
                        className="rounded-2xl border border-app bg-app px-4 py-4"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="text-sm font-medium text-app-0">
                              {textValue(doc?.name)}
                            </div>
                            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-app-4">
                              <span>{textValue(doc?.kind)}</span>
                              {doc?.original_filename ? (
                                <span>• {doc.original_filename}</span>
                              ) : null}
                              {doc?.file_size_bytes ? (
                                <span>
                                  •{" "}
                                  {(Number(doc.file_size_bytes) / 1024).toFixed(
                                    1,
                                  )}{" "}
                                  KB
                                </span>
                              ) : null}
                              {doc?.parse_status ? (
                                <span>• parse {doc.parse_status}</span>
                              ) : null}
                              {doc?.scan_status ? (
                                <span>• scan {doc.scan_status}</span>
                              ) : null}
                            </div>
                          </div>

                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() =>
                                handlePreviewDocument(Number(doc.id), doc?.name)
                              }
                              className="oh-btn oh-btn-secondary"
                            >
                              <Eye className="h-4 w-4" />
                              Preview
                            </button>
                            <button
                              type="button"
                              onClick={() =>
                                handleDownloadDocument(
                                  Number(doc.id),
                                  doc?.name,
                                )
                              }
                              className="oh-btn oh-btn-secondary"
                            >
                              <Download className="h-4 w-4" />
                              Download
                            </button>
                          </div>
                        </div>

                        {doc?.preview_text ? (
                          <div className="mt-3 rounded-2xl border border-app bg-app-panel px-3 py-3 text-xs text-app-3">
                            {String(doc.preview_text).slice(0, 280)}
                            {String(doc.preview_text).length > 280 ? "…" : ""}
                          </div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </Surface>

              <Surface className="p-5">
                <div className="text-sm font-semibold text-app-0">
                  Contact panel
                </div>
                <div className="mt-1 text-xs text-app-4">
                  Clean role-based visibility for who is involved.
                </div>

                {contacts.length === 0 ? (
                  <div className="mt-4 rounded-2xl border border-app bg-app px-4 py-4 text-sm text-app-4">
                    No structured contacts saved yet.
                  </div>
                ) : (
                  <div className="mt-4 space-y-3">
                    {contacts.map((c: any, idx: number) => (
                      <div
                        key={idx}
                        className="rounded-2xl border border-app bg-app px-4 py-4"
                      >
                        <div className="flex items-start justify-between gap-2">
                          <div>
                            <div className="text-sm font-medium text-app-0">
                              {textValue(c?.name)}
                            </div>
                            <div className="mt-1 text-xs uppercase tracking-[0.16em] text-app-4">
                              {textValue(c?.role)}
                            </div>
                          </div>
                          <User2 className="h-4 w-4 text-app-4" />
                        </div>
                        <div className="mt-3 space-y-1 text-sm text-app-3">
                          {c?.email ? <div>{c.email}</div> : null}
                          {c?.phone ? (
                            <div className="inline-flex items-center gap-1">
                              <Phone className="h-3.5 w-3.5" />
                              {c.phone}
                            </div>
                          ) : null}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </Surface>
            </>
          )}
        </div>
      </div>
    </PageShell>
  );
}
