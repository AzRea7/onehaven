import React from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ClipboardList,
  DollarSign,
  FileText,
  Phone,
  RefreshCcw,
  ShieldAlert,
} from "lucide-react";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";

export type FieldValueRow = {
  id?: number | string;
  field_name?: string | null;
  label?: string | null;
  value_text?: string | null;
  value_number?: number | null;
  value_date?: string | null;
  confidence?: number | null;
  conflict?: boolean | null;
  review_state?: string | null;
  extraction_version?: string | null;
  manually_overridden?: boolean | null;
  document_id?: number | string | null;
  document_name?: string | null;
  source_document_id?: number | string | null;
  source_document_name?: string | null;
  source_excerpt?: string | null;
};

export type ReviewDocumentActionable = {
  recommended_next_actions?: string[];
  who_to_contact_next?: Array<{
    role?: string | null;
    name?: string | null;
    email?: string | null;
    phone?: string | null;
    company?: string | null;
    excerpt?: string | null;
  }>;
  deadline_candidates?: Array<{
    label?: string | null;
    date?: string | null;
    excerpt?: string | null;
  }>;
  risk_flags?: Array<{
    code?: string | null;
    label?: string | null;
    severity?: string | null;
    excerpt?: string | null;
  }>;
  mismatch_indicators?: Array<{
    field_name?: string | null;
    parsed_value?: any;
    current_value?: any;
    excerpt?: string | null;
  }>;
};

export type ReviewDocumentRow = {
  id?: number | string;
  name?: string | null;
  kind?: string | null;
  parse_status?: string | null;
  scan_status?: string | null;
  preview_text?: string | null;
  actionable_intelligence?: ReviewDocumentActionable | null;
};

export type MissingDocumentGroup = {
  kind: string;
  label: string;
};

type Props = {
  propertyId: number;
  items?: FieldValueRow[];
  documents?: ReviewDocumentRow[];
  missingDocumentGroups?: MissingDocumentGroup[];
  nextRequiredDocument?: string | null;
  estimatedCloseReadiness?: number;
  onAction?: () => void | Promise<void>;
};

const KEY_FIELD_ORDER = [
  "purchase_price",
  "earnest_money",
  "seller_credits",
  "loan_amount",
  "cash_to_close",
  "closing_costs",
  "buyer_name",
  "seller_name",
  "listing_agent_name",
  "title_company",
  "escrow_officer",
  "loan_type",
  "target_close_date",
  "closing_date",
  "inspection_contingency_date",
  "financing_contingency_date",
  "earnest_money_deadline",
];

const FIELD_PRIORITY: Record<string, number> = {
  purchase_price: 1,
  earnest_money: 2,
  seller_credits: 3,
  loan_amount: 4,
  cash_to_close: 5,
  closing_costs: 6,
  buyer_name: 7,
  seller_name: 8,
  listing_agent_name: 9,
  title_company: 10,
  escrow_officer: 11,
  loan_type: 12,
  target_close_date: 13,
  closing_date: 14,
  inspection_contingency_date: 15,
  financing_contingency_date: 16,
  earnest_money_deadline: 17,
};

const MONEY_FIELDS = new Set([
  "purchase_price",
  "earnest_money",
  "seller_credits",
  "loan_amount",
  "cash_to_close",
  "closing_costs",
]);

const DATE_FIELDS = new Set([
  "target_close_date",
  "closing_date",
  "inspection_contingency_date",
  "financing_contingency_date",
  "earnest_money_deadline",
  "title_objection_deadline",
  "insurance_due_date",
  "walkthrough_datetime",
  "closing_datetime",
]);

function safeArray<T = any>(value: any): T[] {
  return Array.isArray(value) ? value : [];
}

function parseTone(value?: string | null) {
  const v = String(value || "")
    .trim()
    .toLowerCase();
  if (v === "done" || v === "parsed" || v === "completed") {
    return "oh-pill oh-pill-good";
  }
  if (v === "failed" || v === "error") return "oh-pill oh-pill-bad";
  if (v === "pending" || v === "queued" || v === "processing") {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
}

function readinessTone(value?: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "text-app-0";
  if (n >= 75) return "text-emerald-300";
  if (n >= 45) return "text-amber-300";
  return "text-red-300";
}

function formatMoney(value: number | null | undefined) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function formatDateValue(value?: string | null) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleDateString();
}

function cleanText(value: string | null | undefined) {
  const text = String(value || "")
    .replace(/\s+/g, " ")
    .replace(/&amp;/g, "&")
    .trim();

  if (!text) return "—";

  return text.length > 140 ? `${text.slice(0, 140).trim()}…` : text;
}

function bestRow(rows: FieldValueRow[]) {
  return [...rows].sort((a, b) => {
    const aOverride = a.manually_overridden ? 1 : 0;
    const bOverride = b.manually_overridden ? 1 : 0;
    if (aOverride !== bOverride) return bOverride - aOverride;

    const aAccepted =
      String(a.review_state || "").toLowerCase() === "accepted" ? 1 : 0;
    const bAccepted =
      String(b.review_state || "").toLowerCase() === "accepted" ? 1 : 0;
    if (aAccepted !== bAccepted) return bAccepted - aAccepted;

    const aConfidence = Number(a.confidence || 0);
    const bConfidence = Number(b.confidence || 0);
    return bConfidence - aConfidence;
  })[0];
}

function displayValue(row: FieldValueRow) {
  const fieldName = String(row.field_name || "")
    .trim()
    .toLowerCase();

  if (row.value_number != null && Number.isFinite(Number(row.value_number))) {
    if (MONEY_FIELDS.has(fieldName))
      return formatMoney(Number(row.value_number));
    return String(row.value_number);
  }

  if (row.value_date) {
    return formatDateValue(row.value_date);
  }

  if (row.value_text != null && row.value_text !== "") {
    if (DATE_FIELDS.has(fieldName)) return formatDateValue(row.value_text);
    return cleanText(String(row.value_text));
  }

  return "—";
}

function rawValueForConflict(row: FieldValueRow) {
  if (row.value_number != null && Number.isFinite(Number(row.value_number))) {
    return String(row.value_number);
  }
  if (row.value_date) return row.value_date.trim().toLowerCase();
  return String(row.value_text || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function fieldSortKey(field: string) {
  return FIELD_PRIORITY[field] ?? 999;
}

function metricTone(fieldName?: string | null) {
  const key = String(fieldName || "").toLowerCase();
  if (MONEY_FIELDS.has(key)) return "text-emerald-200";
  if (DATE_FIELDS.has(key)) return "text-sky-200";
  return "text-app-0";
}

export default function DocumentFieldReviewPanel({
  propertyId,
  items = [],
  documents = [],
  missingDocumentGroups = [],
  nextRequiredDocument,
  estimatedCloseReadiness,
  onAction,
}: Props) {
  const safeItems = Array.isArray(items) ? items : [];
  const safeDocuments = Array.isArray(documents) ? documents : [];
  const safeMissing = Array.isArray(missingDocumentGroups)
    ? missingDocumentGroups
    : [];

  const grouped = safeItems.reduce<Record<string, FieldValueRow[]>>(
    (acc, row) => {
      const key = String(row.field_name || row.label || "unknown");
      if (!acc[key]) acc[key] = [];
      acc[key].push(row);
      return acc;
    },
    {},
  );

  const groupedEntries = Object.entries(grouped).sort(([a], [b]) => {
    const aPriority = fieldSortKey(a);
    const bPriority = fieldSortKey(b);
    if (aPriority !== bPriority) return aPriority - bPriority;
    return a.localeCompare(b);
  });

  const conflictCount = groupedEntries.filter(([, rows]) => {
    const normalized = rows.map((r) => rawValueForConflict(r)).filter(Boolean);
    return new Set(normalized).size > 1;
  }).length;

  const keyFieldRows = KEY_FIELD_ORDER.map((field) => {
    const rows = grouped[field];
    if (!rows?.length) return null;
    return bestRow(rows);
  }).filter(Boolean) as FieldValueRow[];

  const otherFieldEntries = groupedEntries.filter(
    ([field]) => !KEY_FIELD_ORDER.includes(field),
  );

  return (
    <Surface
      title="Document field review"
      subtitle="Operator-grade extracted facts, source excerpts, risks, deadlines, and mismatch warnings."
      actions={
        <div className="flex flex-wrap items-center gap-2">
          <span className="oh-pill">Property #{propertyId}</span>
          <span className="oh-pill oh-pill-accent">
            <ShieldAlert className="h-3.5 w-3.5" />
            {safeMissing.length} missing groups
          </span>
          <span className="oh-pill oh-pill-warn">
            <AlertTriangle className="h-3.5 w-3.5" />
            {conflictCount} conflicts
          </span>
          <span
            className={`text-sm font-semibold ${readinessTone(estimatedCloseReadiness)}`}
          >
            Readiness{" "}
            {Number.isFinite(Number(estimatedCloseReadiness))
              ? `${Math.round(Number(estimatedCloseReadiness))}%`
              : "—"}
          </span>
          {onAction ? (
            <button
              type="button"
              className="oh-btn oh-btn-secondary oh-btn-sm"
              onClick={() => void onAction()}
            >
              <RefreshCcw className="h-4 w-4" />
              Refresh
            </button>
          ) : null}
        </div>
      }
    >
      <div className="space-y-6">
        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            <ClipboardList className="h-4 w-4" />
            Next required document
          </div>
          <div className="mt-2 text-sm text-app-3">
            {nextRequiredDocument || "No missing required documents"}
          </div>
        </div>

        {safeMissing.length ? (
          <div>
            <div className="mb-3 text-sm font-semibold text-app-1">
              Missing document groups
            </div>
            <div className="flex flex-wrap gap-2">
              {safeMissing.map((row, idx) => (
                <span
                  key={`${row.kind}-${idx}`}
                  className="oh-pill oh-pill-warn"
                >
                  {row.label || row.kind}
                </span>
              ))}
            </div>
          </div>
        ) : null}

        <div>
          <div className="mb-3 text-sm font-semibold text-app-1">
            Key deal terms
          </div>
          {!keyFieldRows.length ? (
            <EmptyState
              icon={DollarSign}
              title="No key terms extracted yet"
              description="Once structured values are parsed, the main deal terms will show here first."
            />
          ) : (
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {keyFieldRows.map((row) => (
                <div
                  key={String(row.id ?? row.field_name)}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                >
                  <div className="text-[11px] uppercase tracking-[0.16em] text-app-4">
                    {row.label || row.field_name || "Field"}
                  </div>
                  <div
                    className={`mt-2 text-base font-semibold ${metricTone(
                      row.field_name,
                    )}`}
                  >
                    {displayValue(row)}
                  </div>
                  <div className="mt-2 text-xs text-app-4">
                    {row.source_document_name ||
                      row.document_name ||
                      (row.source_document_id != null
                        ? `Document #${row.source_document_id}`
                        : "Unknown source")}
                  </div>
                  {row.source_excerpt ? (
                    <div className="mt-2 rounded-lg border border-app/60 bg-app px-2 py-2 text-xs text-app-3">
                      {cleanText(row.source_excerpt)}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          )}
        </div>

        <div>
          <div className="mb-3 text-sm font-semibold text-app-1">Documents</div>
          {!safeDocuments.length ? (
            <EmptyState
              icon={FileText}
              title="No documents uploaded"
              description="Upload acquisition documents to start parsing fields and conflict review."
            />
          ) : (
            <div className="space-y-4">
              {safeDocuments.map((doc, idx) => {
                const actionable = doc.actionable_intelligence || {};
                const contacts = safeArray(
                  actionable.who_to_contact_next,
                ).filter((row) => row && (row.name || row.role));
                const deadlines = safeArray(
                  actionable.deadline_candidates,
                ).filter((row) => row && (row.date || row.label));
                const riskFlags = safeArray(actionable.risk_flags).filter(
                  Boolean,
                );
                const mismatches = safeArray(
                  actionable.mismatch_indicators,
                ).filter(Boolean);
                const actions = safeArray(
                  actionable.recommended_next_actions,
                ).filter(Boolean);

                return (
                  <div
                    key={String(doc.id ?? idx)}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-app-0">
                          {doc.name || "Document"}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {doc.kind || "unknown kind"}
                        </div>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <span className={parseTone(doc.parse_status)}>
                          parse: {doc.parse_status || "unknown"}
                        </span>
                        <span className={parseTone(doc.scan_status)}>
                          scan: {doc.scan_status || "unknown"}
                        </span>
                      </div>
                    </div>

                    {doc.preview_text ? (
                      <div className="mt-3 line-clamp-4 text-sm text-app-3">
                        {doc.preview_text}
                      </div>
                    ) : null}

                    {actions.length ||
                    contacts.length ||
                    deadlines.length ||
                    riskFlags.length ||
                    mismatches.length ? (
                      <div className="mt-4 grid gap-4 xl:grid-cols-2">
                        {actions.length ? (
                          <div className="rounded-2xl border border-app bg-app px-3 py-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
                              Next actions
                            </div>
                            <div className="mt-2 space-y-2 text-sm text-app-1">
                              {actions.slice(0, 5).map((item, actionIdx) => (
                                <div key={`${doc.id}-action-${actionIdx}`}>
                                  • {item}
                                </div>
                              ))}
                            </div>
                          </div>
                        ) : null}

                        {contacts.length ? (
                          <div className="rounded-2xl border border-app bg-app px-3 py-3">
                            <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
                              <Phone className="h-3.5 w-3.5" />
                              Who to contact next
                            </div>
                            <div className="mt-2 space-y-2 text-sm text-app-1">
                              {contacts
                                .slice(0, 4)
                                .map((row: any, contactIdx: number) => (
                                  <div
                                    key={`${doc.id}-contact-${contactIdx}`}
                                    className="rounded-xl border border-app/70 bg-app-panel px-3 py-2"
                                  >
                                    <div className="font-medium text-app-0">
                                      {row.name || row.role || "Contact"}
                                    </div>
                                    <div className="mt-1 text-xs text-app-4">
                                      {[
                                        row.role,
                                        row.company,
                                        row.phone,
                                        row.email,
                                      ]
                                        .filter(Boolean)
                                        .join(" • ") ||
                                        "Document suggests follow-up with this party."}
                                    </div>
                                    {row.excerpt ? (
                                      <div className="mt-2 text-xs text-app-3">
                                        {cleanText(row.excerpt)}
                                      </div>
                                    ) : null}
                                  </div>
                                ))}
                            </div>
                          </div>
                        ) : null}

                        {deadlines.length ? (
                          <div className="rounded-2xl border border-app bg-app px-3 py-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-app-4">
                              Deadline candidates
                            </div>
                            <div className="mt-2 space-y-2 text-sm text-app-1">
                              {deadlines
                                .slice(0, 4)
                                .map((row: any, deadlineIdx: number) => (
                                  <div
                                    key={`${doc.id}-deadline-${deadlineIdx}`}
                                    className="rounded-xl border border-app/70 bg-app-panel px-3 py-2"
                                  >
                                    <div className="font-medium text-app-0">
                                      {row.label || "Deadline"}
                                    </div>
                                    <div className="mt-1 text-xs text-app-4">
                                      {row.date
                                        ? formatDateValue(row.date)
                                        : "No date extracted"}
                                    </div>
                                    {row.excerpt ? (
                                      <div className="mt-2 text-xs text-app-3">
                                        {cleanText(row.excerpt)}
                                      </div>
                                    ) : null}
                                  </div>
                                ))}
                            </div>
                          </div>
                        ) : null}

                        {riskFlags.length ? (
                          <div className="rounded-2xl border border-red-500/30 bg-red-500/10 px-3 py-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-red-200">
                              Risk / warning flags
                            </div>
                            <div className="mt-2 space-y-2 text-sm text-red-100">
                              {riskFlags
                                .slice(0, 4)
                                .map((flag: any, flagIdx: number) => (
                                  <div key={`${doc.id}-risk-${flagIdx}`}>
                                    <div className="font-medium">
                                      {flag.label || flag.code || "Risk"}
                                    </div>
                                    {flag.excerpt ? (
                                      <div className="mt-1 text-xs text-red-100/80">
                                        {cleanText(flag.excerpt)}
                                      </div>
                                    ) : null}
                                  </div>
                                ))}
                            </div>
                          </div>
                        ) : null}

                        {mismatches.length ? (
                          <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-3 py-3 xl:col-span-2">
                            <div className="text-xs font-semibold uppercase tracking-[0.14em] text-amber-100">
                              Mismatch warnings vs current record
                            </div>
                            <div className="mt-2 grid gap-2 md:grid-cols-2">
                              {mismatches
                                .slice(0, 6)
                                .map((row: any, mismatchIdx: number) => (
                                  <div
                                    key={`${doc.id}-mismatch-${mismatchIdx}`}
                                    className="rounded-xl border border-amber-500/20 bg-amber-500/5 px-3 py-2 text-sm text-amber-50"
                                  >
                                    <div className="font-medium">
                                      {String(
                                        row.field_name || "field",
                                      ).replace(/_/g, " ")}
                                    </div>
                                    <div className="mt-1 text-xs">
                                      Doc: {String(row.parsed_value ?? "—")} •
                                      Current:{" "}
                                      {String(row.current_value ?? "—")}
                                    </div>
                                    {row.excerpt ? (
                                      <div className="mt-2 text-xs text-amber-100/80">
                                        {cleanText(row.excerpt)}
                                      </div>
                                    ) : null}
                                  </div>
                                ))}
                            </div>
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <div>
          <div className="mb-3 text-sm font-semibold text-app-1">
            Parsed field values
          </div>
          {!groupedEntries.length ? (
            <EmptyState
              icon={CheckCircle2}
              title="No parsed fields yet"
              description="Once documents are parsed, extracted values will show here by field."
            />
          ) : (
            <div className="space-y-4">
              {otherFieldEntries.map(([field, rows]) => {
                const normalized = rows
                  .map((r) => rawValueForConflict(r))
                  .filter(Boolean);
                const hasConflict = new Set(normalized).size > 1;

                return (
                  <div
                    key={field}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="text-sm font-semibold text-app-0">
                        {rows[0]?.label || field}
                      </div>
                      {hasConflict ? (
                        <span className="oh-pill oh-pill-bad">
                          <AlertTriangle className="h-3.5 w-3.5" />
                          conflict
                        </span>
                      ) : (
                        <span className="oh-pill oh-pill-good">aligned</span>
                      )}
                    </div>

                    <div className="mt-3 space-y-2">
                      {rows.map((row, idx) => (
                        <div
                          key={String(row.id ?? `${field}-${idx}`)}
                          className="rounded-xl border border-app/70 bg-app px-3 py-3 text-sm"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="font-medium text-app-0">
                              {displayValue(row)}
                            </div>
                            {row.confidence != null ? (
                              <div className="text-xs text-app-4">
                                confidence{" "}
                                {Math.round(Number(row.confidence) * 100)}%
                              </div>
                            ) : null}
                          </div>

                          <div className="mt-1 text-xs text-app-4">
                            {row.source_document_name ||
                              row.document_name ||
                              (row.source_document_id != null
                                ? `Document #${row.source_document_id}`
                                : row.document_id != null
                                  ? `Document #${row.document_id}`
                                  : "Unknown source")}
                          </div>

                          {row.source_excerpt ? (
                            <div className="mt-2 rounded-lg border border-app/60 bg-app-panel px-2 py-2 text-xs text-app-3">
                              {cleanText(row.source_excerpt)}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </Surface>
  );
}
