import React from "react";
import {
  AlertTriangle,
  CheckCircle2,
  ClipboardList,
  FileText,
  RefreshCcw,
  ShieldAlert,
} from "lucide-react";
import EmptyState from "./EmptyState";
import Surface from "./Surface";

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
};

export type ReviewDocumentRow = {
  id?: number | string;
  name?: string | null;
  kind?: string | null;
  parse_status?: string | null;
  scan_status?: string | null;
  preview_text?: string | null;
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

function displayValue(row: FieldValueRow) {
  if (row.value_text != null && row.value_text !== "")
    return String(row.value_text);
  if (row.value_number != null && Number.isFinite(Number(row.value_number))) {
    return String(row.value_number);
  }
  if (row.value_date) return row.value_date;
  return "—";
}

function readinessTone(value?: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "text-app-0";
  if (n >= 75) return "text-emerald-300";
  if (n >= 45) return "text-amber-300";
  return "text-red-300";
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

  const groupedEntries = Object.entries(grouped).sort(([a], [b]) =>
    a.localeCompare(b),
  );

  const conflictCount = groupedEntries.filter(([, rows]) => {
    const normalized = rows
      .map((r) => displayValue(r).trim().toLowerCase())
      .filter(Boolean);
    return new Set(normalized).size > 1;
  }).length;

  return (
    <Surface
      title="Document field review"
      subtitle="Parsed values, missing document groups, and close-readiness signals."
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
          <div className="mb-3 text-sm font-semibold text-app-1">Documents</div>

          {!safeDocuments.length ? (
            <EmptyState
              icon={FileText}
              title="No documents uploaded"
              description="Upload acquisition documents to start parsing fields and conflict review."
            />
          ) : (
            <div className="space-y-3">
              {safeDocuments.map((doc, idx) => (
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
                </div>
              ))}
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
              {groupedEntries.map(([field, rows]) => {
                const normalized = rows
                  .map((r) => displayValue(r).trim().toLowerCase())
                  .filter(Boolean);
                const hasConflict = new Set(normalized).size > 1;

                return (
                  <div
                    key={field}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div className="text-sm font-semibold text-app-0">
                        {field}
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
