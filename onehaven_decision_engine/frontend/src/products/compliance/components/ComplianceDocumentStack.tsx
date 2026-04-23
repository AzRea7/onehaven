import React from "react";
import { Download, FileText, Trash2 } from "lucide-react";
import Surface from "@/components/Surface";
import EmptyState from "@/components/EmptyState";
import { api } from "@/lib/api";

type DocumentRow = {
  id: number;
  category?: string | null;
  label?: string | null;
  notes?: string | null;
  original_filename?: string | null;
  inspection_id?: number | null;
  checklist_item_id?: number | null;
  parse_status?: string | null;
  scan_status?: string | null;
  file_size_bytes?: number | null;
  size_bytes?: number | null;
  extracted_text_preview?: string | null;
};

function labelize(value?: string | null) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function toneFor(value?: string | null) {
  const v = String(value || "").toLowerCase();
  if (["clean", "parsed", "complete"].includes(v))
    return "oh-pill oh-pill-good";
  if (["queued", "skipped", "pending"].includes(v))
    return "oh-pill oh-pill-warn";
  if (["infected", "error", "failed"].includes(v)) return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function formatBytes(value?: number | null) {
  const n = Number(value || 0);
  if (!n) return "—";
  if (n >= 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${n} B`;
}

export default function ComplianceDocumentStack({
  propertyId,
  documents,
  onChanged,
}: {
  propertyId?: number;
  documents?: DocumentRow[] | null;
  onChanged?: () => void | Promise<void>;
}) {
  const rows = Array.isArray(documents) ? documents : [];

  async function handleDelete(id: number) {
    if (!propertyId) return;
    await api.delete(`/compliance/properties/${propertyId}/documents/${id}`);
    await onChanged?.();
  }

  async function handleDownload(id: number) {
    if (!propertyId) return;
    window.open(
      `/api/compliance/properties/${propertyId}/documents/${id}`,
      "_blank",
    );
  }

  if (!rows.length) {
    return (
      <Surface
        title="Compliance document stack"
        subtitle="Evidence and compliance-specific files tied to the property, inspection, and checklist state."
      >
        <EmptyState compact title="No compliance documents uploaded yet." />
      </Surface>
    );
  }

  return (
    <Surface
      title="Compliance document stack"
      subtitle="Evidence and compliance-specific files tied to the property, inspection, and checklist state."
      actions={
        <div className="text-xs text-app-4">{rows.length} documents</div>
      }
    >
      <div className="grid gap-3">
        {rows.map((row) => (
          <div
            key={row.id}
            className="rounded-2xl border border-app bg-app-muted px-4 py-4"
          >
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <FileText className="h-4 w-4 text-app-4" />
                  <div className="text-sm font-semibold text-app-0">
                    {row.label ||
                      row.original_filename ||
                      `Document #${row.id}`}
                  </div>
                  <span className="oh-pill">
                    {labelize(row.category || "other_evidence")}
                  </span>
                  {row.parse_status ? (
                    <span className={toneFor(row.parse_status)}>
                      Parse: {labelize(row.parse_status)}
                    </span>
                  ) : null}
                  {row.scan_status ? (
                    <span className={toneFor(row.scan_status)}>
                      Scan: {labelize(row.scan_status)}
                    </span>
                  ) : null}
                </div>

                <div className="mt-2 flex flex-wrap gap-2 text-xs text-app-4">
                  <span>{row.original_filename || "Unnamed file"}</span>
                  <span>
                    {formatBytes(row.file_size_bytes || row.size_bytes)}
                  </span>
                  {row.inspection_id != null ? (
                    <span className="oh-pill">
                      Inspection #{row.inspection_id}
                    </span>
                  ) : null}
                  {row.checklist_item_id != null ? (
                    <span className="oh-pill">
                      Checklist #{row.checklist_item_id}
                    </span>
                  ) : null}
                </div>

                {row.notes ? (
                  <div className="mt-2 text-sm leading-6 text-app-3">
                    {row.notes}
                  </div>
                ) : null}

                {row.extracted_text_preview ? (
                  <div className="mt-3 rounded-2xl border border-app bg-app-panel px-3 py-3 text-xs leading-6 text-app-3">
                    {row.extracted_text_preview}
                  </div>
                ) : null}
              </div>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => void handleDownload(row.id)}
                  className="oh-btn oh-btn-secondary"
                  disabled={!propertyId}
                >
                  <Download className="h-4 w-4" />
                  Open
                </button>
                <button
                  type="button"
                  onClick={() => void handleDelete(row.id)}
                  className="oh-btn oh-btn-secondary text-red-300"
                  disabled={!propertyId}
                >
                  <Trash2 className="h-4 w-4" />
                  Delete
                </button>
              </div>
            </div>
          </div>
        ))}
      </div>
    </Surface>
  );
}
