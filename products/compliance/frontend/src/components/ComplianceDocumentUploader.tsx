import React from "react";
import { FileUp, Loader2 } from "lucide-react";
import AppSelect from "@/components/AppSelect";
import Surface from "@/components/Surface";
import { api } from "@/lib/api";

type Props = {
  propertyId: number;
  inspectionId?: number | null;
  checklistItemId?: number | null;
  onUploaded?: () => void | Promise<void>;
};

const CATEGORY_OPTIONS = [
  { value: "inspection_report", label: "Inspection report" },
  { value: "pass_certificate", label: "Pass certificate" },
  { value: "reinspection_notice", label: "Reinspection notice" },
  { value: "repair_invoice", label: "Repair invoice" },
  { value: "utility_confirmation", label: "Utility confirmation" },
  { value: "smoke_detector_proof", label: "Smoke detector proof" },
  { value: "lead_based_paint_paperwork", label: "Lead-based paint paperwork" },
  { value: "local_jurisdiction_document", label: "Local jurisdiction document" },
  { value: "approval_letter", label: "Approval letter" },
  { value: "denial_letter", label: "Denial letter" },
  { value: "other_evidence", label: "Other evidence" },
];

export default function ComplianceDocumentUploader({
  propertyId,
  inspectionId,
  checklistItemId,
  onUploaded,
}: Props) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [category, setCategory] = React.useState("inspection_report");
  const [notes, setNotes] = React.useState("");
  const [parseDocument, setParseDocument] = React.useState(true);

  async function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;

    setBusy(true);
    setError(null);

    try {
      const form = new FormData();
      form.append("category", category);
      form.append("label", file.name);
      form.append("notes", notes);
      form.append("parse_document", String(parseDocument));
      form.append("file", file);
      if (inspectionId != null) form.append("inspection_id", String(inspectionId));
      if (checklistItemId != null) {
        form.append("checklist_item_id", String(checklistItemId));
      }

      await api.post(`/compliance/properties/${propertyId}/documents/upload`, form);
      await onUploaded?.();
      e.target.value = "";
    } catch (err: any) {
      setError(err?.message || "Document upload failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Surface
      title="Compliance documents"
      subtitle="Upload inspection forms, approvals, receipts, lead paint docs, utility certifications, and other evidence."
    >
      <div className="grid gap-3">
        <div>
          <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
            Document category
          </div>
          <AppSelect
            value={category}
            options={CATEGORY_OPTIONS}
            onChange={setCategory}
          />
        </div>

        <label className="grid gap-2">
          <span className="text-xs uppercase tracking-[0.16em] text-app-4">
            Notes
          </span>
          <textarea
            rows={3}
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Optional context for this document or evidence"
            className="w-full rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-1 outline-none"
          />
        </label>

        <label className="inline-flex items-center gap-2 text-sm text-app-2">
          <input
            type="checkbox"
            checked={parseDocument}
            onChange={(e) => setParseDocument(e.target.checked)}
            className="h-4 w-4 rounded border-app bg-app-panel"
          />
          Run text extraction / parsing when useful
        </label>

        <label className="mt-1 inline-flex cursor-pointer items-center gap-2 rounded-xl border border-app bg-app-panel px-4 py-3 text-sm text-app-1 hover:bg-app-muted">
          <input
            type="file"
            accept=".pdf,.png,.jpg,.jpeg,.doc,.docx,.txt,.csv,.json"
            className="hidden"
            onChange={handleChange}
            disabled={busy}
          />
          {busy ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <FileUp className="h-4 w-4" />
          )}
          {busy ? "Uploading..." : "Upload compliance document"}
        </label>

        <div className="flex flex-wrap gap-2 text-xs text-app-4">
          <span className="oh-pill">Property #{propertyId}</span>
          {inspectionId != null ? (
            <span className="oh-pill">Inspection #{inspectionId}</span>
          ) : null}
          {checklistItemId != null ? (
            <span className="oh-pill">Checklist item #{checklistItemId}</span>
          ) : null}
        </div>

        {error ? <div className="text-xs text-red-300">{error}</div> : null}
      </div>
    </Surface>
  );
}
