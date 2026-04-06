import React from "react";
import { Upload, Loader2, Camera, FilePlus2 } from "lucide-react";
import { api } from "../lib/api";
import Surface from "./Surface";
import AppSelect from "./AppSelect";

type Props = {
  propertyId: number;
  inspectionId?: number | null;
  checklistItemId?: number | null;
  attachToComplianceByDefault?: boolean;
  onUploaded?: () => void | Promise<void>;
};

const KIND_OPTIONS = [
  { value: "unknown", label: "Unknown" },
  { value: "interior", label: "Interior" },
  { value: "exterior", label: "Exterior" },
  { value: "damage", label: "Damage" },
  { value: "issue", label: "Issue" },
  { value: "smoke_detector", label: "Smoke detector" },
  { value: "utility", label: "Utility proof" },
];

const EVIDENCE_OPTIONS = [
  { value: "photo_evidence", label: "Photo evidence" },
  { value: "smoke_detector_proof", label: "Smoke detector proof" },
  { value: "utility_confirmation", label: "Utility confirmation" },
  { value: "other_evidence", label: "Other evidence" },
];

export default function PhotoUploader({
  propertyId,
  inspectionId,
  checklistItemId,
  attachToComplianceByDefault = true,
  onUploaded,
}: Props) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [kind, setKind] = React.useState("unknown");
  const [attachToCompliance, setAttachToCompliance] = React.useState(
    attachToComplianceByDefault,
  );
  const [evidenceCategory, setEvidenceCategory] =
    React.useState("photo_evidence");

  async function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;

    setBusy(true);
    setError(null);

    try {
      const form = new FormData();
      form.append("property_id", String(propertyId));
      form.append("kind", kind);
      form.append("label", file.name);
      form.append("attach_to_compliance", String(attachToCompliance));
      form.append("evidence_category", evidenceCategory);
      if (inspectionId != null)
        form.append("inspection_id", String(inspectionId));
      if (checklistItemId != null) {
        form.append("checklist_item_id", String(checklistItemId));
      }
      form.append("file", file);

      await api.post("/photos/upload", form);
      await onUploaded?.();
      e.target.value = "";
    } catch (err: any) {
      setError(err?.message || "Upload failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Surface
      title="Attach photos"
      subtitle="Zillow photos are auto-attached during import. This uploader is for manual add-ons and compliance evidence."
    >
      <div className="grid gap-3">
        <div className="grid gap-3 md:grid-cols-2">
          <div>
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
              Photo kind
            </div>
            <AppSelect value={kind} options={KIND_OPTIONS} onChange={setKind} />
          </div>

          <div>
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-app-4">
              Evidence category
            </div>
            <AppSelect
              value={evidenceCategory}
              options={EVIDENCE_OPTIONS}
              onChange={setEvidenceCategory}
            />
          </div>
        </div>

        <label className="inline-flex items-center gap-2 text-sm text-app-2">
          <input
            type="checkbox"
            checked={attachToCompliance}
            onChange={(e) => setAttachToCompliance(e.target.checked)}
            className="h-4 w-4 rounded border-app bg-app-panel"
          />
          Mirror this upload into compliance evidence documents
        </label>

        <label className="mt-1 inline-flex cursor-pointer items-center gap-2 rounded-xl border border-app bg-app-panel px-4 py-3 text-sm text-app-1 hover:bg-app-muted">
          <input
            type="file"
            accept="image/*"
            className="hidden"
            onChange={handleChange}
            disabled={busy}
          />
          {busy ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : attachToCompliance ? (
            <FilePlus2 className="h-4 w-4" />
          ) : (
            <Upload className="h-4 w-4" />
          )}
          {busy
            ? "Uploading..."
            : attachToCompliance
              ? "Upload image + attach as evidence"
              : "Upload image"}
        </label>

        <div className="flex flex-wrap gap-2 text-xs text-app-4">
          <span className="inline-flex items-center gap-1">
            <Camera className="h-3.5 w-3.5" />
            Property photos
          </span>
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
