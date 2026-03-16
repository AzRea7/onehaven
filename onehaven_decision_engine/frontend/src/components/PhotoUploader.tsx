import React from "react";
import { Upload, Loader2 } from "lucide-react";
import { api } from "../lib/api";
import Surface from "./Surface";

type Props = {
  propertyId: number;
  onUploaded?: () => void | Promise<void>;
};

export default function PhotoUploader({ propertyId, onUploaded }: Props) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  async function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;

    setBusy(true);
    setError(null);

    try {
      await api.uploadPhoto({
        propertyId,
        file,
        kind: "unknown",
        label: file.name,
      });
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
      subtitle="Zillow photos are auto-attached during import. This uploader is for manual add-ons."
    >
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
        ) : (
          <Upload className="h-4 w-4" />
        )}
        {busy ? "Uploading..." : "Upload image"}
      </label>

      {error ? <div className="mt-3 text-xs text-red-300">{error}</div> : null}
    </Surface>
  );
}
