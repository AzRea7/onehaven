// frontend/src/components/PhotoUploader.tsx
import React from "react";
import { api } from "../lib/api";

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
    <div className="oh-panel p-4">
      <div className="text-sm font-semibold text-white">Attach photos</div>
      <div className="text-xs text-white/60 mt-1">
        Zillow photos are auto-attached during import. This uploader is for
        manual add-ons.
      </div>

      <label className="mt-4 inline-flex cursor-pointer items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-white hover:bg-white/10">
        <input
          type="file"
          accept="image/*"
          className="hidden"
          onChange={handleChange}
          disabled={busy}
        />
        {busy ? "Uploading..." : "Upload image"}
      </label>

      {error ? <div className="mt-3 text-xs text-red-300">{error}</div> : null}
    </div>
  );
}
