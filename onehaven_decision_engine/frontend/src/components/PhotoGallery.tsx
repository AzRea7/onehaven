// frontend/src/components/PhotoGallery.tsx
import React from "react";
import type { PropertyPhoto } from "../lib/api";

type Props = {
  photos: PropertyPhoto[];
  onDelete?: (photoId: number) => Promise<void>;
};

function kindTone(kind: string) {
  const k = (kind || "").toLowerCase();
  if (k === "interior")
    return "border-cyan-400/20 bg-cyan-400/10 text-cyan-200";
  if (k === "exterior")
    return "border-amber-400/20 bg-amber-400/10 text-amber-200";
  return "border-white/10 bg-white/5 text-white/70";
}

export default function PhotoGallery({ photos, onDelete }: Props) {
  const [selected, setSelected] = React.useState<PropertyPhoto | null>(
    photos[0] || null,
  );

  React.useEffect(() => {
    if (!selected && photos.length) setSelected(photos[0]);
    if (selected && !photos.find((p) => p.id === selected.id)) {
      setSelected(photos[0] || null);
    }
  }, [photos, selected]);

  if (!photos.length) {
    return (
      <div className="oh-panel p-5">
        <div className="text-sm font-semibold text-white">Photo gallery</div>
        <div className="mt-3 text-sm text-white/55">
          No photos attached yet.
        </div>
      </div>
    );
  }

  return (
    <div className="oh-panel p-5">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">Photo gallery</div>
          <div className="text-xs text-white/55 mt-1">
            Zillow import + manual uploads both land here.
          </div>
        </div>
        <div className="text-xs text-white/50">{photos.length} photos</div>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-[1.3fr_0.7fr]">
        <div className="rounded-2xl border border-white/10 bg-black/20 overflow-hidden min-h-[320px]">
          {selected ? (
            <img
              src={selected.url}
              alt={selected.label || `Photo ${selected.id}`}
              className="h-full w-full object-cover"
            />
          ) : (
            <div className="p-8 text-white/50">Select a photo</div>
          )}
        </div>

        <div className="grid max-h-[420px] grid-cols-2 gap-3 overflow-auto pr-1">
          {photos.map((photo) => (
            <button
              key={photo.id}
              onClick={() => setSelected(photo)}
              className={`overflow-hidden rounded-2xl border text-left transition ${
                selected?.id === photo.id
                  ? "border-white/40 ring-1 ring-white/30"
                  : "border-white/10 hover:border-white/20"
              }`}
            >
              <div className="aspect-[4/3] bg-black/20">
                <img
                  src={photo.url}
                  alt={photo.label || `Photo ${photo.id}`}
                  className="h-full w-full object-cover"
                />
              </div>

              <div className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <span
                    className={`rounded-full border px-2 py-1 text-[10px] ${kindTone(photo.kind)}`}
                  >
                    {photo.kind || "unknown"}
                  </span>
                  <span className="text-[10px] text-white/45">
                    {photo.source}
                  </span>
                </div>

                <div className="mt-2 line-clamp-2 text-xs text-white/75">
                  {photo.label || photo.url}
                </div>

                {onDelete && photo.source === "upload" ? (
                  <button
                    className="mt-3 text-[11px] text-red-300 hover:text-red-200"
                    onClick={async (e) => {
                      e.stopPropagation();
                      await onDelete(photo.id);
                    }}
                  >
                    Delete
                  </button>
                ) : null}
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
