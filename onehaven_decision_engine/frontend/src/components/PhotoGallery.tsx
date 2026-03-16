import React from "react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

type Photo = {
  id: number;
  url: string;
  label?: string | null;
  kind?: string | null;
  source?: string | null;
};

function kindTone(kind?: string | null) {
  const k = String(kind || "").toLowerCase();
  if (k === "interior") return "oh-pill oh-pill-good";
  if (k === "exterior") return "oh-pill oh-pill-accent";
  if (k === "damage" || k === "issue") return "oh-pill oh-pill-bad";
  return "oh-pill";
}

export default function PhotoGallery({
  photos,
  onDelete,
}: {
  photos?: Photo[];
  onDelete?: (photoId: number) => Promise<void> | void;
}) {
  const rows = Array.isArray(photos) ? photos : [];
  const [selected, setSelected] = React.useState<Photo | null>(rows[0] || null);

  React.useEffect(() => {
    if (!selected && rows.length) setSelected(rows[0]);
    if (selected && !rows.find((p) => p.id === selected.id)) {
      setSelected(rows[0] || null);
    }
  }, [rows, selected]);

  if (!rows.length) {
    return (
      <Surface
        title="Photo gallery"
        subtitle="Zillow import + manual uploads both land here."
      >
        <EmptyState compact title="No photos attached yet." />
      </Surface>
    );
  }

  return (
    <Surface
      title="Photo gallery"
      subtitle="Zillow import + manual uploads both land here."
      actions={<div className="text-xs text-app-4">{rows.length} photos</div>}
    >
      <div className="grid gap-4 lg:grid-cols-[1.3fr_0.7fr]">
        <div className="rounded-2xl border border-app bg-app-muted overflow-hidden min-h-[320px]">
          {selected ? (
            <img
              src={selected.url}
              alt={selected.label || `Photo ${selected.id}`}
              className="h-full w-full object-cover"
            />
          ) : (
            <div className="p-8 text-app-4">Select a photo</div>
          )}
        </div>

        <div className="grid max-h-[420px] grid-cols-2 gap-3 overflow-auto pr-1">
          {rows.map((photo) => (
            <button
              key={photo.id}
              onClick={() => setSelected(photo)}
              className={`overflow-hidden rounded-2xl border text-left transition ${
                selected?.id === photo.id
                  ? "border-app-strong ring-1 ring-white/30"
                  : "border-app hover:border-app-strong"
              }`}
            >
              <div className="aspect-[4/3] bg-app-muted">
                <img
                  src={photo.url}
                  alt={photo.label || `Photo ${photo.id}`}
                  className="h-full w-full object-cover"
                />
              </div>

              <div className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <span className={kindTone(photo.kind)}>
                    {photo.kind || "unknown"}
                  </span>
                  <span className="text-[10px] text-app-4">{photo.source}</span>
                </div>

                <div className="mt-2 line-clamp-2 text-xs text-app-2">
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
    </Surface>
  );
}
