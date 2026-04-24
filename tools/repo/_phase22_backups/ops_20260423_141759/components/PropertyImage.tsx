import React from "react";
import { ExternalLink, ImageIcon } from "lucide-react";

type Props = {
  photos?: string[];
  zillowUrl?: string | null;
  className?: string;
  roundedClassName?: string;
};

export default function PropertyImage({
  photos = [],
  zillowUrl,
  className = "",
  roundedClassName = "rounded-3xl",
}: Props) {
  const [idx, setIdx] = React.useState(0);
  const usable = Array.isArray(photos) ? photos.filter(Boolean) : [];
  const active = usable[idx] || null;

  React.useEffect(() => {
    if (idx > usable.length - 1) setIdx(0);
  }, [idx, usable.length]);

  if (!usable.length) {
    return (
      <div
        className={[
          "relative overflow-hidden border border-app bg-app-muted min-h-[260px] flex items-center justify-center",
          roundedClassName,
          className,
        ].join(" ")}
      >
        <div className="text-center">
          <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full border border-app bg-app-panel text-app-4">
            <ImageIcon className="h-6 w-6" />
          </div>
          <div className="mt-3 text-sm font-semibold text-app-0">
            No property image
          </div>
          <div className="mt-1 text-xs text-app-4">
            Upload photos or ingest them later.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className={[
        "relative overflow-hidden border border-app bg-app-muted",
        roundedClassName,
        className,
      ].join(" ")}
    >
      <div className="aspect-[16/10] w-full bg-app-muted">
        <img
          src={active || ""}
          alt="Property"
          className="h-full w-full object-cover"
        />
      </div>

      <div className="absolute inset-x-0 bottom-0 flex items-center justify-between gap-3 bg-gradient-to-t from-black/65 via-black/15 to-transparent p-4">
        <div className="flex items-center gap-2">
          {usable.slice(0, 5).map((src, i) => (
            <button
              key={`${src}-${i}`}
              type="button"
              onClick={() => setIdx(i)}
              className={[
                "h-10 w-14 overflow-hidden rounded-xl border transition",
                i === idx
                  ? "border-white/60 ring-1 ring-white/40"
                  : "border-white/20 opacity-90 hover:opacity-100",
              ].join(" ")}
            >
              <img
                src={src}
                alt={`Property thumb ${i + 1}`}
                className="h-full w-full object-cover"
              />
            </button>
          ))}
        </div>

        {zillowUrl ? (
          <a
            href={zillowUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-2 rounded-full border border-white/20 bg-black/30 px-3 py-2 text-xs font-semibold text-white hover:bg-black/45"
          >
            Zillow <ExternalLink className="h-3.5 w-3.5" />
          </a>
        ) : null}
      </div>
    </div>
  );
}
