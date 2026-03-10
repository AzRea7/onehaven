import React from "react";

type Props = {
  photos?: string[] | null;
  zillowUrl?: string | null;
  className?: string;
  roundedClassName?: string;
};

export default function PropertyImage({
  photos,
  zillowUrl,
  className,
  roundedClassName = "rounded-2xl",
}: Props) {
  const list = Array.isArray(photos) ? photos.filter(Boolean) : [];
  const [active, setActive] = React.useState(0);

  React.useEffect(() => {
    setActive(0);
  }, [list.length]);

  const main = list[active] || list[0] || null;

  if (!main) {
    return (
      <div className={className}>
        <div
          className={[
            "w-full h-full min-h-[220px] border border-white/10 bg-white/[0.03]",
            "flex flex-col items-center justify-center text-center px-4",
            roundedClassName,
          ].join(" ")}
        >
          <div className="text-sm font-semibold text-white">
            No Zillow photos found
          </div>
          <div className="mt-2 text-xs text-white/55 max-w-[280px]">
            This property does not currently have imported Zillow image URLs in
            the latest Zillow source row.
          </div>

          {zillowUrl ? (
            <a
              href={zillowUrl}
              target="_blank"
              rel="noreferrer"
              className="mt-4 oh-btn cursor-pointer"
            >
              open Zillow ↗
            </a>
          ) : null}
        </div>
      </div>
    );
  }

  return (
    <div className={className}>
      <div className="space-y-3">
        <img
          src={main}
          alt="Property"
          loading="lazy"
          className={[
            "block w-full h-[220px] object-cover border border-white/10 bg-white/[0.02]",
            roundedClassName,
          ].join(" ")}
        />

        {list.length > 1 ? (
          <div className="grid grid-cols-4 gap-2">
            {list.slice(0, 8).map((src, idx) => {
              const selected = idx === active;
              return (
                <button
                  key={`${src}-${idx}`}
                  type="button"
                  onClick={() => setActive(idx)}
                  className={[
                    "relative overflow-hidden rounded-xl border cursor-pointer transition",
                    selected
                      ? "border-white/40 ring-1 ring-white/30"
                      : "border-white/10 hover:border-white/20",
                  ].join(" ")}
                  title={`Photo ${idx + 1}`}
                >
                  <img
                    src={src}
                    alt={`Property thumbnail ${idx + 1}`}
                    loading="lazy"
                    className="block w-full h-16 object-cover bg-white/[0.02]"
                  />
                </button>
              );
            })}
          </div>
        ) : null}
      </div>
    </div>
  );
}
