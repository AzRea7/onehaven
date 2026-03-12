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
            "flex flex-col items-center justify-center text-center px-5",
            roundedClassName,
          ].join(" ")}
        >
          <div className="w-full h-[220px] rounded-[inherit] overflow-hidden flex items-center justify-center bg-[radial-gradient(circle_at_top,_rgba(255,255,255,0.14),_rgba(255,255,255,0.03)_45%,_rgba(255,255,255,0.01)_100%)]">
            <div className="space-y-3">
              <div className="mx-auto h-14 w-14 rounded-2xl border border-white/10 bg-white/[0.04] flex items-center justify-center text-2xl">
                🏠
              </div>
              <div className="text-sm font-semibold text-white">
                Image placeholder
              </div>
              <div className="mx-auto max-w-[290px] text-xs text-white/55 leading-5">
                Automated property ingestion is active. Listing images have not
                been connected yet, so this property is using a placeholder for
                now.
              </div>

              {zillowUrl ? (
                <a
                  href={zillowUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex mt-1 oh-btn cursor-pointer"
                >
                  open listing ↗
                </a>
              ) : null}
            </div>
          </div>
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
