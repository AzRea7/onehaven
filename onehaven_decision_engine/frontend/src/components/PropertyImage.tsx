import React from "react";

type Props = {
  address?: string | null;
  city?: string | null;
  state?: string | null;
  zip?: string | null;
  className?: string;
  roundedClassName?: string;
};

export default function PropertyImage({
  address,
  city,
  state,
  zip,
  className,
  roundedClassName = "rounded-2xl",
}: Props) {
  if (!address) return null;

  const query = encodeURIComponent(
    `${address} ${city ?? ""} ${state ?? ""} ${zip ?? ""}`.trim(),
  );

  // Optional key (recommended)
  const key = (import.meta as any).env?.VITE_GOOGLE_MAPS_KEY as
    | string
    | undefined;

  // If you have a key, you'll get stable results. If not, it may still render but can be limited.
  const src = key
    ? `https://maps.googleapis.com/maps/api/streetview?size=900x600&location=${query}&key=${encodeURIComponent(
        key,
      )}`
    : `https://maps.googleapis.com/maps/api/streetview?size=900x600&location=${query}`;

  return (
    <div className={className}>
      <img
        src={src}
        alt="Property"
        loading="lazy"
        className={[
          "block w-full h-full object-cover border border-white/10 bg-white/[0.02]",
          roundedClassName,
        ].join(" ")}
      />
    </div>
  );
}
