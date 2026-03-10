import React from "react";

function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn" | "bad";
}) {
  const cls =
    tone === "good"
      ? "border-green-400/20 bg-green-400/10 text-green-200"
      : tone === "warn"
        ? "border-yellow-300/20 bg-yellow-300/10 text-yellow-100"
        : tone === "bad"
          ? "border-red-400/20 bg-red-400/10 text-red-200"
          : "border-white/10 bg-white/5 text-white/80";

  return (
    <span className={`text-[11px] px-2 py-1 rounded-full border ${cls}`}>
      {children}
    </span>
  );
}

export default function RiskBadges({
  county,
  isRedZone,
  crimeScore,
  offenderCount,
  lat,
  lng,
}: {
  county?: string | null;
  isRedZone?: boolean | null;
  crimeScore?: number | null;
  offenderCount?: number | null;
  lat?: number | null;
  lng?: number | null;
}) {
  const scoreTone =
    crimeScore == null
      ? "neutral"
      : crimeScore >= 75
        ? "bad"
        : crimeScore >= 40
          ? "warn"
          : "good";

  const offenderTone =
    offenderCount == null
      ? "neutral"
      : offenderCount >= 10
        ? "bad"
        : offenderCount >= 3
          ? "warn"
          : "good";

  return (
    <div className="flex flex-wrap gap-2">
      <Badge>County: {county || "—"}</Badge>
      <Badge tone={isRedZone ? "bad" : "good"}>
        {isRedZone ? "Red zone" : "Not red zone"}
      </Badge>
      <Badge tone={scoreTone}>
        Crime: {crimeScore == null ? "—" : Number(crimeScore).toFixed(1)}
      </Badge>
      <Badge tone={offenderTone}>
        Offenders: {offenderCount == null ? "—" : offenderCount}
      </Badge>
      <Badge>
        Lat/Lng:{" "}
        {lat != null && lng != null
          ? `${Number(lat).toFixed(5)}, ${Number(lng).toFixed(5)}`
          : "—"}
      </Badge>
    </div>
  );
}
