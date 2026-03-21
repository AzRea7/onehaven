import React from "react";
import {
  MapPinned,
  ShieldAlert,
  Users,
  LocateFixed,
  AlertTriangle,
  BadgeCheck,
  Map,
  ShieldCheck,
} from "lucide-react";

function formatConfidence(v?: number | null) {
  if (v == null || !Number.isFinite(Number(v))) return null;
  return Number(v).toFixed(2);
}

function locationStatus(
  lat?: number | null,
  lng?: number | null,
  normalizedAddress?: string | null,
  geocodeConfidence?: number | null,
) {
  if (lat == null || lng == null) {
    return {
      label: "Location incomplete",
      pillClass: "oh-pill oh-pill-bad",
      detail:
        "Coordinates are missing, which weakens distance-based risk, local rule matching, and map confidence.",
    };
  }

  if (!normalizedAddress) {
    return {
      label: "Location partial",
      pillClass: "oh-pill oh-pill-warn",
      detail:
        "Coordinates exist, but the normalized address is incomplete, so location quality is only partial.",
    };
  }

  if (geocodeConfidence != null && Number(geocodeConfidence) < 0.7) {
    return {
      label: "Location approximate",
      pillClass: "oh-pill oh-pill-warn",
      detail:
        "The geocoder returned a lower-confidence match, so downstream locality assumptions should be reviewed.",
    };
  }

  return {
    label: "Location verified",
    pillClass: "oh-pill oh-pill-good",
    detail:
      "Coordinates, normalized address, and geocode quality look strong enough for normal workflow use.",
  };
}

function crimeTone(crimeScore?: number | null) {
  if (crimeScore == null || !Number.isFinite(Number(crimeScore))) {
    return "oh-pill";
  }
  const n = Number(crimeScore);
  if (n >= 70) return "oh-pill oh-pill-bad";
  if (n >= 40) return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-good";
}

function offenderTone(offenderCount?: number | null) {
  if (offenderCount == null || !Number.isFinite(Number(offenderCount))) {
    return "oh-pill";
  }
  const n = Number(offenderCount);
  if (n >= 5) return "oh-pill oh-pill-bad";
  if (n >= 1) return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-good";
}

function summaryTone({
  isRedZone,
  crimeScore,
  offenderCount,
  geocodeConfidence,
  lat,
  lng,
}: {
  isRedZone?: boolean | null;
  crimeScore?: number | null;
  offenderCount?: number | null;
  geocodeConfidence?: number | null;
  lat?: number | null;
  lng?: number | null;
}) {
  if (isRedZone) return "bad";
  if (lat == null || lng == null) return "bad";
  if (
    geocodeConfidence != null &&
    Number.isFinite(Number(geocodeConfidence)) &&
    Number(geocodeConfidence) < 0.7
  ) {
    return "warn";
  }
  if (
    (crimeScore != null && Number(crimeScore) >= 40) ||
    (offenderCount != null && Number(offenderCount) > 0)
  ) {
    return "warn";
  }
  return "good";
}

function SummaryCard({
  tone,
  title,
  detail,
}: {
  tone: "good" | "warn" | "bad";
  title: string;
  detail: string;
}) {
  const cls =
    tone === "good"
      ? "rounded-2xl border border-emerald-500/20 bg-emerald-500/[0.06] px-4 py-4"
      : tone === "warn"
        ? "rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-4"
        : "rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-4";

  const text =
    tone === "good"
      ? "text-emerald-100"
      : tone === "warn"
        ? "text-amber-100"
        : "text-red-100";

  return (
    <div className={cls}>
      <div className={`flex items-center gap-2 text-sm font-semibold ${text}`}>
        {tone === "good" ? (
          <ShieldCheck className="h-4 w-4" />
        ) : (
          <ShieldAlert className="h-4 w-4" />
        )}
        {title}
      </div>
      <div className={`mt-2 text-sm leading-6 ${text}/90`}>{detail}</div>
    </div>
  );
}

export default function RiskBadges({
  county,
  isRedZone,
  crimeScore,
  offenderCount,
  lat,
  lng,
  normalizedAddress,
  geocodeSource,
  geocodeConfidence,
}: {
  county?: string | null;
  isRedZone?: boolean | null;
  crimeScore?: number | null;
  offenderCount?: number | null;
  lat?: number | null;
  lng?: number | null;
  normalizedAddress?: string | null;
  geocodeSource?: string | null;
  geocodeConfidence?: number | null;
}) {
  const redTone = isRedZone ? "oh-pill oh-pill-bad" : "oh-pill oh-pill-good";
  const loc = locationStatus(lat, lng, normalizedAddress, geocodeConfidence);
  const conf = formatConfidence(geocodeConfidence);

  const summary = summaryTone({
    isRedZone,
    crimeScore,
    offenderCount,
    geocodeConfidence,
    lat,
    lng,
  });

  const summaryTitle =
    summary === "good"
      ? "Risk and location posture looks healthy"
      : summary === "warn"
        ? "Some signals need review"
        : "Risk or location data needs attention";

  const summaryDetail =
    summary === "good"
      ? "The property has usable location quality and no obvious headline risk blockers from these location signals."
      : summary === "warn"
        ? "At least one signal, such as crime, offender count, or approximate location confidence, deserves operator review."
        : "Missing location data, red-zone status, or other higher-risk signals may weaken decisions until reviewed or corrected.";

  return (
    <div className="space-y-4">
      <SummaryCard tone={summary} title={summaryTitle} detail={summaryDetail} />

      {normalizedAddress ? (
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            normalized address
          </div>
          <div className="mt-1 text-sm font-semibold text-app-0">
            {normalizedAddress}
          </div>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-2">
        {county ? (
          <span className="oh-pill">
            <MapPinned className="h-3.5 w-3.5" />
            {county}
          </span>
        ) : null}

        <span className={loc.pillClass}>
          <LocateFixed className="h-3.5 w-3.5" />
          {loc.label}
        </span>

        {geocodeSource ? (
          <span className="oh-pill">
            <Map className="h-3.5 w-3.5" />
            source {geocodeSource}
          </span>
        ) : null}

        {conf ? (
          <span className="oh-pill">
            <BadgeCheck className="h-3.5 w-3.5" />
            confidence {conf}
          </span>
        ) : null}

        <span className={redTone}>
          <ShieldAlert className="h-3.5 w-3.5" />
          {isRedZone ? "Red zone" : "Not red zone"}
        </span>

        {crimeScore != null ? (
          <span className={crimeTone(crimeScore)}>
            crime {Number(crimeScore).toFixed(1)}
          </span>
        ) : null}

        {offenderCount != null ? (
          <span className={offenderTone(offenderCount)}>
            <Users className="h-3.5 w-3.5" />
            offenders {offenderCount}
          </span>
        ) : null}

        {lat != null && lng != null ? (
          <span className="oh-pill">
            {Number(lat).toFixed(4)}, {Number(lng).toFixed(4)}
          </span>
        ) : (
          <span className="oh-pill oh-pill-warn">
            <AlertTriangle className="h-3.5 w-3.5" />
            no coordinates
          </span>
        )}
      </div>

      <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
        <div className="text-sm font-semibold text-app-0">
          Why these signals matter
        </div>
        <div className="mt-2 text-sm leading-6 text-app-3">{loc.detail}</div>

        <div className="mt-3 grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Red-zone status
            </div>
            <div className="mt-1 text-sm text-app-2">
              {isRedZone
                ? "This can materially affect acquisition confidence and downstream operating assumptions."
                : "No red-zone flag is currently present in this signal set."}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Crime / offenders
            </div>
            <div className="mt-1 text-sm text-app-2">
              These signals do not make decisions by themselves, but they do
              help explain elevated caution and review needs.
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Location quality
            </div>
            <div className="mt-1 text-sm text-app-2">
              Better location confidence improves rule matching, mapping
              quality, and trust in downstream automation.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
