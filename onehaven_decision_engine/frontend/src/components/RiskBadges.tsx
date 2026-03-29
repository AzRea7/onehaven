import React from "react";
import {
  AlertTriangle,
  BadgeCheck,
  LocateFixed,
  MapPinned,
  ShieldAlert,
  ShieldCheck,
  Users,
} from "lucide-react";

function formatConfidence(v?: number | null) {
  if (v == null || !Number.isFinite(Number(v))) return null;
  return `${Math.round(Number(v) * 100)}%`;
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
        "Coordinates are missing, so crime radius, rent pressure, and locality-driven risk are not trustworthy yet.",
    };
  }

  if (!normalizedAddress) {
    return {
      label: "Location partial",
      pillClass: "oh-pill oh-pill-warn",
      detail:
        "Coordinates exist, but the normalized address is incomplete, so local market assumptions should be reviewed.",
    };
  }

  if (geocodeConfidence != null && Number(geocodeConfidence) < 0.7) {
    return {
      label: "Location approximate",
      pillClass: "oh-pill oh-pill-warn",
      detail:
        "The geocoder returned a lower-confidence match, so block-level risk should be treated cautiously.",
    };
  }

  return {
    label: "Location verified",
    pillClass: "oh-pill oh-pill-good",
    detail:
      "Coordinates, normalized address, and geocode quality are strong enough for neighborhood-level investing decisions.",
  };
}

function crimeTone(crimeScore?: number | null) {
  if (crimeScore == null || !Number.isFinite(Number(crimeScore)))
    return "oh-pill";
  const n = Number(crimeScore);
  if (n >= 85) return "oh-pill oh-pill-bad";
  if (n >= 65) return "oh-pill oh-pill-warn";
  if (n >= 45) return "oh-pill";
  return "oh-pill oh-pill-good";
}

function offenderTone(offenderCount?: number | null) {
  if (offenderCount == null || !Number.isFinite(Number(offenderCount)))
    return "oh-pill";
  const n = Number(offenderCount);
  if (n >= 6) return "oh-pill oh-pill-bad";
  if (n >= 3) return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-good";
}

function areaBandTone(value?: string | null) {
  const v = String(value || "")
    .trim()
    .toLowerCase();
  if (v === "avoid") return "oh-pill oh-pill-bad";
  if (v === "caution" || v === "watch") return "oh-pill oh-pill-warn";
  if (v === "stable" || v === "preferred") return "oh-pill oh-pill-good";
  return "oh-pill";
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
    (crimeScore != null && Number(crimeScore) >= 65) ||
    (offenderCount != null && Number(offenderCount) >= 6)
  ) {
    return "bad";
  }
  if (
    (crimeScore != null && Number(crimeScore) >= 45) ||
    (offenderCount != null && Number(offenderCount) >= 3)
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
  crimeBand,
  crimeSource,
  crimeRadiusMiles,
  crimeIncidentCount,
  crimeConfidence,
  investmentAreaBand,
  offenderCount,
  offenderBand,
  offenderSource,
  riskScore,
  riskBand,
  riskConfidence,
  lat,
  lng,
  normalizedAddress,
  geocodeSource,
  geocodeConfidence,
  compact = false,
}: {
  county?: string | null;
  isRedZone?: boolean | null;
  crimeScore?: number | null;
  crimeBand?: string | null;
  crimeSource?: string | null;
  crimeRadiusMiles?: number | null;
  crimeIncidentCount?: number | null;
  crimeConfidence?: number | null;
  investmentAreaBand?: string | null;
  offenderCount?: number | null;
  offenderBand?: string | null;
  offenderSource?: string | null;
  riskScore?: number | null;
  riskBand?: string | null;
  riskConfidence?: number | null;
  lat?: number | null;
  lng?: number | null;
  normalizedAddress?: string | null;
  geocodeSource?: string | null;
  geocodeConfidence?: number | null;
  compact?: boolean;
}) {
  const redTone = isRedZone ? "oh-pill oh-pill-bad" : "oh-pill oh-pill-good";
  const loc = locationStatus(lat, lng, normalizedAddress, geocodeConfidence);
  const geoConf = formatConfidence(geocodeConfidence);
  const crimeConf = formatConfidence(crimeConfidence);
  const overallConf = formatConfidence(riskConfidence);

  const summary = summaryTone({
    isRedZone,
    crimeScore,
    offenderCount,
    geocodeConfidence,
    lat,
    lng,
  });

  if (compact) {
    return (
      <div className="flex flex-wrap gap-2">
        <span className={loc.pillClass}>
          <LocateFixed className="h-3.5 w-3.5" />
          {loc.label}
        </span>
        <span className={redTone}>
          <ShieldAlert className="h-3.5 w-3.5" />
          {isRedZone ? "Red zone" : "Not red zone"}
        </span>
        <span className={crimeTone(crimeScore)}>
          <MapPinned className="h-3.5 w-3.5" />
          Crime {crimeScore != null ? Math.round(Number(crimeScore)) : "—"}
        </span>
        <span className={areaBandTone(investmentAreaBand)}>
          <BadgeCheck className="h-3.5 w-3.5" />
          {investmentAreaBand || "unknown area"}
        </span>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <SummaryCard
        tone={summary}
        title={
          summary === "bad"
            ? "Area needs heavy caution"
            : summary === "warn"
              ? "Area needs review"
              : "Area looks investable"
        }
        detail={
          isRedZone
            ? "This property sits in a red-zone area or a strongly penalized risk zone, so it should usually be avoided unless there is a very unusual upside."
            : riskScore != null
              ? `Composite area risk is ${Math.round(Number(riskScore))} with band "${riskBand || "unknown"}". Use this as a neighborhood veto signal, not just a ranking tie-breaker.`
              : "Risk has not been computed yet."
        }
      />

      <div className="flex flex-wrap gap-2">
        <span className={loc.pillClass}>
          <LocateFixed className="h-3.5 w-3.5" />
          {loc.label}
        </span>
        <span className={redTone}>
          <ShieldAlert className="h-3.5 w-3.5" />
          {isRedZone ? "Red zone" : "Not red zone"}
        </span>
        <span className={crimeTone(crimeScore)}>
          <MapPinned className="h-3.5 w-3.5" />
          Crime score{" "}
          {crimeScore != null ? Math.round(Number(crimeScore)) : "—"}
        </span>
        <span className={offenderTone(offenderCount)}>
          <Users className="h-3.5 w-3.5" />
          Offenders {offenderCount != null ? offenderCount : "—"}
        </span>
        <span className={areaBandTone(investmentAreaBand)}>
          <BadgeCheck className="h-3.5 w-3.5" />
          {investmentAreaBand || "unknown area"}
        </span>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <MapPinned className="h-4 w-4" />
            Crime model
          </div>
          <div className="mt-3 text-sm text-app-1">
            <div>
              Score: {crimeScore != null ? Math.round(Number(crimeScore)) : "—"}
            </div>
            <div>Band: {crimeBand || "—"}</div>
            <div>Area band: {investmentAreaBand || "—"}</div>
            <div>
              Radius:{" "}
              {crimeRadiusMiles != null
                ? `${Number(crimeRadiusMiles).toFixed(2)} mi`
                : "—"}
            </div>
            <div>Nearby incidents: {crimeIncidentCount ?? "—"}</div>
            <div>Source: {crimeSource || "—"}</div>
            <div>Confidence: {crimeConf || "—"}</div>
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <Users className="h-4 w-4" />
            Offender pressure
          </div>
          <div className="mt-3 text-sm text-app-1">
            <div>Count: {offenderCount ?? "—"}</div>
            <div>Band: {offenderBand || "—"}</div>
            <div>Source: {offenderSource || "—"}</div>
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <BadgeCheck className="h-4 w-4" />
            Composite area risk
          </div>
          <div className="mt-3 text-sm text-app-1">
            <div>
              Risk score:{" "}
              {riskScore != null ? Math.round(Number(riskScore)) : "—"}
            </div>
            <div>Risk band: {riskBand || "—"}</div>
            <div>Risk confidence: {overallConf || "—"}</div>
            <div>County: {county || "—"}</div>
            <div>Geocode source: {geocodeSource || "—"}</div>
            <div>Geocode confidence: {geoConf || "—"}</div>
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-app bg-app-panel px-4 py-4 text-sm text-app-3">
        {loc.detail}
      </div>

      {crimeSource === "heuristic_fallback" ||
      offenderSource === "heuristic_fallback" ? (
        <div className="rounded-2xl border border-amber-400/20 bg-amber-500/[0.06] px-4 py-4 text-sm text-amber-100">
          <div className="flex items-center gap-2 font-semibold">
            <AlertTriangle className="h-4 w-4" />
            Risk model is partially heuristic
          </div>
          <div className="mt-2 leading-6">
            At least one area signal is coming from fallback logic rather than a
            local dataset. Treat this as a useful screen, but not final
            underwriting truth.
          </div>
        </div>
      ) : null}
    </div>
  );
}
