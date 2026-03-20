import React from "react";
import {
  MapPinned,
  ShieldAlert,
  Users,
  LocateFixed,
  AlertTriangle,
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
    };
  }

  if (!normalizedAddress) {
    return {
      label: "Location partial",
      pillClass: "oh-pill oh-pill-warn",
    };
  }

  if (geocodeConfidence != null && Number(geocodeConfidence) < 0.7) {
    return {
      label: "Location approximate",
      pillClass: "oh-pill oh-pill-warn",
    };
  }

  return {
    label: "Location verified",
    pillClass: "oh-pill oh-pill-good",
  };
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

  return (
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
        <span className="oh-pill">source {geocodeSource}</span>
      ) : null}

      {conf ? <span className="oh-pill">confidence {conf}</span> : null}

      <span className={redTone}>
        <ShieldAlert className="h-3.5 w-3.5" />
        {isRedZone ? "Red zone" : "Not red zone"}
      </span>

      {crimeScore != null ? (
        <span className="oh-pill">crime {Number(crimeScore).toFixed(1)}</span>
      ) : null}

      {offenderCount != null ? (
        <span className="oh-pill">
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
  );
}
