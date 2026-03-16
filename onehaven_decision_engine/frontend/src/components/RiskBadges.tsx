import React from "react";
import { MapPinned, ShieldAlert, Users } from "lucide-react";

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
  const redTone = isRedZone ? "oh-pill oh-pill-bad" : "oh-pill oh-pill-good";

  return (
    <div className="flex flex-wrap gap-2">
      {county ? (
        <span className="oh-pill">
          <MapPinned className="h-3.5 w-3.5" />
          {county}
        </span>
      ) : null}

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
      ) : null}
    </div>
  );
}
