# onehaven_decision_engine/backend/app/services/risk_scoring.py
from __future__ import annotations

from typing import Any

from .crime_index import compute_crime_metrics
from .offender_index import compute_offender_metrics


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def composite_risk_band(score: float) -> str:
    if score >= 80:
        return "very_high"
    if score >= 60:
        return "high"
    if score >= 35:
        return "moderate"
    if score >= 15:
        return "low"
    return "very_low"


def compute_property_risk(
    *,
    lat: float | None,
    lng: float | None,
    city: str | None = None,
    county: str | None = None,
    is_red_zone: bool = False,
) -> dict[str, Any]:
    if lat is None or lng is None:
        return {
            "crime_density": None,
            "crime_score": None,
            "crime_band": "unknown",
            "offender_count": None,
            "offender_band": "unknown",
            "risk_score": None,
            "risk_band": "unknown",
            "risk_summary": "missing_coordinates",
            "risk_factors": ["missing_coordinates"],
        }

    crime = compute_crime_metrics(
        lat=float(lat),
        lng=float(lng),
        city=city,
        county=county,
        is_red_zone=bool(is_red_zone),
    )
    offenders = compute_offender_metrics(
        lat=float(lat),
        lng=float(lng),
        city=city,
        county=county,
        radius_miles=1.0,
    )

    crime_score = float(crime["crime_score"])
    offender_count = int(offenders["offender_count"])
    offender_component = min(offender_count * 8.0, 35.0)
    red_zone_component = 25.0 if is_red_zone else 0.0

    raw = (crime_score * 0.70) + offender_component + red_zone_component
    risk_score = round(_clamp(raw), 2)

    factors: list[str] = []
    if is_red_zone:
        factors.append("red_zone")
    if crime_score >= 60:
        factors.append("high_crime_score")
    elif crime_score >= 35:
        factors.append("moderate_crime_score")
    if offender_count >= 6:
        factors.append("high_offender_count")
    elif offender_count >= 3:
        factors.append("moderate_offender_count")

    if not factors:
        factors.append("low_detected_risk")

    return {
        **crime,
        **offenders,
        "risk_score": risk_score,
        "risk_band": composite_risk_band(risk_score),
        "risk_summary": ",".join(factors),
        "risk_factors": factors,
    }