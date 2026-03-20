from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Property
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


def refresh_property_risk(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    lat = getattr(prop, "lat", None)
    lng = getattr(prop, "lng", None)

    if lat is None or lng is None:
        return {
            "ok": False,
            "error": "missing_coordinates",
            "property_id": int(property_id),
        }

    risk = compute_property_risk(
        lat=float(lat),
        lng=float(lng),
        city=getattr(prop, "city", None),
        county=getattr(prop, "county", None),
        is_red_zone=bool(getattr(prop, "is_red_zone", False)),
    )

    if hasattr(prop, "crime_density"):
        prop.crime_density = risk.get("crime_density")
    if hasattr(prop, "crime_score"):
        prop.crime_score = risk.get("crime_score")
    if hasattr(prop, "offender_count"):
        prop.offender_count = risk.get("offender_count")
    if hasattr(prop, "risk_score"):
        prop.risk_score = risk.get("risk_score")
    if hasattr(prop, "risk_band"):
        prop.risk_band = risk.get("risk_band")

    db.add(prop)
    db.commit()
    db.refresh(prop)

    return {
        "ok": True,
        "property_id": int(prop.id),
        "crime_density": getattr(prop, "crime_density", None),
        "crime_score": getattr(prop, "crime_score", None),
        "offender_count": getattr(prop, "offender_count", None),
        "crime_band": risk.get("crime_band"),
        "crime_source": risk.get("crime_source"),
        "offender_band": risk.get("offender_band"),
        "offender_source": risk.get("offender_source"),
        "offender_radius_miles": risk.get("offender_radius_miles"),
        "nearest_offender_miles": risk.get("nearest_offender_miles"),
        "risk_score": risk.get("risk_score"),
        "risk_band": risk.get("risk_band"),
        "risk_summary": risk.get("risk_summary"),
        "risk_factors": risk.get("risk_factors"),
    }
