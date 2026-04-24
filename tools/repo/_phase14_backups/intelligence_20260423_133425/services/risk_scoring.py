from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.services.crime_index import compute_crime_metrics
from app.services.offender_index import compute_offender_metrics

DEFAULT_OFFENDER_RADIUS_MILES = 0.75


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def composite_risk_band(score: float) -> str:
    if score >= 85:
        return "avoid"
    if score >= 65:
        return "caution"
    if score >= 45:
        return "watch"
    if score >= 25:
        return "stable"
    return "preferred"


def _risk_confidence(*, crime_confidence: float | None, offender_source: str | None, missing_coordinates: bool) -> float:
    if missing_coordinates:
        return 0.0
    confidence = float(crime_confidence or 0.45)
    if str(offender_source or "").strip().lower() == "local_dataset":
        confidence += 0.10
    return round(_clamp(confidence, 0.0, 1.0), 2)


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
            "crime_source": None,
            "crime_method": None,
            "crime_radius_miles": None,
            "crime_area_sq_miles": None,
            "crime_area_type": None,
            "crime_incident_count": None,
            "crime_weighted_incident_count": None,
            "crime_nearest_incident_miles": None,
            "crime_dataset_version": None,
            "crime_confidence": 0.0,
            "investment_area_band": "unknown",
            "offender_count": None,
            "offender_band": "unknown",
            "offender_source": None,
            "offender_radius_miles": None,
            "nearest_offender_miles": None,
            "risk_score": None,
            "risk_band": "unknown",
            "risk_summary": "missing_coordinates",
            "risk_factors": ["missing_coordinates"],
            "risk_confidence": 0.0,
            "risk_last_computed_at": datetime.utcnow().isoformat(),
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
        radius_miles=DEFAULT_OFFENDER_RADIUS_MILES,
    )

    crime_score = float(crime["crime_score"])
    offender_count = int(offenders["offender_count"])
    offender_pressure = min(28.0, offender_count * 4.5)
    red_zone_penalty = 14.0 if is_red_zone else 0.0

    # Investing-focused risk composition:
    # local violent/property crime is dominant, offenders are secondary, red-zone a strong tie-breaker.
    risk_score = round(_clamp((crime_score * 0.72) + offender_pressure + red_zone_penalty), 2)

    factors: list[str] = []
    if is_red_zone:
        factors.append("red_zone")
    if crime_score >= 85:
        factors.append("extreme_crime")
    elif crime_score >= 65:
        factors.append("high_crime")
    elif crime_score >= 45:
        factors.append("watch_crime")
    else:
        factors.append("crime_within_buy_box")

    if offender_count >= 10:
        factors.append("very_high_offender_count")
    elif offender_count >= 6:
        factors.append("high_offender_count")
    elif offender_count >= 3:
        factors.append("moderate_offender_count")

    if crime.get("crime_source") != "local_dataset":
        factors.append("crime_heuristic_fallback")
    if offenders.get("offender_source") != "local_dataset":
        factors.append("offender_heuristic_fallback")

    return {
        **crime,
        **offenders,
        "risk_score": risk_score,
        "risk_band": composite_risk_band(risk_score),
        "risk_summary": ",".join(factors),
        "risk_factors": factors,
        "risk_confidence": _risk_confidence(
            crime_confidence=float(crime.get("crime_confidence") or 0.0),
            offender_source=offenders.get("offender_source"),
            missing_coordinates=False,
        ),
        "risk_last_computed_at": datetime.utcnow().isoformat(),
    }


def classify_deal_candidate(
    *,
    normalized_decision: str | None,
    risk_score: float | None,
    projected_monthly_cashflow: float | None,
    dscr: float | None,
    listing_hidden: bool = False,
) -> dict[str, Any]:
    if listing_hidden:
        return {
            "deal_filter_status": "hidden",
            "is_deal_candidate": False,
            "suppress_from_investor": True,
            "hidden_reason": "inactive_listing",
            "candidate_reasons": [],
            "suppress_reasons": ["inactive_listing"],
        }

    decision = str(normalized_decision or "").strip().upper() or "REVIEW"
    risk = float(risk_score) if risk_score is not None else None
    cashflow = float(projected_monthly_cashflow) if projected_monthly_cashflow is not None else None
    dscr_value = float(dscr) if dscr is not None else None

    candidate_reasons: list[str] = []
    suppress_reasons: list[str] = []

    if decision == "GOOD":
        candidate_reasons.append("good_decision")
    elif decision == "REJECT":
        suppress_reasons.append("rejected_decision")

    if cashflow is not None:
        if cashflow > 0:
            candidate_reasons.append("positive_cashflow")
        elif cashflow < 0:
            suppress_reasons.append("negative_cashflow")
    else:
        suppress_reasons.append("missing_cashflow")

    if dscr_value is not None:
        if dscr_value >= 1.20:
            candidate_reasons.append("strong_dscr")
        elif dscr_value < 1.0:
            suppress_reasons.append("weak_dscr")
    else:
        suppress_reasons.append("missing_dscr")

    if risk is not None:
        if risk >= 65:
            suppress_reasons.append("bad_risk")
        elif risk <= 25:
            candidate_reasons.append("preferred_area")
        elif risk <= 45:
            candidate_reasons.append("acceptable_risk")
    else:
        suppress_reasons.append("missing_risk")

    is_candidate = (
        decision != "REJECT"
        and cashflow is not None
        and cashflow > 0
        and dscr_value is not None
        and dscr_value >= 1.0
        and risk is not None
        and risk < 65
    )

    if is_candidate:
        return {
            "deal_filter_status": "candidate",
            "is_deal_candidate": True,
            "suppress_from_investor": False,
            "hidden_reason": None,
            "candidate_reasons": candidate_reasons,
            "suppress_reasons": [],
        }

    primary_reason = (
        "bad_risk"
        if "bad_risk" in suppress_reasons
        else "weak_cashflow"
        if "negative_cashflow" in suppress_reasons or "missing_cashflow" in suppress_reasons
        else "weak_dscr"
        if "weak_dscr" in suppress_reasons or "missing_dscr" in suppress_reasons
        else "low_score"
    )

    return {
        "deal_filter_status": "suppressed",
        "is_deal_candidate": False,
        "suppress_from_investor": True,
        "hidden_reason": primary_reason,
        "candidate_reasons": candidate_reasons,
        "suppress_reasons": suppress_reasons,
    }


def compute_risk_adjusted_score(
    *,
    projected_monthly_cashflow: float | None,
    dscr: float | None,
    rent_gap: float | None,
    risk_score: float | None,
) -> dict[str, Any]:
    cashflow = float(projected_monthly_cashflow) if projected_monthly_cashflow is not None else None
    dscr_value = float(dscr) if dscr is not None else None
    gap = float(rent_gap) if rent_gap is not None else None
    risk = float(risk_score) if risk_score is not None else None

    cashflow_score = 0.0
    if cashflow is not None:
        cashflow_score = max(-25.0, min(35.0, cashflow / 15.0))

    dscr_score = 0.0
    if dscr_value is not None:
        if dscr_value >= 1.5:
            dscr_score = 25.0
        elif dscr_value >= 1.2:
            dscr_score = 18.0
        elif dscr_value >= 1.0:
            dscr_score = 8.0
        else:
            dscr_score = -20.0

    rent_gap_score = 0.0
    if gap is not None:
        # Unknown gap stays neutral. Only real negative values reduce the score.
        rent_gap_score = max(-15.0, min(20.0, gap / 20.0))

    risk_penalty = 0.0
    if risk is not None:
        if risk >= 85:
            risk_penalty = 55.0
        elif risk >= 65:
            risk_penalty = 42.0
        elif risk >= 45:
            risk_penalty = 25.0
        elif risk >= 25:
            risk_penalty = 12.0
        else:
            risk_penalty = max(0.0, risk * 0.20)

    rank_score = round(cashflow_score + dscr_score + rent_gap_score - risk_penalty, 2)

    return {
        "cashflow_score": round(cashflow_score, 2),
        "dscr_score": round(dscr_score, 2),
        "rent_gap_score": round(rent_gap_score, 2),
        "risk_penalty": round(risk_penalty, 2),
        "risk_adjusted_score": rank_score,
        "rank_score": rank_score,
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

    for field in [
        "crime_density",
        "crime_score",
        "crime_band",
        "crime_source",
        "crime_method",
        "crime_radius_miles",
        "crime_area_sq_miles",
        "crime_area_type",
        "crime_incident_count",
        "crime_weighted_incident_count",
        "crime_nearest_incident_miles",
        "crime_dataset_version",
        "crime_confidence",
        "investment_area_band",
        "offender_count",
        "offender_band",
        "offender_source",
        "offender_radius_miles",
        "nearest_offender_miles",
        "risk_score",
        "risk_band",
        "risk_summary",
        "risk_confidence",
    ]:
        if hasattr(prop, field):
            setattr(prop, field, risk.get(field))

    if hasattr(prop, "risk_last_computed_at"):
        raw = risk.get("risk_last_computed_at")
        try:
            prop.risk_last_computed_at = datetime.fromisoformat(str(raw))
        except Exception:
            prop.risk_last_computed_at = datetime.utcnow()

    db.add(prop)
    db.commit()
    db.refresh(prop)

    out = {"ok": True, "property_id": int(prop.id)}
    out.update(risk)
    return out
