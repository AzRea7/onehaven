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


def classify_deal_candidate(
    *,
    normalized_decision: str | None,
    risk_score: float | None,
    projected_monthly_cashflow: float | None,
    dscr: float | None,
    listing_hidden: bool = False,
) -> dict[str, Any]:
    """
    Decide whether a property should show up in the investor pane as a deal candidate.

    Status meanings:
    - candidate: worthy of showing in main investor list
    - suppressed: stays in inventory/history but hidden from the main deals-first list
    - hidden: already hidden for stronger reasons (inactive listing, etc.)
    """
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
        if risk >= 80:
            suppress_reasons.append("bad_risk")
        elif risk <= 35:
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
        and risk < 80
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
        # saturates around +500/mo
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
        # positive gap is good, but cap the effect
        rent_gap_score = max(-15.0, min(20.0, gap / 20.0))

    risk_penalty = 0.0
    if risk is not None:
        # higher risk should reduce score
        risk_penalty = max(0.0, min(45.0, risk * 0.45))

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