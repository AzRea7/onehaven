from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import mean, median
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Property, RentAssumption, RentCalibration, RentComp, RentObservation
from .underwriting import (
    RentCapReason,
    compute_effective_rent_used,
    describe_rent_cap_reason,
    select_market_rent_reference,
)


@dataclass(frozen=True)
class CompsSummary:
    count: int
    median_rent: float
    mean_rent: float
    min_rent: float
    max_rent: float


def summarize_comps(rents: Sequence[float]) -> CompsSummary:
    rents_sorted = sorted(float(r) for r in rents)
    return CompsSummary(
        count=len(rents_sorted),
        median_rent=float(median(rents_sorted)),
        mean_rent=float(mean(rents_sorted)),
        min_rent=float(rents_sorted[0]),
        max_rent=float(rents_sorted[-1]),
    )


def _to_pos_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        return out if out > 0 else None
    except Exception:
        return None


def _to_nonneg_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return max(int(float(value)), 0)
    except Exception:
        return default


def _norm_strategy(strategy: Optional[str]) -> str:
    s = str(strategy or "section8").strip().lower()
    return s if s in {"section8", "market"} else "section8"


def _is_multifamily(property_row: Property) -> bool:
    ptype = str(getattr(property_row, "property_type", "") or "").strip().lower()
    units = _to_nonneg_int(getattr(property_row, "units", None), 0)
    return "multi" in ptype and units > 1


def _normalized_units(property_row: Property) -> int:
    return max(_to_nonneg_int(getattr(property_row, "units", None), 1), 1)


def get_or_create_rent_assumption(
    db: Session,
    property_id: int,
    *,
    org_id: int | None = None,
) -> RentAssumption:
    stmt = select(RentAssumption).where(RentAssumption.property_id == property_id)
    if org_id is not None and hasattr(RentAssumption, "org_id"):
        stmt = stmt.where(RentAssumption.org_id == org_id)

    ra = db.execute(stmt).scalar_one_or_none()
    if ra:
        return ra

    init_kwargs: dict[str, object] = {"property_id": property_id}
    if org_id is not None and hasattr(RentAssumption, "org_id"):
        init_kwargs["org_id"] = org_id

    ra = RentAssumption(**init_kwargs)
    db.add(ra)
    db.flush()
    return ra


def _approved_fmr_ceiling(
    *,
    section8_fmr: Optional[float],
    strategy: str,
    payment_standard_pct: float | None,
    approved_override: Optional[float] = None,
) -> Optional[float]:
    """
    Critical fix:
    Section 8 underwriting uses strict raw HUD FMR as the ceiling.

    We only allow payment_standard_pct to influence non-section8 modes.
    """
    override = _to_pos_float(approved_override)
    fmr = _to_pos_float(section8_fmr)
    mode = _norm_strategy(strategy)

    if fmr is None:
        return override

    if mode == "section8":
        base_ceiling = round(float(fmr), 2)
    else:
        pct = float(payment_standard_pct or 1.0)
        base_ceiling = round(float(fmr) * pct, 2)

    if override is None:
        return base_ceiling

    return round(min(float(override), float(base_ceiling)), 2)


def get_calibration_multiplier(db: Session, *, zip_code: str, bedrooms: int, strategy: str) -> float:
    row = db.execute(
        select(RentCalibration).where(
            RentCalibration.zip == zip_code,
            RentCalibration.bedrooms == bedrooms,
            RentCalibration.strategy == strategy,
        )
    ).scalar_one_or_none()

    if not row:
        return 1.0
    return float(row.multiplier)


def apply_calibration(raw_market_rent: Optional[float], multiplier: float) -> Optional[float]:
    if raw_market_rent is None:
        return None
    return round(float(raw_market_rent) * float(multiplier), 2)


def update_calibration_from_observation(
    db: Session,
    *,
    property_row: Property,
    strategy: str,
    predicted_market_rent: Optional[float],
    achieved_rent: float,
) -> RentCalibration:
    zip_code = property_row.zip
    bedrooms = property_row.bedrooms

    cal = db.execute(
        select(RentCalibration).where(
            RentCalibration.zip == zip_code,
            RentCalibration.bedrooms == bedrooms,
            RentCalibration.strategy == strategy,
        )
    ).scalar_one_or_none()

    if not cal:
        cal = RentCalibration(
            zip=zip_code,
            bedrooms=bedrooms,
            strategy=strategy,
            multiplier=1.0,
            samples=0,
            mape=None,
            updated_at=datetime.utcnow(),
        )
        db.add(cal)
        db.flush()

    if predicted_market_rent and predicted_market_rent > 0:
        ratio = float(achieved_rent) / float(predicted_market_rent)

        alpha = float(settings.rent_calibration_alpha)
        new_mult = (1 - alpha) * float(cal.multiplier) + alpha * float(ratio)

        new_mult = max(
            float(settings.rent_calibration_min_mult),
            min(float(settings.rent_calibration_max_mult), new_mult),
        )

        abs_pe = abs(float(achieved_rent) - float(predicted_market_rent)) / float(predicted_market_rent)
        if cal.mape is None:
            cal.mape = abs_pe
        else:
            cal.mape = (1 - alpha) * float(cal.mape) + alpha * abs_pe

        cal.multiplier = new_mult

    cal.samples = int(cal.samples) + 1
    cal.updated_at = datetime.utcnow()
    db.add(cal)
    return cal


def recompute_rent_fields(
    db: Session,
    *,
    property_id: int,
    strategy: str,
    payment_standard_pct: Optional[float] = None,
) -> dict:
    """
    Shared rent truth used by rent.py, rent_enrich.py, and property snapshots.

    Final logic:
    - pick a conservative market reference using min(RentCast estimate, nearby comp median)
    - calibrate that market reference
    - for Section 8, use strict raw HUD FMR as the ceiling
    - rent_used = min(calibrated_market_reference, raw_fmr_ceiling)
    - if market is missing, fall back to FMR
    - for multifamily, apply the cap per unit then multiply back out
    """
    prop = db.get(Property, property_id)
    if not prop:
        raise ValueError(f"Property {property_id} not found")

    strategy = _norm_strategy(strategy)
    ra = get_or_create_rent_assumption(db, property_id, org_id=getattr(prop, "org_id", None))

    raw_market_rent = _to_pos_float(getattr(ra, "market_rent_estimate", None))
    rent_reasonableness_comp = _to_pos_float(getattr(ra, "rent_reasonableness_comp", None))
    section8_fmr = _to_pos_float(getattr(ra, "section8_fmr", None))

    pct = float(payment_standard_pct) if payment_standard_pct is not None else (
        1.0 if strategy == "section8"
        else float(getattr(settings, "default_payment_standard_pct", 1.0) or 1.0)
    )

    mult = get_calibration_multiplier(
        db,
        zip_code=getattr(prop, "zip", "") or "",
        bedrooms=int(getattr(prop, "bedrooms", 0) or 0),
        strategy=strategy,
    )

    market_reference_rent = select_market_rent_reference(
        market_rent_estimate=raw_market_rent,
        rent_reasonableness_comp=rent_reasonableness_comp,
    )
    calibrated_market = apply_calibration(market_reference_rent, mult)

    approved = _approved_fmr_ceiling(
        section8_fmr=section8_fmr,
        strategy=strategy,
        payment_standard_pct=pct,
        approved_override=_to_pos_float(getattr(ra, "approved_rent_ceiling", None)),
    )

    if strategy == "market":
        market_reason: RentCapReason = "rentcast_under_fmr" if calibrated_market is not None else "missing_rent_inputs"
        return {
            "market_rent_estimate": raw_market_rent,
            "rent_reasonableness_comp": rent_reasonableness_comp,
            "market_reference_rent": market_reference_rent,
            "section8_fmr": section8_fmr,
            "approved_rent_ceiling": approved,
            "calibrated_market_rent": calibrated_market,
            "rent_used": calibrated_market,
            "rent_cap_reason": market_reason,
            "multiplier": mult,
            "payment_standard_pct": pct,
            "explanation": describe_rent_cap_reason(market_reason, strategy=strategy),
        }

    units = _normalized_units(prop)
    is_multi = _is_multifamily(prop)

    unit_market: float | None = None
    unit_fmr: float | None = None
    if is_multi and units > 1:
        if calibrated_market is not None:
            unit_market = round(float(calibrated_market) / float(units), 2)
        if approved is not None:
            unit_fmr = round(float(approved) / float(units), 2)

    rent_used, rent_cap_reason = compute_effective_rent_used(
        property_type=getattr(prop, "property_type", None),
        bedrooms=_to_nonneg_int(getattr(prop, "bedrooms", None), 0),
        units=units,
        rentcast_rent=calibrated_market,
        fmr_rent=approved,
        unit_rentcast_rent=unit_market,
        unit_fmr_rent=unit_fmr,
    )

    return {
        "market_rent_estimate": raw_market_rent,
        "rent_reasonableness_comp": rent_reasonableness_comp,
        "market_reference_rent": market_reference_rent,
        "section8_fmr": section8_fmr,
        "approved_rent_ceiling": approved,
        "calibrated_market_rent": calibrated_market,
        "rent_used": rent_used,
        "rent_cap_reason": rent_cap_reason,
        "multiplier": mult,
        "payment_standard_pct": pct,
        "explanation": describe_rent_cap_reason(rent_cap_reason, strategy=strategy),
    }
