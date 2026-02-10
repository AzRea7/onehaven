from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import median, mean
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Property, RentAssumption, RentCalibration, RentComp, RentObservation


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


def get_or_create_rent_assumption(db: Session, property_id: int) -> RentAssumption:
    ra = db.execute(select(RentAssumption).where(RentAssumption.property_id == property_id)).scalar_one_or_none()
    if ra:
        return ra
    ra = RentAssumption(property_id=property_id)
    db.add(ra)
    db.flush()
    return ra


def compute_approved_ceiling(
    *,
    section8_fmr: Optional[float],
    rent_reasonableness_comp: Optional[float],
    payment_standard_pct: float,
    approved_override: Optional[float] = None,
) -> Optional[float]:
    """
    Operating truth:
      approved_rent_ceiling = min(FMR * payment_standard_pct, median_local_comp)

    If one side is missing, ceiling falls back to the side that exists.
    If approved_override is provided, it wins (manual override).
    """
    if approved_override is not None:
        return float(approved_override)

    candidates: list[float] = []
    if section8_fmr is not None:
        candidates.append(float(section8_fmr) * float(payment_standard_pct))
    if rent_reasonableness_comp is not None:
        candidates.append(float(rent_reasonableness_comp))

    if not candidates:
        return None
    return float(min(candidates))


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
    return float(raw_market_rent) * float(multiplier)


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

        new_mult = max(float(settings.rent_calibration_min_mult), min(float(settings.rent_calibration_max_mult), new_mult))

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
    Computes:
      - approved_rent_ceiling (policy ceiling)
      - calibrated_market_rent (market estimate after calibration)
      - rent_used (what underwriting consumes)
    """
    prop = db.get(Property, property_id)
    if not prop:
        raise ValueError(f"Property {property_id} not found")

    ra = get_or_create_rent_assumption(db, property_id)

    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    approved = compute_approved_ceiling(
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        payment_standard_pct=pct,
        approved_override=ra.approved_rent_ceiling,
    )

    mult = get_calibration_multiplier(db, zip_code=prop.zip, bedrooms=prop.bedrooms, strategy=strategy)
    calibrated_market = apply_calibration(ra.market_rent_estimate, mult)

    s = (strategy or "section8").strip().lower()
    rent_used: Optional[float] = None

    if s == "market":
        rent_used = calibrated_market
    else:
        candidates: list[float] = []
        if calibrated_market is not None:
            candidates.append(float(calibrated_market))
        if approved is not None:
            candidates.append(float(approved))
        rent_used = float(min(candidates)) if candidates else None

    return {
        "approved_rent_ceiling": approved,
        "calibrated_market_rent": calibrated_market,
        "rent_used": rent_used,
        "multiplier": mult,
        "payment_standard_pct": pct,
    }
