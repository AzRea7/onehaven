# backend/app/domain/operating_truth_enforcement.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException

from app.config import settings


@dataclass(frozen=True)
class DealIntakeFacts:
    address: str
    city: str
    state: str
    zip: str
    bedrooms: int
    bathrooms: float
    asking_price: float


def enforce_constitution_for_deal_intake(f: DealIntakeFacts) -> None:
    """
    This is Phase 0: Operating Truth = laws in code.
    Every entry point must call this (intake, create deal, imports, future edits).
    """

    # --- core “laws” ---
    if f.asking_price > settings.max_price:
        raise HTTPException(
            status_code=422,
            detail=f"Constitution: asking_price {f.asking_price} exceeds max_price {settings.max_price}",
        )

    if f.bedrooms < settings.min_bedrooms:
        raise HTTPException(
            status_code=422,
            detail=f"Constitution: bedrooms {f.bedrooms} below min_bedrooms {settings.min_bedrooms}",
        )

    # sanity
    if not f.address.strip() or not f.city.strip() or not f.zip.strip():
        raise HTTPException(status_code=422, detail="Missing address/city/zip")

    if f.bathrooms <= 0:
        raise HTTPException(status_code=422, detail="bathrooms must be > 0")


def enforce_rent_assumption_required(*, has_rent_assumption: bool) -> None:
    """
    Phase 3 rule: if something reaches evaluate, RentAssumption must exist.
    """
    if not has_rent_assumption:
        raise HTTPException(
            status_code=422,
            detail="Phase 3: RentAssumption required before evaluate. Run /rent/enrich or create assumption.",
        )
