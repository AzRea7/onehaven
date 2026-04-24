# backend/app/domain/operating_truth_enforcement.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.domain.operating_truth import (
    TruthViolation,
    enforce_assertion_truth,
    enforce_deal_truth,
    enforce_evidence_truth,
    enforce_jurisdiction_truth,
    enforce_property_truth,
)


@dataclass(frozen=True)
class DealIntakeFacts:
    address: str
    city: str
    state: str
    zip: str
    bedrooms: int
    bathrooms: float
    asking_price: float


def _raise_from_truth_violation(exc: TruthViolation) -> None:
    raise HTTPException(status_code=422, detail=exc.message)


def enforce_constitution_for_deal_intake(f: DealIntakeFacts) -> None:
    """
    Phase 0: Operating Truth = laws in code.
    Every entry point must call this (intake, create deal, imports, future edits).
    """
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

    try:
        enforce_property_truth(
            {
                "address": f.address,
                "city": f.city,
                "state": f.state,
                "zip": f.zip,
                "bedrooms": f.bedrooms,
                "bathrooms": f.bathrooms,
            }
        )
        enforce_deal_truth({"asking_price": f.asking_price})
    except TruthViolation as exc:
        _raise_from_truth_violation(exc)


def enforce_constitution_for_property_and_price(
    *,
    address: str,
    city: str,
    state: str,
    zip: str,
    bedrooms: int,
    bathrooms: float,
    asking_price: float,
) -> None:
    """
    Convenience helper for create/edit paths (keeps routers clean).
    """
    enforce_constitution_for_deal_intake(
        DealIntakeFacts(
            address=address,
            city=city,
            state=state,
            zip=zip,
            bedrooms=int(bedrooms),
            bathrooms=float(bathrooms),
            asking_price=float(asking_price),
        )
    )


def enforce_rent_assumption_required(*, has_rent_assumption: bool) -> None:
    """
    Phase 3 rule: if something reaches evaluate, RentAssumption must exist.
    """
    if not has_rent_assumption:
        raise HTTPException(
            status_code=422,
            detail="Phase 3: RentAssumption required before evaluate. Run /rent/enrich or create assumption.",
        )


def enforce_jurisdiction_payload_or_422(payload: dict[str, Any]) -> None:
    try:
        enforce_jurisdiction_truth(payload)
    except TruthViolation as exc:
        _raise_from_truth_violation(exc)


def enforce_assertion_payload_or_422(payload: dict[str, Any]) -> None:
    try:
        enforce_assertion_truth(payload)
    except TruthViolation as exc:
        _raise_from_truth_violation(exc)


def enforce_evidence_payload_or_422(payload: dict[str, Any]) -> None:
    try:
        enforce_evidence_truth(payload)
    except TruthViolation as exc:
        _raise_from_truth_violation(exc)
