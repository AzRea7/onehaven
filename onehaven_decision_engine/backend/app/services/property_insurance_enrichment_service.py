from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..domain.underwriting import normalize_insurance_profile


BASE_RATE_BY_TYPE = {
    "single_family": 0.0045,
    "multifamily": 0.0055,
    "condo": 0.0035,
    "townhome": 0.0040,
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        return out if out > 0 else None
    except Exception:
        return None


def _coalesce_first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _coerce_meta(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _resolved_price(row: dict[str, Any]) -> float | None:
    meta = _coerce_meta(row.get("acquisition_metadata_json"))
    return _coalesce_first_number(
        row.get("listing_price"),
        meta.get("asking_price"),
        meta.get("listing_price"),
        meta.get("price"),
        meta.get("purchasePrice"),
        meta.get("purchase_price"),
        meta.get("listPrice"),
    )


def _load_property_row(db: Session, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT
                id,
                org_id,
                property_type,
                square_feet,
                year_built,
                listing_price,
                insurance_annual,
                insurance_source,
                insurance_confidence,
                acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()
    return dict(row) if row else None


def enrich_property_insurance(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    force: bool = False,
) -> dict[str, Any]:
    row = _load_property_row(db, org_id=org_id, property_id=property_id)
    if row is None:
        raise ValueError("property not found")

    resolved_price = _resolved_price(row)

    if not force and row.get("insurance_annual") is not None:
        profile = normalize_insurance_profile(
            annual_amount=row.get("insurance_annual"),
            source=row.get("insurance_source"),
            confidence=row.get("insurance_confidence"),
        )
        return {
            "ok": True,
            "property_id": property_id,
            "resolved_price": resolved_price,
            **profile.__dict__,
            "cached": True,
        }

    meta = _coerce_meta(row.get("acquisition_metadata_json"))

    annual_amount = _coalesce_first_number(
        meta.get("insuranceAnnual"),
        meta.get("annualInsurance"),
        meta.get("insurancePremiumAnnual"),
    )
    source = None
    confidence = None

    if annual_amount is not None:
        source = "listing_metadata"
        confidence = 0.90
    else:
        property_type = str(row.get("property_type") or "single_family").strip().lower()
        square_feet = _safe_float(row.get("square_feet"))
        year_built = _safe_float(row.get("year_built"))

        base_rate = BASE_RATE_BY_TYPE.get(property_type, 0.0048)
        age_multiplier = 1.08 if year_built is not None and year_built < 1950 else 1.0
        sqft_floor = max(square_feet or 1000.0, 800.0)
        replacement_basis = max((resolved_price or 0.0) * 0.80, sqft_floor * 140.0)

        annual_amount = replacement_basis * base_rate * age_multiplier
        source = "replacement_cost_estimate"
        confidence = 0.62

    profile = normalize_insurance_profile(
        annual_amount=annual_amount,
        source=source,
        confidence=confidence,
    )

    db.execute(
        text(
            """
            UPDATE properties
            SET insurance_annual = :annual_amount,
                insurance_source = :source,
                insurance_confidence = :confidence,
                monthly_insurance = CASE
                    WHEN :annual_amount IS NOT NULL
                    THEN ROUND((:annual_amount / 12.0)::numeric, 2)
                    ELSE monthly_insurance
                END,
                updated_at = now()
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "annual_amount": profile.annual_amount,
            "source": profile.source,
            "confidence": profile.confidence,
        },
    )
    db.flush()

    return {
        "ok": True,
        "property_id": property_id,
        "resolved_price": resolved_price,
        **profile.__dict__,
        "cached": False,
    }


def get_property_insurance_context(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = _load_property_row(db, org_id=org_id, property_id=property_id) or {}
    return {
        "insurance_annual": _safe_float(row.get("insurance_annual")),
        "insurance_source": row.get("insurance_source"),
        "insurance_confidence": _safe_float(row.get("insurance_confidence")),
        "resolved_price": _resolved_price(row),
    }