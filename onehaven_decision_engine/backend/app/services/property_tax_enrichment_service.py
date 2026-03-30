from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..domain.underwriting import normalize_tax_profile


COUNTY_TAX_RATE_FALLBACK = {
    "wayne": 0.0265,
    "oakland": 0.0215,
    "macomb": 0.0205,
    "washtenaw": 0.0195,
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        return out if out > 0 else None
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
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
                county,
                city,
                state,
                listing_price,
                property_tax_annual,
                property_tax_rate_annual,
                property_tax_source,
                property_tax_confidence,
                property_tax_year,
                acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()
    return dict(row) if row else None


def _metadata_tax_amount(meta: dict[str, Any]) -> float | None:
    return _coalesce_first_number(
        meta.get("propertyTax"),
        meta.get("propertyTaxes"),
        meta.get("annualPropertyTax"),
        meta.get("annualTaxes"),
        meta.get("taxAnnualAmount"),
        meta.get("taxesAnnual"),
    )


def _metadata_tax_rate(meta: dict[str, Any]) -> float | None:
    raw = _coalesce_first_number(
        meta.get("propertyTaxRate"),
        meta.get("taxRate"),
        meta.get("annualTaxRate"),
    )
    if raw is None:
        return None
    return raw / 100.0 if raw > 1 else raw


def _monthly_tax_value(
    *,
    annual_amount: float | None,
) -> float | None:
    if annual_amount is None:
        return None
    return round(float(annual_amount) / 12.0, 2)


def enrich_property_tax(
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

    if not force and row.get("property_tax_annual") is not None:
        profile = normalize_tax_profile(
            annual_amount=row.get("property_tax_annual"),
            annual_rate=row.get("property_tax_rate_annual"),
            asking_price=resolved_price,
            source=row.get("property_tax_source"),
            confidence=row.get("property_tax_confidence"),
            year=row.get("property_tax_year"),
        )
        return {
            "ok": True,
            "property_id": property_id,
            "resolved_price": resolved_price,
            **profile.__dict__,
            "cached": True,
        }

    meta = _coerce_meta(row.get("acquisition_metadata_json"))

    annual_amount = _metadata_tax_amount(meta)
    annual_rate = _metadata_tax_rate(meta)
    source = None
    confidence = None
    year = _safe_int(meta.get("taxYear")) or datetime.utcnow().year

    if annual_amount is not None or annual_rate is not None:
        source = "listing_metadata"
        confidence = 0.92
    else:
        county = str(row.get("county") or "").strip().lower()
        annual_rate = COUNTY_TAX_RATE_FALLBACK.get(county)
        source = "county_rate_fallback" if annual_rate is not None else "missing"
        confidence = 0.55 if annual_rate is not None else 0.0

    profile = normalize_tax_profile(
        annual_amount=annual_amount,
        annual_rate=annual_rate,
        asking_price=resolved_price,
        source=source,
        confidence=confidence,
        year=year,
    )

    monthly_taxes = _monthly_tax_value(annual_amount=profile.annual_amount)

    db.execute(
        text(
            """
            UPDATE properties
            SET property_tax_annual = :annual_amount,
                property_tax_rate_annual = :annual_rate,
                property_tax_source = :source,
                property_tax_confidence = :confidence,
                property_tax_year = :tax_year,
                monthly_taxes = :monthly_taxes,
                updated_at = now()
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "annual_amount": profile.annual_amount,
            "annual_rate": profile.annual_rate,
            "source": profile.source,
            "confidence": profile.confidence,
            "tax_year": profile.year,
            "monthly_taxes": monthly_taxes,
        },
    )
    db.flush()

    return {
        "ok": True,
        "property_id": property_id,
        "resolved_price": resolved_price,
        "monthly_taxes": monthly_taxes,
        **profile.__dict__,
        "cached": False,
    }


def get_property_tax_context(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = _load_property_row(db, org_id=org_id, property_id=property_id) or {}
    return {
        "property_tax_annual": _safe_float(row.get("property_tax_annual")),
        "property_tax_rate_annual": _safe_float(row.get("property_tax_rate_annual")),
        "property_tax_source": row.get("property_tax_source"),
        "property_tax_confidence": _safe_float(row.get("property_tax_confidence")),
        "property_tax_year": _safe_int(row.get("property_tax_year")),
        "resolved_price": _resolved_price(row),
    }