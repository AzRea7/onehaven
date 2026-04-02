from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


BASE_RATE_BY_TYPE: dict[str, float] = {
    "single_family": 0.0046,
    "multi_family": 0.0052,
    "multifamily": 0.0052,
    "duplex": 0.0050,
    "triplex": 0.0052,
    "quadplex": 0.0054,
    "townhouse": 0.0044,
    "condo": 0.0036,
}


@dataclass
class InsuranceProfile:
    annual_amount: float | None
    monthly_amount: float | None
    source: str | None
    confidence: float | None


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _coalesce_first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _coerce_meta(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _resolved_price(row: dict[str, Any]) -> float | None:
    return _coalesce_first_number(
        row.get("listing_price"),
        row.get("asking_price"),
        row.get("price"),
    )


def normalize_insurance_profile(
    *,
    annual_amount: float | None,
    source: str | None,
    confidence: float | None,
) -> InsuranceProfile:
    annual = _safe_float(annual_amount)
    if annual is not None and annual < 0:
        annual = None

    monthly = round(annual / 12.0, 2) if annual is not None else None
    conf = _safe_float(confidence)
    if conf is not None:
        conf = max(0.0, min(1.0, conf))

    return InsuranceProfile(
        annual_amount=round(annual, 2) if annual is not None else None,
        monthly_amount=monthly,
        source=str(source).strip() if source else None,
        confidence=conf,
    )


def _get_existing_property_columns(db: Session) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'properties'
            """
        )
    ).fetchall()
    return {str(row[0]) for row in rows}


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        return dict(row._mapping)
    except Exception:
        return dict(row)


def _load_property_row(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any] | None:
    existing_cols = _get_existing_property_columns(db)

    preferred_cols = [
        "id",
        "org_id",
        "address",
        "city",
        "state",
        "zip",
        "county",
        "property_type",
        "square_feet",
        "bedrooms",
        "bathrooms",
        "units",
        "year_built",
        "listing_price",
        "asking_price",
        "price",
        "insurance_annual",
        "insurance_source",
        "insurance_confidence",
        "monthly_insurance",
        "acquisition_metadata_json",
    ]

    selected_cols = [col for col in preferred_cols if col in existing_cols]
    if not selected_cols:
        return None

    sql = f"""
        SELECT
            {", ".join(selected_cols)}
        FROM properties
        WHERE org_id = :org_id
          AND id = :property_id
        LIMIT 1
    """

    row = db.execute(
        text(sql),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
        },
    ).fetchone()

    data = _row_to_dict(row)
    if data is None:
        return None

    # normalize absent optional fields so the rest of the code can safely use .get(...)
    for optional_key in preferred_cols:
        data.setdefault(optional_key, None)

    return data


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
    source: str | None = None
    confidence: float | None = None

    if annual_amount is not None:
        source = "listing_metadata"
        confidence = 0.90
    else:
        property_type = str(row.get("property_type") or "single_family").strip().lower()
        square_feet = _safe_float(row.get("square_feet"))
        year_built = _safe_float(row.get("year_built"))
        bedrooms = _safe_float(row.get("bedrooms"))
        bathrooms = _safe_float(row.get("bathrooms"))
        units = max(_safe_float(row.get("units"), 1.0) or 1.0, 1.0)

        base_rate = BASE_RATE_BY_TYPE.get(property_type, 0.0048)

        age_multiplier = 1.0
        if year_built is not None and year_built < 1950:
            age_multiplier += 0.10
        elif year_built is not None and year_built < 1975:
            age_multiplier += 0.06

        size_multiplier = 1.0
        if square_feet is not None:
            if square_feet < 900:
                size_multiplier -= 0.04
            elif square_feet > 2200:
                size_multiplier += 0.06
            elif square_feet > 3200:
                size_multiplier += 0.10

        layout_multiplier = 1.0
        if bedrooms is not None and bedrooms >= 4:
            layout_multiplier += 0.03
        if bathrooms is not None and bathrooms >= 3:
            layout_multiplier += 0.02

        unit_multiplier = 1.0
        if property_type in {"multi_family", "multifamily", "duplex", "triplex", "quadplex"}:
            unit_multiplier += min((units - 1.0) * 0.08, 0.24)

        sqft_floor = max(square_feet or 1000.0, 800.0)
        replacement_basis = max((resolved_price or 0.0) * 0.80, sqft_floor * 140.0)

        annual_amount = (
            replacement_basis
            * base_rate
            * age_multiplier
            * size_multiplier
            * layout_multiplier
            * unit_multiplier
        )

        # realism guardrails
        annual_amount = max(annual_amount, 900.0)
        annual_amount = min(annual_amount, max((resolved_price or 150000.0) * 0.018, 12000.0))

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


def get_property_insurance_context(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    row = _load_property_row(db, org_id=org_id, property_id=property_id) or {}
    return {
        "insurance_annual": _safe_float(row.get("insurance_annual")),
        "insurance_source": row.get("insurance_source"),
        "insurance_confidence": _safe_float(row.get("insurance_confidence")),
        "resolved_price": _resolved_price(row),
    }