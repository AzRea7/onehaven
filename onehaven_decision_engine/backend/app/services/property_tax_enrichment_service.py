from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..domain.underwriting import normalize_tax_profile
from .public_tax_lookup_service import lookup_public_tax_record


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
        out = float(str(value).replace(",", "").strip())
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


def _walk_number_paths(obj: Any, candidate_keys: set[str]) -> float | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_norm = str(key).strip().lower()
            if key_norm in candidate_keys:
                parsed = _safe_float(value)
                if parsed is not None:
                    return parsed
            nested = _walk_number_paths(value, candidate_keys)
            if nested is not None:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _walk_number_paths(item, candidate_keys)
            if nested is not None:
                return nested
    return None

def _persist_discovered_tax_metadata(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    parcel_id: str | None,
    lookup_url: str | None,
) -> None:
    if not parcel_id and not lookup_url:
        return

    row = db.execute(
        text(
            """
            SELECT acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()

    if not row:
        return

    meta = _coerce_meta(row.get("acquisition_metadata_json"))
    changed = False

    if parcel_id and not meta.get("parcelId"):
        meta["parcelId"] = parcel_id
        changed = True

    if lookup_url and not meta.get("taxLookupUrl"):
        meta["taxLookupUrl"] = lookup_url
        changed = True

    if not changed:
        return

    db.execute(
        text(
            """
            UPDATE properties
            SET acquisition_metadata_json = :meta_json,
                updated_at = now()
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "meta_json": json.dumps(meta),
        },
    )

def _walk_int_paths(obj: Any, candidate_keys: set[str]) -> int | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_norm = str(key).strip().lower()
            if key_norm in candidate_keys:
                parsed = _safe_int(value)
                if parsed is not None:
                    return parsed
            nested = _walk_int_paths(value, candidate_keys)
            if nested is not None:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _walk_int_paths(item, candidate_keys)
            if nested is not None:
                return nested
    return None


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
                address,
                city,
                state,
                zip,
                county,
                normalized_address,
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
    direct = _coalesce_first_number(
        meta.get("propertyTax"),
        meta.get("propertyTaxes"),
        meta.get("annualPropertyTax"),
        meta.get("annualTaxes"),
        meta.get("taxAnnualAmount"),
        meta.get("taxesAnnual"),
        meta.get("taxAmount"),
        meta.get("annualTaxAmount"),
        meta.get("annual_tax_amount"),
        meta.get("property_tax_annual"),
    )
    if direct is not None:
        return direct

    return _walk_number_paths(
        meta,
        {
            "propertytax",
            "propertytaxes",
            "annualpropertytax",
            "annualtaxes",
            "taxannualamount",
            "taxesannual",
            "taxamount",
            "annualtaxamount",
            "annual_tax_amount",
            "property_tax_annual",
        },
    )


def _metadata_tax_rate(meta: dict[str, Any]) -> float | None:
    raw = _coalesce_first_number(
        meta.get("propertyTaxRate"),
        meta.get("taxRate"),
        meta.get("annualTaxRate"),
        meta.get("property_tax_rate"),
        meta.get("property_tax_rate_annual"),
    )
    if raw is None:
        raw = _walk_number_paths(
            meta,
            {
                "propertytaxrate",
                "taxrate",
                "annualtaxrate",
                "property_tax_rate",
                "property_tax_rate_annual",
            },
        )
    if raw is None:
        return None
    return raw / 100.0 if raw > 1 else raw


def _metadata_tax_year(meta: dict[str, Any]) -> int | None:
    direct = _safe_int(
        meta.get("taxYear")
        or meta.get("propertyTaxYear")
        or meta.get("assessmentYear")
        or meta.get("year")
    )
    if direct is not None:
        return direct

    return _walk_int_paths(
        meta,
        {
            "taxyear",
            "propertytaxyear",
            "assessmentyear",
        },
    )


def _compute_annual_amount_from_rate(*, asking_price: float | None, annual_rate: float | None) -> float | None:
    if asking_price is None or asking_price <= 0:
        return None
    if annual_rate is None or annual_rate <= 0:
        return None
    return round(float(asking_price) * float(annual_rate), 2)


def _compute_monthly_taxes(
    *,
    annual_amount: float | None,
    asking_price: float | None,
    annual_rate: float | None,
) -> float | None:
    resolved_annual = annual_amount
    if resolved_annual is None:
        resolved_annual = _compute_annual_amount_from_rate(
            asking_price=asking_price,
            annual_rate=annual_rate,
        )
    if resolved_annual is None:
        return None
    return round(resolved_annual / 12.0, 2)


def _source_rank(source: Any) -> int:
    normalized = str(source or "").strip().lower()
    if normalized in {
        "public_bsa_current_tax",
        "public_county_tax_portal",
        "assessor_api",
        "county_assessor_api",
        "parcel_tax_api",
    }:
        return 5
    if normalized in {"listing_metadata"}:
        return 4
    if normalized in {"county_rate_fallback"}:
        return 2
    if normalized in {"missing", ""}:
        return 0
    return 1


def _should_keep_existing_tax(row: dict[str, Any], *, force: bool) -> bool:
    if force:
        return False

    existing_annual = _safe_float(row.get("property_tax_annual"))
    existing_rate = _safe_float(row.get("property_tax_rate_annual"))
    existing_confidence = _safe_float(row.get("property_tax_confidence"))
    existing_source = row.get("property_tax_source")

    if existing_annual is None and existing_rate is None:
        return False

    if existing_confidence is not None and existing_confidence >= 0.75:
        return True

    if _source_rank(existing_source) >= 4:
        return True

    return False


def _resolve_tax_inputs_from_row(row: dict[str, Any]) -> dict[str, Any]:
    meta = _coerce_meta(row.get("acquisition_metadata_json"))
    resolved_price = _resolved_price(row)
    year = _metadata_tax_year(meta) or datetime.utcnow().year

    public_hit = lookup_public_tax_record(row=row, asking_price=resolved_price)
    if public_hit is not None and (public_hit.annual_amount is not None or public_hit.annual_rate is not None):
        return {
            "annual_amount": public_hit.annual_amount,
            "annual_rate": public_hit.annual_rate,
            "source": public_hit.source,
            "confidence": public_hit.confidence,
            "year": public_hit.year or year,
            "reason": public_hit.reason or "matched_public_tax_bill",
            "lookup_url": public_hit.lookup_url,
            "parcel_id": public_hit.parcel_id,
            "jurisdiction": public_hit.jurisdiction,
        }

    annual_amount = _metadata_tax_amount(meta)
    annual_rate = _metadata_tax_rate(meta)
    if annual_amount is not None or annual_rate is not None:
        return {
            "annual_amount": annual_amount,
            "annual_rate": annual_rate,
            "source": "listing_metadata",
            "confidence": 0.92,
            "year": year,
            "reason": "matched_listing_metadata",
            "lookup_url": None,
            "parcel_id": None,
            "jurisdiction": None,
        }

    county = str(row.get("county") or "").strip().lower()
    county_rate = COUNTY_TAX_RATE_FALLBACK.get(county)
    if county_rate is not None:
        return {
            "annual_amount": None,
            "annual_rate": county_rate,
            "source": "county_rate_fallback",
            "confidence": 0.55,
            "year": year,
            "reason": "used_county_rate_fallback",
            "lookup_url": None,
            "parcel_id": None,
            "jurisdiction": county or None,
        }

    return {
        "annual_amount": None,
        "annual_rate": None,
        "source": None,
        "confidence": None,
        "year": year,
        "reason": "tax_data_unavailable",
        "lookup_url": None,
        "parcel_id": None,
        "jurisdiction": None,
    }


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

    if _should_keep_existing_tax(row, force=force):
        profile = normalize_tax_profile(
            annual_amount=row.get("property_tax_annual"),
            annual_rate=row.get("property_tax_rate_annual"),
            asking_price=resolved_price,
            source=row.get("property_tax_source"),
            confidence=row.get("property_tax_confidence"),
            year=row.get("property_tax_year"),
        )
        monthly_taxes = _compute_monthly_taxes(
            annual_amount=profile.annual_amount,
            asking_price=resolved_price,
            annual_rate=profile.annual_rate,
        )
        return {
            "ok": True,
            "property_id": property_id,
            "resolved_price": resolved_price,
            "monthly_taxes": monthly_taxes,
            **profile.__dict__,
            "cached": True,
            "reason": "kept_existing_high_confidence_tax",
            "lookup_url": None,
            "parcel_id": None,
            "jurisdiction": None,
        }

    resolved = _resolve_tax_inputs_from_row(row)
    _persist_discovered_tax_metadata(
        db,
        org_id=org_id,
        property_id=property_id,
        parcel_id=resolved.get("parcel_id"),
        lookup_url=resolved.get("lookup_url"),
    )

    profile = normalize_tax_profile(
        annual_amount=resolved.get("annual_amount"),
        annual_rate=resolved.get("annual_rate"),
        asking_price=resolved_price,
        source=resolved.get("source"),
        confidence=resolved.get("confidence"),
        year=resolved.get("year"),
    )

    if profile.annual_amount is None and profile.annual_rate is None:
        return {
            "ok": False,
            "property_id": property_id,
            "resolved_price": resolved_price,
            "monthly_taxes": None,
            "annual_amount": None,
            "annual_rate": None,
            "source": "missing",
            "confidence": 0.0,
            "year": resolved.get("year"),
            "cached": False,
            "reason": resolved.get("reason") or "tax_data_unavailable",
            "lookup_url": resolved.get("lookup_url"),
            "parcel_id": resolved.get("parcel_id"),
            "jurisdiction": resolved.get("jurisdiction"),
        }

    monthly_taxes = _compute_monthly_taxes(
        annual_amount=profile.annual_amount,
        asking_price=resolved_price,
        annual_rate=profile.annual_rate,
    )

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
    db.commit()

    refreshed = _load_property_row(db, org_id=org_id, property_id=property_id) or {}

    return {
        "ok": True,
        "property_id": property_id,
        "resolved_price": resolved_price,
        "monthly_taxes": monthly_taxes,
        "annual_amount": refreshed.get("property_tax_annual"),
        "annual_rate": refreshed.get("property_tax_rate_annual"),
        "source": refreshed.get("property_tax_source"),
        "confidence": refreshed.get("property_tax_confidence"),
        "year": refreshed.get("property_tax_year"),
        "cached": False,
        "reason": resolved.get("reason") or "tax_enriched",
        "lookup_url": resolved.get("lookup_url"),
        "parcel_id": resolved.get("parcel_id"),
        "jurisdiction": resolved.get("jurisdiction"),
    }


def get_property_tax_context(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = _load_property_row(db, org_id=org_id, property_id=property_id) or {}
    annual_amount = _safe_float(row.get("property_tax_annual"))
    annual_rate = _safe_float(row.get("property_tax_rate_annual"))
    resolved_price = _resolved_price(row)
    return {
        "property_tax_annual": annual_amount,
        "property_tax_rate_annual": annual_rate,
        "property_tax_source": row.get("property_tax_source"),
        "property_tax_confidence": _safe_float(row.get("property_tax_confidence")),
        "property_tax_year": _safe_int(row.get("property_tax_year")),
        "monthly_taxes": _compute_monthly_taxes(
            annual_amount=annual_amount,
            asking_price=resolved_price,
            annual_rate=annual_rate,
        ),
        "resolved_price": resolved_price,
    }