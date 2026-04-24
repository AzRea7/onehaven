from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import Deal, Property
from products.intelligence.backend.src.services.property_price_resolution_service import resolve_prices_from_sources
from app.services.tax.service import resolve_property_tax


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        return out
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _resolved_price(
    *,
    payload_asking_price: float | None,
    payload_listing_price: float | None,
    persisted_listing_price: float | None,
) -> float | None:
    for value in (payload_asking_price, payload_listing_price, persisted_listing_price):
        parsed = _safe_float(value)
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _compute_monthly_taxes(
    *,
    annual_amount: float | None,
    asking_price: float | None,
    annual_rate: float | None,
) -> float | None:
    amount = _safe_float(annual_amount)
    if amount is not None and amount >= 0:
        return round(amount / 12.0, 2)

    price = _safe_float(asking_price)
    rate = _safe_float(annual_rate)
    if price is not None and price > 0 and rate is not None and rate >= 0:
        return round((price * rate) / 12.0, 2)

    return None


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
                listing_price,
                property_tax_annual,
                property_tax_rate_annual,
                property_tax_source,
                property_tax_confidence,
                property_tax_year,
                monthly_taxes,
                parcel_id,
                tax_lookup_status,
                tax_lookup_provider,
                tax_lookup_url,
                tax_last_verified_at
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()

    return dict(row) if row else None


def _safe_optional_query(fn, db: Session, default):
    """
    Run an optional DB lookup safely.
    If it fails, rollback the failed transaction state and return default.
    """
    try:
        return fn()
    except Exception:
        db.rollback()
        return default


def _load_price_sources(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> tuple[Property | None, Deal | None, dict[str, Any], dict[str, Any]]:
    prop = _safe_optional_query(
        lambda: (
            db.query(Property)
            .filter(Property.org_id == int(org_id), Property.id == int(property_id))
            .first()
        ),
        db,
        None,
    )

    deal = _safe_optional_query(
        lambda: (
            db.query(Deal)
            .filter(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
            .order_by(Deal.id.desc())
            .first()
        ),
        db,
        None,
    )

    snapshot: dict[str, Any] = _safe_optional_query(
        lambda: _load_snapshot_json(db, org_id=org_id, property_id=property_id),
        db,
        {},
    )

    acquisition_meta: dict[str, Any] = _safe_optional_query(
        lambda: _load_acquisition_meta_json(db, org_id=org_id, property_id=property_id),
        db,
        {},
    )

    return prop, deal, snapshot, acquisition_meta


def _load_snapshot_json(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    snap = db.execute(
        text(
            """
            SELECT snapshot_json
            FROM property_inventory_snapshots
            WHERE org_id = :org_id AND property_id = :property_id
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()

    if not snap or not snap.get("snapshot_json"):
        return {}

    raw = snap.get("snapshot_json")
    if isinstance(raw, dict):
        return raw

    return json.loads(raw)


def _load_acquisition_meta_json(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    meta = db.execute(
        text(
            """
            SELECT acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().first()

    if not meta or not meta.get("acquisition_metadata_json"):
        return {}

    raw = meta.get("acquisition_metadata_json")
    if isinstance(raw, dict):
        return raw

    return json.loads(raw)


def enrich_property_tax(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    force: bool = False,
) -> dict[str, Any]:
    try:
        row = _load_property_row(db, org_id=org_id, property_id=property_id)
        if not row:
            return {
                "ok": False,
                "property_id": int(property_id),
                "reason": "property_not_found",
            }

        prop, deal, snapshot, acquisition_meta = _load_price_sources(
            db,
            org_id=org_id,
            property_id=property_id,
        )

        resolved_prices = resolve_prices_from_sources(
            prop=prop,
            deal=deal,
            snapshot=snapshot,
            acquisition_meta=acquisition_meta,
        )

        payload_asking_price = resolved_prices.get("asking_price")
        payload_listing_price = resolved_prices.get("listing_price")

        resolved_price = _resolved_price(
            payload_asking_price=payload_asking_price,
            payload_listing_price=payload_listing_price,
            persisted_listing_price=row.get("listing_price"),
        )

        resolved = resolve_property_tax(
            db,
            property_row=row,
            asking_price=resolved_price,
            force=force,
        )

        monthly_taxes = _compute_monthly_taxes(
            annual_amount=resolved.annual_amount,
            asking_price=resolved_price,
            annual_rate=resolved.annual_rate,
        )

        db.execute(
            text(
                """
                UPDATE properties
                SET listing_price = COALESCE(:listing_price, listing_price),
                    property_tax_annual = :annual_amount,
                    property_tax_rate_annual = :annual_rate,
                    property_tax_source = :source,
                    property_tax_confidence = :confidence,
                    property_tax_year = :tax_year,
                    monthly_taxes = :monthly_taxes,
                    parcel_id = :parcel_id,
                    tax_lookup_status = :tax_lookup_status,
                    tax_lookup_provider = :tax_lookup_provider,
                    tax_lookup_url = :tax_lookup_url,
                    tax_last_verified_at = now(),
                    updated_at = now()
                WHERE org_id = :org_id AND id = :property_id
                """
            ),
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "listing_price": payload_listing_price,
                "annual_amount": resolved.annual_amount,
                "annual_rate": resolved.annual_rate,
                "source": resolved.source,
                "confidence": resolved.confidence,
                "tax_year": resolved.year,
                "monthly_taxes": monthly_taxes,
                "parcel_id": resolved.parcel_id,
                "tax_lookup_status": resolved.status,
                "tax_lookup_provider": resolved.provider_key,
                "tax_lookup_url": resolved.lookup_url,
            },
        )
        db.commit()

        refreshed = _load_property_row(db, org_id=org_id, property_id=property_id) or {}

        return {
            "ok": True,
            "property_id": int(property_id),
            "resolved_price": resolved_price,
            "monthly_taxes": refreshed.get("monthly_taxes"),
            "annual_amount": refreshed.get("property_tax_annual"),
            "annual_rate": refreshed.get("property_tax_rate_annual"),
            "source": refreshed.get("property_tax_source"),
            "confidence": refreshed.get("property_tax_confidence"),
            "year": refreshed.get("property_tax_year"),
            "cached": bool(resolved.cached),
            "status": refreshed.get("tax_lookup_status"),
            "provider": refreshed.get("tax_lookup_provider"),
            "reason": resolved.reason,
            "lookup_url": refreshed.get("tax_lookup_url"),
            "parcel_id": refreshed.get("parcel_id"),
            "jurisdiction": resolved.jurisdiction,
            "listing_price": refreshed.get("listing_price"),
            "payload_asking_price": payload_asking_price,
            "payload_listing_price": payload_listing_price,
        }
    except Exception:
        db.rollback()
        raise


def get_property_tax_context(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        row = _load_property_row(db, org_id=org_id, property_id=property_id) or {}

        prop, deal, snapshot, acquisition_meta = _load_price_sources(
            db,
            org_id=org_id,
            property_id=property_id,
        )

        resolved_prices = resolve_prices_from_sources(
            prop=prop,
            deal=deal,
            snapshot=snapshot,
            acquisition_meta=acquisition_meta,
        )

        payload_asking_price = resolved_prices.get("asking_price")
        payload_listing_price = resolved_prices.get("listing_price")

        annual_amount = _safe_float(row.get("property_tax_annual"))
        annual_rate = _safe_float(row.get("property_tax_rate_annual"))
        resolved_price = _resolved_price(
            payload_asking_price=payload_asking_price,
            payload_listing_price=payload_listing_price,
            persisted_listing_price=row.get("listing_price"),
        )

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
            "payload_asking_price": payload_asking_price,
            "payload_listing_price": payload_listing_price,
            "parcel_id": row.get("parcel_id"),
            "tax_lookup_status": row.get("tax_lookup_status"),
            "tax_lookup_provider": row.get("tax_lookup_provider"),
            "tax_lookup_url": row.get("tax_lookup_url"),
            "tax_last_verified_at": row.get("tax_last_verified_at"),
        }
    except Exception:
        db.rollback()
        raise