from __future__ import annotations

import os
from typing import Any, Optional

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..models import Property, RentAssumption
from .rentcast_service import RentCastClient, persist_rentcast_comps_and_get_median


def derive_photo_kind(url: str) -> str:
    u = (url or "").lower()
    if any(x in u for x in ["front", "exterior", "outside", "street"]):
        return "exterior"
    if any(x in u for x in ["kitchen", "bath", "bed", "living", "interior", "inside"]):
        return "interior"
    return "unknown"


def normalize_photos(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not raw:
        return out

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                url = item.strip()
                if not url:
                    continue
                out.append({"url": url, "kind": derive_photo_kind(url)})
                continue

            if isinstance(item, dict):
                url = str(
                    item.get("url")
                    or item.get("href")
                    or item.get("photoUrl")
                    or item.get("src")
                    or ""
                ).strip()
                if not url:
                    continue

                kind = str(item.get("kind") or "").strip() or derive_photo_kind(url)
                out.append({"url": url, "kind": kind})
    return out


def canonical_listing_payload(row: dict[str, Any]) -> dict[str, Any]:
    row = row or {}
    price = row.get("asking_price") or row.get("price") or row.get("listPrice") or 0
    market_rent = (
        row.get("market_rent_estimate")
        or row.get("rent_estimate")
        or row.get("rentEstimate")
        or row.get("predictedRent")
    )

    return {
        "external_record_id": str(
            row.get("external_record_id")
            or row.get("listing_id")
            or row.get("listingId")
            or row.get("id")
            or row.get("zpid")
            or ""
        ).strip(),
        "external_url": row.get("external_url") or row.get("listingUrl") or row.get("url"),
        "address": str(row.get("address") or row.get("formattedAddress") or "").strip(),
        "city": str(row.get("city") or "").strip(),
        "county": str(row.get("county") or "").strip() or None,
        "state": str(row.get("state") or "MI").strip() or "MI",
        "zip": str(row.get("zip") or row.get("zipCode") or row.get("postalCode") or "").strip(),
        "bedrooms": int(row.get("bedrooms") or 0),
        "bathrooms": float(row.get("bathrooms") or 1),
        "square_feet": row.get("square_feet") or row.get("squareFootage") or row.get("livingArea"),
        "year_built": row.get("year_built") or row.get("yearBuilt"),
        "property_type": row.get("property_type") or row.get("propertyType") or "single_family",
        "asking_price": float(price or 0),
        "estimated_purchase_price": row.get("estimated_purchase_price") or price,
        "rehab_estimate": float(row.get("rehab_estimate") or 0),
        "market_rent_estimate": market_rent,
        "section8_fmr": row.get("section8_fmr"),
        "approved_rent_ceiling": row.get("approved_rent_ceiling"),
        "inventory_count": row.get("inventory_count"),
        "photos": normalize_photos(row.get("photos")),
        "raw": row.get("raw") or row,
    }


def build_post_import_actions() -> list[dict[str, str]]:
    return [
        {"key": "geo", "label": "Geocode and enrich location"},
        {"key": "risk", "label": "Update crime and zone risk"},
        {"key": "rent", "label": "Refresh rent assumptions"},
        {"key": "evaluate", "label": "Run underwriting"},
        {"key": "workflow", "label": "Refresh workflow gates"},
    ]


def get_rentcast_api_key() -> Optional[str]:
    key = (
        os.getenv("RENTCAST_INGESTION_API_KEY")
        or os.getenv("RENTCAST_API_KEY")
        or ""
    ).strip()
    return key or None


def get_google_maps_api_key() -> Optional[str]:
    key = (
        os.getenv("GOOGLE_MAPS_API_KEY")
        or os.getenv("GOOGLE_GEOCODING_API_KEY")
        or ""
    ).strip()
    return key or None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def refresh_property_rent_assumptions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    rentcast_api_key: Optional[str] = None,
    replace_existing_comps: bool = True,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )
    if prop is None:
        return {"ok": False, "error": "property_not_found"}

    address = str(getattr(prop, "address", "") or "").strip()
    city = str(getattr(prop, "city", "") or "").strip()
    state = str(getattr(prop, "state", "") or "").strip() or "MI"
    zip_code = str(getattr(prop, "zip", "") or "").strip()
    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    bathrooms = float(getattr(prop, "bathrooms", 0) or 0)
    square_feet = getattr(prop, "square_feet", None)

    if not address or not city or not state or not zip_code:
        return {
            "ok": False,
            "error": "missing_address_fields",
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
        }

    api_key = (rentcast_api_key or get_rentcast_api_key() or "").strip()
    if not api_key:
        return {"ok": False, "error": "missing_rentcast_api_key"}

    client = RentCastClient(api_key)
    payload = client.rent_estimate(
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        square_feet=int(square_feet) if square_feet is not None else None,
    )

    market_rent_estimate = client.pick_estimated_rent(payload)
    rent_reasonableness_comp = persist_rentcast_comps_and_get_median(
        db,
        property_id=int(property_id),
        payload=payload,
        replace_existing=replace_existing_comps,
    )

    existing = db.scalar(
        select(RentAssumption).where(
            RentAssumption.org_id == int(org_id),
            RentAssumption.property_id == int(property_id),
        ).order_by(desc(RentAssumption.id))
    )

    created = False
    if existing is None:
        existing = RentAssumption(
            org_id=int(org_id),
            property_id=int(property_id),
        )
        created = True

    if market_rent_estimate is not None:
        existing.market_rent_estimate = float(market_rent_estimate)

    if hasattr(existing, "rent_reasonableness_comp") and rent_reasonableness_comp is not None:
        existing.rent_reasonableness_comp = float(rent_reasonableness_comp)

    approved_existing = _safe_float(getattr(existing, "approved_rent_ceiling", None))
    fmr_existing = _safe_float(getattr(existing, "section8_fmr", None))
    rr_existing = _safe_float(getattr(existing, "rent_reasonableness_comp", None))

    approved_candidates: list[float] = []
    if rr_existing is not None and rr_existing > 0:
        approved_candidates.append(rr_existing)
    if fmr_existing is not None and fmr_existing > 0:
        approved_candidates.append(fmr_existing)

    computed_ceiling = min(approved_candidates) if approved_candidates else None
    if approved_existing is None and computed_ceiling is not None:
        existing.approved_rent_ceiling = float(computed_ceiling)

    db.add(existing)
    db.commit()
    db.refresh(existing)

    return {
        "ok": True,
        "created": created,
        "property_id": int(property_id),
        "market_rent_estimate": _safe_float(getattr(existing, "market_rent_estimate", None)),
        "rent_reasonableness_comp": _safe_float(getattr(existing, "rent_reasonableness_comp", None))
        if hasattr(existing, "rent_reasonableness_comp")
        else rent_reasonableness_comp,
        "approved_rent_ceiling": _safe_float(getattr(existing, "approved_rent_ceiling", None)),
        "section8_fmr": _safe_float(getattr(existing, "section8_fmr", None)),
    }