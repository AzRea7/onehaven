from __future__ import annotations

from typing import Any


def derive_photo_kind(url: str) -> str:
    u = (url or "").lower()
    if any(x in u for x in ["front", "exterior", "outside", "street"]):
        return "exterior"
    if any(x in u for x in ["kitchen", "bath", "bed", "living", "interior", "inside"]):
        return "interior"
    return "unknown"


def canonical_listing_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "external_record_id": str(row.get("external_record_id") or row.get("listing_id") or row.get("zpid") or ""),
        "external_url": row.get("external_url") or row.get("url"),
        "address": row.get("address") or "",
        "city": row.get("city") or "",
        "state": row.get("state") or "MI",
        "zip": row.get("zip") or "",
        "bedrooms": int(row.get("bedrooms") or 0),
        "bathrooms": float(row.get("bathrooms") or 1),
        "square_feet": row.get("square_feet"),
        "year_built": row.get("year_built"),
        "property_type": row.get("property_type") or "single_family",
        "asking_price": float(row.get("asking_price") or 0),
        "estimated_purchase_price": row.get("estimated_purchase_price"),
        "rehab_estimate": float(row.get("rehab_estimate") or 0),
        "market_rent_estimate": row.get("market_rent_estimate"),
        "section8_fmr": row.get("section8_fmr"),
        "approved_rent_ceiling": row.get("approved_rent_ceiling"),
        "inventory_count": row.get("inventory_count"),
        "photos": row.get("photos") or [],
        "raw": row.get("raw") or {},
    }
