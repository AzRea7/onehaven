from __future__ import annotations

from typing import Any


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
                    or ""
                ).strip()
                if not url:
                    continue

                kind = str(item.get("kind") or "").strip() or derive_photo_kind(url)
                out.append({"url": url, "kind": kind})
    return out


def canonical_listing_payload(row: dict[str, Any]) -> dict[str, Any]:
    row = row or {}

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
        "state": str(row.get("state") or "MI").strip() or "MI",
        "zip": str(row.get("zip") or row.get("zipCode") or row.get("postalCode") or "").strip(),
        "bedrooms": int(row.get("bedrooms") or 0),
        "bathrooms": float(row.get("bathrooms") or 1),
        "square_feet": row.get("square_feet") or row.get("squareFootage") or row.get("livingArea"),
        "year_built": row.get("year_built") or row.get("yearBuilt"),
        "property_type": row.get("property_type") or row.get("propertyType") or "single_family",
        "asking_price": float(row.get("asking_price") or row.get("price") or 0),
        "estimated_purchase_price": row.get("estimated_purchase_price") or row.get("price"),
        "rehab_estimate": float(row.get("rehab_estimate") or 0),
        "market_rent_estimate": row.get("market_rent_estimate"),
        "section8_fmr": row.get("section8_fmr"),
        "approved_rent_ceiling": row.get("approved_rent_ceiling"),
        "inventory_count": row.get("inventory_count"),
        "photos": normalize_photos(row.get("photos")),
        "raw": row.get("raw") or row,
    }
