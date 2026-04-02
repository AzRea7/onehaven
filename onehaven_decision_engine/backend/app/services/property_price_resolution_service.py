from __future__ import annotations

from typing import Any


def safe_price(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if out <= 0:
            return None
        return out
    except Exception:
        return None


def coalesce_price(*values: Any) -> float | None:
    for value in values:
        parsed = safe_price(value)
        if parsed is not None:
            return parsed
    return None


def asking_price_from_objects(prop: Any, deal: Any | None) -> float | None:
    if deal is not None:
        for attr in ("asking_price", "list_price", "price", "offer_price", "purchase_price"):
            value = getattr(deal, attr, None)
            parsed = safe_price(value)
            if parsed is not None:
                return parsed

    if prop is not None:
        for attr in ("asking_price", "list_price", "price"):
            value = getattr(prop, attr, None)
            parsed = safe_price(value)
            if parsed is not None:
                return parsed

    return None


def resolve_prices_from_sources(
    *,
    prop: Any,
    deal: Any | None = None,
    snapshot: dict[str, Any] | None = None,
    acquisition_meta: dict[str, Any] | None = None,
) -> dict[str, float | None]:
    snapshot = snapshot or {}
    acquisition_meta = acquisition_meta or {}

    prop_asking = asking_price_from_objects(prop, deal)
    prop_listing = safe_price(getattr(prop, "listing_price", None)) if prop is not None else None

    resolved_asking_price = coalesce_price(
        snapshot.get("asking_price"),
        prop_asking,
        acquisition_meta.get("listing_price"),
        snapshot.get("listing_price"),
        prop_listing,
    )

    resolved_listing_price = coalesce_price(
        acquisition_meta.get("listing_price"),
        snapshot.get("listing_price"),
        prop_listing,
        snapshot.get("asking_price"),
        prop_asking,
    )

    return {
        "asking_price": resolved_asking_price,
        "listing_price": resolved_listing_price,
    }