from __future__ import annotations

from typing import Any

from app.models import TaxLookupResult


def county_rate_fallback(
    *,
    property_row: dict[str, Any],
    asking_price: float | None,
) -> TaxLookupResult:
    county = str(property_row.get("county") or "").strip().lower()
    state = str(property_row.get("state") or "").strip().upper()

    # very small starter table; expand over time
    county_rates: dict[tuple[str, str], float] = {
        ("MI", "wayne"): 0.024,
        ("MI", "oakland"): 0.0175,
        ("MI", "macomb"): 0.0205,
    }

    state_default_rates: dict[str, float] = {
        "MI": 0.0185,
    }

    annual_rate = county_rates.get((state, county)) or state_default_rates.get(state)

    if asking_price and annual_rate:
        annual_amount = round(float(asking_price) * float(annual_rate), 2)
        return TaxLookupResult(
            annual_amount=annual_amount,
            annual_rate=annual_rate,
            source="county_rate_fallback",
            confidence=0.4 if county else 0.3,
            year=None,
            status="county_rate_fallback",
            provider_key="county_rate_adapter",
            lookup_url=None,
            parcel_id=property_row.get("parcel_id"),
            jurisdiction=property_row.get("county") or property_row.get("city"),
            reason="estimated_from_effective_rate",
            raw={"state": state, "county": county},
        )

    return TaxLookupResult(
        annual_amount=None,
        annual_rate=annual_rate,
        source="missing",
        confidence=0.0,
        year=None,
        status="missing",
        provider_key="county_rate_adapter",
        lookup_url=None,
        parcel_id=property_row.get("parcel_id"),
        jurisdiction=property_row.get("county") or property_row.get("city"),
        reason="no_price_or_rate_available",
        raw={"state": state, "county": county},
    )