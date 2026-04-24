from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from onehaven_platform.backend.src.models import TaxLookupResult


def get_cached_tax_result(
    *,
    property_row: dict[str, Any],
    force: bool,
    max_age_days: int = 30,
) -> TaxLookupResult | None:
    if force:
        return None

    confidence = property_row.get("property_tax_confidence")
    source = property_row.get("property_tax_source")
    annual_amount = property_row.get("property_tax_annual")
    annual_rate = property_row.get("property_tax_rate_annual")
    year = property_row.get("property_tax_year")
    parcel_id = property_row.get("parcel_id")
    status = property_row.get("tax_lookup_status")
    provider = property_row.get("tax_lookup_provider")
    lookup_url = property_row.get("tax_lookup_url")
    last_verified = property_row.get("tax_last_verified_at")

    try:
        confidence_f = float(confidence) if confidence is not None else 0.0
    except Exception:
        confidence_f = 0.0

    if confidence_f < 0.85:
        return None

    if last_verified:
        try:
            if isinstance(last_verified, str):
                verified_at = datetime.fromisoformat(last_verified.replace("Z", "+00:00"))
            else:
                verified_at = last_verified
            if datetime.utcnow() - verified_at > timedelta(days=max_age_days):
                return None
        except Exception:
            return None

    if annual_amount is None and annual_rate is None:
        return None

    return TaxLookupResult(
        annual_amount=float(annual_amount) if annual_amount is not None else None,
        annual_rate=float(annual_rate) if annual_rate is not None else None,
        source=str(source or "cached_tax"),
        confidence=confidence_f,
        year=int(year) if year is not None else None,
        status=str(status or "cached"),
        provider_key=str(provider or "cached"),
        lookup_url=str(lookup_url) if lookup_url else None,
        parcel_id=str(parcel_id) if parcel_id else None,
        jurisdiction=property_row.get("county") or property_row.get("city"),
        reason="reused_high_confidence_cached_tax",
        raw=None,
        cached=True,
    )