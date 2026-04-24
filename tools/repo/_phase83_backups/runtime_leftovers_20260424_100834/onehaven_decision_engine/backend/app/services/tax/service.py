from __future__ import annotations

from sqlalchemy.orm import Session

from app.models import TaxLookupResult
from .cache import get_cached_tax_result
from .registry import get_adapters


def resolve_property_tax(
    db: Session,
    *,
    property_row: dict,
    asking_price: float | None,
    force: bool = False,
) -> TaxLookupResult:
    cached = get_cached_tax_result(property_row=property_row, force=force)
    if cached is not None:
        return cached

    adapters = get_adapters(property_row)

    best_result: TaxLookupResult | None = None
    best_score = float("-inf")

    for adapter in adapters:
        try:
            result = adapter.lookup(
                property_row=property_row,
                asking_price=asking_price,
            )
        except Exception as exc:
            result = TaxLookupResult(
                annual_amount=None,
                annual_rate=None,
                source="lookup_error",
                confidence=0.0,
                year=None,
                status="error",
                provider_key=adapter.adapter_key,
                lookup_url=None,
                parcel_id=property_row.get("parcel_id"),
                jurisdiction=property_row.get("county") or property_row.get("city"),
                reason=f"{adapter.adapter_key}_failed:{type(exc).__name__}",
                raw={"error": str(exc)},
                cached=False,
            )

        if result is None:
            continue

        score = float(result.confidence or 0.0)
        if result.annual_amount is not None:
            score += 100.0
        elif result.annual_rate is not None:
            score += 10.0
        elif str(result.status or "").lower() == "missing":
            score -= 100.0

        if score > best_score:
            best_result = result
            best_score = score

        if result.annual_amount is not None and result.confidence >= 0.85:
            return result

    if best_result is not None:
        return best_result

    return TaxLookupResult(
        annual_amount=None,
        annual_rate=None,
        source="missing",
        confidence=0.0,
        year=None,
        status="missing",
        provider_key="none",
        lookup_url=None,
        parcel_id=property_row.get("parcel_id"),
        jurisdiction=property_row.get("county") or property_row.get("city"),
        reason="no_adapter_returned_result",
        raw=None,
        cached=False,
    )