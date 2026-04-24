from __future__ import annotations

from typing import Any

from products.intelligence.backend.src.services.public_tax_lookup_service import lookup_public_tax_record

from ..base import TaxAdapter
from app.models import TaxLookupResult


class BsaTaxAdapter(TaxAdapter):
    adapter_key = "bsa_adapter"

    def supports(self, property_row: dict[str, Any]) -> bool:
        state = str(property_row.get("state") or "").strip().upper()
        county = str(property_row.get("county") or "").strip().lower()
        return state == "MI" and county in {"wayne", "oakland", "macomb"}

    def lookup(
        self,
        *,
        property_row: dict[str, Any],
        asking_price: float | None,
    ) -> TaxLookupResult | None:
        resolved = lookup_public_tax_record(
            address=property_row.get("address"),
            city=property_row.get("city"),
            state=property_row.get("state"),
            zip_code=property_row.get("zip"),
            county=property_row.get("county"),
            asking_price=asking_price,
            parcel_id=property_row.get("parcel_id"),
            lookup_url=property_row.get("tax_lookup_url"),
        )

        if not resolved:
            return None

        found = bool(resolved.get("found"))
        return TaxLookupResult(
            annual_amount=resolved.get("annual_amount"),
            annual_rate=resolved.get("annual_rate"),
            source=str(resolved.get("source") or "bsa_public_record"),
            confidence=float(resolved.get("confidence") or (0.9 if found else 0.0)),
            year=resolved.get("year"),
            status="authoritative_public_record" if found else "missing",
            provider_key=self.adapter_key,
            lookup_url=resolved.get("lookup_url"),
            parcel_id=resolved.get("parcel_id"),
            jurisdiction=resolved.get("jurisdiction"),
            reason=resolved.get("reason"),
            raw=resolved.get("raw"),
            cached=False,
        )