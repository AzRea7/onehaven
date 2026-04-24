from __future__ import annotations

from typing import Any

from ..base import TaxAdapter
from onehaven_platform.backend.src.models import TaxLookupResult


class PublicRecordTaxAdapter(TaxAdapter):
    adapter_key = "public_record_adapter"

    def supports(self, property_row: dict[str, Any]) -> bool:
        return bool(property_row.get("address") and property_row.get("state"))

    def lookup(
        self,
        *,
        property_row: dict[str, Any],
        asking_price: float | None,
    ) -> TaxLookupResult | None:
        # This is intentionally lightweight for now.
        # It gives the orchestrator a stable hook for future generic public-record families.
        return TaxLookupResult(
            annual_amount=None,
            annual_rate=None,
            source="public_record_lookup",
            confidence=0.0,
            year=None,
            status="missing",
            provider_key=self.adapter_key,
            lookup_url=None,
            parcel_id=property_row.get("parcel_id"),
            jurisdiction=property_row.get("county") or property_row.get("city"),
            reason="generic_public_lookup_not_yet_implemented",
            raw=None,
            cached=False,
        )