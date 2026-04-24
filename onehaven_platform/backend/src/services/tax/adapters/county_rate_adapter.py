from __future__ import annotations

from typing import Any

from ..base import TaxAdapter
from ..fallbacks import county_rate_fallback
from onehaven_platform.backend.src.models import TaxLookupResult


class CountyRateTaxAdapter(TaxAdapter):
    adapter_key = "county_rate_adapter"

    def supports(self, property_row: dict[str, Any]) -> bool:
        return True

    def lookup(
        self,
        *,
        property_row: dict[str, Any],
        asking_price: float | None,
    ) -> TaxLookupResult | None:
        result = county_rate_fallback(property_row=property_row, asking_price=asking_price)
        result.provider_key = self.adapter_key
        return result