from __future__ import annotations

from typing import Any, Protocol

from onehaven_platform.backend.src.models import TaxLookupResult


class TaxAdapter(Protocol):
    adapter_key: str

    def supports(self, property_row: dict[str, Any]) -> bool:
        ...

    def lookup(
        self,
        *,
        property_row: dict[str, Any],
        asking_price: float | None,
    ) -> TaxLookupResult | None:
        ...