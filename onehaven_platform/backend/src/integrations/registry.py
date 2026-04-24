from __future__ import annotations

from typing import Any

from .base import TaxAdapter
from .adapters.bsa_adapter import BsaTaxAdapter
from .adapters.public_record_adapter import PublicRecordTaxAdapter
from .adapters.county_rate_adapter import CountyRateTaxAdapter


def get_adapters(property_row: dict[str, Any]) -> list[TaxAdapter]:
    adapters: list[TaxAdapter] = [
        BsaTaxAdapter(),
        PublicRecordTaxAdapter(),
        CountyRateTaxAdapter(),
    ]
    return [adapter for adapter in adapters if adapter.supports(property_row)]