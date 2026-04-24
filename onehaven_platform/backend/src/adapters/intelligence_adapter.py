from __future__ import annotations

from typing import Any


def classify_deal_candidate(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import classify_deal_candidate as impl
    return impl(*args, **kwargs)


def get_risk_score(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import get_risk_score as impl
    return impl(*args, **kwargs)


def get_property_tax_context(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.property_tax_enrichment_service import get_property_tax_context as impl
    return impl(*args, **kwargs)


def get_property_insurance_context(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.property_insurance_enrichment_service import get_property_insurance_context as impl
    return impl(*args, **kwargs)


def RentCastClient(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.rentcast_service import RentCastClient as impl
    return impl(*args, **kwargs)


def RentCastSaleListingResult(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.rentcast_service import RentCastSaleListingResult as impl
    return impl(*args, **kwargs)


def compute_property_risk(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import compute_property_risk as impl
    return impl(*args, **kwargs)


def explain_risk_score(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import explain_risk_score as impl
    return impl(*args, **kwargs)


def build_risk_payload(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import build_risk_payload as impl
    return impl(*args, **kwargs)

def __getattr__(name: str) -> Any:
    import importlib

    modules = [
        "products.intelligence.backend.src.services.risk_scoring",
        "products.intelligence.backend.src.services.rentcast_service",
        "products.intelligence.backend.src.services.market_catalog_service",
        "products.intelligence.backend.src.services.market_sync_service",
        "products.intelligence.backend.src.services.property_tax_enrichment_service",
        "products.intelligence.backend.src.services.property_insurance_enrichment_service",
        "products.intelligence.backend.src.services.property_price_resolution_service",
        "products.intelligence.backend.src.services.zillow_photo_source",
        "products.intelligence.backend.src.services.hud_fmr_service",
        "products.intelligence.backend.src.services.fmr",
        "products.intelligence.backend.src.services.rent_refresh_queue_service",
    ]

    for module_name in modules:
        module = importlib.import_module(module_name)
        if hasattr(module, name):
            return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
