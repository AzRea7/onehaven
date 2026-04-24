from __future__ import annotations

from typing import Any


_EXPORT_MODULES = [
    "products.compliance.backend.src.services.workflow_gate_service",
    "products.compliance.backend.src.services.compliance_document_service",
    "products.compliance.backend.src.services.compliance_photo_analysis_service",
    "products.compliance.backend.src.services.compliance_service",
    "products.compliance.backend.src.services.property_compliance_resolution_service",
]


def __getattr__(name: str) -> Any:
    import importlib

    for module_name in _EXPORT_MODULES:
        module = importlib.import_module(module_name)
        if hasattr(module, name):
            return getattr(module, name)

    raise AttributeError(f"module 'products.compliance.backend.src.services' has no attribute {name!r}")


__all__ = [
    # resolved lazily through __getattr__
]