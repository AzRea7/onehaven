from __future__ import annotations

from typing import Any


def list_active_ingestion_sources(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_source_service import list_active_ingestion_sources as impl
    return impl(*args, **kwargs)


def execute_ingestion_run(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_execute import execute_ingestion_run as impl
    return impl(*args, **kwargs)


def build_runtime_payload(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_scheduler_service import build_runtime_payload as impl
    return impl(*args, **kwargs)


def execute_source_sync(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_execute import execute_source_sync as impl
    return impl(*args, **kwargs)


def execute_post_ingestion_pipeline(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_enrichment_service import execute_post_ingestion_pipeline as impl
    return impl(*args, **kwargs)


def start_ingestion_run(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_service import start_ingestion_run as impl
    return impl(*args, **kwargs)


def get_ingestion_run(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_service import get_ingestion_run as impl
    return impl(*args, **kwargs)


def list_ingestion_runs(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_service import list_ingestion_runs as impl
    return impl(*args, **kwargs)


def ensure_default_manual_sources(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_source_service import ensure_default_manual_sources as impl
    return impl(*args, **kwargs)

def __getattr__(name: str) -> Any:
    import importlib

    modules = [
        "products.acquire.backend.src.services.ingestion_run_execute",
        "products.acquire.backend.src.services.ingestion_run_service",
        "products.acquire.backend.src.services.ingestion_scheduler_service",
        "products.acquire.backend.src.services.ingestion_source_service",
        "products.acquire.backend.src.services.ingestion_enrichment_service",
    ]

    for module_name in modules:
        module = importlib.import_module(module_name)
        if hasattr(module, name):
            return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
