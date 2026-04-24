#!/usr/bin/env python3
from pathlib import Path

FILES = {
    "onehaven_platform/backend/src/adapters/intelligence_adapter.py": '''from __future__ import annotations

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
''',

    "onehaven_platform/backend/src/adapters/acquire_adapter.py": '''from __future__ import annotations

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
''',

    "onehaven_platform/backend/src/adapters/ops_adapter.py": '''from __future__ import annotations

from typing import Any


def build_inventory_snapshots_for_scope(*args: Any, **kwargs: Any) -> Any:
    from products.ops.backend.src.services.properties.inventory_snapshot_service import build_inventory_snapshots_for_scope as impl
    return impl(*args, **kwargs)
''',
}

root = Path(".").resolve()

for rel, content in FILES.items():
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print("repaired", path)

print("Phase 99 complete.")