#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


FILES = {
    "onehaven_platform/backend/src/adapters/__init__.py": '''\
"""\nPlatform adapter boundary package.\n\nAdapters are the only platform-layer modules allowed to call into product-owned\nimplementations during the migration period. Long term, these should become\nports/interfaces with injected implementations.\n"""\n''',

    "onehaven_platform/backend/src/adapters/compliance_adapter.py": '''\
from __future__ import annotations

from typing import Any


def build_property_jurisdiction_blocker(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_property_jurisdiction_blocker as impl,
    )
    return impl(*args, **kwargs)


def build_workflow_summary(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.workflow_gate_service import (
        build_workflow_summary as impl,
    )
    return impl(*args, **kwargs)


def build_property_document_stack(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_document_service import (
        build_property_document_stack as impl,
    )
    return impl(*args, **kwargs)


def analyze_property_photos_for_compliance(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        analyze_property_photos_for_compliance as impl,
    )
    return impl(*args, **kwargs)


def create_compliance_tasks_from_photo_analysis(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.compliance_photo_analysis_service import (
        create_compliance_tasks_from_photo_analysis as impl,
    )
    return impl(*args, **kwargs)


def evaluate_trust(*args: Any, **kwargs: Any) -> Any:
    from products.compliance.backend.src.services.trust_service import evaluate_trust as impl
    return impl(*args, **kwargs)
''',

    "onehaven_platform/backend/src/adapters/intelligence_adapter.py": '''\
from __future__ import annotations

from typing import Any


def get_risk_score(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.risk_scoring import get_risk_score as impl
    return impl(*args, **kwargs)


def resolve_property_tax_context(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.property_tax_enrichment_service import (
        get_property_tax_context as impl,
    )
    return impl(*args, **kwargs)


def resolve_property_insurance_context(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.property_insurance_enrichment_service import (
        get_property_insurance_context as impl,
    )
    return impl(*args, **kwargs)


def resolve_rentcast_client(*args: Any, **kwargs: Any) -> Any:
    from products.intelligence.backend.src.services.rentcast_service import RentCastClient
    return RentCastClient(*args, **kwargs)
''',

    "onehaven_platform/backend/src/adapters/acquire_adapter.py": '''\
from __future__ import annotations

from typing import Any


def list_active_ingestion_sources(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_source_service import (
        list_active_ingestion_sources as impl,
    )
    return impl(*args, **kwargs)


def execute_ingestion_run(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_run_execute import (
        execute_ingestion_run as impl,
    )
    return impl(*args, **kwargs)


def build_ingestion_runtime_payload(*args: Any, **kwargs: Any) -> Any:
    from products.acquire.backend.src.services.ingestion_scheduler_service import (
        build_runtime_payload as impl,
    )
    return impl(*args, **kwargs)
''',

    "onehaven_platform/backend/src/adapters/ops_adapter.py": '''\
from __future__ import annotations

from typing import Any


def build_inventory_snapshots_for_scope(*args: Any, **kwargs: Any) -> Any:
    from products.ops.backend.src.services.properties.inventory_snapshot_service import (
        build_inventory_snapshots_for_scope as impl,
    )
    return impl(*args, **kwargs)
''',
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    written = 0
    skipped = 0

    for rel, content in FILES.items():
        path = root / rel
        if path.exists() and not args.overwrite:
            skipped += 1
            continue

        if args.dry_run:
            print("[DRY RUN] write", path)
            written += 1
            continue

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written += 1

    print("Phase 97 complete.")
    print({"written": written, "skipped": skipped, "dry_run": args.dry_run})


if __name__ == "__main__":
    main()