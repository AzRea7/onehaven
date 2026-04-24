#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

FILES = {
    "products/compliance/backend/src/services/product/compliance_brief_service.py": '''from __future__ import annotations

from typing import Any


def build_compliance_brief(*, db: Any, property_id: int, org_id: int | None = None) -> dict[str, Any]:
    """
    Product-facing compliance decision entrypoint.

    Routers should call this instead of stitching together policy, inspection,
    projection, document, and workflow data directly.
    """
    from onehaven_platform.backend.src.services.compliance_projection_service import (
        build_property_jurisdiction_blocker,
        build_workflow_summary,
    )

    return {
        "property_id": property_id,
        "org_id": org_id,
        "status": "needs_integration",
        "jurisdiction_blocker": build_property_jurisdiction_blocker(db=db, property_id=property_id, org_id=org_id),
        "workflow_summary": build_workflow_summary(db=db, property_id=property_id, org_id=org_id),
    }
''',

    "products/acquire/backend/src/services/product/acquisition_workspace_service.py": '''from __future__ import annotations

from typing import Any


def build_acquisition_workspace(*, db: Any, property_id: int, org_id: int | None = None) -> dict[str, Any]:
    """
    Product-facing acquisition workspace entrypoint.

    This should become the single service used by acquisition routers/pages for
    due diligence, missing docs, blockers, and close readiness.
    """
    return {
        "property_id": property_id,
        "org_id": org_id,
        "status": "needs_integration",
        "checklist": [],
        "blockers": [],
        "missing_documents": [],
        "close_readiness": "unknown",
    }
''',

    "products/intelligence/backend/src/services/product/deal_intelligence_service.py": '''from __future__ import annotations

from typing import Any


def build_deal_intelligence(*, db: Any, property_id: int, org_id: int | None = None) -> dict[str, Any]:
    """
    Product-facing investor/deal intelligence entrypoint.

    This should become the single service used by investor routers/pages for
    scoring, ranked deals, compliance drag, and buy/caution/avoid decisions.
    """
    return {
        "property_id": property_id,
        "org_id": org_id,
        "status": "needs_integration",
        "recommendation": "unknown",
        "score": None,
        "risks": [],
        "compliance_drag": None,
    }
''',

    "products/ops/backend/src/services/product/property_ops_summary_service.py": '''from __future__ import annotations

from typing import Any


def build_property_ops_summary(*, db: Any, property_id: int, org_id: int | None = None) -> dict[str, Any]:
    """
    Product-facing property operations entrypoint.

    This should become the single service used by ops routers/pages for urgent
    tasks, lease issues, inspection schedule, and turnover readiness.
    """
    return {
        "property_id": property_id,
        "org_id": org_id,
        "status": "needs_integration",
        "urgent_tasks": [],
        "lease_issues": [],
        "inspection_schedule": [],
        "turnover_readiness": "unknown",
    }
''',
}

INIT = "products/{product}/backend/src/services/product/__init__.py"

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

    for product in ["compliance", "acquire", "intelligence", "ops"]:
        init = root / INIT.format(product=product)
        if args.dry_run:
            print("[DRY RUN] touch", init)
            continue
        init.parent.mkdir(parents=True, exist_ok=True)
        init.touch(exist_ok=True)

    print("Phase 93 complete.")
    print({"written": written, "skipped": skipped, "dry_run": args.dry_run})

if __name__ == "__main__":
    main()