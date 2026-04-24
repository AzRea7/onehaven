from __future__ import annotations

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
