from __future__ import annotations

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
