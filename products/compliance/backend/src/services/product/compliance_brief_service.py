from __future__ import annotations

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
