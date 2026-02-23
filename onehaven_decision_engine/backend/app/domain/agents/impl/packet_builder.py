# onehaven_decision_engine/backend/app/domain/agents/impl/packet_builder.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.services.jurisdiction_profile_service import resolve_jurisdiction_profile


def run_packet_builder(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Uses JurisdictionProfile.workflow_steps_json to generate a “packet readiness” checklist.
    This is your bridge from policy truth → operations reality.
    """
    if not property_id:
        return {"summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    jp = resolve_jurisdiction_profile(db, org_id=org_id, prop=prop)
    steps = jp["effective_steps"]

    # Convert steps into “recommended tasks”
    actions: list[dict[str, Any]] = []
    for i, s in enumerate(steps[:50]):
        title = s.get("title") or f"Packet step {i+1}"
        actions.append(
            {
                "op": "recommend",
                "entity_type": "WorkflowChecklistItem",
                "entity_id": None,
                "payload": {
                    "property_id": prop.id,
                    "title": title,
                    "category": s.get("category") or "packet",
                    "details": s,
                },
                "priority": s.get("priority") or "medium",
            }
        )

    citations = []
    if jp["profile"] and jp["profile"].get("source_urls"):
        citations.append({"type": "jurisdiction_profile_sources", "urls": jp["profile"]["source_urls"]})

    return {
        "summary": f"Packet builder produced {len(actions)} checklist recommendations from jurisdiction profile.",
        "facts": {
            "jurisdiction_profile": (jp["profile"]["name"] if jp["profile"] else None),
            "step_count": len(steps),
        },
        "actions": actions,
        "citations": citations,
    }