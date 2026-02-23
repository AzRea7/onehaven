# onehaven_decision_engine/backend/app/domain/agents/impl/timeline_nudger.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.services.property_state_machine import compute_and_persist_stage


def run_timeline_nudger(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic ops continuity agent:
    - reads PropertyState next_actions (Phase 4)
    - turns them into structured reminders/tasks (recommend-only)
    """
    if not property_id:
        return {"summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    st = compute_and_persist_stage(db, org_id=org_id, property=prop)

    next_actions = []
    try:
        import json

        next_actions = json.loads(getattr(st, "next_actions_json", None) or "[]")
        if not isinstance(next_actions, list):
            next_actions = []
    except Exception:
        next_actions = []

    actions = []
    for na in next_actions[:25]:
        actions.append(
            {
                "op": "recommend",
                "entity_type": "Reminder",
                "entity_id": None,
                "payload": {"property_id": prop.id, "text": str(na)},
                "priority": "medium",
            }
        )

    return {
        "summary": f"Timeline nudger produced {len(actions)} next-action reminders from PropertyState.",
        "facts": {"stage": getattr(st, "current_stage", None), "next_actions": next_actions},
        "actions": actions,
        "citations": [],
    }