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
    Deterministic ops continuity agent (recommend-only):
    - reads PropertyState next_actions
    - turns them into structured recommendations
    - MUST NOT emit actions[] because contract.mode = recommend_only
    """
    if not property_id:
        return {
            "agent_key": "timeline_nudger",
            "summary": "No property_id provided.",
            "facts": {},
            "actions": [],
            "recommendations": [],
            "citations": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {
            "agent_key": "timeline_nudger",
            "summary": "Property not found.",
            "facts": {},
            "actions": [],
            "recommendations": [],
            "citations": [],
        }

    st = compute_and_persist_stage(db, org_id=org_id, property=prop)

    next_actions: list[Any] = []
    try:
        import json

        next_actions = json.loads(getattr(st, "next_actions_json", None) or "[]")
        if not isinstance(next_actions, list):
            next_actions = []
    except Exception:
        next_actions = []

    # recommend-only: suggestions live here (NOT in actions)
    recommendations: list[dict[str, Any]] = []
    for na in next_actions[:25]:
        recommendations.append(
            {
                "type": "reminder",
                "property_id": int(prop.id),
                "text": str(na),
                "reason": "Keeps timeline pressure visible in the ops loop.",
                "priority": "medium",
            }
        )

    return {
        "agent_key": "timeline_nudger",
        "summary": f"Timeline nudger produced {len(recommendations)} recommendations from PropertyState.",
        "facts": {"stage": getattr(st, "current_stage", None), "next_actions": next_actions},
        "actions": [],  # ✅ contract-compliant
        "recommendations": recommendations,
        "citations": [],
    }
