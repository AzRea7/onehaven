# onehaven_decision_engine/backend/app/services/stage_guard.py
from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.domain.workflow.stages import stage_gte
from app.services.property_state_machine import get_state_payload
from app.services.policy_projection_service import build_property_compliance_brief
from app.models import Property

def require_stage(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    min_stage: str,
    action: str,
) -> dict:
    st = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    cur = str(st.get("current_stage") or "deal")

    policy_blockers = []
    prop = db.query(Property).filter(Property.id == property_id, Property.org_id == org_id).first()
    if prop:
        brief = build_property_compliance_brief(
            db,
            org_id=None,
            state=prop.state or "MI",
            county=getattr(prop, "county", None),
            city=prop.city,
            pha_name=None,
        )
        policy_blockers = brief.get("blocking_items", [])

    if not stage_gte(cur, min_stage):
        why = st.get("blocked_reason") or f"Requires stage ≥ {min_stage} to {action}."
        raise HTTPException(
            status_code=409,
            detail={
                "error": "stage_locked",
                "current_stage": cur,
                "required_stage": min_stage,
                "action": action,
                "why": why,
                "policy_blockers": policy_blockers,
            },
        )

    return st