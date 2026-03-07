from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..domain.workflow.stages import stage_gte
from ..models import Property
from ..services.policy_projection_service import build_property_compliance_brief
from ..services.property_state_machine import get_state_payload, get_transition_payload
from ..services.workflow_gate_service import build_workflow_summary


def _policy_blockers(db: Session, *, org_id: int, property_id: int) -> list:
    prop = db.scalar(
        select(Property).where(
            Property.id == property_id,
            Property.org_id == org_id,
        )
    )
    if not prop:
        return []

    brief = build_property_compliance_brief(
        db,
        org_id=None,
        state=prop.state or "MI",
        county=getattr(prop, "county", None),
        city=prop.city,
        pha_name=None,
    )
    return brief.get("blocking_items", []) or []


def require_stage(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    min_stage: str,
    action: str,
) -> dict:
    st = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    cur = str(st.get("current_stage") or "import")
    workflow = build_workflow_summary(db, org_id=org_id, property_id=property_id, recompute=False)

    if not stage_gte(cur, min_stage):
        why = f"Requires stage ≥ {min_stage} to {action}."
        next_actions = st.get("next_actions") or []
        if next_actions:
            why = f"{why} Next: {next_actions[0]}"

        raise HTTPException(
            status_code=409,
            detail={
                "error": "stage_locked",
                "current_stage": cur,
                "required_stage": min_stage,
                "action": action,
                "why": why,
                "next_actions": next_actions,
                "constraints": st.get("constraints") or {},
                "policy_blockers": _policy_blockers(db, org_id=org_id, property_id=property_id),
                "workflow": workflow,
            },
        )

    return st


def require_next_stage_available(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    action: str,
) -> dict:
    tx = get_transition_payload(db, org_id=org_id, property_id=property_id)
    gate = tx.get("gate") or {}
    workflow = build_workflow_summary(db, org_id=org_id, property_id=property_id, recompute=False)

    if not bool(gate.get("ok")):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "stage_transition_blocked",
                "current_stage": tx.get("current_stage"),
                "action": action,
                "why": gate.get("blocked_reason") or "Next stage is blocked.",
                "allowed_next_stage": gate.get("allowed_next_stage"),
                "constraints": tx.get("constraints") or {},
                "next_actions": tx.get("next_actions") or [],
                "policy_blockers": _policy_blockers(db, org_id=org_id, property_id=property_id),
                "workflow": workflow,
            },
        )

    return tx