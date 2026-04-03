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
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
    if prop is None:
        return []
    try:
        brief = build_property_compliance_brief(db, org_id=org_id, property=prop)
    except Exception:
        return []
    blockers = brief.get("blockers") if isinstance(brief, dict) else []
    return blockers if isinstance(blockers, list) else []


def require_stage(db: Session, *, org_id: int, property_id: int, min_stage: str, action: str) -> dict:
    state = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    current_stage = state.get("current_stage") or "discovered"
    if stage_gte(current_stage, min_stage):
        return state
    raise HTTPException(status_code=409, detail={
        "error": "stage_locked",
        "property_id": property_id,
        "current_stage": current_stage,
        "required_stage": min_stage,
        "action": action,
        "why": f"{action} requires workflow stage >= {min_stage}.",
        "next_actions": state.get("next_actions") or [],
        "constraints": state.get("constraints") or {},
        "policy_blockers": _policy_blockers(db, org_id=org_id, property_id=property_id),
        "workflow": build_workflow_summary(db, org_id=org_id, property_id=property_id, recompute=False),
    })


def require_next_stage_available(db: Session, *, org_id: int, property_id: int, action: str) -> dict:
    tx = get_transition_payload(db, org_id=org_id, property_id=property_id)
    gate = tx.get("gate") or {}
    if gate.get("ok"):
        return tx
    raise HTTPException(status_code=409, detail={
        "error": "stage_transition_blocked",
        "property_id": property_id,
        "current_stage": tx.get("current_stage"),
        "allowed_next_stage": gate.get("allowed_next_stage"),
        "action": action,
        "why": gate.get("blocked_reason"),
        "next_actions": tx.get("next_actions") or [],
        "constraints": tx.get("constraints") or {},
        "policy_blockers": _policy_blockers(db, org_id=org_id, property_id=property_id),
        "workflow": build_workflow_summary(db, org_id=org_id, property_id=property_id, recompute=False),
    })


def require_start_acquisition(db: Session, *, org_id: int, property_id: int, action: str = "start acquisition") -> dict:
    state = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    constraints = state.get("constraints") or {}
    start_gate = ((constraints.get("acquisition") or {}).get("start_gate") or {}) if isinstance(constraints, dict) else {}
    if start_gate.get("ok"):
        return state
    raise HTTPException(status_code=409, detail={
        "error": "start_acquisition_blocked",
        "property_id": property_id,
        "current_stage": state.get("current_stage"),
        "action": action,
        "why": start_gate.get("blocked_reason") or "Minimum pre-offer pursuit criteria are not complete.",
        "blockers": start_gate.get("blockers") or [],
        "next_actions": state.get("next_actions") or [],
        "constraints": constraints,
        "policy_blockers": _policy_blockers(db, org_id=org_id, property_id=property_id),
        "workflow": build_workflow_summary(db, org_id=org_id, property_id=property_id, recompute=False),
    })
