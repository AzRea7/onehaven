# onehaven_decision_engine/backend/app/services/stage_guard.py
from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.domain.workflow.stages import stage_gte
from app.services.property_state_machine import get_state_payload


def require_stage(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    min_stage: str,
    action: str,
) -> dict:
    """
    API-level lock: blocks creation of downstream objects unless stage >= min_stage.

    Returns the latest state payload so callers can use it without recomputing.
    """
    st = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)
    cur = str(st.get("current_stage") or "deal")

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
            },
        )

    return st