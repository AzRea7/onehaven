from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.workflow.stages import clamp_stage, stage_gte
from ..models import Property, PropertyState, WorkflowEvent
from ..schemas import PropertyStateUpsert, WorkflowEventCreate, WorkflowEventOut
from ..services.property_state_machine import (
    ensure_state_row,
    get_state_payload,
    get_transition_payload,
    sync_property_state,
)

router = APIRouter(prefix="/workflow", tags=["workflow"])


def _must_get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != org_id:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


@router.post("/events", response_model=WorkflowEventOut)
def post_event(
    payload: WorkflowEventCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    if payload.property_id is not None:
        _must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

    ev = WorkflowEvent(
        org_id=p.org_id,
        property_id=payload.property_id,
        actor_user_id=p.user_id,
        event_type=payload.event_type,
        payload_json=json.dumps(payload.payload or {}),
        created_at=datetime.utcnow(),
    )
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return ev


@router.get("/events", response_model=list[WorkflowEventOut])
def list_events(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(WorkflowEvent).where(WorkflowEvent.org_id == p.org_id).order_by(desc(WorkflowEvent.id))
    if property_id is not None:
        _must_get_property(db, org_id=p.org_id, property_id=property_id)
        q = q.where(WorkflowEvent.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/state", response_model=dict)
def upsert_state(
    payload: PropertyStateUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Step 2 rule:
    current_stage is authoritative from the state machine, not a user-editable toy.
    This endpoint lets you persist extra constraints/outstanding task metadata,
    but it will not allow arbitrary stage jumping.
    """
    _must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

    row = ensure_state_row(db, org_id=p.org_id, property_id=payload.property_id)
    existing_constraints = json.loads(row.constraints_json or "{}") if row.constraints_json else {}
    existing_tasks = json.loads(row.outstanding_tasks_json or "{}") if row.outstanding_tasks_json else {}

    if payload.constraints is not None and isinstance(payload.constraints, dict):
        existing_constraints.update(payload.constraints)

    if payload.outstanding_tasks is not None and isinstance(payload.outstanding_tasks, dict):
        existing_tasks.update(payload.outstanding_tasks)

    if payload.current_stage is not None:
        computed = get_state_payload(db, org_id=p.org_id, property_id=payload.property_id, recompute=True)
        requested = clamp_stage(payload.current_stage)
        suggested = clamp_stage(computed.get("suggested_stage"))
        # Only allow a manual stage set if it does not exceed computed truth.
        if stage_gte(requested, suggested) and requested != suggested:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "manual_stage_override_blocked",
                    "requested_stage": requested,
                    "suggested_stage": suggested,
                    "why": "Stage cannot be manually advanced beyond computed workflow truth.",
                    "next_actions": computed.get("next_actions") or [],
                },
            )
        row.current_stage = requested

    row.constraints_json = json.dumps(existing_constraints)
    row.outstanding_tasks_json = json.dumps(existing_tasks)
    row.updated_at = datetime.utcnow()

    db.add(row)
    db.commit()

    # Always sync back to canonical truth after persistence.
    sync_property_state(db, org_id=p.org_id, property_id=payload.property_id)
    db.commit()

    return get_state_payload(db, org_id=p.org_id, property_id=payload.property_id, recompute=True)


@router.get("/state/{property_id}", response_model=dict)
def get_state(
    property_id: int,
    recompute: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=recompute)


@router.get("/transition/{property_id}", response_model=dict)
def get_transition(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return get_transition_payload(db, org_id=p.org_id, property_id=property_id)


@router.post("/advance/{property_id}", response_model=dict)
def advance(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Validates whether the next stage is unlocked.
    Because stage truth is data-driven, this endpoint mostly acts as a gate/diagnostic.
    If the next stage is already reflected by computed facts, we persist the synced state.
    """
    _must_get_property(db, org_id=p.org_id, property_id=property_id)

    tx = get_transition_payload(db, org_id=p.org_id, property_id=property_id)
    gate = tx.get("gate") or {}

    if not gate.get("ok"):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "stage_transition_blocked",
                "property_id": property_id,
                "current_stage": tx.get("current_stage"),
                "why": gate.get("blocked_reason"),
                "allowed_next_stage": gate.get("allowed_next_stage"),
                "constraints": tx.get("constraints") or {},
                "next_actions": tx.get("next_actions") or [],
            },
        )

    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()

    return {
        "ok": True,
        "property_id": property_id,
        "advanced_to": gate.get("allowed_next_stage"),
        "state": get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=True),
    }
