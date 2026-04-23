from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from app.db import get_db
from app.domain.workflow.panes import pane_catalog
from app.domain.workflow.stages import clamp_stage, stage_catalog, stage_gte
from app.models import Property, WorkflowEvent
from app.schemas import PropertyStateUpsert, WorkflowEventCreate, WorkflowEventOut
from app.services.properties.state_machine import ensure_state_row, get_state_payload, get_transition_payload, sync_property_state
from app.services.stage_guard import require_start_acquisition
from app.services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/workflow", tags=["workflow"])


def _must_get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != org_id:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


@router.get("/catalog", response_model=dict)
def workflow_catalog():
    return {"stages": stage_catalog(), "panes": pane_catalog(), "decision_states": ["GOOD", "REVIEW", "REJECT"]}


@router.post("/events", response_model=WorkflowEventOut)
def post_event(payload: WorkflowEventCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    if payload.property_id is not None:
        _must_get_property(db, org_id=p.org_id, property_id=payload.property_id)
    ev = WorkflowEvent(org_id=p.org_id, property_id=payload.property_id, actor_user_id=p.user_id, event_type=payload.event_type, payload_json=json.dumps(payload.payload or {}), created_at=datetime.utcnow())
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return ev


@router.get("/events", response_model=list[WorkflowEventOut])
def list_events(property_id: int | None = Query(default=None), limit: int = Query(default=200, ge=1, le=500), db: Session = Depends(get_db), p=Depends(get_principal)):
    q = select(WorkflowEvent).where(WorkflowEvent.org_id == p.org_id).order_by(desc(WorkflowEvent.id))
    if property_id is not None:
        _must_get_property(db, org_id=p.org_id, property_id=property_id)
        q = q.where(WorkflowEvent.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/state", response_model=dict)
def upsert_state(payload: PropertyStateUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
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
        if stage_gte(requested, suggested) and requested != suggested:
            raise HTTPException(status_code=409, detail={
                "error": "manual_stage_override_blocked",
                "requested_stage": requested,
                "suggested_stage": suggested,
                "why": "Stage cannot be manually advanced beyond computed workflow truth.",
                "next_actions": computed.get("next_actions") or [],
                "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=payload.property_id, principal=p, recompute=False),
            })
        row.current_stage = requested
    row.constraints_json = json.dumps(existing_constraints)
    row.outstanding_tasks_json = json.dumps(existing_tasks)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    sync_property_state(db, org_id=p.org_id, property_id=payload.property_id)
    db.commit()
    return {"state": get_state_payload(db, org_id=p.org_id, property_id=payload.property_id, recompute=True), "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=payload.property_id, principal=p, recompute=False)}


@router.get("/state/{property_id}", response_model=dict)
def get_state(property_id: int, recompute: bool = Query(default=True), db: Session = Depends(get_db), p=Depends(get_principal)):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return {"state": get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=recompute), "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, principal=p, recompute=False)}


@router.get("/transition/{property_id}", response_model=dict)
def get_transition(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return {"transition": get_transition_payload(db, org_id=p.org_id, property_id=property_id), "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, principal=p, recompute=False)}


@router.post("/start-acquisition/{property_id}", response_model=dict)
def start_acquisition(property_id: int, payload: dict = Body(default_factory=dict), db: Session = Depends(get_db), p=Depends(get_principal)):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_start_acquisition(db, org_id=p.org_id, property_id=property_id)
    row = ensure_state_row(db, org_id=p.org_id, property_id=property_id)
    existing_constraints = json.loads(row.constraints_json or "{}") if row.constraints_json else {}
    acquisition = existing_constraints.get("acquisition") if isinstance(existing_constraints.get("acquisition"), dict) else {}
    acquisition.update({
        "start_requested": True,
        "manual_start_approved": True,
        "pursuit_status": "active",
        "stage": "pursuing",
        "started_at": datetime.utcnow().isoformat(),
        "started_by_user_id": p.user_id,
    })
    if isinstance(payload, dict):
        acquisition.update(payload)
    existing_constraints["acquisition"] = acquisition
    row.constraints_json = json.dumps(existing_constraints)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()
    return {"ok": True, "property_id": property_id, "started_stage": "pursuing", "state": get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=True), "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, principal=p, recompute=False)}


@router.post("/advance/{property_id}", response_model=dict)
def advance(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    tx = get_transition_payload(db, org_id=p.org_id, property_id=property_id)
    gate = tx.get("gate") or {}
    if not gate.get("ok"):
        raise HTTPException(status_code=409, detail={
            "error": "stage_transition_blocked",
            "property_id": property_id,
            "current_stage": tx.get("current_stage"),
            "why": gate.get("blocked_reason"),
            "allowed_next_stage": gate.get("allowed_next_stage"),
            "constraints": tx.get("constraints") or {},
            "next_actions": tx.get("next_actions") or [],
            "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, principal=p, recompute=False),
        })
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()
    return {"ok": True, "property_id": property_id, "advanced_to": gate.get("allowed_next_stage"), "advanced_to_label": gate.get("allowed_next_stage_label"), "state": get_state_payload(db, org_id=p.org_id, property_id=property_id, recompute=True), "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, principal=p, recompute=False)}
