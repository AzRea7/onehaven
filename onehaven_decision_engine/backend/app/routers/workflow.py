from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import WorkflowEvent, PropertyState, Property
from ..schemas import WorkflowEventCreate, WorkflowEventOut, PropertyStateUpsert, PropertyStateOut

router = APIRouter(prefix="/workflow", tags=["workflow"])


@router.post("/events", response_model=WorkflowEventOut)
def post_event(
    payload: WorkflowEventCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    if payload.property_id is not None:
        prop = db.get(Property, payload.property_id)
        if not prop or prop.org_id != p.org_id:
            raise HTTPException(status_code=404, detail="property not found")

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
        q = q.where(WorkflowEvent.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/state", response_model=PropertyStateOut)
def upsert_state(
    payload: PropertyStateUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    existing = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == p.org_id,
            PropertyState.property_id == payload.property_id,
        )
    )

    now = datetime.utcnow()

    if existing:
        if payload.current_stage is not None:
            existing.current_stage = payload.current_stage
        if payload.constraints is not None:
            existing.constraints_json = json.dumps(payload.constraints)
        if payload.outstanding_tasks is not None:
            existing.outstanding_tasks_json = json.dumps(payload.outstanding_tasks)
        existing.updated_at = now
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing

    row = PropertyState(
        org_id=p.org_id,
        property_id=payload.property_id,
        current_stage=payload.current_stage or "deal",
        constraints_json=json.dumps(payload.constraints or {}),
        outstanding_tasks_json=json.dumps(payload.outstanding_tasks or {}),
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/state/{property_id}", response_model=PropertyStateOut)
def get_state(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == p.org_id,
            PropertyState.property_id == property_id,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="state not found")
    return row
