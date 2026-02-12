# backend/app/routers/agents.py
from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import AgentRun, AgentMessage, AgentSlotAssignment
from ..schemas import (
    AgentSpecOut,
    AgentRunCreate,
    AgentRunOut,
    AgentMessageCreate,
    AgentMessageOut,
    AgentSlotSpecOut,
    AgentSlotAssignmentUpsert,
    AgentSlotAssignmentOut,
)
from ..domain.agents.registry import AGENTS, SLOTS


router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("", response_model=list[AgentSpecOut])
def list_agents():
    out: list[AgentSpecOut] = []
    for a in AGENTS.values():
        out.append(
            AgentSpecOut(
                agent_key=getattr(a, "key", None),
                title=getattr(a, "name", None),
                description=getattr(a, "description", None),
                notes=None,
            )
        )
    return out


# -----------------------------
# Agent Runs
# -----------------------------
@router.post("/runs", response_model=AgentRunOut)
def create_run(payload: AgentRunCreate, db: Session = Depends(get_db)):
    if payload.agent_key not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent_key")

    run = AgentRun(
        agent_key=payload.agent_key,
        property_id=payload.property_id,
        payload_json=json.dumps(payload.payload or {}),
        status="created",
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


@router.get("/runs", response_model=list[AgentRunOut])
def list_runs(
    agent_key: str | None = Query(default=None),
    property_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = select(AgentRun).order_by(desc(AgentRun.id))
    if agent_key:
        q = q.where(AgentRun.agent_key == agent_key)
    if property_id is not None:
        q = q.where(AgentRun.property_id == property_id)
    rows = db.scalars(q.limit(limit)).all()
    return list(rows)


# -----------------------------
# Agent Messages
# -----------------------------
@router.post("/messages", response_model=AgentMessageOut)
def post_message(payload: AgentMessageCreate, db: Session = Depends(get_db)):
    msg = AgentMessage(
        thread_key=payload.thread_key,
        sender=payload.sender,
        recipient=payload.recipient,
        message=payload.message,
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


@router.get("/messages", response_model=list[AgentMessageOut])
def list_messages(
    thread_key: str,
    recipient: str | None = None,
    limit: int = 200,
    db: Session = Depends(get_db),
):
    q = (
        select(AgentMessage)
        .where(AgentMessage.thread_key == thread_key)
        .order_by(AgentMessage.id.asc())
    )
    if recipient:
        q = q.where(AgentMessage.recipient == recipient)
    return list(db.scalars(q.limit(limit)).all())


# -----------------------------
# NEW: Slot Specs + Slot Assignments
# -----------------------------
@router.get("/slots/specs", response_model=list[AgentSlotSpecOut])
def slot_specs():
    return [
        AgentSlotSpecOut(
            slot_key=s.slot_key,
            title=s.title,
            description=s.description,
            owner_type=s.owner_type,
            default_status=s.default_status,
        )
        for s in SLOTS
    ]


@router.get("/slots/assignments", response_model=list[AgentSlotAssignmentOut])
def slot_assignments(
    property_id: int | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = select(AgentSlotAssignment).order_by(desc(AgentSlotAssignment.updated_at))
    if property_id is not None:
        q = q.where(AgentSlotAssignment.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/slots/assignments", response_model=AgentSlotAssignmentOut)
def upsert_slot_assignment(payload: AgentSlotAssignmentUpsert, db: Session = Depends(get_db)):
    # find existing for (slot_key, property_id)
    existing = db.scalar(
        select(AgentSlotAssignment).where(
            AgentSlotAssignment.slot_key == payload.slot_key,
            AgentSlotAssignment.property_id == payload.property_id,
        ).limit(1)
    )

    if existing:
        if payload.owner_type is not None:
            existing.owner_type = payload.owner_type
        if payload.assignee is not None:
            existing.assignee = payload.assignee
        if payload.status is not None:
            existing.status = payload.status
        if payload.notes is not None:
            existing.notes = payload.notes
        existing.updated_at = datetime.utcnow()
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing

    row = AgentSlotAssignment(
        slot_key=payload.slot_key,
        property_id=payload.property_id,
        owner_type=payload.owner_type or "human",
        assignee=payload.assignee,
        status=payload.status or "idle",
        notes=payload.notes,
        updated_at=datetime.utcnow(),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
