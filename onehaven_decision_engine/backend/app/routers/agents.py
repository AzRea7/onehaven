# backend/app/routers/agents.py
from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import AgentMessage, AgentRun, AgentSlotAssignment, WorkflowEvent, Property
from ..schemas import (
    AgentMessageCreate,
    AgentMessageOut,
    AgentRunCreate,
    AgentRunOut,
    AgentSlotAssignmentOut,
    AgentSlotAssignmentUpsert,
    AgentSlotSpecOut,
    AgentSpecOut,
)
from ..domain.agents.registry import AGENTS, SLOTS, AGENT_SPECS
from ..services.agent_engine import create_and_execute_run

router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("", response_model=list[AgentSpecOut])
def list_agents(p=Depends(get_principal)):
    out: list[AgentSpecOut] = []
    for spec in AGENT_SPECS.values():
        out.append(
            AgentSpecOut(
                agent_key=spec["agent_key"],
                title=spec["title"],
                description=spec.get("description"),
                needs_human=bool(spec.get("needs_human", False)),
                category=spec.get("category"),
                sidebar_slots=[],
            )
        )
    return out


@router.get("/registry", response_model=dict)
def registry(p=Depends(get_principal)):
    return {
        "agents": list(AGENT_SPECS.values()),
        "slots": [
            {
                "slot_key": s.slot_key,
                "title": s.title,
                "description": s.description,
                "owner_type": s.owner_type,
                "default_status": s.default_status,
            }
            for s in SLOTS
        ],
    }


@router.post("/runs", response_model=AgentRunOut)
def create_run(payload: AgentRunCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    if payload.agent_key not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent_key")

    if payload.property_id is not None:
        prop = db.scalar(select(Property).where(Property.id == payload.property_id, Property.org_id == p.org_id))
        if not prop:
            raise HTTPException(status_code=404, detail="property not found")

    # IMPORTANT: your agent_engine.create_and_execute_run expects input_payload (dict) and actor_user_id
    res = create_and_execute_run(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        agent_key=payload.agent_key,
        property_id=payload.property_id,
        input_payload=payload.input_json or {},
        dispatch=True,  # async by default (worker)
    )

    # res is a dict (mode async/sync). But your response_model is AgentRunOut.
    # To keep it stable for your UI, return the created AgentRun row when async.
    if res.get("mode") == "async":
        run_id = int(res["run_id"])
        run = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == p.org_id))
        if not run:
            raise HTTPException(status_code=500, detail="run created but not found")
        return run

    # sync path (rare)
    run_id = int(res["run_id"])
    run = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == p.org_id))
    if not run:
        raise HTTPException(status_code=500, detail="run executed but row not found")
    return run


@router.get("/runs", response_model=list[AgentRunOut])
def list_runs(
    agent_key: str | None = Query(default=None),
    property_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(AgentRun).where(AgentRun.org_id == p.org_id).order_by(desc(AgentRun.id))
    if agent_key:
        q = q.where(AgentRun.agent_key == agent_key)
    if property_id is not None:
        q = q.where(AgentRun.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/messages", response_model=AgentMessageOut)
def post_message(payload: AgentMessageCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    msg = AgentMessage(
        org_id=p.org_id,
        thread_key=payload.thread_key,
        sender=payload.sender,
        recipient=payload.recipient,
        message=payload.message,
        created_at=datetime.utcnow(),
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
    p=Depends(get_principal),
):
    q = (
        select(AgentMessage)
        .where(AgentMessage.org_id == p.org_id)
        .where(AgentMessage.thread_key == thread_key)
        .order_by(AgentMessage.id.asc())
    )
    if recipient:
        q = q.where(AgentMessage.recipient == recipient)
    return list(db.scalars(q.limit(limit)).all())


@router.get("/slots/specs", response_model=list[AgentSlotSpecOut])
def slot_specs(p=Depends(get_principal)):
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
    p=Depends(get_principal),
):
    q = select(AgentSlotAssignment).where(AgentSlotAssignment.org_id == p.org_id).order_by(
        desc(AgentSlotAssignment.updated_at)
    )
    if property_id is not None:
        prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
        if not prop:
            raise HTTPException(status_code=404, detail="property not found")
        q = q.where(AgentSlotAssignment.property_id == property_id)

    return list(db.scalars(q.limit(limit)).all())


@router.post("/slots/assignments", response_model=AgentSlotAssignmentOut)
def upsert_slot_assignment(payload: AgentSlotAssignmentUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    if payload.property_id is not None:
        prop = db.scalar(select(Property).where(Property.id == payload.property_id, Property.org_id == p.org_id))
        if not prop:
            raise HTTPException(status_code=404, detail="property not found")

    existing = db.scalar(
        select(AgentSlotAssignment)
        .where(
            AgentSlotAssignment.org_id == p.org_id,
            AgentSlotAssignment.slot_key == payload.slot_key,
            AgentSlotAssignment.property_id == payload.property_id,
        )
        .limit(1)
    )

    now = datetime.utcnow()

    def _emit_event(owner_type: str, assignee: str | None, status: str):
        db.add(
            WorkflowEvent(
                org_id=p.org_id,
                property_id=payload.property_id,
                actor_user_id=p.user_id,
                event_type="slot_assigned",
                payload_json=json.dumps(
                    {"slot_key": payload.slot_key, "owner_type": owner_type, "assignee": assignee, "status": status}
                ),
                created_at=now,
            )
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
        existing.updated_at = now
        db.add(existing)

        _emit_event(existing.owner_type, existing.assignee, existing.status)
        db.commit()
        db.refresh(existing)
        return existing

    row = AgentSlotAssignment(
        org_id=p.org_id,
        slot_key=payload.slot_key,
        property_id=payload.property_id,
        owner_type=payload.owner_type or "human",
        assignee=payload.assignee,
        status=payload.status or "idle",
        notes=payload.notes,
        updated_at=now,
        created_at=now,
    )
    db.add(row)
    _emit_event(row.owner_type, row.assignee, row.status)
    db.commit()
    db.refresh(row)
    return row
