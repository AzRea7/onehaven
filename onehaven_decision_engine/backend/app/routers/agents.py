from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import AgentRun, AgentMessage, AgentSlotAssignment, WorkflowEvent
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
def list_agents(p=Depends(get_principal)):
    out: list[AgentSpecOut] = []
    for a in AGENTS.values():
        out.append(
            AgentSpecOut(
                agent_key=getattr(a, "key", ""),
                title=getattr(a, "name", ""),
                description=getattr(a, "description", None),
                needs_human=bool(getattr(a, "needs_human", False)),
                category=getattr(a, "category", None),
                sidebar_slots=[],
            )
        )
    return out


@router.post("/runs", response_model=AgentRunOut)
def create_run(payload: AgentRunCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    if payload.agent_key not in AGENTS:
        raise HTTPException(status_code=404, detail="unknown agent_key")

    run = AgentRun(
        org_id=p.org_id,
        agent_key=payload.agent_key,
        property_id=payload.property_id,
        status=payload.status or "queued",
        input_json=payload.input_json,
        output_json=None,
        created_at=datetime.utcnow(),
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
    p=Depends(get_principal),
):
    q = select(AgentRun).where(AgentRun.org_id == p.org_id).order_by(desc(AgentRun.id))
    if agent_key:
        q = q.where(AgentRun.agent_key == agent_key)
    if property_id is not None:
        q = q.where(AgentRun.property_id == property_id)
    rows = db.scalars(q.limit(limit)).all()
    return list(rows)


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
    q = (
        select(AgentSlotAssignment)
        .where(AgentSlotAssignment.org_id == p.org_id)
        .order_by(desc(AgentSlotAssignment.updated_at))
    )
    if property_id is not None:
        q = q.where(AgentSlotAssignment.property_id == property_id)
    return list(db.scalars(q.limit(limit)).all())


@router.post("/slots/assignments", response_model=AgentSlotAssignmentOut)
def upsert_slot_assignment(payload: AgentSlotAssignmentUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    existing = db.scalar(
        select(AgentSlotAssignment).where(
            AgentSlotAssignment.org_id == p.org_id,
            AgentSlotAssignment.slot_key == payload.slot_key,
            AgentSlotAssignment.property_id == payload.property_id,
        ).limit(1)
    )

    now = datetime.utcnow()

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

        db.add(
            WorkflowEvent(
                org_id=p.org_id,
                property_id=payload.property_id,
                actor_user_id=p.user_id,
                event_type="slot_assigned",
                payload_json=json.dumps(
                    {
                        "slot_key": payload.slot_key,
                        "owner_type": existing.owner_type,
                        "assignee": existing.assignee,
                        "status": existing.status,
                    }
                ),
                created_at=now,
            )
        )

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

    db.add(
        WorkflowEvent(
            org_id=p.org_id,
            property_id=payload.property_id,
            actor_user_id=p.user_id,
            event_type="slot_assigned",
            payload_json=json.dumps(
                {
                    "slot_key": payload.slot_key,
                    "owner_type": row.owner_type,
                    "assignee": row.assignee,
                    "status": row.status,
                }
            ),
            created_at=now,
        )
    )

    db.commit()
    db.refresh(row)
    return row
