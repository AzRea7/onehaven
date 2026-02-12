# backend/app/routers/agents.py
from __future__ import annotations

import json
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import AgentRun, AgentMessage
from ..schemas import (
    AgentSpecOut,
    AgentRunCreate,
    AgentRunOut,
    AgentMessageCreate,
    AgentMessageOut,
)
from ..domain.agents.registry import AGENTS


router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("", response_model=list[AgentSpecOut])
def list_agents():
    out: list[AgentSpecOut] = []
    for a in AGENTS.values():
        # AgentSpec typically has: key, name, description, default_payload_schema
        out.append(
            AgentSpecOut(
                agent_key=getattr(a, "key", None),
                title=getattr(a, "name", None),
                description=getattr(a, "description", None),
                notes=None,
            )
        )
    return out


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
