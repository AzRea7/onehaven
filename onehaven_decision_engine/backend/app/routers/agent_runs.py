# onehaven_decision_engine/backend/app/routers/agent_runs.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import AgentRun, AgentMessage
from ..schemas import AgentRunCreate, AgentRunOut, AgentMessageCreate, AgentMessageOut

from ..domain.agents.run_service import create_run, append_message, close_run

router = APIRouter(tags=["agents"])


@router.post("/agent-runs", response_model=AgentRunOut)
def create_agent_run(
    inp: AgentRunCreate,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = create_run(
        db=db,
        org_id=principal.org_id,
        created_by_user_id=principal.user_id,
        agent_key=inp.agent_key,
        property_id=inp.property_id,
        title=inp.title,
        input_json=inp.input_json,
    )
    return run


@router.get("/agent-runs", response_model=list[AgentRunOut])
def list_agent_runs(
    limit: int = 50,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = db.execute(
        select(AgentRun)
        .where(AgentRun.org_id == principal.org_id)
        .order_by(desc(AgentRun.created_at))
        .limit(limit)
    ).scalars().all()
    return rows


@router.get("/agent-runs/{run_id}/messages", response_model=list[AgentMessageOut])
def list_agent_run_messages(
    run_id: int,
    limit: int = 200,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    # Ensure run exists
    run = db.execute(
        select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == principal.org_id)
    ).scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Agent run not found")

    msgs = db.execute(
        select(AgentMessage)
        .where(AgentMessage.org_id == principal.org_id, AgentMessage.run_id == run_id)
        .order_by(AgentMessage.created_at.asc())
        .limit(limit)
    ).scalars().all()
    return msgs


@router.post("/agent-runs/{run_id}/messages", response_model=AgentMessageOut)
def add_agent_run_message(
    run_id: int,
    inp: AgentMessageCreate,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    try:
        msg = append_message(
            db=db,
            org_id=principal.org_id,
            run_id=run_id,
            role=inp.role,
            content=inp.content,
            data_json=inp.data_json,
        )
        return msg
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/agent-runs/{run_id}/close", response_model=AgentRunOut)
def close_agent_run(
    run_id: int,
    status: str = "done",  # done|blocked|cancelled
    summary: str | None = None,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    try:
        run = close_run(
            db=db,
            org_id=principal.org_id,
            run_id=run_id,
            status=status,
            summary=summary,
        )
        return run
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))