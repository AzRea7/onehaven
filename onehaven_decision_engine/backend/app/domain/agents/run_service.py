# onehaven_decision_engine/backend/app/domain/agents/run_service.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...models import AgentRun, AgentMessage


def create_run(
    db: Session,
    org_id: int,
    created_by_user_id: int,
    agent_key: str,
    property_id: int | None = None,
    title: str | None = None,
    input_json: dict | None = None,
) -> AgentRun:
    run = AgentRun(
        org_id=org_id,
        agent_key=agent_key,
        property_id=property_id,
        status="open",
        created_by_user_id=created_by_user_id,
        title=title or f"{agent_key} run",
        input_json=input_json or {},
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def append_message(
    db: Session,
    org_id: int,
    run_id: int,
    role: str,
    content: str,
    data_json: dict | None = None,
) -> AgentMessage:
    # Ensure run exists & belongs to org
    run = db.execute(
        select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id)
    ).scalar_one_or_none()
    if not run:
        raise ValueError("Run not found")

    msg = AgentMessage(
        org_id=org_id,
        run_id=run_id,
        role=role,
        content=content,
        data_json=data_json or {},
    )
    db.add(msg)

    # Keep run timestamps fresh
    run.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(msg)
    return msg


def close_run(
    db: Session,
    org_id: int,
    run_id: int,
    status: str,
    summary: str | None = None,
) -> AgentRun:
    run = db.execute(
        select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id)
    ).scalar_one_or_none()
    if not run:
        raise ValueError("Run not found")

    run.status = status
    run.summary = summary
    run.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(run)
    return run