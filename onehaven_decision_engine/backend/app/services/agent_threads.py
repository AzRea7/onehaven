# onehaven_decision_engine/backend/app/services/agent_threads.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..models import AgentMessage, AgentRun


def thread_key_for_run(run_id: int) -> str:
    return f"run:{int(run_id)}"


def post_message(db: Session, *, org_id: int, run_id: int, sender: str, message: str) -> AgentMessage:
    run = db.get(AgentRun, int(run_id))
    if run is None or run.org_id != org_id:
        raise ValueError("run not found")

    row = AgentMessage(
        org_id=org_id,
        run_id=int(run_id),
        thread_key=thread_key_for_run(run_id),
        sender=str(sender),
        message=str(message),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    return row


def list_messages(db: Session, *, org_id: int, run_id: int) -> list[AgentMessage]:
    run = db.get(AgentRun, int(run_id))
    if run is None or run.org_id != org_id:
        raise ValueError("run not found")

    rows = db.execute(
        select(AgentMessage)
        .where(AgentMessage.org_id == org_id)
        .where(AgentMessage.run_id == int(run_id))
        .order_by(AgentMessage.id.asc())
    ).scalars().all()
    return rows