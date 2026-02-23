# backend/app/services/agent_engine.py
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import AgentRun
from ..domain.agents.executor import execute_agent


def create_run(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    agent_key: str,
    idempotency_key: str | None = None,
    input_json: dict | None = None,
) -> AgentRun:
    # hard idempotency: if same idempotency_key exists, return it
    if idempotency_key:
        existing = db.scalar(
            select(AgentRun)
            .where(AgentRun.org_id == org_id)
            .where(AgentRun.idempotency_key == idempotency_key)
        )
        if existing is not None:
            return existing

    r = AgentRun(
        org_id=org_id,
        property_id=property_id,
        agent_key=agent_key,
        status="queued",
        input_json=json.dumps(input_json or {}, ensure_ascii=False),
        output_json=None,
        idempotency_key=idempotency_key,
        created_at=datetime.utcnow(),
        started_at=None,
        finished_at=None,
        attempts=0,
        last_error=None,
        heartbeat_at=None,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def execute_run_now(db: Session, *, org_id: int, run_id: int) -> dict:
    run = db.scalar(select(AgentRun).where(AgentRun.id == run_id).where(AgentRun.org_id == org_id))
    if run is None:
        raise ValueError("run not found")

    if run.status in {"done", "failed", "timed_out"}:
        # idempotent
        return {"status": run.status, "output": json.loads(run.output_json or "{}")}

    try:
        out = execute_agent(db, org_id=org_id, run=run)
        run.status = "done"
        run.output_json = json.dumps(out, ensure_ascii=False)
        run.finished_at = datetime.utcnow()
        db.commit()
        return out
    except Exception as e:
        run.status = "failed"
        run.last_error = f"{type(e).__name__}: {e}"
        run.finished_at = datetime.utcnow()
        db.commit()
        raise


def enqueue_run(db: Session, *, org_id: int, run_id: int) -> dict:
    """
    Prefer Celery if broker configured; otherwise keep 'queued' for poll-worker.
    """
    if settings.celery_broker_url:
        from ..workers.agent_tasks import execute_agent_run_task

        execute_agent_run_task.delay(org_id=org_id, run_id=run_id)
        return {"enqueued": True, "via": "celery"}

    return {"enqueued": True, "via": "poll_worker"}