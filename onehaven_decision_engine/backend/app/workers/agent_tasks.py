# backend/app/workers/agent_tasks.py
from __future__ import annotations

from datetime import datetime, timedelta

from celery import shared_task
from sqlalchemy import select

from .celery_app import celery_app
from ..db import SessionLocal
from ..models import AgentRun
from ..services.agent_engine import execute_run_now


@shared_task(bind=True, max_retries=3, default_retry_delay=5)
def execute_agent_run_task(self, org_id: int, run_id: int) -> dict:
    db = SessionLocal()
    try:
        run = db.scalar(select(AgentRun).where(AgentRun.id == run_id).where(AgentRun.org_id == org_id))
        if run is None:
            return {"ok": False, "reason": "run_not_found"}

        # idempotency: if already finished, do nothing
        if run.status in {"done", "failed", "timed_out"}:
            return {"ok": True, "status": run.status, "idempotent": True}

        # mark running / attempt
        run.status = "running"
        run.started_at = run.started_at or datetime.utcnow()
        run.attempts = int(getattr(run, "attempts", 0) or 0) + 1
        run.heartbeat_at = datetime.utcnow()
        db.commit()

        out = execute_run_now(db, org_id=org_id, run_id=run_id)
        return {"ok": True, "output": out}
    except Exception as e:
        # store error and retry bounded
        try:
            run = db.scalar(select(AgentRun).where(AgentRun.id == run_id))
            if run is not None:
                run.last_error = f"{type(e).__name__}: {e}"
                run.heartbeat_at = datetime.utcnow()
                db.commit()
        except Exception:
            db.rollback()

        if self.request.retries >= 2:
            # fail hard
            try:
                run = db.scalar(select(AgentRun).where(AgentRun.id == run_id))
                if run is not None:
                    run.status = "failed"
                    run.finished_at = datetime.utcnow()
                    db.commit()
            except Exception:
                db.rollback()
            return {"ok": False, "reason": "failed_final", "error": str(e)}

        raise self.retry(exc=e)
    finally:
        db.close()


celery_app.tasks.register(execute_agent_run_task)