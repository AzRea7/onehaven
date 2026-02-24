# backend/app/workers/agent_tasks.py 
from __future__ import annotations

from datetime import datetime
import os

from sqlalchemy import select

from ..db import SessionLocal
from ..models import AgentRun
from ..services.agent_engine import execute_run_now, sweep_stuck_runs
from .celery_app import celery_app

TERMINAL = {"done", "failed", "timed_out"}


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    name="app.workers.agent_tasks.execute_agent_run",
)
def execute_agent_run(self, org_id: int, run_id: int) -> dict:
    """
    Executes a single AgentRun.
    Safe to retry, idempotent on terminal states.
    """
    db = SessionLocal()
    try:
        run = db.scalar(
            select(AgentRun)
            .where(AgentRun.id == run_id)
            .where(AgentRun.org_id == org_id)
        )
        if run is None:
            return {"ok": False, "reason": "run_not_found"}

        # Idempotency: do nothing if already terminal
        if run.status in TERMINAL:
            return {"ok": True, "status": run.status, "idempotent": True}

        # Mark running
        run.status = "running"
        run.started_at = run.started_at or datetime.utcnow()
        run.attempts = int(run.attempts or 0) + 1
        run.heartbeat_at = datetime.utcnow()
        db.commit()

        result = execute_run_now(
            db,
            org_id=org_id,
            run_id=run_id,
            attempt_number=int(run.attempts),
        )
        return {"ok": True, "result": result}

    except Exception as e:
        # Persist error for visibility
        try:
            run2 = db.scalar(
                select(AgentRun)
                .where(AgentRun.id == run_id)
                .where(AgentRun.org_id == org_id)
            )
            if run2:
                run2.last_error = f"{type(e).__name__}: {e}"
                run2.heartbeat_at = datetime.utcnow()
                db.commit()
        except Exception:
            db.rollback()

        # Final retry exhausted → fail hard
        if self.request.retries >= 2:
            try:
                run3 = db.scalar(
                    select(AgentRun)
                    .where(AgentRun.id == run_id)
                    .where(AgentRun.org_id == org_id)
                )
                if run3:
                    run3.status = "failed"
                    run3.finished_at = datetime.utcnow()
                    db.commit()
            except Exception:
                db.rollback()

            return {"ok": False, "reason": "failed_final", "error": str(e)}

        raise self.retry(exc=e)

    finally:
        db.close()


@celery_app.task(name="app.workers.agent_tasks.sweep_stuck_agent_runs")
def sweep_stuck_agent_runs() -> dict:
    """
    Periodic self-healing sweep.
    Requires celery-beat if you want it scheduled.
    """
    db = SessionLocal()
    try:
        timeout_s = int(os.getenv("AGENTS_RUN_TIMEOUT_SECONDS", "120"))
        res = sweep_stuck_runs(
            db,
            timeout_seconds=timeout_s,
            queued_max_hours=12,
        )
        return {"ok": True, "sweep": res}
    finally:
        db.close()
