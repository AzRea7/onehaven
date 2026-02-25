# backend/app/workers/agent_tasks.py
from __future__ import annotations

from datetime import datetime
import os

from sqlalchemy import select

from ..db import SessionLocal
from ..models import AgentRun, AgentRunDeadletter
from ..services.agent_engine import execute_run_now, sweep_stuck_runs
from ..services.agent_orchestrator import on_run_terminal
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
    Safe to retry; execute_run_now owns lifecycle transitions + tracing.
    After terminal: orchestrator may enqueue next runs.
    """
    db = SessionLocal()
    try:
        run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
        if run is None:
            return {"ok": False, "reason": "run_not_found"}

        if (run.status or "").lower() in TERMINAL:
            return {"ok": True, "status": run.status, "idempotent": True}

        attempt = int(run.attempts or 0) + 1
        result = execute_run_now(db, org_id=int(org_id), run_id=int(run_id), attempt_number=attempt)

        # ✅ chaining hook
        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2 and (run2.status or "").lower() in TERMINAL:
                on_run_terminal(db, org_id=int(org_id), run_id=int(run_id))
                db.commit()
        except Exception as chain_err:
            db.rollback()
            # chaining errors should never break worker; record as deadletter
            try:
                run3 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
                db.add(
                    AgentRunDeadletter(
                        org_id=int(org_id),
                        run_id=int(run_id),
                        agent_key=str(run3.agent_key if run3 else "unknown"),
                        reason=f"orchestrator_error: {type(chain_err).__name__}",
                        payload_json=str({"error": str(chain_err)}).replace("'", '"'),
                        created_at=datetime.utcnow(),
                    )
                )
                db.commit()
            except Exception:
                db.rollback()

        return {"ok": True, "result": result}

    except Exception as e:
        # Persist error for visibility
        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2:
                run2.last_error = f"{type(e).__name__}: {e}"
                run2.heartbeat_at = datetime.utcnow()
                db.commit()
        except Exception:
            db.rollback()

        if self.request.retries >= 2:
            try:
                run3 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
                if run3:
                    run3.status = "failed"
                    run3.finished_at = datetime.utcnow()
                    db.add(run3)
                    db.add(
                        AgentRunDeadletter(
                            org_id=int(org_id),
                            run_id=int(run_id),
                            agent_key=str(run3.agent_key),
                            reason="poison_run_final_retry",
                            payload_json=str({"error": str(e)}).replace("'", '"'),
                            created_at=datetime.utcnow(),
                        )
                    )
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
    Requires celery-beat to schedule.
    """
    db = SessionLocal()
    try:
        timeout_s = int(os.getenv("AGENTS_RUN_TIMEOUT_SECONDS", "120"))
        res = sweep_stuck_runs(db, timeout_seconds=timeout_s, queued_max_hours=12)
        return {"ok": True, "sweep": res}
    finally:
        db.close()
        