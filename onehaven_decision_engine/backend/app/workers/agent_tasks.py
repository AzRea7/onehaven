# backend/app/workers/agent_tasks.py
from __future__ import annotations

from datetime import datetime
import os
import random

from sqlalchemy import select

from ..config import settings
from ..db import SessionLocal
from ..models import AgentRun, AgentRunDeadletter
from ..services.agent_engine import execute_run_now, sweep_stuck_runs
from ..services.agent_orchestrator import on_run_terminal
from ..services.trust_service import record_signal, recompute_and_persist
from .celery_app import celery_app

TERMINAL = {"done", "failed", "timed_out", "blocked"}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _backoff_seconds(retries: int) -> int:
    base = int(getattr(settings, "agents_retry_base_seconds", 5) or 5)
    cap = int(getattr(settings, "agents_retry_max_seconds", 120) or 120)
    delay = min(cap, base * (2 ** max(0, int(retries))))
    jitter = int(delay * 0.2)
    if jitter > 0:
        delay = max(1, delay + random.randint(-jitter, jitter))
    return delay


def _emit_agent_trust(db, *, org_id: int, run: AgentRun) -> None:
    """
    Best-effort: never crash worker because trust failed.
    """
    try:
        status = (run.status or "").lower()
        ok = 1.0 if status == "done" else 0.0

        record_signal(
            db,
            org_id=int(org_id),
            entity_type="agent",
            entity_id=str(run.agent_key),
            signal_key="agent.run.success",
            value=ok,
            weight=1.0,
            meta={"run_id": int(run.id), "status": status},
        )
        recompute_and_persist(db, org_id=int(org_id), entity_type="agent", entity_id=str(run.agent_key))
    except Exception:
        pass


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    name="app.workers.agent_tasks.execute_agent_run",
)
def execute_agent_run(self, org_id: int, run_id: int) -> dict:
    db = SessionLocal()
    try:
        run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
        if run is None:
            return {"ok": False, "reason": "run_not_found"}

        if (run.status or "").lower() in TERMINAL:
            return {"ok": True, "status": run.status, "idempotent": True}

        attempt_number = int(getattr(run, "attempts", 0) or 0) + 1
        try:
            run.attempts = attempt_number
            run.heartbeat_at = _utcnow()
            db.add(run)
            db.commit()
        except Exception:
            db.rollback()

        result = execute_run_now(db, org_id=int(org_id), run_id=int(run_id), attempt_number=attempt_number)

        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2 and (run2.status or "").lower() in TERMINAL:
                _emit_agent_trust(db, org_id=int(org_id), run=run2)
                on_run_terminal(db, org_id=int(org_id), run_id=int(run_id))
                db.commit()
        except Exception as chain_err:
            db.rollback()
            try:
                run3 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
                db.add(
                    AgentRunDeadletter(
                        org_id=int(org_id),
                        run_id=int(run_id),
                        agent_key=str(run3.agent_key if run3 else "unknown"),
                        reason=f"orchestrator_error:{type(chain_err).__name__}",
                        error=str(chain_err),
                        created_at=_utcnow(),
                    )
                )
                db.commit()
            except Exception:
                db.rollback()

        return {"ok": True, "result": result, "attempt": attempt_number}

    except Exception as e:
        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2:
                run2.last_error = f"{type(e).__name__}: {e}"
                run2.heartbeat_at = _utcnow()
                db.add(run2)
                db.commit()
        except Exception:
            db.rollback()

        retries = int(getattr(self.request, "retries", 0) or 0)
        max_retries = int(getattr(self, "max_retries", 3) or 3)
        is_final = retries >= (max_retries - 1)

        if is_final:
            try:
                run3 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
                if run3:
                    run3.status = "failed"
                    run3.finished_at = _utcnow()
                    run3.last_error = f"{type(e).__name__}: {e}"
                    db.add(run3)
                    db.add(
                        AgentRunDeadletter(
                            org_id=int(org_id),
                            run_id=int(run_id),
                            agent_key=str(getattr(run3, "agent_key", "unknown")),
                            reason="poison_run_final_retry",
                            error=str(e),
                            created_at=_utcnow(),
                        )
                    )
                    db.commit()

                    _emit_agent_trust(db, org_id=int(org_id), run=run3)

                    try:
                        on_run_terminal(db, org_id=int(org_id), run_id=int(run_id))
                        db.commit()
                    except Exception:
                        db.rollback()
            except Exception:
                db.rollback()

            return {"ok": False, "reason": "failed_final", "error": str(e), "retries": retries}

        delay = _backoff_seconds(retries=retries)
        raise self.retry(exc=e, countdown=delay)

    finally:
        db.close()


@celery_app.task(name="app.workers.agent_tasks.sweep_stuck_agent_runs")
def sweep_stuck_agent_runs() -> dict:
    db = SessionLocal()
    try:
        timeout_s = int(getattr(settings, "agents_run_timeout_seconds", 120) or 120)
        env_timeout = os.getenv("AGENTS_RUN_TIMEOUT_SECONDS")
        if env_timeout:
            try:
                timeout_s = int(env_timeout)
            except Exception:
                pass

        res = sweep_stuck_runs(db, timeout_seconds=timeout_s, queued_max_hours=12)
        return {"ok": True, "sweep": res}
    finally:
        db.close()
        