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
from .celery_app import celery_app

TERMINAL = {"done", "failed", "timed_out", "blocked"}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _backoff_seconds(retries: int) -> int:
    """
    Exponential backoff with jitter.
    retries is the current retry count (0 for first retry attempt).
    """
    base = int(getattr(settings, "agents_retry_base_seconds", 5) or 5)
    cap = int(getattr(settings, "agents_retry_max_seconds", 120) or 120)

    # exponential: base * 2^retries, capped
    delay = min(cap, base * (2 ** max(0, int(retries))))

    # jitter: +/- 20%
    jitter = int(delay * 0.2)
    if jitter > 0:
        delay = max(1, delay + random.randint(-jitter, jitter))
    return delay


@celery_app.task(
    bind=True,
    max_retries=3,  # total retries after the initial attempt
    default_retry_delay=5,  # fallback; we override countdown dynamically
    name="app.workers.agent_tasks.execute_agent_run",
)
def execute_agent_run(self, org_id: int, run_id: int) -> dict:
    """
    Executes a single AgentRun.

    Safe to retry; execute_run_now owns lifecycle transitions + tracing.
    After terminal: orchestrator may enqueue next runs (best-effort) and refresh property state.

    Professional behavior:
      - exponential backoff retries
      - poison-run deadletter on final failure
      - never crash the worker on orchestrator hook failures
    """
    db = SessionLocal()
    try:
        run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
        if run is None:
            return {"ok": False, "reason": "run_not_found"}

        # idempotency: if already terminal, do nothing
        if (run.status or "").lower() in TERMINAL:
            return {"ok": True, "status": run.status, "idempotent": True}

        # bump attempts (persisted so operators can see retry history)
        attempt_number = int(getattr(run, "attempts", 0) or 0) + 1
        try:
            run.attempts = attempt_number
            run.heartbeat_at = _utcnow()
            db.add(run)
            db.commit()
        except Exception:
            db.rollback()

        # execute
        result = execute_run_now(db, org_id=int(org_id), run_id=int(run_id), attempt_number=attempt_number)

        # chaining hook (best-effort; never break worker)
        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2 and (run2.status or "").lower() in TERMINAL:
                on_run_terminal(db, org_id=int(org_id), run_id=int(run_id))
                db.commit()
        except Exception as chain_err:
            db.rollback()
            # record deadletter but keep worker alive
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
        # Persist error for visibility
        try:
            run2 = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
            if run2:
                run2.last_error = f"{type(e).__name__}: {e}"
                run2.heartbeat_at = _utcnow()
                db.add(run2)
                db.commit()
        except Exception:
            db.rollback()

        # Determine if this is the final failure.
        # Celery increments request.retries AFTER scheduling retry, so:
        # - retries==0 means we're about to schedule the 1st retry
        # - retries==2 means we're about to schedule the 3rd retry
        retries = int(getattr(self.request, "retries", 0) or 0)
        max_retries = int(getattr(self, "max_retries", 3) or 3)
        is_final = retries >= (max_retries - 1)

        if is_final:
            # poison-run handling after final retry
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

                    # best-effort terminal hook (so the system can move on / mark pipeline)
                    try:
                        on_run_terminal(db, org_id=int(org_id), run_id=int(run_id))
                        db.commit()
                    except Exception:
                        db.rollback()
            except Exception:
                db.rollback()

            return {"ok": False, "reason": "failed_final", "error": str(e), "retries": retries}

        # schedule retry with exponential backoff
        delay = _backoff_seconds(retries=retries)
        raise self.retry(exc=e, countdown=delay)

    finally:
        db.close()


@celery_app.task(name="app.workers.agent_tasks.sweep_stuck_agent_runs")
def sweep_stuck_agent_runs() -> dict:
    """
    Periodic self-healing sweep.
    Requires celery-beat to schedule.

    - marks running runs as timed_out if heartbeat too old
    - can requeue queued runs stuck too long (depending on sweep_stuck_runs behavior)
    """
    db = SessionLocal()
    try:
        # Prefer settings; allow env override as last-resort
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
        