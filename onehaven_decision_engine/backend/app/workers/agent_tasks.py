# onehaven_decision_engine/backend/app/workers/agent_tasks.py
from __future__ import annotations

import os
from celery import shared_task

from app.db import SessionLocal
from app.services.agent_engine import execute_run_now


@shared_task(bind=True, name="app.workers.agent_tasks.execute_agent_run")
def execute_agent_run(self, *, org_id: int, run_id: int) -> dict:
    """
    Celery task: execute an AgentRun with retries, idempotency, and timeout semantics.
    """
    max_retries = int(os.getenv("AGENTS_MAX_RETRIES", "3"))

    db = SessionLocal()
    try:
        # If execute_run_now raises, we retry. If it returns failed status, we do not retry.
        out = execute_run_now(db, org_id=org_id, run_id=run_id, attempt_number=(self.request.retries + 1))
        if out.get("status") == "retryable_error":
            raise RuntimeError(out.get("error") or "retryable_error")
        return out
    except Exception as e:
        if self.request.retries >= max_retries:
            # Final failure: mark run as failed inside engine (it’s safe to call it; it’s idempotent)
            try:
                execute_run_now(db, org_id=org_id, run_id=run_id, attempt_number=max_retries, force_fail=str(e))
            except Exception:
                pass
            raise
        raise self.retry(exc=e, countdown=min(5 * (2 ** self.request.retries), 60))
    finally:
        db.close()