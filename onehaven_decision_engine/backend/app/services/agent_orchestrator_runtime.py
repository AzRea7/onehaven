# backend/app/services/agent_orchestrator_runtime.py
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun
from app.services.agent_engine import create_run
from app.services.agent_orchestrator import plan_agent_runs
from app.workers.agent_tasks import execute_agent_run


TERMINAL = {"done", "failed", "timed_out"}


def on_run_terminal(db: Session, *, org_id: int, run_id: int) -> None:
    """
    Called by the worker after execute_run_now().
    Decides next agent runs and enqueues them with idempotency keys.
    """
    run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
    if not run:
        return

    st = str(run.status or "").lower()
    if st not in TERMINAL and st != "blocked":
        return

    if run.property_id is None:
        return

    # Plan next stage runs (idempotent thanks to idempotency_key)
    planned = plan_agent_runs(db, org_id=int(org_id), property_id=int(run.property_id))

    created_ids: list[int] = []
    for p in planned:
        r = create_run(
            db,
            org_id=int(org_id),
            actor_user_id=run.created_by_user_id,
            agent_key=p.agent_key,
            property_id=p.property_id,
            input_payload={},
            idempotency_key=p.idempotency_key,
        )
        created_ids.append(int(r.id))

        # dispatch only if it is still queued (idempotency could return an old run)
        if (r.status or "") == "queued":
            execute_agent_run.delay(org_id=int(org_id), run_id=int(r.id))

    # caller commits