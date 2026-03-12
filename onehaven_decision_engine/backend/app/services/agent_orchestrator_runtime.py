# backend/app/services/agent_orchestrator_runtime.py
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun
from app.services.agent_engine import create_run, serialize_run
from app.services.agent_orchestrator import plan_agent_runs
from app.services.agent_trace import emit_trace_safe
from app.workers.agent_tasks import execute_agent_run

TERMINAL = {"done", "failed", "timed_out", "blocked"}


def on_run_terminal(db: Session, *, org_id: int, run_id: int) -> None:
    """
    Called by the worker after execute_run_now().
    Decides next agent runs and enqueues them with idempotency keys.

    The important bit here is not magical orchestration pixie dust.
    It is deterministic fan-out with traceability:
      - only terminal runs can trigger the planner
      - planner output is idempotent
      - every follow-up enqueue gets a trace breadcrumb
    """
    run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
    if not run:
        return

    status = str(run.status or "").lower()
    if status not in TERMINAL:
        return

    if run.property_id is None:
        return

    planned = plan_agent_runs(db, org_id=int(org_id), property_id=int(run.property_id))
    if not planned:
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(run.agent_key),
            event_type="orchestrator_no_followups",
            payload={"property_id": int(run.property_id), "status": status},
            level="info",
        )
        return

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

        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(run.agent_key),
            event_type="orchestrator_followup_planned",
            payload={
                "source_run_id": int(run.id),
                "planned_run_id": int(r.id),
                "planned_agent_key": p.agent_key,
                "idempotency_key": p.idempotency_key,
                "planned_status": serialize_run(r)["status"],
            },
            level="info",
        )

        if (r.status or "") == "queued":
            execute_agent_run.delay(org_id=int(org_id), run_id=int(r.id))

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(run.agent_key),
        event_type="orchestrator_fanout_complete",
        payload={"created_run_ids": created_ids, "count": len(created_ids)},
        level="info",
    )
