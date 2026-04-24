# backend/app/services/agent_orchestrator_runtime.py
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun
from onehaven_platform.backend.src.services.agent_engine import create_run, serialize_run
from onehaven_platform.backend.src.services.agent_orchestrator import plan_agent_runs
from onehaven_platform.backend.src.services.agent_trace import emit_trace_safe

TERMINAL = {"done", "failed", "timed_out", "blocked"}


def on_run_terminal(db: Session, *, org_id: int, run_id: int) -> None:
    """
    Called by the worker after a run reaches terminal-ish state.

    This layer is intentionally deterministic:
      - only terminal runs can trigger follow-up planning
      - follow-up runs are created via idempotency keys
      - only newly queued runs are dispatched
      - every fan-out step leaves trace breadcrumbs
    """
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == int(org_id),
        )
    )
    if run is None:
        return

    status = str(getattr(run, "status", "") or "").lower()
    if status not in TERMINAL:
        return

    property_id = getattr(run, "property_id", None)
    if property_id is None:
        return

    planned = plan_agent_runs(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
    )

    if not planned:
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(getattr(run, "agent_key", "")),
            event_type="orchestrator_no_followups",
            payload={"property_id": int(property_id), "status": status},
            level="info",
            property_id=int(property_id),
        )
        return

    created_ids: list[int] = []

    for p in planned:
        created = create_run(
            db,
            org_id=int(org_id),
            actor_user_id=getattr(run, "created_by_user_id", None),
            agent_key=str(p.agent_key),
            property_id=int(p.property_id),
            input_payload={},
            idempotency_key=str(p.idempotency_key),
        )
        created_ids.append(int(created.id))

        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(getattr(run, "agent_key", "")),
            event_type="orchestrator_followup_planned",
            payload={
                "source_run_id": int(run.id),
                "planned_run_id": int(created.id),
                "planned_agent_key": str(p.agent_key),
                "idempotency_key": str(p.idempotency_key),
                "planned_status": serialize_run(created)["status"],
            },
            level="info",
            property_id=int(property_id),
        )

        if (getattr(created, "status", "") or "").lower() == "queued":
            from app.workers.agent_tasks import execute_agent_run

            execute_agent_run.delay(org_id=int(org_id), run_id=int(created.id))

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(getattr(run, "agent_key", "")),
        event_type="orchestrator_fanout_complete",
        payload={"created_run_ids": created_ids, "count": len(created_ids)},
        level="info",
        property_id=int(property_id),
    )