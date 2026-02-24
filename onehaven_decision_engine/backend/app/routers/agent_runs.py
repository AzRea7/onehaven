# backend/app/routers/agent_runs.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..models import AgentRun
from ..services.agent_engine import create_run, mark_approved
from ..services.agent_orchestrator import plan_agent_runs
from ..services.agent_actions import apply_run_actions

# If your worker is Celery-based, this import is fine.
# If it's not configured yet, keep dispatch=false in calls.
from ..workers.agent_tasks import execute_agent_run

router = APIRouter(prefix="/agent-runs", tags=["agents"])


@router.get("")
def list_runs(
    property_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = select(AgentRun).where(AgentRun.org_id == principal.org_id).order_by(AgentRun.id.desc())
    if property_id:
        q = q.where(AgentRun.property_id == property_id)
    rows = db.scalars(q).all()

    return [
        {
            "id": int(r.id),
            "org_id": int(r.org_id),
            "property_id": r.property_id,
            "agent_key": r.agent_key,
            "status": r.status,
            "attempts": getattr(r, "attempts", 0),
            "last_error": getattr(r, "last_error", None),
            "created_at": r.created_at,
            "started_at": getattr(r, "started_at", None),
            "finished_at": getattr(r, "finished_at", None),
            "approval_status": getattr(r, "approval_status", None),
            "approved_at": getattr(r, "approved_at", None),
            "output_json": getattr(r, "output_json", None),
            "proposed_actions_json": getattr(r, "proposed_actions_json", None),
        }
        for r in rows
    ]


@router.post("/plan")
def plan_runs(
    property_id: int = Query(...),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    plan = plan_agent_runs(db, org_id=principal.org_id, property_id=property_id)

    # PlannedRun does NOT contain input_payload (yet). Provide a stable default.
    # If later you want agent-specific inputs, build them here based on agent_key.
    return [
        {
            "agent_key": p.agent_key,
            "property_id": p.property_id,
            "reason": p.reason,
            "input_payload": {},  # ✅ safe default
            "idempotency_key": p.idempotency_key,
        }
        for p in plan
    ]


@router.post("/enqueue")
def enqueue_planned_runs(
    property_id: int = Query(...),
    dispatch: bool = Query(default=True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    plan = plan_agent_runs(db, org_id=principal.org_id, property_id=property_id)

    created: list[AgentRun] = []
    for p in plan:
        r = create_run(
            db,
            org_id=principal.org_id,
            actor_user_id=principal.user_id,
            agent_key=p.agent_key,
            property_id=p.property_id,
            input_payload={},  # ✅ safe default until you add agent-specific inputs
            idempotency_key=p.idempotency_key,
        )
        created.append(r)

        # Dispatch is optional; keep false while worker wiring is incomplete.
        if dispatch and r.status == "queued":
            execute_agent_run.delay(org_id=principal.org_id, run_id=int(r.id))

    return {"planned": len(plan), "created": [int(r.id) for r in created]}


@router.post("/{run_id}/dispatch")
def dispatch_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    r = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == principal.org_id))
    if r is None:
        return {"ok": False, "error": "not_found"}

    execute_agent_run.delay(org_id=principal.org_id, run_id=int(r.id))
    return {"ok": True, "queued": True, "run_id": int(r.id)}


@router.post("/{run_id}/approve")
def approve_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)
    r = mark_approved(db, org_id=principal.org_id, actor_user_id=principal.user_id, run_id=int(run_id))
    return {
        "ok": True,
        "run_id": int(r.id),
        "status": r.status,
        "approval_status": getattr(r, "approval_status", None),
        "approved_at": getattr(r, "approved_at", None),
    }


@router.post("/{run_id}/apply")
def apply_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)
    res = apply_run_actions(db, org_id=principal.org_id, actor_user_id=principal.user_id, run_id=int(run_id))
    return {
        "ok": res.ok,
        "status": res.status,
        "run_id": res.run_id,
        "applied_count": res.applied_count,
        "errors": res.errors,
    }
