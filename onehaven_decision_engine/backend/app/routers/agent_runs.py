# onehaven_decision_engine/backend/app/routers/agent_runs.py
from __future__ import annotations
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.auth import get_principal, require_owner
from app.db import get_db
from app.models import AgentRun
from app.services.agent_engine import create_run, mark_approved
from app.services.agent_orchestrator import plan_agent_runs
from app.services.agent_actions import apply_run_actions
from app.workers.agent_tasks import execute_agent_run


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
            "id": r.id,
            "org_id": r.org_id,
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
    return [
        {
            "agent_key": p.agent_key,
            "property_id": p.property_id,
            "reason": p.reason,
            "input_payload": p.input_payload,
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
    created = []
    for p in plan:
        r = create_run(
            db,
            org_id=principal.org_id,
            actor_user_id=principal.user_id,
            agent_key=p.agent_key,
            property_id=p.property_id,
            input_payload=p.input_payload,
            idempotency_key=p.idempotency_key,
        )
        created.append(r)

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


@router.post("/{run_id}/reject")
def reject_run(
    run_id: int,
    reason: str = Query(default="rejected_by_owner"),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)

    r = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if r is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    # If it's already terminal, don't mutate history (idempotent behavior)
    if r.status in {"done", "failed"}:
        return {
            "ok": True,
            "run_id": int(r.id),
            "status": r.status,
            "approval_status": getattr(r, "approval_status", None),
            "last_error": getattr(r, "last_error", None),
        }

    # Deterministic reject path:
    # - "blocked" => owner rejected proposed mutations
    # - "queued/running" => treat as cancellation + rejection (still auditable)
    r.status = "failed"
    r.approval_status = "rejected"
    r.last_error = f"rejected: {reason}"

    # Safety: clear proposed actions so nothing can be applied later by accident
    if hasattr(r, "proposed_actions_json"):
        r.proposed_actions_json = None

    # Stamp finished_at for consistency with other terminal transitions
    if hasattr(r, "finished_at"):
        r.finished_at = datetime.utcnow()

    db.add(r)

    # Emit workflow event for audit trail
    db.add(
        WorkflowEvent(
            org_id=principal.org_id,
            property_id=r.property_id,
            actor_user_id=principal.user_id,
            event_type="agent_run_rejected",
            payload_json=json.dumps(
                {
                    "run_id": int(r.id),
                    "agent_key": str(r.agent_key),
                    "reason": reason,
                    "prev_status": str(getattr(r, "status", None)),
                }
            ),
            created_at=datetime.utcnow(),
        )
    )

    db.commit()
    db.refresh(r)

    return {
        "ok": True,
        "run_id": int(r.id),
        "status": r.status,
        "approval_status": r.approval_status,
        "last_error": r.last_error,
        "finished_at": r.finished_at if hasattr(r, "finished_at") else None,
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