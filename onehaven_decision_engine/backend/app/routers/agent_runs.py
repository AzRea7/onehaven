# backend/app/routers/agent_runs.py
from __future__ import annotations

import json
import time
from typing import Any, Optional, Generator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db, SessionLocal
from ..models import AgentRun, AgentMessage, AgentTraceEvent
from ..services.agent_engine import create_run, mark_approved
from ..services.agent_orchestrator import plan_agent_runs
from ..services.agent_actions import apply_run_actions
from ..workers.agent_tasks import execute_agent_run

router = APIRouter(prefix="/agent-runs", tags=["agents"])


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


@router.get("/")
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


@router.get("/{run_id}")
def get_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    r = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == principal.org_id))
    if r is None:
        return {"detail": "Not Found"}

    return {
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
        "output": _loads(getattr(r, "output_json", None), {}),
        "proposed": _loads(getattr(r, "proposed_actions_json", None), []),
        "idempotency_key": getattr(r, "idempotency_key", None),
    }


@router.get("/{run_id}/messages")
def get_run_messages(
    run_id: int,
    after_id: int | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == principal.org_id))
    if run is None:
        return {"detail": "Not Found"}

    q = (
        select(AgentMessage)
        .where(AgentMessage.org_id == principal.org_id)
        .where(AgentMessage.run_id == int(run_id))
        .order_by(AgentMessage.id.asc())
    )
    if after_id is not None:
        q = q.where(AgentMessage.id > int(after_id))

    rows = list(db.scalars(q.limit(int(limit))).all())
    out: list[dict[str, Any]] = []

    for m in rows:
        try:
            evt = json.loads(m.message or "{}")
        except Exception:
            evt = {"type": "raw", "message": m.message}

        out.append(
            {
                "id": int(m.id),
                "run_id": int(m.run_id) if getattr(m, "run_id", None) is not None else None,
                "thread_key": getattr(m, "thread_key", None),
                "sender": getattr(m, "sender", None),
                "recipient": getattr(m, "recipient", None),
                "created_at": getattr(m, "created_at", None),
                "event": evt,
            }
        )

    return out


@router.get("/{run_id}/stream")
def stream_run_messages(
    run_id: int,
    since_id: int | None = Query(default=None),
    poll_ms: int = Query(default=750, ge=200, le=5000),
    principal=Depends(get_principal),
):
    """
    SSE stream is powered by agent_trace_events (durable, structured).
    """
    # validate once outside generator
    db0 = SessionLocal()
    try:
        run = db0.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == principal.org_id))
        if run is None:
            return {"detail": "Not Found"}
    finally:
        db0.close()

    start_id = int(since_id or 0)

    def gen() -> Generator[str, None, None]:
        last_id = start_id
        yield "retry: 1500\n\n"
        yield "event: hello\ndata: {}\n\n"

        while True:
            db = SessionLocal()
            try:
                q = (
                    select(AgentTraceEvent)
                    .where(AgentTraceEvent.org_id == principal.org_id)
                    .where(AgentTraceEvent.run_id == int(run_id))
                    .where(AgentTraceEvent.id > int(last_id))
                    .order_by(AgentTraceEvent.id.asc())
                    .limit(500)
                )
                rows = list(db.scalars(q).all())
            finally:
                db.close()

            if rows:
                for ev in rows:
                    last_id = max(last_id, int(ev.id))

                    raw = getattr(ev, "payload_json", None) or "{}"
                    try:
                        decoded = json.loads(raw)
                    except Exception:
                        decoded = {"type": "raw", "raw": raw}

                    out = {
                        "id": int(ev.id),
                        "created_at": str(getattr(ev, "created_at", "")),
                        "agent_key": getattr(ev, "agent_key", None),
                        "event_type": getattr(ev, "event_type", None),
                        # full decoded envelope (type/level/ts/payload)
                        "event": decoded,
                        # convenience: the "payload" field only
                        "payload": decoded.get("payload") if isinstance(decoded, dict) else None,
                    }
                    yield f"event: trace\ndata: {json.dumps(out)}\n\n"
            else:
                yield "event: ping\ndata: {}\n\n"

            time.sleep(float(poll_ms) / 1000.0)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


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
            "input_payload": {},
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
            input_payload={},
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
