# backend/app/routers/agent_runs.py
from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Generator, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from app.db import SessionLocal, get_db
from app.models import AgentMessage, AgentRun, AgentRunDeadletter, AgentSlotAssignment, AgentTraceEvent
from app.services.agent_engine import (
    apply_approved,
    create_run,
    mark_approved,
    reject_run,
    serialize_run,
)
from app.services.agent_orchestrator import plan_agent_runs
from ..workers.agent_tasks import execute_agent_run

router = APIRouter(prefix="/agent-runs", tags=["agents"])

TERMINAL = {"done", "failed", "timed_out", "blocked"}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def _serialize_trace(ev: AgentTraceEvent) -> dict[str, Any]:
    raw = getattr(ev, "payload_json", None) or "{}"
    decoded = _loads(raw, {})
    if not isinstance(decoded, dict):
        decoded = {"type": "raw", "raw": raw, "payload": {"raw": raw}}

    return {
        "id": int(ev.id),
        "run_id": int(ev.run_id),
        "property_id": getattr(ev, "property_id", None),
        "created_at": getattr(ev, "created_at", None),
        "agent_key": getattr(ev, "agent_key", None),
        "event_type": getattr(ev, "event_type", None),
        "event": decoded,
        "payload": decoded.get("payload") if isinstance(decoded.get("payload"), dict) else {},
        "level": decoded.get("level"),
        "ts": decoded.get("ts"),
    }


def _run_metrics(db: Session, *, org_id: int, run_id: int) -> dict[str, Any]:
    trace_rows = db.scalars(
        select(AgentTraceEvent)
        .where(AgentTraceEvent.org_id == int(org_id), AgentTraceEvent.run_id == int(run_id))
        .order_by(AgentTraceEvent.id.asc())
    ).all()
    message_rows = db.scalars(
        select(AgentMessage)
        .where(AgentMessage.org_id == int(org_id), AgentMessage.run_id == int(run_id))
        .order_by(AgentMessage.id.asc())
    ).all()

    level_counts = {"info": 0, "warn": 0, "error": 0}
    for ev in trace_rows:
        decoded = _loads(getattr(ev, "payload_json", None), {})
        level = str((decoded or {}).get("level") or "info").lower()
        if level in level_counts:
            level_counts[level] += 1

    return {
        "trace_count": len(trace_rows),
        "message_count": len(message_rows),
        "last_trace_at": trace_rows[-1].created_at if trace_rows else None,
        "level_counts": level_counts,
    }


def _serialize_run_detail(db: Session, run: AgentRun) -> dict[str, Any]:
    payload = serialize_run(run)
    payload.update(_run_metrics(db, org_id=int(run.org_id), run_id=int(run.id)))
    return payload


def _history_filters_query(principal, *, agent_key: str | None, property_id: int | None, status: str | None):
    q = select(AgentRun).where(AgentRun.org_id == principal.org_id)
    if property_id is not None:
        q = q.where(AgentRun.property_id == int(property_id))
    if agent_key:
        q = q.where(AgentRun.agent_key == str(agent_key).strip())
    if status:
        q = q.where(AgentRun.status == str(status).strip())
    return q


@router.get("/")
def list_runs(
    property_id: int | None = Query(default=None),
    agent_key: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = db.scalars(
        _history_filters_query(
            principal,
            agent_key=agent_key,
            property_id=property_id,
            status=status,
        )
        .order_by(AgentRun.id.desc())
        .limit(int(limit))
    ).all()
    return [_serialize_run_detail(db, r) for r in rows]


@router.get("/summary")
def runs_summary(
    property_id: int | None = Query(default=None),
    agent_key: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=250, ge=10, le=1000),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    rows = db.scalars(
        _history_filters_query(
            principal,
            agent_key=agent_key,
            property_id=property_id,
            status=status,
        )
        .order_by(AgentRun.id.desc())
        .limit(int(limit))
    ).all()

    by_status: dict[str, int] = {}
    by_agent: dict[str, dict[str, Any]] = {}
    pending_approval = 0
    stale_running = 0
    failures = 0
    avg_duration_samples: list[int] = []

    for r in rows:
        item = serialize_run(r)
        st = str(item["status"])
        by_status[st] = by_status.get(st, 0) + 1

        ak = str(item["agent_key"])
        entry = by_agent.setdefault(
            ak,
            {
                "agent_key": ak,
                "total": 0,
                "done": 0,
                "failed": 0,
                "blocked": 0,
                "queued": 0,
                "running": 0,
                "timed_out": 0,
                "last_run_id": 0,
                "last_status": None,
                "last_property_id": None,
            },
        )
        entry["total"] += 1
        entry[st] = entry.get(st, 0) + 1
        if int(item["id"]) > int(entry["last_run_id"]):
            entry["last_run_id"] = int(item["id"])
            entry["last_status"] = st
            entry["last_property_id"] = item["property_id"]

        if item["approval_status"] == "pending":
            pending_approval += 1
        if item["runtime_health"] == "stale":
            stale_running += 1
        if st in {"failed", "timed_out"}:
            failures += 1
        if isinstance(item["duration_ms"], int):
            avg_duration_samples.append(int(item["duration_ms"]))

    average_duration_ms = int(sum(avg_duration_samples) / len(avg_duration_samples)) if avg_duration_samples else None

    return {
        "total": len(rows),
        "pending_approval": pending_approval,
        "stale_running": stale_running,
        "failures": failures,
        "average_duration_ms": average_duration_ms,
        "by_status": by_status,
        "by_agent": sorted(by_agent.values(), key=lambda x: (-(x["total"]), x["agent_key"])),
    }


@router.get("/history")
def run_history(
    property_id: int | None = Query(default=None),
    agent_key: str | None = Query(default=None),
    status: str | None = Query(default=None),
    approval_status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = _history_filters_query(principal, agent_key=agent_key, property_id=property_id, status=status)
    if approval_status:
        q = q.where(AgentRun.approval_status == str(approval_status).strip())
    rows = db.scalars(q.order_by(AgentRun.id.desc()).limit(int(limit))).all()
    return {"rows": [_serialize_run_detail(db, r) for r in rows], "count": len(rows)}


@router.get("/compare")
def compare_runs(
    run_ids: str = Query(..., description="Comma-separated run ids"),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    parsed: list[int] = []
    for chunk in str(run_ids).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            parsed.append(int(chunk))
        except Exception:
            pass
    parsed = list(dict.fromkeys(parsed))[:4]
    if len(parsed) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two valid run_ids")

    rows = db.scalars(
        select(AgentRun)
        .where(AgentRun.org_id == principal.org_id)
        .where(AgentRun.id.in_(parsed))
        .order_by(AgentRun.id.desc())
    ).all()
    if len(rows) < 2:
        raise HTTPException(status_code=404, detail="Not enough runs found for compare")

    serialized = [_serialize_run_detail(db, r) for r in rows]
    agents = sorted({str(r["agent_key"]) for r in serialized})
    statuses = sorted({str(r["status"]) for r in serialized})
    properties = sorted({int(r["property_id"]) for r in serialized if r["property_id"] is not None})

    return {
        "rows": serialized,
        "diff": {
            "agent_keys": agents,
            "statuses": statuses,
            "property_ids": properties,
            "all_same_agent": len(agents) == 1,
            "all_same_property": len(properties) <= 1,
        },
    }


@router.get("/property/{property_id}/cockpit")
def property_agent_cockpit(
    property_id: int,
    limit: int = Query(default=30, ge=5, le=200),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    runs = db.scalars(
        select(AgentRun)
        .where(AgentRun.org_id == principal.org_id, AgentRun.property_id == int(property_id))
        .order_by(AgentRun.id.desc())
        .limit(int(limit))
    ).all()
    slots = db.scalars(
        select(AgentSlotAssignment)
        .where(AgentSlotAssignment.org_id == principal.org_id)
        .where((AgentSlotAssignment.property_id == int(property_id)) | (AgentSlotAssignment.property_id == None))  # noqa: E711
        .order_by(AgentSlotAssignment.updated_at.desc(), AgentSlotAssignment.id.desc())
    ).all()

    latest_by_agent: dict[str, dict[str, Any]] = {}
    for r in runs:
        latest_by_agent.setdefault(str(r.agent_key), _serialize_run_detail(db, r))

    slot_rows = []
    seen_slot_keys: set[str] = set()
    for s in slots:
        slot_key = str(getattr(s, "slot_key", ""))
        if not slot_key or slot_key in seen_slot_keys:
            continue
        seen_slot_keys.add(slot_key)
        slot_rows.append(
            {
                "id": int(s.id),
                "slot_key": slot_key,
                "property_id": getattr(s, "property_id", None),
                "owner_type": getattr(s, "owner_type", None),
                "assignee": getattr(s, "assignee", None),
                "status": getattr(s, "status", None),
                "notes": getattr(s, "notes", None),
                "updated_at": getattr(s, "updated_at", None),
                "latest_run": latest_by_agent.get(slot_key),
            }
        )

    return {
        "property_id": int(property_id),
        "summary": runs_summary(
            property_id=property_id,
            agent_key=None,
            status=None,
            limit=limit,
            db=db,
            principal=principal,
        ),
        "latest_runs": list(latest_by_agent.values()),
        "slots": slot_rows,
    }


@router.get("/deadletter")
def list_deadletters(
    limit: int = Query(default=50, ge=1, le=500),
    run_id: int | None = Query(default=None),
    include_acked: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = select(AgentRunDeadletter).where(AgentRunDeadletter.org_id == principal.org_id).order_by(AgentRunDeadletter.id.desc())
    if run_id is not None:
        q = q.where(AgentRunDeadletter.run_id == int(run_id))

    if not include_acked:
        try:
            q = q.where(getattr(AgentRunDeadletter, "acked_at") == None)  # noqa: E711
        except Exception:
            pass

    rows = db.scalars(q.limit(int(limit))).all()
    out: list[dict[str, Any]] = []

    for d in rows:
        out.append(
            {
                "id": int(getattr(d, "id")),
                "org_id": int(getattr(d, "org_id")),
                "run_id": int(getattr(d, "run_id")),
                "agent_key": getattr(d, "agent_key", None),
                "reason": getattr(d, "reason", None),
                "error": getattr(d, "error", None),
                "created_at": getattr(d, "created_at", None),
                "acked_at": getattr(d, "acked_at", None),
                "acked_by_user_id": getattr(d, "acked_by_user_id", None),
            }
        )
    return out


@router.post("/deadletter/{dead_id}/ack")
def ack_deadletter(
    dead_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)

    dead = db.scalar(
        select(AgentRunDeadletter).where(
            AgentRunDeadletter.id == int(dead_id),
            AgentRunDeadletter.org_id == principal.org_id,
        )
    )
    if dead is None:
        raise HTTPException(status_code=404, detail="Deadletter not found")

    changed = False
    try:
        if getattr(dead, "acked_at", None) is None:
            setattr(dead, "acked_at", _utcnow())
            changed = True
    except Exception:
        pass

    try:
        if getattr(dead, "acked_by_user_id", None) is None:
            setattr(dead, "acked_by_user_id", int(principal.user_id))
            changed = True
    except Exception:
        pass

    if changed:
        db.add(dead)
        db.commit()
        db.refresh(dead)

    return {"ok": True, "dead_id": int(dead_id)}


@router.get("/{run_id}")
def get_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")
    return _serialize_run_detail(db, run)


@router.get("/{run_id}/trace")
def get_run_trace(
    run_id: int,
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    rows = db.scalars(
        select(AgentTraceEvent)
        .where(AgentTraceEvent.org_id == principal.org_id, AgentTraceEvent.run_id == int(run_id))
        .order_by(AgentTraceEvent.id.asc())
        .limit(int(limit))
    ).all()
    return {"rows": [_serialize_trace(ev) for ev in rows], "count": len(rows)}


@router.get("/{run_id}/messages")
def get_run_messages(
    run_id: int,
    after_id: int | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

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
        out.append(
            {
                "id": int(m.id),
                "run_id": int(m.run_id) if getattr(m, "run_id", None) is not None else None,
                "thread_key": getattr(m, "thread_key", None),
                "sender": getattr(m, "sender", None),
                "recipient": getattr(m, "recipient", None),
                "created_at": getattr(m, "created_at", None),
                "event": _loads(getattr(m, "message", None), {"type": "raw", "message": getattr(m, "message", None)}),
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
    db0 = SessionLocal()
    try:
        run = db0.scalar(
            select(AgentRun).where(
                AgentRun.id == int(run_id),
                AgentRun.org_id == principal.org_id,
            )
        )
        if run is None:
            raise HTTPException(status_code=404, detail="AgentRun not found")
    finally:
        db0.close()

    start_id = int(since_id or 0)

    def gen() -> Generator[str, None, None]:
        last_id = start_id
        yield "retry: 1500\n"
        yield "event: hello\ndata: {}\n\n"

        while True:
            db = SessionLocal()
            try:
                rows = list(
                    db.scalars(
                        select(AgentTraceEvent)
                        .where(AgentTraceEvent.org_id == principal.org_id)
                        .where(AgentTraceEvent.run_id == int(run_id))
                        .where(AgentTraceEvent.id > int(last_id))
                        .order_by(AgentTraceEvent.id.asc())
                        .limit(500)
                    ).all()
                )
            finally:
                db.close()

            if rows:
                for ev in rows:
                    last_id = max(last_id, int(ev.id))
                    out = _serialize_trace(ev)
                    yield f"event: trace\ndata: {json.dumps(out, default=str)}\n\n"
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
        run = create_run(
            db,
            org_id=principal.org_id,
            actor_user_id=principal.user_id,
            agent_key=p.agent_key,
            property_id=p.property_id,
            input_payload={},
            idempotency_key=p.idempotency_key,
        )
        created.append(run)

        if dispatch and (getattr(run, "status", "") or "").lower() == "queued":
            execute_agent_run.delay(org_id=principal.org_id, run_id=int(run.id))

    return {"planned": len(plan), "created": [_serialize_run_detail(db, r) for r in created]}


@router.post("/{run_id}/dispatch")
def dispatch_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    execute_agent_run.delay(org_id=principal.org_id, run_id=int(run.id))
    return {"ok": True, "queued": True, "run_id": int(run.id)}


@router.post("/{run_id}/retry")
def retry_run(
    run_id: int,
    dispatch: bool = Query(default=True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)

    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == principal.org_id,
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if (getattr(run, "status", "") or "").lower() not in TERMINAL:
        raise HTTPException(status_code=409, detail={"code": "run_not_terminal", "status": run.status})

    run.status = "queued"
    run.started_at = None
    run.finished_at = None
    run.last_error = None
    run.heartbeat_at = None

    try:
        if getattr(run, "approval_status", None) in ("rejected", "approved"):
            run.approval_status = "pending"
            run.approved_at = None
            run.approved_by_user_id = None
    except Exception:
        pass

    try:
        ev = AgentTraceEvent(
            org_id=principal.org_id,
            run_id=int(run.id),
            property_id=getattr(run, "property_id", None),
            agent_key=str(getattr(run, "agent_key", "")),
            event_type="retry_requested",
            payload_json=_dumps(
                {
                    "type": "retry_requested",
                    "payload": {
                        "run_id": int(run.id),
                        "by_user_id": int(principal.user_id),
                        "ts": _utcnow().isoformat(),
                    },
                    "level": "info",
                    "ts": _utcnow().isoformat(),
                }
            ),
            created_at=_utcnow(),
        )
        db.add(ev)
    except Exception:
        pass

    db.add(run)
    db.commit()
    db.refresh(run)

    if dispatch:
        execute_agent_run.delay(org_id=principal.org_id, run_id=int(run.id))
        return {"ok": True, "run": _serialize_run_detail(db, run), "queued": True}

    return {"ok": True, "run": _serialize_run_detail(db, run), "queued": False}


@router.post("/{run_id}/approve")
def approve_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)
    run = mark_approved(
        db,
        org_id=principal.org_id,
        actor_user_id=principal.user_id,
        run_id=int(run_id),
    )
    return {"ok": True, "run": _serialize_run_detail(db, run)}


@router.post("/{run_id}/reject")
def reject_run_route(
    run_id: int,
    reason: str = Query(default="rejected_by_owner"),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)
    run = reject_run(
        db,
        org_id=principal.org_id,
        actor_user_id=principal.user_id,
        run_id=int(run_id),
        reason=reason,
    )
    return {"ok": True, "run": _serialize_run_detail(db, run)}


@router.post("/{run_id}/apply")
def apply_run(
    run_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    require_owner(principal)
    return apply_approved(
        db,
        org_id=principal.org_id,
        actor_user_id=principal.user_id,
        run_id=int(run_id),
    )