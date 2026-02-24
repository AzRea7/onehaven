# backend/app/services/agent_engine.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.agents.contracts import get_contract, validate_agent_output
from app.domain.agents.executor import execute_agent
from app.models import AgentRun, WorkflowEvent
from app.services.agent_actions import apply_run_actions
from app.services.agent_trace import emit_trace_safe


TERMINAL = {"done", "failed", "timed_out"}
ACTIVE = {"queued", "running", "blocked"}


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _now() -> datetime:
    return datetime.utcnow()


def _emit_event(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    actor_user_id: Optional[int],
    event_type: str,
    payload: dict[str, Any],
) -> None:
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=property_id,
            actor_user_id=actor_user_id,
            event_type=event_type,
            payload_json=_dumps(payload),
            created_at=_now(),
        )
    )


def create_run(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    agent_key: str,
    property_id: Optional[int],
    input_payload: dict[str, Any] | None,
    idempotency_key: Optional[str] = None,
) -> AgentRun:
    now = _now()

    if idempotency_key:
        existing = db.scalar(
            select(AgentRun)
            .where(AgentRun.org_id == org_id)
            .where(AgentRun.idempotency_key == idempotency_key)
        )
        if existing is not None:
            return existing

    r = AgentRun(
        org_id=org_id,
        property_id=property_id,
        agent_key=agent_key,
        status="queued",
        input_json=_dumps(input_payload or {}),
        output_json=None,
        created_by_user_id=actor_user_id,
        created_at=now,
        started_at=None,
        finished_at=None,
        attempts=0,
        last_error=None,
        idempotency_key=idempotency_key,
        heartbeat_at=None,
        approved_by_user_id=None,
        approved_at=None,
        approval_status="not_required",
        proposed_actions_json=None,
    )

    db.add(r)
    db.commit()
    db.refresh(r)

    _emit_event(
        db,
        org_id=org_id,
        property_id=r.property_id,
        actor_user_id=actor_user_id,
        event_type="agent_run_created",
        payload={"run_id": int(r.id), "agent_key": str(r.agent_key), "idempotency_key": idempotency_key},
    )
    db.commit()

    return r


def create_and_execute_run(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    agent_key: str,
    property_id: Optional[int],
    input_payload: dict[str, Any] | None,
    idempotency_key: Optional[str] = None,
    dispatch: bool = True,
) -> dict[str, Any]:
    r = create_run(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        agent_key=agent_key,
        property_id=property_id,
        input_payload=input_payload,
        idempotency_key=idempotency_key,
    )

    if dispatch:
        from app.workers.agent_tasks import execute_agent_run  # noqa

        if r.status == "queued":
            execute_agent_run.delay(org_id=org_id, run_id=int(r.id))

        return {"ok": True, "mode": "async", "run_id": int(r.id), "status": r.status}

    res = execute_run_now(db, org_id=org_id, run_id=int(r.id), attempt_number=1)
    return {"ok": True, "mode": "sync", "run_id": int(r.id), "result": res}


def mark_approved(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> AgentRun:
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if r.approval_status != "pending":
        return r

    r.approval_status = "approved"
    r.approved_by_user_id = actor_user_id
    r.approved_at = _now()
    db.add(r)

    _emit_event(
        db,
        org_id=org_id,
        property_id=r.property_id,
        actor_user_id=actor_user_id,
        event_type="agent_run_approved",
        payload={"run_id": int(r.id), "agent_key": str(r.agent_key)},
    )

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=str(r.agent_key),
        event_type="approved",
        payload={"approved_by_user_id": actor_user_id, "approved_at": r.approved_at.isoformat() if r.approved_at else None},
        level="info",
    )

    db.commit()
    db.refresh(r)
    return r


def reject_run(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
    reason: str = "rejected",
) -> AgentRun:
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if r.status in TERMINAL:
        return r

    prev_status = r.status

    r.status = "failed"
    r.approval_status = "rejected"
    r.last_error = f"rejected: {reason}"
    r.finished_at = _now()
    r.proposed_actions_json = None

    db.add(r)

    _emit_event(
        db,
        org_id=org_id,
        property_id=r.property_id,
        actor_user_id=actor_user_id,
        event_type="agent_run_rejected",
        payload={"run_id": int(r.id), "agent_key": str(r.agent_key), "reason": reason, "prev_status": prev_status},
    )

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=str(r.agent_key),
        event_type="rejected",
        payload={"reason": reason, "prev_status": prev_status},
        level="warn",
    )

    db.commit()
    db.refresh(r)
    return r


def apply_approved(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> dict[str, Any]:
    """
    ✅ Single source of truth: delegate ALL apply logic to services/agent_actions.apply_run_actions().
    """
    res = apply_run_actions(db, org_id=org_id, actor_user_id=actor_user_id, run_id=run_id)

    # Trace apply result for UI
    agent_key = "unknown"
    run = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if run is not None:
        agent_key = str(run.agent_key)

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(run_id),
        agent_key=agent_key,
        event_type="applied",
        payload={"applied_count": int(res.applied_count), "errors": res.errors, "status": res.status},
        level="info" if res.ok else "error",
    )
    db.commit()

    return {
        "ok": bool(res.ok),
        "status": res.status,
        "run_id": int(res.run_id),
        "applied": int(res.applied_count),
        "errors": res.errors,
    }


def execute_run_now(
    db: Session,
    *,
    org_id: int,
    run_id: int,
    attempt_number: int = 1,
    force_fail: Optional[str] = None,
) -> dict[str, Any]:
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        return {"ok": False, "status": "not_found", "run_id": run_id}

    agent_key = str(r.agent_key)

    if r.status in TERMINAL:
        return {"ok": True, "status": r.status, "run_id": int(r.id), "output": _loads(r.output_json, {})}

    if force_fail:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = force_fail
        r.attempts = max(int(r.attempts or 0), attempt_number)
        db.add(r)

        emit_trace_safe(
            db,
            org_id=org_id,
            run_id=int(r.id),
            agent_key=agent_key,
            event_type="error",
            payload={"error": force_fail, "forced": True},
            level="error",
        )
        db.commit()
        return {"ok": False, "status": "failed", "run_id": int(r.id), "error": force_fail}

    timeout_s = int(os.getenv("AGENTS_RUN_TIMEOUT_SECONDS", "120"))

    if r.status == "running" and r.started_at:
        if (_now() - r.started_at) > timedelta(seconds=timeout_s):
            r.status = "timed_out"
            r.finished_at = _now()
            r.last_error = f"timeout after {timeout_s}s"
            db.add(r)
            _emit_event(
                db,
                org_id=org_id,
                property_id=r.property_id,
                actor_user_id=r.created_by_user_id,
                event_type="agent_run_timed_out",
                payload={"run_id": int(r.id), "agent_key": agent_key, "timeout_s": timeout_s},
            )
            emit_trace_safe(
                db,
                org_id=org_id,
                run_id=int(r.id),
                agent_key=agent_key,
                event_type="timed_out",
                payload={"timeout_s": timeout_s},
                level="error",
            )
            db.commit()
            return {"ok": False, "status": "timed_out", "run_id": int(r.id), "error": r.last_error}

    r.status = "running"
    r.started_at = r.started_at or _now()
    r.heartbeat_at = _now()
    r.attempts = max(int(r.attempts or 0), attempt_number)
    db.add(r)
    db.commit()

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=agent_key,
        event_type="started",
        payload={"attempt": attempt_number, "property_id": r.property_id, "status": "running"},
        level="info",
    )
    db.commit()

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=agent_key,
        event_type="context_loaded",
        payload={"property_id": r.property_id, "has_input_json": bool(r.input_json)},
        level="info",
    )
    db.commit()

    try:
        emit_trace_safe(
            db,
            org_id=org_id,
            run_id=int(r.id),
            agent_key=agent_key,
            event_type="executing_agent",
            payload={"agent_key": agent_key},
            level="info",
        )
        db.commit()

        res = execute_agent(
            db,
            org_id=org_id,
            agent_key=agent_key,
            property_id=int(r.property_id) if r.property_id else None,
            input_json=r.input_json,
        )
        output = res.output if hasattr(res, "output") else res
        if output is None:
            output = {}
    except Exception as e:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = str(e)
        db.add(r)
        _emit_event(
            db,
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=r.created_by_user_id,
            event_type="agent_run_failed",
            payload={"run_id": int(r.id), "agent_key": agent_key, "error": str(e)},
        )
        emit_trace_safe(
            db,
            org_id=org_id,
            run_id=int(r.id),
            agent_key=agent_key,
            event_type="error",
            payload={"error": str(e)},
            level="error",
        )
        db.commit()
        return {"ok": False, "status": "failed", "run_id": int(r.id), "error": str(e)}

    r.output_json = _dumps(output)
    r.heartbeat_at = _now()
    db.add(r)
    db.commit()

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=agent_key,
        event_type="agent_output",
        payload={
            "summary": (output.get("summary") if isinstance(output, dict) else None),
            "actions_count": len(output.get("actions") or []) if isinstance(output, dict) else 0,
            "recommendations_count": len(output.get("recommendations") or []) if isinstance(output, dict) else 0,
        },
        level="info",
    )
    db.commit()

    contract = get_contract(agent_key)
    ok, errs = validate_agent_output(agent_key, output if isinstance(output, dict) else {})
    if not ok:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = "Contract validation failed: " + "; ".join(errs)
        db.add(r)
        _emit_event(
            db,
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=r.created_by_user_id,
            event_type="agent_run_failed_contract",
            payload={"run_id": int(r.id), "agent_key": agent_key, "errors": errs},
        )
        emit_trace_safe(
            db,
            org_id=org_id,
            run_id=int(r.id),
            agent_key=agent_key,
            event_type="validation_failed",
            payload={"errors": errs, "mode": contract.mode},
            level="error",
        )
        db.commit()
        return {"ok": False, "status": "failed", "run_id": int(r.id), "error": r.last_error}

    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=agent_key,
        event_type="validation_ok",
        payload={"mode": contract.mode},
        level="info",
    )
    db.commit()

    # ✅ Persist proposed_actions_json ONLY for mutation modes, and ONLY actions[] (never recommendations)
    if isinstance(output, dict) and contract.mode != "recommend_only":
        actions = output.get("actions")
        if isinstance(actions, list) and actions:
            r.proposed_actions_json = _dumps(actions)
        else:
            r.proposed_actions_json = _dumps([])

    # recommend_only => auto-done
    if contract.mode == "recommend_only":
        r.status = "done"
        r.finished_at = _now()
        r.approval_status = "not_required"
        db.add(r)
        _emit_event(
            db,
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=r.created_by_user_id,
            event_type="agent_run_done",
            payload={"run_id": int(r.id), "agent_key": agent_key, "mode": "recommend_only"},
        )
        emit_trace_safe(
            db,
            org_id=org_id,
            run_id=int(r.id),
            agent_key=agent_key,
            event_type="done",
            payload={"mode": "recommend_only"},
            level="info",
        )
        db.commit()
        return {"ok": True, "status": "done", "run_id": int(r.id), "output": output}

    # otherwise blocked pending approval
    r.status = "blocked"
    r.finished_at = _now()
    r.approval_status = "pending"
    db.add(r)

    _emit_event(
        db,
        org_id=org_id,
        property_id=r.property_id,
        actor_user_id=r.created_by_user_id,
        event_type="agent_run_requires_approval",
        payload={"run_id": int(r.id), "agent_key": agent_key},
    )
    emit_trace_safe(
        db,
        org_id=org_id,
        run_id=int(r.id),
        agent_key=agent_key,
        event_type="blocked_pending_approval",
        payload={"mode": contract.mode, "approval_status": "pending"},
        level="warn",
    )

    db.commit()
    return {"ok": True, "status": "blocked", "run_id": int(r.id), "needs_approval": True, "output": output}


def sweep_stuck_runs(
    db: Session,
    *,
    timeout_seconds: int,
    queued_max_hours: int = 12,
) -> dict[str, Any]:
    now = _now()
    changed = 0
    details: list[dict[str, Any]] = []

    q = select(AgentRun).order_by(AgentRun.id.desc()).limit(500)
    rows = db.scalars(q).all()

    for r in rows:
        st = (r.status or "").lower()

        if st == "running" and r.started_at:
            if (now - r.started_at) > timedelta(seconds=int(timeout_seconds)):
                r.status = "timed_out"
                r.finished_at = now
                r.last_error = f"sweeper timeout after {timeout_seconds}s"
                db.add(r)
                _emit_event(
                    db,
                    org_id=int(r.org_id),
                    property_id=r.property_id,
                    actor_user_id=None,
                    event_type="agent_run_swept_timeout",
                    payload={"run_id": int(r.id), "agent_key": str(r.agent_key)},
                )
                emit_trace_safe(
                    db,
                    org_id=int(r.org_id),
                    run_id=int(r.id),
                    agent_key=str(r.agent_key),
                    event_type="swept_timeout",
                    payload={"timeout_seconds": int(timeout_seconds)},
                    level="warn",
                )
                changed += 1
                details.append({"run_id": int(r.id), "action": "timed_out"})

        if st == "queued" and r.created_at:
            if (now - r.created_at) > timedelta(hours=int(queued_max_hours)):
                r.status = "failed"
                r.finished_at = now
                r.last_error = f"sweeper: queued > {queued_max_hours}h"
                db.add(r)
                _emit_event(
                    db,
                    org_id=int(r.org_id),
                    property_id=r.property_id,
                    actor_user_id=None,
                    event_type="agent_run_swept_queued",
                    payload={"run_id": int(r.id), "agent_key": str(r.agent_key)},
                )
                emit_trace_safe(
                    db,
                    org_id=int(r.org_id),
                    run_id=int(r.id),
                    agent_key=str(r.agent_key),
                    event_type="swept_queued",
                    payload={"queued_max_hours": int(queued_max_hours)},
                    level="warn",
                )
                changed += 1
                details.append({"run_id": int(r.id), "action": "failed_queued"})

    if changed:
        db.commit()

    return {"changed": changed, "details": details}
