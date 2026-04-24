# backend/app/services/agent_engine.py
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.domain.agents.contracts import get_contract, validate_agent_output
from onehaven_platform.backend.src.domain.agents.executor import execute_agent
from onehaven_platform.backend.src.models import AgentRun, WorkflowEvent
from onehaven_platform.backend.src.services.agent_actions import apply_run_actions
from onehaven_platform.backend.src.services.agent_trace import emit_trace_safe

TERMINAL = {"done", "failed", "timed_out"}
ACTIVE = {"queued", "running", "blocked"}
RUN_STATUSES = {"queued", "running", "done", "failed", "blocked", "timed_out"}
APPROVAL_STATUSES = {"not_required", "pending", "approved", "rejected"}


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


def _now_utc() -> datetime:
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
            org_id=int(org_id),
            property_id=property_id,
            actor_user_id=actor_user_id,
            event_type=str(event_type),
            payload_json=_dumps(payload),
            created_at=_now_utc(),
        )
    )


def _get_input_json(run: AgentRun) -> Optional[str]:
    return (
        getattr(run, "input_json", None)
        or getattr(run, "input_payload_json", None)
        or getattr(run, "input_payload", None)
    )


def _get_output_json(run: AgentRun) -> Optional[str]:
    return (
        getattr(run, "output_json", None)
        or getattr(run, "output_payload_json", None)
        or getattr(run, "output_payload", None)
    )


def _get_proposed_actions_json(run: AgentRun) -> Optional[str]:
    return (
        getattr(run, "proposed_actions_json", None)
        or getattr(run, "actions_json", None)
    )


def normalize_run_status(status: Optional[str]) -> str:
    s = str(status or "queued").strip().lower()
    return s if s in RUN_STATUSES else "queued"


def normalize_approval_status(status: Optional[str]) -> str:
    s = str(status or "not_required").strip().lower()
    return s if s in APPROVAL_STATUSES else "not_required"


def infer_runtime_health(run: AgentRun) -> str:
    status = normalize_run_status(getattr(run, "status", None))
    approval_status = normalize_approval_status(getattr(run, "approval_status", None))
    timeout_s = int(getattr(settings, "agents_run_timeout_seconds", 120) or 120)
    now = _now_utc()

    if status in TERMINAL:
        return "terminal"

    if status == "blocked":
        if approval_status == "pending":
            return "awaiting_approval"
        return "terminal"

    if status == "queued":
        return "queued"

    if status != "running":
        return status

    heartbeat_at = getattr(run, "heartbeat_at", None)
    started_at = getattr(run, "started_at", None)
    ref = heartbeat_at or started_at
    if ref is None:
        return "running"

    age = (now - ref).total_seconds()
    if age > timeout_s:
        return "stale"
    if age > max(15, timeout_s // 3):
        return "lagging"
    return "healthy"


def serialize_run(run: AgentRun) -> dict[str, Any]:
    started_at = getattr(run, "started_at", None)
    finished_at = getattr(run, "finished_at", None)
    heartbeat_at = getattr(run, "heartbeat_at", None)
    created_at = getattr(run, "created_at", None)

    duration_ms: int | None = None
    if started_at and finished_at:
        duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    elif started_at and normalize_run_status(getattr(run, "status", None)) == "running":
        duration_ms = max(0, int((_now_utc() - started_at).total_seconds() * 1000))

    output = _loads(_get_output_json(run), {})
    proposed = _loads(_get_proposed_actions_json(run), [])

    return {
        "id": int(getattr(run, "id")),
        "org_id": int(getattr(run, "org_id")),
        "property_id": getattr(run, "property_id", None),
        "agent_key": str(getattr(run, "agent_key", "")),
        "status": normalize_run_status(getattr(run, "status", None)),
        "runtime_health": infer_runtime_health(run),
        "attempts": int(getattr(run, "attempts", 0) or 0),
        "last_error": getattr(run, "last_error", None),
        "created_at": created_at,
        "started_at": started_at,
        "heartbeat_at": heartbeat_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "approval_status": normalize_approval_status(getattr(run, "approval_status", None)),
        "approved_at": getattr(run, "approved_at", None),
        "approved_by_user_id": getattr(run, "approved_by_user_id", None),
        "created_by_user_id": getattr(run, "created_by_user_id", None),
        "idempotency_key": getattr(run, "idempotency_key", None),
        "has_output": bool(output),
        "has_proposed_actions": bool(proposed),
        "output": output,
        "proposed": proposed,
        "output_json": _get_output_json(run),
        "proposed_actions_json": _get_proposed_actions_json(run),
    }


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
    now = _now_utc()

    if idempotency_key:
        existing = db.scalar(
            select(AgentRun).where(
                AgentRun.org_id == int(org_id),
                AgentRun.idempotency_key == str(idempotency_key),
            )
        )
        if existing is not None:
            return existing

    run = AgentRun(
        org_id=int(org_id),
        property_id=property_id,
        agent_key=str(agent_key),
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

    db.add(run)
    db.commit()
    db.refresh(run)

    _emit_event(
        db,
        org_id=int(org_id),
        property_id=run.property_id,
        actor_user_id=actor_user_id,
        event_type="agent_run_created",
        payload={
            "run_id": int(run.id),
            "agent_key": str(run.agent_key),
            "idempotency_key": idempotency_key,
        },
    )
    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(run.agent_key),
        event_type="queued",
        payload={
            "property_id": run.property_id,
            "idempotency_key": idempotency_key,
            "attempt": 0,
        },
        level="info",
        property_id=run.property_id,
    )
    db.commit()

    return run


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
    run = create_run(
        db,
        org_id=int(org_id),
        actor_user_id=actor_user_id,
        agent_key=str(agent_key),
        property_id=property_id,
        input_payload=input_payload,
        idempotency_key=idempotency_key,
    )

    if dispatch:
        from onehaven_platform.backend.src.jobs.agent_tasks import execute_agent_run

        if normalize_run_status(getattr(run, "status", None)) == "queued":
            execute_agent_run.delay(org_id=int(org_id), run_id=int(run.id))

        return {"ok": True, "mode": "async", "run_id": int(run.id), "status": run.status}

    result = execute_run_now(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        attempt_number=1,
    )
    return {"ok": True, "mode": "sync", "run_id": int(run.id), "result": result}


def mark_approved(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> AgentRun:
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == int(org_id),
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if normalize_approval_status(getattr(run, "approval_status", None)) != "pending":
        return run

    run.approval_status = "approved"
    run.approved_by_user_id = int(actor_user_id)
    run.approved_at = _now_utc()
    db.add(run)

    _emit_event(
        db,
        org_id=int(org_id),
        property_id=run.property_id,
        actor_user_id=int(actor_user_id),
        event_type="agent_run_approved",
        payload={"run_id": int(run.id), "agent_key": str(run.agent_key)},
    )

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(run.agent_key),
        event_type="approved",
        payload={
            "approved_by_user_id": int(actor_user_id),
            "approved_at": run.approved_at.isoformat() if run.approved_at else None,
        },
        level="info",
        property_id=run.property_id,
    )

    db.commit()
    db.refresh(run)
    return run


def reject_run(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
    reason: str = "rejected",
) -> AgentRun:
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == int(org_id),
        )
    )
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if normalize_run_status(getattr(run, "status", None)) in TERMINAL:
        return run

    prev_status = normalize_run_status(getattr(run, "status", None))

    run.status = "failed"
    run.approval_status = "rejected"
    run.last_error = f"rejected: {reason}"
    run.finished_at = _now_utc()
    run.proposed_actions_json = None

    db.add(run)

    _emit_event(
        db,
        org_id=int(org_id),
        property_id=run.property_id,
        actor_user_id=int(actor_user_id),
        event_type="agent_run_rejected",
        payload={
            "run_id": int(run.id),
            "agent_key": str(run.agent_key),
            "reason": str(reason),
            "prev_status": prev_status,
        },
    )

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(run.agent_key),
        event_type="rejected",
        payload={"reason": str(reason), "prev_status": prev_status},
        level="warn",
        property_id=run.property_id,
    )

    db.commit()
    db.refresh(run)
    return run


def apply_approved(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> dict[str, Any]:
    result = apply_run_actions(
        db,
        org_id=int(org_id),
        actor_user_id=int(actor_user_id),
        run_id=int(run_id),
    )

    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == int(org_id),
        )
    )
    agent_key = str(getattr(run, "agent_key", "unknown"))

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run_id),
        agent_key=agent_key,
        event_type="applied",
        payload={
            "applied_count": int(result.applied_count),
            "errors": result.errors,
            "status": result.status,
        },
        level="info" if result.ok else "error",
        property_id=getattr(run, "property_id", None) if run else None,
    )
    db.commit()

    return {
        "ok": bool(result.ok),
        "status": result.status,
        "run_id": int(result.run_id),
        "applied": int(result.applied_count),
        "errors": result.errors,
    }


def execute_run_now(
    db: Session,
    *,
    org_id: int,
    run_id: int,
    attempt_number: int = 1,
    force_fail: Optional[str] = None,
) -> dict[str, Any]:
    run = db.scalar(
        select(AgentRun).where(
            AgentRun.id == int(run_id),
            AgentRun.org_id == int(org_id),
        )
    )
    if run is None:
        return {"ok": False, "status": "not_found", "run_id": int(run_id)}

    agent_key = str(getattr(run, "agent_key", ""))

    if normalize_run_status(getattr(run, "status", None)) in TERMINAL:
        return {
            "ok": True,
            "status": normalize_run_status(getattr(run, "status", None)),
            "run_id": int(run.id),
            "output": _loads(_get_output_json(run), {}),
        }

    if force_fail:
        run.status = "failed"
        run.finished_at = _now_utc()
        run.last_error = str(force_fail)
        run.attempts = max(int(getattr(run, "attempts", 0) or 0), int(attempt_number))
        db.add(run)

        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=agent_key,
            event_type="error",
            payload={"error": str(force_fail), "forced": True},
            level="error",
            property_id=run.property_id,
        )
        db.commit()
        return {
            "ok": False,
            "status": "failed",
            "run_id": int(run.id),
            "error": str(force_fail),
        }

    timeout_s = int(getattr(settings, "agents_run_timeout_seconds", 120) or 120)

    if normalize_run_status(getattr(run, "status", None)) == "running" and getattr(run, "started_at", None):
        if (_now_utc() - run.started_at) > timedelta(seconds=timeout_s):
            run.status = "timed_out"
            run.finished_at = _now_utc()
            run.last_error = f"timeout after {timeout_s}s"
            db.add(run)
            _emit_event(
                db,
                org_id=int(org_id),
                property_id=run.property_id,
                actor_user_id=run.created_by_user_id,
                event_type="agent_run_timed_out",
                payload={"run_id": int(run.id), "agent_key": agent_key, "timeout_s": timeout_s},
            )
            emit_trace_safe(
                db,
                org_id=int(org_id),
                run_id=int(run.id),
                agent_key=agent_key,
                event_type="timed_out",
                payload={"timeout_s": timeout_s},
                level="error",
                property_id=run.property_id,
            )
            db.commit()
            return {
                "ok": False,
                "status": "timed_out",
                "run_id": int(run.id),
                "error": run.last_error,
            }

    run.status = "running"
    run.started_at = getattr(run, "started_at", None) or _now_utc()
    run.heartbeat_at = _now_utc()
    run.attempts = max(int(getattr(run, "attempts", 0) or 0), int(attempt_number))
    db.add(run)
    db.commit()

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=agent_key,
        event_type="started",
        payload={"attempt": int(attempt_number), "property_id": run.property_id, "status": "running"},
        level="info",
        property_id=run.property_id,
    )
    db.commit()

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=agent_key,
        event_type="context_loaded",
        payload={"property_id": run.property_id, "has_input_json": bool(_get_input_json(run))},
        level="info",
        property_id=run.property_id,
    )
    db.commit()

    try:
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=agent_key,
            event_type="executing_agent",
            payload={"agent_key": agent_key},
            level="info",
            property_id=run.property_id,
        )
        db.commit()

        result = execute_agent(
            db,
            org_id=int(org_id),
            agent_key=agent_key,
            property_id=int(run.property_id) if run.property_id is not None else None,
            input_json=_get_input_json(run),
        )
        output = result.output if hasattr(result, "output") else result
        if output is None:
            output = {}
    except Exception as e:
        run.status = "failed"
        run.finished_at = _now_utc()
        run.last_error = str(e)
        db.add(run)

        _emit_event(
            db,
            org_id=int(org_id),
            property_id=run.property_id,
            actor_user_id=run.created_by_user_id,
            event_type="agent_run_failed",
            payload={"run_id": int(run.id), "agent_key": agent_key, "error": str(e)},
        )
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=agent_key,
            event_type="error",
            payload={"error": str(e)},
            level="error",
            property_id=run.property_id,
        )
        db.commit()
        return {
            "ok": False,
            "status": "failed",
            "run_id": int(run.id),
            "error": str(e),
        }

    run.output_json = _dumps(output)
    run.heartbeat_at = _now_utc()
    db.add(run)
    db.commit()

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=agent_key,
        event_type="agent_output",
        payload={
            "summary": output.get("summary") if isinstance(output, dict) else None,
            "actions_count": len(output.get("actions") or []) if isinstance(output, dict) else 0,
            "recommendations_count": len(output.get("recommendations") or []) if isinstance(output, dict) else 0,
        },
        level="info",
        property_id=run.property_id,
    )
    db.commit()

    contract = get_contract(agent_key)
    ok, errs = validate_agent_output(
        agent_key,
        output if isinstance(output, dict) else {},
    )
    if not ok:
        run.status = "failed"
        run.finished_at = _now_utc()
        run.last_error = "Contract validation failed: " + "; ".join(errs)
        db.add(run)
        _emit_event(
            db,
            org_id=int(org_id),
            property_id=run.property_id,
            actor_user_id=run.created_by_user_id,
            event_type="agent_run_failed_contract",
            payload={"run_id": int(run.id), "agent_key": agent_key, "errors": errs},
        )
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=agent_key,
            event_type="validation_failed",
            payload={"errors": errs, "mode": contract.mode},
            level="error",
            property_id=run.property_id,
        )
        db.commit()
        return {
            "ok": False,
            "status": "failed",
            "run_id": int(run.id),
            "error": run.last_error,
        }

    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=agent_key,
        event_type="validation_ok",
        payload={"mode": contract.mode},
        level="info",
        property_id=run.property_id,
    )
    db.commit()

    if isinstance(output, dict) and contract.mode != "recommend_only":
        actions = output.get("actions")
        run.proposed_actions_json = _dumps(actions if isinstance(actions, list) else [])

    if contract.mode == "recommend_only":
        run.status = "done"
        run.finished_at = _now_utc()
        run.approval_status = "not_required"
        db.add(run)

        _emit_event(
            db,
            org_id=int(org_id),
            property_id=run.property_id,
            actor_user_id=run.created_by_user_id,
            event_type="agent_run_done",
            payload={"run_id": int(run.id), "agent_key": agent_key, "mode": "recommend_only"},
        )
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=agent_key,
            event_type="done",
            payload={"mode": "recommend_only"},
            level="info",
            property_id=run.property_id,
        )
        db.commit()
        return {
            "ok": True,
            "status": "done",
            "run_id": int(run.id),
            "output": output,
        }

    run.status = "blocked"
    run.finished_at = _now_utc()
    run.approval_status = "pending"
    db.add(run)

    _emit_event(
        db,
        org_id=int(org_id),
        property_id=run.property_id,
        actor_user_id=run.created_by_user_id,
        event_type="agent_run_requires_approval",
        payload={"run_id": int(run.id), "agent_key": agent_key},
    )
    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=agent_key,
        event_type="blocked_pending_approval",
        payload={"mode": contract.mode, "approval_status": "pending"},
        level="warn",
        property_id=run.property_id,
    )
    db.commit()

    return {
        "ok": True,
        "status": "blocked",
        "run_id": int(run.id),
        "needs_approval": True,
        "output": output,
    }


def sweep_stuck_runs(
    db: Session,
    *,
    timeout_seconds: int,
    queued_max_hours: int = 12,
) -> dict[str, Any]:
    now = _now_utc()
    changed = 0
    details: list[dict[str, Any]] = []

    rows = db.scalars(
        select(AgentRun).order_by(AgentRun.id.desc()).limit(500)
    ).all()

    for run in rows:
        st = normalize_run_status(getattr(run, "status", None))

        if st == "running" and getattr(run, "started_at", None):
            ref = getattr(run, "heartbeat_at", None) or getattr(run, "started_at", None)
            if ref and (now - ref) > timedelta(seconds=int(timeout_seconds)):
                run.status = "timed_out"
                run.finished_at = now
                run.last_error = f"sweeper timeout after {timeout_seconds}s"
                db.add(run)
                _emit_event(
                    db,
                    org_id=int(run.org_id),
                    property_id=getattr(run, "property_id", None),
                    actor_user_id=None,
                    event_type="agent_run_swept_timeout",
                    payload={"run_id": int(run.id), "agent_key": str(run.agent_key)},
                )
                emit_trace_safe(
                    db,
                    org_id=int(run.org_id),
                    run_id=int(run.id),
                    agent_key=str(run.agent_key),
                    event_type="swept_timeout",
                    payload={"timeout_seconds": int(timeout_seconds)},
                    level="warn",
                    property_id=getattr(run, "property_id", None),
                )
                changed += 1
                details.append({"run_id": int(run.id), "action": "timed_out"})

        if st == "queued" and getattr(run, "created_at", None):
            if (now - run.created_at) > timedelta(hours=int(queued_max_hours)):
                run.status = "failed"
                run.finished_at = now
                run.last_error = f"sweeper: queued > {queued_max_hours}h"
                db.add(run)
                _emit_event(
                    db,
                    org_id=int(run.org_id),
                    property_id=getattr(run, "property_id", None),
                    actor_user_id=None,
                    event_type="agent_run_swept_queued",
                    payload={"run_id": int(run.id), "agent_key": str(run.agent_key)},
                )
                emit_trace_safe(
                    db,
                    org_id=int(run.org_id),
                    run_id=int(run.id),
                    agent_key=str(run.agent_key),
                    event_type="swept_queued",
                    payload={"queued_max_hours": int(queued_max_hours)},
                    level="warn",
                    property_id=getattr(run, "property_id", None),
                )
                changed += 1
                details.append({"run_id": int(run.id), "action": "failed_queued"})

    if changed:
        db.commit()

    return {"changed": changed, "details": details}
