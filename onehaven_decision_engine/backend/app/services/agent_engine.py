# onehaven_decision_engine/backend/app/services/agent_engine.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.agents.contracts import get_contract, validate_agent_output
from app.domain.agents.executor import execute_agent, apply_proposed_actions
from app.models import AgentRun, WorkflowEvent


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
    """
    Convenience API expected by routers/agents.py.

    - create run (idempotent)
    - dispatch to worker (preferred) OR execute synchronously (dev)
    """
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
        # avoid import cycles at boot
        from app.workers.agent_tasks import execute_agent_run  # import-outside-toplevel

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

    if r.approval_status not in {"pending"}:
        return r

    r.approval_status = "approved"
    r.approved_by_user_id = actor_user_id
    r.approved_at = _now()
    db.add(r)

    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=actor_user_id,
            event_type="agent_run_approved",
            payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key}),
            created_at=_now(),
        )
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
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if r.approval_status != "approved":
        raise HTTPException(status_code=400, detail="Run is not approved")

    actions = _loads(r.proposed_actions_json, [])
    if not isinstance(actions, list) or not actions:
        return {"ok": True, "applied": 0, "run_id": r.id}

    applied, skipped = apply_proposed_actions(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        run=r,
        actions=actions,
    )

    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=actor_user_id,
            event_type="agent_actions_applied",
            payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key, "applied": applied, "skipped": skipped}),
            created_at=_now(),
        )
    )

    r.status = "done"
    r.finished_at = _now()
    db.add(r)

    db.commit()

    return {"ok": True, "applied": applied, "skipped": skipped, "run_id": r.id}


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

    if r.status in {"done", "failed", "blocked"}:
        return {"ok": True, "status": r.status, "run_id": r.id, "output": _loads(r.output_json, {})}

    if force_fail:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = force_fail
        r.attempts = max(int(r.attempts or 0), attempt_number)
        db.add(r)
        db.commit()
        return {"ok": False, "status": "failed", "run_id": r.id, "error": force_fail}

    timeout_s = int(os.getenv("AGENTS_RUN_TIMEOUT_SECONDS", "120"))
    if r.status == "running" and r.started_at:
        if (_now() - r.started_at) > timedelta(seconds=timeout_s):
            r.status = "failed"
            r.finished_at = _now()
            r.last_error = f"timeout after {timeout_s}s"
            db.add(r)
            db.commit()
            return {"ok": False, "status": "failed", "run_id": r.id, "error": r.last_error}

    r.status = "running"
    r.started_at = r.started_at or _now()
    r.heartbeat_at = _now()
    r.attempts = max(int(r.attempts or 0), attempt_number)
    db.add(r)
    db.commit()

    try:
        res = execute_agent(
            db,
            org_id=org_id,
            agent_key=str(r.agent_key),
            property_id=int(r.property_id) if r.property_id else None,
            input_json=r.input_json,
        )
        output = res.output or {}
    except Exception as e:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = str(e)
        db.add(r)
        db.commit()
        return {"ok": False, "status": "retryable_error", "run_id": r.id, "error": str(e)}

    r.output_json = _dumps(output)
    r.heartbeat_at = _now()

    contract = get_contract(str(r.agent_key))
    ok, errs = validate_agent_output(str(r.agent_key), output)
    if not ok:
        r.status = "failed"
        r.finished_at = _now()
        r.last_error = "Contract validation failed: " + "; ".join(errs)
        db.add(r)
        db.commit()
        return {"ok": False, "status": "failed", "run_id": r.id, "error": r.last_error}

    actions = output.get("actions") if isinstance(output, dict) else None
    if isinstance(actions, list) and actions:
        r.proposed_actions_json = _dumps(actions)

    if contract.mode == "recommend_only":
        r.status = "done"
        r.finished_at = _now()
        r.approval_status = "not_required"
        db.add(r)
        db.commit()
        return {"ok": True, "status": "done", "run_id": r.id, "output": output}

    r.status = "blocked"
    r.finished_at = _now()
    r.approval_status = "pending"
    db.add(r)

    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=r.created_by_user_id,
            event_type="agent_run_requires_approval",
            payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key}),
            created_at=_now(),
        )
    )
    db.commit()

    return {"ok": True, "status": "blocked", "run_id": r.id, "needs_approval": True, "output": output}
