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
from app.domain.agents.executor import execute_agent
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
    now = datetime.utcnow()

    # If idempotency_key is supplied and exists, return the existing run.
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

    if r.approval_status not in {"pending", "not_required"}:
        return r

    r.approval_status = "approved"
    r.approved_by_user_id = actor_user_id
    r.approved_at = datetime.utcnow()
    db.add(r)

    # audit via workflow event
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=actor_user_id,
            event_type="agent_run_approved",
            payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key}),
            created_at=datetime.utcnow(),
        )
    )
    db.commit()
    db.refresh(r)
    return r


def execute_run_now(
    db: Session,
    *,
    org_id: int,
    run_id: int,
    attempt_number: int = 1,
    force_fail: Optional[str] = None,
) -> dict[str, Any]:
    """
    Executes a queued run with:
    - idempotency (status gate + idempotency_key unique)
    - retries (worker controlled)
    - timeout/stuck semantics
    - contract enforcement
    - approval semantics for mutation agents
    """
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        return {"ok": False, "status": "not_found", "run_id": run_id}

    # Idempotency: if already finished, return stored output
    if r.status in {"done", "failed", "blocked"}:
        return {"ok": True, "status": r.status, "run_id": r.id, "output": _loads(r.output_json, {})}

    # If forced final failure (used by worker max-retry path)
    if force_fail:
        r.status = "failed"
        r.finished_at = datetime.utcnow()
        r.last_error = force_fail
        r.attempts = max(int(r.attempts or 0), attempt_number)
        db.add(r)
        db.commit()
        return {"ok": False, "status": "failed", "run_id": r.id, "error": force_fail}

    # Timeout protection: if running too long, mark failed and allow requeue logic upstream
    timeout_s = int(os.getenv("AGENTS_RUN_TIMEOUT_SECONDS", "120"))
    if r.status == "running" and r.started_at:
        if (datetime.utcnow() - r.started_at) > timedelta(seconds=timeout_s):
            r.status = "failed"
            r.finished_at = datetime.utcnow()
            r.last_error = f"timeout after {timeout_s}s"
            db.add(r)
            db.commit()
            return {"ok": False, "status": "failed", "run_id": r.id, "error": r.last_error}

    # Transition to running
    r.status = "running"
    r.started_at = r.started_at or datetime.utcnow()
    r.heartbeat_at = datetime.utcnow()
    r.attempts = max(int(r.attempts or 0), attempt_number)
    db.add(r)
    db.commit()

    # Execute the agent (deterministic today; LLM later behind same interface)
    try:
        res = execute_agent(
            db,
            org_id=org_id,
            agent_key=str(r.agent_key),
            property_id=int(r.property_id) if r.property_id else None,
            input_json=r.input_json,
        )
    except Exception as e:
        r.status = "failed"
        r.finished_at = datetime.utcnow()
        r.last_error = str(e)
        db.add(r)
        db.commit()
        # Let Celery retry transient failures if desired:
        return {"ok": False, "status": "retryable_error", "run_id": r.id, "error": str(e)}

    # Store raw output always
    output = res.output or {}
    r.output_json = _dumps(output)
    r.heartbeat_at = datetime.utcnow()

    # Contract enforcement
    contract = get_contract(str(r.agent_key))
    ok, errs = validate_agent_output(str(r.agent_key), output)

    if not ok:
        r.status = "failed"
        r.finished_at = datetime.utcnow()
        r.last_error = "Contract validation failed: " + "; ".join(errs)
        db.add(r)
        db.commit()
        return {"ok": False, "status": "failed", "run_id": r.id, "error": r.last_error}

    # Approval semantics for mutation-capable agents
    actions = output.get("actions") if isinstance(output, dict) else None
    has_actions = isinstance(actions, list) and len(actions) > 0

    if contract.mode == "recommend_only":
        r.status = "done"
        r.finished_at = datetime.utcnow()
        r.approval_status = "not_required"
        db.add(r)
        db.commit()
        return {"ok": True, "status": "done", "run_id": r.id, "output": output}

    # mutate modes
    if has_actions:
        r.proposed_actions_json = _dumps(actions)

    if contract.mode == "mutate_requires_approval":
        # Block until approved (don’t mutate database yet)
        r.status = "blocked"
        r.finished_at = datetime.utcnow()
        r.approval_status = "pending"
        db.add(r)
        db.add(
            WorkflowEvent(
                org_id=org_id,
                property_id=r.property_id,
                actor_user_id=r.created_by_user_id,
                event_type="agent_run_requires_approval",
                payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key}),
                created_at=datetime.utcnow(),
            )
        )
        db.commit()
        return {"ok": True, "status": "blocked", "run_id": r.id, "needs_approval": True, "output": output}

    # autonomous_mutate (future): apply actions here.
    # For now, keep it safe: still mark blocked unless you explicitly decide to implement action application.
    r.status = "blocked"
    r.finished_at = datetime.utcnow()
    r.approval_status = "pending"
    r.last_error = "autonomous_mutate not enabled yet (safety default)."
    db.add(r)
    db.commit()
    return {"ok": True, "status": "blocked", "run_id": r.id, "needs_approval": True, "output": output}