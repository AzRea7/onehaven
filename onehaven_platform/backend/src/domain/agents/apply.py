from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.agents.contracts import get_contract
from onehaven_platform.backend.src.domain.agents.validate import validate_agent_output
from onehaven_platform.backend.src.models import AgentRun, PropertyChecklistItem, RehabTask, WorkflowEvent
from onehaven_platform.backend.src.services.agent_trace import emit_trace_safe


@dataclass(frozen=True)
class ApplyResult:
    ok: bool
    status: str
    run_id: int
    applied_count: int
    errors: list[str]


def _now() -> datetime:
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


def apply_run_actions(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> ApplyResult:
    run = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
    if run is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    if str(run.status) == "done":
        return ApplyResult(ok=True, status="done", run_id=int(run.id), applied_count=0, errors=[])

    if str(run.status) != "blocked":
        raise HTTPException(status_code=409, detail=f"Run not applyable (status={run.status})")

    if str(getattr(run, "approval_status", "")) != "approved":
        raise HTTPException(status_code=403, detail="Run not approved")

    output = _loads(getattr(run, "output_json", None), {})
    validation = validate_agent_output(str(run.agent_key), output)
    if not validation.ok:
        raise HTTPException(status_code=409, detail="Cannot apply invalid agent output")

    contract = get_contract(str(run.agent_key))
    actions = _loads(getattr(run, "proposed_actions_json", None), [])

    if contract.mode == "recommend_only" or not isinstance(actions, list) or not actions:
        run.status = "done"
        run.finished_at = _now()
        run.last_error = None
        db.add(run)
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(run.agent_key),
            event_type="applied",
            payload={"applied_count": 0, "status": "done", "note": "no actions"},
            level="info",
            property_id=getattr(run, "property_id", None),
        )
        db.commit()
        return ApplyResult(ok=True, status="done", run_id=int(run.id), applied_count=0, errors=[])

    applied_count = 0
    errors: list[str] = []

    for idx, action in enumerate(actions):
        try:
            entity_type = str(action.get("entity_type") or "")
            op = str(action.get("op") or "")
            data = action.get("data") or {}
            if entity_type not in contract.allowed_entity_types:
                raise ValueError(f"entity_type '{entity_type}' not allowed")
            if op not in contract.allowed_operations:
                raise ValueError(f"operation '{op}' not allowed")
            if not isinstance(data, dict):
                raise ValueError("action.data must be object")

            if entity_type == "rehab_task" and op == "create":
                _apply_create_rehab_task(db, org_id=int(org_id), property_id=getattr(run, "property_id", None), data=data)
            elif entity_type == "checklist_item" and op == "update_status":
                _apply_update_checklist_item(db, org_id=int(org_id), property_id=getattr(run, "property_id", None), data=data)
            elif entity_type == "workflow_event" and op == "create":
                _apply_create_workflow_event(
                    db,
                    org_id=int(org_id),
                    property_id=getattr(run, "property_id", None),
                    actor_user_id=int(actor_user_id),
                    data=data,
                )
            else:
                raise ValueError(f"unsupported action '{entity_type}:{op}'")
            applied_count += 1
        except Exception as exc:
            errors.append(f"actions[{idx}] failed: {type(exc).__name__}: {exc}")

    if errors:
        run.status = "failed"
        run.finished_at = _now()
        run.last_error = "Apply failed: " + "; ".join(errors[:10])
        db.add(run)
        emit_trace_safe(
            db,
            org_id=int(org_id),
            run_id=int(run.id),
            agent_key=str(run.agent_key),
            event_type="apply_failed",
            payload={"applied_count": applied_count, "errors": errors[:50]},
            level="error",
            property_id=getattr(run, "property_id", None),
        )
        db.commit()
        return ApplyResult(ok=False, status="failed", run_id=int(run.id), applied_count=applied_count, errors=errors)

    run.status = "done"
    run.finished_at = _now()
    run.last_error = None
    db.add(run)
    emit_trace_safe(
        db,
        org_id=int(org_id),
        run_id=int(run.id),
        agent_key=str(run.agent_key),
        event_type="applied",
        payload={"applied_count": applied_count, "status": "done"},
        level="info",
        property_id=getattr(run, "property_id", None),
    )
    db.commit()
    return ApplyResult(ok=True, status="done", run_id=int(run.id), applied_count=applied_count, errors=[])


def _apply_create_rehab_task(db: Session, *, org_id: int, property_id: Optional[int], data: dict[str, Any]) -> None:
    if property_id is None:
        raise ValueError("rehab_task create requires property_id")
    title = str(data.get("title") or "").strip()
    if not title:
        raise ValueError("rehab_task.title required")

    existing = db.scalar(
        select(RehabTask).where(
            RehabTask.org_id == int(org_id),
            RehabTask.property_id == int(property_id),
            RehabTask.title == title,
        )
    )
    if existing is not None:
        return

    task = RehabTask(
        org_id=int(org_id),
        property_id=int(property_id),
        title=title,
        category=str(data.get("category") or "rehab"),
        inspection_relevant=bool(data.get("inspection_relevant", True)),
        status=str(data.get("status") or "todo"),
        cost_estimate=float(data["cost_estimate"]) if data.get("cost_estimate") is not None else None,
        vendor=str(data.get("vendor") or "") or None,
        deadline=(data.get("deadline") if isinstance(data.get("deadline"), str) else None),
        notes=str(data.get("notes") or "") or None,
        created_at=_now(),
    )
    db.add(task)


def _apply_update_checklist_item(db: Session, *, org_id: int, property_id: Optional[int], data: dict[str, Any]) -> None:
    if property_id is None:
        raise ValueError("checklist item update requires property_id")
    item_id = data.get("item_id")
    if item_id is None:
        raise ValueError("checklist_item.item_id required")

    row = db.scalar(
        select(PropertyChecklistItem)
        .where(PropertyChecklistItem.id == int(item_id))
        .where(PropertyChecklistItem.org_id == int(org_id))
        .where(PropertyChecklistItem.property_id == int(property_id))
    )
    if row is None:
        raise ValueError("checklist item not found")

    if "status" in data:
        row.status = str(data["status"])
    if "notes" in data:
        row.notes = str(data["notes"])
    db.add(row)


def _apply_create_workflow_event(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    actor_user_id: int,
    data: dict[str, Any],
) -> None:
    event_type = str(data.get("event_type") or "").strip()
    if not event_type:
        raise ValueError("workflow_event.event_type required")
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    row = WorkflowEvent(
        org_id=int(org_id),
        property_id=int(property_id) if property_id is not None else None,
        actor_user_id=int(actor_user_id),
        event_type=event_type,
        payload_json=_dumps(payload),
        created_at=_now(),
    )
    db.add(row)
