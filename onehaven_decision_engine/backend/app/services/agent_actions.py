# onehaven_decision_engine/backend/app/services/agent_actions.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.agents.contracts import get_contract, validate_agent_output
from app.models import AgentRun, WorkflowEvent, RehabTask, PropertyChecklistItem


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


@dataclass(frozen=True)
class ApplyResult:
    ok: bool
    status: str
    run_id: int
    applied_count: int
    errors: list[str]


def apply_run_actions(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run_id: int,
) -> ApplyResult:
    """
    Applies the AgentRun.proposed_actions_json to the database.

    Safety / SaaS semantics:
    - Org boundary enforced on every write.
    - Run must be approval-gated:
        - status must be "blocked"
        - approval_status must be "approved"
    - Apply is idempotent:
        - if run is already "done", returns without re-applying.
    - Writes an auditable WorkflowEvent with summary.
    """
    r = db.scalar(select(AgentRun).where(AgentRun.id == run_id, AgentRun.org_id == org_id))
    if r is None:
        raise HTTPException(status_code=404, detail="AgentRun not found")

    # Idempotent apply: if already done, never double-write.
    if r.status == "done":
        return ApplyResult(ok=True, status="done", run_id=int(r.id), applied_count=0, errors=[])

    if r.status != "blocked":
        raise HTTPException(status_code=409, detail=f"Run not in applyable state (status={r.status})")

    if getattr(r, "approval_status", None) != "approved":
        raise HTTPException(status_code=403, detail="Run not approved")

    # Must have actions
    actions = _loads(getattr(r, "proposed_actions_json", None), [])
    if not isinstance(actions, list) or not actions:
        # Nothing to apply; treat as done (still auditable)
        r.status = "done"
        r.finished_at = datetime.utcnow()
        db.add(r)
        db.add(
            WorkflowEvent(
                org_id=org_id,
                property_id=r.property_id,
                actor_user_id=actor_user_id,
                event_type="agent_actions_applied",
                payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key, "applied": 0}),
                created_at=datetime.utcnow(),
            )
        )
        db.commit()
        return ApplyResult(ok=True, status="done", run_id=int(r.id), applied_count=0, errors=[])

    # Re-validate output (defense-in-depth)
    output = _loads(r.output_json, {})
    ok, errs = validate_agent_output(str(r.agent_key), output)
    if not ok:
        raise HTTPException(status_code=409, detail="Cannot apply: output no longer validates contract")

    contract = get_contract(str(r.agent_key))

    applied = 0
    errors: list[str] = []

    # Apply each action
    for i, a in enumerate(actions):
        if not isinstance(a, dict):
            errors.append(f"actions[{i}] not an object")
            continue

        entity_type = str(a.get("entity_type") or "")
        op = str(a.get("op") or "")
        data = a.get("data")

        # Hard allowlist from contract (prevents “LLM writes anything”)
        if entity_type not in set(contract.allowed_entity_types):
            errors.append(f"actions[{i}] entity_type '{entity_type}' not allowed by contract")
            continue
        if op not in set(contract.allowed_operations):
            errors.append(f"actions[{i}] op '{op}' not allowed by contract")
            continue
        if not isinstance(data, dict):
            errors.append(f"actions[{i}] data must be object")
            continue

        try:
            if entity_type == "rehab_task" and op == "create":
                _apply_create_rehab_task(db, org_id=org_id, property_id=r.property_id, data=data)
                applied += 1

            elif entity_type == "checklist_item" and op == "update_status":
                _apply_update_checklist_item(db, org_id=org_id, property_id=r.property_id, data=data)
                applied += 1

            elif entity_type == "workflow_event" and op == "create":
                _apply_create_workflow_event(db, org_id=org_id, property_id=r.property_id, actor_user_id=actor_user_id, data=data)
                applied += 1

            else:
                errors.append(f"actions[{i}] unsupported action type '{entity_type}:{op}'")
        except Exception as e:
            errors.append(f"actions[{i}] failed: {type(e).__name__}: {e}")

    # If any action failed, fail closed (do NOT mark done)
    if errors:
        r.status = "failed"
        r.finished_at = datetime.utcnow()
        r.last_error = "Apply failed: " + "; ".join(errors[:10])
        db.add(r)
        db.add(
            WorkflowEvent(
                org_id=org_id,
                property_id=r.property_id,
                actor_user_id=actor_user_id,
                event_type="agent_actions_apply_failed",
                payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key, "applied": applied, "errors": errors[:50]}),
                created_at=datetime.utcnow(),
            )
        )
        db.commit()
        return ApplyResult(ok=False, status="failed", run_id=int(r.id), applied_count=applied, errors=errors)

    # Success
    r.status = "done"
    r.finished_at = datetime.utcnow()
    db.add(r)
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=r.property_id,
            actor_user_id=actor_user_id,
            event_type="agent_actions_applied",
            payload_json=_dumps({"run_id": r.id, "agent_key": r.agent_key, "applied": applied}),
            created_at=datetime.utcnow(),
        )
    )
    db.commit()
    return ApplyResult(ok=True, status="done", run_id=int(r.id), applied_count=applied, errors=[])


def _apply_create_rehab_task(db: Session, *, org_id: int, property_id: Optional[int], data: dict[str, Any]) -> None:
    if not property_id:
        raise ValueError("rehab_task requires property_id")

    title = str(data.get("title") or "").strip()
    if not title:
        raise ValueError("rehab_task.title required")

    task = RehabTask(
        org_id=org_id,
        property_id=int(property_id),
        title=title,
        category=str(data.get("category") or "general"),
        inspection_relevant=bool(data.get("inspection_relevant", True)),
        status=str(data.get("status") or "todo"),
        cost_estimate=float(data["cost_estimate"]) if data.get("cost_estimate") is not None else None,
        vendor=str(data.get("vendor") or "") or None,
        deadline=str(data.get("deadline") or "") or None,
        notes=str(data.get("notes") or "") or None,
        created_at=datetime.utcnow(),
    )
    db.add(task)


def _apply_update_checklist_item(db: Session, *, org_id: int, property_id: Optional[int], data: dict[str, Any]) -> None:
    if not property_id:
        raise ValueError("checklist_item update requires property_id")

    item_id = data.get("item_id")
    if item_id is None:
        raise ValueError("checklist_item.item_id required")

    row = db.scalar(
        select(PropertyChecklistItem)
        .where(PropertyChecklistItem.id == int(item_id))
        .where(PropertyChecklistItem.org_id == org_id)
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
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=int(property_id) if property_id else None,
            actor_user_id=actor_user_id,
            event_type=event_type,
            payload_json=_dumps(payload),
            created_at=datetime.utcnow(),
        )
    )