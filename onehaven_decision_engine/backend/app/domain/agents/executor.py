# onehaven_decision_engine/backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional, Tuple, List

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.domain.agents.contracts import get_contract
from app.domain.agents.registry import AGENTS
from app.models import WorkflowEvent, RehabTask, PropertyChecklistItem, AgentRun


@dataclass
class AgentResult:
    status: str  # "done" | "failed"
    output: dict[str, Any]
    error: Optional[str] = None


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


def execute_agent(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    input_json: Optional[str],
) -> AgentResult:
    fn = AGENTS.get(agent_key)
    if fn is None:
        return AgentResult(status="failed", output={}, error=f"Unknown agent_key: {agent_key}")

    payload = _loads(input_json, {})
    out = fn(
        db,
        org_id=org_id,
        property_id=property_id,
        input_payload=payload if isinstance(payload, dict) else {},
    )

    if not isinstance(out, dict):
        return AgentResult(status="failed", output={}, error="Agent returned non-dict output")

    out.setdefault("actions", [])
    return AgentResult(status="done", output=out, error=None)


def apply_proposed_actions(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    run: AgentRun,
    actions: List[dict[str, Any]],
) -> Tuple[int, int]:
    """
    Applies a SAFE subset of actions:
      - create RehabTask
      - create WorkflowEvent
      - update PropertyChecklistItem.status (via op=update_status)

    Everything else is skipped.
    Returns (applied_count, skipped_count)
    """
    contract = get_contract(str(run.agent_key))

    applied = 0
    skipped = 0

    for a in actions:
        if not isinstance(a, dict):
            skipped += 1
            continue

        entity_type = str(a.get("entity_type") or "")
        op = str(a.get("op") or "")
        payload = a.get("payload") if isinstance(a.get("payload"), dict) else {}
        reason = str(a.get("reason") or "")

        # hard safety gates (must match contract)
        if entity_type not in contract.allowed_entity_types:
            skipped += 1
            continue
        if op not in contract.allowed_operations:
            skipped += 1
            continue

        # ---- RehabTask.create ----
        if entity_type == "RehabTask" and op == "create":
            title = str(payload.get("title") or "").strip()
            if not title:
                skipped += 1
                continue

            t = RehabTask(
                org_id=org_id,
                property_id=run.property_id,
                title=title,
                category=str(payload.get("category") or "general"),
                status=str(payload.get("status") or "todo"),
                cost_estimate=float(payload.get("cost_estimate") or 0.0),
                notes=str(payload.get("notes") or reason or ""),
            )
            db.add(t)
            applied += 1
            continue

        # ---- WorkflowEvent.create ----
        if entity_type == "WorkflowEvent" and op == "create":
            event_type = str(payload.get("event_type") or "").strip()
            if not event_type:
                skipped += 1
                continue

            db.add(
                WorkflowEvent(
                    org_id=org_id,
                    property_id=run.property_id,
                    actor_user_id=actor_user_id,
                    event_type=event_type,
                    payload_json=_dumps(payload.get("payload") or {}),
                )
            )
            applied += 1
            continue

        # ---- PropertyChecklistItem.update_status ----
        if entity_type == "PropertyChecklistItem" and op == "update_status":
            item_id = payload.get("id")
            new_status = str(payload.get("status") or "").strip()
            if not item_id or not new_status:
                skipped += 1
                continue

            item = db.scalar(
                select(PropertyChecklistItem).where(
                    PropertyChecklistItem.org_id == org_id,
                    PropertyChecklistItem.id == int(item_id),
                )
            )
            if item is None:
                skipped += 1
                continue

            item.status = new_status
            db.add(item)
            applied += 1
            continue

        skipped += 1

    return applied, skipped