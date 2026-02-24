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
      - rehab_task.create
      - workflow_event.create
      - checklist_item.update_status

    Contract canonical schema:
      {
        "entity_type": "rehab_task" | "workflow_event" | "checklist_item",
        "op": "create" | "update_status",
        "data": { ... }
      }

    Backwards-compat:
      Older actions might be TitleCase entity_type + "payload" instead of "data".
      We'll normalize so existing runs in DB can still apply.
    """
    contract = get_contract(str(run.agent_key))

    applied = 0
    skipped = 0

    def _normalize_entity_type(et: str) -> str:
        et = (et or "").strip()
        # Canonical values
        if et in {"rehab_task", "workflow_event", "checklist_item"}:
            return et
        # Backward-compat TitleCase / legacy names
        mapping = {
            "RehabTask": "rehab_task",
            "WorkflowEvent": "workflow_event",
            "PropertyChecklistItem": "checklist_item",
        }
        return mapping.get(et, et)

    for a in actions:
        if not isinstance(a, dict):
            skipped += 1
            continue

        entity_type_raw = str(a.get("entity_type") or "")
        entity_type = _normalize_entity_type(entity_type_raw)
        op = str(a.get("op") or "").strip()

        # Canonical uses "data"; legacy used "payload"
        data = a.get("data") if isinstance(a.get("data"), dict) else {}
        if not data:
            legacy_payload = a.get("payload") if isinstance(a.get("payload"), dict) else {}
            data = legacy_payload

        reason = str(a.get("reason") or "")

        # hard safety gates (must match contract)
        # NOTE: contract.allowed_entity_types are snake_case in your updated contracts.py
        if entity_type not in contract.allowed_entity_types:
            skipped += 1
            continue
        if op not in contract.allowed_operations:
            skipped += 1
            continue

        # ---- rehab_task.create ----
        if entity_type == "rehab_task" and op == "create":
            title = str(data.get("title") or "").strip()
            if not title:
                skipped += 1
                continue

            t = RehabTask(
                org_id=org_id,
                property_id=run.property_id,
                title=title,
                category=str(data.get("category") or "general"),
                status=str(data.get("status") or "todo"),
                cost_estimate=float(data.get("cost_estimate") or 0.0),
                notes=str(data.get("notes") or reason or ""),
            )
            db.add(t)
            applied += 1
            continue

        # ---- workflow_event.create ----
        if entity_type == "workflow_event" and op == "create":
            event_type = str(data.get("event_type") or "").strip()
            if not event_type:
                skipped += 1
                continue

            db.add(
                WorkflowEvent(
                    org_id=org_id,
                    property_id=run.property_id,
                    actor_user_id=actor_user_id,
                    event_type=event_type,
                    payload_json=_dumps(data.get("payload") or {}),
                )
            )
            applied += 1
            continue

        # ---- checklist_item.update_status ----
        if entity_type == "checklist_item" and op == "update_status":
            item_id = data.get("id")
            new_status = str(data.get("status") or "").strip()
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
