# backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy.orm import Session

from .contracts import validate_agent_output
from .registry import AGENTS, AgentContext
from ...models import WorkflowEvent, AgentRun


def execute_agent(db: Session, *, org_id: int, run: AgentRun) -> dict:
    agent_key = str(run.agent_key)

    fn = AGENTS.get(agent_key)
    if fn is None:
        raise ValueError(f"Unknown agent_key={agent_key}")

    output = fn(db, AgentContext(org_id=org_id, property_id=int(run.property_id), run_id=int(run.id)))

    ok, errors = validate_agent_output(agent_key, output)
    if not ok:
        raise ValueError("Contract validation failed: " + "; ".join(errors))

    # recommend-only: write WorkflowEvents as auditable artifacts
    actions = output.get("actions") or []
    for a in actions:
        data = a.get("data") or {}
        event_type = data.get("event_type")
        payload = data.get("payload")
        if not event_type:
            continue

        we = WorkflowEvent(
            org_id=org_id,
            property_id=int(run.property_id),
            actor_user_id=None,
            event_type=str(event_type),
            payload_json=json.dumps(payload, ensure_ascii=False) if payload is not None else None,
            created_at=datetime.utcnow(),
        )
        db.add(we)

    return output