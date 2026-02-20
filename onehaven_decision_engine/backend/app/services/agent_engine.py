# backend/app/services/agent_engine.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import AgentRun, AgentMessage, Property
from ..domain.agents.registry import AGENTS
from ..domain.agents.executor import execute_agent


def create_and_execute_run(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    input_json: Optional[str],
) -> AgentRun:
    if agent_key not in AGENTS:
        raise ValueError("unknown agent_key")

    if property_id is not None:
        p = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
        if not p:
            raise ValueError("property not found")

    run = AgentRun(
        org_id=org_id,
        agent_key=agent_key,
        property_id=property_id,
        status="running",
        input_json=input_json or "{}",
        output_json=None,
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        inp = json.loads(run.input_json or "{}")
        if not isinstance(inp, dict):
            inp = {}
    except Exception:
        inp = {}

    out = execute_agent(db, org_id=org_id, agent_key=agent_key, property_id=property_id, input_obj=inp)

    run.output_json = json.dumps(out)
    run.status = "done"
    db.add(run)

    # Emit thread message
    msg = AgentMessage(
        org_id=org_id,
        thread_key=f"run:{run.id}",
        sender=agent_key,
        recipient="human",
        message=str(out.get("summary") or "done"),
        created_at=datetime.utcnow(),
    )
    db.add(msg)

    db.commit()
    db.refresh(run)
    return run