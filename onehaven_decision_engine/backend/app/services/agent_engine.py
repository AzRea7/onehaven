# backend/app/services/agent_engine.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import AgentRun, AgentMessage, Property
from ..domain.agents.registry import AGENTS
from ..domain.agents.executor import execute_agent


def _safe_json_load(s: str | None) -> dict:
    if not s:
        return {}
    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _safe_json_dump(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return json.dumps({"ok": False, "error": "failed_to_serialize_output"})


def create_and_execute_run(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    input_json: Optional[str],
) -> AgentRun:
    """
    Deterministic v1:
      - validates agent_key
      - enforces org boundary on property (if provided)
      - creates AgentRun(status=running)
      - executes agent
      - writes AgentRun(output_json, status)
      - emits a thread message in AgentMessage(thread_key=run:{id})
    """
    if agent_key not in AGENTS:
        raise ValueError("unknown agent_key")

    if property_id is not None:
        prop = db.scalar(
            select(Property).where(Property.id == property_id).where(Property.org_id == org_id)
        )
        if not prop:
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

    inp = _safe_json_load(run.input_json)

    try:
        out = execute_agent(
            db,
            org_id=org_id,
            agent_key=agent_key,
            property_id=property_id,
            input_obj=inp,
        )

        # normalize “out” to dict (agents should return dict, but don’t trust it)
        if not isinstance(out, dict):
            out = {"ok": True, "summary": str(out)}

        run.output_json = _safe_json_dump(out)
        run.status = "done"
        db.add(run)

        msg_text = str(out.get("summary") or out.get("message") or "done")
        db.add(
            AgentMessage(
                org_id=org_id,
                thread_key=f"run:{run.id}",
                sender=agent_key,
                recipient="human",
                message=msg_text,
                created_at=datetime.utcnow(),
            )
        )

        db.commit()
        db.refresh(run)
        return run

    except Exception as e:
        # harden: never leave runs “running” forever
        err = {"ok": False, "error": type(e).__name__, "detail": str(e)}
        run.output_json = _safe_json_dump(err)
        run.status = "error"
        db.add(run)

        db.add(
            AgentMessage(
                org_id=org_id,
                thread_key=f"run:{run.id}",
                sender=agent_key,
                recipient="human",
                message=f"error: {type(e).__name__}: {e}",
                created_at=datetime.utcnow(),
            )
        )

        db.commit()
        db.refresh(run)
        return run
    