from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun, AgentTraceEvent, AgentMessage


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def emit_trace_safe(
    db: Session,
    *,
    org_id: int,
    run_id: int,
    agent_key: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    level: str = "info",
    property_id: Optional[int] = None,
) -> None:
    """
    Append a structured trace event.

    Source of truth for SSE: agent_trace_events.

    Optionally mirrors into agent_messages if TRACE_MIRROR_TO_MESSAGES=1.
    """
    pid = property_id
    if pid is None:
        r = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id), AgentRun.org_id == int(org_id)))
        if r is not None:
            pid = int(r.property_id) if r.property_id is not None else None

    evt = {
        "type": str(event_type),
        "level": str(level),
        "agent_key": str(agent_key),
        "ts": datetime.utcnow().isoformat(),
        "payload": payload or {},
    }

    db.add(
        AgentTraceEvent(
            org_id=int(org_id),
            run_id=int(run_id),
            property_id=int(pid) if pid is not None else None,
            agent_key=str(agent_key),
            event_type=str(event_type),
            payload_json=_dumps(evt),
        )
    )

    if os.getenv("TRACE_MIRROR_TO_MESSAGES", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}:
        db.add(
            AgentMessage(
                org_id=int(org_id),
                run_id=int(run_id),
                property_id=int(pid) if pid is not None else None,
                thread_key=f"run:{int(run_id)}",
                sender=str(agent_key),
                recipient="trace",
                message=_dumps(evt),
            )
        )
        