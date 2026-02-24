from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.models import AgentMessage


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
    Append a structured trace event to agent_messages.

    - message is JSON (router parses it)
    - thread_key is stable per run
    """
    evt = {
        "type": event_type,
        "level": level,
        "agent_key": agent_key,
        "ts": datetime.utcnow().isoformat(),
        "payload": payload or {},
    }

    db.add(
        AgentMessage(
            org_id=org_id,
            run_id=int(run_id),
            property_id=int(property_id) if property_id is not None else None,
            thread_key=f"run:{int(run_id)}",
            sender=str(agent_key),
            recipient="trace",
            message=_dumps(evt),
        )
    )
    # caller controls commit pattern; but your current agent_engine commits often
    # so keep this "no commit" and let caller commit.
    