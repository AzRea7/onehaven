# onehaven_decision_engine/backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.agents.registry import AGENTS


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

    # normalize
    out.setdefault("actions", [])
    return AgentResult(status="done", output=out, error=None)