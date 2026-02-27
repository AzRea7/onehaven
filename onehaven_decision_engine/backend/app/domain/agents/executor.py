# backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.agents.registry import AGENTS

# Optional trust wiring (no hard dependency)
try:
    from app.services.trust_service import record_signal, recompute_and_persist  # type: ignore
except Exception:  # pragma: no cover
    def record_signal(*args, **kwargs):  # type: ignore
        return None

    def recompute_and_persist(*args, **kwargs):  # type: ignore
        return None


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
        # Trust: unknown agent is a stability failure (schema drift / UI mismatch).
        try:
            record_signal(
                db,
                org_id=org_id,
                entity_type="provider",
                entity_id="agent_engine",
                signal_key="agent.unknown_agent_key",
                value=0.0,
                meta={"agent_key": agent_key},
            )
            recompute_and_persist(db, org_id, "provider", "agent_engine")
        except Exception:
            pass

        return AgentResult(status="failed", output={}, error=f"Unknown agent_key: {agent_key}")

    payload = _loads(input_json, {})
    try:
        out = fn(
            db,
            org_id=org_id,
            property_id=property_id,
            input_payload=payload if isinstance(payload, dict) else {},
        )
    except Exception as e:
        # Trust: agent hard-failed during execution.
        try:
            record_signal(
                db,
                org_id=org_id,
                entity_type="provider",
                entity_id=f"agent:{agent_key}",
                signal_key=f"agent.{agent_key}.success",
                value=0.0,
                meta={"property_id": property_id, "error": str(e)},
            )
            recompute_and_persist(db, org_id, "provider", f"agent:{agent_key}")
            if property_id is not None:
                record_signal(
                    db,
                    org_id=org_id,
                    entity_type="property",
                    entity_id=str(property_id),
                    signal_key=f"agent.{agent_key}.success",
                    value=0.0,
                    meta={"error": str(e)},
                )
                recompute_and_persist(db, org_id, "property", str(property_id))
        except Exception:
            pass

        return AgentResult(status="failed", output={}, error=str(e))

    if not isinstance(out, dict):
        try:
            record_signal(
                db,
                org_id=org_id,
                entity_type="provider",
                entity_id=f"agent:{agent_key}",
                signal_key=f"agent.{agent_key}.output_is_dict",
                value=0.0,
                meta={"property_id": property_id},
            )
            recompute_and_persist(db, org_id, "provider", f"agent:{agent_key}")
        except Exception:
            pass

        return AgentResult(status="failed", output={}, error="Agent returned non-dict output")

    out.setdefault("actions", [])

    # Trust: agent executed successfully (not “actions applied”, just “agent returned a valid dict output”).
    try:
        record_signal(
            db,
            org_id=org_id,
            entity_type="provider",
            entity_id=f"agent:{agent_key}",
            signal_key=f"agent.{agent_key}.success",
            value=1.0,
            meta={"property_id": property_id, "actions_count": len(out.get("actions") or [])},
        )
        recompute_and_persist(db, org_id, "provider", f"agent:{agent_key}")

        if property_id is not None:
            record_signal(
                db,
                org_id=org_id,
                entity_type="property",
                entity_id=str(property_id),
                signal_key=f"agent.{agent_key}.success",
                value=1.0,
                meta={"actions_count": len(out.get("actions") or [])},
            )
            recompute_and_persist(db, org_id, "property", str(property_id))
    except Exception:
        pass

    return AgentResult(status="done", output=out, error=None)
