# backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.agents.contracts import canonical_agent_key, get_contract
from onehaven_platform.backend.src.domain.agents.registry import AGENTS
from onehaven_platform.backend.src.services.agent_concurrency import (
    enforce_org_concurrency,
    release_agent_lock,
    try_acquire_agent_lock,
)

try:
    from onehaven_platform.backend.src.adapters.compliance_adapter import record_signal, recompute_and_persist  # type: ignore
except Exception:  # pragma: no cover
    def record_signal(*args, **kwargs):
        return None

    def recompute_and_persist(*args, **kwargs):
        return None


@dataclass
class AgentResult:
    status: str
    output: dict[str, Any]
    error: Optional[str] = None


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _trust(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    ok: float,
    meta: dict[str, Any],
) -> None:
    try:
        record_signal(
            db,
            org_id=int(org_id),
            entity_type="provider",
            entity_id=f"agent:{agent_key}",
            signal_key=f"agent.{agent_key}.success",
            value=float(ok),
            meta=meta,
        )
        try:
            recompute_and_persist(db, int(org_id), "provider", f"agent:{agent_key}")
        except TypeError:
            recompute_and_persist(
                db,
                org_id=int(org_id),
                entity_type="provider",
                entity_id=f"agent:{agent_key}",
            )

        if property_id is not None:
            record_signal(
                db,
                org_id=int(org_id),
                entity_type="property",
                entity_id=str(property_id),
                signal_key=f"agent.{agent_key}.success",
                value=float(ok),
                meta=meta,
            )
            try:
                recompute_and_persist(db, int(org_id), "property", str(property_id))
            except TypeError:
                recompute_and_persist(
                    db,
                    org_id=int(org_id),
                    entity_type="property",
                    entity_id=str(property_id),
                )
    except Exception:
        pass


def execute_agent(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    input_json: Optional[str],
) -> AgentResult:
    resolved_agent_key = canonical_agent_key(agent_key)
    fn = AGENTS.get(resolved_agent_key)

    if fn is None:
        _trust(
            db,
            org_id=int(org_id),
            agent_key="agent_engine",
            property_id=property_id,
            ok=0.0,
            meta={"unknown_agent_key": agent_key, "resolved_agent_key": resolved_agent_key},
        )
        return AgentResult(
            status="failed",
            output={},
            error=f"Unknown agent_key: {agent_key}",
        )

    payload = _loads(input_json, {})
    if not isinstance(payload, dict):
        payload = {}

    contract = get_contract(resolved_agent_key)
    lock_acquired = False

    try:
        enforce_org_concurrency(db, org_id=int(org_id))
        lock_acquired = try_acquire_agent_lock(
            db,
            org_id=int(org_id),
            agent_key=str(resolved_agent_key),
        )
        if not lock_acquired:
            _trust(
                db,
                org_id=int(org_id),
                agent_key=resolved_agent_key,
                property_id=property_id,
                ok=0.0,
                meta={"error": "agent_lock_busy", "resolved_agent_key": resolved_agent_key},
            )
            return AgentResult(
                status="failed",
                output={},
                error=f"agent_lock_busy:{resolved_agent_key}",
            )

        output = fn(
            db,
            org_id=int(org_id),
            property_id=property_id,
            input_payload=payload,
        )

        if not isinstance(output, dict):
            _trust(
                db,
                org_id=int(org_id),
                agent_key=resolved_agent_key,
                property_id=property_id,
                ok=0.0,
                meta={"error": "non_dict_output", "resolved_agent_key": resolved_agent_key},
            )
            return AgentResult(
                status="failed",
                output={},
                error="Agent returned non-dict output",
            )

        output.setdefault("agent_key", resolved_agent_key)
        output.setdefault("summary", f"{resolved_agent_key} completed")
        output.setdefault("facts", {"property_id": property_id})
        output.setdefault("confidence", 0.75)

        if contract.mode != "recommend_only":
            output.setdefault("actions", [])
            output.setdefault("recommendations", [])
            output.setdefault("needs_human_review", bool(contract.requires_human))
        else:
            output.setdefault("recommendations", [])
            output["actions"] = []
            output.setdefault("needs_human_review", False)

        _trust(
            db,
            org_id=int(org_id),
            agent_key=resolved_agent_key,
            property_id=property_id,
            ok=1.0,
            meta={
                "property_id": property_id,
                "resolved_agent_key": resolved_agent_key,
                "requested_agent_key": agent_key,
                "actions_count": len(output.get("actions") or []),
                "recommendations_count": len(output.get("recommendations") or []),
            },
        )
        return AgentResult(status="done", output=output, error=None)

    except Exception as exc:
        _trust(
            db,
            org_id=int(org_id),
            agent_key=resolved_agent_key,
            property_id=property_id,
            ok=0.0,
            meta={
                "error": str(exc),
                "resolved_agent_key": resolved_agent_key,
                "requested_agent_key": agent_key,
            },
        )
        return AgentResult(status="failed", output={}, error=str(exc))

    finally:
        if lock_acquired:
            try:
                release_agent_lock(
                    db,
                    org_id=int(org_id),
                    agent_key=str(resolved_agent_key),
                )
            except Exception:
                pass
            