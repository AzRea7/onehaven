# backend/app/services/agent_orchestrator.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import AgentRun, Property
from .property_state_machine import compute_and_persist_stage


@dataclass(frozen=True)
class PlannedRun:
    property_id: int
    agent_key: str
    reason: str
    idempotency_key: str


def _loads_json(val: Any):
    """
    Defensive JSON loader:
      - If val is already list/dict -> return as-is
      - If val is a JSON string -> parse
      - If val is a plain string (not JSON) -> return the raw string
      - Else -> None
    """
    if val is None:
        return None
    if isinstance(val, (list, dict, int, float, bool)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return s
    return None


def _normalize_next_actions(raw: Any) -> list[dict[str, Any]]:
    """
    Guarantee list[dict] so downstream code can safely do (a or {}).get("type").
    Accepts:
      - list[dict]
      - list[str]
      - dict
      - str
      - JSON string encoding any of the above
    """
    decoded = _loads_json(raw)

    if decoded is None:
        return []

    # Single dict
    if isinstance(decoded, dict):
        return [decoded]

    # Single string
    if isinstance(decoded, str):
        return [{"type": decoded}]

    # List
    if isinstance(decoded, list):
        out: list[dict[str, Any]] = []
        for a in decoded:
            if a is None:
                continue
            if isinstance(a, dict):
                out.append(a)
            elif isinstance(a, str):
                out.append({"type": a})
            else:
                out.append({"type": "note", "value": str(a)})
        return out

    # Unknown -> stringify
    return [{"type": "note", "value": str(decoded)}]


def _fingerprint(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def plan_agent_runs(db: Session, *, org_id: int, property_id: int) -> List[PlannedRun]:
    """
    Deterministic orchestration:
      - reads property stage + next actions
      - selects agents
      - enforces caps to prevent spam loops
    """
    prop = db.scalar(select(Property).where(Property.org_id == org_id).where(Property.id == property_id))
    if prop is None:
        return []

    # Ensure state is up to date (Phase 4 hook)
    st = compute_and_persist_stage(db, org_id=org_id, property=prop)

    # Anti-spam cap: max N runs/property/hour
    bucket = _hour_bucket(datetime.utcnow())
    count = db.scalar(
        select(func.count(AgentRun.id))
        .where(AgentRun.org_id == org_id)
        .where(AgentRun.property_id == property_id)
        .where(AgentRun.created_at >= bucket)
    )
    if int(count or 0) >= int(settings.agents_max_runs_per_property_per_hour):
        return []

    # ✅ critical: normalize to list[dict]
    next_actions = _normalize_next_actions(getattr(st, "outstanding_tasks_json", None))

    # constraints might be list/dict, JSON str, or junk string. Keep it safe.
    constraints = _loads_json(getattr(st, "constraints_json", None)) or []
    if isinstance(constraints, str):
        constraints = [{"type": "note", "value": constraints}]

    stage = (getattr(st, "current_stage", "deal") or "deal").strip().lower()

    planned: list[tuple[str, str]] = []

    # Core logic
    if stage in {"deal", "intake"}:
        planned.append(("deal_intake", "stage=deal/intake"))
        planned.append(("public_records_check", "stage=deal/intake"))
        planned.append(("packet_builder", "stage=deal/intake (packet readiness begins early)"))

    if stage in {"rent", "underwrite", "evaluate"}:
        planned.append(("rent_reasonableness", "stage implies rent validation"))
        planned.append(("packet_builder", "rent stage: ensure packet checklist exists"))

    if stage in {"compliance", "inspection"}:
        planned.append(("hqs_precheck", "stage implies HQS readiness"))
        planned.append(("timeline_nudger", "compliance stage needs timeline pressure"))

    # Trigger-based: next actions (now safe even if original stored strings)
    for a in next_actions:
        typ = str((a or {}).get("type") or "").lower()

        if "valuation_due" in typ:
            planned.append(("timeline_nudger", "next_action=valuation_due"))

        if "rent_gap" in typ:
            planned.append(("rent_reasonableness", "next_action=rent_gap"))

    # Build idempotency keys based on state snapshot
    state_blob = {
        "plan_version": getattr(settings, "decision_version", "v0"),
        "stage": stage,
        "next_actions": next_actions,
        "constraints": constraints,
        "property_id": property_id,
    }
    fp = _fingerprint(state_blob)

    out: List[PlannedRun] = []
    for agent_key, reason in planned:
        idem = f"{org_id}:{property_id}:{agent_key}:{fp}"
        out.append(
            PlannedRun(
                property_id=property_id,
                agent_key=agent_key,
                reason=reason,
                idempotency_key=idem
            )
        )

    # De-dupe (keep first reason)
    uniq: dict[str, PlannedRun] = {}
    for r in out:
        uniq.setdefault(r.agent_key, r)
    return list(uniq.values())
