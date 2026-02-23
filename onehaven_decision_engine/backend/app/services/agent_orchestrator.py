# backend/app/services/agent_orchestrator.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..config import settings
from ..models import AgentRun, Property, PropertyState
from ..policy_models import JurisdictionProfile
from .property_state_machine import compute_and_persist_stage


@dataclass(frozen=True)
class PlannedRun:
    agent_key: str
    reason: str
    idempotency_key: str


def _loads_json(s: Optional[str]):
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


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

    next_actions = _loads_json(getattr(st, "outstanding_tasks_json", None)) or []
    constraints = _loads_json(getattr(st, "constraints_json", None)) or []

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

    # Trigger-based: next actions
    for a in next_actions:
        typ = str((a or {}).get("type") or "").lower()
        if "valuation_due" in typ:
            planned.append(("timeline_nudger", "next_action=valuation_due"))
        if "rent_gap" in typ:
            planned.append(("rent_reasonableness", "next_action=rent_gap"))

    # Build idempotency keys based on state snapshot
    state_blob = {
        "stage": stage,
        "next_actions": next_actions,
        "constraints": constraints,
        "property_id": property_id,
    }
    fp = _fingerprint(state_blob)

    out: List[PlannedRun] = []
    for agent_key, reason in planned:
        idem = f"{org_id}:{property_id}:{agent_key}:{fp}"
        out.append(PlannedRun(agent_key=agent_key, reason=reason, idempotency_key=idem))

    # De-dupe (keep first reason)
    uniq: dict[str, PlannedRun] = {}
    for r in out:
        uniq.setdefault(r.agent_key, r)
    return list(uniq.values())