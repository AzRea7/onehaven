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
    decoded = _loads_json(raw)

    if decoded is None:
        return []

    if isinstance(decoded, dict):
        return [decoded]

    if isinstance(decoded, str):
        return [{"type": decoded}]

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

    return [{"type": "note", "value": str(decoded)}]


def _fingerprint(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def plan_agent_runs(db: Session, *, org_id: int, property_id: int) -> List[PlannedRun]:
    prop = db.scalar(select(Property).where(Property.org_id == org_id).where(Property.id == property_id))
    if prop is None:
        return []

    # This call is allowed to write the state record (your system treats "state" as derived truth)
    st = compute_and_persist_stage(db, org_id=org_id, property=prop)

    # Rate-limit per property per hour (prevents runaway enqueue loops)
    bucket = _hour_bucket(datetime.utcnow())
    count = db.scalar(
        select(func.count(AgentRun.id))
        .where(AgentRun.org_id == org_id)
        .where(AgentRun.property_id == property_id)
        .where(AgentRun.created_at >= bucket)
    )
    if int(count or 0) >= int(settings.agents_max_runs_per_property_per_hour):
        return []

    next_actions = _normalize_next_actions(getattr(st, "outstanding_tasks_json", None))

    constraints = _loads_json(getattr(st, "constraints_json", None)) or []
    if isinstance(constraints, str):
        constraints = [{"type": "note", "value": constraints}]

    stage = (getattr(st, "current_stage", "deal") or "deal").strip().lower()

    planned: list[tuple[str, str]] = []

    # Stage-triggered specialists
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

    # Next-action-triggered specialists
    for a in next_actions:
        typ = str((a or {}).get("type") or "").lower()

        if "valuation_due" in typ:
            planned.append(("timeline_nudger", "next_action=valuation_due"))

        if "rent_gap" in typ:
            planned.append(("rent_reasonableness", "next_action=rent_gap"))

    # Always run the Judge last (recommend-only)
    planned.append(("ops_judge", "synthesize specialist outputs into a ranked next-step plan"))

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
                idempotency_key=idem,
            )
        )

    # Deduplicate by agent_key (keep first reason)
    uniq: dict[str, PlannedRun] = {}
    for r in out:
        uniq.setdefault(r.agent_key, r)
    return list(uniq.values())


# -----------------------------------------------------------------------------
# ✅ Missing worker hook
# -----------------------------------------------------------------------------
def _safe_json_dump(x: Any) -> str:
    try:
        return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return "{}"


def on_run_terminal(db: Session, *, run_id: int) -> None:
    """
    Called by worker when an AgentRun reaches a terminal state (done/failed/timed_out/blocked).
    Keep this lightweight and idempotent.

    What we do (MVP):
      1) Recompute & persist PropertyState (so UI stage/next actions update)
      2) Optionally add a simple "outstanding task" when certain agents fail
         (so your planner has something deterministic to react to)
    """
    r = db.scalar(select(AgentRun).where(AgentRun.id == int(run_id)))
    if r is None:
        return

    org_id = int(r.org_id)
    property_id = int(r.property_id) if getattr(r, "property_id", None) is not None else None
    if property_id is None:
        return

    prop = db.scalar(select(Property).where(Property.org_id == org_id).where(Property.id == property_id))
    if prop is None:
        return

    # 1) Always refresh derived state after a run finishes.
    st = compute_and_persist_stage(db, org_id=org_id, property=prop)

    # 2) Minimal deterministic failure → next_action hint
    status = str(getattr(r, "status", "") or "").lower()
    agent_key = str(getattr(r, "agent_key", "") or "").lower()

    if status in {"failed", "timed_out"}:
        # Keep format consistent with planner: list[dict[type,...]]
        existing = _normalize_next_actions(getattr(st, "outstanding_tasks_json", None))

        # Avoid duplicates
        def has_type(t: str) -> bool:
            return any(str(x.get("type", "")).lower() == t.lower() for x in existing if isinstance(x, dict))

        # Map a few common agent failures into actionable nudges.
        if agent_key == "rent_reasonableness" and not has_type("rent_gap"):
            existing.append({"type": "rent_gap", "source": "agent_failure", "run_id": int(r.id)})
        if agent_key in {"hqs_precheck", "packet_builder"} and not has_type("packet_incomplete"):
            existing.append({"type": "packet_incomplete", "source": "agent_failure", "run_id": int(r.id)})

        # Write back if we changed anything
        setattr(st, "outstanding_tasks_json", _safe_json_dump(existing))
        db.add(st)

    db.commit()
    