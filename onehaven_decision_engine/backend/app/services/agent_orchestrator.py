# onehaven_decision_engine/backend/app/services/agent_orchestrator.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.domain.fingerprint import stable_fingerprint
from app.models import AgentRun, PropertyState, WorkflowEvent


@dataclass(frozen=True)
class PlannedAgentRun:
    agent_key: str
    property_id: int
    reason: str
    input_payload: Dict[str, Any]
    idempotency_key: str


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _hourly_cap(db: Session, org_id: int, property_id: int) -> bool:
    """
    Hard cap: max N runs per property per hour (prevents agent spam loops).
    """
    max_per_hr = int(os.getenv("AGENTS_MAX_RUNS_PER_PROPERTY_PER_HOUR", "3"))
    since = datetime.utcnow() - timedelta(hours=1)

    n = db.scalar(
        select(func.count())
        .select_from(AgentRun)
        .where(AgentRun.org_id == org_id)
        .where(AgentRun.property_id == property_id)
        .where(AgentRun.created_at >= since)
    ) or 0

    return int(n) < max_per_hr


def _state_fingerprint(*, stage: str, constraints: Any, tasks: Any) -> str:
    return stable_fingerprint(
        {
            "stage": stage,
            "constraints": constraints,
            "tasks": tasks,
        }
    )


def plan_agent_runs(db: Session, *, org_id: int, property_id: int) -> List[PlannedAgentRun]:
    """
    Deterministic planner:
    - Reads PropertyState + recent workflow events
    - Chooses which agents to run now
    - Produces idempotency keys based on state fingerprint
    """
    ps = db.scalar(
        select(PropertyState)
        .where(PropertyState.org_id == org_id)
        .where(PropertyState.property_id == property_id)
    )
    if ps is None:
        return []

    if not _hourly_cap(db, org_id, property_id):
        return []

    stage = (ps.current_stage or "deal").strip().lower()
    constraints = _loads(ps.constraints_json, {})
    tasks = _loads(ps.outstanding_tasks_json, [])

    fp = _state_fingerprint(stage=stage, constraints=constraints, tasks=tasks)

    # Very explicit routing rules (you can expand over time):
    # Think of this like a “finite-state playbook”, not an LLM decision.
    plan: List[PlannedAgentRun] = []

    def add(agent_key: str, reason: str, payload: Dict[str, Any]) -> None:
        idem = stable_fingerprint({"org": org_id, "property": property_id, "agent": agent_key, "state_fp": fp})
        plan.append(
            PlannedAgentRun(
                agent_key=agent_key,
                property_id=property_id,
                reason=reason,
                input_payload=payload,
                idempotency_key=idem,
            )
        )

    # Stage-based defaults
    if stage in {"deal", "intake"}:
        add(
            "deal_intake",
            "Property is in deal/intake stage: validate intake completeness.",
            {"property_id": property_id, "stage": stage},
        )

    if stage in {"rent", "underwrite"}:
        add(
            "rent_reasonableness",
            "Property is in rent/underwrite stage: package rent narrative inputs.",
            {"property_id": property_id, "stage": stage},
        )

    if stage in {"compliance", "inspection"}:
        add(
            "hqs_precheck",
            "Property is in compliance/inspection stage: predict HQS fail points.",
            {"property_id": property_id, "stage": stage, "max_items": 15},
        )

    # Task-driven triggers (from your Phase 4 next-actions)
    # If your outstanding_tasks_json includes codes like "valuation_due" / "rent_gap", this catches them.
    tcodes = set()
    if isinstance(tasks, list):
        for t in tasks:
            if isinstance(t, dict) and isinstance(t.get("code"), str):
                tcodes.add(t["code"])

    if "valuation_due" in tcodes and stage not in {"deal", "intake"}:
        add(
            "rent_reasonableness",
            "Valuation due: run rent packaging again as supporting evidence bundle (placeholder agent).",
            {"property_id": property_id, "stage": stage, "trigger": "valuation_due"},
        )

    # Anti-repeat: don’t re-run same agent if we already have a run with same idempotency key
    out: List[PlannedAgentRun] = []
    for pr in plan:
        exists = db.scalar(
            select(AgentRun.id)
            .where(AgentRun.org_id == org_id)
            .where(AgentRun.idempotency_key == pr.idempotency_key)
        )
        if exists:
            continue
        out.append(pr)

    return out