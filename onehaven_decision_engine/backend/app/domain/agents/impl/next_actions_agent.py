from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AgentRun, PropertyState


def _loads(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return None


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    state = None
    if property_id is not None:
        state = db.scalar(select(PropertyState).where(PropertyState.org_id == int(org_id), PropertyState.property_id == int(property_id)))

    outstanding = _loads(getattr(state, "outstanding_tasks_json", None)) or {}
    constraints = _loads(getattr(state, "constraints_json", None)) or {}

    runs = []
    if property_id is not None:
        runs = db.scalars(
            select(AgentRun)
            .where(AgentRun.org_id == int(org_id), AgentRun.property_id == int(property_id))
            .order_by(AgentRun.id.desc())
            .limit(12)
        ).all()

    blocked = [r for r in runs if str(getattr(r, "approval_status", "")).lower() == "pending"]
    failed = [r for r in runs if str(getattr(r, "status", "")).lower() in {"failed", "timed_out"}]

    recs: list[dict[str, Any]] = []
    if blocked:
        recs.append(
            {
                "type": "approval_queue",
                "title": "Review pending approval-gated agent actions",
                "reason": f"{len(blocked)} run(s) are blocked waiting for approval.",
                "priority": "high",
            }
        )
    if failed:
        recs.append(
            {
                "type": "failed_runs",
                "title": "Investigate recent failed runs",
                "reason": f"{len(failed)} run(s) failed or timed out and may hide real workflow blockers.",
                "priority": "high",
            }
        )
    recs.append(
        {
            "type": "state_cleanup",
            "title": "Resolve top workflow blockers",
            "reason": "Turn state constraints and outstanding tasks into a short operator checklist instead of letting entropy win.",
            "priority": "medium",
        }
    )

    return {
        "agent_key": "next_actions",
        "summary": "Next Actions synthesized the current state, pending approvals, and recent run health into operator CTAs.",
        "facts": {
            "property_id": property_id,
            "current_stage": getattr(state, "current_stage", None) if state is not None else None,
            "pending_approvals": len(blocked),
            "failed_runs": len(failed),
            "outstanding_tasks": outstanding,
            "constraints": constraints,
        },
        "recommendations": recs,
    }
