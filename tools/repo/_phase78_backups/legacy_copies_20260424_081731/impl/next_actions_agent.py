# backend/app/domain/agents/impl/next_actions_agent.py
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.agents.llm_router import run_llm_agent
from app.models import AgentRun, Property


def _loads_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return value
    return value


def _extract_latest_runs(db: Session, *, org_id: int, property_id: int, limit: int = 12) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(AgentRun)
        .where(AgentRun.org_id == int(org_id), AgentRun.property_id == int(property_id))
        .order_by(AgentRun.id.desc())
        .limit(int(limit))
    ).all()

    out: list[dict[str, Any]] = []
    for row in rows:
        output = _loads_json(getattr(row, "output_json", None))
        proposed = _loads_json(getattr(row, "proposed_actions_json", None))
        out.append(
            {
                "run_id": int(row.id),
                "agent_key": str(row.agent_key),
                "status": str(row.status),
                "approval_status": str(getattr(row, "approval_status", None) or "not_required"),
                "last_error": getattr(row, "last_error", None),
                "summary": output.get("summary") if isinstance(output, dict) else None,
                "has_proposed_actions": bool(proposed),
            }
        )
    return out


def run_next_actions_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "next_actions",
            "summary": "Next actions skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before next actions can be synthesized.",
                    "priority": "high",
                }
            ],
            "actions": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "next_actions",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "recommendations": [],
            "actions": [],
        }

    latest_runs = _extract_latest_runs(db, org_id=int(org_id), property_id=int(property_id))
    pending_approvals = [r for r in latest_runs if str(r.get("approval_status", "")).lower() == "pending"]
    failed_runs = [r for r in latest_runs if str(r.get("status", "")).lower() in {"failed", "timed_out"}]
    missing_specialists: list[str] = []

    must_ship = {"underwrite", "rent_reasonableness", "hqs_precheck", "packet_builder"}
    seen = {str(r.get("agent_key") or "") for r in latest_runs}
    for agent_key in sorted(must_ship):
        if agent_key not in seen:
            missing_specialists.append(agent_key)

    recommendations: list[dict[str, Any]] = []
    if pending_approvals:
        recommendations.append(
            {
                "type": "clear_pending_approvals",
                "reason": f"{len(pending_approvals)} agent run(s) are blocked pending approval.",
                "priority": "high",
                "run_ids": [int(r["run_id"]) for r in pending_approvals[:10]],
            }
        )
    if failed_runs:
        recommendations.append(
            {
                "type": "retry_failed_runs",
                "reason": f"{len(failed_runs)} agent run(s) failed or timed out and should be reviewed.",
                "priority": "high",
                "run_ids": [int(r["run_id"]) for r in failed_runs[:10]],
            }
        )
    if missing_specialists:
        recommendations.append(
            {
                "type": "run_missing_specialists",
                "reason": "Some core specialist agents have not run yet for this property.",
                "priority": "medium",
                "agents": missing_specialists,
            }
        )
    if not recommendations:
        recommendations.append(
            {
                "type": "review_recent_outputs",
                "reason": "Core specialist runs exist; review the latest outputs and progress the property.",
                "priority": "medium",
            }
        )

    facts = {
        "property_id": int(property_id),
        "address": getattr(prop, "address", None),
        "pending_approvals": len(pending_approvals),
        "failed_runs": len(failed_runs),
        "missing_specialists": missing_specialists,
        "latest_runs": latest_runs[:10],
    }

    deterministic = {
        "agent_key": "next_actions",
        "summary": "Next actions synthesized from current property run history.",
        "facts": facts,
        "recommendations": recommendations,
        "actions": [],
        "confidence": 0.86,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="next_actions",
            context={"deterministic_baseline": deterministic, "input_payload": input_payload},
            mode="hybrid",
        )
        llm_output["facts"] = {**facts, **(llm_output.get("facts") or {})}
        llm_output["agent_key"] = "next_actions"
        llm_output["actions"] = []
        return llm_output
    except Exception:
        return deterministic
    