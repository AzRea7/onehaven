# backend/app/domain/agents/impl/packet_builder_agent.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.agents.llm_router import run_llm_agent
from app.models import Property
from app.policy_models import JurisdictionProfile


def _to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return [value]


def run_packet_builder_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "packet_builder",
            "summary": "Packet builder skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before packet builder can run.",
                    "priority": "high",
                }
            ],
            "actions": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "packet_builder",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "recommendations": [],
            "actions": [],
        }

    jurisdiction = db.scalar(
        select(JurisdictionProfile).where(
            JurisdictionProfile.org_id == int(org_id),
            JurisdictionProfile.state == getattr(prop, "state", None),
            JurisdictionProfile.city == getattr(prop, "city", None),
        )
    )

    packet_requirements = []
    workflow_steps = []
    if jurisdiction is not None:
        packet_requirements = _to_list(getattr(jurisdiction, "packet_requirements_json", None))
        workflow_steps = _to_list(getattr(jurisdiction, "workflow_steps_json", None))

    missing_artifacts: list[str] = []
    if not packet_requirements and not workflow_steps:
        missing_artifacts.append("jurisdiction_packet_profile")

    recommendations = [
        {
            "type": "packet_checklist_generated",
            "title": "Packet checklist generated",
            "reason": "Use this checklist to drive packet completion for RFTA/HAP onboarding.",
            "priority": "medium",
            "packet_requirements_count": len(packet_requirements),
            "workflow_steps_count": len(workflow_steps),
        }
    ]
    if missing_artifacts:
        recommendations.append(
            {
                "type": "missing_packet_profile_data",
                "reason": "Jurisdiction packet requirements are missing or sparse, so packet quality may be limited.",
                "priority": "high",
                "missing": missing_artifacts,
            }
        )

    facts = {
        "property_id": int(property_id),
        "address": getattr(prop, "address", None),
        "jurisdiction_profile_found": jurisdiction is not None,
        "packet_requirements": packet_requirements,
        "workflow_steps": workflow_steps,
        "missing_artifacts": missing_artifacts,
    }

    deterministic = {
        "agent_key": "packet_builder",
        "summary": "Jurisdiction-specific packet checklist assembled.",
        "facts": facts,
        "recommendations": recommendations,
        "actions": [],
        "confidence": 0.84,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="packet_builder",
            context={"deterministic_baseline": deterministic, "input_payload": input_payload},
            mode="hybrid",
        )
        llm_output["facts"] = {**facts, **(llm_output.get("facts") or {})}
        llm_output["agent_key"] = "packet_builder"
        llm_output["actions"] = []
        return llm_output
    except Exception:
        return deterministic
    