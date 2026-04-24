# backend/app/domain/agents/impl/hqs_precheck_agent.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.agents.llm_router import run_llm_agent
from onehaven_platform.backend.src.adapters.compliance_adapter import get_effective_hqs_items
from onehaven_platform.backend.src.models import Property


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v if v is not None else default)
    except Exception:
        return default


def run_hqs_precheck_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "hqs_precheck",
            "summary": "HQS precheck skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "actions": [],
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before HQS precheck can run.",
                    "priority": "high",
                }
            ],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "hqs_precheck",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
            "recommendations": [],
        }

    lib = get_effective_hqs_items(db, org_id=int(org_id), prop=prop)
    items = lib.get("items") or []
    likely_fails = [x for x in items if str(x.get("severity") or "").lower() == "fail"]
    warn_items = [x for x in items if str(x.get("severity") or "").lower() in {"warn", "warning"}]

    actions: list[dict[str, Any]] = []
    recommendations: list[dict[str, Any]] = []

    for item in likely_fails[:8]:
        code = str(item.get("code") or "unknown")
        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(property_id),
                    "title": f"HQS precheck: {code}",
                    "category": item.get("category") or "safety",
                    "status": "todo",
                    "cost_estimate": _safe_float(item.get("default_cost_estimate")),
                    "notes": item.get("suggested_fix") or f"Investigate and remediate {code}",
                    "inspection_relevant": True,
                },
                "reason": "Likely HQS fail item should become a rehab task pending human approval.",
            }
        )

    if likely_fails:
        recommendations.append(
            {
                "type": "likely_hqs_failures_found",
                "reason": f"{len(likely_fails)} likely HQS fail items were identified from the baseline checklist.",
                "priority": "high",
            }
        )
    elif warn_items:
        recommendations.append(
            {
                "type": "hqs_warning_items_found",
                "reason": f"{len(warn_items)} warning-level HQS items were identified.",
                "priority": "medium",
            }
        )
    else:
        recommendations.append(
            {
                "type": "no_major_hqs_findings",
                "reason": "No likely fail items were identified in the baseline HQS pass.",
                "priority": "low",
            }
        )

    facts = {
        "property_id": int(property_id),
        "address": getattr(prop, "address", None),
        "hqs_items_total": len(items),
        "likely_fail_count": len(likely_fails),
        "warning_count": len(warn_items),
        "sample_fail_codes": [str(x.get("code") or "") for x in likely_fails[:8]],
    }

    deterministic = {
        "agent_key": "hqs_precheck",
        "summary": "HQS precheck generated from the canonical HQS baseline.",
        "facts": facts,
        "actions": actions,
        "recommendations": recommendations,
        "confidence": 0.82,
        "needs_human_review": True,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="hqs_precheck",
            context={"deterministic_baseline": deterministic, "input_payload": input_payload},
            mode="hybrid",
        )
        llm_output["facts"] = {**facts, **(llm_output.get("facts") or {})}
        llm_output["agent_key"] = "hqs_precheck"
        if not isinstance(llm_output.get("actions"), list):
            llm_output["actions"] = actions
        llm_output["needs_human_review"] = True
        return llm_output
    except Exception:
        return deterministic
    