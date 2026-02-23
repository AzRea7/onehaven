# onehaven_decision_engine/backend/app/domain/agents/registry.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import Property, Deal, PropertyState
from app.policy_models import JurisdictionProfile
from app.services.hud_fmr_service import get_fmr_for_property
from app.domain.compliance.hqs_library import load_hqs_items


def _property_context(db: Session, org_id: int, property_id: Optional[int]) -> dict[str, Any]:
    if not property_id:
        return {"property": None, "deal": None, "stage": None}

    p = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    d = db.scalar(select(Deal).where(Deal.org_id == org_id, Deal.property_id == property_id))
    s = db.scalar(select(PropertyState).where(PropertyState.org_id == org_id, PropertyState.property_id == property_id))
    return {"property": p, "deal": d, "stage": (s.current_stage if s else None)}


def agent_deal_intake(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]
    d = ctx["deal"]

    missing = []
    if p is None:
        missing.append("property")
    else:
        if not getattr(p, "address", None): missing.append("address")
        if not getattr(p, "city", None): missing.append("city")
        if not getattr(p, "zip", None): missing.append("zip")
        if not getattr(p, "bedrooms", None): missing.append("bedrooms")

    if d is None:
        missing.append("deal")
    else:
        if getattr(d, "purchase_price", None) in (None, 0): missing.append("purchase_price")

    actions = []
    if missing:
        actions.append({
            "entity_type": "WorkflowEvent",
            "op": "create",
            "payload": {
                "event_type": "deal_intake_missing_fields",
                "payload": {"missing": missing},
            },
            "reason": "Intake cannot complete until required fields exist."
        })

    return {
        "agent_key": "deal_intake",
        "summary": "Deal intake validation + next required steps.",
        "facts": {
            "property_id": property_id,
            "missing": missing,
        },
        "actions": actions,
    }


def agent_rent_reasonableness(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]

    if p is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
        }

    # Deterministic baseline: HUD FMR (gross rent anchor)
    fmr = get_fmr_for_property(db, org_id=org_id, prop=p)

    # Required comparability factors (these are the ones your tests should demand)
    factors = [
        "location",
        "quality",
        "size",
        "unit_type",
        "age",
        "amenities",
        "services",
        "utilities",
    ]

    # Deterministic recommendation: conservative band around payment standard
    # (Later you’ll tighten this using comps + utility allowance)
    recommended = None
    if isinstance(fmr, dict) and fmr.get("fmr") is not None:
        recommended = float(fmr["fmr"]) * float(fmr.get("payment_standard_pct") or 1.10)

    return {
        "agent_key": "rent_reasonableness",
        "summary": "Rent reasonableness baseline computed using HUD FMR anchor; comps layer can refine.",
        "facts": {
            "property_id": property_id,
            "hud_fmr": fmr,
            "required_comparability_factors": factors,
            "recommended_gross_rent": recommended,
        },
        "actions": [
            {
                "entity_type": "WorkflowEvent",
                "op": "create",
                "payload": {
                    "event_type": "rent_reasonableness_computed",
                    "payload": {
                        "recommended_gross_rent": recommended,
                        "factors": factors,
                        "hud_fmr": fmr,
                    },
                },
                "reason": "Persist a traceable rent reasonableness artifact."
            }
        ],
    }


def agent_hqs_precheck(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]

    if p is None:
        return {"agent_key": "hqs_precheck", "summary": "No property found.", "facts": {"property_id": property_id}, "actions": []}

    # Canonical HQS library baseline
    items = load_hqs_items()

    # Deterministic “likely fail points” starter:
    # (Later: incorporate historical inspection failures + rehab history)
    likely = [x for x in items if x.get("severity") == "fail"][:12]

    proposed = []
    for it in likely[:6]:
        proposed.append({
            "entity_type": "RehabTask",
            "op": "create",
            "payload": {
                "title": f"HQS precheck: {it.get('title')}",
                "category": it.get("category") or "safety",
                "status": "todo",
                "cost_estimate": float(it.get("default_cost_estimate") or 0.0),
                "notes": it.get("remediation_hint") or "",
            },
            "reason": "Convert likely HQS failures into rehab tasks (approval required).",
        })

    return {
        "agent_key": "hqs_precheck",
        "summary": "HQS precheck generated from canonical HQS baseline; proposes rehab tasks for likely fails.",
        "facts": {
            "property_id": property_id,
            "hqs_items_total": len(items),
            "likely_fail_count": len(likely),
        },
        "actions": proposed,
    }


def agent_packet_builder(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]

    if p is None:
        return {"agent_key": "packet_builder", "summary": "No property found.", "facts": {"property_id": property_id}, "actions": []}

    # Pull jurisdiction profile (seeded + versioned)
    jp = db.scalar(
        select(JurisdictionProfile).where(
            JurisdictionProfile.org_id == org_id,
            JurisdictionProfile.state == p.state,
            JurisdictionProfile.city == p.city,
        )
    )

    checklist = (jp.packet_requirements_json if jp and jp.packet_requirements_json else None)

    return {
        "agent_key": "packet_builder",
        "summary": "Builds a jurisdiction-specific Section 8 packet checklist (RFTA/HAP onboarding artifacts).",
        "facts": {
            "property_id": property_id,
            "jurisdiction_profile_found": jp is not None,
            "packet_checklist": checklist,
        },
        "actions": [
            {
                "entity_type": "WorkflowEvent",
                "op": "create",
                "payload": {
                    "event_type": "packet_checklist_generated",
                    "payload": {"packet_checklist": checklist or []},
                },
                "reason": "Persist packet checklist so ops can track completion."
            }
        ],
    }


def agent_timeline_nudger(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    # This agent turns “next actions” into workflow events (later tasks/reminders).
    # You already generate next actions in Phase 4; this makes it operational.
    return {
        "agent_key": "timeline_nudger",
        "summary": "Converts outstanding constraints into reminders and nudges (workflow continuity).",
        "facts": {"property_id": property_id},
        "actions": [
            {
                "entity_type": "WorkflowEvent",
                "op": "create",
                "payload": {
                    "event_type": "timeline_nudge",
                    "payload": {"property_id": property_id},
                },
                "reason": "Keeps ops loop moving (no silent stalls)."
            }
        ],
    }


AGENTS = {
    "deal_intake": agent_deal_intake,
    "rent_reasonableness": agent_rent_reasonableness,
    "hqs_precheck": agent_hqs_precheck,
    "packet_builder": agent_packet_builder,
    "timeline_nudger": agent_timeline_nudger,
}