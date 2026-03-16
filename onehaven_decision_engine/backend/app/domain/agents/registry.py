# backend/app/domain/agents/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Deal, Property, PropertyState
from app.policy_models import JurisdictionProfile
from app.services.hud_fmr_service import get_or_fetch_fmr
from app.domain.compliance.hqs_library import get_effective_hqs_items

# Explicit specialist agents
from app.domain.agents.impl.underwrite_agent import run_underwrite_agent
from app.domain.agents.impl.rent_reasonableness_agent import run_rent_reasonableness_agent
from app.domain.agents.impl.hqs_precheck_agent import run_hqs_precheck_agent
from app.domain.agents.impl.packet_builder_agent import run_packet_builder_agent
from app.domain.agents.impl.photo_rehab_agent import run_photo_rehab_agent
from app.domain.agents.impl.next_actions_agent import run_next_actions_agent

# Existing deterministic agents that are still useful
from app.domain.agents.impl.deal_intake import run_deal_intake
from app.domain.agents.impl.ops_judge import run_ops_judge
from app.domain.agents.impl.timeline_nudger import run_timeline_nudger


AgentFn = Callable[[Session, int, Optional[int], dict[str, Any]], dict[str, Any]]


def _property_context(db: Session, org_id: int, property_id: Optional[int]) -> dict[str, Any]:
    if not property_id:
        return {"property": None, "deal": None, "stage": None, "jurisdiction_profile": None}

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    deal = db.scalar(
        select(Deal)
        .where(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
        .order_by(Deal.id.desc())
    )
    state = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == int(org_id),
            PropertyState.property_id == int(property_id),
        )
    )

    jp = None
    if prop is not None:
        try:
            jp = db.scalar(
                select(JurisdictionProfile).where(
                    JurisdictionProfile.org_id == int(org_id),
                    JurisdictionProfile.state == getattr(prop, "state", None),
                    JurisdictionProfile.city == getattr(prop, "city", None),
                )
            )
        except Exception:
            jp = None

    return {
        "property": prop,
        "deal": deal,
        "stage": (getattr(state, "current_stage", None) if state else None),
        "jurisdiction_profile": jp,
    }


def _fallback_deal_intake(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    prop = ctx["property"]
    deal = ctx["deal"]

    missing: list[str] = []
    if prop is None:
        missing.append("property")
    else:
        for field in ("address", "city", "state", "zip", "bedrooms"):
            if not getattr(prop, field, None):
                missing.append(f"property.{field}")

    if deal is None:
        missing.append("deal")
    else:
        if getattr(deal, "purchase_price", None) in (None, 0):
            missing.append("deal.purchase_price")

    recommendations: list[dict[str, Any]] = []
    if missing:
        recommendations.append(
            {
                "type": "missing_fields",
                "property_id": property_id,
                "missing": missing,
                "reason": "Deal intake cannot complete until required fields exist.",
                "priority": "high",
            }
        )

    return {
        "agent_key": "deal_intake",
        "summary": "Deal intake validation + next required steps.",
        "facts": {"property_id": property_id, "missing": missing, "stage": ctx["stage"]},
        "actions": [],
        "recommendations": recommendations,
    }


def _fallback_rent_reasonableness(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    prop = ctx["property"]
    if prop is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
            "recommendations": [],
        }

    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    fmr = get_or_fetch_fmr(
        db,
        org_id=int(org_id),
        area_name=(getattr(prop, "city", None) or "UNKNOWN"),
        state=(getattr(prop, "state", None) or "MI"),
        bedrooms=bedrooms,
    )
    fmr_val = float(getattr(fmr, "fmr", 0.0) or 0.0)
    required_factors = [
        "location",
        "quality",
        "size",
        "unit_type",
        "age",
        "amenities",
        "services",
        "utilities",
    ]
    recommended = fmr_val * 1.10 if fmr_val else None

    return {
        "agent_key": "rent_reasonableness",
        "summary": "Rent reasonableness baseline computed using HUD FMR cache anchor.",
        "facts": {
            "property_id": property_id,
            "hud_fmr": {
                "area_name": getattr(fmr, "area_name", None),
                "state": getattr(fmr, "state", None),
                "bedrooms": getattr(fmr, "bedrooms", None),
                "fmr": fmr_val,
            },
            "required_comparability_factors": required_factors,
            "recommended_gross_rent": recommended,
        },
        "actions": [],
        "recommendations": [
            {
                "type": "rent_reasonableness_computed",
                "property_id": property_id,
                "recommended_gross_rent": recommended,
                "factors": required_factors,
                "fmr": fmr_val,
                "reason": "Use this as a baseline and justify the contract rent with comps and utilities.",
                "priority": "medium",
            }
        ],
    }


def _fallback_hqs_precheck(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    prop = ctx["property"]
    if prop is None:
        return {
            "agent_key": "hqs_precheck",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
        }

    lib = get_effective_hqs_items(db, org_id=int(org_id), prop=prop)
    items = lib.get("items") or []
    likely = [x for x in items if str(x.get("severity") or "").lower() == "fail"][:12]

    actions: list[dict[str, Any]] = []
    for item in likely[:6]:
        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(prop.id),
                    "title": f"HQS precheck: {item.get('code')}",
                    "category": item.get("category") or "safety",
                    "status": "todo",
                    "cost_estimate": float(item.get("default_cost_estimate") or 0.0),
                    "notes": item.get("suggested_fix") or "",
                },
                "reason": "Convert likely HQS failures into rehab tasks.",
            }
        )

    return {
        "agent_key": "hqs_precheck",
        "summary": "HQS precheck generated from the canonical HQS baseline.",
        "facts": {
            "property_id": property_id,
            "hqs_items_total": len(items),
            "likely_fail_count": len(likely),
        },
        "actions": actions,
    }


def _fallback_packet_builder(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    jp = ctx["jurisdiction_profile"]

    checklist = None
    if jp is not None:
        checklist = (
            getattr(jp, "packet_requirements_json", None)
            or getattr(jp, "workflow_steps_json", None)
            or []
        )

    return {
        "agent_key": "packet_builder",
        "summary": "Builds a jurisdiction-specific Section 8 packet checklist.",
        "facts": {
            "property_id": property_id,
            "jurisdiction_profile_found": jp is not None,
            "packet_checklist": checklist,
        },
        "actions": [],
        "recommendations": [
            {
                "type": "packet_checklist_generated",
                "property_id": property_id,
                "packet_checklist": checklist or [],
                "reason": "Use as the completion checklist for RFTA/HAP onboarding artifacts.",
                "priority": "medium",
            }
        ],
    }


def _call_impl_or_fallback(
    impl: Optional[AgentFn],
    fallback: AgentFn,
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    try:
        if impl is not None:
            return impl(db, int(org_id), property_id, input_payload or {})
    except Exception:
        pass
    return fallback(db, int(org_id), property_id, input_payload or {})


def agent_deal_intake(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_deal_intake if callable(run_deal_intake) else None,
        _fallback_deal_intake,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_underwrite(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_underwrite_agent(db, int(org_id), property_id, input_payload or {})


def agent_rent_reasonableness(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_rent_reasonableness_agent if callable(run_rent_reasonableness_agent) else None,
        _fallback_rent_reasonableness,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_hqs_precheck(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_hqs_precheck_agent if callable(run_hqs_precheck_agent) else None,
        _fallback_hqs_precheck,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_packet_builder(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_packet_builder_agent if callable(run_packet_builder_agent) else None,
        _fallback_packet_builder,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_photo_rehab(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_photo_rehab_agent(db, int(org_id), property_id, input_payload or {})


def agent_next_actions(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_next_actions_agent(db, int(org_id), property_id, input_payload or {})


def agent_timeline_nudger(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_timeline_nudger(db, int(org_id), property_id, input_payload or {})


def agent_ops_judge(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_ops_judge(db, int(org_id), property_id, input_payload or {})


AGENTS: dict[str, AgentFn] = {
    "deal_intake": agent_deal_intake,
    "underwrite": agent_underwrite,
    "rent_reasonableness": agent_rent_reasonableness,
    "hqs_precheck": agent_hqs_precheck,
    "packet_builder": agent_packet_builder,
    "photo_rehab": agent_photo_rehab,
    "next_actions": agent_next_actions,
    "timeline_nudger": agent_timeline_nudger,
    "ops_judge": agent_ops_judge,
}


AGENT_SPECS: dict[str, dict[str, Any]] = {
    "deal_intake": {
        "agent_key": "deal_intake",
        "title": "Deal Intake",
        "description": "Validate required deal/property fields and produce next steps.",
        "category": "intake",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
    "underwrite": {
        "agent_key": "underwrite",
        "title": "Deal Underwrite",
        "description": "Compute DSCR/CoC assumptions and summarize underwriting risk.",
        "category": "deal",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "rent_reasonableness": {
        "agent_key": "rent_reasonableness",
        "title": "Rent Reasonableness",
        "description": "Compute rent reasonableness baseline using HUD FMR, caps, and comps context.",
        "category": "rent",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "hqs_precheck": {
        "agent_key": "hqs_precheck",
        "title": "HQS Precheck",
        "description": "Generate HQS readiness precheck and propose rehab tasks for likely failures.",
        "category": "compliance",
        "needs_human": True,
        "deterministic": True,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "packet_builder": {
        "agent_key": "packet_builder",
        "title": "Packet Builder",
        "description": "Generate packet scaffolding and missing-doc checklist.",
        "category": "packet",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "photo_rehab": {
        "agent_key": "photo_rehab",
        "title": "Rehab From Photos",
        "description": "Analyze property photos and propose rehab issues/tasks.",
        "category": "rehab",
        "needs_human": True,
        "deterministic": False,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "next_actions": {
        "agent_key": "next_actions",
        "title": "Next Actions Planner",
        "description": "Turn current property state and blockers into ranked next actions.",
        "category": "ops",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": True,
        "default_payload_schema": {"property_id": "number"},
    },
    "timeline_nudger": {
        "agent_key": "timeline_nudger",
        "title": "Timeline Nudger",
        "description": "Create workflow continuity nudges to prevent stalls.",
        "category": "ops",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
    "ops_judge": {
        "agent_key": "ops_judge",
        "title": "Ops Judge",
        "description": "Synthesize multiple agent outputs into a ranked plan and risk flags.",
        "category": "ops",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
}


@dataclass(frozen=True)
class SlotSpec:
    slot_key: str
    title: str
    description: str
    default_agent_key: str
    default_payload_schema: dict[str, Any]
    owner_type: str = "human"
    default_status: str = "idle"


SLOTS = [
    SlotSpec(
        slot_key="deal_intake",
        title="Deal Intake",
        description="Validate required deal/property fields and produce next steps.",
        default_agent_key="deal_intake",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="underwrite",
        title="Deal Underwrite",
        description="Compute DSCR/CoC assumptions and summarize underwriting risk.",
        default_agent_key="underwrite",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="rent_reasonableness",
        title="Rent Reasonableness",
        description="Compute rent reasonableness baseline using HUD FMR and comps context.",
        default_agent_key="rent_reasonableness",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="hqs_precheck",
        title="HQS Precheck",
        description="Generate HQS readiness precheck and propose rehab tasks for likely failures.",
        default_agent_key="hqs_precheck",
        default_payload_schema={"property_id": "number"},
        owner_type="human",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="packet_builder",
        title="Packet Builder",
        description="Generate jurisdiction packet checklist and missing artifact scaffold.",
        default_agent_key="packet_builder",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="photo_rehab",
        title="Rehab From Photos",
        description="Analyze property photos and propose rehab issues/tasks.",
        default_agent_key="photo_rehab",
        default_payload_schema={"property_id": "number"},
        owner_type="human",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="next_actions",
        title="Next Actions Planner",
        description="Turn state, blockers, and recent runs into ranked next actions.",
        default_agent_key="next_actions",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="timeline_nudger",
        title="Timeline Nudger",
        description="Create workflow continuity events to prevent stalls.",
        default_agent_key="timeline_nudger",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
    SlotSpec(
        slot_key="ops_judge",
        title="Ops Judge",
        description="Synthesize multiple agent outputs into a ranked plan and risk flags.",
        default_agent_key="ops_judge",
        default_payload_schema={"property_id": "number"},
        owner_type="agent",
        default_status="idle",
    ),
]
