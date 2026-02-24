# backend/app/domain/agents/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import Property, Deal, PropertyState
from app.policy_models import JurisdictionProfile
from app.services.hud_fmr_service import get_or_fetch_fmr
from app.domain.compliance.hqs_library import get_effective_hqs_items


def _property_context(db: Session, org_id: int, property_id: Optional[int]) -> dict[str, Any]:
    if not property_id:
        return {"property": None, "deal": None, "stage": None}

    p = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    d = db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(Deal.id.desc())
    )
    s = db.scalar(select(PropertyState).where(PropertyState.org_id == org_id, PropertyState.property_id == property_id))
    return {"property": p, "deal": d, "stage": (s.current_stage if s else None)}


def agent_deal_intake(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]
    d = ctx["deal"]

    missing = []
    if p is None:
        missing.append("property")
    else:
        if not getattr(p, "address", None):
            missing.append("property.address")
        if not getattr(p, "city", None):
            missing.append("property.city")
        if not getattr(p, "state", None):
            missing.append("property.state")
        if not getattr(p, "zip", None):
            missing.append("property.zip")
        if not getattr(p, "bedrooms", None):
            missing.append("property.bedrooms")

    if d is None:
        missing.append("deal")
    else:
        if getattr(d, "purchase_price", None) in (None, 0):
            missing.append("deal.purchase_price")

    # deal_intake contract is recommend_only => actions must be empty
    recommendations = []
    if missing:
        recommendations.append(
            {
                "type": "missing_fields",
                "property_id": property_id,
                "missing": missing,
                "reason": "Intake cannot complete until required fields exist.",
                "priority": "high",
            }
        )

    return {
        "agent_key": "deal_intake",
        "summary": "Deal intake validation + next required steps.",
        "facts": {"property_id": property_id, "missing": missing, "stage": ctx["stage"]},
        "actions": [],  # ✅ recommend_only
        "recommendations": recommendations,
    }


def agent_rent_reasonableness(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]
    if p is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],  # ✅ recommend_only
            "recommendations": [],
        }

    bedrooms = int(getattr(p, "bedrooms", 0) or 0)
    fmr = get_or_fetch_fmr(
        db,
        org_id=org_id,
        area_name=(p.city or "UNKNOWN"),
        state=(p.state or "MI"),
        bedrooms=bedrooms,
    )
    fmr_val = float(getattr(fmr, "fmr", 0.0) or 0.0)

    factors = ["location", "quality", "size", "unit_type", "age", "amenities", "services", "utilities"]
    recommended = fmr_val * 1.10 if fmr_val else None

    return {
        "agent_key": "rent_reasonableness",
        "summary": "Rent reasonableness baseline computed using HUD FMR cache anchor (deterministic).",
        "facts": {
            "property_id": property_id,
            "hud_fmr": {
                "area_name": getattr(fmr, "area_name", None),
                "state": getattr(fmr, "state", None),
                "bedrooms": getattr(fmr, "bedrooms", None),
                "fmr": fmr_val,
            },
            "required_comparability_factors": factors,
            "recommended_gross_rent": recommended,
        },
        "actions": [],  # ✅ recommend_only
        "recommendations": [
            {
                "type": "rent_reasonableness_computed",
                "property_id": property_id,
                "recommended_gross_rent": recommended,
                "factors": factors,
                "fmr": fmr_val,
                "reason": "Use as baseline; compare comps and utilities to justify contract rent.",
                "priority": "medium",
            }
        ],
    }


def agent_hqs_precheck(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]
    if p is None:
        return {
            "agent_key": "hqs_precheck",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
        }

    lib = get_effective_hqs_items(db, org_id=org_id, prop=p)
    items = lib.get("items") or []
    likely = [x for x in items if str(x.get("severity") or "").lower() == "fail"][:12]

    actions: list[dict[str, Any]] = []
    for it in likely[:6]:
        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(p.id),
                    "title": f"HQS precheck: {it.get('code')}",
                    "category": it.get("category") or "safety",
                    "status": "todo",
                    "cost_estimate": float(it.get("default_cost_estimate") or 0.0),
                    "notes": it.get("suggested_fix") or "",
                },
                "reason": "Convert likely HQS failures into rehab tasks (approval required).",
            }
        )

    # mutate_requires_approval contract requires non-empty actions[]
    if not actions:
        actions.append(
            {
                "entity_type": "workflow_event",
                "op": "create",
                "data": {
                    "property_id": int(p.id),
                    "event_type": "hqs_precheck_no_findings",
                    "payload": {"property_id": int(p.id)},
                },
                "reason": "No obvious HQS fail items found in baseline library.",
            }
        )

    return {
        "agent_key": "hqs_precheck",
        "summary": "HQS precheck generated from canonical HQS baseline; proposes rehab tasks for likely fails.",
        "facts": {
            "property_id": property_id,
            "hqs_items_total": len(items),
            "likely_fail_count": len(likely),
        },
        "actions": actions,  # ✅ contract compliant
    }


def agent_packet_builder(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    p = ctx["property"]
    if p is None:
        return {
            "agent_key": "packet_builder",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],  # ✅ recommend_only in your contracts
            "recommendations": [],
        }

    jp = db.scalar(
        select(JurisdictionProfile).where(
            JurisdictionProfile.org_id == org_id,
            JurisdictionProfile.state == p.state,
            JurisdictionProfile.city == p.city,
        )
    )

    checklist = None
    if jp is not None:
        checklist = getattr(jp, "packet_requirements_json", None)

    return {
        "agent_key": "packet_builder",
        "summary": "Builds a jurisdiction-specific Section 8 packet checklist (RFTA/HAP onboarding artifacts).",
        "facts": {
            "property_id": property_id,
            "jurisdiction_profile_found": jp is not None,
            "packet_checklist": checklist,
        },
        "actions": [],  # ✅ recommend_only
        "recommendations": [
            {
                "type": "packet_checklist_generated",
                "property_id": property_id,
                "packet_checklist": checklist or [],
                "reason": "Use as your completion checklist for RFTA/HAP onboarding artifacts.",
                "priority": "medium",
            }
        ],
    }


def agent_timeline_nudger(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    # recommend_only contract => must not emit actions[]
    return {
        "agent_key": "timeline_nudger",
        "summary": "Converts outstanding constraints into reminders and nudges (workflow continuity).",
        "facts": {"property_id": property_id},
        "actions": [],  # ✅ contract-compliant
        "recommendations": [
            {
                "type": "timeline_nudge",
                "property_id": property_id,
                "reason": "Keeps ops loop moving (no silent stalls).",
                "priority": "medium",
            }
        ],
    }


# ✅ Execution mapping (used by executor)
AGENTS: dict[str, Any] = {
    "deal_intake": agent_deal_intake,
    "rent_reasonableness": agent_rent_reasonableness,
    "hqs_precheck": agent_hqs_precheck,
    "packet_builder": agent_packet_builder,
    "timeline_nudger": agent_timeline_nudger,
}


# ✅ UI-safe metadata (used by /agents and /agents/registry)
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
    "rent_reasonableness": {
        "agent_key": "rent_reasonableness",
        "title": "Rent Reasonableness",
        "description": "Compute rent reasonableness baseline using HUD FMR cache + factors checklist.",
        "category": "rent",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
    "hqs_precheck": {
        "agent_key": "hqs_precheck",
        "title": "HQS Precheck",
        "description": "Generate HQS readiness precheck and propose rehab tasks for likely failures.",
        "category": "compliance",
        "needs_human": True,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
    "packet_builder": {
        "agent_key": "packet_builder",
        "title": "Packet Builder",
        "description": "Generate jurisdiction profile packet checklist (RFTA/HAP onboarding).",
        "category": "packet",
        "needs_human": True,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
    "timeline_nudger": {
        "agent_key": "timeline_nudger",
        "title": "Timeline Nudger",
        "description": "Create workflow continuity events to prevent stalls.",
        "category": "ops",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "default_payload_schema": {"property_id": "number"},
    },
}


# -------------------------------------------------------------------
# Slot specs: what the UI shows + what can be assigned to humans/agents
# -------------------------------------------------------------------

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
        slot_key="rent_reasonableness",
        title="Rent Reasonableness",
        description="Compute rent reasonableness baseline from HUD FMR cache + factors checklist.",
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
        description="Generate jurisdiction profile packet checklist (RFTA/HAP onboarding).",
        default_agent_key="packet_builder",
        default_payload_schema={"property_id": "number"},
        owner_type="human",
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
]
