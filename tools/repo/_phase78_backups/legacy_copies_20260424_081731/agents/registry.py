# backend/app/domain/agents/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Deal, Property, PropertyState
from app.policy_models import JurisdictionProfile
from products.intelligence.backend.src.services.hud_fmr_service import get_or_fetch_fmr
from products.compliance.backend.src.domain.compliance.hqs_library import get_effective_hqs_items

# Optional specialist imports.
# We keep these defensive so the registry does not hard-crash if one agent module
# is still being built out.
try:
    from onehaven_platform.backend.src.domain.agents.impl.underwrite_agent import run_underwrite_agent
except Exception:  # pragma: no cover
    run_underwrite_agent = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.rent_reasonableness_agent import run_rent_reasonableness_agent
except Exception:  # pragma: no cover
    run_rent_reasonableness_agent = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.hqs_precheck_agent import run_hqs_precheck_agent
except Exception:  # pragma: no cover
    run_hqs_precheck_agent = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.packet_builder_agent import run_packet_builder_agent
except Exception:  # pragma: no cover
    run_packet_builder_agent = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.photo_rehab_agent import run_photo_rehab_agent
except Exception:  # pragma: no cover
    run_photo_rehab_agent = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.next_actions_agent import run_next_actions_agent
except Exception:  # pragma: no cover
    run_next_actions_agent = None  # type: ignore

# Existing useful deterministic agents.
try:
    from onehaven_platform.backend.src.domain.agents.impl.deal_intake import run_deal_intake
except Exception:  # pragma: no cover
    run_deal_intake = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.ops_judge import run_ops_judge
except Exception:  # pragma: no cover
    run_ops_judge = None  # type: ignore

try:
    from onehaven_platform.backend.src.domain.agents.impl.timeline_nudger import run_timeline_nudger
except Exception:  # pragma: no cover
    run_timeline_nudger = None  # type: ignore

# Optional trust recompute seam. This lets trust behave like a deterministic agent.
try:
    from products.compliance.backend.src.services.trust_service import recompute_and_persist as trust_recompute_and_persist
except Exception:  # pragma: no cover
    trust_recompute_and_persist = None  # type: ignore


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

    jurisdiction = None
    if prop is not None:
        try:
            jurisdiction = db.scalar(
                select(JurisdictionProfile).where(
                    JurisdictionProfile.org_id == int(org_id),
                    JurisdictionProfile.state == getattr(prop, "state", None),
                    JurisdictionProfile.city == getattr(prop, "city", None),
                )
            )
        except Exception:
            jurisdiction = None

    return {
        "property": prop,
        "deal": deal,
        "stage": getattr(state, "current_stage", None) if state else None,
        "jurisdiction_profile": jurisdiction,
    }


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v or 0)
    except Exception:
        return default


def _has_property_photos(prop: Any) -> bool:
    if prop is None:
        return False
    for field in (
        "photos_json",
        "image_urls_json",
        "zillow_photos_json",
        "listing_photos_json",
        "photo_urls_json",
    ):
        val = getattr(prop, field, None)
        if isinstance(val, list) and len(val) > 0:
            return True
        if isinstance(val, str) and val.strip():
            return True
    return False


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
    else:
        recommendations.append(
            {
                "type": "intake_ready",
                "property_id": property_id,
                "reason": "Core property and deal fields are present.",
                "priority": "medium",
            }
        )

    return {
        "agent_key": "deal_intake",
        "summary": "Deal intake validation and next required steps.",
        "facts": {
            "property_id": property_id,
            "missing": missing,
            "stage": ctx["stage"],
        },
        "actions": [],
        "recommendations": recommendations,
    }


def _fallback_underwrite(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    prop = ctx["property"]
    deal = ctx["deal"]

    if prop is None or deal is None:
        return {
            "agent_key": "underwrite",
            "summary": "Underwrite unavailable because property or deal record is missing.",
            "facts": {
                "property_id": property_id,
                "property_found": prop is not None,
                "deal_found": deal is not None,
            },
            "actions": [],
            "recommendations": [
                {
                    "type": "missing_underwriting_inputs",
                    "property_id": property_id,
                    "reason": "A property and deal record are both required before underwriting can be computed.",
                    "priority": "high",
                }
            ],
        }

    purchase_price = _safe_float(getattr(deal, "purchase_price", None))
    rehab_budget = _safe_float(
        getattr(deal, "rehab_budget", None)
        or getattr(deal, "estimated_rehab_cost", None)
        or input_payload.get("rehab_budget")
    )
    monthly_rent = _safe_float(
        getattr(deal, "monthly_rent", None)
        or getattr(deal, "target_rent", None)
        or getattr(deal, "contract_rent", None)
        or input_payload.get("monthly_rent")
    )
    taxes = _safe_float(getattr(deal, "taxes_monthly", None) or input_payload.get("taxes_monthly"))
    insurance = _safe_float(getattr(deal, "insurance_monthly", None) or input_payload.get("insurance_monthly"))
    maintenance = _safe_float(input_payload.get("maintenance_monthly"))
    management = _safe_float(input_payload.get("management_monthly"))
    vacancy = _safe_float(input_payload.get("vacancy_monthly"))

    total_monthly_expenses = taxes + insurance + maintenance + management + vacancy
    noi = max(0.0, monthly_rent - total_monthly_expenses)
    total_basis = purchase_price + rehab_budget

    coc = (noi * 12.0 / total_basis) if total_basis > 0 else None

    recommendations: list[dict[str, Any]] = []
    if monthly_rent <= 0:
        recommendations.append(
            {
                "type": "missing_rent_input",
                "property_id": property_id,
                "reason": "No monthly rent value is available, so underwriting is only partial.",
                "priority": "high",
            }
        )
    if total_basis <= 0:
        recommendations.append(
            {
                "type": "missing_basis_input",
                "property_id": property_id,
                "reason": "Purchase price and rehab basis are missing or zero.",
                "priority": "high",
            }
        )
    if coc is not None:
        recommendations.append(
            {
                "type": "underwrite_snapshot",
                "property_id": property_id,
                "cash_on_cash_estimate": coc,
                "reason": "Use as a deterministic baseline before LLM explanation/sensitivity layers.",
                "priority": "medium",
            }
        )

    return {
        "agent_key": "underwrite",
        "summary": "Deterministic underwriting baseline computed from deal and rent inputs.",
        "facts": {
            "property_id": property_id,
            "purchase_price": purchase_price,
            "rehab_budget": rehab_budget,
            "monthly_rent": monthly_rent,
            "monthly_expenses": total_monthly_expenses,
            "noi_monthly": noi,
            "cash_on_cash_estimate": coc,
            "stage": ctx["stage"],
        },
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

    bedrooms = _safe_int(getattr(prop, "bedrooms", 0))
    fmr = get_or_fetch_fmr(
        db,
        org_id=int(org_id),
        area_name=(getattr(prop, "city", None) or "UNKNOWN"),
        state=(getattr(prop, "state", None) or "MI"),
        bedrooms=bedrooms,
    )
    fmr_val = _safe_float(getattr(fmr, "fmr", 0.0))
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
    recommended = round(fmr_val * 1.10, 2) if fmr_val else None

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
                "reason": "Use this baseline and justify the final contract rent with comps and utilities.",
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
                    "cost_estimate": _safe_float(item.get("default_cost_estimate")),
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
    jurisdiction = ctx["jurisdiction_profile"]

    checklist = None
    if jurisdiction is not None:
        checklist = (
            getattr(jurisdiction, "packet_requirements_json", None)
            or getattr(jurisdiction, "workflow_steps_json", None)
            or []
        )

    return {
        "agent_key": "packet_builder",
        "summary": "Builds a jurisdiction-specific Section 8 packet checklist.",
        "facts": {
            "property_id": property_id,
            "jurisdiction_profile_found": jurisdiction is not None,
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


def _fallback_photo_rehab(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)
    prop = ctx["property"]

    has_photos = _has_property_photos(prop)
    actions: list[dict[str, Any]] = []
    recommendations: list[dict[str, Any]] = []

    if has_photos:
        recommendations.append(
            {
                "type": "photo_rehab_ready",
                "property_id": property_id,
                "reason": "Property appears to have photos available; vision pass can be run.",
                "priority": "medium",
            }
        )
    else:
        recommendations.append(
            {
                "type": "missing_property_photos",
                "property_id": property_id,
                "reason": "Photo-based rehab analysis cannot run until listing/property photos are attached.",
                "priority": "high",
            }
        )

    return {
        "agent_key": "photo_rehab",
        "summary": "Photo rehab readiness check completed.",
        "facts": {
            "property_id": property_id,
            "photos_available": has_photos,
        },
        "actions": actions,
        "recommendations": recommendations,
    }


def _fallback_next_actions(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    ctx = _property_context(db, org_id, property_id)

    recs: list[dict[str, Any]] = [
        {
            "type": "review_state",
            "property_id": property_id,
            "reason": "Review outstanding blockers and the latest specialist outputs.",
            "priority": "medium",
        }
    ]

    if ctx["deal"] is None:
        recs.insert(
            0,
            {
                "type": "create_deal_record",
                "property_id": property_id,
                "reason": "A deal record is needed before underwriting and downstream decision steps.",
                "priority": "high",
            },
        )

    if ctx["jurisdiction_profile"] is None:
        recs.append(
            {
                "type": "missing_jurisdiction_profile",
                "property_id": property_id,
                "reason": "Packet/compliance quality improves once jurisdiction profile data exists.",
                "priority": "medium",
            }
        )

    return {
        "agent_key": "next_actions",
        "summary": "Next actions synthesized from current property context.",
        "facts": {
            "property_id": property_id,
            "stage": ctx["stage"],
            "deal_found": ctx["deal"] is not None,
            "jurisdiction_profile_found": ctx["jurisdiction_profile"] is not None,
        },
        "actions": [],
        "recommendations": recs,
    }


def _fallback_timeline_nudger(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "agent_key": "timeline_nudger",
        "summary": "Workflow continuity nudge generated.",
        "facts": {"property_id": property_id},
        "actions": [],
        "recommendations": [
            {
                "type": "timeline_nudge",
                "property_id": property_id,
                "reason": "Keep the property workflow moving and avoid silent stalls.",
                "priority": "medium",
            }
        ],
    }


def _fallback_ops_judge(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "agent_key": "ops_judge",
        "summary": "Ops judge synthesized current backend state into a ranked review recommendation.",
        "facts": {"property_id": property_id},
        "actions": [],
        "recommendations": [
            {
                "type": "review_specialist_outputs",
                "property_id": property_id,
                "reason": "Review the latest runs and clear the highest-impact blockers first.",
                "priority": "medium",
            }
        ],
    }


def _fallback_trust_recompute(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "trust_recompute",
            "summary": "Trust recompute skipped because no property_id was supplied.",
            "facts": {"property_id": property_id, "recomputed": False},
            "actions": [],
            "recommendations": [],
        }

    recomputed = False
    error = None
    if trust_recompute_and_persist is not None:
        try:
            try:
                trust_recompute_and_persist(db, int(org_id), "property", str(property_id))
            except TypeError:
                trust_recompute_and_persist(
                    db,
                    org_id=int(org_id),
                    entity_type="property",
                    entity_id=str(property_id),
                )
            recomputed = True
        except Exception as exc:  # pragma: no cover
            error = str(exc)

    recs: list[dict[str, Any]] = []
    if recomputed:
        recs.append(
            {
                "type": "trust_recomputed",
                "property_id": property_id,
                "reason": "Property trust score was recomputed successfully.",
                "priority": "low",
            }
        )
    elif error:
        recs.append(
            {
                "type": "trust_recompute_failed",
                "property_id": property_id,
                "reason": error,
                "priority": "high",
            }
        )

    return {
        "agent_key": "trust_recompute",
        "summary": "Deterministic trust recompute completed.",
        "facts": {
            "property_id": property_id,
            "recomputed": recomputed,
            "error": error,
        },
        "actions": [],
        "recommendations": recs,
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
    return _call_impl_or_fallback(
        run_underwrite_agent if callable(run_underwrite_agent) else None,
        _fallback_underwrite,
        db,
        org_id,
        property_id,
        input_payload,
    )


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
    return _call_impl_or_fallback(
        run_photo_rehab_agent if callable(run_photo_rehab_agent) else None,
        _fallback_photo_rehab,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_next_actions(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_next_actions_agent if callable(run_next_actions_agent) else None,
        _fallback_next_actions,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_timeline_nudger(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_timeline_nudger if callable(run_timeline_nudger) else None,
        _fallback_timeline_nudger,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_ops_judge(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _call_impl_or_fallback(
        run_ops_judge if callable(run_ops_judge) else None,
        _fallback_ops_judge,
        db,
        org_id,
        property_id,
        input_payload,
    )


def agent_trust_recompute(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    return _fallback_trust_recompute(db, org_id, property_id, input_payload)


AGENTS: dict[str, AgentFn] = {
    "deal_intake": agent_deal_intake,
    "underwrite": agent_underwrite,
    "deal_underwrite": agent_underwrite,   # alias for business-facing naming
    "rent_reasonableness": agent_rent_reasonableness,
    "hqs_precheck": agent_hqs_precheck,
    "packet_builder": agent_packet_builder,
    "photo_rehab": agent_photo_rehab,
    "rehab_from_photos": agent_photo_rehab,  # alias
    "next_actions": agent_next_actions,
    "next_actions_planner": agent_next_actions,  # alias
    "timeline_nudger": agent_timeline_nudger,
    "ops_judge": agent_ops_judge,
    "trust_recompute": agent_trust_recompute,
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
        "canonical_key": "deal_intake",
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
        "canonical_key": "underwrite",
        "aliases": ["deal_underwrite"],
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
        "canonical_key": "rent_reasonableness",
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
        "canonical_key": "hqs_precheck",
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
        "canonical_key": "packet_builder",
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
        "canonical_key": "photo_rehab",
        "aliases": ["rehab_from_photos"],
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
        "canonical_key": "next_actions",
        "aliases": ["next_actions_planner"],
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
        "canonical_key": "timeline_nudger",
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
        "canonical_key": "ops_judge",
        "default_payload_schema": {"property_id": "number"},
    },
    "trust_recompute": {
        "agent_key": "trust_recompute",
        "title": "Trust Recompute",
        "description": "Deterministically recompute trust for the property and emit audit-friendly result facts.",
        "category": "ops",
        "needs_human": False,
        "deterministic": True,
        "llm_capable": False,
        "canonical_key": "trust_recompute",
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
        slot_key="trust_recompute",
        title="Trust Recompute",
        description="Deterministically recompute trust after specialist runs or manual overrides.",
        default_agent_key="trust_recompute",
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
