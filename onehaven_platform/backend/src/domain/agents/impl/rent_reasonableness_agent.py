# backend/app/domain/agents/impl/rent_reasonableness_agent.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.domain.agents.llm_router import run_llm_agent
from onehaven_platform.backend.src.models import Deal, Property
from onehaven_platform.backend.src.adapters.intelligence_adapter import get_or_fetch_fmr

try:
    from onehaven_platform.backend.src.adapters.intelligence_adapter import recompute_rent_fields  # type: ignore
except Exception:  # pragma: no cover
    recompute_rent_fields = None  # type: ignore


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v if v is not None else default)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v if v is not None else default)
    except Exception:
        return default


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
        .order_by(Deal.id.desc())
    )


def run_rent_reasonableness_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "Rent reasonableness skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before rent reasonableness can run.",
                    "priority": "high",
                }
            ],
            "actions": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    deal = _latest_deal(db, org_id=int(org_id), property_id=int(property_id))

    if prop is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "recommendations": [],
            "actions": [],
        }

    bedrooms = _safe_int(getattr(prop, "bedrooms", None))
    city = getattr(prop, "city", None) or "UNKNOWN"
    state = getattr(prop, "state", None) or "MI"
    strategy = str(input_payload.get("strategy") or getattr(deal, "strategy", None) or "section8").strip().lower()

    fmr = get_or_fetch_fmr(
        db,
        org_id=int(org_id),
        area_name=str(city),
        state=str(state),
        bedrooms=bedrooms,
    )
    fmr_val = _safe_float(getattr(fmr, "fmr", None))
    payment_standard_pct = _safe_float(
        input_payload.get("payment_standard_pct"),
        float(getattr(settings, "default_payment_standard_pct", 1.10)),
    )

    computed = None
    if recompute_rent_fields is not None:
        try:
            computed = recompute_rent_fields(
                db,
                property_id=int(property_id),
                strategy=strategy,
                payment_standard_pct=payment_standard_pct,
            )
        except Exception:
            computed = None

    approved_rent_ceiling = None
    calibrated_market_rent = None
    rent_used = None
    multiplier = None

    if isinstance(computed, dict):
        approved_rent_ceiling = computed.get("approved_rent_ceiling")
        calibrated_market_rent = computed.get("calibrated_market_rent")
        rent_used = computed.get("rent_used")
        multiplier = computed.get("multiplier")

    recommended_gross_rent = None
    candidates: list[float] = []
    for val in (
        _safe_float(rent_used, 0.0),
        _safe_float(approved_rent_ceiling, 0.0),
        _safe_float(calibrated_market_rent, 0.0),
        round(fmr_val * payment_standard_pct, 2) if fmr_val > 0 else 0.0,
    ):
        if val > 0:
            candidates.append(val)
    if candidates:
        recommended_gross_rent = min(candidates) if strategy == "section8" else max(candidates)

    facts = {
        "property_id": int(property_id),
        "address": getattr(prop, "address", None),
        "strategy": strategy,
        "bedrooms": bedrooms,
        "hud_fmr": {
            "area_name": getattr(fmr, "area_name", None),
            "state": getattr(fmr, "state", None),
            "bedrooms": getattr(fmr, "bedrooms", None),
            "fmr": fmr_val,
        },
        "payment_standard_pct": payment_standard_pct,
        "approved_rent_ceiling": approved_rent_ceiling,
        "calibrated_market_rent": calibrated_market_rent,
        "rent_used": rent_used,
        "multiplier": multiplier,
        "recommended_gross_rent": recommended_gross_rent,
        "required_comparability_factors": [
            "location",
            "quality",
            "size",
            "unit_type",
            "age",
            "amenities",
            "services",
            "utilities",
        ],
    }

    deterministic = {
        "agent_key": "rent_reasonableness",
        "summary": "Rent reasonableness baseline computed from HUD FMR and calibrated rent inputs.",
        "facts": facts,
        "recommendations": [
            {
                "type": "rent_reasonableness_computed",
                "title": "Rent baseline computed",
                "reason": "Use this baseline and comparable-unit explanation when setting the final asking/contract rent.",
                "priority": "medium",
            }
        ],
        "actions": [],
        "confidence": 0.88,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="rent_reasonableness",
            context={"deterministic_baseline": deterministic, "input_payload": input_payload},
            mode="hybrid",
        )
        llm_output["facts"] = {**facts, **(llm_output.get("facts") or {})}
        llm_output["agent_key"] = "rent_reasonableness"
        llm_output["actions"] = []
        return llm_output
    except Exception:
        return deterministic
    