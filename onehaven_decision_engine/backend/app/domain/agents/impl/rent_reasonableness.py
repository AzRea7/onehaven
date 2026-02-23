# onehaven_decision_engine/backend/app/domain/agents/impl/rent_reasonableness.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property, RentAssumption
from app.policy_models import HudFmrRecord, RentComp
from app.services.hud_fmr_service import get_or_fetch_fmr
from app.services.rent_comp_selection import select_best_comps


_COMPARABILITY_FACTORS = [
    "location",
    "quality",
    "size",
    "unit_type",
    "age",
    "amenities",
    "services",
    "utilities",
]


def run_rent_reasonableness(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic v1 that is actually operational:
    - anchors to HUD FMR cache (and fetches if missing)
    - selects internal comps via deterministic similarity scoring
    - outputs recommended range + justification checklist
    """
    if not property_id:
        return {"summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.org_id == org_id, RentAssumption.property_id == property_id)
        .order_by(RentAssumption.id.desc())
    )
    target = float(getattr(ra, "target_rent", 0.0) or 0.0)

    bedrooms = int(prop.bedrooms or 0)
    fmr = get_or_fetch_fmr(
        db,
        org_id=org_id,
        area_name=(prop.city or "UNKNOWN"),
        state=(prop.state or "MI"),
        bedrooms=bedrooms,
    )

    # Select comps (internal DB)
    comps = select_best_comps(db, org_id=org_id, prop=prop, limit=5)

    comp_rents = [float(c.rent or 0.0) for c in comps if float(c.rent or 0.0) > 0]
    comp_median = sorted(comp_rents)[len(comp_rents) // 2] if comp_rents else 0.0

    # Deterministic range logic:
    # - If comps exist: center around median; clip by FMR ± 20% (soft bound)
    # - If comps missing: fall back to FMR band
    fmr_val = float(getattr(fmr, "fmr", 0.0) or 0.0)

    if comp_median > 0:
        low = max(0.0, min(comp_median * 0.92, fmr_val * 1.20 if fmr_val else comp_median * 0.92))
        high = max(low, max(comp_median * 1.08, fmr_val * 0.80 if fmr_val else comp_median * 1.08))
    else:
        low = fmr_val * 0.90 if fmr_val else 0.0
        high = fmr_val * 1.10 if fmr_val else 0.0

    rec = (low + high) / 2.0 if high > 0 else 0.0

    # actions: recommend setting rent, and if target is out of band, flag it
    actions: list[dict[str, Any]] = []
    actions.append(
        {
            "op": "recommend",
            "entity_type": "RentRecommendation",
            "entity_id": None,
            "payload": {
                "property_id": prop.id,
                "recommended_rent": round(rec, 2),
                "suggested_range": [round(low, 2), round(high, 2)],
                "fmr_anchor": round(fmr_val, 2),
                "comp_median": round(comp_median, 2),
                "factors_checked": list(_COMPARABILITY_FACTORS),
            },
            "priority": "high",
        }
    )

    if target > 0 and (target < low or target > high):
        actions.append(
            {
                "op": "recommend",
                "entity_type": "RentOutOfBandAlert",
                "entity_id": None,
                "payload": {
                    "property_id": prop.id,
                    "target_rent": round(target, 2),
                    "band": [round(low, 2), round(high, 2)],
                    "reason": "Target rent is outside deterministic reasonable band (comps + FMR anchor).",
                },
                "priority": "high",
            }
        )

    citations = [
        {
            "type": "hud_fmr_cache",
            "record": {
                "area_name": getattr(fmr, "area_name", None),
                "state": getattr(fmr, "state", None),
                "bedrooms": getattr(fmr, "bedrooms", None),
                "fmr": float(getattr(fmr, "fmr", 0.0) or 0.0),
                "effective_date": (getattr(fmr, "effective_date", None).isoformat() if getattr(fmr, "effective_date", None) else None),
                "source_urls": _safe_json(getattr(fmr, "source_urls_json", None)),
            },
        },
        {"type": "rent_comps_used", "comp_ids": [int(c.id) for c in comps]},
    ]

    return {
        "summary": f"Rent reasonableness computed using {len(comps)} comps and HUD FMR anchor.",
        "facts": {
            "property_id": prop.id,
            "bedrooms": bedrooms,
            "target_rent": round(target, 2),
            "fmr": round(fmr_val, 2),
            "comp_median": round(comp_median, 2),
            "band": [round(low, 2), round(high, 2)],
            "factors": list(_COMPARABILITY_FACTORS),
        },
        "actions": actions,
        "citations": citations,
    }


def _safe_json(s: Optional[str]):
    if not s:
        return []
    try:
        import json

        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []