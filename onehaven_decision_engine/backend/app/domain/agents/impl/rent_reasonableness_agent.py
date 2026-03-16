from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.services.hud_fmr_service import get_or_fetch_fmr


REQUIRED_FACTORS = [
    "location",
    "quality",
    "size",
    "bedroom_count",
    "utilities",
    "condition",
    "amenities",
    "unit_type",
]


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    prop = None
    if property_id is not None:
        prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))

    if prop is None:
        return {
            "agent_key": "rent_reasonableness",
            "summary": "No property found for rent reasonableness.",
            "facts": {"property_id": property_id},
            "recommendations": [],
        }

    bedrooms = int(getattr(prop, "bedrooms", 0) or 0)
    fmr_row = get_or_fetch_fmr(
        db,
        org_id=int(org_id),
        area_name=str(getattr(prop, "city", None) or "UNKNOWN"),
        state=str(getattr(prop, "state", None) or "MI"),
        bedrooms=bedrooms,
    )
    fmr = float(getattr(fmr_row, "fmr", 0.0) or 0.0)
    suggested_rent = round(fmr * 1.1, 2) if fmr else None

    recs = [
        {
            "type": "rent_reasonableness_band",
            "title": "Use a supported rent band",
            "reason": "Anchor proposed rent to HUD FMR baseline, then justify variance with local comps and utility responsibility.",
            "priority": "high",
        },
        {
            "type": "rent_reasonableness_documentation",
            "title": "Document comparable factors",
            "reason": "Keep a plain-English explanation for each required factor so packet review is not a bureaucratic ambush.",
            "priority": "medium",
        },
    ]

    return {
        "agent_key": "rent_reasonableness",
        "summary": "Rent reasonableness baseline computed from HUD FMR anchor and required comparability factors.",
        "facts": {
            "property_id": property_id,
            "bedrooms": bedrooms,
            "city": getattr(prop, "city", None),
            "state": getattr(prop, "state", None),
            "hud_fmr": round(fmr, 2),
            "recommended_gross_rent": suggested_rent,
            "required_factors": REQUIRED_FACTORS,
        },
        "recommendations": recs,
    }
