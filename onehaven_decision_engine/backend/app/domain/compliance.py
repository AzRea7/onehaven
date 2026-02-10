from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..models import Property, Inspection, InspectionItem, Inspector


# -------------------- Analytics --------------------

def top_fail_points(
    db: Session,
    city: str,
    state: str = "MI",
    inspector_id: Optional[int] = None,
    limit: int = 10,
) -> dict:
    insp_q = select(func.count(Inspection.id)).join(Property, Property.id == Inspection.property_id).where(
        Property.city == city,
        Property.state == state,
    )
    if inspector_id is not None:
        insp_q = insp_q.where(Inspection.inspector_id == inspector_id)

    inspection_count = db.scalar(insp_q) or 0

    item_q = (
        select(InspectionItem.code, func.count(InspectionItem.id).label("cnt"))
        .join(Inspection, Inspection.id == InspectionItem.inspection_id)
        .join(Property, Property.id == Inspection.property_id)
        .where(
            Property.city == city,
            Property.state == state,
            InspectionItem.failed == True,  # noqa: E712
        )
        .group_by(InspectionItem.code)
        .order_by(func.count(InspectionItem.id).desc())
        .limit(limit)
    )
    if inspector_id is not None:
        item_q = item_q.where(Inspection.inspector_id == inspector_id)

    rows = db.execute(item_q).all()

    top = []
    for code, cnt in rows:
        rate = (cnt / inspection_count) if inspection_count > 0 else 0.0
        top.append({"code": code, "count": int(cnt), "rate": round(rate, 3)})

    return {"inspection_count": int(inspection_count), "top": top}


def compliance_stats(db: Session, city: str, state: str = "MI", limit: int = 10) -> dict:
    total = db.scalar(
        select(func.count(Inspection.id))
        .join(Property, Property.id == Inspection.property_id)
        .where(Property.city == city, Property.state == state)
    ) or 0

    passed = db.scalar(
        select(func.count(Inspection.id))
        .join(Property, Property.id == Inspection.property_id)
        .where(Property.city == city, Property.state == state, Inspection.passed == True)  # noqa: E712
    ) or 0

    reinspect = db.scalar(
        select(func.count(Inspection.id))
        .join(Property, Property.id == Inspection.property_id)
        .where(Property.city == city, Property.state == state, Inspection.reinspect_required == True)  # noqa: E712
    ) or 0

    tfp = top_fail_points(db, city=city, state=state, inspector_id=None, limit=limit)

    pass_rate = (passed / total) if total > 0 else 0.0
    reinspect_rate = (reinspect / total) if total > 0 else 0.0

    return {
        "inspections": int(total),
        "pass_rate": round(pass_rate, 3),
        "reinspect_rate": round(reinspect_rate, 3),
        "top_fail_points": tfp["top"],
    }


# -------------------- Phase 3: Checklist Generation --------------------

@dataclass(frozen=True)
class ChecklistItem:
    item_code: str
    category: str
    description: str
    severity: int = 2  # 1..5
    common_fail: bool = False
    applies_if: Optional[str] = None


def generate_hqs_checklist_for_property(prop: Property) -> list[ChecklistItem]:
    """
    Minimal, code-first HQS-ish checklist generator.
    Deterministic rules, property-tailored.
    (You can keep expanding this file as your rules grow.)
    """
    items: list[ChecklistItem] = []

    year = prop.year_built or None

    # ---- Electrical / safety ----
    items.append(ChecklistItem(
        item_code="SMOKE_CO",
        category="safety",
        description="Smoke detectors present and working; CO detectors where required.",
        severity=5,
        common_fail=True,
    ))
    items.append(ChecklistItem(
        item_code="GFCI",
        category="electrical",
        description="GFCI protection at kitchen/bath/outlets near water (as required).",
        severity=4,
        common_fail=True,
    ))
    items.append(ChecklistItem(
        item_code="OUTLETS_SWITCHES",
        category="electrical",
        description="No missing/broken cover plates; outlets/switches functional; no exposed wiring.",
        severity=4,
        common_fail=True,
    ))

    # ---- Stairs / railings ----
    items.append(ChecklistItem(
        item_code="HANDRAIL",
        category="structure",
        description="Handrails secure on stairs; guardrails where needed.",
        severity=4,
        common_fail=True,
    ))

    # ---- Plumbing / water ----
    items.append(ChecklistItem(
        item_code="HOT_WATER",
        category="plumbing",
        description="Hot water available; water heater venting and relief valve/pipe present.",
        severity=4,
        common_fail=False,
    ))
    items.append(ChecklistItem(
        item_code="LEAKS",
        category="plumbing",
        description="No active leaks at fixtures, supply lines, drains; no mold-inducing moisture.",
        severity=4,
        common_fail=True,
    ))

    # ---- Windows / doors ----
    items.append(ChecklistItem(
        item_code="WINDOWS",
        category="envelope",
        description="Windows open/close/lock; no broken panes; egress where required.",
        severity=3,
        common_fail=True,
    ))
    items.append(ChecklistItem(
        item_code="DOORS_LOCKS",
        category="envelope",
        description="Exterior doors secure, weather-tight, functional locks.",
        severity=4,
        common_fail=True,
    ))

    # ---- Floors / trip hazards ----
    items.append(ChecklistItem(
        item_code="TRIP_HAZARDS",
        category="interior",
        description="No major trip hazards: loose flooring, torn carpet, uneven transitions.",
        severity=3,
        common_fail=True,
    ))

    # ---- Heating ----
    items.append(ChecklistItem(
        item_code="HEAT",
        category="mechanical",
        description="Permanent heat source functional; maintains safe indoor temperature.",
        severity=5,
        common_fail=False,
    ))

    # ---- Pre-1978 paint rule ----
    if year is not None and year < 1978:
        items.append(ChecklistItem(
            item_code="LEAD_PAINT",
            category="health",
            description="Pre-1978: no deteriorated paint; stabilize/encapsulate; follow lead-safe practices.",
            severity=5,
            common_fail=True,
            applies_if="year_built < 1978",
        ))

    # ---- Garage-specific (only if garage exists) ----
    if getattr(prop, "has_garage", False):
        items.append(ChecklistItem(
            item_code="GARAGE_SAFETY",
            category="safety",
            description="Garage: no exposed wiring, proper door operation, safe storage (no hazards).",
            severity=3,
            common_fail=False,
            applies_if="has_garage == True",
        ))

    return items
