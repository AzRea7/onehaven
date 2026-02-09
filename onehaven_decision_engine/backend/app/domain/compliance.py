from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..models import Property, Inspection, InspectionItem, Inspector


def top_fail_points(
    db: Session,
    city: str,
    state: str = "MI",
    inspector_id: Optional[int] = None,
    limit: int = 10,
) -> dict:
    # count inspections in window
    insp_q = select(func.count(Inspection.id)).join(Property, Property.id == Inspection.property_id).where(
        Property.city == city,
        Property.state == state,
    )
    if inspector_id is not None:
        insp_q = insp_q.where(Inspection.inspector_id == inspector_id)

    inspection_count = db.scalar(insp_q) or 0

    # count failed items by code
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
