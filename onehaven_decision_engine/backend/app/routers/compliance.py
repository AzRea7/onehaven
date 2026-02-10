from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Property
from ..schemas import ChecklistOut, ChecklistItemOut

router = APIRouter(prefix="/compliance", tags=["compliance"])


# Keep it in code first. Promote to DB later.
_SECTION8_TEMPLATE: list[dict] = [
    {"category": "Electrical", "item_code": "GFCI", "description": "GFCI protection near sinks / wet areas", "severity": 3, "common_fail": True},
    {"category": "Electrical", "item_code": "OUTLET_COVERS", "description": "Missing/broken outlet/switch covers", "severity": 2, "common_fail": True},
    {"category": "Safety", "item_code": "SMOKE_CO_DETECTORS", "description": "Smoke/CO detectors installed and working", "severity": 4, "common_fail": True},
    {"category": "Safety", "item_code": "HANDRAILS", "description": "Handrails on stairs / steps where required", "severity": 3, "common_fail": True},
    {"category": "Exterior", "item_code": "BROKEN_WINDOWS", "description": "No broken/cracked windows; lockable and weather-tight", "severity": 3, "common_fail": True},
    {"category": "Interior", "item_code": "TRIP_HAZARDS", "description": "No trip hazards (loose flooring, torn carpet, bad transitions)", "severity": 3, "common_fail": True},
    {"category": "Plumbing", "item_code": "LEAKS", "description": "No active plumbing leaks; fixtures secure", "severity": 3, "common_fail": True},
    {"category": "HVAC", "item_code": "HEAT_WORKS", "description": "Permanent heat source operational", "severity": 4, "common_fail": True},
    # Conditionally applied
    {"category": "Lead Paint", "item_code": "LEAD_PAINT_FLAGS", "description": "Potential lead paint hazards (pre-1978): peeling/chipping paint", "severity": 5, "common_fail": True, "applies_if": {"year_built_lt": 1978}},
    {"category": "Garage", "item_code": "GARAGE_DOOR_SAFE", "description": "Garage door operates safely; no unsafe springs/rails", "severity": 2, "common_fail": False, "applies_if": {"has_garage": True}},
]


def _applies(item: dict, *, year_built: int | None, has_garage: bool) -> bool:
    cond = item.get("applies_if")
    if not cond:
        return True

    if "year_built_lt" in cond:
        y = year_built if year_built is not None else 9999
        if not (y < int(cond["year_built_lt"])):
            return False

    if "has_garage" in cond:
        if bool(cond["has_garage"]) != bool(has_garage):
            return False

    return True


@router.post("/checklist/{property_id}", response_model=ChecklistOut)
def generate_checklist(
    property_id: int,
    strategy: str = Query(default="section8"),
    db: Session = Depends(get_db),
):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    items: list[ChecklistItemOut] = []
    for raw in _SECTION8_TEMPLATE:
        if not _applies(raw, year_built=prop.year_built, has_garage=prop.has_garage):
            continue
        items.append(
            ChecklistItemOut(
                category=raw["category"],
                item_code=raw["item_code"],
                description=raw["description"],
                severity=int(raw.get("severity", 1)),
                common_fail=bool(raw.get("common_fail", True)),
                applies_if=raw.get("applies_if"),
            )
        )

    return ChecklistOut(
        property_id=property_id,
        strategy=strategy,
        city=prop.city,
        state=prop.state,
        items=items,
    )
