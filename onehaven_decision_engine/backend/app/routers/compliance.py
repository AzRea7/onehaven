# backend/app/routers/compliance.py
from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from ..db import get_db
from ..models import Property, ChecklistTemplateItem, PropertyChecklist
from ..schemas import (
    ChecklistOut,
    ChecklistItemOut,
    ChecklistTemplateItemUpsert,
    ChecklistTemplateItemOut,
    PropertyChecklistOut,
)

router = APIRouter(prefix="/compliance", tags=["compliance"])


# ---- Keep your code template as a fallback (so you don't get blocked mid-migration) ----
_SECTION8_TEMPLATE: list[dict] = [
    {"category": "Electrical", "item_code": "GFCI", "description": "GFCI protection near sinks / wet areas", "severity": 3, "common_fail": True},
    {"category": "Electrical", "item_code": "OUTLET_COVERS", "description": "Missing/broken outlet/switch covers", "severity": 2, "common_fail": True},
    {"category": "Safety", "item_code": "SMOKE_CO_DETECTORS", "description": "Smoke/CO detectors installed and working", "severity": 4, "common_fail": True},
    {"category": "Safety", "item_code": "HANDRAILS", "description": "Handrails on stairs / steps where required", "severity": 3, "common_fail": True},
    {"category": "Exterior", "item_code": "BROKEN_WINDOWS", "description": "No broken/cracked windows; lockable and weather-tight", "severity": 3, "common_fail": True},
    {"category": "Interior", "item_code": "TRIP_HAZARDS", "description": "No trip hazards (loose flooring, torn carpet, bad transitions)", "severity": 3, "common_fail": True},
    {"category": "Plumbing", "item_code": "LEAKS", "description": "No active plumbing leaks; fixtures secure", "severity": 3, "common_fail": True},
    {"category": "HVAC", "item_code": "HEAT_WORKS", "description": "Permanent heat source operational", "severity": 4, "common_fail": True},
    {"category": "Lead Paint", "item_code": "LEAD_PAINT_FLAGS", "description": "Potential lead paint hazards (pre-1978): peeling/chipping paint", "severity": 5, "common_fail": True, "applies_if": {"year_built_lt": 1978}},
    {"category": "Garage", "item_code": "GARAGE_DOOR_SAFE", "description": "Garage door operates safely; no unsafe springs/rails", "severity": 2, "common_fail": False, "applies_if": {"has_garage": True}},
]


def _applies(cond: dict | None, *, year_built: int | None, has_garage: bool, property_type: str) -> bool:
    if not cond:
        return True

    if "year_built_lt" in cond:
        y = year_built if year_built is not None else 9999
        if not (y < int(cond["year_built_lt"])):
            return False

    if "has_garage" in cond:
        if bool(cond["has_garage"]) != bool(has_garage):
            return False

    if "property_type_in" in cond:
        allowed = cond.get("property_type_in") or []
        if isinstance(allowed, list) and property_type not in allowed:
            return False

    return True


@router.get("/templates", response_model=list[ChecklistTemplateItemOut])
def list_templates(
    strategy: str = Query(default="section8"),
    version: str = Query(default="v1"),
    db: Session = Depends(get_db),
):
    rows = db.scalars(
        select(ChecklistTemplateItem)
        .where(ChecklistTemplateItem.strategy == strategy, ChecklistTemplateItem.version == version)
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()
    return rows


@router.put("/templates", response_model=ChecklistTemplateItemOut)
def upsert_template(payload: ChecklistTemplateItemUpsert, db: Session = Depends(get_db)):
    row = db.scalar(
        select(ChecklistTemplateItem).where(
            ChecklistTemplateItem.strategy == payload.strategy,
            ChecklistTemplateItem.version == payload.version,
            ChecklistTemplateItem.code == payload.code,
        )
    )

    applies_if_json = json.dumps(payload.applies_if) if payload.applies_if else None

    if not row:
        row = ChecklistTemplateItem(
            strategy=payload.strategy,
            version=payload.version,
            code=payload.code,
            category=payload.category,
            description=payload.description,
            applies_if_json=applies_if_json,
            severity=int(payload.severity),
            common_fail=bool(payload.common_fail),
            created_at=datetime.utcnow(),
        )
        db.add(row)
    else:
        row.category = payload.category
        row.description = payload.description
        row.applies_if_json = applies_if_json
        row.severity = int(payload.severity)
        row.common_fail = bool(payload.common_fail)

    db.commit()
    db.refresh(row)
    return row


@router.post("/checklist/{property_id}", response_model=ChecklistOut)
def generate_checklist(
    property_id: int,
    strategy: str = Query(default="section8"),
    version: str = Query(default="v1"),
    persist: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    # 1) Try DB templates first
    tmpl = db.scalars(
        select(ChecklistTemplateItem)
        .where(ChecklistTemplateItem.strategy == strategy, ChecklistTemplateItem.version == version)
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()

    items: list[ChecklistItemOut] = []

    if tmpl:
        for t in tmpl:
            cond = None
            if t.applies_if_json:
                try:
                    cond = json.loads(t.applies_if_json)
                except Exception:
                    cond = None

            if not _applies(cond, year_built=prop.year_built, has_garage=prop.has_garage, property_type=prop.property_type):
                continue

            items.append(
                ChecklistItemOut(
                    category=t.category,
                    item_code=t.code,
                    description=t.description,
                    severity=int(t.severity),
                    common_fail=bool(t.common_fail),
                    applies_if=cond,
                )
            )
    else:
        # 2) fallback to in-code template
        for raw in _SECTION8_TEMPLATE:
            cond = raw.get("applies_if")
            if not _applies(cond, year_built=prop.year_built, has_garage=prop.has_garage, property_type=prop.property_type):
                continue
            items.append(
                ChecklistItemOut(
                    category=raw["category"],
                    item_code=raw["item_code"],
                    description=raw["description"],
                    severity=int(raw.get("severity", 1)),
                    common_fail=bool(raw.get("common_fail", True)),
                    applies_if=cond,
                )
            )

    out = ChecklistOut(
        property_id=property_id,
        strategy=strategy,
        city=prop.city,
        state=prop.state,
        items=items,
    )

    if persist:
        row = PropertyChecklist(
            property_id=property_id,
            strategy=strategy,
            version=version,
            generated_at=out.generated_at,
            items_json=json.dumps([i.model_dump() for i in items]),
        )
        db.add(row)
        db.commit()

    return out


@router.get("/checklist/{property_id}/latest", response_model=PropertyChecklistOut)
def get_latest_checklist(
    property_id: int,
    db: Session = Depends(get_db),
):
    row = db.scalar(
        select(PropertyChecklist)
        .where(PropertyChecklist.property_id == property_id)
        .order_by(desc(PropertyChecklist.id))
        .limit(1)
    )
    if not row:
        raise HTTPException(status_code=404, detail="no checklist found for property")

    try:
        parsed = json.loads(row.items_json or "[]")
    except Exception:
        parsed = []

    items = [ChecklistItemOut(**x) for x in parsed if isinstance(x, dict)]

    return PropertyChecklistOut(
        id=row.id,
        property_id=row.property_id,
        strategy=row.strategy,
        version=row.version,
        generated_at=row.generated_at,
        items=items,
    )
