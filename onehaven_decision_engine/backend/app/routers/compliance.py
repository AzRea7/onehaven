from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from ..auth import get_principal, require_owner
from ..db import get_db
from ..models import (
    Property,
    ChecklistTemplateItem,
    PropertyChecklist,
    PropertyChecklistItem,
    AuditEvent,
    WorkflowEvent,
    AppUser,
)
from ..schemas import (
    ChecklistOut,
    ChecklistItemOut,
    ChecklistTemplateItemUpsert,
    ChecklistTemplateItemOut,
    PropertyChecklistOut,
    ChecklistItemUpdateIn,
)

router = APIRouter(prefix="/compliance", tags=["compliance"])


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


def _items_from_templates(prop: Property, tmpl_rows: list[ChecklistTemplateItem]) -> list[ChecklistItemOut]:
    items: list[ChecklistItemOut] = []
    for t in tmpl_rows:
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
                status="todo",
            )
        )
    return items


def _items_from_fallback(prop: Property) -> list[ChecklistItemOut]:
    items: list[ChecklistItemOut] = []
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
                status="todo",
            )
        )
    return items


def _merge_state(db: Session, *, org_id: int, property_id: int, items: list[ChecklistItemOut]) -> list[ChecklistItemOut]:
    state_rows = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()
    state_by_code: dict[str, PropertyChecklistItem] = {r.item_code: r for r in state_rows}

    user_ids = {r.marked_by_user_id for r in state_rows if r.marked_by_user_id}
    users_by_id: dict[int, str] = {}
    if user_ids:
        for u in db.scalars(select(AppUser).where(AppUser.id.in_(list(user_ids)))).all():
            users_by_id[u.id] = u.email

    out: list[ChecklistItemOut] = []
    for i in items:
        r = state_by_code.get(i.item_code)
        if r:
            i.status = r.status
            i.marked_at = r.marked_at
            i.proof_url = r.proof_url
            i.notes = r.notes
            if r.marked_by_user_id:
                i.marked_by = users_by_id.get(r.marked_by_user_id)
        out.append(i)
    return out


@router.get("/templates", response_model=list[ChecklistTemplateItemOut])
def list_templates(
    strategy: str = Query(default="section8"),
    version: str = Query(default="v1"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    # global-by-design (for now)
    rows = db.scalars(
        select(ChecklistTemplateItem)
        .where(ChecklistTemplateItem.strategy == strategy, ChecklistTemplateItem.version == version)
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()
    return rows


@router.put("/templates", response_model=ChecklistTemplateItemOut)
def upsert_template(payload: ChecklistTemplateItemUpsert, db: Session = Depends(get_db), p=Depends(get_principal)):
    # global-by-design right now: only owners can mutate
    require_owner(p)

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
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    tmpl = db.scalars(
        select(ChecklistTemplateItem)
        .where(ChecklistTemplateItem.strategy == strategy, ChecklistTemplateItem.version == version)
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()

    items = _items_from_templates(prop, tmpl) if tmpl else _items_from_fallback(prop)

    out = ChecklistOut(
        property_id=property_id,
        strategy=strategy,
        city=prop.city,
        state=prop.state,
        items=items,
    )

    if persist:
        now = datetime.utcnow()

        # ✅ UPSERT to satisfy uq_property_checklists_org_property_strategy_version
        row = db.scalar(
            select(PropertyChecklist).where(
                PropertyChecklist.org_id == p.org_id,
                PropertyChecklist.property_id == property_id,
                PropertyChecklist.strategy == strategy,
                PropertyChecklist.version == version,
            )
        )

        if not row:
            row = PropertyChecklist(
                org_id=p.org_id,
                property_id=property_id,
                strategy=strategy,
                version=version,
                generated_at=out.generated_at,
                items_json=json.dumps([i.model_dump() for i in items]),
            )
            db.add(row)
            db.flush()
        else:
            row.generated_at = out.generated_at
            row.items_json = json.dumps([i.model_dump() for i in items])
            db.add(row)
            db.flush()

        for i in items:
            existing = db.scalar(
                select(PropertyChecklistItem).where(
                    PropertyChecklistItem.org_id == p.org_id,
                    PropertyChecklistItem.property_id == property_id,
                    PropertyChecklistItem.item_code == i.item_code,
                )
            )
            applies_if_json = json.dumps(i.applies_if) if i.applies_if else None

            if not existing:
                db.add(
                    PropertyChecklistItem(
                        org_id=p.org_id,
                        property_id=property_id,
                        checklist_id=row.id,
                        item_code=i.item_code,
                        category=i.category,
                        description=i.description,
                        severity=int(i.severity),
                        common_fail=bool(i.common_fail),
                        applies_if_json=applies_if_json,
                        status="todo",
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                existing.checklist_id = row.id
                existing.category = i.category
                existing.description = i.description
                existing.severity = int(i.severity)
                existing.common_fail = bool(i.common_fail)
                existing.applies_if_json = applies_if_json
                existing.updated_at = now
                db.add(existing)

        db.commit()

        out.items = _merge_state(db, org_id=p.org_id, property_id=property_id, items=out.items)


    return out


@router.get("/checklist/{property_id}/latest", response_model=PropertyChecklistOut)
def get_latest_checklist(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    row = db.scalar(
        select(PropertyChecklist)
        .where(PropertyChecklist.property_id == property_id)
        .where(PropertyChecklist.org_id == p.org_id)
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
    items = _merge_state(db, org_id=p.org_id, property_id=property_id, items=items)

    return PropertyChecklistOut(
        id=row.id,
        org_id=row.org_id,
        property_id=row.property_id,
        strategy=row.strategy,
        version=row.version,
        generated_at=row.generated_at,
        items=items,
    )


@router.patch("/checklist/{property_id}/items/{item_code}", response_model=ChecklistItemOut)
def update_checklist_item(
    property_id: int,
    item_code: str,
    payload: ChecklistItemUpdateIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    row = db.scalar(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
            PropertyChecklistItem.item_code == item_code,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="checklist item not found (generate checklist first)")

    before = {
        "status": row.status,
        "proof_url": row.proof_url,
        "notes": row.notes,
        "marked_by_user_id": row.marked_by_user_id,
        "marked_at": row.marked_at.isoformat() if row.marked_at else None,
    }

    now = datetime.utcnow()

    # ✅ Normalize + validate status values (supports your UI: failed)
    if payload.status is not None:
        s = (payload.status or "").strip().lower()
        allowed = {"todo", "in_progress", "done", "blocked", "failed"}
        if s not in allowed:
            raise HTTPException(status_code=400, detail=f"invalid status: {payload.status}")
        row.status = s
        row.marked_by_user_id = p.user_id
        row.marked_at = now

    if payload.proof_url is not None:
        row.proof_url = payload.proof_url
    if payload.notes is not None:
        row.notes = payload.notes

    row.updated_at = now
    db.add(row)

    after = {
        "status": row.status,
        "proof_url": row.proof_url,
        "notes": row.notes,
        "marked_by_user_id": row.marked_by_user_id,
        "marked_at": row.marked_at.isoformat() if row.marked_at else None,
    }

    db.add(
        AuditEvent(
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="checklist_item_updated",
            entity_type="property_checklist_item",
            entity_id=f"{property_id}:{item_code}",
            before_json=json.dumps(before),
            after_json=json.dumps(after),
            created_at=now,
        )
    )
    db.add(
        WorkflowEvent(
            org_id=p.org_id,
            property_id=property_id,
            actor_user_id=p.user_id,
            event_type="checklist_item_updated",
            payload_json=json.dumps({"item_code": item_code, "status": row.status}),
            created_at=now,
        )
    )

    db.commit()
    db.refresh(row)

    return ChecklistItemOut(
        item_code=row.item_code,
        category=row.category,
        description=row.description,
        severity=int(row.severity),
        common_fail=bool(row.common_fail),
        applies_if=json.loads(row.applies_if_json) if row.applies_if_json else None,
        status=row.status,
        marked_at=row.marked_at,
        proof_url=row.proof_url,
        notes=row.notes,
        marked_by=p.email,
    )
