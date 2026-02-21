# backend/app/routers/inspections.py
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..auth import get_principal, require_operator
from ..db import get_db
from ..models import (
    Inspector,
    Inspection,
    InspectionItem,
    Property,
    PropertyChecklistItem,
    RehabTask,
)
from ..schemas import (
    InspectorUpsert,
    InspectorOut,
    InspectionCreate,
    InspectionOut,
    InspectionItemCreate,
    InspectionItemOut,
    InspectionItemResolve,
    PredictFailPointsOut,
    ComplianceStatsOut,
)
from ..domain.audit import emit_audit
from ..services.ownership import must_get_property
from ..services.events_facade import wf
from ..services.property_state_machine import advance_stage_if_needed

from ..domain.compliance.inspection_mapping import map_inspection_code

# keep your analytics helpers
from ..domain.compliance import top_fail_points, compliance_stats

router = APIRouter(prefix="/inspections", tags=["inspections"])


def _normalize_code(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "_")


# -----------------------------
# Inspectors
# -----------------------------
@router.put("/inspectors", response_model=InspectorOut)
def upsert_inspector(
    payload: InspectorUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectorOut:
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Inspector name is required")

    stmt = select(Inspector).where(Inspector.name == name)
    if payload.agency is None:
        stmt = stmt.where(Inspector.agency.is_(None))
    else:
        stmt = stmt.where(Inspector.agency == payload.agency)

    ins = db.scalar(stmt)

    if ins is None:
        ins = Inspector(name=name, agency=payload.agency)
        db.add(ins)
        db.commit()
        db.refresh(ins)
        return ins

    ins.agency = payload.agency
    db.commit()
    db.refresh(ins)
    return ins


# -----------------------------
# Inspections
# -----------------------------
@router.post("", response_model=InspectionOut)
def create_inspection(
    payload: InspectionCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectionOut:
    # org boundary via property
    must_get_property(db, org_id=p.org_id, property_id=payload.property_id)

    # Validate inspector if provided
    if payload.inspector_id is not None:
        ins = db.get(Inspector, payload.inspector_id)
        if not ins:
            raise HTTPException(status_code=400, detail="Invalid inspector_id")

    insp = Inspection(
        property_id=payload.property_id,
        inspector_id=payload.inspector_id,
        inspection_date=payload.inspection_date or datetime.utcnow(),
        passed=payload.passed,
        reinspect_required=payload.reinspect_required,
        notes=payload.notes,
    )
    db.add(insp)
    db.commit()
    db.refresh(insp)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection.create",
        entity_type="Inspection",
        entity_id=str(insp.id),
        before=None,
        after={"property_id": insp.property_id, "passed": insp.passed, "reinspect_required": insp.reinspect_required},
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="inspection.created",
        property_id=insp.property_id,
        payload={"inspection_id": insp.id},
    )

    advance_stage_if_needed(db, org_id=p.org_id, property_id=insp.property_id, suggested_stage="compliance")

    db.commit()
    return insp


@router.post("/{inspection_id}/items", response_model=InspectionItemOut)
def add_item(
    inspection_id: int,
    payload: InspectionItemCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectionItemOut:
    insp = db.get(Inspection, inspection_id)
    if not insp:
        raise HTTPException(status_code=404, detail="Inspection not found")

    # Ensure inspection property belongs to org
    prop = db.scalar(select(Property).where(Property.id == insp.property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    code = _normalize_code(payload.code)
    if not code:
        raise HTTPException(status_code=400, detail="Invalid code")

    existing = db.scalar(
        select(InspectionItem).where(
            InspectionItem.inspection_id == inspection_id,
            InspectionItem.code == code,
        )
    )

    before = None
    if existing:
        before = {
            "failed": existing.failed,
            "severity": existing.severity,
            "location": existing.location,
            "details": existing.details,
            "resolved_at": existing.resolved_at.isoformat() if existing.resolved_at else None,
        }

        existing.failed = payload.failed
        existing.severity = payload.severity
        existing.location = payload.location
        existing.details = payload.details

        if existing.failed is False and existing.resolved_at is None:
            existing.resolved_at = datetime.utcnow()
        if existing.failed is True and existing.resolved_at is not None:
            existing.resolved_at = None
            existing.resolution_notes = None

        row = existing
    else:
        row = InspectionItem(
            inspection_id=inspection_id,
            code=code,
            failed=payload.failed,
            severity=payload.severity,
            location=payload.location,
            details=payload.details,
        )
        if row.failed is False:
            row.resolved_at = datetime.utcnow()

        db.add(row)

    db.commit()
    db.refresh(row)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection_item.upsert",
        entity_type="InspectionItem",
        entity_id=str(row.id),
        before=before,
        after={
            "inspection_id": inspection_id,
            "code": row.code,
            "failed": row.failed,
            "severity": row.severity,
        },
    )

    # -----------------------------
    # Phase 3 closure: failure -> checklist + rehab task
    # -----------------------------
    if row.failed:
        mapped = map_inspection_code(row.code)

        if mapped:
            # Flag checklist item as failed (create if missing)
            ci = db.scalar(
                select(PropertyChecklistItem).where(
                    PropertyChecklistItem.org_id == p.org_id,
                    PropertyChecklistItem.property_id == prop.id,
                    PropertyChecklistItem.item_code == mapped.checklist_code,
                )
            )

            now = datetime.utcnow()
            if ci is None:
                ci = PropertyChecklistItem(
                    org_id=p.org_id,
                    property_id=prop.id,
                    checklist_id=None,
                    item_code=mapped.checklist_code,
                    category="inspection",
                    description=f"Auto-mapped from inspection failure: {row.code}",
                    severity=max(int(row.severity or 2), 2),
                    common_fail=True,
                    applies_if_json=None,
                    status="failed",
                    marked_by_user_id=p.user_id,
                    marked_at=now,
                    notes=row.details,
                    created_at=now,
                    updated_at=now,
                )
                db.add(ci)
            else:
                ci.status = "failed"
                ci.marked_by_user_id = p.user_id
                ci.marked_at = now
                ci.updated_at = now
                if row.details:
                    ci.notes = (ci.notes or "") + (("\n" if ci.notes else "") + row.details)

            # Create rehab task (idempotent by title)
            if mapped.rehab_title:
                existing_task = db.scalar(
                    select(RehabTask).where(
                        RehabTask.org_id == p.org_id,
                        RehabTask.property_id == prop.id,
                        RehabTask.title == mapped.rehab_title,
                    )
                )
                if existing_task is None:
                    task = RehabTask(
                        org_id=p.org_id,
                        property_id=prop.id,
                        title=mapped.rehab_title,
                        category=mapped.rehab_category,
                        inspection_relevant=bool(mapped.inspection_relevant),
                        status="todo",
                        created_at=now,
                    )
                    db.add(task)

            wf(
                db,
                org_id=p.org_id,
                actor_user_id=p.user_id,
                event_type="inspection.item_failed",
                property_id=prop.id,
                payload={
                    "inspection_id": insp.id,
                    "code": row.code,
                    "mapped_checklist": mapped.checklist_code,
                    "rehab_task_title": mapped.rehab_title,
                },
            )

            db.commit()

    return row


@router.patch("/items/{item_id}/resolve", response_model=InspectionItemOut)
def resolve_item(
    item_id: int,
    payload: InspectionItemResolve,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectionItemOut:
    item = db.get(InspectionItem, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Inspection item not found")

    insp = db.get(Inspection, item.inspection_id)
    if not insp:
        raise HTTPException(status_code=404, detail="Inspection not found")

    # org boundary
    must_get_property(db, org_id=p.org_id, property_id=insp.property_id)

    before = {
        "failed": item.failed,
        "resolved_at": item.resolved_at.isoformat() if item.resolved_at else None,
        "resolution_notes": item.resolution_notes,
    }

    item.failed = False
    item.resolution_notes = payload.resolution_notes
    item.resolved_at = payload.resolved_at or datetime.utcnow()

    db.commit()
    db.refresh(item)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection_item.resolve",
        entity_type="InspectionItem",
        entity_id=str(item.id),
        before=before,
        after={
            "failed": item.failed,
            "resolved_at": item.resolved_at.isoformat() if item.resolved_at else None,
            "resolution_notes": item.resolution_notes,
        },
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="inspection.item_resolved",
        property_id=insp.property_id,
        payload={"item_id": item.id, "code": item.code},
    )
    db.commit()
    return item


# -----------------------------
# Analytics / Prediction
# -----------------------------
@router.get("/predict", response_model=PredictFailPointsOut)
def predict_fail_points(
    city: str = Query(...),
    state: str = Query("MI"),
    inspector_id: int | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
) -> PredictFailPointsOut:
    inspector_name = None
    if inspector_id is not None:
        ins = db.get(Inspector, inspector_id)
        if not ins:
            raise HTTPException(status_code=400, detail="Invalid inspector_id")
        inspector_name = ins.name

    out = top_fail_points(db, city=city, state=state, inspector_id=inspector_id, limit=limit)

    return PredictFailPointsOut(
        city=city,
        inspector=inspector_name,
        window_inspections=out.get("inspection_count", 0),
        top_fail_points=out.get("top", []),
    )


@router.get("/stats", response_model=ComplianceStatsOut)
def stats(
    city: str = Query(...),
    state: str = Query("MI"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
) -> ComplianceStatsOut:
    s = compliance_stats(db, city=city, state=state, limit=limit)

    return ComplianceStatsOut(
        city=city,
        inspections=s.get("inspections", 0),
        pass_rate=s.get("pass_rate", 0.0),
        reinspect_rate=s.get("reinspect_rate", 0.0),
        top_fail_points=s.get("top_fail_points", []),
    )