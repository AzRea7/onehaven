from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Property, Inspector, Inspection, InspectionItem
from ..schemas import (
    InspectorUpsert,
    InspectorOut,
    InspectionCreate,
    InspectionOut,
    InspectionItemCreate,
    InspectionItemOut,
    PredictFailPointsOut,
    ComplianceStatsOut,
)
from ..domain.compliance import top_fail_points, compliance_stats

router = APIRouter(prefix="/inspections", tags=["inspections"])


@router.put("/inspectors", response_model=InspectorOut)
def upsert_inspector(payload: InspectorUpsert, db: Session = Depends(get_db)):
    existing = db.scalar(
        select(Inspector).where(Inspector.name == payload.name, Inspector.agency == payload.agency)
    )
    if existing:
        return existing

    ins = Inspector(name=payload.name, agency=payload.agency)
    db.add(ins)
    db.commit()
    db.refresh(ins)
    return ins


@router.post("", response_model=InspectionOut)
def create_inspection(payload: InspectionCreate, db: Session = Depends(get_db)):
    prop = db.get(Property, payload.property_id)
    if not prop:
        raise HTTPException(status_code=400, detail="Invalid property_id")

    if payload.inspector_id is not None:
        ins = db.get(Inspector, payload.inspector_id)
        if not ins:
            raise HTTPException(status_code=400, detail="Invalid inspector_id")

    dt = payload.inspection_date or datetime.utcnow()

    insp = Inspection(
        property_id=payload.property_id,
        inspector_id=payload.inspector_id,
        inspection_date=dt,
        passed=payload.passed,
        reinspect_required=payload.reinspect_required,
        notes=payload.notes,
    )
    db.add(insp)
    db.commit()
    db.refresh(insp)
    return insp


@router.post("/{inspection_id}/items", response_model=InspectionItemOut)
def add_item(inspection_id: int, payload: InspectionItemCreate, db: Session = Depends(get_db)):
    insp = db.get(Inspection, inspection_id)
    if not insp:
        raise HTTPException(status_code=404, detail="Inspection not found")

    code = payload.code.strip().upper().replace(" ", "_")
    if not code:
        raise HTTPException(status_code=400, detail="Invalid code")

    # enforce unique code per inspection
    existing = db.scalar(select(InspectionItem).where(InspectionItem.inspection_id == inspection_id, InspectionItem.code == code))
    if existing:
        # update in place (idempotent add)
        existing.failed = payload.failed
        existing.severity = payload.severity
        existing.location = payload.location
        existing.details = payload.details
        db.commit()
        db.refresh(existing)
        return existing

    item = InspectionItem(
        inspection_id=inspection_id,
        code=code,
        failed=payload.failed,
        severity=payload.severity,
        location=payload.location,
        details=payload.details,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.get("/predict", response_model=PredictFailPointsOut)
def predict_fail_points(
    city: str = Query(...),
    state: str = Query("MI"),
    inspector_id: int | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
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
        window_inspections=out["inspection_count"],
        top_fail_points=out["top"],
    )


@router.get("/stats", response_model=ComplianceStatsOut)
def stats(
    city: str = Query(...),
    state: str = Query("MI"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    s = compliance_stats(db, city=city, state=state, limit=limit)
    return ComplianceStatsOut(
        city=city,
        inspections=s["inspections"],
        pass_rate=s["pass_rate"],
        reinspect_rate=s["reinspect_rate"],
        top_fail_points=s["top_fail_points"],
    )
