from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from ..auth import get_principal
from ..db import get_db
from ..models import Property, RehabTask
from ..schemas import RehabTaskCreate, RehabTaskOut

router = APIRouter(prefix="/rehab", tags=["rehab"])


@router.post("/tasks", response_model=RehabTaskOut)
def create_task(payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.scalar(select(Property).where(Property.id == payload.property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    row = RehabTask(
        org_id=p.org_id,
        property_id=payload.property_id,
        title=payload.title,
        category=payload.category,
        inspection_relevant=payload.inspection_relevant,
        status=payload.status,
        cost_estimate=payload.cost_estimate,
        vendor=payload.vendor,
        deadline=payload.deadline,
        notes=payload.notes,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/tasks", response_model=list[RehabTaskOut])
def list_tasks(
    property_id: int = Query(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == p.org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    rows = db.scalars(
        select(RehabTask)
        .where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        .order_by(desc(RehabTask.id))
    ).all()
    return rows


@router.patch("/tasks/{task_id}", response_model=RehabTaskOut)
def update_task(task_id: int, payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    # payload uses RehabTaskCreate to keep it simple; we’ll treat fields as full update.
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    # if property_id changes, validate org ownership
    if payload.property_id != row.property_id:
        prop = db.scalar(select(Property).where(Property.id == payload.property_id, Property.org_id == p.org_id))
        if not prop:
            raise HTTPException(status_code=404, detail="property not found")
        row.property_id = payload.property_id

    row.title = payload.title
    row.category = payload.category
    row.inspection_relevant = payload.inspection_relevant
    row.status = payload.status
    row.cost_estimate = payload.cost_estimate
    row.vendor = payload.vendor
    row.deadline = payload.deadline
    row.notes = payload.notes

    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/tasks/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    db.delete(row)
    db.commit()
    return {"ok": True}
