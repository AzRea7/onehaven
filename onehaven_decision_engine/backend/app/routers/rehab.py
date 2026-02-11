# backend/app/routers/rehab.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import RehabTask
from ..schemas import RehabTaskCreate, RehabTaskOut

router = APIRouter(prefix="/rehab", tags=["rehab"])


@router.post("/tasks", response_model=RehabTaskOut)
def create_task(payload: RehabTaskCreate, db: Session = Depends(get_db)):
    row = RehabTask(**payload.model_dump())
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/tasks", response_model=list[RehabTaskOut])
def list_tasks(
    property_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    q = select(RehabTask).order_by(desc(RehabTask.id))
    if property_id is not None:
        q = q.where(RehabTask.property_id == property_id)
    if status:
        q = q.where(RehabTask.status == status)
    return list(db.scalars(q.limit(limit)).all())


@router.patch("/tasks/{task_id}", response_model=RehabTaskOut)
def update_task(task_id: int, payload: dict, db: Session = Depends(get_db)):
    row = db.get(RehabTask, task_id)
    if not row:
        raise HTTPException(status_code=404, detail="task not found")
    for k, v in payload.items():
        if hasattr(row, k):
            setattr(row, k, v)
    db.commit()
    db.refresh(row)
    return row
