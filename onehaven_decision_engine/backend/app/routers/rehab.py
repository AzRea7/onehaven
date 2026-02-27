from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, desc

from ..auth import get_principal
from ..db import get_db
from ..models import Property, RehabTask
from ..schemas import RehabTaskCreate, RehabTaskOut
from ..domain.audit import emit_audit
from ..domain.events import emit_workflow_event

router = APIRouter(prefix="/rehab", tags=["rehab"])


def _get_property_or_404(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


@router.post("/tasks", response_model=RehabTaskOut)
def create_task(payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    _get_property_or_404(db, org_id=p.org_id, property_id=payload.property_id)

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
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="rehab_task.create",
        entity_type="RehabTask",
        entity_id=str(row.id),
        before=None,
        after=row.model_dump(),
    )
    emit_workflow_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rehab_task_created",
        property_id=row.property_id,
        payload={"task_id": int(row.id), "property_id": int(row.property_id), "status": row.status},
    )

    db.commit()
    db.refresh(row)
    return row


@router.get("/tasks", response_model=list[RehabTaskOut])
def list_tasks(property_id: int = Query(...), db: Session = Depends(get_db), p=Depends(get_principal)):
    _get_property_or_404(db, org_id=p.org_id, property_id=property_id)
    rows = db.scalars(
        select(RehabTask)
        .where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        .order_by(desc(RehabTask.id))
    ).all()
    return rows


@router.patch("/tasks/{task_id}", response_model=RehabTaskOut)
def update_task(task_id: int, payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    before = row.model_dump()
    old_status = getattr(row, "status", None)

    if payload.property_id != row.property_id:
        _get_property_or_404(db, org_id=p.org_id, property_id=payload.property_id)
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
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="rehab_task.update",
        entity_type="RehabTask",
        entity_id=str(row.id),
        before=before,
        after=row.model_dump(),
    )

    if old_status != row.status:
        emit_workflow_event(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            event_type="rehab_task_status_changed",
            property_id=row.property_id,
            payload={"task_id": int(row.id), "property_id": int(row.property_id), "from": old_status, "to": row.status},
        )
    else:
        emit_workflow_event(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            event_type="rehab_task_updated",
            property_id=row.property_id,
            payload={"task_id": int(row.id), "property_id": int(row.property_id)},
        )

    db.commit()
    db.refresh(row)
    return row


@router.delete("/tasks/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    before = row.model_dump()
    prop_id = int(row.property_id)

    db.delete(row)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="rehab_task.delete",
        entity_type="RehabTask",
        entity_id=str(task_id),
        before=before,
        after=None,
    )
    emit_workflow_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rehab_task_deleted",
        property_id=prop_id,
        payload={"task_id": int(task_id), "property_id": prop_id},
    )

    db.commit()
    return {"ok": True}
