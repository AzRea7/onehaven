from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from app.db import get_db
from app.domain.audit import emit_audit
from app.domain.events import emit_workflow_event
from app.models import Property, RehabTask
from app.schemas import RehabTaskCreate, RehabTaskOut, RehabPhotoAnalysisOut
from app.services.properties.state_machine import sync_property_state
from app.services.stage_guard import require_stage
from app.services.workflow_gate_service import build_workflow_summary
from app.services.photo_rehab_agent import analyze_property_photos, analyze_and_create_rehab_tasks

router = APIRouter(prefix="/rehab", tags=["rehab"])


def _get_property_or_404(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


def _task_payload(row: RehabTask) -> dict:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "title": row.title,
        "category": row.category,
        "inspection_relevant": row.inspection_relevant,
        "status": row.status,
        "cost_estimate": row.cost_estimate,
        "vendor": row.vendor,
        "deadline": row.deadline.isoformat() if row.deadline else None,
        "notes": row.notes,
    }


@router.post("/tasks", response_model=RehabTaskOut)
def create_task(payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    _get_property_or_404(db, org_id=p.org_id, property_id=payload.property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="acquisition",
        action="create rehab tasks",
    )

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
        after=_task_payload(row),
    )
    emit_workflow_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rehab_task_created",
        property_id=row.property_id,
        payload={"task_id": int(row.id), "property_id": int(row.property_id), "status": row.status},
    )

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    db.commit()
    db.refresh(row)
    return row


@router.get("/tasks", response_model=list[RehabTaskOut])
def list_tasks(property_id: int = Query(...), db: Session = Depends(get_db), p=Depends(get_principal)):
    _get_property_or_404(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="acquisition",
        action="view rehab tasks",
    )

    rows = db.scalars(
        select(RehabTask)
        .where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        .order_by(desc(RehabTask.id))
    ).all()
    return rows

@router.get("/from-photos/{property_id}", response_model=RehabPhotoAnalysisOut)
def preview_rehab_from_photos(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _get_property_or_404(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="acquisition",
        action="analyze rehab from photos",
    )
    return analyze_property_photos(db, org_id=p.org_id, property_id=property_id)


@router.post("/from-photos/{property_id}", response_model=RehabPhotoAnalysisOut)
def generate_rehab_from_photos(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _get_property_or_404(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="acquisition",
        action="generate rehab tasks from photos",
    )
    return analyze_and_create_rehab_tasks(db, org_id=p.org_id, property_id=property_id)

@router.get("/tasks/summary/{property_id}", response_model=dict)
def rehab_summary(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    _get_property_or_404(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="acquisition",
        action="view rehab summary",
    )

    rows = db.scalars(
        select(RehabTask)
        .where(RehabTask.org_id == p.org_id, RehabTask.property_id == property_id)
        .order_by(desc(RehabTask.id))
    ).all()

    total = len(rows)
    done = sum(1 for r in rows if (r.status or "todo").lower() == "done")
    blocked = sum(1 for r in rows if (r.status or "todo").lower() == "blocked")
    in_progress = sum(1 for r in rows if (r.status or "todo").lower() == "in_progress")
    todo = total - done - blocked - in_progress

    cost_estimate_sum = 0.0
    for r in rows:
        if r.cost_estimate is not None:
            try:
                cost_estimate_sum += float(r.cost_estimate or 0.0)
            except Exception:
                pass

    return {
        "property_id": property_id,
        "summary": {
            "total": total,
            "todo": todo,
            "in_progress": in_progress,
            "blocked": blocked,
            "done": done,
            "cost_estimate_sum": round(cost_estimate_sum, 2),
        },
        "workflow": build_workflow_summary(db, org_id=p.org_id, property_id=property_id, recompute=True),
    }


@router.patch("/tasks/{task_id}", response_model=RehabTaskOut)
def update_task(task_id: int, payload: RehabTaskCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="acquisition",
        action="update rehab tasks",
    )

    before = _task_payload(row)
    old_status = (row.status or "").lower()
    old_property_id = row.property_id

    if payload.property_id != row.property_id:
        _get_property_or_404(db, org_id=p.org_id, property_id=payload.property_id)
        require_stage(
            db,
            org_id=p.org_id,
            property_id=payload.property_id,
            min_stage="acquisition",
            action="move rehab task to another property",
        )
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
        after=_task_payload(row),
    )

    if old_status != (row.status or "").lower():
        emit_workflow_event(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            event_type="rehab_task_status_changed",
            property_id=row.property_id,
            payload={
                "task_id": int(row.id),
                "property_id": int(row.property_id),
                "from": old_status,
                "to": row.status,
            },
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

    sync_property_state(db, org_id=p.org_id, property_id=row.property_id)
    if old_property_id != row.property_id:
        sync_property_state(db, org_id=p.org_id, property_id=old_property_id)

    db.commit()
    db.refresh(row)
    return row


@router.delete("/tasks/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    row = db.scalar(select(RehabTask).where(RehabTask.id == task_id, RehabTask.org_id == p.org_id))
    if not row:
        raise HTTPException(status_code=404, detail="rehab task not found")

    require_stage(
        db,
        org_id=p.org_id,
        property_id=row.property_id,
        min_stage="acquisition",
        action="delete rehab tasks",
    )

    before = _task_payload(row)
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

    sync_property_state(db, org_id=p.org_id, property_id=prop_id)
    db.commit()
    return {"ok": True}