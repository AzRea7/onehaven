from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_operator
from ..db import get_db
from ..domain.audit import emit_audit
from ..domain.compliance import compliance_stats, top_fail_points
from ..domain.compliance.inspection_mapping import map_inspection_code
from ..models import (
    Inspection,
    InspectionItem,
    Inspector,
    Property,
    PropertyChecklistItem,
    RehabTask,
)
from ..schemas import (
    ComplianceStatsOut,
    InspectionCreate,
    InspectionItemCreate,
    InspectionItemOut,
    InspectionItemResolve,
    InspectionOut,
    InspectorOut,
    InspectorUpsert,
    PredictFailPointsOut,
)
from ..services.compliance_service import apply_inspection_form_results
from ..services.events_facade import wf
from ..services.inspection_failure_task_service import (
    build_failure_next_actions,
    create_failure_tasks_from_inspection,
)
from ..services.inspection_readiness_service import build_property_readiness_summary
from ..services.inspection_scheduling_service import (
    build_inspection_ics_payload,
    build_inspection_timeline,
    build_property_schedule_summary,
    cancel_inspection_appointment,
    mark_inspection_completed,
    schedule_inspection_appointment,
    send_inspection_reminder,
)
from ..services.inspector_communication_service import (
    build_inspection_reminder_message,
    build_inspector_contact_payload,
)
from ..services.ownership import must_get_property
from ..services.property_state_machine import sync_property_state
from ..services.stage_guard import require_stage
from ..services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/inspections", tags=["inspections"])


def _normalize_code(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "_")


def _inspection_payload(row: Inspection) -> dict[str, Any]:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "inspector_id": row.inspector_id,
        "inspection_date": row.inspection_date.isoformat() if row.inspection_date else None,
        "passed": row.passed,
        "reinspect_required": row.reinspect_required,
        "notes": row.notes,
        "template_key": getattr(row, "template_key", None),
        "template_version": getattr(row, "template_version", None),
        "result_status": getattr(row, "result_status", None),
        "readiness_score": getattr(row, "readiness_score", None),
        "readiness_status": getattr(row, "readiness_status", None),
        "submitted_at": getattr(row, "submitted_at", None),
        "completed_at": getattr(row, "completed_at", None),
    }


def _inspection_item_payload(row: InspectionItem) -> dict[str, Any]:
    return {
        "id": row.id,
        "inspection_id": row.inspection_id,
        "code": row.code,
        "failed": row.failed,
        "severity": row.severity,
        "location": row.location,
        "details": row.details,
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
        "resolution_notes": row.resolution_notes,
        "category": getattr(row, "category", None),
        "result_status": getattr(row, "result_status", None),
        "fail_reason": getattr(row, "fail_reason", None),
        "remediation_guidance": getattr(row, "remediation_guidance", None),
        "evidence_json": getattr(row, "evidence_json", None),
        "photo_references_json": getattr(row, "photo_references_json", None),
        "standard_label": getattr(row, "standard_label", None),
        "standard_citation": getattr(row, "standard_citation", None),
        "readiness_impact": getattr(row, "readiness_impact", None),
        "requires_reinspection": getattr(row, "requires_reinspection", None),
    }


def _must_get_org_inspection(db: Session, *, org_id: int, inspection_id: int) -> Inspection:
    row = db.scalar(
        select(Inspection).where(
            Inspection.id == inspection_id,
            Inspection.org_id == org_id,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="Inspection not found")
    return row


def _must_get_property_for_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> Property:
    return must_get_property(db, org_id=org_id, property_id=property_id)




def _step7_pdf_dataset_status() -> dict[str, Any]:
    roots: list[str] = []
    raw = os.getenv("POLICY_PDFS_ROOT", "") or os.getenv("POLICY_PDF_ROOTS", "") or os.getenv("POLICY_PDF_ROOT", "") or os.getenv("NSPIRE_PDF_ROOT", "")
    for piece in str(raw).split(os.pathsep):
        piece = str(piece).strip()
        if piece and Path(piece).exists():
            roots.append(str(Path(piece)))
    for fallback in (Path("backend/data/pdfs").resolve(), Path("/app/backend/data/pdfs"), Path("/mnt/data/pdfs"), Path("/mnt/data/PDFs"), Path("/mnt/data/pfs"), Path(r"/mnt/data/step67_pdf_zip/pdfs")):
        if fallback.exists() and str(fallback) not in roots:
            roots.append(str(fallback))
    return {"available": bool(roots), "roots": roots}

def _require_inspection_preview_stage(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    action: str,
) -> None:
    """
    Allow read-only inspection preview endpoints to be visible earlier than full
    compliance execution while preserving strict gates for mutations.
    """
    require_stage(
        db,
        org_id=org_id,
        property_id=property_id,
        min_stage="underwritten",
        action=action,
    )


def _sync_checklist_state_from_inspection_item(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
    inspection_item: InspectionItem,
) -> dict[str, Any] | None:
    mapped = map_inspection_code(inspection_item.code)
    if not mapped:
        return None

    now = datetime.utcnow()
    ci = db.scalar(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == org_id,
            PropertyChecklistItem.property_id == property_id,
            PropertyChecklistItem.item_code == mapped.checklist_code,
        )
    )

    if ci is None:
        ci = PropertyChecklistItem(
            org_id=org_id,
            property_id=property_id,
            checklist_id=None,
            item_code=mapped.checklist_code,
            category="inspection",
            description=f"Auto-mapped from inspection item: {inspection_item.code}",
            severity=max(int(inspection_item.severity or 2), 2),
            common_fail=True,
            applies_if_json=None,
            status="failed" if inspection_item.failed else "done",
            marked_by_user_id=actor_user_id,
            marked_at=now,
            notes=inspection_item.details,
            created_at=now,
            updated_at=now,
        )
        db.add(ci)
    else:
        ci.status = "failed" if inspection_item.failed else "done"
        ci.marked_by_user_id = actor_user_id
        ci.marked_at = now
        ci.updated_at = now
        if inspection_item.details:
            ci.notes = inspection_item.details
        db.add(ci)

    return {
        "mapped_checklist_code": mapped.checklist_code,
        "rehab_title": mapped.rehab_title,
        "rehab_category": mapped.rehab_category,
        "inspection_relevant": bool(mapped.inspection_relevant),
    }


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


@router.post("", response_model=InspectionOut)
def create_inspection(
    payload: InspectionCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectionOut:
    _must_get_property_for_inspection(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
    )
    require_stage(
        db,
        org_id=p.org_id,
        property_id=payload.property_id,
        min_stage="compliance",
        action="create inspection",
    )

    if payload.inspector_id is not None:
        ins = db.get(Inspector, payload.inspector_id)
        if not ins:
            raise HTTPException(status_code=400, detail="Invalid inspector_id")

    insp = Inspection(
        org_id=p.org_id,
        property_id=payload.property_id,
        inspector_id=payload.inspector_id,
        inspection_date=payload.inspection_date or datetime.utcnow(),
        passed=payload.passed,
        reinspect_required=payload.reinspect_required,
        notes=payload.notes,
    )
    db.add(insp)
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection.create",
        entity_type="Inspection",
        entity_id=str(insp.id),
        before=None,
        after=_inspection_payload(insp),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="inspection.created",
        property_id=insp.property_id,
        payload={"inspection_id": insp.id},
    )

    sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
    db.commit()
    db.refresh(insp)
    return insp


@router.get("/{inspection_id}", response_model=dict)
def get_inspection(
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    rows = db.scalars(
        select(InspectionItem)
        .where(InspectionItem.inspection_id == inspection_id)
        .order_by(InspectionItem.id.asc())
    ).all()
    return {
        "ok": True,
        "inspection": _inspection_payload(insp),
        "items": [_inspection_item_payload(r) for r in rows],
        "count": len(rows),
    }


@router.get("/property/{property_id}", response_model=dict)
def list_property_inspections(
    property_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view inspection history",
    )
    rows = db.scalars(
        select(Inspection)
        .where(
            Inspection.org_id == p.org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(limit)
    ).all()
    return {
        "ok": True,
        "property_id": property_id,
        "count": len(rows),
        "rows": [_inspection_payload(r) for r in rows],
    }


@router.post("/{inspection_id}/items", response_model=InspectionItemOut)
def add_item(
    inspection_id: int,
    payload: InspectionItemCreate,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
) -> InspectionItemOut:
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="add inspection item",
    )

    prop = db.scalar(
        select(Property).where(
            Property.id == insp.property_id,
            Property.org_id == p.org_id,
        )
    )
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
        before = _inspection_item_payload(existing)
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
    db.flush()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection_item.upsert",
        entity_type="InspectionItem",
        entity_id=str(row.id),
        before=before,
        after=_inspection_item_payload(row),
    )

    mapping_meta = _sync_checklist_state_from_inspection_item(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        property_id=prop.id,
        inspection_item=row,
    )

    if row.failed:
        if mapping_meta and mapping_meta.get("rehab_title"):
            now = datetime.utcnow()
            existing_task = db.scalar(
                select(RehabTask).where(
                    RehabTask.org_id == p.org_id,
                    RehabTask.property_id == prop.id,
                    RehabTask.title == mapping_meta["rehab_title"],
                )
            )
            if existing_task is None:
                task = RehabTask(
                    org_id=p.org_id,
                    property_id=prop.id,
                    title=mapping_meta["rehab_title"],
                    category=mapping_meta["rehab_category"],
                    inspection_relevant=bool(mapping_meta["inspection_relevant"]),
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
                "mapped_checklist": mapping_meta["mapped_checklist_code"] if mapping_meta else None,
                "rehab_task_title": mapping_meta["rehab_title"] if mapping_meta else None,
            },
        )
    else:
        wf(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            event_type="inspection.item_upserted",
            property_id=insp.property_id,
            payload={
                "inspection_id": insp.id,
                "item_id": row.id,
                "code": row.code,
                "failed": row.failed,
                "mapped_checklist": mapping_meta["mapped_checklist_code"] if mapping_meta else None,
            },
        )

    sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
    db.commit()
    db.refresh(row)
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

    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=item.inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="resolve inspection item",
    )

    before = _inspection_item_payload(item)
    item.failed = False
    item.resolution_notes = payload.resolution_notes
    item.resolved_at = payload.resolved_at or datetime.utcnow()
    db.flush()

    _sync_checklist_state_from_inspection_item(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        property_id=insp.property_id,
        inspection_item=item,
    )

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="inspection_item.resolve",
        entity_type="InspectionItem",
        entity_id=str(item.id),
        before=before,
        after=_inspection_item_payload(item),
    )
    wf(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="inspection.item_resolved",
        property_id=insp.property_id,
        payload={"item_id": item.id, "code": item.code},
    )

    sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
    db.commit()
    db.refresh(item)
    return item


@router.post("/{inspection_id}/submit-form", response_model=dict)
def submit_inspection_form(
    inspection_id: int,
    raw_payload: dict[str, Any] | list[dict[str, Any]] = Body(...),
    sync_checklist: bool = Query(default=True),
    create_failure_tasks: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="submit inspection form",
    )
    try:
        result = apply_inspection_form_results(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            property_id=insp.property_id,
            inspection_id=inspection_id,
            raw_payload=raw_payload,
            sync_checklist=sync_checklist,
            create_failure_tasks=create_failure_tasks,
        )
        sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=insp.property_id,
                recompute=True,
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"inspection form submission failed: {e}")


@router.get("/{inspection_id}/normalized-results", response_model=dict)
def inspection_normalized_results(
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    rows = db.scalars(
        select(InspectionItem)
        .where(InspectionItem.inspection_id == inspection_id)
        .order_by(InspectionItem.id.asc())
    ).all()
    return {
        "ok": True,
        "inspection": _inspection_payload(insp),
        "items": [_inspection_item_payload(r) for r in rows],
        "count": len(rows),
    }


@router.get("/{inspection_id}/readiness-summary", response_model=dict)
def inspection_readiness_summary(
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    return build_property_readiness_summary(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
    )


@router.post("/{inspection_id}/tasks/from-failures", response_model=dict)
def inspection_tasks_from_failures(
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="generate tasks from inspection failures",
    )
    try:
        result = create_failure_tasks_from_inspection(
            db,
            org_id=p.org_id,
            property_id=insp.property_id,
            inspection_id=inspection_id,
        )
        sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=insp.property_id,
                recompute=True,
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failure task generation failed: {e}")


@router.get("/{inspection_id}/failure-actions", response_model=dict)
def inspection_failure_actions(
    inspection_id: int,
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    return build_failure_next_actions(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        inspection_id=inspection_id,
        limit=limit,
    )


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

    out = top_fail_points(
        db,
        city=city,
        state=state,
        inspector_id=inspector_id,
        limit=limit,
    )
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


@router.get("/property/{property_id}/readiness", response_model=dict)
def inspection_readiness(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view inspection readiness",
    )

    latest = db.scalar(
        select(Inspection)
        .where(
            Inspection.org_id == p.org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(1)
    )
    items = db.scalars(
        select(InspectionItem)
        .join(Inspection, Inspection.id == InspectionItem.inspection_id)
        .where(
            Inspection.org_id == p.org_id,
            Inspection.property_id == property_id,
        )
    ).all()
    open_failed = [i for i in items if bool(i.failed) and i.resolved_at is None]
    readiness_summary = build_property_readiness_summary(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    return {
        "property_id": property_id,
        "latest_inspection": _inspection_payload(latest) if latest else None,
        "open_failed_count": len(open_failed),
        "open_failed_items": [_inspection_item_payload(i) for i in open_failed[:25]],
        "readiness_summary": readiness_summary,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }




@router.post("/{inspection_id}/appointment", response_model=dict)
def upsert_inspection_appointment(
    inspection_id: int,
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="schedule inspection appointment",
    )
    try:
        result = schedule_inspection_appointment(
            db,
            org_id=int(p.org_id),
            actor_user_id=int(p.user_id),
            inspection_id=int(inspection_id),
            scheduled_for=payload.get("scheduled_for"),
            inspector_name=payload.get("inspector_name"),
            inspector_company=payload.get("inspector_company"),
            inspector_email=payload.get("inspector_email"),
            inspector_phone=payload.get("inspector_phone"),
            reminder_offsets=payload.get("reminder_offsets"),
            appointment_notes=payload.get("appointment_notes"),
            status=payload.get("status"),
            calendar_provider=payload.get("calendar_provider"),
        )
        sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=insp.property_id,
                recompute=True,
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{inspection_id}/appointment/cancel", response_model=dict)
def cancel_appointment(
    inspection_id: int,
    payload: dict[str, Any] = Body(default={}),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="cancel inspection appointment",
    )
    try:
        result = cancel_inspection_appointment(
            db,
            org_id=int(p.org_id),
            actor_user_id=int(p.user_id),
            inspection_id=int(inspection_id),
            reason=payload.get("reason"),
        )
        sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=insp.property_id,
                recompute=True,
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{inspection_id}/mark-completed", response_model=dict)
def mark_completed(
    inspection_id: int,
    payload: dict[str, Any] = Body(default={}),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="mark inspection completed",
    )
    try:
        result = mark_inspection_completed(
            db,
            org_id=int(p.org_id),
            actor_user_id=int(p.user_id),
            inspection_id=int(inspection_id),
            status=payload.get("status"),
            passed=payload.get("passed"),
            reinspect_required=payload.get("reinspect_required"),
            notes=payload.get("notes"),
            completed_at=payload.get("completed_at"),
        )
        sync_property_state(db, org_id=p.org_id, property_id=insp.property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=insp.property_id,
                recompute=True,
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{inspection_id}/remind", response_model=dict)
def send_reminder(
    inspection_id: int,
    payload: dict[str, Any] = Body(default={}),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=insp.property_id,
        min_stage="compliance",
        action="send inspection reminder",
    )
    try:
        result = send_inspection_reminder(
            db,
            org_id=int(p.org_id),
            actor_user_id=int(p.user_id),
            inspection_id=int(inspection_id),
            reminder_offset_minutes=payload.get("reminder_offset_minutes"),
        )
        db.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{inspection_id}/contact-payload", response_model=dict)
def inspection_contact_payload(
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    return {
        "ok": True,
        "inspection_id": int(inspection_id),
        "property_id": int(insp.property_id),
        "contact_payload": build_inspector_contact_payload(
            db,
            org_id=int(p.org_id),
            inspection_id=int(inspection_id),
        ),
    }


@router.get("/{inspection_id}/reminder-payload", response_model=dict)
def inspection_reminder_payload(
    inspection_id: int,
    reminder_offset_minutes: int = Query(default=60, ge=0, le=10080),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    insp = _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    return {
        "ok": True,
        "inspection_id": int(inspection_id),
        "property_id": int(insp.property_id),
        "payload": build_inspection_reminder_message(
            db,
            org_id=int(p.org_id),
            inspection_id=int(inspection_id),
            reminder_offset_minutes=int(reminder_offset_minutes),
        ),
    }


@router.get("/{inspection_id}/ics", response_model=dict)
def inspection_ics_export(
    inspection_id: int,
    duration_minutes: int = Query(default=60, ge=15, le=480),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_org_inspection(db, org_id=p.org_id, inspection_id=inspection_id)
    try:
        return {
            "ok": True,
            **build_inspection_ics_payload(
                db,
                org_id=int(p.org_id),
                inspection_id=int(inspection_id),
                duration_minutes=int(duration_minutes),
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/property/{property_id}/timeline", response_model=dict)
def inspection_timeline(
    property_id: int,
    limit: int = Query(default=100, ge=1, le=300),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    _require_inspection_preview_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        action="view inspection timeline",
    )
    return build_inspection_timeline(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        limit=int(limit),
    )


@router.get("/property/{property_id}/schedule-summary", response_model=dict)
def inspection_schedule_summary(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    _require_inspection_preview_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        action="view inspection schedule summary",
    )
    return build_property_schedule_summary(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
    )


# --- Step 7 additive inspection workflow endpoints ---
@router.get("/property/{property_id}/template", response_model=dict)
def inspection_template_preview(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    _require_inspection_preview_stage(db, org_id=p.org_id, property_id=property_id, action="preview inspection template")
    from ..services.inspection_template_service import build_inspection_template
    payload = build_inspection_template(db, org_id=p.org_id, property_id=property_id)
    if isinstance(payload, dict):
        payload.setdefault("pdf_dataset_status", _step7_pdf_dataset_status())
    return payload


@router.get("/property/{property_id}/task-preview", response_model=dict)
def inspection_task_preview(
    property_id: int,
    inspection_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    _require_inspection_preview_stage(db, org_id=p.org_id, property_id=property_id, action="preview inspection tasks")
    from ..services.inspection_failure_task_service import collect_failure_task_blueprints
    payload = collect_failure_task_blueprints(db, org_id=p.org_id, property_id=property_id, inspection_id=inspection_id)
    if isinstance(payload, dict):
        payload.setdefault("pdf_dataset_status", _step7_pdf_dataset_status())
    return payload


@router.get("/property/{property_id}/operational-readiness", response_model=dict)
def inspection_operational_readiness(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property_for_inspection(db, org_id=p.org_id, property_id=property_id)
    _require_inspection_preview_stage(db, org_id=p.org_id, property_id=property_id, action="view operational readiness")
    payload = build_property_readiness_summary(db, org_id=p.org_id, property_id=property_id)
    if isinstance(payload, dict):
        payload.setdefault("pdf_dataset_status", _step7_pdf_dataset_status())
    return payload
