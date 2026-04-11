# backend/app/routers/compliance.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..domain.compliance.compliance_completion import compute_compliance_status
from ..models import (
    AppUser,
    AuditEvent,
    ChecklistTemplateItem,
    Inspection,
    Property,
    PropertyChecklist,
    PropertyChecklistItem,
    WorkflowEvent,
)
from ..schemas import (
    ChecklistItemOut,
    ChecklistItemUpdateIn,
    ChecklistOut,
    ChecklistTemplateItemOut,
    ChecklistTemplateItemUpsert,
    PropertyChecklistOut,
)
from ..services.compliance_document_service import (
    create_compliance_document_from_upload,
    delete_compliance_document,
    get_compliance_document,
    list_compliance_documents,
)
from ..services.compliance_service import (
    apply_inspection_form_results,
    build_property_document_stack_snapshot,
    build_property_inspection_readiness,
    generate_policy_tasks_for_property,
    preview_property_inspection_template,
    run_hqs as run_hqs_service,
)
from ..services.inspection_failure_task_service import (
    build_failure_next_actions,
    create_failure_tasks_from_inspection,
)
from ..services.inspection_readiness_service import build_property_readiness_summary
from ..services.inspection_scheduling_service import (
    build_inspection_timeline,
    build_property_schedule_summary,
)
from ..services.inspector_communication_service import build_inspector_contact_payload
from ..services.jurisdiction_notification_service import (
    build_impacted_property_notifications,
    notify_impacted_properties_for_rule_change,
)
from ..services.jurisdiction_profile_service import resolve_operational_policy
from ..services.policy_projection_service import (
    build_property_compliance_brief,
    build_property_projection_snapshot,
    rebuild_property_projection,
    sync_document_evidence_for_property,
)
from ..services.property_state_machine import sync_property_state
from ..services.stage_guard import require_stage
from ..services.workflow_gate_service import build_workflow_summary

router = APIRouter(prefix="/compliance", tags=["compliance"])


_SECTION8_TEMPLATE: list[dict[str, Any]] = [
    {
        "category": "Electrical",
        "item_code": "GFCI",
        "description": "GFCI protection near sinks / wet areas",
        "severity": 3,
        "common_fail": True,
    },
    {
        "category": "Electrical",
        "item_code": "OUTLET_COVERS",
        "description": "Missing/broken outlet/switch covers",
        "severity": 2,
        "common_fail": True,
    },
    {
        "category": "Safety",
        "item_code": "SMOKE_CO_DETECTORS",
        "description": "Smoke/CO detectors installed and working",
        "severity": 4,
        "common_fail": True,
    },
    {
        "category": "Safety",
        "item_code": "HANDRAILS",
        "description": "Handrails on stairs / steps where required",
        "severity": 3,
        "common_fail": True,
    },
    {
        "category": "Exterior",
        "item_code": "BROKEN_WINDOWS",
        "description": "No broken/cracked windows; lockable and weather-tight",
        "severity": 3,
        "common_fail": True,
    },
    {
        "category": "Interior",
        "item_code": "TRIP_HAZARDS",
        "description": "No trip hazards (loose flooring, torn carpet, bad transitions)",
        "severity": 3,
        "common_fail": True,
    },
    {
        "category": "Plumbing",
        "item_code": "LEAKS",
        "description": "No active plumbing leaks; fixtures secure",
        "severity": 3,
        "common_fail": True,
    },
    {
        "category": "HVAC",
        "item_code": "HEAT_WORKS",
        "description": "Permanent heat source operational",
        "severity": 4,
        "common_fail": True,
    },
    {
        "category": "Lead Paint",
        "item_code": "LEAD_PAINT_FLAGS",
        "description": "Potential lead paint hazards (pre-1978): peeling/chipping paint",
        "severity": 5,
        "common_fail": True,
        "applies_if": {"year_built_lt": 1978},
    },
    {
        "category": "Garage",
        "item_code": "GARAGE_DOOR_SAFE",
        "description": "Garage door operates safely; no unsafe springs/rails",
        "severity": 2,
        "common_fail": False,
        "applies_if": {"has_garage": True},
    },
]

_ALLOWED_STATUS = {"todo", "in_progress", "done", "blocked", "failed"}


def _must_get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.id == property_id,
            Property.org_id == org_id,
        )
    )
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")
    return prop


def _must_get_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int,
) -> Inspection:
    row = db.scalar(
        select(Inspection).where(
            Inspection.id == inspection_id,
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="inspection not found for property")
    return row


def _applies(
    cond: dict[str, Any] | None,
    *,
    year_built: int | None,
    has_garage: bool,
    property_type: str | None,
) -> bool:
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
        if isinstance(allowed, list) and (property_type or "") not in allowed:
            return False
    return True


def _items_from_templates(
    prop: Property,
    tmpl_rows: list[ChecklistTemplateItem],
) -> list[ChecklistItemOut]:
    items: list[ChecklistItemOut] = []
    for t in tmpl_rows:
        cond = None
        if t.applies_if_json:
            try:
                cond = json.loads(t.applies_if_json)
            except Exception:
                cond = None
        if not _applies(
            cond,
            year_built=prop.year_built,
            has_garage=prop.has_garage,
            property_type=prop.property_type,
        ):
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
        if not _applies(
            cond,
            year_built=prop.year_built,
            has_garage=prop.has_garage,
            property_type=prop.property_type,
        ):
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


def _items_from_policy_brief(brief: dict[str, Any]) -> list[ChecklistItemOut]:
    items: list[ChecklistItemOut] = []
    required_actions = brief.get("required_actions") or []
    blocking_items = brief.get("blocking_items") or []

    for raw in required_actions:
        code = str(
            raw.get("code")
            or raw.get("rule_key")
            or raw.get("title")
            or "POLICY_ACTION"
        ).upper().replace(" ", "_")
        items.append(
            ChecklistItemOut(
                category=str(raw.get("category") or "Policy"),
                item_code=code,
                description=str(raw.get("title") or raw.get("description") or code),
                severity=int(raw.get("severity") or 3),
                common_fail=bool(raw.get("common_fail", True)),
                applies_if=raw.get("applies_if"),
                status="todo",
            )
        )

    for raw in blocking_items:
        code = str(
            raw.get("code")
            or raw.get("rule_key")
            or raw.get("title")
            or "POLICY_BLOCKER"
        ).upper().replace(" ", "_")
        if any(x.item_code == code for x in items):
            continue
        items.append(
            ChecklistItemOut(
                category=str(raw.get("category") or "Policy Blocker"),
                item_code=code,
                description=str(raw.get("title") or raw.get("description") or code),
                severity=int(raw.get("severity") or 5),
                common_fail=True,
                applies_if=raw.get("applies_if"),
                status="todo",
            )
        )

    return items


def _dedupe_items(items: list[ChecklistItemOut]) -> list[ChecklistItemOut]:
    seen: dict[str, ChecklistItemOut] = {}
    for item in items:
        code = item.item_code.strip().upper()
        item.item_code = code
        if code not in seen:
            seen[code] = item
            continue
        existing = seen[code]
        if int(item.severity) > int(existing.severity):
            seen[code] = item
    return sorted(
        seen.values(),
        key=lambda x: (str(x.category or "").lower(), str(x.item_code or "").lower()),
    )


def _merge_state(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    items: list[ChecklistItemOut],
) -> list[ChecklistItemOut]:
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


def _summarize_status(items: list[PropertyChecklistItem]) -> dict[str, Any]:
    total = len(items)
    counts = {s: 0 for s in _ALLOWED_STATUS}
    for x in items:
        s = (x.status or "todo").strip().lower()
        if s not in _ALLOWED_STATUS:
            s = "todo"
        counts[s] += 1

    done = counts["done"]
    failed = counts["failed"]
    blocked = counts["blocked"]
    in_progress = counts["in_progress"]
    todo = counts["todo"]
    pct_done = (done / total) if total else 0.0

    return {
        "total": total,
        "done": done,
        "failed": failed,
        "blocked": blocked,
        "in_progress": in_progress,
        "todo": todo,
        "pct_done": round(pct_done, 4),
    }


def _is_template_version_locked(
    db: Session,
    *,
    org_id: int,
    strategy: str,
    version: str,
) -> bool:
    if (version or "").strip().lower() != "v1":
        return False
    insp_id = db.scalar(
        select(Inspection.id).where(Inspection.org_id == org_id).limit(1)
    )
    return insp_id is not None


def _inspection_row_payload(row: Inspection) -> dict[str, Any]:
    return {
        "id": row.id,
        "property_id": row.property_id,
        "inspection_date": row.inspection_date.isoformat() if row.inspection_date else None,
        "passed": row.passed,
        "reinspect_required": row.reinspect_required,
        "notes": row.notes,
        "inspector": getattr(row, "inspector", None),
        "jurisdiction": getattr(row, "jurisdiction", None),
        "template_key": getattr(row, "template_key", None),
        "template_version": getattr(row, "template_version", None),
        "result_status": getattr(row, "result_status", None),
        "readiness_score": getattr(row, "readiness_score", None),
        "readiness_status": getattr(row, "readiness_status", None),
        "submitted_at": getattr(row, "submitted_at", None),
        "completed_at": getattr(row, "completed_at", None),
    }


def _history_rows_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(Inspection)
        .where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(limit)
    ).all()
    return [_inspection_row_payload(r) for r in rows]


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        return str(value)
    except Exception:
        return None


def _serialize_doc(row: Any) -> dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "property_id": getattr(row, "property_id", None),
        "inspection_id": getattr(row, "inspection_id", None),
        "checklist_item_id": getattr(row, "checklist_item_id", None),
        "category": getattr(row, "category", None),
        "label": getattr(row, "label", None),
        "original_filename": getattr(row, "original_filename", None),
        "storage_key": getattr(row, "storage_key", None),
        "content_type": getattr(row, "content_type", None),
        "size_bytes": getattr(row, "size_bytes", None),
        "scan_status": getattr(row, "scan_status", None),
        "parse_status": getattr(row, "parse_status", None),
        "extracted_text_preview": getattr(row, "extracted_text_preview", None),
        "metadata_json": json.loads(getattr(row, "metadata_json", None) or "{}"),
        "parser_meta_json": json.loads(getattr(row, "parser_meta_json", None) or "{}"),
        "created_at": _iso(getattr(row, "created_at", None)),
        "updated_at": _iso(getattr(row, "updated_at", None)),
    }


@router.get("/templates", response_model=list[ChecklistTemplateItemOut])
def list_templates(
    strategy: str = Query(default="section8"),
    version: str = Query(default="v1"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    rows = db.scalars(
        select(ChecklistTemplateItem)
        .where(
            ChecklistTemplateItem.strategy == strategy,
            ChecklistTemplateItem.version == version,
        )
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()
    return rows


@router.put("/templates", response_model=ChecklistTemplateItemOut)
def upsert_template(
    payload: ChecklistTemplateItemUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    require_owner(p)
    if _is_template_version_locked(
        db,
        org_id=p.org_id,
        strategy=payload.strategy,
        version=payload.version,
    ):
        raise HTTPException(
            status_code=409,
            detail="Template version v1 is locked because inspections exist. Create version v2 instead.",
        )

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
    include_policy: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="generate compliance checklist",
    )

    tmpl = db.scalars(
        select(ChecklistTemplateItem)
        .where(
            ChecklistTemplateItem.strategy == strategy,
            ChecklistTemplateItem.version == version,
        )
        .order_by(ChecklistTemplateItem.category, ChecklistTemplateItem.code)
    ).all()

    base_items = _items_from_templates(prop, tmpl) if tmpl else _items_from_fallback(prop)

    policy_items: list[ChecklistItemOut] = []
    jurisdiction: dict[str, Any] = {}
    if include_policy:
        jurisdiction = resolve_operational_policy(
            db,
            org_id=p.org_id,
            city=prop.city,
            county=getattr(prop, "county", None),
            state=prop.state or "MI",
        )
        policy_items = _items_from_policy_brief(jurisdiction)

    items = _dedupe_items(base_items + policy_items)
    out = ChecklistOut(
        property_id=property_id,
        strategy=strategy,
        city=prop.city,
        state=prop.state,
        items=items,
    )

    if persist:
        now = datetime.utcnow()
        row = db.scalar(
            select(PropertyChecklist).where(
                PropertyChecklist.org_id == p.org_id,
                PropertyChecklist.property_id == property_id,
                PropertyChecklist.strategy == strategy,
                PropertyChecklist.version == version,
            )
        )
        serialized = [i.model_dump() for i in items]
        if not row:
            row = PropertyChecklist(
                org_id=p.org_id,
                property_id=property_id,
                strategy=strategy,
                version=version,
                generated_at=out.generated_at,
                items_json=json.dumps(serialized, default=str),
            )
            db.add(row)
            db.flush()
        else:
            row.generated_at = out.generated_at
            row.items_json = json.dumps(serialized, default=str)
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

        db.add(
            WorkflowEvent(
                org_id=p.org_id,
                property_id=property_id,
                actor_user_id=p.user_id,
                event_type="compliance.checklist_generated",
                payload_json=json.dumps(
                    {
                        "strategy": strategy,
                        "version": version,
                        "include_policy": include_policy,
                        "jurisdiction_profile_id": jurisdiction.get("profile_id"),
                        "policy_required_actions": len(jurisdiction.get("required_actions") or []),
                        "policy_blocking_items": len(jurisdiction.get("blocking_items") or []),
                    }
                ),
                created_at=now,
            )
        )
        sync_property_state(db, org_id=p.org_id, property_id=property_id)
        db.commit()

    out.items = _merge_state(
        db,
        org_id=p.org_id,
        property_id=property_id,
        items=out.items,
    )
    return out


@router.get("/checklist/{property_id}/latest", response_model=PropertyChecklistOut)
def get_latest_checklist(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view compliance checklist",
    )

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


@router.get("/property/{property_id}/brief", response_model=dict)
def property_brief(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view property compliance brief",
    )
    return build_property_compliance_brief(
        db,
        org_id=p.org_id,
        state=getattr(prop, "state", None) or "MI",
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
        pha_name=None,
        property_id=int(property_id),
        property=prop,
    )


@router.get("/property/{property_id}/inspection-readiness", response_model=dict)
def property_inspection_readiness(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view property inspection readiness",
    )
    readiness = build_property_inspection_readiness(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    history = _history_rows_for_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        limit=25,
    )
    readiness["inspection_history"] = history
    return readiness


@router.get("/property/{property_id}/record", response_model=dict)
def property_compliance_record(
    property_id: int,
    inspection_limit: int = Query(default=25, ge=1, le=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view property compliance record",
    )

    latest_checklist = db.scalar(
        select(PropertyChecklist)
        .where(
            PropertyChecklist.org_id == p.org_id,
            PropertyChecklist.property_id == property_id,
        )
        .order_by(desc(PropertyChecklist.id))
        .limit(1)
    )
    checklist_items = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()
    checklist_summary = _summarize_status(checklist_items)

    readiness = build_property_inspection_readiness(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    completion = compute_compliance_status(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    readiness_summary = build_property_readiness_summary(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    inspection_template = preview_property_inspection_template(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    history = _history_rows_for_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        limit=inspection_limit,
    )
    latest_inspection = history[0] if history else None
    jurisdiction = resolve_operational_policy(
        db,
        org_id=p.org_id,
        city=prop.city,
        county=getattr(prop, "county", None),
        state=prop.state or "MI",
    )

    return {
        "ok": True,
        "property": {
            "id": prop.id,
            "address": getattr(prop, "address", None),
            "city": prop.city,
            "county": getattr(prop, "county", None),
            "state": prop.state or "MI",
            "property_type": getattr(prop, "property_type", None),
            "year_built": getattr(prop, "year_built", None),
            "has_garage": bool(getattr(prop, "has_garage", False)),
        },
        "checklist": {
            "latest_id": latest_checklist.id if latest_checklist else None,
            "strategy": latest_checklist.strategy if latest_checklist else None,
            "version": latest_checklist.version if latest_checklist else None,
            "generated_at": latest_checklist.generated_at.isoformat() if latest_checklist and latest_checklist.generated_at else None,
            "summary": checklist_summary,
            "items": [
                {
                    "id": row.id,
                    "item_code": row.item_code,
                    "category": row.category,
                    "description": row.description,
                    "severity": row.severity,
                    "common_fail": row.common_fail,
                    "status": row.status,
                    "notes": row.notes,
                    "proof_url": row.proof_url,
                    "marked_at": row.marked_at.isoformat() if row.marked_at else None,
                }
                for row in checklist_items
            ],
        },
        "readiness": readiness,
        "readiness_summary": readiness_summary,
        "completion": completion,
        "template_preview": inspection_template,
        "jurisdiction": jurisdiction,
        "latest_inspection": latest_inspection,
        "inspection_history": history,
    }


@router.post("/checklist/{property_id}/items/{item_code}", response_model=dict)
def update_checklist_item(
    property_id: int,
    item_code: str,
    payload: ChecklistItemUpdateIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="update compliance checklist item",
    )

    row = db.scalar(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
            PropertyChecklistItem.item_code == item_code.upper(),
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="checklist item not found")

    if payload.status is not None:
        status = payload.status.strip().lower()
        if status not in _ALLOWED_STATUS:
            raise HTTPException(status_code=400, detail="invalid checklist status")
        row.status = status
    if payload.notes is not None:
        row.notes = payload.notes
    if payload.proof_url is not None:
        row.proof_url = payload.proof_url

    row.marked_at = datetime.utcnow()
    row.marked_by_user_id = p.user_id
    row.updated_at = datetime.utcnow()
    db.add(row)

    db.add(
        WorkflowEvent(
            org_id=p.org_id,
            property_id=property_id,
            actor_user_id=p.user_id,
            event_type="compliance.checklist_item_updated",
            payload_json=json.dumps(
                {
                    "item_code": row.item_code,
                    "status": row.status,
                    "notes": row.notes,
                    "proof_url": row.proof_url,
                }
            ),
            created_at=datetime.utcnow(),
        )
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()

    return {
        "ok": True,
        "item_code": row.item_code,
        "status": row.status,
        "marked_at": row.marked_at.isoformat() if row.marked_at else None,
    }


@router.get("/properties/{property_id}/brief", response_model=dict)
def get_property_compliance_brief_v2(
    property_id: int,
    rebuild_projection: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    if rebuild_projection:
        rebuild_property_projection(
            db,
            org_id=principal.org_id,
            property_id=int(property_id),
            property=prop,
        )

    brief = build_property_compliance_brief(
        db,
        org_id=principal.org_id,
        state=getattr(prop, "state", None),
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
        pha_name=getattr(prop, "program_type", None),
        property_id=int(property_id),
        property=prop,
    )
    workflow = build_workflow_summary(
        db,
        org_id=principal.org_id,
        property_id=int(property_id),
        principal=principal,
        recompute=False,
    )
    docs = build_property_document_stack_snapshot(
        db,
        org_id=principal.org_id,
        property_id=int(property_id),
    )

    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
            "property_type": getattr(prop, "property_type", None),
            "program_type": getattr(prop, "program_type", None),
            "current_stage": getattr(prop, "current_stage", None),
            "current_pane": getattr(prop, "current_pane", None),
        },
        "brief": brief,
        "workflow": workflow,
        "documents": docs,
    }


@router.get("/properties/{property_id}/projection", response_model=dict)
def get_property_compliance_projection(
    property_id: int,
    rebuild: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=principal.org_id, property_id=property_id)

    snapshot = (
        rebuild_property_projection(
            db,
            org_id=principal.org_id,
            property_id=int(property_id),
            property=prop,
        )
        if rebuild
        else build_property_projection_snapshot(
            db,
            org_id=principal.org_id,
            property_id=int(property_id),
        )
    )

    return {
        "ok": True,
        "property": {
            "id": int(prop.id),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
        },
        **snapshot,
    }


@router.get("/properties/{property_id}/workflow", response_model=dict)
def get_property_compliance_workflow(
    property_id: int,
    recompute: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    summary = build_workflow_summary(
        db,
        org_id=principal.org_id,
        property_id=int(property_id),
        principal=principal,
        recompute=recompute,
    )
    return {
        "ok": True,
        "workflow": summary,
    }


@router.get("/properties/{property_id}/documents", response_model=dict)
def get_property_compliance_documents(
    property_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    rows = list_compliance_documents(
        db,
        org_id=principal.org_id,
        property_id=int(property_id),
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "documents": [_serialize_doc(row) for row in rows],
    }


@router.get("/properties/{property_id}/document-stack", response_model=dict)
def get_property_document_stack(
    property_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    snapshot = build_property_document_stack_snapshot(
        db,
        org_id=principal.org_id,
        property_id=int(property_id),
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "documents": snapshot,
    }


@router.post("/properties/{property_id}/documents/upload", response_model=dict)
async def upload_property_document(
    property_id: int,
    file: UploadFile = File(...),
    category: str = Form(...),
    label: str | None = Form(default=None),
    inspection_id: int | None = Form(default=None),
    checklist_item_id: int | None = Form(default=None),
    notes: str | None = Form(default=None),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    created = await create_compliance_document_from_upload(
        db,
        org_id=int(principal.org_id),
        actor_user_id=int(principal.user_id),
        property_id=int(property_id),
        upload=file,
        category=category,
        label=label,
        inspection_id=inspection_id,
        checklist_item_id=checklist_item_id,
        notes=notes,
    )
    sync_document_evidence_for_property(
        db,
        org_id=int(principal.org_id),
        property_id=int(property_id),
        document_id=int(created.id),
    )
    return {
        "ok": True,
        "document": _serialize_doc(created),
    }


@router.get("/properties/{property_id}/documents/{document_id}", response_model=dict)
def get_property_document(
    property_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    row = get_compliance_document(
        db,
        org_id=int(principal.org_id),
        document_id=int(document_id),
    )
    if row is None or int(getattr(row, "property_id", 0) or 0) != int(property_id):
        raise HTTPException(status_code=404, detail="document not found")
    return {
        "ok": True,
        "document": _serialize_doc(row),
    }


@router.delete("/properties/{property_id}/documents/{document_id}", response_model=dict)
def remove_property_document(
    property_id: int,
    document_id: int,
    hard_delete_file: bool = Query(default=False),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    _must_get_property(db, org_id=principal.org_id, property_id=property_id)
    ok = delete_compliance_document(
        db,
        org_id=int(principal.org_id),
        actor_user_id=int(principal.user_id),
        document_id=int(document_id),
        hard_delete_file=bool(hard_delete_file),
    )
    return {
        "ok": bool(ok),
        "document_id": int(document_id),
    }


@router.get("/notifications/impacted-properties", response_model=dict)
def preview_impacted_property_notifications(
    jurisdiction_slug: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    payload = build_impacted_property_notifications(
        db,
        org_id=principal.org_id,
        jurisdiction_slug=jurisdiction_slug,
        changed_rules=[],
        trigger_payload={},
        limit=limit,
    )
    return payload


@router.post("/notifications/impacted-properties", response_model=dict)
def create_impacted_property_notifications(
    payload: dict[str, Any] = Body(default_factory=dict),
    db: Session = Depends(get_db),
    principal=Depends(require_owner),
):
    jurisdiction_slug = str(payload.get("jurisdiction_slug") or "").strip()
    if not jurisdiction_slug:
        raise HTTPException(status_code=400, detail="jurisdiction_slug_required")

    result = notify_impacted_properties_for_rule_change(
        db,
        org_id=principal.org_id,
        jurisdiction_slug=jurisdiction_slug,
        changed_rules=list(payload.get("changed_rules") or []),
        trigger_payload=dict(payload.get("trigger_payload") or {}),
        limit=int(payload.get("limit") or 200),
    )
    return result


@router.get("/queue", response_model=dict)
def get_compliance_queue(
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    props = list(
        db.scalars(
            select(Property)
            .where(Property.org_id == int(principal.org_id))
            .order_by(desc(Property.id))
            .limit(limit)
        ).all()
    )

    rows: list[dict[str, Any]] = []
    blocker_rollup: dict[str, dict[str, Any]] = {}
    stale_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []

    for prop in props:
        workflow = build_workflow_summary(
            db,
            org_id=principal.org_id,
            property_id=int(prop.id),
            principal=principal,
            recompute=False,
        )
        projection = workflow.get("compliance_projection") or {}
        compliance_gate = workflow.get("compliance_gate") or {}
        pre_close_risk = workflow.get("pre_close_risk") or {}

        blockers = [
            str(item.get("rule_key") or item.get("title") or item.get("blocker") or "")
            for item in (compliance_gate.get("blockers") or [])
            if str(item.get("rule_key") or item.get("title") or item.get("blocker") or "").strip()
        ]

        row = {
            "property_id": int(prop.id),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "state": getattr(prop, "state", None),
            "county": getattr(prop, "county", None),
            "current_stage": workflow.get("current_stage"),
            "current_stage_label": workflow.get("current_stage_label"),
            "current_pane": workflow.get("current_pane"),
            "current_pane_label": workflow.get("current_pane_label"),
            "urgency": compliance_gate.get("severity"),
            "blockers": blockers,
            "next_actions": workflow.get("next_actions") or [],
            "compliance": {
                "readiness_score": projection.get("readiness_score"),
                "result_status": projection.get("projection_status"),
                "blocked_count": projection.get("blocking_count"),
                "open_failed_items": projection.get("failing_count"),
                "reinspect_required": bool(projection.get("stale_count")),
            },
            "jurisdiction": {
                "completeness_status": (workflow.get("compliance_projection") or {}).get("projection_status"),
                "completeness_score": projection.get("confidence_score"),
                "is_stale": bool(projection.get("stale_count")),
                "gate_ok": bool(compliance_gate.get("ok", True)),
                "stale_reason": compliance_gate.get("warning_reason"),
                "coverage_confidence": projection.get("confidence_score"),
                "confidence_label": compliance_gate.get("severity"),
                "production_readiness": pre_close_risk.get("status"),
                "missing_categories": [],
                "covered_categories": [],
                "required_categories": [],
                "resolved_rule_version": projection.get("rules_version"),
                "last_refreshed_at": projection.get("last_projected_at"),
            },
        }
        rows.append(row)

        for blocker in blockers:
            bucket = blocker_rollup.setdefault(
                blocker,
                {
                    "blocker": blocker,
                    "count": 0,
                    "example_property_id": int(prop.id),
                    "example_address": getattr(prop, "address", None),
                    "example_city": getattr(prop, "city", None),
                    "urgency": compliance_gate.get("severity"),
                },
            )
            bucket["count"] = int(bucket["count"]) + 1

        if compliance_gate.get("stale_count") or workflow.get("post_close_recheck", {}).get("needed"):
            stale_rows.append(
                {
                    "property_id": int(prop.id),
                    "address": getattr(prop, "address", None),
                    "city": getattr(prop, "city", None),
                    "pane": workflow.get("current_pane"),
                    "stage": workflow.get("current_stage"),
                    "urgency": compliance_gate.get("severity"),
                    "reasons": list(compliance_gate.get("warnings") or []),
                }
            )

        action_rows.append(
            {
                "property_id": int(prop.id),
                "address": getattr(prop, "address", None),
                "city": getattr(prop, "city", None),
                "stage": workflow.get("current_stage"),
                "pane": workflow.get("current_pane"),
                "urgency": compliance_gate.get("severity"),
                "blocker": compliance_gate.get("blocked_reason"),
                "action": (workflow.get("primary_action") or {}).get("title"),
            }
        )

    return {
        "ok": True,
        "queue": rows,
        "rows": rows,
        "blockers": list(blocker_rollup.values()),
        "actions": action_rows,
        "next_actions": action_rows,
        "stale": stale_rows,
        "stale_items": stale_rows,
        "kpis": {
            "total_properties": len(rows),
            "with_blockers": sum(1 for row in rows if (row.get("compliance") or {}).get("blocked_count")),
            "stale_items": len(stale_rows),
            "critical_items": sum(1 for row in rows if row.get("urgency") == "high"),
        },
        "queue_counts": {
            "total": len(rows),
            "by_urgency": {
                "high": sum(1 for row in rows if row.get("urgency") == "high"),
                "warning": sum(1 for row in rows if row.get("urgency") == "warning"),
                "info": sum(1 for row in rows if row.get("urgency") == "info"),
            },
            "by_status": {
                "blocked": sum(1 for row in rows if ((row.get("compliance") or {}).get("result_status") or "") == "blocked"),
                "warning": sum(1 for row in rows if row.get("urgency") == "warning"),
                "ok": sum(1 for row in rows if row.get("urgency") == "info"),
            },
            "by_stage": {},
        },
        "counts": {
            "total": len(rows),
            "by_urgency": {
                "high": sum(1 for row in rows if row.get("urgency") == "high"),
                "warning": sum(1 for row in rows if row.get("urgency") == "warning"),
                "info": sum(1 for row in rows if row.get("urgency") == "info"),
            },
        },
    }


@router.get("/status/{property_id}", response_model=dict)
def property_status(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    readiness = build_property_readiness_summary(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )
    workflow = build_workflow_summary(
        db,
        org_id=p.org_id,
        property_id=property_id,
        principal=p,
        recompute=False,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "readiness": readiness,
        "workflow": workflow,
    }


@router.post("/hqs/{property_id}", response_model=dict)
def run_hqs(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="run hqs",
    )
    result = run_hqs_service(
        db,
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=p.user_id,
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    return result


@router.post("/inspections/{property_id}/{inspection_id}/apply-results", response_model=dict)
def apply_results(
    property_id: int,
    inspection_id: int,
    payload: dict[str, Any] = Body(default_factory=dict),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    _must_get_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    result = apply_inspection_form_results(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        actor_user_id=p.user_id,
        payload=payload,
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    return result


@router.post("/inspections/{property_id}/{inspection_id}/create-failure-tasks", response_model=dict)
def create_failure_tasks(
    property_id: int,
    inspection_id: int,
    payload: dict[str, Any] = Body(default_factory=dict),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    _must_get_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    result = create_failure_tasks_from_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        actor_user_id=p.user_id,
        blocking=bool(payload.get("blocking", False)),
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    return result


@router.get("/inspections/{property_id}/{inspection_id}/failure-next-actions", response_model=dict)
def failure_next_actions(
    property_id: int,
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    _must_get_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    return build_failure_next_actions(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )


@router.get("/inspections/property/{property_id}/timeline", response_model=dict)
def inspection_timeline(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return build_inspection_timeline(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )


@router.get("/inspections/property/{property_id}/schedule-summary", response_model=dict)
def inspection_schedule_summary(
    property_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    return build_property_schedule_summary(
        db,
        org_id=p.org_id,
        property_id=property_id,
    )


@router.get("/inspections/{property_id}/{inspection_id}/contact-payload", response_model=dict)
def inspection_contact_payload(
    property_id: int,
    inspection_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    _must_get_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    return build_inspector_contact_payload(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )


@router.post("/property/{property_id}/generate-policy-tasks", response_model=dict)
def generate_policy_tasks(
    property_id: int,
    payload: dict[str, Any] = Body(default_factory=dict),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = _must_get_property(db, org_id=p.org_id, property_id=property_id)
    result = generate_policy_tasks_for_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        actor_user_id=p.user_id,
        state=getattr(prop, "state", None) or "MI",
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
        pha_name=payload.get("pha_name"),
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    return result


@router.get("/audit/{property_id}", response_model=dict)
def compliance_audit(
    property_id: int,
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)

    workflow_events = list(
        db.scalars(
            select(WorkflowEvent)
            .where(
                WorkflowEvent.org_id == p.org_id,
                WorkflowEvent.property_id == property_id,
            )
            .order_by(desc(WorkflowEvent.id))
            .limit(limit)
        ).all()
    )
    audit_rows = list(
        db.scalars(
            select(AuditEvent)
            .where(AuditEvent.org_id == p.org_id)
            .order_by(desc(AuditEvent.id))
            .limit(limit)
        ).all()
    )

    return {
        "ok": True,
        "property_id": int(property_id),
        "workflow_events": [
            {
                "id": row.id,
                "event_type": row.event_type,
                "payload_json": json.loads(row.payload_json or "{}"),
                "created_at": _iso(row.created_at),
            }
            for row in workflow_events
        ],
        "audit_events": [
            {
                "id": row.id,
                "action": row.action,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "before_json": json.loads(row.before_json or "{}") if row.before_json else None,
                "after_json": json.loads(row.after_json or "{}") if row.after_json else None,
                "created_at": _iso(row.created_at),
            }
            for row in audit_rows
            if str(row.entity_id or "") == str(property_id)
            or str(row.entity_type or "").startswith("property")
            or str(row.entity_type or "").startswith("compliance")
        ],
    }