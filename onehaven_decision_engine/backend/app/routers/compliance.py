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
from ..services.compliance_document_service import (
    create_compliance_document_from_upload,
    delete_compliance_document,
    get_compliance_document,
    list_compliance_documents,
)
from ..services.jurisdiction_profile_service import resolve_operational_policy
from ..services.policy_projection_service import build_property_compliance_brief
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
            "generated_at": latest_checklist.generated_at if latest_checklist else None,
            "summary": checklist_summary,
        },
        "latest_inspection": latest_inspection,
        "inspection_history": history,
        "inspection_attempt_count": len(history),
        "inspection_template": inspection_template,
        "readiness": readiness,
        "readiness_summary": readiness_summary,
        "completion": {
            "completion_pct": completion.completion_pct,
            "completion_projection_pct": completion.completion_projection_pct,
            "failed_count": completion.failed_count,
            "blocked_count": completion.blocked_count,
            "latest_inspection_passed": completion.latest_inspection_passed,
            "latest_readiness_score": completion.latest_readiness_score,
            "latest_readiness_status": completion.latest_readiness_status,
            "latest_result_status": completion.latest_result_status,
            "posture": completion.posture,
            "is_compliant": completion.is_compliant,
            "critical_count": completion.critical_count,
            "unresolved_count": completion.unresolved_count,
            "reinspection_needed": completion.reinspection_needed,
        },
        "jurisdiction": jurisdiction,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/property/{property_id}/inspections", response_model=dict)
def property_inspection_history(
    property_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view property inspection history",
    )
    rows = _history_rows_for_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        limit=limit,
    )
    return {
        "ok": True,
        "property_id": property_id,
        "count": len(rows),
        "rows": rows,
    }


@router.get("/status/{property_id}", response_model=dict)
def compliance_status(
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
        action="view compliance status",
    )

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
    history = _history_rows_for_property(
        db,
        org_id=p.org_id,
        property_id=property_id,
        limit=25,
    )

    return {
        "property_id": property_id,
        "passed": bool(
            readiness["readiness"]["hqs_ready"]
            and readiness["readiness"]["local_ready"]
            and readiness["readiness"]["voucher_ready"]
            and readiness["readiness"]["lease_up_ready"]
        ),
        "overall_status": readiness["overall_status"],
        "score_pct": readiness["score_pct"],
        "completion_pct": readiness.get("completion_pct"),
        "completion_projection_pct": readiness.get("completion_projection_pct"),
        "posture": readiness.get("posture"),
        "readiness": readiness["readiness"],
        "counts": readiness["counts"],
        "blocking_items": readiness["blocking_items"],
        "warning_items": readiness["warning_items"],
        "recommended_actions": readiness["recommended_actions"],
        "coverage": readiness["coverage"],
        "latest_inspection": readiness.get("latest_inspection"),
        "inspection_history": history,
        "completion": {
            "completion_pct": completion.completion_pct,
            "completion_projection_pct": completion.completion_projection_pct,
            "failed_count": completion.failed_count,
            "blocked_count": completion.blocked_count,
            "latest_inspection_passed": completion.latest_inspection_passed,
            "latest_readiness_score": completion.latest_readiness_score,
            "latest_readiness_status": completion.latest_readiness_status,
            "latest_result_status": completion.latest_result_status,
            "posture": completion.posture,
            "is_compliant": completion.is_compliant,
            "critical_count": completion.critical_count,
            "unresolved_count": completion.unresolved_count,
            "reinspection_needed": completion.reinspection_needed,
        },
    }


@router.post("/run/{property_id}", response_model=dict)
def run_compliance_hqs(
    property_id: int,
    auto_create_rehab_tasks: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="run compliance HQS",
    )
    try:
        result = run_hqs_service(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            actor_email=p.email,
            property_id=property_id,
            auto_create_rehab_tasks=auto_create_rehab_tasks,
        )
        sync_property_state(db, org_id=p.org_id, property_id=property_id)
        db.commit()
        return {
            **result,
            "workflow": build_workflow_summary(
                db,
                org_id=p.org_id,
                property_id=property_id,
                recompute=True,
            ),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"run failed: {e}")


@router.post("/property/{property_id}/generate-policy-tasks", response_model=dict)
def generate_policy_tasks(
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
        action="generate policy tasks",
    )
    result = generate_policy_tasks_for_property(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        property_id=property_id,
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()
    return {
        **result,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.post("/inspection/{property_id}/{inspection_id}/apply-form", response_model=dict)
def apply_form_results(
    property_id: int,
    inspection_id: int,
    raw_payload: dict[str, Any] | list[dict[str, Any]] = Body(...),
    sync_checklist: bool = Query(default=True),
    create_failure_tasks: bool = Query(default=True),
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
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="apply inspection form results",
    )
    result = apply_inspection_form_results(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        property_id=property_id,
        inspection_id=inspection_id,
        raw_payload=raw_payload,
        sync_checklist=sync_checklist,
        create_failure_tasks=create_failure_tasks,
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()
    return {
        **result,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/inspection/{property_id}/{inspection_id}/failure-actions", response_model=dict)
def failure_actions(
    property_id: int,
    inspection_id: int,
    limit: int = Query(default=10, ge=1, le=50),
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
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view failure actions",
    )
    return build_failure_next_actions(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        limit=limit,
    )


@router.post("/inspection/{property_id}/{inspection_id}/tasks-from-failures", response_model=dict)
def tasks_from_failures(
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
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="create failure tasks",
    )
    out = create_failure_tasks_from_inspection(
        db,
        org_id=p.org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()
    return {
        **out,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/timeline/{property_id}", response_model=dict)
def compliance_timeline(
    property_id: int,
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)

    rows = db.scalars(
        select(WorkflowEvent)
        .where(
            WorkflowEvent.org_id == p.org_id,
            WorkflowEvent.property_id == property_id,
        )
        .order_by(desc(WorkflowEvent.id))
        .limit(limit)
    ).all()

    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            payload = json.loads(r.payload_json or "{}")
        except Exception:
            payload = {"raw": r.payload_json}
        out.append(
            {
                "id": r.id,
                "event_type": r.event_type,
                "created_at": r.created_at,
                "payload": payload,
                "actor_user_id": r.actor_user_id,
            }
        )
    return {"property_id": property_id, "rows": out, "count": len(out)}


@router.get("/run_hqs/{property_id}", response_model=dict)
def run_hqs_summary_only(
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
        action="view HQS summary",
    )

    items = db.scalars(
        select(PropertyChecklistItem).where(
            PropertyChecklistItem.org_id == p.org_id,
            PropertyChecklistItem.property_id == property_id,
        )
    ).all()
    total = len(items)
    passed_ct = sum(1 for x in items if (x.status or "").lower() == "done")
    failed_ct = sum(1 for x in items if (x.status or "").lower() == "failed")
    blocked_ct = sum(1 for x in items if (x.status or "").lower() == "blocked")
    not_yet = total - passed_ct - failed_ct - blocked_ct
    score_pct = round((passed_ct / total) * 100.0, 2) if total else 0.0
    fail_codes = sorted(
        [x.item_code for x in items if (x.status or "").lower() == "failed" and x.item_code]
    )
    jurisdiction = resolve_operational_policy(
        db,
        org_id=p.org_id,
        city=prop.city,
        county=getattr(prop, "county", None),
        state=prop.state or "MI",
    )

    latest_inspection = db.scalar(
        select(Inspection)
        .where(
            Inspection.org_id == p.org_id,
            Inspection.property_id == property_id,
        )
        .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
        .limit(1)
    )

    return {
        "property_id": property_id,
        "total": total,
        "passed": passed_ct,
        "failed": failed_ct,
        "blocked": blocked_ct,
        "not_yet": not_yet,
        "score_pct": score_pct,
        "fail_codes": fail_codes,
        "latest_inspection": _inspection_row_payload(latest_inspection) if latest_inspection else None,
        "jurisdiction_profile_id": jurisdiction.get("profile_id"),
        "jurisdiction_scope": jurisdiction.get("scope"),
        "jurisdiction_match_level": jurisdiction.get("match_level"),
        "jurisdiction_required_actions": len(jurisdiction.get("required_actions") or []),
        "jurisdiction_blocking_items": len(jurisdiction.get("blocking_items") or []),
    }



@router.get("/properties/{property_id}/schedule-summary", response_model=dict)
def property_schedule_summary(
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
        action="view inspection schedule summary",
    )
    summary = build_property_schedule_summary(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
    )
    return {
        **summary,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/properties/{property_id}/timeline", response_model=dict)
def property_inspection_timeline(
    property_id: int,
    limit: int = Query(default=100, ge=1, le=300),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view inspection timeline",
    )
    return {
        **build_inspection_timeline(
            db,
            org_id=int(p.org_id),
            property_id=int(property_id),
            limit=int(limit),
        ),
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/properties/{property_id}/inspector-contact", response_model=dict)
def property_inspector_contact_payload(
    property_id: int,
    inspection_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view inspector communication payload",
    )

    if inspection_id is None:
        latest = db.scalar(
            select(Inspection)
            .where(
                Inspection.org_id == p.org_id,
                Inspection.property_id == property_id,
            )
            .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
            .limit(1)
        )
        if latest is None:
            raise HTTPException(status_code=404, detail="no inspections found for property")
        inspection_id = int(latest.id)

    return {
        "ok": True,
        "property_id": int(property_id),
        "inspection_id": int(inspection_id),
        "payload": build_inspector_contact_payload(
            db,
            org_id=int(p.org_id),
            inspection_id=int(inspection_id),
        ),
    }

_ALLOWED_COMPLIANCE_DOCUMENT_CATEGORIES = {
    "inspection_report",
    "pass_certificate",
    "reinspection_notice",
    "repair_invoice",
    "utility_confirmation",
    "smoke_detector_proof",
    "lead_based_paint_paperwork",
    "local_jurisdiction_document",
    "approval_letter",
    "denial_letter",
    "photo_evidence",
    "other_evidence",
}


def _normalize_document_category(value: str | None) -> str:
    raw = str(value or "other_evidence").strip().lower().replace(" ", "_")
    return raw if raw in _ALLOWED_COMPLIANCE_DOCUMENT_CATEGORIES else "other_evidence"


@router.get("/properties/{property_id}/documents", response_model=dict)
def property_compliance_documents(
    property_id: int,
    inspection_id: int | None = Query(default=None),
    checklist_item_id: int | None = Query(default=None),
    category: str | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="view compliance documents",
    )
    rows = list_compliance_documents(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
        category=_normalize_document_category(category) if category else None,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "count": len(rows),
        "rows": rows,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.post("/properties/{property_id}/documents/upload", response_model=dict)
async def upload_property_compliance_document(
    property_id: int,
    category: str = Form(...),
    file: UploadFile = File(...),
    inspection_id: int | None = Form(default=None),
    checklist_item_id: int | None = Form(default=None),
    label: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    parse_document: bool = Form(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    _must_get_property(db, org_id=p.org_id, property_id=property_id)
    require_stage(
        db,
        org_id=p.org_id,
        property_id=property_id,
        min_stage="compliance",
        action="upload compliance document",
    )

    if inspection_id is not None:
        _must_get_inspection(
            db,
            org_id=p.org_id,
            property_id=property_id,
            inspection_id=int(inspection_id),
        )

    row = await create_compliance_document_from_upload(
        db,
        org_id=int(p.org_id),
        actor_user_id=int(p.user_id),
        property_id=int(property_id),
        category=_normalize_document_category(category),
        upload=file,
        inspection_id=int(inspection_id) if inspection_id is not None else None,
        checklist_item_id=int(checklist_item_id) if checklist_item_id is not None else None,
        label=label,
        notes=notes,
        parse_document=bool(parse_document),
    )

    sync_property_state(db, org_id=p.org_id, property_id=property_id)
    db.commit()

    return {
        "ok": True,
        "document": row,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }


@router.get("/documents/{document_id}", response_model=dict)
def get_property_compliance_document(
    document_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = get_compliance_document(db, org_id=int(p.org_id), document_id=int(document_id))
    return {"ok": True, "document": row}


@router.get("/documents/{document_id}/download")
def download_property_compliance_document(
    document_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = get_compliance_document(db, org_id=int(p.org_id), document_id=int(document_id))
    path = row.get("absolute_path")
    if not path:
        raise HTTPException(status_code=404, detail="document file not found")
    from fastapi.responses import FileResponse
    filename = row.get("original_filename") or row.get("storage_key") or f"document_{document_id}"
    return FileResponse(path, filename=filename)


@router.delete("/documents/{document_id}", response_model=dict)
def delete_property_compliance_document(
    document_id: int,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    result = delete_compliance_document(
        db,
        org_id=int(p.org_id),
        actor_user_id=int(p.user_id),
        document_id=int(document_id),
    )
    db.commit()
    return {"ok": True, **result}


@router.get("/properties/{property_id}/document-stack", response_model=dict)
def property_compliance_document_stack(
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
        action="view compliance document stack",
    )
    out = build_property_document_stack_snapshot(
        db,
        org_id=int(p.org_id),
        property_id=int(property_id),
    )
    return {
        **out,
        "workflow": build_workflow_summary(
            db,
            org_id=p.org_id,
            property_id=property_id,
            recompute=True,
        ),
    }
