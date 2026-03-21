from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..domain.compliance.inspection_mapping import map_inspection_code
from ..domain.compliance.top_fail_points import top_fail_points
from ..models import Inspection, InspectionItem, Property, RehabTask


def _now() -> datetime:
    return datetime.utcnow()


def _j(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def _severity_rank(value: Any) -> int:
    if value is None:
        return 3
    if isinstance(value, int):
        return int(value)

    s = str(value).strip().lower()
    if s == "critical":
        return 4
    if s == "fail":
        return 3
    if s == "warn":
        return 2
    return 1


def _priority_from_item(item: InspectionItem) -> str:
    sev = _severity_rank(getattr(item, "severity", None))
    status = str(getattr(item, "result_status", "") or "").strip().lower()

    if status in {"blocked", "inconclusive"}:
        return "high"
    if sev >= 4:
        return "high"
    if sev == 3:
        return "med"
    return "low"


def _task_category_from_item(item: InspectionItem, default: str = "compliance_repair") -> str:
    category = str(getattr(item, "category", "") or "").strip().lower()
    if category:
        return category
    return default


def _task_title_from_item(item: InspectionItem) -> str:
    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)
    if mapped and mapped.rehab_title:
        return str(mapped.rehab_title).strip()

    label = (
        getattr(item, "standard_label", None)
        or getattr(item, "fail_reason", None)
        or getattr(item, "code", None)
        or "Inspection remediation"
    )
    label_text = str(label).strip()
    if not label_text:
        label_text = "Inspection remediation"

    if label_text.lower().startswith("repair") or label_text.lower().startswith("install"):
        return label_text
    return f"Fix: {label_text}"


def _task_notes_from_item(
    *,
    item: InspectionItem,
    inspection: Inspection,
    property_obj: Property | None,
) -> str:
    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)
    fail_reason = str(getattr(item, "fail_reason", "") or "").strip()
    remediation = str(getattr(item, "remediation_guidance", "") or "").strip()
    details = str(getattr(item, "details", "") or "").strip()
    location = str(getattr(item, "location", "") or "").strip()
    result_status = str(getattr(item, "result_status", "") or "").strip().lower()
    severity = getattr(item, "severity", None)

    lines = [
        "Auto-generated from inspection failure.",
        f"Inspection ID: {getattr(inspection, 'id', None)}",
        f"Inspection code: {code or 'UNKNOWN'}",
        f"Result status: {result_status or 'unknown'}",
        f"Severity: {severity}",
    ]

    if property_obj is not None:
        lines.append(
            "Property: "
            f"{getattr(property_obj, 'address', '')}, "
            f"{getattr(property_obj, 'city', '')}, "
            f"{getattr(property_obj, 'state', '')} "
            f"{getattr(property_obj, 'zip', '')}"
        )

    if location:
        lines.append(f"Location: {location}")
    if fail_reason:
        lines.append(f"Fail reason: {fail_reason}")
    if details:
        lines.append(f"Inspector details: {details}")
    if remediation:
        lines.append(f"Remediation guidance: {remediation}")
    elif mapped and mapped.default_fail_reason:
        lines.append(f"Default issue: {mapped.default_fail_reason}")
    if mapped and mapped.rehab_category:
        lines.append(f"Mapped rehab category: {mapped.rehab_category}")

    standard_label = str(getattr(item, "standard_label", "") or "").strip()
    standard_citation = str(getattr(item, "standard_citation", "") or "").strip()
    if standard_label:
        lines.append(f"Standard: {standard_label}")
    if standard_citation:
        lines.append(f"Citation: {standard_citation}")

    return "\n".join(lines).strip()


def _task_exists(db: Session, *, org_id: int, property_id: int, title: str) -> bool:
    existing = db.scalar(
        select(RehabTask).where(
            RehabTask.org_id == org_id,
            RehabTask.property_id == property_id,
            RehabTask.title == title,
        )
    )
    return existing is not None


@dataclass(frozen=True)
class FailureTaskBlueprint:
    inspection_item_id: int
    code: str
    title: str
    category: str
    priority: str
    notes: str
    inspection_relevant: bool = True
    requires_reinspection: bool = True
    rehab_category: str | None = None


def _blueprint_from_item(
    *,
    item: InspectionItem,
    inspection: Inspection,
    property_obj: Property | None,
) -> FailureTaskBlueprint | None:
    result_status = str(getattr(item, "result_status", "") or "").strip().lower()
    failed = bool(getattr(item, "failed", False))

    if result_status not in {"fail", "blocked", "inconclusive"} and not failed:
        return None

    code = str(getattr(item, "code", "") or "").strip().upper()
    mapped = map_inspection_code(code)

    return FailureTaskBlueprint(
        inspection_item_id=int(getattr(item, "id")),
        code=code,
        title=_task_title_from_item(item),
        category=(mapped.rehab_category if mapped and mapped.rehab_category else _task_category_from_item(item)),
        priority=_priority_from_item(item),
        notes=_task_notes_from_item(item=item, inspection=inspection, property_obj=property_obj),
        inspection_relevant=bool(getattr(item, "requires_reinspection", True)),
        requires_reinspection=bool(getattr(item, "requires_reinspection", True)),
        rehab_category=(mapped.rehab_category if mapped and mapped.rehab_category else None),
    )


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id, Inspection.property_id == property_id)
        .order_by(Inspection.id.desc())
        .limit(1)
    )


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property | None:
    return db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )


def _inspection_items(
    db: Session,
    *,
    inspection_id: int,
) -> list[InspectionItem]:
    return list(
        db.scalars(
            select(InspectionItem)
            .where(InspectionItem.inspection_id == inspection_id)
            .order_by(InspectionItem.id.asc())
        ).all()
    )


def collect_failure_task_blueprints(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    inspection = None
    if inspection_id is not None:
        inspection = db.scalar(
            select(Inspection).where(
                Inspection.org_id == org_id,
                Inspection.property_id == property_id,
                Inspection.id == inspection_id,
            )
        )
    else:
        inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)

    if inspection is None:
        return {
            "ok": True,
            "inspection_id": None,
            "property_id": property_id,
            "blueprints": [],
            "counts": {
                "total_items": 0,
                "failure_like_items": 0,
            },
        }

    property_obj = _get_property(db, org_id=org_id, property_id=property_id)
    items = _inspection_items(db, inspection_id=int(inspection.id))

    blueprints: list[FailureTaskBlueprint] = []
    for item in items:
        bp = _blueprint_from_item(item=item, inspection=inspection, property_obj=property_obj)
        if bp is not None:
            blueprints.append(bp)

    blueprints = sorted(
        blueprints,
        key=lambda b: (
            0 if b.priority == "high" else 1 if b.priority == "med" else 2,
            b.category,
            b.code,
            b.title,
        ),
    )

    return {
        "ok": True,
        "inspection_id": int(inspection.id),
        "property_id": property_id,
        "blueprints": blueprints,
        "counts": {
            "total_items": len(items),
            "failure_like_items": len(blueprints),
        },
    }


def create_failure_tasks_from_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    collected = collect_failure_task_blueprints(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )

    created = 0
    skipped_existing = 0
    created_titles: list[str] = []

    for bp in collected["blueprints"]:
        if _task_exists(db, org_id=org_id, property_id=property_id, title=bp.title):
            skipped_existing += 1
            continue

        row = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=bp.title,
            category=bp.category or "compliance_repair",
            inspection_relevant=bool(bp.inspection_relevant),
            status="todo",
            notes=bp.notes,
            created_at=_now(),
        )

        if hasattr(row, "updated_at"):
            row.updated_at = _now()
        if hasattr(row, "cost_estimate"):
            row.cost_estimate = None

        db.add(row)
        created += 1
        created_titles.append(bp.title)

    return {
        "ok": True,
        "inspection_id": collected["inspection_id"],
        "property_id": property_id,
        "created": created,
        "skipped_existing": skipped_existing,
        "titles": created_titles,
        "counts": collected["counts"],
    }


def build_failure_next_actions(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    collected = collect_failure_task_blueprints(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )

    fail_point_rows = [{"code": bp.code, "count": 1} for bp in collected["blueprints"]]
    ranked_fail_points = top_fail_points(fail_point_rows, limit=max(1, int(limit)))

    actions: list[dict[str, Any]] = []
    for bp in collected["blueprints"][: max(1, int(limit))]:
        actions.append(
            {
                "code": bp.code,
                "title": bp.title,
                "category": bp.category,
                "priority": bp.priority,
                "requires_reinspection": bp.requires_reinspection,
                "notes": bp.notes,
            }
        )

    return {
        "ok": True,
        "inspection_id": collected["inspection_id"],
        "property_id": property_id,
        "top_fail_points": ranked_fail_points,
        "recommended_actions": actions,
        "counts": collected["counts"],
    }