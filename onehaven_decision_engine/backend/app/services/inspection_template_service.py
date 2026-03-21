from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..domain.compliance.checklist_templates import (
    ChecklistTemplateItem,
    template_items_from_effective_rules,
    template_lookup,
)
from ..domain.compliance.hqs_library import get_effective_hqs_items
from ..domain.compliance.inspection_mapping import map_raw_form_answers
from ..domain.compliance.inspection_rules import score_readiness
from ..models import (
    Inspection,
    InspectionItem,
    Property,
    PropertyChecklist,
    PropertyChecklistItem,
)
from .jurisdiction_profile_service import resolve_operational_policy


def _now() -> datetime:
    return datetime.utcnow()


def _j(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def _severity_to_int(severity: str | None) -> int:
    s = str(severity or "").strip().lower()
    if s == "critical":
        return 4
    if s == "fail":
        return 3
    if s == "warn":
        return 2
    return 1


def _checklist_status_from_result(result_status: str) -> str:
    s = str(result_status or "").strip().lower()
    if s == "pass":
        return "done"
    if s == "fail":
        return "failed"
    if s in {"blocked", "inconclusive"}:
        return "blocked"
    if s == "not_applicable":
        return "done"
    return "todo"


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )
    if not prop:
        raise ValueError("property not found")
    return prop


def _find_property_checklist(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: str,
    version: str,
) -> PropertyChecklist | None:
    return db.scalar(
        select(PropertyChecklist).where(
            PropertyChecklist.org_id == org_id,
            PropertyChecklist.property_id == property_id,
            PropertyChecklist.strategy == strategy,
            PropertyChecklist.version == version,
        )
    )


def _find_checklist_items(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> list[PropertyChecklistItem]:
    return list(
        db.scalars(
            select(PropertyChecklistItem)
            .where(
                PropertyChecklistItem.org_id == org_id,
                PropertyChecklistItem.property_id == property_id,
            )
            .order_by(PropertyChecklistItem.id.asc())
        ).all()
    )


def _find_inspection_items(
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


def build_inspection_template(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prop = _get_property(db, org_id=org_id, property_id=property_id)

    if profile_summary is None:
        profile_summary = resolve_operational_policy(
            db,
            org_id=org_id,
            state=getattr(prop, "state", None) or "MI",
            county=getattr(prop, "county", None),
            city=getattr(prop, "city", None),
        )

    effective = get_effective_hqs_items(
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=profile_summary or {},
    )

    template_items = template_items_from_effective_rules(effective.get("items") or [])
    if not template_items:
        template_items = []

    template_key = template_items[0].template_key if template_items else "hud_52580a"
    template_version = template_items[0].template_version if template_items else "hud_52580a_2019"

    items = [
        {
            "code": item.code,
            "description": item.description,
            "category": item.category,
            "default_status": item.default_status,
            "severity": item.severity,
            "severity_int": _severity_to_int(item.severity),
            "common_fail": item.common_fail,
            "inspection_rule_code": item.inspection_rule_code,
            "suggested_fix": item.suggested_fix,
            "template_key": item.template_key,
            "template_version": item.template_version,
            "section": item.section,
            "item_number": item.item_number,
            "room_scope": item.room_scope,
            "not_applicable_allowed": item.not_applicable_allowed,
        }
        for item in template_items
    ]

    return {
        "ok": True,
        "template_key": template_key,
        "template_version": template_version,
        "property_id": property_id,
        "profile_summary": profile_summary or {},
        "items": items,
        "sources": effective.get("sources") or [],
        "counts": {
            "total": len(items),
            **(effective.get("counts") or {}),
        },
    }


def ensure_template_backed_checklist(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    template = build_inspection_template(
        db,
        org_id=org_id,
        property_id=property_id,
        profile_summary=profile_summary,
    )

    strategy = str(template["template_key"])
    version = str(template["template_version"])
    now = _now()

    checklist = _find_property_checklist(
        db,
        org_id=org_id,
        property_id=property_id,
        strategy=strategy,
        version=version,
    )

    created_checklist = False
    if checklist is None:
        checklist = PropertyChecklist(
            org_id=org_id,
            property_id=property_id,
            strategy=strategy,
            version=version,
            generated_at=now,
            items_json=_j(template["items"]),
        )
        db.add(checklist)
        db.flush()
        created_checklist = True
    else:
        checklist.items_json = _j(template["items"])

    existing_rows = _find_checklist_items(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    by_code = {str(r.item_code or "").strip().upper(): r for r in existing_rows}

    created_items = 0
    updated_items = 0

    for item in template["items"]:
        code = str(item["code"]).strip().upper()
        row = by_code.get(code)
        if row is None:
            row = PropertyChecklistItem(
                org_id=org_id,
                property_id=property_id,
                checklist_id=checklist.id,
                item_code=code,
                category=str(item["category"]),
                description=str(item["description"]),
                severity=int(item["severity_int"]),
                common_fail=bool(item["common_fail"]),
                applies_if_json=_j(
                    {
                        "template_key": item["template_key"],
                        "template_version": item["template_version"],
                        "section": item.get("section"),
                        "item_number": item.get("item_number"),
                        "room_scope": item.get("room_scope"),
                        "inspection_rule_code": item.get("inspection_rule_code"),
                        "not_applicable_allowed": item.get("not_applicable_allowed", False),
                    }
                ),
                status=str(item.get("default_status") or "todo"),
                notes=item.get("suggested_fix"),
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            created_items += 1
        else:
            changed = False
            if row.checklist_id != checklist.id:
                row.checklist_id = checklist.id
                changed = True
            if row.category != str(item["category"]):
                row.category = str(item["category"])
                changed = True
            if row.description != str(item["description"]):
                row.description = str(item["description"])
                changed = True
            desired_severity = int(item["severity_int"])
            if int(row.severity or 0) != desired_severity:
                row.severity = desired_severity
                changed = True
            desired_common_fail = bool(item["common_fail"])
            if bool(row.common_fail) != desired_common_fail:
                row.common_fail = desired_common_fail
                changed = True

            desired_applies = _j(
                {
                    "template_key": item["template_key"],
                    "template_version": item["template_version"],
                    "section": item.get("section"),
                    "item_number": item.get("item_number"),
                    "room_scope": item.get("room_scope"),
                    "inspection_rule_code": item.get("inspection_rule_code"),
                    "not_applicable_allowed": item.get("not_applicable_allowed", False),
                }
            )
            if (row.applies_if_json or "") != desired_applies:
                row.applies_if_json = desired_applies
                changed = True

            if changed:
                row.updated_at = now
                updated_items += 1

    return {
        "ok": True,
        "template_key": strategy,
        "template_version": version,
        "created_checklist": created_checklist,
        "created_items": created_items,
        "updated_items": updated_items,
        "checklist_id": checklist.id,
        "template": template,
    }


def map_raw_inspection_payload(
    *,
    raw_payload: dict[str, Any] | list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    return map_raw_form_answers(raw_payload)


def apply_raw_inspection_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int,
    raw_payload: dict[str, Any] | list[dict[str, Any]] | None,
    sync_checklist: bool = True,
) -> dict[str, Any]:
    template_info = ensure_template_backed_checklist(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    template_rows = template_info["template"]["items"]
    lookup = template_lookup(
        [
            ChecklistTemplateItem(
                code=str(r["code"]),
                description=str(r["description"]),
                category=str(r["category"]),
                default_status=str(r.get("default_status") or "todo"),
                severity=str(r["severity"]),
                common_fail=bool(r["common_fail"]),
                inspection_rule_code=r.get("inspection_rule_code"),
                suggested_fix=r.get("suggested_fix"),
                template_key=str(r["template_key"]),
                template_version=str(r["template_version"]),
                section=r.get("section"),
                item_number=r.get("item_number"),
                room_scope=r.get("room_scope"),
                not_applicable_allowed=bool(r.get("not_applicable_allowed", False)),
            )
            for r in template_rows
        ]
    )

    mapped_rows = map_raw_inspection_payload(raw_payload=raw_payload)

    inspection = db.scalar(
        select(Inspection).where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
            Inspection.id == inspection_id,
        )
    )
    if inspection is None:
        raise ValueError("inspection not found")

    existing_items = _find_inspection_items(db, inspection_id=inspection_id)
    existing_by_code = {str(r.code or "").strip().upper(): r for r in existing_items}

    checklist_rows = _find_checklist_items(db, org_id=org_id, property_id=property_id)
    checklist_by_code = {str(r.item_code or "").strip().upper(): r for r in checklist_rows}

    now = _now()
    created = 0
    updated = 0

    for row in mapped_rows:
        code = str(row["code"]).strip().upper()
        template_item = lookup.get(code)
        if template_item is None:
            continue

        item = existing_by_code.get(code)
        if item is None:
            item = InspectionItem(
                inspection_id=inspection_id,
                code=code,
                failed=bool(row["failed"]),
                severity=int(row["severity_int"]),
                location=row.get("location"),
                details=row.get("details"),
                category=row.get("category"),
                result_status=row.get("result_status"),
                fail_reason=row.get("fail_reason"),
                remediation_guidance=row.get("remediation_guidance"),
                evidence_json=row.get("evidence_json") or "[]",
                photo_references_json=row.get("photo_references_json") or "[]",
                standard_label=row.get("standard_label"),
                standard_citation=row.get("standard_citation"),
                readiness_impact=float(row.get("readiness_impact") or 0.0),
                requires_reinspection=bool(row.get("requires_reinspection", False)),
                created_at=now,
            )
            db.add(item)
            created += 1
        else:
            item.failed = bool(row["failed"])
            item.severity = int(row["severity_int"])
            item.location = row.get("location")
            item.details = row.get("details")
            if hasattr(item, "category"):
                item.category = row.get("category")
            if hasattr(item, "result_status"):
                item.result_status = row.get("result_status")
            if hasattr(item, "fail_reason"):
                item.fail_reason = row.get("fail_reason")
            if hasattr(item, "remediation_guidance"):
                item.remediation_guidance = row.get("remediation_guidance")
            if hasattr(item, "evidence_json"):
                item.evidence_json = row.get("evidence_json") or "[]"
            if hasattr(item, "photo_references_json"):
                item.photo_references_json = row.get("photo_references_json") or "[]"
            if hasattr(item, "standard_label"):
                item.standard_label = row.get("standard_label")
            if hasattr(item, "standard_citation"):
                item.standard_citation = row.get("standard_citation")
            if hasattr(item, "readiness_impact"):
                item.readiness_impact = float(row.get("readiness_impact") or 0.0)
            if hasattr(item, "requires_reinspection"):
                item.requires_reinspection = bool(row.get("requires_reinspection", False))
            updated += 1

        if sync_checklist:
            checklist_row = checklist_by_code.get(code)
            if checklist_row is not None:
                checklist_row.status = _checklist_status_from_result(str(row["result_status"]))
                checklist_row.notes = row.get("fail_reason") or row.get("remediation_guidance") or checklist_row.notes
                checklist_row.updated_at = now

    db.flush()

    final_items = _find_inspection_items(db, inspection_id=inspection_id)
    readiness = score_readiness(final_items)

    inspection.template_key = template_info["template_key"]
    inspection.template_version = template_info["template_version"]
    if hasattr(inspection, "inspection_status"):
        inspection.inspection_status = "completed"
    if hasattr(inspection, "result_status"):
        inspection.result_status = readiness.result_status
    if hasattr(inspection, "readiness_score"):
        inspection.readiness_score = readiness.readiness_score
    if hasattr(inspection, "readiness_status"):
        inspection.readiness_status = readiness.readiness_status
    if hasattr(inspection, "total_items"):
        inspection.total_items = readiness.total_items
    if hasattr(inspection, "passed_items"):
        inspection.passed_items = readiness.passed_items
    if hasattr(inspection, "failed_items"):
        inspection.failed_items = readiness.failed_items
    if hasattr(inspection, "blocked_items"):
        inspection.blocked_items = readiness.blocked_items
    if hasattr(inspection, "na_items"):
        inspection.na_items = readiness.na_items
    if hasattr(inspection, "failed_critical_items"):
        inspection.failed_critical_items = readiness.failed_critical_items
    if hasattr(inspection, "last_scored_at"):
        inspection.last_scored_at = now
    if hasattr(inspection, "completed_at") and inspection.completed_at is None:
        inspection.completed_at = now

    inspection.passed = readiness.result_status == "pass"
    inspection.reinspect_required = readiness.result_status != "pass"
    if hasattr(inspection, "evidence_summary_json"):
        inspection.evidence_summary_json = _j(
            {
                "mapped_result_count": len(mapped_rows),
                "created_items": created,
                "updated_items": updated,
            }
        )

    return {
        "ok": True,
        "inspection_id": inspection_id,
        "template_key": template_info["template_key"],
        "template_version": template_info["template_version"],
        "mapped_count": len(mapped_rows),
        "created_items": created,
        "updated_items": updated,
        "readiness": {
            "score": readiness.readiness_score,
            "status": readiness.readiness_status,
            "result_status": readiness.result_status,
            "counts": {
                "total_items": readiness.total_items,
                "scored_items": readiness.scored_items,
                "passed_items": readiness.passed_items,
                "failed_items": readiness.failed_items,
                "blocked_items": readiness.blocked_items,
                "na_items": readiness.na_items,
                "failed_critical_items": readiness.failed_critical_items,
            },
        },
    }