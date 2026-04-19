# backend/app/services/inspection_template_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import desc, select
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


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default
    return default


def _rollback_quietly(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        pass


def _severity_to_int(severity: str | None) -> int:
    s = str(severity or "").strip().lower()
    if s == "critical":
        return 4
    if s == "fail":
        return 3
    if s == "warn":
        return 2
    return 1


def _severity_to_label(value: Any) -> str:
    rank = _severity_to_int(str(value or ""))
    if rank >= 4:
        return "critical"
    if rank == 3:
        return "fail"
    if rank == 2:
        return "warn"
    return "info"


def _checklist_status_from_result(result_status: str) -> str:
    s = str(result_status or "").strip().lower()
    if s == "pass":
        return "done"
    if s == "fail":
        return "failed"
    if s in {"blocked", "inconclusive", "pending"}:
        return "blocked"
    if s == "not_applicable":
        return "done"
    return "todo"


def _normalize_result_status(value: Any, *, default: str = "pending") -> str:
    s = str(value or "").strip().lower()
    if s in {"pass", "fail", "blocked", "not_applicable", "inconclusive", "pending"}:
        return s
    if s in {"na", "n/a"}:
        return "not_applicable"
    if s in {"done", "completed", "complete", "passed"}:
        return "pass"
    if s in {"failed", "open"}:
        return "fail"
    return default


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


def _get_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int,
) -> Inspection:
    inspection = db.scalar(
        select(Inspection).where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
            Inspection.id == inspection_id,
        )
    )
    if inspection is None:
        raise ValueError("inspection not found")
    return inspection


def _latest_inspection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(
            Inspection.org_id == org_id,
            Inspection.property_id == property_id,
        )
        .order_by(
            desc(Inspection.inspection_date),
            desc(Inspection.created_at),
            desc(Inspection.id),
        )
        .limit(1)
    )


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


def _extract_payload_meta(raw_payload: dict[str, Any] | list[dict[str, Any]] | None) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        meta = raw_payload.get("inspection") or raw_payload.get("inspection_meta") or raw_payload.get("meta") or {}
        if isinstance(meta, dict):
            return meta
    return {}


def _safe_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    parsed = _json_loads(value, None)
    if isinstance(parsed, list):
        return parsed
    if parsed is None:
        return []
    return [parsed]


def _coalesce_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _safe_profile_summary(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(profile_summary, dict):
        return profile_summary
    try:
        resolved = resolve_operational_policy(
            db,
            org_id=org_id,
            state=getattr(prop, "state", None) or "MI",
            county=getattr(prop, "county", None),
            city=getattr(prop, "city", None),
        )
        return resolved if isinstance(resolved, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _safe_effective_hqs_items(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any],
) -> dict[str, Any]:
    try:
        effective = get_effective_hqs_items(
            db,
            org_id=org_id,
            prop=prop,
            profile_summary=profile_summary or {},
        )
        return effective if isinstance(effective, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {"items": [], "sources": [], "counts": {}}


def _safe_find_property_checklist(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: str,
    version: str,
) -> PropertyChecklist | None:
    try:
        return _find_property_checklist(
            db,
            org_id=org_id,
            property_id=property_id,
            strategy=strategy,
            version=version,
        )
    except Exception:
        _rollback_quietly(db)
        return None


def _safe_find_checklist_items(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> list[PropertyChecklistItem]:
    try:
        return _find_checklist_items(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    except Exception:
        _rollback_quietly(db)
        return []


def _build_template_item(row: dict[str, Any]) -> ChecklistTemplateItem:
    return ChecklistTemplateItem(
        code=str(row["code"]),
        description=str(row["description"]),
        category=str(row["category"]),
        default_status=str(row.get("default_status") or "todo"),
        severity=str(row["severity"]),
        common_fail=bool(row["common_fail"]),
        inspection_rule_code=row.get("inspection_rule_code"),
        suggested_fix=row.get("suggested_fix"),
        template_key=str(row["template_key"]),
        template_version=str(row["template_version"]),
        section=row.get("section"),
        item_number=row.get("item_number"),
        room_scope=row.get("room_scope"),
        not_applicable_allowed=bool(row.get("not_applicable_allowed", False)),
    )


def _build_checklist_metadata(
    *,
    template_item: ChecklistTemplateItem,
    property_id: int,
    inspection_id: int | None,
    result_status: str | None = None,
    fail_reason: str | None = None,
    remediation_guidance: str | None = None,
    evidence: list[Any] | None = None,
    photos: list[Any] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    metadata = {
        "template_key": template_item.template_key,
        "template_version": template_item.template_version,
        "section": template_item.section,
        "item_number": template_item.item_number,
        "room_scope": template_item.room_scope,
        "inspection_rule_code": template_item.inspection_rule_code,
        "not_applicable_allowed": template_item.not_applicable_allowed,
        "property_id": property_id,
        "inspection_id": inspection_id,
        "mapped_code": template_item.code,
    }
    if result_status is not None:
        metadata["latest_result_status"] = result_status
    if fail_reason:
        metadata["latest_fail_reason"] = fail_reason
    if remediation_guidance:
        metadata["latest_remediation_guidance"] = remediation_guidance
    if notes:
        metadata["latest_notes"] = notes
    if evidence is not None:
        metadata["latest_evidence"] = evidence
    if photos is not None:
        metadata["latest_photos"] = photos
    return metadata


def build_inspection_template(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prop = _get_property(db, org_id=org_id, property_id=property_id)

    safe_profile_summary = _safe_profile_summary(
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=profile_summary,
    )

    effective = _safe_effective_hqs_items(
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=safe_profile_summary,
    )

    template_items = template_items_from_effective_rules(effective.get("items") or []) or []
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
        "profile_summary": safe_profile_summary,
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

    checklist = _safe_find_property_checklist(
        db,
        org_id=org_id,
        property_id=property_id,
        strategy=strategy,
        version=version,
    )

    created_checklist = False
    if checklist is None:
        try:
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
        except Exception:
            _rollback_quietly(db)
            checklist = None
    else:
        try:
            checklist.items_json = _j(template["items"])
            if hasattr(checklist, "generated_at") and getattr(checklist, "generated_at", None) is None:
                checklist.generated_at = now
        except Exception:
            _rollback_quietly(db)

    existing_rows = _safe_find_checklist_items(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    by_code = {str(r.item_code or "").strip().upper(): r for r in existing_rows}

    created_items = 0
    updated_items = 0

    for item_dict in template["items"]:
        template_item = _build_template_item(item_dict)
        code = template_item.code.strip().upper()
        desired_applies = _j(
            _build_checklist_metadata(
                template_item=template_item,
                property_id=property_id,
                inspection_id=None,
            )
        )
        row = by_code.get(code)
        if row is None:
            try:
                row = PropertyChecklistItem(
                    org_id=org_id,
                    property_id=property_id,
                    checklist_id=getattr(checklist, "id", None),
                    item_code=code,
                    category=template_item.category,
                    description=template_item.description,
                    severity=_severity_to_int(template_item.severity),
                    common_fail=bool(template_item.common_fail),
                    applies_if_json=desired_applies,
                    status=str(template_item.default_status or "todo"),
                    notes=template_item.suggested_fix,
                    created_at=now,
                    updated_at=now,
                )
                db.add(row)
                created_items += 1
            except Exception:
                _rollback_quietly(db)
            continue

        changed = False
        try:
            checklist_id = getattr(checklist, "id", None)
            if checklist_id is not None and getattr(row, "checklist_id", None) != checklist_id:
                row.checklist_id = checklist_id
                changed = True
            if getattr(row, "category", None) != template_item.category:
                row.category = template_item.category
                changed = True
            if getattr(row, "description", None) != template_item.description:
                row.description = template_item.description
                changed = True
            desired_severity = _severity_to_int(template_item.severity)
            if int(getattr(row, "severity", 0) or 0) != desired_severity:
                row.severity = desired_severity
                changed = True
            desired_common_fail = bool(template_item.common_fail)
            if bool(getattr(row, "common_fail", False)) != desired_common_fail:
                row.common_fail = desired_common_fail
                changed = True
            if (getattr(row, "applies_if_json", None) or "") != desired_applies:
                row.applies_if_json = desired_applies
                changed = True
            if changed:
                row.updated_at = now
                updated_items += 1
        except Exception:
            _rollback_quietly(db)

    checklist_id = getattr(checklist, "id", None)

    return {
        "ok": True,
        "template_key": strategy,
        "template_version": version,
        "created_checklist": created_checklist,
        "created_items": created_items,
        "updated_items": updated_items,
        "checklist_id": checklist_id,
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
    template_items = [_build_template_item(row) for row in template_rows]
    template_by_code = template_lookup(template_items)

    inspection = _get_inspection(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
    )
    payload_meta = _extract_payload_meta(raw_payload)

    existing_items = _find_inspection_items(db, inspection_id=inspection_id)
    existing_by_code = {str(r.code or "").strip().upper(): r for r in existing_items}

    checklist_rows = _find_checklist_items(db, org_id=org_id, property_id=property_id)
    checklist_by_code = {str(r.item_code or "").strip().upper(): r for r in checklist_rows}

    mapped_rows = map_raw_inspection_payload(raw_payload=raw_payload)
    mapped_by_code: dict[str, dict[str, Any]] = {}
    for row in mapped_rows:
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        mapped_by_code[code] = row

    now = _now()
    created = 0
    updated = 0

    for template_item in template_items:
        code = template_item.code.strip().upper()
        mapped = mapped_by_code.get(code, {})
        result_status = _normalize_result_status(mapped.get("result_status"), default="pending")
        fail_reason = _coalesce_text(mapped.get("fail_reason"))
        remediation_guidance = _coalesce_text(mapped.get("remediation_guidance"), template_item.suggested_fix)
        evidence = _safe_list(mapped.get("evidence_json"))
        photos = _safe_list(mapped.get("photo_references_json"))
        details = _coalesce_text(mapped.get("details"))
        notes = _coalesce_text(details, fail_reason, remediation_guidance)

        item = existing_by_code.get(code)
        if item is None:
            item = InspectionItem(
                inspection_id=inspection_id,
                code=code,
                failed=bool(result_status == "fail" or mapped.get("failed", False)),
                severity=int(mapped.get("severity_int") or _severity_to_int(template_item.severity)),
                location=mapped.get("location"),
                details=details,
                category=mapped.get("category") or template_item.category,
                result_status=result_status,
                fail_reason=fail_reason,
                remediation_guidance=remediation_guidance,
                evidence_json=_j(evidence),
                photo_references_json=_j(photos),
                standard_label=mapped.get("standard_label") or template_item.description,
                standard_citation=mapped.get("standard_citation") or template_item.inspection_rule_code,
                readiness_impact=float(mapped.get("readiness_impact") or 0.0),
                requires_reinspection=bool(
                    mapped.get("requires_reinspection", result_status in {"fail", "blocked", "inconclusive"})
                ),
                created_at=now,
            )
            db.add(item)
            created += 1
        else:
            item.failed = bool(result_status == "fail" or mapped.get("failed", False))
            item.severity = int(mapped.get("severity_int") or _severity_to_int(template_item.severity))
            item.location = mapped.get("location")
            item.details = details
            if hasattr(item, "category"):
                item.category = mapped.get("category") or template_item.category
            if hasattr(item, "result_status"):
                item.result_status = result_status
            if hasattr(item, "fail_reason"):
                item.fail_reason = fail_reason
            if hasattr(item, "remediation_guidance"):
                item.remediation_guidance = remediation_guidance
            if hasattr(item, "evidence_json"):
                item.evidence_json = _j(evidence)
            if hasattr(item, "photo_references_json"):
                item.photo_references_json = _j(photos)
            if hasattr(item, "standard_label"):
                item.standard_label = mapped.get("standard_label") or template_item.description
            if hasattr(item, "standard_citation"):
                item.standard_citation = mapped.get("standard_citation") or template_item.inspection_rule_code
            if hasattr(item, "readiness_impact"):
                item.readiness_impact = float(mapped.get("readiness_impact") or 0.0)
            if hasattr(item, "requires_reinspection"):
                item.requires_reinspection = bool(
                    mapped.get("requires_reinspection", result_status in {"fail", "blocked", "inconclusive"})
                )
            if hasattr(item, "updated_at"):
                item.updated_at = now
            updated += 1

        checklist_row = checklist_by_code.get(code)
        if sync_checklist and checklist_row is not None:
            checklist_row.status = _checklist_status_from_result(result_status)
            checklist_row.notes = notes or checklist_row.notes
            checklist_row.updated_at = now
            checklist_row.applies_if_json = _j(
                _build_checklist_metadata(
                    template_item=template_item,
                    property_id=property_id,
                    inspection_id=inspection_id,
                    result_status=result_status,
                    fail_reason=fail_reason,
                    remediation_guidance=remediation_guidance,
                    evidence=evidence,
                    photos=photos,
                    notes=notes,
                )
            )

    db.flush()

    final_items = _find_inspection_items(db, inspection_id=inspection_id)
    readiness = score_readiness(final_items)

    inspection.template_key = template_info["template_key"]
    inspection.template_version = template_info["template_version"]

    if hasattr(inspection, "inspection_status"):
        inspection.inspection_status = "completed"
    if hasattr(inspection, "result_status"):
        inspection.result_status = str(readiness.result_status)
    if hasattr(inspection, "readiness_score"):
        inspection.readiness_score = float(readiness.readiness_score)
    if hasattr(inspection, "readiness_status"):
        inspection.readiness_status = str(readiness.readiness_status)
    if hasattr(inspection, "total_items"):
        inspection.total_items = int(readiness.total_items)
    if hasattr(inspection, "passed_items"):
        inspection.passed_items = int(readiness.passed_items)
    if hasattr(inspection, "failed_items"):
        inspection.failed_items = int(readiness.failed_items)
    if hasattr(inspection, "blocked_items"):
        inspection.blocked_items = int(readiness.blocked_items)
    if hasattr(inspection, "na_items"):
        inspection.na_items = int(readiness.na_items)
    if hasattr(inspection, "failed_critical_items"):
        inspection.failed_critical_items = int(readiness.failed_critical_items)
    if hasattr(inspection, "last_scored_at"):
        inspection.last_scored_at = now
    if hasattr(inspection, "completed_at") and getattr(inspection, "completed_at", None) is None:
        inspection.completed_at = now

    inspection.passed = bool(readiness.result_status == "pass")
    inspection.reinspect_required = bool(readiness.result_status != "pass")

    if hasattr(inspection, "inspection_date") and payload_meta.get("inspection_date"):
        try:
            inspection.inspection_date = payload_meta.get("inspection_date")
        except Exception:
            pass
    if hasattr(inspection, "inspector") and payload_meta.get("inspector"):
        inspection.inspector = payload_meta.get("inspector")
    if hasattr(inspection, "jurisdiction") and payload_meta.get("jurisdiction"):
        inspection.jurisdiction = payload_meta.get("jurisdiction")

    evidence_summary = {
        "mapped_result_count": len(mapped_by_code),
        "template_item_count": len(template_items),
        "created_items": created,
        "updated_items": updated,
        "inspection_context": {
            "inspection_id": inspection_id,
            "inspection_date": str(getattr(inspection, "inspection_date", None) or payload_meta.get("inspection_date") or ""),
            "inspector": getattr(inspection, "inspector", None) or payload_meta.get("inspector"),
            "jurisdiction": getattr(inspection, "jurisdiction", None) or payload_meta.get("jurisdiction"),
            "template_key": template_info["template_key"],
            "template_version": template_info["template_version"],
        },
    }
    if hasattr(inspection, "evidence_summary_json"):
        inspection.evidence_summary_json = _j(evidence_summary)

    latest = _latest_inspection(db, org_id=org_id, property_id=property_id)
    history = {
        "latest_inspection_id": int(latest.id) if latest is not None else inspection_id,
        "current_inspection_id": inspection_id,
        "is_latest_inspection": bool(latest is not None and int(latest.id) == int(inspection_id)),
        "passed": bool(inspection.passed),
        "reinspect_required": bool(inspection.reinspect_required),
    }

    return {
        "ok": True,
        "inspection_id": inspection_id,
        "template_key": template_info["template_key"],
        "template_version": template_info["template_version"],
        "mapped_count": len(mapped_by_code),
        "template_item_count": len(template_items),
        "created_items": created,
        "updated_items": updated,
        "history": history,
        "readiness": {
            "score": float(readiness.readiness_score),
            "status": str(readiness.readiness_status),
            "result_status": str(readiness.result_status),
            "counts": {
                "total_items": int(readiness.total_items),
                "scored_items": int(readiness.scored_items),
                "passed_items": int(readiness.passed_items),
                "failed_items": int(readiness.failed_items),
                "blocked_items": int(readiness.blocked_items),
                "na_items": int(readiness.na_items),
                "failed_critical_items": int(readiness.failed_critical_items),
            },
        },
        "template_lookup_count": len(template_by_code),
    }

# --- Step 7 additive NSPIRE checklist + PDF context extensions ---
import os as _step7_os
from pathlib import Path as _Step7Path

_step7_prev_build_inspection_template = build_inspection_template
_step7_prev_ensure_template_backed_checklist = ensure_template_backed_checklist
_step7_prev_apply_raw_inspection_payload = apply_raw_inspection_payload


def _step7_pdf_roots() -> list[str]:
    raw = _step7_os.getenv("POLICY_PDF_ROOTS") or _step7_os.getenv("POLICY_PDFS_ROOT") or _step7_os.getenv("POLICY_PDF_ROOT") or ""
    roots: list[str] = []
    for piece in str(raw).split(_step7_os.pathsep):
        piece = str(piece).strip()
        if piece:
            roots.append(piece)
    for fallback in ("/mnt/data/pdfs", "/mnt/data/pdfs", "/mnt/data/PDFs", "/mnt/data/pfs"):
        if fallback not in roots and _step7_os.path.isdir(fallback):
            roots.append(fallback)
    return roots


def _step7_pdf_context_for_item(item: dict[str, Any]) -> dict[str, Any]:
    source = item.get("source") or {}
    context = {
        "pdf_roots": _step7_pdf_roots(),
        "has_pdf_roots": bool(_step7_pdf_roots()),
        "standard_citation": item.get("standard_citation"),
        "standard_label": item.get("standard_label"),
        "nspire_standard_code": item.get("nspire_standard_code") or item.get("standard_code"),
        "nspire_standard_key": item.get("nspire_standard_key"),
        "source_type": source.get("type") if isinstance(source, dict) else None,
    }
    return context


def _step7_enrich_template_item_dict(item: dict[str, Any], effective_by_code: dict[str, dict[str, Any]]) -> dict[str, Any]:
    code = str(item.get("code") or "").strip().upper()
    raw = dict(effective_by_code.get(code) or {})
    enriched = dict(item)
    severity = str(raw.get("nspire_designation") or raw.get("severity") or item.get("severity") or "fail").strip().lower()
    correction_days = raw.get("correction_days")
    if correction_days is None:
        if severity == "life_threatening":
            correction_days = 1
        elif severity in {"severe", "moderate", "critical", "fail"}:
            correction_days = 30
    enriched.update({
        "inspection_rule_code": raw.get("inspection_rule_code") or item.get("inspection_rule_code") or code,
        "nspire_standard_key": raw.get("nspire_standard_key"),
        "nspire_standard_code": raw.get("nspire_standard_code") or raw.get("standard_code"),
        "nspire_standard_label": raw.get("nspire_standard_label") or raw.get("standard_label"),
        "nspire_deficiency_description": raw.get("nspire_deficiency_description") or raw.get("description"),
        "nspire_designation": raw.get("nspire_designation") or severity,
        "correction_days": correction_days,
        "affirmative_habitability_requirement": bool(raw.get("affirmative_habitability_requirement", False)),
        "standard_label": raw.get("standard_label") or item.get("description"),
        "standard_citation": raw.get("standard_citation") or item.get("standard_citation"),
        "pdf_context": _step7_pdf_context_for_item(raw or item),
    })
    return enriched


def build_inspection_template(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = _step7_prev_build_inspection_template(
        db,
        org_id=org_id,
        property_id=property_id,
        profile_summary=profile_summary,
    )
    try:
        prop = _get_property(db, org_id=org_id, property_id=property_id)
        safe_profile = _safe_profile_summary(db, org_id=org_id, prop=prop, profile_summary=profile_summary)
        effective = _safe_effective_hqs_items(db, org_id=org_id, prop=prop, profile_summary=safe_profile)
        effective_by_code = {str((row.get("code") or "")).strip().upper(): dict(row) for row in (effective.get("items") or []) if str(row.get("code") or "").strip()}
        items = [_step7_enrich_template_item_dict(dict(item), effective_by_code) for item in (base.get("items") or [])]
        counts = dict(base.get("counts") or {})
        counts.update({
            "nspire_backed": sum(1 for row in items if row.get("nspire_standard_key") or row.get("nspire_standard_code")),
            "life_threatening": sum(1 for row in items if str(row.get("nspire_designation") or "").strip().lower() == "life_threatening"),
            "with_deadlines": sum(1 for row in items if row.get("correction_days") not in {None, 0, "", False}),
            "pdf_context_available": int(bool(_step7_pdf_roots())),
        })
        return {
            **base,
            "items": items,
            "counts": counts,
            "template_context": {
                "nsire_or_nspire_backed": bool(counts.get("nspire_backed")),
                "pdf_roots": _step7_pdf_roots(),
                "pdf_context_available": bool(_step7_pdf_roots()),
            },
        }
    except Exception as e:
        return {
            **base,
            "template_context": {
                "pdf_roots": _step7_pdf_roots(),
                "pdf_context_available": bool(_step7_pdf_roots()),
                "enrichment_error": str(e),
            },
        }


def ensure_template_backed_checklist(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = _step7_prev_ensure_template_backed_checklist(
        db,
        org_id=org_id,
        property_id=property_id,
        profile_summary=profile_summary,
    )
    try:
        template = result.get("template") or {}
        template_by_code = {str((row.get("code") or "")).strip().upper(): row for row in (template.get("items") or []) if str(row.get("code") or "").strip()}
        rows = _safe_find_checklist_items(db, org_id=org_id, property_id=property_id)
        updated_step7 = 0
        for row in rows:
            code = str(getattr(row, "item_code", None) or "").strip().upper()
            tpl = template_by_code.get(code)
            if not tpl:
                continue
            meta = _json_loads(getattr(row, "applies_if_json", None), {})
            if not isinstance(meta, dict):
                meta = {}
            meta.update({
                "correction_days": tpl.get("correction_days"),
                "nspire_designation": tpl.get("nspire_designation"),
                "nspire_standard_key": tpl.get("nspire_standard_key"),
                "nspire_standard_code": tpl.get("nspire_standard_code"),
                "standard_citation": tpl.get("standard_citation"),
                "pdf_context": tpl.get("pdf_context") or {},
            })
            desired = _j(meta)
            if (getattr(row, "applies_if_json", None) or "") != desired:
                row.applies_if_json = desired
                if hasattr(row, "updated_at"):
                    row.updated_at = _now()
                db.add(row)
                updated_step7 += 1
        result["step7_updated_items"] = updated_step7
        return result
    except Exception as e:
        result["step7_sync_error"] = str(e)
        return result


def apply_raw_inspection_payload(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int,
    raw_payload: dict[str, Any] | list[dict[str, Any]] | None,
    sync_checklist: bool = True,
) -> dict[str, Any]:
    result = _step7_prev_apply_raw_inspection_payload(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        raw_payload=raw_payload,
        sync_checklist=sync_checklist,
    )
    try:
        rows = _find_inspection_items(db, inspection_id=inspection_id)
        life_threatening = 0
        severe = 0
        moderate = 0
        low = 0
        for row in rows:
            details = _json_loads(getattr(row, "evidence_json", None), [])
            sev = str(getattr(row, "standard_label", None) or "").strip().lower()
            applies = None
            # fallback via checklist metadata
            code = str(getattr(row, "code", None) or "").strip().upper()
            for ci in _find_checklist_items(db, org_id=org_id, property_id=property_id):
                if str(getattr(ci, "item_code", None) or "").strip().upper() == code:
                    applies = _json_loads(getattr(ci, "applies_if_json", None), {})
                    break
            designation = None
            if isinstance(applies, dict):
                designation = str(applies.get("nspire_designation") or "").strip().lower() or None
            if designation == "life_threatening":
                life_threatening += 1
            elif designation == "severe":
                severe += 1
            elif designation == "moderate":
                moderate += 1
            elif designation == "low":
                low += 1
        result["nspire_counts"] = {
            "life_threatening": life_threatening,
            "severe": severe,
            "moderate": moderate,
            "low": low,
        }
        result["pdf_context_available"] = bool(_step7_pdf_roots())
        return result
    except Exception as e:
        result["step7_apply_error"] = str(e)
        return result
