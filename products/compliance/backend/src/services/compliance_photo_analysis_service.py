from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.compliance.photo_to_hqs_mapper import (
    PHOTO_RULE_MAPPINGS,
    PhotoRuleMapping,
    mapping_for_rule_code,
)
from app.models import Inspection, Property, PropertyPhoto, RehabTask
from app.services.property_photo_service import list_property_photos


def _now() -> datetime:
    return datetime.utcnow()


def _severity_rank(value: str) -> int:
    v = str(value or "").strip().lower()
    if v == "critical":
        return 4
    if v in {"high", "fail"}:
        return 3
    if v in {"medium", "warn"}:
        return 2
    return 1


def _task_status_for_finding(is_blocker: bool) -> str:
    return "blocked" if is_blocker else "todo"


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )
    if not prop:
        raise ValueError("property not found")
    return prop


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == int(org_id), Inspection.property_id == int(property_id))
        .order_by(desc(Inspection.inspection_date), desc(Inspection.created_at), desc(Inspection.id))
        .limit(1)
    )


def _kind_summary(photos: list[PropertyPhoto]) -> dict[str, int]:
    return {
        "interior": sum(1 for p in photos if str(getattr(p, "kind", "") or "").lower() == "interior"),
        "exterior": sum(1 for p in photos if str(getattr(p, "kind", "") or "").lower() == "exterior"),
        "unknown": sum(1 for p in photos if str(getattr(p, "kind", "") or "").lower() == "unknown"),
    }


def _pick_evidence_ids(photos: list[PropertyPhoto], *, preferred_kind: str | None = None, limit: int = 3) -> list[int]:
    picked: list[int] = []
    preferred = str(preferred_kind or "").strip().lower() or None
    if preferred:
        picked.extend(int(p.id) for p in photos if str(getattr(p, "kind", "") or "").lower() == preferred and getattr(p, "id", None) is not None)
    if len(picked) < limit:
        picked.extend(int(p.id) for p in photos if getattr(p, "id", None) is not None and int(p.id) not in picked)
    return picked[:limit]


def _mapping_to_finding(
    mapping: PhotoRuleMapping,
    *,
    photos: list[PropertyPhoto],
    property_id: int,
    inspection: Inspection | None,
    checklist_item_id: int | None,
) -> dict[str, Any]:
    evidence_kind = "interior"
    if mapping.rehab_category in {"paint", "windows"}:
        evidence_kind = "exterior"

    evidence_photo_ids = _pick_evidence_ids(photos, preferred_kind=evidence_kind, limit=3)
    jurisdiction = getattr(inspection, "jurisdiction", None) if inspection is not None else None
    template_key = getattr(inspection, "template_key", None) if inspection is not None else None
    template_version = getattr(inspection, "template_version", None) if inspection is not None else None

    return {
        "code": mapping.rule_code,
        "observed_issue": mapping.observed_issue,
        "probable_failed_inspection_item": mapping.probable_failed_inspection_item,
        "severity": mapping.severity,
        "confidence": 0.66 if mapping.severity == "critical" else 0.61,
        "recommended_fix": mapping.recommended_fix,
        "requires_reinspection": bool(mapping.requires_reinspection),
        "rehab_category": mapping.rehab_category,
        "jurisdiction": jurisdiction,
        "rule_mapping": {
            "code": mapping.rule_code,
            "standard_label": mapping.standard_label,
            "standard_citation": mapping.standard_citation,
            "template_key": template_key or "hud_52580a",
            "template_version": template_version or "hud_52580a_2019",
        },
        "inspection_id": int(getattr(inspection, "id", 0) or 0) or None,
        "checklist_item_id": checklist_item_id,
        "property_id": int(property_id),
        "evidence_photo_ids": evidence_photo_ids,
        "source": "compliance_photo_analysis_service",
        "status": "suggested",
        "hard_blocker_candidate": mapping.severity == "critical",
        "human_review_required": True,
        "title": f"Fix: {mapping.probable_failed_inspection_item}",
        "notes": f"Observed issue from uploaded photo set: {mapping.observed_issue}.",
    }


def _deterministic_compliance_findings(
    *,
    photos: list[PropertyPhoto],
    property_id: int,
    inspection: Inspection | None,
    checklist_item_id: int | None,
) -> list[dict[str, Any]]:
    if not photos:
        return []

    has_interior = any(str(getattr(p, "kind", "") or "").lower() == "interior" for p in photos)
    has_exterior = any(str(getattr(p, "kind", "") or "").lower() == "exterior" for p in photos)

    selected: list[PhotoRuleMapping] = []
    for mapping in PHOTO_RULE_MAPPINGS:
        if mapping.trigger in {"outlet_cover_missing", "handrail_missing", "missing_smoke_detector", "exposed_wiring"} and has_interior:
            selected.append(mapping)
        elif mapping.trigger in {"chipping_paint", "window_sash_broken"} and has_exterior:
            selected.append(mapping)

    if not selected:
        fallback = mapping_for_rule_code("WINDOW_WEATHER_TIGHT_SECURE") or PHOTO_RULE_MAPPINGS[0]
        selected = [fallback]

    findings = [
        _mapping_to_finding(
            mapping,
            photos=photos,
            property_id=property_id,
            inspection=inspection,
            checklist_item_id=checklist_item_id,
        )
        for mapping in selected
    ]
    findings.sort(key=lambda row: (_severity_rank(str(row.get("severity"))), float(row.get("confidence") or 0.0)), reverse=True)
    return findings


def analyze_property_photos_for_compliance(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
) -> dict[str, Any]:
    _get_property(db, org_id=org_id, property_id=property_id)
    photos = list_property_photos(db, org_id=org_id, property_id=property_id)
    if not photos:
        return {
            "ok": False,
            "property_id": int(property_id),
            "photo_count": 0,
            "findings": [],
            "summary": {"interior": 0, "exterior": 0, "unknown": 0},
            "code": "no_photos",
            "requires_human_review": True,
        }

    inspection = None
    if inspection_id is not None:
        inspection = db.scalar(
            select(Inspection).where(
                Inspection.org_id == int(org_id),
                Inspection.property_id == int(property_id),
                Inspection.id == int(inspection_id),
            )
        )
    if inspection is None:
        inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)

    findings = _deterministic_compliance_findings(
        photos=photos,
        property_id=property_id,
        inspection=inspection,
        checklist_item_id=checklist_item_id,
    )
    summary = _kind_summary(photos)

    return {
        "ok": True,
        "property_id": int(property_id),
        "photo_count": len(photos),
        "summary": summary,
        "findings": findings,
        "issues": findings,
        "requires_human_review": True,
        "latest_inspection_id": int(getattr(inspection, "id", 0) or 0) or None,
        "template_key": getattr(inspection, "template_key", None) if inspection is not None else None,
        "template_version": getattr(inspection, "template_version", None) if inspection is not None else None,
        "jurisdiction": getattr(inspection, "jurisdiction", None) if inspection is not None else None,
        "estimated_blockers": sum(1 for row in findings if row.get("hard_blocker_candidate")),
        "estimated_reinspect_items": sum(1 for row in findings if row.get("requires_reinspection")),
    }


def _finding_task_title(finding: dict[str, Any]) -> str:
    probable = str(finding.get("probable_failed_inspection_item") or "").strip()
    issue = str(finding.get("observed_issue") or "").strip()
    if probable:
        return f"Resolve: {probable}"
    if issue:
        return f"Resolve photo finding: {issue}"
    return "Resolve compliance photo finding"


def _task_exists(db: Session, *, org_id: int, property_id: int, title: str) -> bool:
    existing = db.scalar(
        select(RehabTask).where(
            RehabTask.org_id == int(org_id),
            RehabTask.property_id == int(property_id),
            RehabTask.title == title,
        )
    )
    return existing is not None


def create_compliance_tasks_from_photo_analysis(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    analysis: dict[str, Any],
    confirmed_codes: Iterable[str] | None = None,
    mark_blocking: bool = False,
) -> dict[str, Any]:
    if not analysis.get("ok"):
        return {
            "ok": False,
            "property_id": int(property_id),
            "created": 0,
            "created_task_ids": [],
            "findings": [],
            "code": analysis.get("code", "analysis_failed"),
        }

    confirmed = {str(code).strip().upper() for code in (confirmed_codes or []) if str(code).strip()}
    findings = list(analysis.get("findings") or analysis.get("issues") or [])
    selected_findings = [
        row for row in findings
        if not confirmed or str(row.get("code") or "").strip().upper() in confirmed
    ]

    created = 0
    created_task_ids: list[int] = []

    for finding in selected_findings:
        title = _finding_task_title(finding)
        if _task_exists(db, org_id=org_id, property_id=property_id, title=title):
            continue

        blocker = bool(mark_blocking and finding.get("hard_blocker_candidate"))
        notes_payload = {
            "source": "compliance_photo_analysis_service",
            "observed_issue": finding.get("observed_issue"),
            "probable_failed_inspection_item": finding.get("probable_failed_inspection_item"),
            "severity": finding.get("severity"),
            "confidence": finding.get("confidence"),
            "recommended_fix": finding.get("recommended_fix"),
            "requires_reinspection": finding.get("requires_reinspection"),
            "rule_mapping": finding.get("rule_mapping"),
            "evidence_photo_ids": finding.get("evidence_photo_ids", []),
            "human_review_required": True,
            "status": "confirmed" if confirmed else "auto_confirmed",
        }

        row = RehabTask(
            org_id=int(org_id),
            property_id=int(property_id),
            title=title,
            category=str(finding.get("rehab_category") or "compliance_repair"),
            inspection_relevant=True,
            status=_task_status_for_finding(blocker),
            cost_estimate=0.0,
            vendor=None,
            deadline=None,
            notes=json.dumps(notes_payload),
            created_at=_now(),
        )
        db.add(row)
        db.flush()
        created += 1
        created_task_ids.append(int(getattr(row, "id", 0) or 0))

    db.commit()
    return {
        "ok": True,
        "property_id": int(property_id),
        "created": created,
        "created_task_ids": created_task_ids,
        "findings": selected_findings,
        "confirmed_codes": sorted(confirmed),
        "requires_human_review": True,
        "mark_blocking": bool(mark_blocking),
    }
