from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property, PropertyPhoto, RehabTask
from app.products.compliance.services.compliance_photo_analysis_service import (
    analyze_property_photos_for_compliance as analyze_compliance_photo_findings,
    create_compliance_tasks_from_photo_analysis,
)


def _now() -> datetime:
    return datetime.utcnow()


def _severity_rank(value: str) -> int:
    v = (value or "").lower()
    if v == "critical":
        return 4
    if v == "high":
        return 3
    if v == "medium":
        return 2
    return 1


def _task_status_for_blocker(is_blocker: bool) -> str:
    return "blocked" if is_blocker else "todo"


def _deterministic_issue_bank(photos: list[PropertyPhoto]) -> list[dict[str, Any]]:
    """
    Deterministic placeholder analysis for rehab workflow.
    The compliance chunk now remaps the same photo inventory into HQS-specific
    findings through compliance_photo_analysis_service, but we keep the rehab
    contract stable for existing tests and UI.
    """
    has_interior = any((p.kind or "").lower() == "interior" for p in photos)
    has_exterior = any((p.kind or "").lower() == "exterior" for p in photos)

    issues: list[dict[str, Any]] = []

    if has_exterior:
        issues.append(
            {
                "title": "Exterior paint / siding review",
                "category": "exterior",
                "severity": "high",
                "estimated_cost": 2800.0,
                "blocker": False,
                "notes": "Detected exterior source imagery. Review paint, siding, and moisture-exposed surfaces.",
                "evidence_photo_ids": [int(p.id) for p in photos if (p.kind or "").lower() == "exterior"][:3],
            }
        )
        issues.append(
            {
                "title": "Window and trim condition check",
                "category": "windows",
                "severity": "high",
                "estimated_cost": 1600.0,
                "blocker": True,
                "notes": "Exterior photos suggest windows or trim should be validated before lease-up and compliance.",
                "evidence_photo_ids": [int(p.id) for p in photos if (p.kind or "").lower() == "exterior"][:2],
            }
        )

    if has_interior:
        issues.append(
            {
                "title": "Kitchen / bath turnover scope",
                "category": "interior_finish",
                "severity": "medium",
                "estimated_cost": 4200.0,
                "blocker": False,
                "notes": "Interior photos present. Evaluate cabinets, counters, fixtures, flooring, and paint-ready surfaces.",
                "evidence_photo_ids": [int(p.id) for p in photos if (p.kind or "").lower() == "interior"][:4],
            }
        )
        issues.append(
            {
                "title": "Life-safety interior punch list",
                "category": "safety",
                "severity": "critical",
                "estimated_cost": 1200.0,
                "blocker": True,
                "notes": "Interior review should confirm stairs, railings, GFCI, smoke or CO alarms, and trip hazards.",
                "evidence_photo_ids": [int(p.id) for p in photos if (p.kind or "").lower() == "interior"][:4],
            }
        )

    if not issues:
        issues.append(
            {
                "title": "General photo-based rehab walkthrough",
                "category": "rehab",
                "severity": "medium",
                "estimated_cost": 1500.0,
                "blocker": False,
                "notes": "Photos were present but not strongly classifiable. Manual review required.",
                "evidence_photo_ids": [int(p.id) for p in photos][:3],
            }
        )

    issues.sort(key=lambda x: _severity_rank(x["severity"]), reverse=True)
    return issues


def analyze_property_photos(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )
    if not prop:
        raise ValueError("property not found")

    photos = list(
        db.scalars(
            select(PropertyPhoto)
            .where(
                PropertyPhoto.org_id == org_id,
                PropertyPhoto.property_id == property_id,
            )
            .order_by(PropertyPhoto.sort_order.asc(), PropertyPhoto.id.asc())
        ).all()
    )

    if not photos:
        return {
            "ok": False,
            "property_id": property_id,
            "photo_count": 0,
            "issues": [],
            "summary": {"interior": 0, "exterior": 0, "unknown": 0},
            "code": "no_photos",
        }

    summary = {
        "interior": sum(1 for p in photos if (p.kind or "").lower() == "interior"),
        "exterior": sum(1 for p in photos if (p.kind or "").lower() == "exterior"),
        "unknown": sum(1 for p in photos if (p.kind or "").lower() == "unknown"),
    }

    issues = _deterministic_issue_bank(photos)

    return {
        "ok": True,
        "property_id": property_id,
        "photo_count": len(photos),
        "summary": summary,
        "issues": issues,
    }


def create_rehab_tasks_from_analysis(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    analysis: dict[str, Any],
) -> dict[str, Any]:
    if not analysis.get("ok"):
        return {
            "ok": False,
            "property_id": property_id,
            "created": 0,
            "issues": [],
            "code": analysis.get("code", "analysis_failed"),
        }

    created = 0
    created_task_ids: list[int] = []

    for issue in analysis.get("issues", []):
        title = str(issue.get("title") or "").strip()
        if not title:
            continue

        existing = db.scalar(
            select(RehabTask).where(
                RehabTask.org_id == org_id,
                RehabTask.property_id == property_id,
                RehabTask.title == title,
            )
        )
        if existing:
            continue

        notes_payload = {
            "source": "photo_rehab_agent",
            "severity": issue.get("severity"),
            "blocker": bool(issue.get("blocker")),
            "evidence_photo_ids": issue.get("evidence_photo_ids", []),
            "agent_notes": issue.get("notes"),
        }

        row = RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=str(issue.get("category") or "rehab"),
            inspection_relevant=bool(issue.get("blocker", False)),
            status=_task_status_for_blocker(bool(issue.get("blocker"))),
            cost_estimate=float(issue.get("estimated_cost") or 0.0),
            vendor=None,
            deadline=None,
            notes=json.dumps(notes_payload),
            created_at=_now(),
        )
        db.add(row)
        db.flush()

        created += 1
        created_task_ids.append(int(row.id))

    db.commit()

    return {
        "ok": True,
        "property_id": property_id,
        "created": created,
        "created_task_ids": created_task_ids,
        "issues": analysis.get("issues", []),
        "photo_count": analysis.get("photo_count", 0),
        "summary": analysis.get("summary", {}),
    }


def analyze_and_create_rehab_tasks(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    analysis = analyze_property_photos(db, org_id=org_id, property_id=property_id)
    return create_rehab_tasks_from_analysis(
        db,
        org_id=org_id,
        property_id=property_id,
        analysis=analysis,
    )


def analyze_property_photos_for_compliance(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
) -> dict[str, Any]:
    return analyze_compliance_photo_findings(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        checklist_item_id=checklist_item_id,
    )


def analyze_and_create_compliance_tasks(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    confirmed_codes: list[str] | None = None,
    mark_blocking: bool = False,
) -> dict[str, Any]:
    analysis = analyze_compliance_photo_findings(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        checklist_item_id=checklist_item_id,
    )
    return create_compliance_tasks_from_photo_analysis(
        db,
        org_id=org_id,
        property_id=property_id,
        analysis=analysis,
        confirmed_codes=confirmed_codes,
        mark_blocking=mark_blocking,
    )
