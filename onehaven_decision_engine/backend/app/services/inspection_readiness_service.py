from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..domain.compliance.hqs import summarize_items
from ..domain.compliance.inspection_rules import (
    normalize_inspection_item_status,
    score_readiness,
)
from ..models import Inspection, InspectionItem, PropertyChecklistItem


@dataclass(frozen=True)
class InspectionReadinessScore:
    property_id: int
    latest_inspection_id: int | None
    template_key: str | None
    template_version: str | None

    completion_pct: float
    readiness_score: float
    readiness_status: str
    result_status: str

    total_items: int
    scored_items: int
    passed_items: int
    failed_items: int
    blocked_items: int
    na_items: int
    unknown_items: int
    failed_critical_items: int

    latest_inspection_passed: bool
    checklist_failed_count: int
    checklist_blocked_count: int

    hqs_ready: bool
    local_ready: bool
    voucher_ready: bool
    lease_up_ready: bool
    is_compliant: bool

    posture: str
    completion_projection_pct: float


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
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


def _checklist_rows(db: Session, *, org_id: int, property_id: int) -> list[PropertyChecklistItem]:
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


def _inspection_rows(db: Session, *, inspection_id: int) -> list[InspectionItem]:
    return list(
        db.scalars(
            select(InspectionItem)
            .where(InspectionItem.inspection_id == inspection_id)
            .order_by(InspectionItem.id.asc())
        ).all()
    )


def _status_from_checklist_row(row: PropertyChecklistItem) -> str:
    status = str(getattr(row, "status", "") or "").strip().lower()
    if status in {"done", "pass", "passed", "complete", "completed"}:
        return "pass"
    if status in {"failed", "fail", "open"}:
        return "fail"
    if status in {"blocked"}:
        return "blocked"
    if status in {"not_applicable", "na", "n/a"}:
        return "not_applicable"
    if status in {"todo", "in_progress"}:
        return "pending"
    return "pending"


def _checklist_as_scored_rows(rows: list[PropertyChecklistItem]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        status = _status_from_checklist_row(row)
        severity_num = int(getattr(row, "severity", 3) or 3)
        severity = "critical" if severity_num >= 4 else "fail" if severity_num == 3 else "warn" if severity_num == 2 else "info"
        out.append(
            {
                "code": str(getattr(row, "item_code", None) or ""),
                "category": str(getattr(row, "category", None) or "other"),
                "result_status": status,
                "severity": severity,
                "failed": status == "fail",
            }
        )
    return out


def _completion_projection_pct(
    *,
    completion_pct: float,
    readiness_score: float,
    failed_count: int,
    blocked_count: int,
    failed_critical_items: int,
) -> float:
    """
    Conservative projection:
      - start from current completion %
      - cap by readiness score
      - apply penalties for unresolved blockers/fails
    """
    penalty = 0.0
    penalty += float(failed_count) * 4.0
    penalty += float(blocked_count) * 3.0
    penalty += float(failed_critical_items) * 8.0

    projected = min(float(completion_pct), float(readiness_score))
    projected = max(0.0, projected - penalty)
    return round(projected, 2)


def _inspection_grade_posture(
    *,
    readiness_status: str,
    result_status: str,
    failed_count: int,
    blocked_count: int,
    failed_critical_items: int,
    latest_inspection_passed: bool,
    completion_pct: float,
) -> str:
    if latest_inspection_passed and failed_count == 0 and blocked_count == 0 and completion_pct >= 95.0:
        return "inspection_ready"

    if failed_critical_items > 0:
        return "critical_failures"

    if result_status == "fail" and (failed_count > 0 or blocked_count > 0):
        return "needs_remediation"

    if readiness_status in {"needs_work", "critical"}:
        return "not_ready"

    if completion_pct < 95.0:
        return "in_progress"

    return "unknown"


def compute_property_readiness_score(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> InspectionReadinessScore:
    checklist_rows = _checklist_rows(db, org_id=org_id, property_id=property_id)
    latest = _latest_inspection(db, org_id=org_id, property_id=property_id)

    latest_inspection_id = int(latest.id) if latest is not None else None
    template_key = getattr(latest, "template_key", None) if latest is not None else None
    template_version = getattr(latest, "template_version", None) if latest is not None else None

    latest_inspection_passed = False
    readiness_score_value = 0.0
    readiness_status = "unknown"
    result_status = "pending"
    total_items = 0
    scored_items = 0
    passed_items = 0
    failed_items = 0
    blocked_items = 0
    na_items = 0
    failed_critical_items = 0

    if latest is not None:
        latest_inspection_passed = bool(
            normalize_inspection_item_status(
                getattr(latest, "result_status", None),
                failed=(False if getattr(latest, "passed", None) is True else None),
            )
            == "pass"
            or getattr(latest, "passed", False)
        )

        readiness_score_value = float(getattr(latest, "readiness_score", 0.0) or 0.0)
        readiness_status = str(getattr(latest, "readiness_status", "unknown") or "unknown")
        result_status = str(getattr(latest, "result_status", "pending") or "pending")

        inspection_items = _inspection_rows(db, inspection_id=int(latest.id))
        if inspection_items:
            scored = score_readiness(inspection_items)
            readiness_score_value = float(scored.readiness_score)
            readiness_status = str(scored.readiness_status)
            result_status = str(scored.result_status)
            latest_inspection_passed = bool(scored.result_status == "pass")

            total_items = int(scored.total_items)
            scored_items = int(scored.scored_items)
            passed_items = int(scored.passed_items)
            failed_items = int(scored.failed_items)
            blocked_items = int(scored.blocked_items)
            na_items = int(scored.na_items)
            failed_critical_items = int(scored.failed_critical_items)

    if total_items == 0:
        checklist_scored = _checklist_as_scored_rows(checklist_rows)
        if checklist_scored:
            scored = score_readiness(checklist_scored)
            readiness_score_value = float(scored.readiness_score)
            readiness_status = str(scored.readiness_status)
            result_status = str(scored.result_status)

            total_items = int(scored.total_items)
            scored_items = int(scored.scored_items)
            passed_items = int(scored.passed_items)
            failed_items = int(scored.failed_items)
            blocked_items = int(scored.blocked_items)
            na_items = int(scored.na_items)
            failed_critical_items = int(scored.failed_critical_items)

    summary = summarize_items(checklist_rows, latest_inspection_passed=latest_inspection_passed)
    completion_pct = float(round(summary.pct_done * 100.0, 2))

    checklist_failed_count = sum(
        1 for row in checklist_rows
        if str(getattr(row, "status", "") or "").strip().lower() in {"failed", "fail", "open"}
    )
    checklist_blocked_count = sum(
        1 for row in checklist_rows
        if str(getattr(row, "status", "") or "").strip().lower() in {"blocked"}
    )

    unknown_items = max(
        0,
        int(total_items) - int(passed_items) - int(failed_items) - int(blocked_items) - int(na_items),
    )

    hqs_ready = failed_items == 0 and blocked_items == 0 and checklist_failed_count == 0 and checklist_blocked_count == 0
    local_ready = result_status != "fail" and failed_critical_items == 0
    voucher_ready = hqs_ready and result_status != "fail"
    lease_up_ready = voucher_ready and completion_pct >= 95.0

    is_compliant = bool(
        completion_pct >= 95.0
        and checklist_failed_count == 0
        and checklist_blocked_count == 0
        and latest_inspection_passed
    )

    completion_projection_pct = _completion_projection_pct(
        completion_pct=completion_pct,
        readiness_score=readiness_score_value,
        failed_count=failed_items + checklist_failed_count,
        blocked_count=blocked_items + checklist_blocked_count,
        failed_critical_items=failed_critical_items,
    )

    posture = _inspection_grade_posture(
        readiness_status=readiness_status,
        result_status=result_status,
        failed_count=failed_items + checklist_failed_count,
        blocked_count=blocked_items + checklist_blocked_count,
        failed_critical_items=failed_critical_items,
        latest_inspection_passed=latest_inspection_passed,
        completion_pct=completion_pct,
    )

    return InspectionReadinessScore(
        property_id=int(property_id),
        latest_inspection_id=latest_inspection_id,
        template_key=template_key,
        template_version=template_version,
        completion_pct=completion_pct,
        readiness_score=float(round(readiness_score_value, 2)),
        readiness_status=readiness_status,
        result_status=result_status,
        total_items=int(total_items),
        scored_items=int(scored_items),
        passed_items=int(passed_items),
        failed_items=int(failed_items),
        blocked_items=int(blocked_items),
        na_items=int(na_items),
        unknown_items=int(unknown_items),
        failed_critical_items=int(failed_critical_items),
        latest_inspection_passed=bool(latest_inspection_passed),
        checklist_failed_count=int(checklist_failed_count),
        checklist_blocked_count=int(checklist_blocked_count),
        hqs_ready=bool(hqs_ready),
        local_ready=bool(local_ready),
        voucher_ready=bool(voucher_ready),
        lease_up_ready=bool(lease_up_ready),
        is_compliant=bool(is_compliant),
        posture=posture,
        completion_projection_pct=float(completion_projection_pct),
    )


def build_property_readiness_summary(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    score = compute_property_readiness_score(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    return {
        "ok": True,
        "property_id": score.property_id,
        "latest_inspection_id": score.latest_inspection_id,
        "template_key": score.template_key,
        "template_version": score.template_version,
        "completion": {
            "pct": score.completion_pct,
            "projection_pct": score.completion_projection_pct,
            "is_compliant": score.is_compliant,
        },
        "readiness": {
            "score": score.readiness_score,
            "status": score.readiness_status,
            "result_status": score.result_status,
            "posture": score.posture,
            "latest_inspection_passed": score.latest_inspection_passed,
            "hqs_ready": score.hqs_ready,
            "local_ready": score.local_ready,
            "voucher_ready": score.voucher_ready,
            "lease_up_ready": score.lease_up_ready,
        },
        "counts": {
            "total_items": score.total_items,
            "scored_items": score.scored_items,
            "passed_items": score.passed_items,
            "failed_items": score.failed_items,
            "blocked_items": score.blocked_items,
            "na_items": score.na_items,
            "unknown_items": score.unknown_items,
            "failed_critical_items": score.failed_critical_items,
            "checklist_failed_count": score.checklist_failed_count,
            "checklist_blocked_count": score.checklist_blocked_count,
        },
        "raw": asdict(score),
    }