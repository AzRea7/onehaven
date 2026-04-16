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
from ..services.policy_projection_service import build_property_projection_snapshot, rebuild_property_projection


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
    unresolved_failure_count: int
    unresolved_blocked_count: int
    unresolved_critical_count: int
    evidence_blocking_count: int
    evidence_unknown_count: int
    evidence_conflicting_count: int
    evidence_stale_count: int

    hqs_ready: bool
    local_ready: bool
    voucher_ready: bool
    lease_up_ready: bool
    is_compliant: bool
    reinspect_required: bool

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


def _severity_label_from_row(row: PropertyChecklistItem) -> str:
    severity_num = int(getattr(row, "severity", 3) or 3)
    if severity_num >= 4:
        return "critical"
    if severity_num == 3:
        return "fail"
    if severity_num == 2:
        return "warn"
    return "info"


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
        severity = _severity_label_from_row(row)
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
    reinspect_required: bool,
) -> str:
    if latest_inspection_passed and failed_count == 0 and blocked_count == 0 and completion_pct >= 95.0:
        return "inspection_ready"
    if failed_critical_items > 0:
        return "critical_failures"
    if reinspect_required:
        return "reinspection_required"
    if result_status == "fail" and (failed_count > 0 or blocked_count > 0):
        return "needs_remediation"
    if readiness_status in {"needs_work", "critical", "blocked"}:
        return "not_ready"
    if completion_pct < 95.0:
        return "in_progress"
    return "unknown"


def _compute_combined_status(
    *,
    latest_result_status: str,
    latest_passed: bool,
    unresolved_failure_count: int,
    unresolved_blocked_count: int,
    unresolved_critical_count: int,
) -> tuple[str, str, bool]:
    if unresolved_critical_count > 0:
        return "critical", "fail", True
    if unresolved_failure_count > 0 or unresolved_blocked_count > 0:
        return "needs_work" if unresolved_blocked_count == 0 else "blocked", "fail", True
    if latest_result_status in {"blocked", "inconclusive"}:
        return "blocked", "fail", True
    if latest_result_status == "fail":
        return "needs_work", "fail", True
    if latest_passed or latest_result_status == "pass":
        return "ready", "pass", False
    return "unknown", "pending", True


def _jurisdiction_trust_flags(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        snapshot = build_property_projection_snapshot(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
    except Exception:
        return {}

    projection = snapshot.get("projection") or {}
    projection_reason = projection.get("projection_reason") or {}
    trust = projection_reason.get("jurisdiction_trust") or {}
    return trust if isinstance(trust, dict) else {}


def _build_property_jurisdiction_blocker(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    from ..services.workflow_gate_service import build_property_jurisdiction_blocker

    return build_property_jurisdiction_blocker(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
    )


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
    unknown_items = 0
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
            unknown_items = int(scored.unknown_items)
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
            unknown_items = int(scored.unknown_items)
            failed_critical_items = int(scored.failed_critical_items)

    checklist_summary = summarize_items(checklist_rows)
    checklist_failed_count = int(checklist_summary.get("failed_count", 0) or 0)
    checklist_blocked_count = int(checklist_summary.get("blocked_count", 0) or 0)
    unresolved_failure_count = int(checklist_summary.get("unresolved_failure_count", checklist_failed_count) or 0)
    unresolved_blocked_count = int(checklist_summary.get("unresolved_blocked_count", checklist_blocked_count) or 0)
    unresolved_critical_count = int(checklist_summary.get("unresolved_critical_count", 0) or 0)
    evidence_blocking_count = int(checklist_summary.get("evidence_blocking_count", 0) or 0)
    evidence_unknown_count = int(checklist_summary.get("evidence_unknown_count", 0) or 0)
    evidence_conflicting_count = int(checklist_summary.get("evidence_conflicting_count", 0) or 0)
    evidence_stale_count = int(checklist_summary.get("evidence_stale_count", 0) or 0)

    combined_readiness_status, combined_result_status, combined_reinspect_required = _compute_combined_status(
        latest_result_status=result_status,
        latest_passed=latest_inspection_passed,
        unresolved_failure_count=unresolved_failure_count,
        unresolved_blocked_count=unresolved_blocked_count,
        unresolved_critical_count=unresolved_critical_count,
    )

    completion_pct = round((float(passed_items + na_items) / float(max(total_items, 1))) * 100.0, 2)

    hqs_ready = unresolved_failure_count == 0 and unresolved_blocked_count == 0 and combined_result_status != "fail"
    local_ready = unresolved_critical_count == 0 and combined_result_status != "fail"
    voucher_ready = hqs_ready and local_ready
    lease_up_ready = voucher_ready and completion_pct >= 95.0 and not combined_reinspect_required

    is_compliant = bool(
        completion_pct >= 95.0
        and unresolved_failure_count == 0
        and unresolved_blocked_count == 0
        and latest_inspection_passed
        and not combined_reinspect_required
    )

    jurisdiction_blocker = _build_property_jurisdiction_blocker(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    trust = jurisdiction_blocker.get("jurisdiction_trust") or _jurisdiction_trust_flags(db, org_id=org_id, property_id=property_id)
    trust_hard_block = bool(jurisdiction_blocker.get("blocking"))

    if trust_hard_block:
        local_ready = False
        voucher_ready = False
        lease_up_ready = False
        is_compliant = False
        if combined_readiness_status == "ready":
            combined_readiness_status = "blocked"
        if combined_result_status == "pass":
            combined_result_status = "fail"
        combined_reinspect_required = True

    completion_projection_pct = _completion_projection_pct(
        completion_pct=completion_pct,
        readiness_score=readiness_score_value,
        failed_count=failed_items + unresolved_failure_count,
        blocked_count=blocked_items + unresolved_blocked_count,
        failed_critical_items=failed_critical_items + unresolved_critical_count,
    )

    posture = _inspection_grade_posture(
        readiness_status=combined_readiness_status,
        result_status=combined_result_status,
        failed_count=failed_items + unresolved_failure_count,
        blocked_count=blocked_items + unresolved_blocked_count,
        failed_critical_items=failed_critical_items + unresolved_critical_count,
        latest_inspection_passed=latest_inspection_passed,
        completion_pct=completion_pct,
        reinspect_required=combined_reinspect_required,
    )
    if trust_hard_block:
        posture = "jurisdiction_blocked"

    return InspectionReadinessScore(
        property_id=int(property_id),
        latest_inspection_id=latest_inspection_id,
        template_key=template_key,
        template_version=template_version,
        completion_pct=completion_pct,
        readiness_score=float(round(readiness_score_value, 2)),
        readiness_status=combined_readiness_status,
        result_status=combined_result_status,
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
        unresolved_failure_count=int(unresolved_failure_count),
        unresolved_blocked_count=int(unresolved_blocked_count),
        unresolved_critical_count=int(unresolved_critical_count),
        evidence_blocking_count=int(evidence_blocking_count),
        evidence_unknown_count=int(evidence_unknown_count),
        evidence_conflicting_count=int(evidence_conflicting_count),
        evidence_stale_count=int(evidence_stale_count),
        hqs_ready=bool(hqs_ready),
        local_ready=bool(local_ready),
        voucher_ready=bool(voucher_ready),
        lease_up_ready=bool(lease_up_ready),
        is_compliant=bool(is_compliant),
        reinspect_required=bool(combined_reinspect_required),
        posture=posture,
        completion_projection_pct=float(completion_projection_pct),
    )


def _projection_proof_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    snapshot = build_property_projection_snapshot(db, org_id=org_id, property_id=property_id)
    projection = snapshot.get("projection") or {}
    if not isinstance(projection, dict):
        projection = {}
    return {
        "proof_obligations": list(snapshot.get("proof_obligations") or projection.get("proof_obligations") or []),
        "proof_counts": dict(snapshot.get("proof_counts") or projection.get("proof_counts") or {}),
    }


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
    proof = _projection_proof_summary(db, org_id=org_id, property_id=property_id)
    jurisdiction_blocker = _build_property_jurisdiction_blocker(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    acquisition_ready = bool(
        score.readiness_score >= 60.0
        and score.failed_items == 0
        and score.blocked_items == 0
        and not bool(jurisdiction_blocker.get("blocking"))
    )

    acquisition_blockers: list[str] = []
    if score.failed_items > 0 or score.unresolved_failure_count > 0:
        acquisition_blockers.append("inspection_failures")
    if score.blocked_items > 0 or score.unresolved_blocked_count > 0:
        acquisition_blockers.append("inspection_blockers")
    if score.unresolved_critical_count > 0:
        acquisition_blockers.append("critical_compliance_findings")
    if bool(jurisdiction_blocker.get("blocking")):
        acquisition_blockers.append("jurisdiction_blocked")

    acquisition_next_actions: list[str] = []
    if "inspection_failures" in acquisition_blockers:
        acquisition_next_actions.append("Resolve failed inspection and checklist items.")
    if "inspection_blockers" in acquisition_blockers:
        acquisition_next_actions.append("Clear blocked inspection or evidence items.")
    if "critical_compliance_findings" in acquisition_blockers:
        acquisition_next_actions.append("Resolve critical compliance findings before proceeding.")
    if "jurisdiction_blocked" in acquisition_blockers:
        acquisition_next_actions.append("Resolve jurisdiction trust blockers or missing proof.")

    return {
        "ok": True,
        "property_id": int(property_id),
        "readiness": {
            "score": float(round(score.readiness_score, 2)),
            "status": score.readiness_status,
            "result_status": score.result_status,
            "posture": score.posture,
            "completion_pct": float(round(score.completion_pct, 2)),
            "completion_projection_pct": float(round(score.completion_projection_pct, 2)),
            "latest_inspection_passed": bool(score.latest_inspection_passed),
            "hqs_ready": bool(score.hqs_ready),
            "local_ready": bool(score.local_ready),
            "voucher_ready": bool(score.voucher_ready),
            "lease_up_ready": bool(score.lease_up_ready),
            "is_compliant": bool(score.is_compliant),
            "reinspect_required": bool(score.reinspect_required),
            "acquisition_ready": acquisition_ready,
            "acquisition_blockers": acquisition_blockers,
            "acquisition_next_actions": acquisition_next_actions,
        },
        "counts": {
            "total_items": int(score.total_items),
            "scored_items": int(score.scored_items),
            "passed_items": int(score.passed_items),
            "failed_items": int(score.failed_items),
            "blocked_items": int(score.blocked_items),
            "na_items": int(score.na_items),
            "unknown_items": int(score.unknown_items),
            "failed_critical_items": int(score.failed_critical_items),
            "checklist_failed_count": int(score.checklist_failed_count),
            "checklist_blocked_count": int(score.checklist_blocked_count),
            "unresolved_failure_count": int(score.unresolved_failure_count),
            "unresolved_blocked_count": int(score.unresolved_blocked_count),
            "unresolved_critical_count": int(score.unresolved_critical_count),
            "evidence_blocking_count": int(score.evidence_blocking_count),
            "evidence_unknown_count": int(score.evidence_unknown_count),
            "evidence_conflicting_count": int(score.evidence_conflicting_count),
            "evidence_stale_count": int(score.evidence_stale_count),
        },
        "completion": {
            "proof_obligations": list(proof.get("proof_obligations") or []),
            "proof_counts": dict(proof.get("proof_counts") or {}),
        },
        "trust_blocker_reasons": list(jurisdiction_blocker.get("blocking_reasons") or []),
        "manual_review_reasons": list(jurisdiction_blocker.get("manual_review_reasons") or []),
        "raw": asdict(score),
    }


def rebuild_and_summarize_property_readiness(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    rebuild_property_projection(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )


def build_property_readiness_with_schedule(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    """
    Chunk 2 helper: combine readiness posture with appointment/reminder state.
    Imported lazily to avoid circular imports.
    """
    from ..services.inspection_scheduling_service import build_property_schedule_summary

    readiness = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    schedule = build_property_schedule_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "readiness_summary": readiness,
        "schedule_summary": schedule,
    }