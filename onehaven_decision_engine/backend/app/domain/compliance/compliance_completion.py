from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ...services.inspection_readiness_service import compute_property_readiness_score


@dataclass(frozen=True)
class ComplianceStatus:
    completion_pct: float
    completion_projection_pct: float
    failed_count: int
    blocked_count: int
    latest_inspection_passed: bool
    latest_readiness_score: float
    latest_readiness_status: str
    latest_result_status: str
    posture: str
    is_compliant: bool
    unresolved_count: int = 0
    reinspection_needed: bool = False


def _extract_latest_inspection_summary(readiness: Any) -> dict[str, Any]:
    latest = getattr(readiness, "latest_inspection_summary", None) or {}
    if not isinstance(latest, dict):
        latest = {}

    latest_failed = int(latest.get("failed", 0) or 0)
    latest_blocked = int(latest.get("blocked", 0) or 0)
    latest_pending = int(latest.get("pending", 0) or 0)
    latest_inconclusive = int(latest.get("inconclusive", 0) or 0)

    unresolved = latest_failed + latest_blocked + latest_pending + latest_inconclusive
    return {
        "failed": latest_failed,
        "blocked": latest_blocked,
        "pending": latest_pending,
        "inconclusive": latest_inconclusive,
        "unresolved": unresolved,
        "passed": unresolved == 0 and bool(latest),
    }


def compute_compliance_status(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> ComplianceStatus:
    readiness = compute_property_readiness_score(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    latest_summary = _extract_latest_inspection_summary(readiness)

    failed_count = int(
        max(
            latest_summary["failed"],
            int(getattr(readiness, "failed_items", 0) or 0) + int(getattr(readiness, "checklist_failed_count", 0) or 0),
        )
    )
    blocked_count = int(
        max(
            latest_summary["blocked"],
            int(getattr(readiness, "blocked_items", 0) or 0) + int(getattr(readiness, "checklist_blocked_count", 0) or 0),
        )
    )

    unresolved_count = int(
        max(
            latest_summary["unresolved"],
            failed_count + blocked_count,
        )
    )

    latest_inspection_passed = bool(
        getattr(readiness, "latest_inspection_passed", False) or latest_summary["passed"]
    )

    return ComplianceStatus(
        completion_pct=float(round(float(getattr(readiness, "completion_pct", 0.0) or 0.0), 2)),
        completion_projection_pct=float(
            round(float(getattr(readiness, "completion_projection_pct", 0.0) or 0.0), 2)
        ),
        failed_count=failed_count,
        blocked_count=blocked_count,
        latest_inspection_passed=latest_inspection_passed,
        latest_readiness_score=float(round(float(getattr(readiness, "readiness_score", 0.0) or 0.0), 2)),
        latest_readiness_status=str(getattr(readiness, "readiness_status", "unknown") or "unknown"),
        latest_result_status=str(getattr(readiness, "result_status", "unknown") or "unknown"),
        posture=str(getattr(readiness, "posture", "unknown") or "unknown"),
        is_compliant=bool(getattr(readiness, "is_compliant", False) and unresolved_count == 0),
        unresolved_count=unresolved_count,
        reinspection_needed=bool(
            unresolved_count > 0
            or getattr(readiness, "reinspection_needed", False)
            or getattr(readiness, "checklist_reinspection_needed", False)
        ),
    )
