from __future__ import annotations

from dataclasses import dataclass

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

    failed_count = int(readiness.failed_items + readiness.checklist_failed_count)
    blocked_count = int(readiness.blocked_items + readiness.checklist_blocked_count)

    return ComplianceStatus(
        completion_pct=float(round(readiness.completion_pct, 2)),
        completion_projection_pct=float(round(readiness.completion_projection_pct, 2)),
        failed_count=failed_count,
        blocked_count=blocked_count,
        latest_inspection_passed=bool(readiness.latest_inspection_passed),
        latest_readiness_score=float(round(readiness.readiness_score, 2)),
        latest_readiness_status=readiness.readiness_status,
        latest_result_status=readiness.result_status,
        posture=readiness.posture,
        is_compliant=bool(readiness.is_compliant),
    )