# onehaven_decision_engine/backend/app/domain/compliance/compliance_completion.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session

from ...models import PropertyChecklistItem, Inspection


@dataclass(frozen=True)
class ComplianceStatus:
    completion_pct: float
    failed_count: int
    latest_inspection_passed: bool
    is_compliant: bool


def compute_compliance_status(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> ComplianceStatus:
    # Completion % from checklist items
    total = db.scalar(
        select(func.count()).select_from(PropertyChecklistItem)
        .where(PropertyChecklistItem.org_id == org_id)
        .where(PropertyChecklistItem.property_id == property_id)
    ) or 0

    done = db.scalar(
        select(func.count()).select_from(PropertyChecklistItem)
        .where(PropertyChecklistItem.org_id == org_id)
        .where(PropertyChecklistItem.property_id == property_id)
        .where(PropertyChecklistItem.status == "done")
    ) or 0

    failed = db.scalar(
        select(func.count()).select_from(PropertyChecklistItem)
        .where(PropertyChecklistItem.org_id == org_id)
        .where(PropertyChecklistItem.property_id == property_id)
        .where(PropertyChecklistItem.status == "failed")
    ) or 0

    completion_pct = (float(done) / float(total) * 100.0) if total > 0 else 0.0

    # Latest inspection passed?
    latest = db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id)
        .where(Inspection.property_id == property_id)
        .order_by(desc(Inspection.inspected_at))
        .limit(1)
    )
    latest_passed = bool(getattr(latest, "passed", False)) if latest is not None else False

    # Completion rule
    is_compliant = (completion_pct >= 95.0) and (int(failed) == 0) and latest_passed

    return ComplianceStatus(
        completion_pct=float(round(completion_pct, 2)),
        failed_count=int(failed),
        latest_inspection_passed=bool(latest_passed),
        is_compliant=bool(is_compliant),
    )