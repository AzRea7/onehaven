from __future__ import annotations

from datetime import datetime

from app.models import InspectionItem, PropertyChecklistItem
from app.services.inspection_readiness_service import (
    build_property_readiness_summary,
    compute_property_readiness_score,
)


def _seed_checklist(db_session, *, org_id: int, property_id: int):
    db_session.add_all(
        [
            PropertyChecklistItem(
                org_id=org_id,
                property_id=property_id,
                item_code="SMOKE_DETECTORS",
                category="safety",
                description="Smoke detectors",
                severity=3,
                common_fail=True,
                status="done",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
            PropertyChecklistItem(
                org_id=org_id,
                property_id=property_id,
                item_code="GFCI_KITCHEN",
                category="electrical",
                description="Kitchen GFCI",
                severity=3,
                common_fail=True,
                status="failed",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ),
        ]
    )
    db_session.commit()


def test_compute_property_readiness_score_detects_not_ready(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_checklist(db_session, org_id=org.id, property_id=prop.id)

    db_session.add_all(
        [
            InspectionItem(
                inspection_id=insp.id,
                code="SMOKE_DETECTORS",
                failed=False,
                severity=3,
                details="ok",
            ),
            InspectionItem(
                inspection_id=insp.id,
                code="GFCI_KITCHEN",
                failed=True,
                severity=4,
                details="missing",
            ),
        ]
    )
    db_session.commit()

    score = compute_property_readiness_score(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert score.property_id == prop.id
    assert score.total_items >= 2
    assert score.failed_items >= 1
    assert score.failed_critical_items >= 1
    assert score.hqs_ready is False
    assert score.voucher_ready is False
    assert score.posture in {"critical_failures", "needs_remediation", "not_ready"}


def test_build_property_readiness_summary_returns_structured_payload(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="SMOKE_DETECTORS",
            failed=False,
            severity=3,
            details="ok",
        )
    )
    db_session.commit()

    summary = build_property_readiness_summary(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert summary["ok"] is True
    assert summary["property_id"] == prop.id
    assert "completion" in summary
    assert "readiness" in summary
    assert "counts" in summary
    assert "raw" in summary