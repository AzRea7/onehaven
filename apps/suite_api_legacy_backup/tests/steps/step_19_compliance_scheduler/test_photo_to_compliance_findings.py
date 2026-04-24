from __future__ import annotations

from app.models import RehabTask
from app.services.compliance_photo_analysis_service import (
    analyze_property_photos_for_compliance,
    create_compliance_tasks_from_photo_analysis,
)


def test_analyze_property_photos_returns_rule_mapped_findings(photo_seed, db_session):
    org = photo_seed["org"]
    prop = photo_seed["property"]
    inspection = photo_seed["inspection"]

    analysis = analyze_property_photos_for_compliance(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=inspection.id,
    )

    assert analysis["ok"] is True
    assert analysis["property_id"] == prop.id
    assert analysis["photo_count"] == 2
    assert analysis["summary"]["interior"] == 1
    assert analysis["summary"]["exterior"] == 1
    assert len(analysis["findings"]) >= 2

    first = analysis["findings"][0]
    assert "code" in first
    assert "observed_issue" in first
    assert "recommended_fix" in first
    assert "rule_mapping" in first
    assert first["rule_mapping"]["template_key"] == "hud_52580a"


def test_create_compliance_tasks_from_photo_analysis_is_idempotent(photo_seed, db_session):
    org = photo_seed["org"]
    prop = photo_seed["property"]
    inspection = photo_seed["inspection"]

    analysis = analyze_property_photos_for_compliance(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=inspection.id,
    )
    selected_codes = [row["code"] for row in analysis["findings"][:2]]

    first = create_compliance_tasks_from_photo_analysis(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        analysis=analysis,
        confirmed_codes=selected_codes,
        mark_blocking=True,
    )
    second = create_compliance_tasks_from_photo_analysis(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        analysis=analysis,
        confirmed_codes=selected_codes,
        mark_blocking=True,
    )

    tasks = db_session.query(RehabTask).filter(
        RehabTask.org_id == org.id,
        RehabTask.property_id == prop.id,
    ).all()

    assert first["ok"] is True
    assert first["created"] == len(first["created_task_ids"])
    assert first["created"] >= 1
    assert second["created"] == 0
    assert len(tasks) == first["created"]
    assert all(task.notes for task in tasks)
