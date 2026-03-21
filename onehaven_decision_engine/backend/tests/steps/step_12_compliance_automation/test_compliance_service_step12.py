from __future__ import annotations

from datetime import datetime

from app.models import (
    Inspection,
    InspectionItem,
    Organization,
    Property,
    PropertyChecklistItem,
    PropertyState,
    RehabTask,
)
from app.services.compliance_service import (
    apply_inspection_form_results,
    build_property_inspection_readiness,
    generate_policy_tasks_for_property,
    run_hqs,
)
from app.services.property_state_machine import sync_property_state


def _seed(db):
    org = Organization(slug="step12-service-org", name="Step12 Service Org")
    db.add(org)
    db.commit()
    db.refresh(org)

    prop = Property(
        org_id=org.id,
        address="456 Readiness St",
        city="Warren",
        state="MI",
        zip="48091",
        county="Macomb",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1300,
        year_built=1965,
        has_garage=False,
        property_type="single_family",
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)

    db.add(
        PropertyState(
            org_id=org.id,
            property_id=prop.id,
            current_stage="compliance",
            constraints_json="{}",
            outstanding_tasks_json="{}",
            updated_at=datetime.utcnow(),
        )
    )
    db.commit()
    return org, prop


def _create_inspection(db, *, org_id: int, property_id: int):
    insp = Inspection(
        org_id=org_id,
        property_id=property_id,
        inspection_date=datetime.utcnow(),
        passed=False,
        reinspect_required=True,
        notes="new real-form inspection",
    )
    db.add(insp)
    db.commit()
    db.refresh(insp)
    return insp


def test_apply_inspection_form_results_maps_payload_and_creates_failure_tasks(db_session):
    org, prop = _seed(db_session)
    insp = _create_inspection(db_session, org_id=org.id, property_id=prop.id)

    raw_payload = {
        "items": [
            {
                "code": "SMOKE_DETECTORS",
                "result": "pass",
                "details": "all good",
            },
            {
                "code": "GFCI_KITCHEN",
                "result": "fail",
                "details": "missing GFCI by sink",
                "location": "kitchen",
            },
        ]
    }

    result = apply_inspection_form_results(
        db_session,
        org_id=org.id,
        actor_user_id=1,
        property_id=prop.id,
        inspection_id=insp.id,
        raw_payload=raw_payload,
        sync_checklist=True,
        create_failure_tasks=True,
    )

    assert result["ok"] is True
    assert result["inspection_id"] == insp.id
    assert result["mapped_count"] >= 1
    assert "readiness" in result
    assert "failure_tasks" in result

    rows = db_session.query(InspectionItem).filter(
        InspectionItem.inspection_id == insp.id
    ).all()
    assert len(rows) >= 1

    tasks = db_session.query(RehabTask).filter(
        RehabTask.org_id == org.id,
        RehabTask.property_id == prop.id,
    ).all()
    assert len(tasks) >= 1


def test_build_property_inspection_readiness_returns_real_projection(db_session):
    org, prop = _seed(db_session)
    insp = _create_inspection(db_session, org_id=org.id, property_id=prop.id)

    db_session.add_all(
        [
            PropertyChecklistItem(
                org_id=org.id,
                property_id=prop.id,
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
                org_id=org.id,
                property_id=prop.id,
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
    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="GFCI_KITCHEN",
            failed=True,
            severity=3,
            details="missing",
            location="kitchen",
        )
    )
    db_session.commit()

    body = build_property_inspection_readiness(
        db_session,
        org_id=org.id,
        property_id=prop.id,
    )

    assert body["ok"] is True
    assert body["property"]["id"] == prop.id
    assert "score_pct" in body
    assert "completion_pct" in body
    assert "completion_projection_pct" in body
    assert "posture" in body
    assert "readiness_summary" in body
    assert "inspection_failure_actions" in body
    assert body["counts"]["failing"] >= 1


def test_generate_policy_tasks_for_property_includes_failure_tasks(db_session):
    org, prop = _seed(db_session)
    insp = _create_inspection(db_session, org_id=org.id, property_id=prop.id)

    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="SMOKE_DETECTOR_MISSING",
            failed=True,
            severity=4,
            details="none present",
            location="hallway",
        )
    )
    db_session.commit()

    result = generate_policy_tasks_for_property(
        db_session,
        org_id=org.id,
        actor_user_id=1,
        property_id=prop.id,
    )

    assert result["ok"] is True
    assert result["property_id"] == prop.id
    assert "inspection_failure_tasks" in result
    assert result["inspection_failure_tasks"]["created"] >= 1

    titles = result["titles"]
    assert isinstance(titles, list)
    assert len(titles) >= 1


def test_run_hqs_returns_readiness_and_task_generation(db_session):
    org, prop = _seed(db_session)
    insp = _create_inspection(db_session, org_id=org.id, property_id=prop.id)

    db_session.add(
        InspectionItem(
            inspection_id=insp.id,
            code="HANDRAIL_MISSING",
            failed=True,
            severity=3,
            details="rear stairs",
            location="rear stairs",
        )
    )
    db_session.commit()
    sync_property_state(db_session, org_id=org.id, property_id=prop.id)
    db_session.commit()

    result = run_hqs(
        db_session,
        org_id=org.id,
        actor_user_id=1,
        property_id=prop.id,
        create_tasks=True,
    )

    assert result["ok"] is True
    assert "inspection_readiness" in result
    assert "task_generation" in result
    assert "readiness_summary" in result
    