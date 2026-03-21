from __future__ import annotations

from app.models import InspectionItem, RehabTask
from app.services.inspection_failure_task_service import (
    build_failure_next_actions,
    collect_failure_task_blueprints,
    create_failure_tasks_from_inspection,
)


def _seed_failures(db_session, inspection_id: int):
    db_session.add_all(
        [
            InspectionItem(
                inspection_id=inspection_id,
                code="SMOKE_DETECTOR_MISSING",
                failed=True,
                severity=4,
                details="missing first floor",
                location="hallway",
            ),
            InspectionItem(
                inspection_id=inspection_id,
                code="GFCI_MISSING",
                failed=True,
                severity=3,
                details="kitchen sink wall",
                location="kitchen",
            ),
        ]
    )
    db_session.commit()


def test_collect_failure_task_blueprints(real_inspection_seed, real_inspection, db_session):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_failures(db_session, insp.id)

    result = collect_failure_task_blueprints(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=insp.id,
    )

    assert result["ok"] is True
    assert result["inspection_id"] == insp.id
    assert result["counts"]["failure_like_items"] >= 2
    assert len(result["blueprints"]) >= 2


def test_create_failure_tasks_from_inspection_is_idempotent(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_failures(db_session, insp.id)

    first = create_failure_tasks_from_inspection(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=insp.id,
    )
    db_session.commit()

    second = create_failure_tasks_from_inspection(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=insp.id,
    )
    db_session.commit()

    assert first["created"] >= 2
    assert second["created"] == 0
    assert second["skipped_existing"] >= 2

    tasks = db_session.query(RehabTask).filter(
        RehabTask.org_id == org.id,
        RehabTask.property_id == prop.id,
    ).all()
    assert len(tasks) >= 2


def test_build_failure_next_actions_returns_ranked_actions(
    real_inspection_seed,
    real_inspection,
    db_session,
):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]
    insp = real_inspection

    _seed_failures(db_session, insp.id)

    result = build_failure_next_actions(
        db_session,
        org_id=org.id,
        property_id=prop.id,
        inspection_id=insp.id,
        limit=10,
    )

    assert result["ok"] is True
    assert result["inspection_id"] == insp.id
    assert isinstance(result["recommended_actions"], list)
    assert isinstance(result["top_fail_points"], list)
    assert len(result["recommended_actions"]) >= 2