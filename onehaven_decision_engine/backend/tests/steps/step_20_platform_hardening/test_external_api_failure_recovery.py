from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, Property
from app.services.ingestion_run_execute import execute_source_sync


def test_external_service_failure_does_not_corrupt_recoverable_property_state(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    external_api_failure,
):
    org = org_factory(slug="step20-failure-org", name="Step20 Failure Org")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-failure-source",
        sample_rows=[dict(sample_listing_payload)],
    )

    run = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    props = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()

    assert run is not None
    assert len(props) == 1
    assert len(deals) == 1
    assert len(external_api_failure) == 1


def test_partial_failure_path_is_consistent_and_rerunnable(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    external_api_failure,
    fake_post_pipeline,
    monkeypatch,
):
    org = org_factory(slug="step20-retry-org", name="Step20 Retry Org")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-retry-source",
        sample_rows=[dict(sample_listing_payload)],
    )

    first_run = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    assert first_run is not None
    assert len(external_api_failure) == 1

    # swap pipeline from failure to success
    monkeypatch.setattr(
        "app.services.ingestion_run_execute.execute_post_ingestion_pipeline",
        lambda db_session, *, org_id, property_id, actor_user_id=None, emit_events=False: {
            "geo_ok": True,
            "risk_ok": True,
            "rent_ok": True,
            "evaluate_ok": True,
            "state_ok": True,
            "workflow_ok": True,
            "next_actions_ok": True,
            "partial": False,
            "errors": [],
        },
    )

    second_run = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="retry",
        runtime_config={"limit": 10},
    )

    props = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()

    assert second_run is not None
    assert len(props) == 1
    assert len(deals) == 1