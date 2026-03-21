from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, Property, PropertyState, RentAssumption
from app.services.ingestion_run_execute import execute_source_sync


def test_end_to_end_property_pipeline_is_rerun_safe(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-e2e", name="Step20 E2E")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-e2e",
        sample_rows=[dict(sample_listing_payload)],
    )

    run1 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    props_after_first = db_session.scalars(
        select(Property).where(Property.org_id == org.id)
    ).all()
    deals_after_first = db_session.scalars(
        select(Deal).where(Deal.org_id == org.id)
    ).all()
    states_after_first = db_session.scalars(
        select(PropertyState).where(PropertyState.org_id == org.id)
    ).all()
    rents_after_first = db_session.scalars(
        select(RentAssumption).where(RentAssumption.org_id == org.id)
    ).all()

    assert run1 is not None
    assert len(props_after_first) == 1
    assert len(deals_after_first) == 1
    assert len(states_after_first) == 1
    assert len(rents_after_first) == 1
    assert len(fake_post_pipeline) == 1

    prop = props_after_first[0]
    assert prop.address == "123 Main St"
    assert prop.normalized_address is not None
    assert prop.geocode_source == "google"

    run2 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual_retry",
        runtime_config={"limit": 10},
    )

    props_after_second = db_session.scalars(
        select(Property).where(Property.org_id == org.id)
    ).all()
    deals_after_second = db_session.scalars(
        select(Deal).where(Deal.org_id == org.id)
    ).all()
    states_after_second = db_session.scalars(
        select(PropertyState).where(PropertyState.org_id == org.id)
    ).all()
    rents_after_second = db_session.scalars(
        select(RentAssumption).where(RentAssumption.org_id == org.id)
    ).all()

    assert run2 is not None
    assert len(props_after_second) == 1
    assert len(deals_after_second) == 1
    assert len(states_after_second) == 1
    assert len(rents_after_second) == 1

    # rerun is allowed, but must remain dedupe-safe
    assert len(fake_post_pipeline) >= 1