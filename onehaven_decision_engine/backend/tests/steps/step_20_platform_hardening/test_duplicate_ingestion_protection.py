from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, IngestionRecordLink, Property
from app.services.ingestion_run_execute import execute_source_sync


def test_same_source_payload_does_not_create_duplicate_properties_or_deals(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-dupe-org", name="Step20 Duplicate Org")

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-duplicate-protection",
        sample_rows=[dict(sample_listing_payload)],
    )

    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )
    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="retry",
        runtime_config={"limit": 10},
    )

    props = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()
    links = db_session.scalars(select(IngestionRecordLink).where(IngestionRecordLink.org_id == org.id)).all()

    assert len(props) == 1
    assert len(deals) == 1
    assert len(links) == 1
    assert len(fake_post_pipeline) >= 1


def test_retry_run_does_not_multiply_records_when_external_id_matches(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-dupe-external", name="Step20 External ID Org")
    payload = dict(sample_listing_payload)
    payload["external_record_id"] = "dedupe-key-1"

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-external-id",
        sample_rows=[payload],
    )

    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )
    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    props = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()

    assert len(props) == 1
    assert len(deals) == 1