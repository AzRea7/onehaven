from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import select

from app.models import Deal, IngestionRecordLink, Property
from app.routers import properties as properties_router
from products.acquire.backend.src.services.ingestion_run_execute import execute_source_sync



def _replace_sample_rows(db_session, source, rows: list[dict]) -> None:
    source.config_json = {**dict(source.config_json or {}), "sample_rows": rows}
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)



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



def test_retry_run_updates_price_and_status_in_place_without_creating_duplicates(
    db_session,
    org_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-dupe-update", name="Step20 In Place Update Org")

    first_payload = {
        **dict(sample_listing_payload),
        "external_record_id": "dedupe-key-1",
        "listing_status": "Active",
        "asking_price": 85000,
        "listing_price": 85000,
    }
    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-external-id",
        sample_rows=[first_payload],
    )

    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    first_prop = db_session.scalars(select(Property).where(Property.org_id == org.id)).one()
    first_prop_id = int(first_prop.id)

    second_payload = {
        **first_payload,
        "asking_price": 79999,
        "listing_price": 79999,
        "listing_status": "Inactive",
        "listing_removed_at": "2026-03-29T00:00:00Z",
    }
    _replace_sample_rows(db_session, source, [second_payload])

    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    props = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()
    links = db_session.scalars(select(IngestionRecordLink).where(IngestionRecordLink.org_id == org.id)).all()

    assert len(props) == 1
    assert len(deals) == 1
    assert len(links) == 1

    prop = props[0]
    assert int(prop.id) == first_prop_id
    assert str(prop.listing_status or "").lower() == "inactive"
    assert bool(prop.listing_hidden) is True
    assert float(prop.listing_price or 0) == 79999



def test_deals_only_rankings_ignore_hidden_and_non_candidate_rows(monkeypatch):
    rows = [
        {
            "id": 101,
            "rank_score": 90.0,
            "normalized_decision": "GOOD",
            "current_workflow_stage": "deal",
            "deal_filter_status": "visible",
            "is_deal_candidate": True,
            "listing_hidden": False,
        },
        {
            "id": 102,
            "rank_score": 150.0,
            "normalized_decision": "GOOD",
            "current_workflow_stage": "deal",
            "deal_filter_status": "hidden",
            "is_deal_candidate": True,
            "listing_hidden": True,
        },
        {
            "id": 103,
            "rank_score": 120.0,
            "normalized_decision": "REJECT",
            "current_workflow_stage": "deal",
            "deal_filter_status": "suppressed",
            "is_deal_candidate": False,
            "listing_hidden": False,
        },
    ]

    monkeypatch.setattr(
        properties_router,
        "build_inventory_snapshots_for_scope",
        lambda *args, **kwargs: {"rows": list(rows)},
    )

    result = properties_router.list_properties(
        state=None,
        city=None,
        county=None,
        q=None,
        stage=None,
        decision=None,
        only_red_zone=False,
        exclude_red_zone=False,
        min_crime_score=None,
        max_crime_score=None,
        min_offender_count=None,
        max_offender_count=None,
        hide_stale=False,
        hide_very_stale=False,
        freshness=None,
        limit=50,
        db=SimpleNamespace(),
        p=SimpleNamespace(org_id=1, id=1, user_id=1),
        include_hidden=True,
        deals_only=True,
        include_suppressed=False,
        sort="rank_score",
    )

    assert [int(row["id"]) for row in result] == [101]
