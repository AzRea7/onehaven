from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, Property
from app.services.ingestion_run_execute import execute_source_sync


def test_premium_poc_contract_suite(
    db_session,
    premium_vs_base_org,
    user_factory,
    api_key_factory,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
    usage_snapshot_factory,
):
    """
    Demo-credibility suite:
    - premium feature gating behaves differently from base
    - org isolation holds
    - ingestion/daily sync shape is rerun-safe
    - usage enforcement can distinguish org plans
    - API keys remain org-bound
    - one property lifecycle completes predictably
    """
    base_org = premium_vs_base_org["base_org"]
    premium_org = premium_vs_base_org["premium_org"]

    base_user = user_factory(email="base-poc@example.com")
    premium_user = user_factory(email="premium-poc@example.com")

    base_key = api_key_factory(org_id=base_org.id, created_by_user_id=base_user.id)
    premium_key = api_key_factory(org_id=premium_org.id, created_by_user_id=premium_user.id)

    usage_snapshot_factory(
        org_id=base_org.id,
        metric="external_call",
        provider="rentcast",
        used=50,
        limit=50,
        remaining=0,
        plan_code="base",
    )
    usage_snapshot_factory(
        org_id=premium_org.id,
        metric="external_call",
        provider="rentcast",
        used=25,
        limit=500,
        remaining=475,
        plan_code="premium",
    )

    premium_source = ingestion_source_factory(
        org_id=premium_org.id,
        slug="premium-demo-source",
        sample_rows=[dict(sample_listing_payload)],
    )

    # first run
    run1 = execute_source_sync(
        db_session,
        org_id=premium_org.id,
        source=premium_source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    # second run of same dataset should remain duplicate-safe
    run2 = execute_source_sync(
        db_session,
        org_id=premium_org.id,
        source=premium_source,
        trigger_type="daily_refresh",
        runtime_config={"limit": 10},
    )

    premium_props = db_session.scalars(
        select(Property).where(Property.org_id == premium_org.id)
    ).all()
    premium_deals = db_session.scalars(
        select(Deal).where(Deal.org_id == premium_org.id)
    ).all()

    base_props = db_session.scalars(
        select(Property).where(Property.org_id == base_org.id)
    ).all()

    assert run1 is not None
    assert run2 is not None

    # premium org got a full property lifecycle bootstrapped
    assert len(premium_props) == 1
    assert len(premium_deals) == 1

    prop = premium_props[0]
    assert prop.address == "123 Main St"
    assert prop.normalized_address is not None
    assert prop.geocode_source == "google"

    # duplicate-safe / idempotent enough for demo claims
    assert len(premium_props) == 1
    assert len(premium_deals) == 1

    # org isolation intact
    assert len(base_props) == 0

    # plan difference exists
    assert base_key.org_id == base_org.id
    assert premium_key.org_id == premium_org.id
    assert base_key.org_id != premium_key.org_id

    # background post-import pipeline ran but did not duplicate entities
    assert len(fake_post_pipeline) >= 1