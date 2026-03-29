from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, Property
from app.services.ingestion_run_execute import execute_source_sync



def _get_json(client, path: str, headers: dict, **params):
    response = client.get(path, headers=headers, params=params)
    assert response.status_code == 200, response.text
    return response.json()



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



def test_partial_failure_keeps_listing_fields_and_retry_recovers_detail_payload(
    db_session,
    org_factory,
    principal_factory,
    client_with_principal,
    ingestion_source_factory,
    sample_listing_payload,
    external_api_failure,
    monkeypatch,
):
    org = org_factory(slug="step20-retry-org", name="Step20 Retry Org")
    principal = principal_factory(org=org)
    client, headers = client_with_principal(principal)

    payload = {
        **dict(sample_listing_payload),
        "external_record_id": "step20-failure-detail-1",
        "listing_status": "Inactive",
        "listing_price": 81234,
        "asking_price": 81234,
        "listing_days_on_market": 21,
        "listing_last_seen_at": "2026-03-29T00:00:00Z",
        "listing_removed_at": "2026-03-29T00:00:00Z",
        "listing_zillow_url": "https://www.zillow.com/homedetails/failure-case/",
        "listing_agent_name": "Failure Case Agent",
        "listing_office_name": "Failure Case Office",
    }

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-retry-source",
        sample_rows=[payload],
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

    props_after_failure = db_session.scalars(
        select(Property).where(Property.org_id == org.id)
    ).all()
    deals_after_failure = db_session.scalars(
        select(Deal).where(Deal.org_id == org.id)
    ).all()

    assert len(props_after_failure) == 1
    assert len(deals_after_failure) == 1

    prop = props_after_failure[0]
    prop_id = int(prop.id)
    assert str(prop.listing_status or "").lower() == "inactive"
    assert bool(prop.listing_hidden) is True
    assert float(prop.listing_price or 0) == 81234
    assert prop.listing_removed_at is not None

    visible_rows = _get_json(client, "/properties", headers, limit=25)
    assert visible_rows == []

    hidden_rows = _get_json(client, "/properties", headers, limit=25, include_hidden=True)
    assert [int(row["id"]) for row in hidden_rows] == [prop_id]
    assert bool(hidden_rows[0]["listing_hidden"]) is True

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

    props_after_retry = db_session.scalars(
        select(Property).where(Property.org_id == org.id)
    ).all()
    deals_after_retry = db_session.scalars(
        select(Deal).where(Deal.org_id == org.id)
    ).all()

    assert second_run is not None
    assert len(props_after_retry) == 1
    assert len(deals_after_retry) == 1

    detail = _get_json(client, f"/properties/{prop_id}/view", headers)
    property_payload = detail["property"]
    assert str(property_payload["listing_status"]).lower() == "inactive"
    assert property_payload["listing_hidden"] is True
    assert float(property_payload["listing_price"]) == 81234
    assert property_payload["listing_days_on_market"] == 21
    assert property_payload["listing_zillow_url"] == "https://www.zillow.com/homedetails/failure-case/"
    assert property_payload["listing_agent_name"] == "Failure Case Agent"
    assert property_payload["listing_office_name"] == "Failure Case Office"
