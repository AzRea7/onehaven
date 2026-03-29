from __future__ import annotations

from sqlalchemy import select

from app.models import Deal, Property, PropertyState, RentAssumption
from app.services.ingestion_run_execute import execute_source_sync


def _replace_sample_rows(db_session, source, rows: list[dict]) -> None:
    source.config_json = {**dict(source.config_json or {}), "sample_rows": rows}
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)



def _get_json(client, path: str, headers: dict, **params):
    response = client.get(path, headers=headers, params=params)
    assert response.status_code == 200, response.text
    return response.json()



def test_end_to_end_property_pipeline_reconciles_listing_lifecycle_and_detail_payload(
    db_session,
    org_factory,
    principal_factory,
    client_with_principal,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-e2e", name="Step20 E2E")
    principal = principal_factory(org=org)
    client, headers = client_with_principal(principal)

    active_payload = {
        **dict(sample_listing_payload),
        "external_record_id": "ext-step20-e2e-1",
        "asking_price": 85000,
        "listing_price": 85000,
        "listing_status": "Active",
        "listing_days_on_market": 12,
        "listing_listed_at": "2026-03-20T00:00:00Z",
        "listing_last_seen_at": "2026-03-28T00:00:00Z",
        "listing_zillow_url": "https://www.zillow.com/homedetails/123-Main-St-Detroit-MI-48226/",
        "listing_agent_name": "Alice Agent",
        "listing_agent_phone": "313-555-0101",
        "listing_agent_email": "alice@example.com",
        "listing_office_name": "Main Street Realty",
        "listing_office_phone": "313-555-0202",
        "listing_office_email": "office@example.com",
    }

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-e2e",
        sample_rows=[active_payload],
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
    prop_id = int(prop.id)
    assert prop.address == "123 Main St"
    assert prop.normalized_address is not None
    assert prop.geocode_source == "google"
    assert str(prop.listing_status or "").lower() == "active"
    assert bool(prop.listing_hidden) is False
    assert float(prop.listing_price or 0) == 85000

    visible_rows_after_first = _get_json(client, "/properties", headers, limit=25)
    assert [int(row["id"]) for row in visible_rows_after_first] == [prop_id]

    detail_after_first = _get_json(client, f"/properties/{prop_id}/view", headers)
    property_payload = detail_after_first["property"]
    assert str(property_payload["listing_status"]).lower() == "active"
    assert property_payload["listing_hidden"] is False
    assert float(property_payload["listing_price"]) == 85000
    assert property_payload["listing_days_on_market"] == 12
    assert property_payload["listing_zillow_url"]
    assert property_payload["listing_agent_name"] == "Alice Agent"
    assert property_payload["listing_office_name"] == "Main Street Realty"

    inactive_payload = {
        **active_payload,
        "asking_price": 79000,
        "listing_price": 79000,
        "listing_status": "Inactive",
        "listing_removed_at": "2026-03-29T00:00:00Z",
        "listing_last_seen_at": "2026-03-29T00:00:00Z",
    }
    _replace_sample_rows(db_session, source, [inactive_payload])

    run2 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual_retry",
        runtime_config={"limit": 10},
    )
    assert run2 is not None

    props_after_second = db_session.scalars(
        select(Property).where(Property.org_id == org.id)
    ).all()
    deals_after_second = db_session.scalars(
        select(Deal).where(Deal.org_id == org.id)
    ).all()
    assert len(props_after_second) == 1
    assert len(deals_after_second) == 1

    prop_after_second = props_after_second[0]
    assert int(prop_after_second.id) == prop_id
    assert str(prop_after_second.listing_status or "").lower() == "inactive"
    assert bool(prop_after_second.listing_hidden) is True
    assert float(prop_after_second.listing_price or 0) == 79000
    assert prop_after_second.listing_removed_at is not None

    visible_rows_after_inactive = _get_json(client, "/properties", headers, limit=25)
    assert visible_rows_after_inactive == []

    hidden_rows_after_inactive = _get_json(
        client,
        "/properties",
        headers,
        limit=25,
        include_hidden=True,
    )
    assert [int(row["id"]) for row in hidden_rows_after_inactive] == [prop_id]
    assert str(hidden_rows_after_inactive[0]["listing_status"]).lower() == "inactive"
    assert bool(hidden_rows_after_inactive[0]["listing_hidden"]) is True

    revived_payload = {
        **active_payload,
        "asking_price": 76000,
        "listing_price": 76000,
        "listing_status": "Active",
        "listing_last_seen_at": "2026-03-30T00:00:00Z",
    }
    _replace_sample_rows(db_session, source, [revived_payload])

    run3 = execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual_reactivate",
        runtime_config={"limit": 10},
    )
    assert run3 is not None

    final_prop = db_session.get(Property, prop_id)
    assert final_prop is not None
    assert str(final_prop.listing_status or "").lower() == "active"
    assert bool(final_prop.listing_hidden) is False
    assert float(final_prop.listing_price or 0) == 76000

    visible_rows_after_revival = _get_json(client, "/properties", headers, limit=25)
    assert [int(row["id"]) for row in visible_rows_after_revival] == [prop_id]

    final_detail = _get_json(client, f"/properties/{prop_id}/view", headers)
    final_property_payload = final_detail["property"]
    assert str(final_property_payload["listing_status"]).lower() == "active"
    assert final_property_payload["listing_hidden"] is False
    assert float(final_property_payload["listing_price"]) == 76000
    assert final_property_payload["listing_zillow_url"].startswith("https://")
    assert final_property_payload["listing_agent_phone"] == "313-555-0101"
    assert final_property_payload["listing_office_email"] == "office@example.com"
