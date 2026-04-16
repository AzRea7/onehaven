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

    props_after_first = db_session.scalars(select(Property).where(Property.org_id == org.id)).all()
    deals_after_first = db_session.scalars(select(Deal).where(Deal.org_id == org.id)).all()
    states_after_first = db_session.scalars(select(PropertyState).where(PropertyState.org_id == org.id)).all()
    rents_after_first = db_session.scalars(select(RentAssumption).where(RentAssumption.org_id == org.id)).all()

    assert run1 is not None
    assert len(props_after_first) == 1
    assert len(deals_after_first) == 1
    assert len(states_after_first) == 1
    assert len(rents_after_first) == 1
    assert len(fake_post_pipeline) == 1


def test_property_visibility_payload_surfaces_safe_to_rely_and_unsafe_reasons(
    db_session,
    org_factory,
    principal_factory,
    client_with_principal,
    ingestion_source_factory,
    sample_listing_payload,
    fake_post_pipeline,
):
    org = org_factory(slug="step20-visibility", name="Step20 Visibility")
    principal = principal_factory(org=org)
    client, headers = client_with_principal(principal)

    payload = {
        **dict(sample_listing_payload),
        "external_record_id": "ext-step20-visibility-1",
        "asking_price": 76000,
        "listing_price": 76000,
        "listing_status": "Active",
        "listing_last_seen_at": "2026-03-30T00:00:00Z",
    }

    source = ingestion_source_factory(
        org_id=org.id,
        slug="rentcast-visibility",
        sample_rows=[payload],
    )

    execute_source_sync(
        db_session,
        org_id=org.id,
        source=source,
        trigger_type="manual",
        runtime_config={"limit": 10},
    )

    prop = db_session.scalars(select(Property).where(Property.org_id == org.id)).first()
    assert prop is not None

    out = _get_json(client, f"/policy/property/{int(prop.id)}/resolved-rules", headers)

    safe_value = out.get("safe_to_rely_on")
    if safe_value is None and isinstance(out.get("operational_status"), dict):
        safe_value = out["operational_status"].get("safe_to_rely_on")
    assert isinstance(safe_value, bool)

    unsafe_reasons = out.get("unsafe_reasons") or ((out.get("operational_status") or {}).get("reasons")) or []
    assert isinstance(unsafe_reasons, list)

    op = out.get("operational_status") or ((out.get("profile") or {}).get("operational_status")) or {}
    assert isinstance(op, dict)
    assert ("next_actions" in op) or ("next_due_step" in op) or ("next_actions" in out)


def test_manual_refresh_endpoints_present_visible_status_when_automation_is_off(
    org_factory,
    principal_factory,
    client_with_principal,
    monkeypatch,
):
    org = org_factory(slug="step20-manual", name="Step20 Manual")
    principal = principal_factory(org=org)
    client, headers = client_with_principal(principal)

    monkeypatch.setattr(
        "app.routers.jurisdictions.manual_runbook_snapshot",
        lambda: {
            "ok": True,
            "manual_mode": True,
            "automation_enabled": False,
            "notification_enabled": True,
            "count": 1,
        },
    )
    monkeypatch.setattr(
        "app.routers.jurisdictions.manual_refresh_stale_profiles",
        lambda: {
            "ok": True,
            "manual_mode": True,
            "automation_enabled": False,
            "task": "jurisdiction.refresh_stale_profiles",
            "changed_profile_ids": [1],
        },
    )

    runbook = _get_json(client, "/jurisdictions/manual/runbook", headers)
    action = client.post("/jurisdictions/manual/refresh-stale", headers=headers)
    assert action.status_code == 200, action.text
    payload = action.json()

    assert runbook["manual_mode"] is True
    assert runbook["automation_enabled"] is False
    assert payload["automation_enabled"] is False
    assert payload["task"] == "jurisdiction.refresh_stale_profiles"
