# backend/tests/steps/step_10_automated_ingestion/test_ingestion_sync_now.py
from __future__ import annotations

from app.models import IngestionSource
from app.routers import ingestion as router_mod


def test_sync_now_endpoint_queues_task_with_runtime_filters(
    client_with_auth_headers,
    auth_context,
    db_session,
    monkeypatch,
):
    client, headers = client_with_auth_headers
    org_id = auth_context["org"].id

    source = IngestionSource(
        org_id=org_id,
        provider="rentcast",
        slug="rentcast-sale-listings",
        display_name="RentCast Sale Listings",
        source_type="api",
        status="connected",
        is_enabled=True,
        config_json={},
        credentials_json={},
        cursor_json={},
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    captured = {}

    class DummyJob:
        id = "task-123"

    def fake_delay(org_id, source_id, trigger_type, runtime_config):
        captured["org_id"] = org_id
        captured["source_id"] = source_id
        captured["trigger_type"] = trigger_type
        captured["runtime_config"] = runtime_config
        return DummyJob()

    monkeypatch.setattr(router_mod.sync_source_task, "delay", fake_delay)

    resp = client.post(
        f"/api/ingestion/sources/{source.id}/sync",
        headers=headers,
        json={
            "trigger_type": "manual",
            "state": "MI",
            "county": "wayne",
            "city": "Detroit",
            "max_price": 125000,
            "min_bedrooms": 3,
            "limit": 75,
        },
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["ok"] is True
    assert body["queued"] is True
    assert body["task_id"] == "task-123"

    assert captured["org_id"] == org_id
    assert captured["source_id"] == source.id
    assert captured["trigger_type"] == "manual"
    assert captured["runtime_config"]["city"] == "Detroit"
    assert captured["runtime_config"]["county"] == "wayne"
    assert captured["runtime_config"]["max_price"] == 125000
    assert captured["runtime_config"]["min_bedrooms"] == 3
    assert captured["runtime_config"]["limit"] == 75


def test_sync_now_endpoint_execute_inline_uses_same_pipeline_path(
    client_with_auth_headers,
    auth_context,
    db_session,
    monkeypatch,
):
    client, headers = client_with_auth_headers
    org_id = auth_context["org"].id

    source = IngestionSource(
        org_id=org_id,
        provider="rentcast",
        slug="rentcast-sale-listings-inline",
        display_name="RentCast Sale Listings Inline",
        source_type="api",
        status="connected",
        is_enabled=True,
        config_json={},
        credentials_json={},
        cursor_json={},
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    captured = {}

    def fake_execute_source_sync(db, org_id, source, trigger_type, runtime_config):
        captured["org_id"] = org_id
        captured["source_id"] = source.id
        captured["trigger_type"] = trigger_type
        captured["runtime_config"] = dict(runtime_config)
        return type(
            "InlineRun",
            (),
            {
                "id": 456,
                "status": "success",
                "source_id": source.id,
                "trigger_type": trigger_type,
                "summary_json": {
                    "records_seen": 10,
                    "records_imported": 3,
                    "properties_created": 2,
                    "properties_updated": 1,
                    "deals_created": 2,
                    "deals_updated": 1,
                    "rent_rows_upserted": 3,
                    "photos_upserted": 6,
                    "duplicates_skipped": 0,
                    "invalid_rows": 0,
                    "filtered_out": 7,
                    "geo_enriched": 3,
                    "risk_scored": 3,
                    "rent_refreshed": 3,
                    "evaluated": 3,
                    "state_synced": 3,
                    "workflow_synced": 3,
                    "next_actions_seeded": 3,
                    "post_import_failures": 0,
                    "post_import_partials": 0,
                    "post_import_errors": [],
                    "filter_reason_counts": {"max_price": 7},
                },
            },
        )()

    monkeypatch.setattr(router_mod, "execute_source_sync", fake_execute_source_sync)

    resp = client.post(
        f"/api/ingestion/sources/{source.id}/sync",
        headers=headers,
        json={
            "trigger_type": "manual",
            "execute_inline": True,
            "state": "MI",
            "county": "wayne",
            "city": "Detroit",
            "max_price": 125000,
            "limit": 50,
        },
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["ok"] is True
    assert body["queued"] is False
    assert body["normal_path"] is True
    assert body["run_id"] == 456
    assert body["status"] == "success"

    assert captured["org_id"] == org_id
    assert captured["source_id"] == source.id
    assert captured["trigger_type"] == "manual"
    assert captured["runtime_config"]["city"] == "Detroit"
    assert captured["runtime_config"]["county"] == "wayne"
    assert captured["runtime_config"]["limit"] == 50

    outcome = body["pipeline_outcome"]
    assert outcome["records_imported"] == 3
    assert outcome["enrichments_completed"]["geo"] == 3
    assert outcome["enrichments_completed"]["risk"] == 3
    assert outcome["enrichments_completed"]["rent"] == 3
    assert outcome["evaluations_completed"] == 3
    assert outcome["workflow"]["state_synced"] == 3
    assert outcome["workflow"]["workflow_synced"] == 3
    assert outcome["workflow"]["next_actions_seeded"] == 3
    assert outcome["filter_reason_counts"] == {"max_price": 7}
    