from app.models import IngestionSource
from app.routers import ingestion as router_mod


def test_sync_now_endpoint_queues_task_with_runtime_filters(
    client_with_auth_headers,
    db_session,
    monkeypatch,
):
    client, headers = client_with_auth_headers

    source = IngestionSource(
        org_id=1,
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

    assert captured["org_id"] == 1
    assert captured["source_id"] == source.id
    assert captured["trigger_type"] == "manual"
    assert captured["runtime_config"]["city"] == "Detroit"
    assert captured["runtime_config"]["county"] == "wayne"
    assert captured["runtime_config"]["max_price"] == 125000
    assert captured["runtime_config"]["min_bedrooms"] == 3
    assert captured["runtime_config"]["limit"] == 50
    