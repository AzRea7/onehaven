from app.models import IngestionSource


def test_sync_now_endpoint_queues_task(client_with_auth_headers, db_session):
    client, headers = client_with_auth_headers

    source = IngestionSource(
        org_id=1,
        provider="zillow",
        slug="zillow-api",
        display_name="Zillow API",
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

    resp = client.post(f"/api/ingestion/sources/{source.id}/sync", headers=headers, json={"trigger_type": "manual"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert "queued" in body