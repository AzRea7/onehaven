from __future__ import annotations


def test_property_snapshot_returns_occupancy_and_active_lease(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/tenants/property/{prop.id}/snapshot")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["occupancy_status"] == "occupied"
    assert body["counts"]["active_leases"] == 1
    assert body["active_lease"] is not None
    assert body["active_lease"]["status"] == "active"
    assert body["active_lease"]["is_section8_like"] is True


def test_pipeline_returns_counts_and_rows(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/tenants/pipeline?property_id={prop.id}")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["counts"]["total"] >= 1
    assert body["counts"]["active"] == 1
    assert body["counts"]["voucher_backed"] == 1
    assert len(body["rows"]) >= 1
    assert body["rows"][0]["status"] == "active"