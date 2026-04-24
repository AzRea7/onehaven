from __future__ import annotations


def test_equity_snapshot_returns_latest_and_kpis(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/equity/property/{prop.id}/snapshot")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["has_valuation"] is True
    assert body["latest"] is not None
    assert body["kpis"]["estimated_value"] == 152000.0
    assert body["kpis"]["loan_balance"] == 101500.0
    assert body["kpis"]["estimated_equity"] == 50500.0


def test_equity_timeline_returns_items(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/equity/property/{prop.id}/timeline")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["count"] >= 1
    assert len(body["items"]) >= 1
    assert body["items"][0]["estimated_equity"] == 50500.0


def test_equity_suggestions_returns_future_dates(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(
        f"/api/equity/valuation/suggestions?property_id={prop.id}&cadence=quarterly&count=3"
    )
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["cadence"] == "quarterly"
    assert len(body["suggestions"]) == 3