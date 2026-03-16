from __future__ import annotations

from datetime import datetime


def test_cash_snapshot_returns_collection_and_windows(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/cash/property/{prop.id}/snapshot?days=90")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["days"] == 90
    assert "last_30_days" in body
    assert "last_90_days" in body
    assert body["expected_rent_window"] >= 0
    assert body["collection_rate_pct"] >= 0
    assert body["last_30_days"]["income"] == 1650.0


def test_cash_rollup_returns_yearly_kpis(client, seed_endgame_data):
    prop = seed_endgame_data["property"]
    year = datetime.utcnow().year

    res = client.get(f"/api/cash/rollup?property_id={prop.id}&year={year}")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["year"] == year
    assert len(body["months"]) == 12
    assert "kpis" in body
    assert body["kpis"]["collected_income"] >= 1650.0
    assert body["kpis"]["capex"] >= 1800.0


def test_cash_ledger_returns_running_effect(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/cash/property/{prop.id}/ledger?days=180")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property_id"] == prop.id
    assert body["count"] >= 3
    assert all("running_cash_effect" in row for row in body["rows"])