from __future__ import annotations


def test_ops_property_summary_returns_endgame_sections(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/ops/property/{prop.id}/summary?cash_days=90")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["property"]["id"] == prop.id
    assert body["stage"] == "equity"
    assert "tenant" in body
    assert "cash" in body
    assert "equity" in body
    assert "endgame" in body
    assert "decision_health" in body

    assert body["tenant"]["occupancy_status"] == "occupied"
    assert body["cash"]["last_30_days"]["income"] == 1650.0
    assert body["equity"]["estimated_equity"] == 50500.0
    assert body["endgame"]["tenant_ready"] is True
    assert body["endgame"]["equity_tracked"] is True


def test_ops_workflow_summary_returns_current_stage(client, seed_endgame_data):
    prop = seed_endgame_data["property"]

    res = client.get(f"/api/ops/property/{prop.id}/workflow")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["current_stage"] == "equity"
    assert body["current_stage_label"] == "Equity"
    assert len(body["next_actions"]) >= 1