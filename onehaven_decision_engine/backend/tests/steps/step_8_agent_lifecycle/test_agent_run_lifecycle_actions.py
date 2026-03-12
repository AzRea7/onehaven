from __future__ import annotations

from app.routers import agent_runs as agent_runs_router


def test_plan_enqueue_and_dispatch_routes_are_wired(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "plan_runs_for_property",
        lambda db, org_id, property_id: {
            "ok": True,
            "property_id": property_id,
            "planned": [
                {"agent_key": "deal_intake", "idempotency_key": "deal:91:v1"},
                {"agent_key": "ops_judge", "idempotency_key": "judge:91:v1"},
            ],
        },
    )
    monkeypatch.setattr(
        agent_runs_router,
        "enqueue_planned_runs",
        lambda db, org_id, property_id, actor_user_id: {
            "ok": True,
            "property_id": property_id,
            "created_run_ids": [7001, 7002],
        },
    )
    monkeypatch.setattr(
        agent_runs_router,
        "dispatch_run",
        lambda db, org_id, run_id: {
            "ok": True,
            "run_id": run_id,
            "status": "queued",
            "dispatched": True,
        },
    )

    res_plan = client.post("/api/agent-runs/plan", json={"property_id": 91})
    assert res_plan.status_code == 200, res_plan.text
    assert res_plan.json()["planned"][0]["agent_key"] == "deal_intake"

    res_enqueue = client.post("/api/agent-runs/enqueue", json={"property_id": 91})
    assert res_enqueue.status_code == 200, res_enqueue.text
    assert res_enqueue.json()["created_run_ids"] == [7001, 7002]

    res_dispatch = client.post("/api/agent-runs/7001/dispatch")
    assert res_dispatch.status_code == 200, res_dispatch.text
    assert res_dispatch.json()["dispatched"] is True


def test_run_detail_route_returns_full_lifecycle_record(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "get_run_detail",
        lambda db, org_id, run_id: {
            "ok": True,
            "id": run_id,
            "agent_key": "packet_builder",
            "property_id": 222,
            "status": "blocked",
            "runtime_health": "awaiting_approval",
            "approval_status": "pending",
            "attempts": 2,
            "last_error": None,
            "proposed_actions": [{"type": "create_checklist_item"}],
        },
    )

    res = client.get("/api/agent-runs/611")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["id"] == 611
    assert out["status"] == "blocked"
    assert out["runtime_health"] == "awaiting_approval"
    assert out["approval_status"] == "pending"
    assert out["attempts"] == 2
    