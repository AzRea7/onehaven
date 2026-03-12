from __future__ import annotations

from types import SimpleNamespace

from app.routers import agent_runs as agent_runs_router


def test_agent_runs_summary_route_returns_expected_shape(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "build_runs_summary",
        lambda db, org_id, property_id=None: {
            "ok": True,
            "org_id": org_id,
            "property_id": property_id,
            "totals": {
                "all": 8,
                "queued": 1,
                "running": 2,
                "blocked": 1,
                "done": 3,
                "failed": 1,
            },
            "approval": {
                "awaiting": 1,
                "approved": 2,
                "rejected": 1,
            },
            "health": {
                "healthy": 4,
                "lagging": 1,
                "stale": 1,
                "terminal": 2,
            },
        },
    )

    res = client.get("/api/agent-runs/summary")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["org_id"] == 1
    assert out["totals"]["all"] == 8
    assert "health" in out
    assert "approval" in out


def test_agent_runs_history_route_returns_rows_with_status_and_runtime_health(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "list_run_history",
        lambda db, org_id, property_id=None, limit=50, agent_key=None, status=None: {
            "ok": True,
            "items": [
                {
                    "id": 101,
                    "agent_key": "deal_intake",
                    "property_id": 55,
                    "status": "done",
                    "runtime_health": "terminal",
                    "approval_status": "not_required",
                    "attempts": 1,
                    "started_at": "2026-03-12T12:00:00",
                    "finished_at": "2026-03-12T12:00:02",
                    "duration_ms": 2150,
                },
                {
                    "id": 102,
                    "agent_key": "ops_judge",
                    "property_id": 55,
                    "status": "running",
                    "runtime_health": "healthy",
                    "approval_status": "not_required",
                    "attempts": 1,
                    "started_at": "2026-03-12T12:10:00",
                    "finished_at": None,
                    "duration_ms": None,
                },
            ],
            "count": 2,
        },
    )

    res = client.get("/api/agent-runs/history?property_id=55&limit=20")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["count"] == 2
    assert out["items"][0]["status"] == "done"
    assert out["items"][0]["runtime_health"] == "terminal"
    assert out["items"][1]["status"] == "running"


def test_agent_runs_compare_route_returns_multiple_runs(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "compare_runs",
        lambda db, org_id, run_ids: {
            "ok": True,
            "items": [
                {
                    "id": 201,
                    "agent_key": "deal_intake",
                    "status": "done",
                    "approval_status": "not_required",
                    "runtime_health": "terminal",
                    "property_id": 44,
                },
                {
                    "id": 202,
                    "agent_key": "deal_intake",
                    "status": "failed",
                    "approval_status": "not_required",
                    "runtime_health": "terminal",
                    "property_id": 44,
                },
            ],
            "count": 2,
        },
    )

    res = client.get("/api/agent-runs/compare?run_ids=201&run_ids=202")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["count"] == 2
    assert {x["id"] for x in out["items"]} == {201, 202}


def test_agent_runs_trace_route_returns_timeline(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "get_run_trace",
        lambda db, org_id, run_id: {
            "ok": True,
            "run_id": run_id,
            "events": [
                {"ts": "2026-03-12T12:00:00", "event_type": "queued", "message": "Run queued"},
                {"ts": "2026-03-12T12:00:01", "event_type": "started", "message": "Worker picked up run"},
                {"ts": "2026-03-12T12:00:03", "event_type": "finished", "message": "Run completed"},
            ],
        },
    )

    res = client.get("/api/agent-runs/301/trace")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run_id"] == 301
    assert len(out["events"]) == 3
    assert out["events"][1]["event_type"] == "started"


def test_property_agent_cockpit_route_returns_property_focused_view(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "build_property_agent_cockpit",
        lambda db, org_id, property_id: {
            "ok": True,
            "property_id": property_id,
            "summary": {"queued": 1, "running": 1, "blocked": 1, "done": 4, "failed": 0},
            "latest_runs": [
                {"id": 401, "agent_key": "deal_intake", "status": "done"},
                {"id": 402, "agent_key": "ops_judge", "status": "running"},
            ],
            "slots": [
                {"slot_key": "deal_intake", "status": "done"},
                {"slot_key": "ops_judge", "status": "running"},
            ],
        },
    )

    res = client.get("/api/agent-runs/property/88/cockpit")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["property_id"] == 88
    assert out["summary"]["running"] == 1
    assert len(out["slots"]) == 2


def test_reject_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "reject_run",
        lambda db, org_id, run_id, actor_user_id, reason=None: {
            "ok": True,
            "run_id": run_id,
            "status": "blocked",
            "approval_status": "rejected",
            "reason": reason or "rejected by operator",
        },
    )

    res = client.post(
        "/api/agent-runs/501/reject",
        json={"reason": "invalid proposed mutation"},
    )
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run_id"] == 501
    assert out["approval_status"] == "rejected"


def test_approve_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "approve_run",
        lambda db, org_id, run_id, actor_user_id: {
            "ok": True,
            "run_id": run_id,
            "status": "blocked",
            "approval_status": "approved",
        },
    )

    res = client.post("/api/agent-runs/502/approve")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run_id"] == 502
    assert out["approval_status"] == "approved"


def test_apply_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "apply_run_actions",
        lambda db, org_id, run_id, actor_user_id: {
            "ok": True,
            "run_id": run_id,
            "status": "done",
            "applied": True,
            "applied_count": 2,
        },
    )

    res = client.post("/api/agent-runs/503/apply")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run_id"] == 503
    assert out["applied"] is True
    assert out["applied_count"] == 2


def test_retry_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "retry_run",
        lambda db, org_id, run_id, actor_user_id: {
            "ok": True,
            "old_run_id": run_id,
            "new_run_id": 9001,
            "status": "queued",
        },
    )

    res = client.post("/api/agent-runs/504/retry")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["old_run_id"] == 504
    assert out["new_run_id"] == 9001
    assert out["status"] == "queued"