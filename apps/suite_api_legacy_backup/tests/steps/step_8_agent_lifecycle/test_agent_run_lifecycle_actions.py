from __future__ import annotations

from types import SimpleNamespace

from app.routers import agent_runs as agent_runs_router


def test_plan_enqueue_and_dispatch_routes_are_wired(client, fake_db, monkeypatch):
    monkeypatch.setattr(
        agent_runs_router,
        "plan_agent_runs",
        lambda db, org_id, property_id: [
            SimpleNamespace(
                agent_key="deal_intake",
                property_id=property_id,
                reason="deal required",
                idempotency_key="deal:91:v1",
            ),
            SimpleNamespace(
                agent_key="ops_judge",
                property_id=property_id,
                reason="judge required",
                idempotency_key="judge:91:v1",
            ),
        ],
    )

    created_runs = [
        SimpleNamespace(
            id=7001,
            org_id=1,
            agent_key="deal_intake",
            property_id=91,
            status="queued",
            approval_status="not_required",
        ),
        SimpleNamespace(
            id=7002,
            org_id=1,
            agent_key="ops_judge",
            property_id=91,
            status="queued",
            approval_status="not_required",
        ),
    ]

    state = {"idx": 0}

    def _create_run(db, org_id, actor_user_id, agent_key, property_id, input_payload, idempotency_key):
        run = created_runs[state["idx"]]
        state["idx"] += 1
        return run

    monkeypatch.setattr(agent_runs_router, "create_run", _create_run)
    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
        },
    )

    dispatched = []

    class _DelayStub:
        @staticmethod
        def delay(org_id, run_id):
            dispatched.append((org_id, run_id))

    monkeypatch.setattr(agent_runs_router, "execute_agent_run", _DelayStub)

    res_plan = client.post("/api/agent-runs/plan?property_id=91")
    assert res_plan.status_code == 200, res_plan.text
    assert res_plan.json()[0]["agent_key"] == "deal_intake"
    assert res_plan.json()[1]["idempotency_key"] == "judge:91:v1"

    res_enqueue = client.post("/api/agent-runs/enqueue?property_id=91")
    assert res_enqueue.status_code == 200, res_enqueue.text
    out = res_enqueue.json()
    assert out["planned"] == 2
    assert [row["id"] for row in out["created"]] == [7001, 7002]
    assert dispatched == [(1, 7001), (1, 7002)]

    fake_db.queue_scalar(
        SimpleNamespace(
            id=7001,
            org_id=1,
            agent_key="deal_intake",
            property_id=91,
            status="queued",
            approval_status="not_required",
        )
    )

    res_dispatch = client.post("/api/agent-runs/7001/dispatch")
    assert res_dispatch.status_code == 200, res_dispatch.text
    assert res_dispatch.json()["ok"] is True
    assert res_dispatch.json()["queued"] is True
    assert res_dispatch.json()["run_id"] == 7001
    assert dispatched[-1] == (1, 7001)


def test_run_detail_route_returns_full_lifecycle_record(client, fake_db, monkeypatch):
    fake_db.queue_scalar(
        SimpleNamespace(
            id=611,
            org_id=1,
            agent_key="packet_builder",
            property_id=222,
            status="blocked",
            approval_status="pending",
            attempts=2,
            started_at=None,
            finished_at=None,
            heartbeat_at=None,
            last_error=None,
            created_at=None,
        )
    )

    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "ok": True,
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "runtime_health": "awaiting_approval",
            "approval_status": run.approval_status,
            "attempts": run.attempts,
            "last_error": run.last_error,
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
    assert out["proposed_actions"] == [{"type": "create_checklist_item"}]
    