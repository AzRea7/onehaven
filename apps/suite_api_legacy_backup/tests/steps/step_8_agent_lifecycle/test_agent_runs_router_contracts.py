from __future__ import annotations

from types import SimpleNamespace

from app.routers import agent_runs as agent_runs_router


def _run(
    *,
    run_id: int,
    agent_key: str,
    property_id: int,
    status: str,
    approval_status: str = "not_required",
):
    return SimpleNamespace(
        id=run_id,
        org_id=1,
        agent_key=agent_key,
        property_id=property_id,
        status=status,
        approval_status=approval_status,
        attempts=1,
        started_at=None,
        finished_at=None,
        heartbeat_at=None,
        last_error=None,
        created_at=None,
    )


def test_agent_runs_summary_route_returns_expected_shape(client, fake_db, monkeypatch):
    fake_db.queue_scalars(
        [
            _run(run_id=101, agent_key="deal_intake", property_id=55, status="queued"),
            _run(run_id=102, agent_key="ops_judge", property_id=55, status="running"),
            _run(run_id=103, agent_key="ops_judge", property_id=55, status="running"),
            _run(run_id=104, agent_key="packet_builder", property_id=55, status="blocked", approval_status="pending"),
            _run(run_id=105, agent_key="deal_intake", property_id=55, status="done"),
            _run(run_id=106, agent_key="deal_intake", property_id=55, status="done"),
            _run(run_id=107, agent_key="next_actions", property_id=55, status="done"),
            _run(run_id=108, agent_key="timeline_nudger", property_id=55, status="failed"),
        ]
    )

    def _serialize_run(run):
        runtime_health = {
            101: "queued",
            102: "healthy",
            103: "healthy",
            104: "awaiting_approval",
            105: "terminal",
            106: "terminal",
            107: "terminal",
            108: "terminal",
        }[run.id]
        return {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "approval_status": run.approval_status,
            "runtime_health": runtime_health,
            "duration_ms": 1000 if run.status in {"done", "failed"} else None,
        }

    monkeypatch.setattr(agent_runs_router, "serialize_run", _serialize_run)

    res = client.get("/api/agent-runs/summary")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["total"] == 8
    assert out["pending_approval"] == 1
    assert out["failures"] == 1
    assert out["by_status"]["queued"] == 1
    assert out["by_status"]["running"] == 2
    assert out["by_status"]["blocked"] == 1
    assert out["by_status"]["done"] == 3
    assert out["by_status"]["failed"] == 1
    assert isinstance(out["by_agent"], list)
    assert len(out["by_agent"]) >= 1


def test_agent_runs_history_route_returns_rows_with_status_and_runtime_health(client, fake_db, monkeypatch):
    fake_db.queue_scalars(
        [
            _run(run_id=101, agent_key="deal_intake", property_id=55, status="done"),
            _run(run_id=102, agent_key="ops_judge", property_id=55, status="running"),
        ]
    )

    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "runtime_health": "terminal" if run.status == "done" else "healthy",
            "approval_status": run.approval_status,
            "attempts": 1,
            "started_at": "2026-03-12T12:00:00",
            "finished_at": "2026-03-12T12:00:02" if run.status == "done" else None,
            "duration_ms": 2150 if run.status == "done" else None,
        },
    )

    res = client.get("/api/agent-runs/history?property_id=55&limit=50")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["count"] == 2
    assert out["rows"][0]["status"] in {"done", "running"}
    assert "runtime_health" in out["rows"][0]
    assert "approval_status" in out["rows"][0]


def test_agent_runs_compare_route_returns_multiple_runs(client, fake_db, monkeypatch):
    fake_db.queue_scalars(
        [
            _run(run_id=202, agent_key="deal_intake", property_id=44, status="failed"),
            _run(run_id=201, agent_key="deal_intake", property_id=44, status="done"),
        ]
    )

    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "approval_status": run.approval_status,
            "runtime_health": "terminal",
        },
    )

    res = client.get("/api/agent-runs/compare?run_ids=201,202")
    assert res.status_code == 200, res.text

    out = res.json()
    assert len(out["rows"]) == 2
    assert out["diff"]["all_same_agent"] is True
    assert out["diff"]["all_same_property"] is True
    assert out["diff"]["agent_keys"] == ["deal_intake"]


def test_agent_runs_trace_route_returns_timeline(client, fake_db):
    fake_db.queue_scalar(
        _run(run_id=301, agent_key="deal_intake", property_id=55, status="done")
    )
    fake_db.queue_scalars(
        [
            SimpleNamespace(
                id=1,
                run_id=301,
                property_id=55,
                created_at="2026-03-12T12:00:00",
                agent_key="deal_intake",
                event_type="queued",
                payload_json='{"type":"queued","message":"Run queued","payload":{},"ts":"2026-03-12T12:00:00"}',
            ),
            SimpleNamespace(
                id=2,
                run_id=301,
                property_id=55,
                created_at="2026-03-12T12:00:01",
                agent_key="deal_intake",
                event_type="started",
                payload_json='{"type":"started","message":"Worker picked up run","payload":{},"ts":"2026-03-12T12:00:01"}',
            ),
            SimpleNamespace(
                id=3,
                run_id=301,
                property_id=55,
                created_at="2026-03-12T12:00:03",
                agent_key="deal_intake",
                event_type="finished",
                payload_json='{"type":"finished","message":"Run completed","payload":{},"ts":"2026-03-12T12:00:03"}',
            ),
        ]
    )

    res = client.get("/api/agent-runs/301/trace")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["count"] == 3
    assert out["rows"][0]["event_type"] == "queued"
    assert out["rows"][1]["event_type"] == "started"
    assert out["rows"][2]["event_type"] == "finished"


def test_property_agent_cockpit_route_returns_property_focused_view(client, fake_db, monkeypatch):
    fake_db.queue_scalars(
        [
            _run(run_id=402, agent_key="ops_judge", property_id=88, status="running"),
            _run(run_id=401, agent_key="deal_intake", property_id=88, status="done"),
        ],
        [
            SimpleNamespace(
                id=1,
                org_id=1,
                slot_key="deal_intake",
                property_id=88,
                owner_type="agent",
                assignee="deal_intake",
                status="done",
                notes=None,
                updated_at="2026-03-12T12:05:00",
            ),
            SimpleNamespace(
                id=2,
                org_id=1,
                slot_key="ops_judge",
                property_id=88,
                owner_type="agent",
                assignee="ops_judge",
                status="running",
                notes=None,
                updated_at="2026-03-12T12:06:00",
            ),
        ],
        [
            _run(run_id=402, agent_key="ops_judge", property_id=88, status="running"),
            _run(run_id=401, agent_key="deal_intake", property_id=88, status="done"),
        ],
    )

    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "approval_status": run.approval_status,
            "runtime_health": "healthy" if run.status == "running" else "terminal",
        },
    )
    monkeypatch.setattr(
        agent_runs_router,
        "serialize_run",
        lambda run: {
            "id": run.id,
            "agent_key": run.agent_key,
            "property_id": run.property_id,
            "status": run.status,
            "approval_status": run.approval_status,
            "runtime_health": "healthy" if run.status == "running" else "terminal",
            "duration_ms": None,
        },
    )

    res = client.get("/api/agent-runs/property/88/cockpit")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["property_id"] == 88
    assert "summary" in out
    assert len(out["latest_runs"]) == 2
    assert len(out["slots"]) == 2
    assert out["slots"][0]["slot_key"] in {"deal_intake", "ops_judge"}


def test_reject_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(agent_runs_router, "require_owner", lambda principal: None)
    monkeypatch.setattr(
        agent_runs_router,
        "reject_run",
        lambda db, org_id, actor_user_id, run_id, reason: SimpleNamespace(
            id=run_id,
            org_id=org_id,
            agent_key="packet_builder",
            property_id=55,
            status="blocked",
            approval_status="rejected",
            attempts=1,
            started_at=None,
            finished_at=None,
            heartbeat_at=None,
            last_error=reason,
            created_at=None,
        ),
    )
    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "status": run.status,
            "approval_status": run.approval_status,
            "last_error": run.last_error,
        },
    )

    res = client.post("/api/agent-runs/501/reject?reason=invalid_proposed_mutation")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run"]["id"] == 501
    assert out["run"]["status"] == "blocked"
    assert out["run"]["approval_status"] == "rejected"


def test_approve_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(agent_runs_router, "require_owner", lambda principal: None)
    monkeypatch.setattr(
        agent_runs_router,
        "mark_approved",
        lambda db, org_id, actor_user_id, run_id: SimpleNamespace(
            id=run_id,
            org_id=org_id,
            agent_key="packet_builder",
            property_id=55,
            status="blocked",
            approval_status="approved",
            attempts=1,
            started_at=None,
            finished_at=None,
            heartbeat_at=None,
            last_error=None,
            created_at=None,
        ),
    )
    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "status": run.status,
            "approval_status": run.approval_status,
        },
    )

    res = client.post("/api/agent-runs/502/approve")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["run"]["id"] == 502
    assert out["run"]["approval_status"] == "approved"


def test_apply_run_route_is_wired(client, monkeypatch):
    monkeypatch.setattr(agent_runs_router, "require_owner", lambda principal: None)
    monkeypatch.setattr(
        agent_runs_router,
        "apply_approved",
        lambda db, org_id, actor_user_id, run_id: {
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


def test_retry_run_route_is_wired(client, fake_db, monkeypatch):
    monkeypatch.setattr(agent_runs_router, "require_owner", lambda principal: None)

    fake_db.queue_scalar(
        SimpleNamespace(
            id=504,
            org_id=1,
            agent_key="deal_intake",
            property_id=77,
            status="failed",
            approval_status="not_required",
            attempts=1,
            started_at="2026-03-12T12:00:00",
            finished_at="2026-03-12T12:00:05",
            heartbeat_at="2026-03-12T12:00:04",
            last_error="boom",
            created_at=None,
            approved_at=None,
            approved_by_user_id=None,
        )
    )

    queued = []

    class _DelayStub:
        @staticmethod
        def delay(org_id, run_id):
            queued.append((org_id, run_id))

    monkeypatch.setattr(agent_runs_router, "execute_agent_run", _DelayStub)
    monkeypatch.setattr(
        agent_runs_router,
        "_serialize_run_detail",
        lambda db, run: {
            "id": run.id,
            "status": run.status,
            "approval_status": run.approval_status,
        },
    )

    res = client.post("/api/agent-runs/504/retry")
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["queued"] is True
    assert out["run"]["id"] == 504
    assert out["run"]["status"] == "queued"
    assert queued == [(1, 504)]
    