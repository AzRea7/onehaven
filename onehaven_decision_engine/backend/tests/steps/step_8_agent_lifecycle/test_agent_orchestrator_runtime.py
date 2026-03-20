from __future__ import annotations

from types import SimpleNamespace

from app.services import agent_orchestrator_runtime


def test_on_run_terminal_noop_when_run_missing(monkeypatch):
    class FakeDB:
        def scalar(self, *args, **kwargs):
            return None

    called = {"planned": False, "trace": False}

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda *args, **kwargs: called.__setitem__("planned", True),
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "emit_trace_safe",
        lambda *args, **kwargs: called.__setitem__("trace", True),
    )

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=999)

    assert called["planned"] is False
    assert called["trace"] is False


def test_on_run_terminal_noop_for_non_terminal_status(monkeypatch):
    run = SimpleNamespace(
        id=10,
        org_id=1,
        property_id=123,
        status="running",
        created_by_user_id=99,
    )

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return run

    called = {"planned": False, "trace": False}

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda *args, **kwargs: called.__setitem__("planned", True),
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "emit_trace_safe",
        lambda *args, **kwargs: called.__setitem__("trace", True),
    )

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=10)

    assert called["planned"] is False
    assert called["trace"] is False


def test_on_run_terminal_noop_when_property_missing(monkeypatch):
    run = SimpleNamespace(
        id=11,
        org_id=1,
        property_id=None,
        status="done",
        created_by_user_id=99,
    )

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return run

    called = {"planned": False}

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda *args, **kwargs: called.__setitem__("planned", True),
    )

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=11)
    assert called["planned"] is False


def test_on_run_terminal_creates_followup_runs_and_emits_trace(monkeypatch):
    terminal_run = SimpleNamespace(
        id=12,
        org_id=1,
        property_id=456,
        status="done",
        created_by_user_id=99,
        agent_key="deal_intake",
    )

    planned = [
        SimpleNamespace(agent_key="deal_intake", property_id=456, idempotency_key="deal:456:v1"),
        SimpleNamespace(agent_key="ops_judge", property_id=456, idempotency_key="judge:456:v1"),
    ]

    created = {
        "deal:456:v1": SimpleNamespace(id=1001, status="queued", agent_key="deal_intake"),
        "judge:456:v1": SimpleNamespace(id=1002, status="done", agent_key="ops_judge"),
    }

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return terminal_run

    traces = []

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda db, org_id, property_id: planned,
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "create_run",
        lambda db, org_id, actor_user_id, agent_key, property_id, input_payload, idempotency_key: created[idempotency_key],
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "serialize_run",
        lambda run: {
            "id": run.id,
            "status": run.status,
            "agent_key": run.agent_key,
        },
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "emit_trace_safe",
        lambda *args, **kwargs: traces.append(kwargs),
    )

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=12)

    assert len(traces) >= 1
    # created_ids are collected for both planned follow-ups
    assert any("created_run_ids" in (t.get("payload") or {}) for t in traces)


def test_on_run_terminal_emits_no_followup_trace_when_plan_is_empty(monkeypatch):
    blocked_run = SimpleNamespace(
        id=13,
        org_id=1,
        property_id=457,
        status="blocked",
        created_by_user_id=99,
        agent_key="ops_judge",
    )

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return blocked_run

    traces = []

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda db, org_id, property_id: [],
    )
    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "emit_trace_safe",
        lambda *args, **kwargs: traces.append(kwargs),
    )

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=13)

    assert len(traces) == 1
    assert traces[0]["event_type"] == "orchestrator_no_followups"
    assert traces[0]["payload"]["property_id"] == 457
    