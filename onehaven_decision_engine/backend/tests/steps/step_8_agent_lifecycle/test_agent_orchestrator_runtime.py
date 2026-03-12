from __future__ import annotations

from types import SimpleNamespace

from app.services import agent_orchestrator_runtime


def test_on_run_terminal_noop_when_run_missing(monkeypatch):
    class FakeDB:
        def scalar(self, *args, **kwargs):
            return None

    called = {"planned": False, "delay": False}

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda *args, **kwargs: called.__setitem__("planned", True),
    )

    class FakeTask:
        @staticmethod
        def delay(*args, **kwargs):
            called["delay"] = True

    monkeypatch.setattr(agent_orchestrator_runtime, "execute_agent_run", FakeTask)

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=999)

    assert called["planned"] is False
    assert called["delay"] is False


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

    called = {"planned": False, "delay": False}

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda *args, **kwargs: called.__setitem__("planned", True),
    )

    class FakeTask:
        @staticmethod
        def delay(*args, **kwargs):
            called["delay"] = True

    monkeypatch.setattr(agent_orchestrator_runtime, "execute_agent_run", FakeTask)

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=10)

    assert called["planned"] is False
    assert called["delay"] is False


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


def test_on_run_terminal_plans_and_dispatches_only_newly_queued_runs(monkeypatch):
    terminal_run = SimpleNamespace(
        id=12,
        org_id=1,
        property_id=456,
        status="done",
        created_by_user_id=99,
    )

    planned = [
        SimpleNamespace(agent_key="deal_intake", property_id=456, idempotency_key="deal:456:v1"),
        SimpleNamespace(agent_key="ops_judge", property_id=456, idempotency_key="judge:456:v1"),
    ]

    created = {
        "deal:456:v1": SimpleNamespace(id=1001, status="queued"),
        "judge:456:v1": SimpleNamespace(id=1002, status="done"),  # old/idempotent existing run
    }

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return terminal_run

    delayed = []

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

    class FakeTask:
        @staticmethod
        def delay(*args, **kwargs):
            delayed.append(kwargs)

    monkeypatch.setattr(agent_orchestrator_runtime, "execute_agent_run", FakeTask)

    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=12)

    assert len(delayed) == 1
    assert delayed[0]["org_id"] == 1
    assert delayed[0]["run_id"] == 1001


def test_on_run_terminal_also_handles_blocked_runs(monkeypatch):
    blocked_run = SimpleNamespace(
        id=13,
        org_id=1,
        property_id=457,
        status="blocked",
        created_by_user_id=99,
    )

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return blocked_run

    monkeypatch.setattr(
        agent_orchestrator_runtime,
        "plan_agent_runs",
        lambda db, org_id, property_id: [],
    )

    class FakeTask:
        @staticmethod
        def delay(*args, **kwargs):
            raise AssertionError("should not dispatch anything for empty plan")

    monkeypatch.setattr(agent_orchestrator_runtime, "execute_agent_run", FakeTask)

    # Should simply not explode. Software occasionally tries to explode for fun.
    agent_orchestrator_runtime.on_run_terminal(FakeDB(), org_id=1, run_id=13)