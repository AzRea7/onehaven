from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app.services import agent_engine


def _run(
    *,
    status: str,
    approval_status: str = "not_required",
    heartbeat_at=None,
    started_at=None,
    finished_at=None,
):
    return SimpleNamespace(
        id=1,
        org_id=1,
        agent_key="deal_intake",
        property_id=77,
        status=status,
        approval_status=approval_status,
        heartbeat_at=heartbeat_at,
        started_at=started_at,
        finished_at=finished_at,
        attempts=1,
        last_error=None,
        created_at=datetime.utcnow(),
        proposed_actions_json=None,
        input_payload_json="{}",
        output_payload_json="{}",
    )


def _patch_now(monkeypatch, now: datetime):
    if hasattr(agent_engine, "_now_utc"):
        monkeypatch.setattr(agent_engine, "_now_utc", lambda: now)


@pytest.mark.parametrize(
    "status,approval_status,expected",
    [
        ("queued", "not_required", "queued"),
        ("done", "not_required", "terminal"),
        ("failed", "not_required", "terminal"),
        ("timed_out", "not_required", "terminal"),
        ("blocked", "pending", "awaiting_approval"),
        ("blocked", "approved", "terminal"),
        ("blocked", "rejected", "terminal"),
    ],
)
def test_infer_runtime_health_for_non_running_statuses(monkeypatch, status, approval_status, expected):
    now = datetime(2026, 3, 12, 12, 0, 0)
    _patch_now(monkeypatch, now)

    assert hasattr(agent_engine, "infer_runtime_health"), (
        "agent_engine.py should expose infer_runtime_health() "
        "after the step 8 lifecycle update"
    )

    out = agent_engine.infer_runtime_health(
        _run(status=status, approval_status=approval_status),
    )
    assert out == expected


def test_infer_runtime_health_returns_healthy_for_recent_heartbeat(monkeypatch):
    now = datetime(2026, 3, 12, 12, 0, 0)
    _patch_now(monkeypatch, now)

    run = _run(
        status="running",
        heartbeat_at=now - timedelta(seconds=10),
        started_at=now - timedelta(seconds=20),
    )

    out = agent_engine.infer_runtime_health(run)
    assert out == "healthy"


def test_infer_runtime_health_returns_lagging_for_old_heartbeat(monkeypatch):
    now = datetime(2026, 3, 12, 12, 0, 0)
    _patch_now(monkeypatch, now)

    run = _run(
        status="running",
        heartbeat_at=now - timedelta(minutes=5),
        started_at=now - timedelta(minutes=6),
    )

    out = agent_engine.infer_runtime_health(run)
    assert out in {"lagging", "stale"}


def test_infer_runtime_health_returns_stale_when_running_but_no_heartbeat_for_too_long(monkeypatch):
    now = datetime(2026, 3, 12, 12, 0, 0)
    _patch_now(monkeypatch, now)

    run = _run(
        status="running",
        heartbeat_at=None,
        started_at=now - timedelta(minutes=30),
    )

    out = agent_engine.infer_runtime_health(run)
    assert out == "stale"


def test_serialize_run_exposes_runtime_health_if_serializer_exists(monkeypatch):
    now = datetime(2026, 3, 12, 12, 0, 0)
    _patch_now(monkeypatch, now)

    if not hasattr(agent_engine, "serialize_run"):
        pytest.skip("serialize_run() not present in agent_engine.py")

    run = _run(
        status="running",
        heartbeat_at=now - timedelta(seconds=5),
        started_at=now - timedelta(seconds=15),
    )

    out = agent_engine.serialize_run(run)

    assert out["id"] == 1
    assert out["agent_key"] == "deal_intake"
    assert out["status"] == "running"
    assert out["runtime_health"] == "healthy"