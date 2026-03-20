from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.services import stage_guard


def test_require_stage_allows_when_current_stage_meets_minimum(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "cash",
            "next_actions": [],
            "constraints": {},
        },
    )
    monkeypatch.setattr(stage_guard, "_policy_blockers", lambda db, org_id, property_id: [])

    result = stage_guard.require_stage(
        db=SimpleNamespace(),
        org_id=1,
        property_id=101,
        min_stage="tenant",
        action="view cash transactions",
    )

    assert result["current_stage"] == "cash"


def test_require_stage_blocks_when_current_stage_is_before_minimum(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "rehab",
            "next_actions": ["Pass inspection", "Create lease"],
            "constraints": {"inspection": {"passed": False}},
        },
    )
    monkeypatch.setattr(
        stage_guard,
        "_policy_blockers",
        lambda db, org_id, property_id: [{"code": "LOCAL_AGENT_REQUIRED"}],
    )
    monkeypatch.setattr(
        stage_guard,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "rehab",
            "gate_status": "BLOCKED",
        },
    )

    with pytest.raises(HTTPException) as exc:
        stage_guard.require_stage(
            db=SimpleNamespace(),
            org_id=1,
            property_id=101,
            min_stage="cash",
            action="view cash transactions",
        )

    assert exc.value.status_code == 409
    detail = exc.value.detail
    assert detail["error"] == "stage_locked"
    assert detail["current_stage"] == "rehab"
    assert detail["required_stage"] == "cash"
    assert detail["action"] == "view cash transactions"
    assert detail["next_actions"] == ["Pass inspection", "Create lease"]
    assert detail["policy_blockers"] == [{"code": "LOCAL_AGENT_REQUIRED"}]
    assert detail["workflow"]["gate_status"] == "BLOCKED"


def test_require_next_stage_available_allows_when_gate_is_open(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "current_stage": "deal",
            "gate": {"ok": True, "allowed_next_stage": "rehab"},
            "next_actions": [],
            "constraints": {},
        },
    )

    result = stage_guard.require_next_stage_available(
        db=SimpleNamespace(),
        org_id=1,
        property_id=202,
        action="advance workflow",
    )

    assert result["gate"]["ok"] is True
    assert result["gate"]["allowed_next_stage"] == "rehab"


def test_require_next_stage_available_blocks_when_gate_is_closed(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "current_stage": "compliance",
            "gate": {
                "ok": False,
                "allowed_next_stage": "tenant",
                "blocked_reason": "Pass inspection first.",
            },
            "next_actions": ["Resolve failed inspection items"],
            "constraints": {"inspection": {"passed": False}},
        },
    )
    monkeypatch.setattr(
        stage_guard,
        "_policy_blockers",
        lambda db, org_id, property_id: [],
    )
    monkeypatch.setattr(
        stage_guard,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "compliance",
            "gate_status": "BLOCKED",
        },
    )

    with pytest.raises(HTTPException) as exc:
        stage_guard.require_next_stage_available(
            db=SimpleNamespace(),
            org_id=1,
            property_id=202,
            action="advance workflow",
        )

    assert exc.value.status_code == 409
    detail = exc.value.detail
    assert detail["error"] == "stage_transition_blocked"
    assert detail["current_stage"] == "compliance"
    assert detail["allowed_next_stage"] == "tenant"
    assert detail["why"] == "Pass inspection first."
    assert detail["next_actions"] == ["Resolve failed inspection items"]
    assert detail["workflow"]["current_stage"] == "compliance"
    