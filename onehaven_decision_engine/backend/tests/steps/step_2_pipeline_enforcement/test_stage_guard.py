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
        min_stage="lease",
        action="view cash transactions",
    )

    assert result["current_stage"] == "cash"


def test_require_stage_raises_409_with_useful_context(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "deal",
            "next_actions": ["Run underwriting evaluation for the deal."],
            "constraints": {"missing_underwriting": True},
        },
    )
    monkeypatch.setattr(
        stage_guard,
        "_policy_blockers",
        lambda db, org_id, property_id: [{"rule": "example"}],
    )

    with pytest.raises(HTTPException) as exc:
        stage_guard.require_stage(
            db=SimpleNamespace(),
            org_id=1,
            property_id=101,
            min_stage="compliance",
            action="create inspection",
        )

    err = exc.value
    assert err.status_code == 409
    assert err.detail["error"] == "stage_locked"
    assert err.detail["current_stage"] == "deal"
    assert err.detail["required_stage"] == "compliance"
    assert err.detail["action"] == "create inspection"
    assert "Run underwriting evaluation" in err.detail["why"]
    assert err.detail["next_actions"] == ["Run underwriting evaluation for the deal."]
    assert err.detail["constraints"] == {"missing_underwriting": True}
    assert err.detail["policy_blockers"] == [{"rule": "example"}]


def test_require_next_stage_available_allows_when_gate_open(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "current_stage": "deal",
            "gate": {
                "ok": True,
                "blocked_reason": None,
                "allowed_next_stage": "decision",
            },
            "constraints": {},
            "next_actions": [],
        },
    )
    monkeypatch.setattr(stage_guard, "_policy_blockers", lambda db, org_id, property_id: [])

    result = stage_guard.require_next_stage_available(
        db=SimpleNamespace(),
        org_id=1,
        property_id=101,
        action="advance workflow",
    )

    assert result["gate"]["ok"] is True
    assert result["gate"]["allowed_next_stage"] == "decision"


def test_require_next_stage_available_raises_409_with_context(monkeypatch):
    monkeypatch.setattr(
        stage_guard,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "current_stage": "rehab_exec",
            "gate": {
                "ok": False,
                "blocked_reason": "Complete rehab execution tasks first.",
                "allowed_next_stage": "compliance",
            },
            "constraints": {"rehab_open_tasks": 3},
            "next_actions": ["Complete rehab execution tasks (3 open)."],
        },
    )
    monkeypatch.setattr(stage_guard, "_policy_blockers", lambda db, org_id, property_id: [])

    with pytest.raises(HTTPException) as exc:
        stage_guard.require_next_stage_available(
            db=SimpleNamespace(),
            org_id=1,
            property_id=101,
            action="advance workflow",
        )

    err = exc.value
    assert err.status_code == 409
    assert err.detail["error"] == "stage_transition_blocked"
    assert err.detail["current_stage"] == "rehab_exec"
    assert err.detail["action"] == "advance workflow"
    assert err.detail["allowed_next_stage"] == "compliance"
    assert err.detail["constraints"] == {"rehab_open_tasks": 3}
    assert err.detail["next_actions"] == ["Complete rehab execution tasks (3 open)."]
    assert "Complete rehab execution tasks first." in err.detail["why"]