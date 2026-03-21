from __future__ import annotations

from app.services import workflow_gate_service as gates


def test_workflow_summary_primary_action_is_next_action_when_blocked(monkeypatch):
    monkeypatch.setattr(
        gates,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "rehab",
            "normalized_decision": "GOOD",
            "gate_status": "BLOCKED",
            "gate": {"ok": False, "allowed_next_stage": "compliance"},
            "next_actions": ["Complete rehab tasks (3 still open)."],
            "constraints": {"rehab": {"open_tasks": 3}},
            "outstanding_tasks": {"blockers": ["rehab_open_tasks"]},
            "stage_completion_summary": {
                "by_stage": [
                    {"stage": "deal", "is_complete": True},
                    {"stage": "rehab", "is_complete": False},
                ]
            },
            "updated_at": None,
        },
    )
    monkeypatch.setattr(
        gates,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "gate": {"ok": False, "allowed_next_stage": "compliance"},
        },
    )

    summary = gates.build_workflow_summary(
        db=None,
        org_id=1,
        property_id=77,
        recompute=True,
    )

    assert summary["current_stage"] == "rehab"
    assert summary["gate_status"] == "BLOCKED"
    assert summary["primary_action"]["kind"] == "next_action"
    assert summary["primary_action"]["title"] == "Complete rehab tasks (3 still open)."
    assert summary["next_stage"] == "compliance"
    assert summary["outstanding_tasks"]["blockers"] == ["rehab_open_tasks"]


def test_workflow_summary_includes_jurisdiction_blocker_when_present(monkeypatch):
    monkeypatch.setattr(
        gates,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "compliance",
            "normalized_decision": "GOOD",
            "gate_status": "BLOCKED",
            "gate": {
                "ok": False,
                "allowed_next_stage": "tenant",
                "blocked_reason": "Jurisdiction coverage incomplete.",
            },
            "next_actions": [
                "Refresh jurisdiction profile and verify missing registration rules.",
            ],
            "constraints": {
                "jurisdiction": {
                    "profile_id": 901,
                    "completeness_status": "partial",
                    "stale_status": "stale",
                }
            },
            "outstanding_tasks": {
                "blockers": [
                    "jurisdiction_incomplete",
                    "jurisdiction_stale",
                ]
            },
            "stage_completion_summary": {
                "by_stage": [
                    {"stage": "deal", "is_complete": True},
                    {"stage": "rehab", "is_complete": True},
                    {"stage": "compliance", "is_complete": False},
                ]
            },
            "updated_at": None,
        },
    )
    monkeypatch.setattr(
        gates,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "gate": {
                "ok": False,
                "allowed_next_stage": "tenant",
                "blocked_reason": "Jurisdiction coverage incomplete.",
            },
        },
    )

    summary = gates.build_workflow_summary(
        db=None,
        org_id=1,
        property_id=88,
        recompute=True,
    )

    assert summary["current_stage"] == "compliance"
    assert summary["gate_status"] == "BLOCKED"
    assert summary["primary_action"]["kind"] == "next_action"
    assert "Refresh jurisdiction profile" in summary["primary_action"]["title"]
    assert "jurisdiction_incomplete" in summary["outstanding_tasks"]["blockers"]
    assert "jurisdiction_stale" in summary["outstanding_tasks"]["blockers"]
    