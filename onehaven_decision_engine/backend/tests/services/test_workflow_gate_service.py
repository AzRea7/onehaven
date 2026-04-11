# backend/tests/services/test_workflow_gate_service.py
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services import workflow_gate_service as svc


@pytest.fixture()
def patched_workflow(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        svc,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "under_contract",
            "next_actions": ["Resolve city inspection blockers"],
            "stage_completion_summary": {"by_stage": [{"stage": "deal", "is_complete": True}]},
            "constraints": {},
            "outstanding_tasks": {},
            "gate_status": "warning",
            "normalized_decision": "proceed_with_conditions",
            "updated_at": "2026-04-11T12:00:00Z",
            "transition_reason": "offer_accepted",
            "transition_at": "2026-04-11T12:00:00Z",
            "is_auto_routed": True,
        },
    )
    monkeypatch.setattr(
        svc,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "gate": {
                "ok": True,
                "allowed_next_stage": "closing",
            }
        },
    )
    monkeypatch.setattr(
        svc,
        "build_pane_context",
        lambda current_stage, constraints, principal, org_id: {
            "current_pane": "acquisition",
            "current_pane_label": "Acquisition",
            "suggested_pane": "acquisition",
            "suggested_pane_label": "Acquisition",
            "suggested_next_pane": "compliance",
            "suggested_next_pane_label": "Compliance",
            "visible_pane": "acquisition",
            "visible_pane_label": "Acquisition",
            "is_current_pane_visible": True,
            "allowed_panes": ["investor", "acquisition", "compliance"],
            "allowed_pane_labels": ["Investor", "Acquisition", "Compliance"],
            "route_reason": "Property is in acquisition workflow.",
            "catalog": [],
        },
    )


def test_build_workflow_summary_blocks_pre_close_on_compliance_blocker(
    monkeypatch: pytest.MonkeyPatch,
    patched_workflow: None,
):
    monkeypatch.setattr(
        svc,
        "build_property_projection_snapshot",
        lambda db, org_id, property_id: {
            "projection": {
                "projection_status": "blocked",
                "blocking_count": 2,
                "unknown_count": 1,
                "stale_count": 0,
                "conflicting_count": 0,
                "readiness_score": 42.0,
                "confidence_score": 0.58,
                "projected_compliance_cost": 4500.0,
                "projected_days_to_rent": 21,
                "rules_version": "v2026.04.11",
                "impacted_rules": [{"rule_key": "rental_registration_required"}],
                "unresolved_evidence_gaps": [{"rule_key": "certificate_required", "gap": "No pass certificate"}],
            },
            "blockers": [
                {
                    "rule_key": "rental_registration_required",
                    "title": "Rental registration required",
                    "evaluation_status": "failed",
                    "evidence_gap": "No city registration proof",
                }
            ],
        },
    )

    result = svc.build_workflow_summary(
        db=None,
        org_id=101,
        property_id=55,
        principal=SimpleNamespace(org_id=101, user_id=7),
        recompute=False,
    )

    assert result["current_stage"] == "under_contract"
    assert result["compliance_projection"]["blocking_count"] == 2
    assert result["compliance_gate"]["ok"] is False
    assert result["compliance_gate"]["status"] == "blocked"
    assert result["compliance_gate"]["blocking_count"] == 2
    assert result["pre_close_risk"]["blocking"] is True
    assert result["pre_close_risk"]["status"] == "blocked"
    assert result["gate"]["ok"] is False
    assert result["gate"]["code"] == "compliance_projection_blocked"
    assert result["primary_action"]["pane"] == "compliance"
    assert "Pre-close compliance blocker" in (result["compliance_gate"]["blocked_reason"] or "")


def test_build_workflow_summary_warns_but_does_not_block_when_only_unknown_and_stale(
    monkeypatch: pytest.MonkeyPatch,
    patched_workflow: None,
):
    monkeypatch.setattr(
        svc,
        "build_property_projection_snapshot",
        lambda db, org_id, property_id: {
            "projection": {
                "projection_status": "warning",
                "blocking_count": 0,
                "unknown_count": 2,
                "stale_count": 1,
                "conflicting_count": 0,
                "readiness_score": 76.0,
                "confidence_score": 0.71,
                "projected_compliance_cost": 900.0,
                "projected_days_to_rent": 5,
                "impacted_rules": [],
                "unresolved_evidence_gaps": [{"rule_key": "utility_confirmation", "gap": "No current utility proof"}],
            },
            "blockers": [],
        },
    )

    result = svc.build_workflow_summary(
        db=None,
        org_id=101,
        property_id=56,
        principal=SimpleNamespace(org_id=101, user_id=7),
        recompute=False,
    )

    assert result["compliance_gate"]["ok"] is True
    assert result["compliance_gate"]["status"] == "warning"
    assert result["gate"]["ok"] is True
    assert result["pre_close_risk"]["blocking"] is False
    assert result["pre_close_risk"]["status"] == "warning"
    assert result["compliance_gate"]["unknown_count"] == 2
    assert result["compliance_gate"]["stale_count"] == 1
    assert len(result["compliance_gate"]["warnings"]) >= 2


def test_build_workflow_summary_requires_post_close_recheck_for_owned_property(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(
        svc,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "owned",
            "next_actions": [],
            "stage_completion_summary": {"by_stage": []},
            "constraints": {},
            "outstanding_tasks": {},
            "gate_status": "ok",
            "normalized_decision": "owned",
            "updated_at": "2026-04-11T12:00:00Z",
            "transition_reason": "closed",
            "transition_at": "2026-04-11T12:00:00Z",
            "is_auto_routed": True,
        },
    )
    monkeypatch.setattr(
        svc,
        "get_transition_payload",
        lambda db, org_id, property_id: {"gate": {"ok": True, "allowed_next_stage": "lease_up"}},
    )
    monkeypatch.setattr(
        svc,
        "build_pane_context",
        lambda current_stage, constraints, principal, org_id: {
            "current_pane": "management",
            "current_pane_label": "Management",
            "suggested_pane": "management",
            "suggested_pane_label": "Management",
            "suggested_next_pane": "management",
            "suggested_next_pane_label": "Management",
            "visible_pane": "management",
            "visible_pane_label": "Management",
            "is_current_pane_visible": True,
            "allowed_panes": ["management", "compliance"],
            "allowed_pane_labels": ["Management", "Compliance"],
            "route_reason": "Property is in post-close operations.",
            "catalog": [],
        },
    )
    monkeypatch.setattr(
        svc,
        "build_property_projection_snapshot",
        lambda db, org_id, property_id: {
            "projection": {
                "projection_status": "warning",
                "blocking_count": 0,
                "unknown_count": 0,
                "stale_count": 1,
                "conflicting_count": 0,
                "readiness_score": 88.0,
                "confidence_score": 0.84,
                "projected_compliance_cost": 0.0,
                "projected_days_to_rent": 0,
                "impacted_rules": [],
                "unresolved_evidence_gaps": [],
            },
            "blockers": [],
        },
    )

    result = svc.build_workflow_summary(
        db=None,
        org_id=101,
        property_id=57,
        principal=SimpleNamespace(org_id=101, user_id=7),
        recompute=False,
    )

    assert result["current_stage"] == "owned"
    assert result["compliance_gate"]["post_close_recheck_needed"] is True
    assert result["post_close_recheck"]["needed"] is True
    assert result["post_close_recheck"]["status"] == "recheck_required"
    assert "stale" in (result["post_close_recheck"]["reason"] or "").lower()