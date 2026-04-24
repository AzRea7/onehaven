# backend/tests/services/test_phase7_projection_regressions.py
from __future__ import annotations

from app.services import workflow_gate_service as workflow_svc


def test_phase7_property_brief_projection_shape_regression():
    """
    This test intentionally asserts the minimum projection shape the phase-7 frontend now relies on.
    It protects the UI from silent backend contract drift.
    """
    projection = {
        "projection_status": "warning",
        "blocking_count": 0,
        "unknown_count": 2,
        "stale_count": 1,
        "conflicting_count": 0,
        "evidence_gap_count": 1,
        "confirmed_count": 3,
        "inferred_count": 1,
        "failing_count": 0,
        "readiness_score": 78.0,
        "projected_compliance_cost": 950.0,
        "projected_days_to_rent": 4,
        "confidence_score": 0.82,
        "rules_version": "v2026.04.11",
        "impacted_rules": [],
        "unresolved_evidence_gaps": [],
        "last_projected_at": "2026-04-11T12:00:00Z",
    }

    required_keys = {
        "projection_status",
        "blocking_count",
        "unknown_count",
        "stale_count",
        "conflicting_count",
        "evidence_gap_count",
        "confirmed_count",
        "inferred_count",
        "failing_count",
        "readiness_score",
        "projected_compliance_cost",
        "projected_days_to_rent",
        "confidence_score",
        "rules_version",
        "impacted_rules",
        "unresolved_evidence_gaps",
        "last_projected_at",
    }

    assert required_keys.issubset(set(projection.keys()))


def test_phase6_compliance_gate_shape_regression():
    gate = workflow_svc._build_compliance_gate(
        {
            "projection": {
                "projection_status": "warning",
                "blocking_count": 0,
                "unknown_count": 1,
                "stale_count": 1,
                "conflicting_count": 0,
                "readiness_score": 79.0,
                "confidence_score": 0.66,
                "projected_compliance_cost": 700.0,
                "projected_days_to_rent": 3,
                "impacted_rules": [],
                "unresolved_evidence_gaps": [{"rule_key": "utility_confirmation", "gap": "No proof uploaded"}],
            },
            "blockers": [],
        },
        current_stage="under_contract",
    )

    assert gate["status"] == "warning"
    assert gate["ok"] is True
    assert gate["unknown_count"] == 1
    assert gate["stale_count"] == 1
    assert gate["projected_compliance_cost"] == 700.0
    assert gate["projected_days_to_rent"] == 3
    assert isinstance(gate["warnings"], list)
    assert isinstance(gate["unresolved_evidence_gaps"], list)