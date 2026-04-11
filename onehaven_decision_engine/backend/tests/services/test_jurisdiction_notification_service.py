# backend/tests/services/test_jurisdiction_notification_service.py
from __future__ import annotations

from types import SimpleNamespace

from app.services import jurisdiction_notification_service as svc


def test_build_property_rule_change_notification_marks_high_severity_for_blockers():
    projection = SimpleNamespace(
        id=501,
        property_id=77,
        org_id=12,
        jurisdiction_slug="detroit-wayne",
        impacted_rules_json='[{"rule_key": "rental_registration_required"}]',
        projection_reason_json='{"reason": "rule_change"}',
        blocking_count=2,
        unknown_count=0,
        stale_count=0,
        conflicting_count=0,
        confidence_score=0.92,
        projection_status="blocked",
        projected_compliance_cost=3200.0,
        projected_days_to_rent=14,
    )

    payload = svc.build_property_rule_change_notification(
        property_projection=projection,
        changed_rules=[{"rule_key": "rental_registration_required"}],
        trigger_payload={"source_id": 91},
    )

    assert payload["kind"] == "property_rule_change_impact"
    assert payload["level"] == "high"
    assert payload["property_id"] == 77
    assert payload["blocking_count"] == 2
    assert payload["changed_rules"][0]["rule_key"] == "rental_registration_required"
    assert "blocker" in payload["message"].lower()


def test_build_property_rule_change_notification_marks_warning_for_low_confidence():
    projection = SimpleNamespace(
        id=502,
        property_id=78,
        org_id=12,
        jurisdiction_slug="pontiac-oakland",
        impacted_rules_json="[]",
        projection_reason_json="{}",
        blocking_count=0,
        unknown_count=1,
        stale_count=0,
        conflicting_count=0,
        confidence_score=0.49,
        projection_status="warning",
        projected_compliance_cost=600.0,
        projected_days_to_rent=4,
    )

    payload = svc.build_property_rule_change_notification(
        property_projection=projection,
        changed_rules=[{"rule_key": "utility_confirmation_required"}],
    )

    assert payload["kind"] == "property_rule_change_impact"
    assert payload["level"] == "warning"
    assert payload["unknown_count"] == 1
    assert payload["confidence_score"] == 0.49
    assert "confidence" in payload["message"].lower() or "unknown" in payload["message"].lower()


def test_build_post_close_reevaluation_trigger_contains_projection_summary():
    projection = SimpleNamespace(
        id=503,
        property_id=79,
        org_id=12,
        jurisdiction_slug="southfield-oakland",
        blocking_count=0,
        unknown_count=0,
        stale_count=1,
        conflicting_count=0,
        confidence_score=0.81,
    )

    payload = svc.build_post_close_reevaluation_trigger(
        property_projection=projection,
        changed_rules=[{"rule_key": "certificate_required_before_occupancy"}],
    )

    assert payload["kind"] == "property_post_close_recheck"
    assert payload["property_id"] == 79
    assert payload["stale_count"] == 1
    assert payload["changed_rules"][0]["rule_key"] == "certificate_required_before_occupancy"
    assert "re-evaluated" in payload["message"].lower()