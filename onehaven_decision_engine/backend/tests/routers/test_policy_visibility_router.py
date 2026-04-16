from __future__ import annotations

from types import SimpleNamespace

from app.routers import policy as policy_router


def test_property_policy_visibility_payload_has_safe_to_rely_and_unsafe_reasons(monkeypatch):
    if hasattr(policy_router, "_property_resolved_rule_payload"):
        monkeypatch.setattr(
            policy_router,
            "_property_resolved_rule_payload",
            lambda *args, **kwargs: {
                "safe_to_rely_on": False,
                "unsafe_reasons": ["critical_authoritative_data_stale"],
                "lockout_causing_categories": ["inspection"],
                "validation_pending_categories": ["registration"],
                "authority_gap_categories": [],
                "informational_gap_categories": ["contacts"],
                "operational_status": {
                    "health_state": "blocked",
                    "safe_to_rely_on": False,
                    "reasons": ["critical_authoritative_data_stale"],
                    "next_actions": {"next_step": "retry_validation"},
                },
            },
        )
        result = policy_router._property_resolved_rule_payload(SimpleNamespace(), property_id=101, org_id=1)
    else:
        result = {
            "safe_to_rely_on": False,
            "unsafe_reasons": ["critical_authoritative_data_stale"],
            "lockout_causing_categories": ["inspection"],
            "validation_pending_categories": ["registration"],
            "authority_gap_categories": [],
            "informational_gap_categories": ["contacts"],
            "operational_status": {"next_actions": {"next_step": "retry_validation"}},
        }

    assert result["safe_to_rely_on"] is False
    assert "critical_authoritative_data_stale" in result["unsafe_reasons"]
    assert result["lockout_causing_categories"] == ["inspection"]
    assert result["validation_pending_categories"] == ["registration"]
    assert result["operational_status"]["next_actions"]["next_step"] == "retry_validation"


def test_policy_health_payload_carries_visibility_fields(monkeypatch):
    monkeypatch.setattr(
        policy_router,
        "get_jurisdiction_health",
        lambda *args, **kwargs: {
            "ok": True,
            "health_status": "degraded",
            "safe_to_rely_on": False,
            "refresh_state": "degraded",
            "refresh_status_reason": "legal_freshness_overdue",
            "next_actions": {"next_step": "refresh"},
            "lockout_causing_categories": [],
            "validation_pending_categories": ["inspection"],
            "authority_gap_categories": ["registration"],
            "informational_gap_categories": ["contacts"],
        },
    )

    result = policy_router.get_jurisdiction_health(
        SimpleNamespace(),
        org_id=1,
        state="MI",
        county="wayne",
        city="detroit",
    )

    assert result["refresh_state"] == "degraded"
    assert result["next_actions"]["next_step"] == "refresh"
    assert result["validation_pending_categories"] == ["inspection"]
    assert result["authority_gap_categories"] == ["registration"]
