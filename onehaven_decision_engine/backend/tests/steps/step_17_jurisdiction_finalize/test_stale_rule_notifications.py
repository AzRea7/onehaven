from __future__ import annotations

from app.services import jurisdiction_notification_service as notify_svc


def test_build_stale_notification_payload_contains_market_and_reason():
    payload = notify_svc.build_stale_rule_notification(
        state="MI",
        county="macomb",
        city="warren",
        pha_name=None,
        stale_reason="source_freshness_expired",
        missing_categories=["certificate_of_occupancy"],
    )

    assert payload["state"] == "MI"
    assert payload["county"] == "macomb"
    assert payload["city"] == "warren"
    assert payload["stale_reason"] == "source_freshness_expired"
    assert "certificate_of_occupancy" in payload["missing_categories"]


def test_notification_service_marks_notification_needed_when_stale():
    out = notify_svc.should_notify_stale_rules(
        completeness_status="partial",
        stale_status="stale",
        stale_reason="source_freshness_expired",
        missing_categories=["inspection"],
    )

    assert out["notify"] is True
    assert out["severity"] in {"warning", "high"}
    assert out["reason"] == "source_freshness_expired"


def test_notification_service_skips_when_fresh_and_complete():
    out = notify_svc.should_notify_stale_rules(
        completeness_status="complete",
        stale_status="fresh",
        stale_reason=None,
        missing_categories=[],
    )

    assert out["notify"] is False