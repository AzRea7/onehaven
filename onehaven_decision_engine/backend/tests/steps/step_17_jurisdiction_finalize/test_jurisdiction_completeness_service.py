from __future__ import annotations

from app.services import jurisdiction_completeness_service as svc


def test_compute_completeness_marks_complete_when_all_required_categories_verified():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "registration",
            "inspection",
            "occupancy",
        ],
        category_coverage={
            "registration": "verified",
            "inspection": "verified",
            "occupancy": "verified",
        },
        stale_status="fresh",
        state="MI",
        county="macomb",
        city="warren",
    )

    assert payload["completeness_status"] == "complete"
    assert payload["missing_categories"] == []
    assert payload["stale_status"] == "fresh"
    assert payload["completeness_score"] == 1.0
    assert "rule_family_inventory" in payload
    assert "expected_rule_universe" in payload


def test_compute_completeness_marks_partial_when_missing_or_conditional():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "registration",
            "inspection",
            "occupancy",
            "source_of_income",
        ],
        category_coverage={
            "registration": "verified",
            "inspection": "verified",
            "occupancy": "conditional",
            "source_of_income": "missing",
        },
        stale_status="fresh",
        state="MI",
        county="macomb",
        city="warren",
    )

    assert payload["completeness_status"] == "partial"
    assert "source_of_income" in payload["missing_categories"]
    assert "occupancy" in payload["conditional_categories"]
    assert payload["completeness_score"] < 1.0
    assert "documents" in payload["operational_heuristic_categories"]


def test_compute_completeness_marks_stale_when_status_is_stale():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "registration",
            "inspection",
        ],
        category_coverage={
            "registration": "verified",
            "inspection": "verified",
        },
        stale_status="stale",
        state="MI",
        county="macomb",
        city="warren",
    )

    assert payload["completeness_status"] in {"stale", "partial"}
    assert payload["stale_status"] == "stale"
    assert "registration" in payload["property_proof_required_categories"]
