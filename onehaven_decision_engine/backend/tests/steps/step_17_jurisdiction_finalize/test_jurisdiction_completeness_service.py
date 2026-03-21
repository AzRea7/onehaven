from __future__ import annotations

from app.services import jurisdiction_completeness_service as svc


def test_compute_completeness_marks_complete_when_all_required_categories_verified():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "rental_registration",
            "inspection",
            "certificate_of_occupancy",
        ],
        category_coverage={
            "rental_registration": "verified",
            "inspection": "verified",
            "certificate_of_occupancy": "verified",
        },
        stale_status="fresh",
    )

    assert payload["completeness_status"] == "complete"
    assert payload["missing_categories"] == []
    assert payload["stale_status"] == "fresh"
    assert payload["completeness_score"] == 1.0


def test_compute_completeness_marks_partial_when_missing_or_conditional():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "rental_registration",
            "inspection",
            "certificate_of_occupancy",
            "source_of_income",
        ],
        category_coverage={
            "rental_registration": "verified",
            "inspection": "verified",
            "certificate_of_occupancy": "conditional",
            "source_of_income": "missing",
        },
        stale_status="fresh",
    )

    assert payload["completeness_status"] == "partial"
    assert "source_of_income" in payload["missing_categories"]
    assert "certificate_of_occupancy" in payload["conditional_categories"]
    assert payload["completeness_score"] < 1.0


def test_compute_completeness_marks_stale_when_status_is_stale():
    payload = svc.compute_jurisdiction_completeness(
        required_categories=[
            "rental_registration",
            "inspection",
        ],
        category_coverage={
            "rental_registration": "verified",
            "inspection": "verified",
        },
        stale_status="stale",
    )

    assert payload["completeness_status"] in {"stale", "partial"}
    assert payload["stale_status"] == "stale"