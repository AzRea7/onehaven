from __future__ import annotations

from app.services import jurisdiction_task_mapper as mapper


def test_jurisdiction_task_mapper_generates_required_actions_from_missing_coverage():
    out = mapper.build_jurisdiction_tasks(
        market={
            "state": "MI",
            "county": "macomb",
            "city": "warren",
        },
        required_categories=[
            "rental_registration",
            "inspection",
            "certificate_of_occupancy",
            "source_of_income",
        ],
        category_coverage={
            "rental_registration": "verified",
            "inspection": "missing",
            "certificate_of_occupancy": "conditional",
            "source_of_income": "missing",
        },
        stale_status="fresh",
    )

    action_codes = {x["code"] for x in out["required_actions"]}
    blocker_codes = {x["code"] for x in out["blocking_items"]}

    assert "JURISDICTION_VERIFY_INSPECTION" in action_codes
    assert "JURISDICTION_VERIFY_SOURCE_OF_INCOME" in action_codes
    assert "JURISDICTION_CERTIFICATE_CONDITIONAL_REVIEW" in blocker_codes


def test_jurisdiction_task_mapper_adds_stale_refresh_action():
    out = mapper.build_jurisdiction_tasks(
        market={
            "state": "MI",
            "county": "wayne",
            "city": "detroit",
        },
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

    action_codes = {x["code"] for x in out["required_actions"]}
    assert "JURISDICTION_REFRESH_STALE_RULES" in action_codes