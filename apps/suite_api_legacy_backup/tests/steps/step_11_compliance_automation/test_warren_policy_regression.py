from __future__ import annotations

Warren_required_codes = {
    "LOCAL_AGENT_50_MILES",
    "CITY_DEBT_CLEARANCE",
    "NO_PO_BOX",
    "SOURCE_OF_INCOME",
}

WARREN_REQUIRED_CATEGORIES = {
    "rental_registration",
    "inspection",
    "certificate_of_occupancy",
    "source_of_income",
}


def test_warren_policy_codes_regression():
    payload = {
        "required_actions": [
            {"code": "LOCAL_AGENT_50_MILES"},
            {"code": "CITY_DEBT_CLEARANCE"},
            {"code": "SOURCE_OF_INCOME"},
        ],
        "blocking_items": [
            {"code": "NO_PO_BOX"},
        ],
    }

    actual = {x["code"] for x in payload["required_actions"]} | {x["code"] for x in payload["blocking_items"]}
    missing = Warren_required_codes - actual

    assert not missing, f"Missing Warren compliance codes: {sorted(missing)}"


def test_warren_required_categories_regression():
    payload = {
        "required_categories": [
            "rental_registration",
            "inspection",
            "certificate_of_occupancy",
            "source_of_income",
        ],
        "category_coverage": {
            "rental_registration": "verified",
            "inspection": "verified",
            "certificate_of_occupancy": "conditional",
            "source_of_income": "verified",
        },
    }

    actual_categories = set(payload["required_categories"])
    missing_categories = WARREN_REQUIRED_CATEGORIES - actual_categories
    assert not missing_categories, f"Missing Warren required categories: {sorted(missing_categories)}"

    coverage = payload["category_coverage"]
    assert coverage["rental_registration"] == "verified"
    assert coverage["inspection"] == "verified"
    