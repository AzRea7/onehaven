from __future__ import annotations

Warren_required_codes = {
    "LOCAL_AGENT_50_MILES",
    "CITY_DEBT_CLEARANCE",
    "NO_PO_BOX",
    "SOURCE_OF_INCOME",
}


def test_warren_policy_codes_regression():
    # Replace this with however you seed or build Warren policy payloads.
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