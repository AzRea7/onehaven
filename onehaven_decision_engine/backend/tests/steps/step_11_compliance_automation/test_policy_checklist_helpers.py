from __future__ import annotations

from app.routers import compliance as router_mod


def test_items_from_policy_brief_builds_checklist_items():
    brief = {
        "required_actions": [
            {
                "code": "LOCAL_AGENT_50_MILES",
                "title": "Designate responsible local agent within 50 miles",
                "category": "Licensing",
                "severity": 4,
            },
            {
                "code": "CITY_DEBT_CLEARANCE",
                "title": "Provide proof no fees or debts are owed to the city",
                "category": "Licensing",
                "severity": 5,
            },
        ],
        "blocking_items": [
            {
                "code": "NO_PO_BOX",
                "title": "Owner legal address cannot be a P.O. Box",
                "category": "Registration",
                "severity": 5,
            }
        ],
    }

    items = router_mod._items_from_policy_brief(brief)
    codes = {x.item_code for x in items}

    assert "LOCAL_AGENT_50_MILES" in codes
    assert "CITY_DEBT_CLEARANCE" in codes
    assert "NO_PO_BOX" in codes