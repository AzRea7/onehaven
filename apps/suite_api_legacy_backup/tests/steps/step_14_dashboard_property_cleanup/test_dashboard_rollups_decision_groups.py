# backend/tests/steps/step_14_dashboard_property_cleanup/test_dashboard_rollups_decision_groups.py
from __future__ import annotations

from app.services import dashboard_rollups
from app.services.property_state_machine import normalize_decision_bucket


def test_decision_bucket_collapses_to_three_states():
    assert normalize_decision_bucket("PASS") == "GOOD"
    assert normalize_decision_bucket("GOOD_DEAL") == "GOOD"
    assert normalize_decision_bucket("APPROVED") == "GOOD"

    assert normalize_decision_bucket("REVIEW") == "REVIEW"
    assert normalize_decision_bucket("UNKNOWN") == "REVIEW"
    assert normalize_decision_bucket(None) == "REVIEW"

    assert normalize_decision_bucket("FAIL") == "REJECT"
    assert normalize_decision_bucket("REJECT") == "REJECT"
    assert normalize_decision_bucket("NO_GO") == "REJECT"


def test_dashboard_rollups_only_expose_three_decision_groups(monkeypatch):
    class FakeProp:
        def __init__(self, prop_id: int, county: str = "wayne"):
            self.id = prop_id
            self.org_id = 1
            self.address = f"{prop_id} Main St"
            self.city = "Detroit"
            self.state = "MI"
            self.county = county
            self.zip = "48201"
            self.is_red_zone = False
            self.crime_score = 42.0
            self.offender_count = 1

    props = [FakeProp(1), FakeProp(2), FakeProp(3), FakeProp(4)]

    class FakeScalarResult:
        def __init__(self, rows):
            self.rows = rows

        def all(self):
            return list(self.rows)

    class FakeDB:
        def scalars(self, stmt):
            return FakeScalarResult(props)

    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_deal",
        lambda db, org_id, property_id: type("Deal", (), {"asking_price": 90000})(),
    )

    decisions = {
        1: "PASS",
        2: "UNKNOWN",
        3: "FAIL",
        4: "GOOD_DEAL",
    }
    stages = {
        1: "deal",
        2: "rehab",
        3: "compliance",
        4: "tenant",
    }

    monkeypatch.setattr(
        dashboard_rollups,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "normalized_decision": normalize_decision_bucket(decisions[property_id]),
            "current_stage": stages[property_id],
        },
    )
    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_uw",
        lambda db, org_id, property_id: type(
            "UW",
            (),
            {
                "cash_flow": 500.0,
                "dscr": 1.25,
            },
        )(),
    )

    result = dashboard_rollups.compute_rollups(FakeDB(), org_id=1)

    assert set(result["decision_counts"].keys()) <= {"GOOD", "REVIEW", "REJECT"}
    assert result["decision_counts"]["GOOD"] == 2
    assert result["decision_counts"]["REVIEW"] == 1
    assert result["decision_counts"]["REJECT"] == 1

    row_decisions = {row["property_id"]: row["decision"] for row in result["rows"]}
    assert row_decisions[1] == "GOOD"
    assert row_decisions[2] == "REVIEW"
    assert row_decisions[3] == "REJECT"
    assert row_decisions[4] == "GOOD"
    