# backend/tests/steps/step_15_property_workflow_cleanup/test_dashboard_summary_contracts.py
from __future__ import annotations

from app.services import dashboard_rollups


def test_dashboard_rows_keep_summary_contract_fields(monkeypatch):
    class FakeProp:
        def __init__(self, prop_id: int):
            self.id = prop_id
            self.org_id = 1
            self.address = "123 Main St"
            self.city = "Detroit"
            self.state = "MI"
            self.county = "wayne"
            self.zip = "48201"
            self.is_red_zone = False
            self.crime_score = 55.0
            self.offender_count = 2

    class FakeScalarResult:
        def all(self):
            return [FakeProp(1)]

    class FakeDB:
        def scalars(self, stmt):
            return FakeScalarResult()

    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_deal",
        lambda db, org_id, property_id: type("Deal", (), {"asking_price": 85000})(),
    )
    monkeypatch.setattr(
        dashboard_rollups,
        "_latest_uw",
        lambda db, org_id, property_id: type(
            "UW",
            (),
            {
                "cash_flow": 525.0,
                "dscr": 1.31,
            },
        )(),
    )
    monkeypatch.setattr(
        dashboard_rollups,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "normalized_decision": "GOOD",
            "current_stage": "rehab",
        },
    )

    result = dashboard_rollups.compute_rollups(FakeDB(), org_id=1)

    assert len(result["rows"]) == 1
    row = result["rows"][0]

    expected = {
        "property_id",
        "address",
        "city",
        "state",
        "county",
        "zip",
        "asking_price",
        "projected_monthly_cashflow",
        "dscr",
        "crime_score",
        "offender_count",
        "decision",
        "stage",
    }
    assert expected.issubset(set(row.keys()))

    assert row["asking_price"] == 85000
    assert row["projected_monthly_cashflow"] == 525.0
    assert row["dscr"] == 1.31
    assert row["crime_score"] == 55.0
    assert row["decision"] == "GOOD"
    assert row["stage"] == "rehab"
    