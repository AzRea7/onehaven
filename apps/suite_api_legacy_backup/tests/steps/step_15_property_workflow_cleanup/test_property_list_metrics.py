# backend/tests/steps/step_15_property_workflow_cleanup/test_property_list_metrics.py
from __future__ import annotations

from types import SimpleNamespace

from app.routers import properties as properties_router


def test_build_property_list_item_contains_ui_metrics_contract(monkeypatch):
    prop = SimpleNamespace(
        id=77,
        org_id=1,
        address="123 Main St",
        city="Detroit",
        state="MI",
        county="wayne",
        zip="48201",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1950,
        property_type="single_family",
        lat=None,
        lng=None,
        crime_score=61.0,
        offender_count=2,
        is_red_zone=False,
        rent_assumption=None,
        rent_comps=[],
    )

    monkeypatch.setattr(
        properties_router,
        "_latest_deal",
        lambda db, org_id, property_id: SimpleNamespace(
            id=1,
            asking_price=85000,
            updated_at=None,
        ),
    )
    monkeypatch.setattr(
        properties_router,
        "_latest_underwriting",
        lambda db, org_id, property_id: SimpleNamespace(
            id=2,
            decision="PASS",
            cash_flow=525.0,
            dscr=1.31,
        ),
    )
    monkeypatch.setattr(
        properties_router,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "rehab",
            "current_stage_label": "Rehab",
            "normalized_decision": "GOOD",
            "gate_status": "BLOCKED",
            "gate": {"ok": False, "allowed_next_stage": "compliance"},
            "stage_completion_summary": {"completed_count": 1, "total_count": 6},
            "next_actions": ["Complete rehab tasks"],
        },
    )
    monkeypatch.setattr(
        properties_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "rehab",
            "next_stage": "compliance",
        },
    )
    monkeypatch.setattr(
        properties_router.PropertyOut,
        "model_validate",
        classmethod(
            lambda cls, obj, from_attributes=True: SimpleNamespace(
                model_dump=lambda: {
                    "id": obj.id,
                    "address": obj.address,
                    "city": obj.city,
                    "state": obj.state,
                    "county": obj.county,
                    "zip": obj.zip,
                    "bedrooms": obj.bedrooms,
                    "bathrooms": obj.bathrooms,
                    "square_feet": obj.square_feet,
                    "year_built": obj.year_built,
                    "crime_score": obj.crime_score,
                    "offender_count": obj.offender_count,
                    "is_red_zone": obj.is_red_zone,
                }
            )
        ),
    )

    row = properties_router._build_property_list_item(
        db=SimpleNamespace(),
        org_id=1,
        prop=prop,
    )

    assert row["asking_price"] == 85000
    assert row["projected_monthly_cashflow"] == 525.0
    assert row["dscr"] == 1.31
    assert row["crime_score"] == 61.0
    assert row["normalized_decision"] == "GOOD"
    assert row["current_workflow_stage"] == "rehab"

    expected_keys = {
        "asking_price",
        "projected_monthly_cashflow",
        "dscr",
        "crime_score",
        "crime_label",
        "normalized_decision",
        "current_workflow_stage",
        "current_workflow_stage_label",
        "gate_status",
        "gate",
        "stage_completion_summary",
        "next_actions",
        "workflow",
    }
    assert expected_keys.issubset(set(row.keys()))
    