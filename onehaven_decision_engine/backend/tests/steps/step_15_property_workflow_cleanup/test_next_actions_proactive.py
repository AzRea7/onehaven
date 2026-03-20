# backend/tests/steps/step_15_property_workflow_cleanup/test_next_actions_proactive.py
from __future__ import annotations

from types import SimpleNamespace

from app.routers import ops as ops_router


def test_property_ops_summary_exposes_proactive_next_actions(monkeypatch):
    prop = SimpleNamespace(
        id=44,
        org_id=1,
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip="48201",
        county="wayne",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1950,
        lat=None,
        lng=None,
        crime_score=45.0,
        offender_count=1,
        is_red_zone=False,
    )

    class FakeDB:
        def scalar(self, *args, **kwargs):
            return prop

    monkeypatch.setattr(
        ops_router,
        "compute_and_persist_stage",
        lambda db, org_id, property: SimpleNamespace(
            current_stage="rehab",
            updated_at=None,
        ),
    )
    monkeypatch.setattr(
        ops_router,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "rehab",
            "normalized_decision": "GOOD",
            "gate_status": "BLOCKED",
            "gate": {"ok": False, "allowed_next_stage": "compliance"},
            "constraints": {"rehab": {"open": 3}},
            "outstanding_tasks": {"blockers": ["rehab_open_tasks"]},
            "next_actions": [
                "Complete rehab tasks (3 still open).",
                "Schedule inspection after rehab is complete.",
            ],
            "stage_completion_summary": {"completed_count": 1, "total_count": 6},
        },
    )
    monkeypatch.setattr(
        ops_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "rehab",
            "current_stage_label": "Rehab",
            "primary_action": {
                "kind": "next_action",
                "title": "Complete rehab tasks (3 still open).",
            },
        },
    )
    monkeypatch.setattr(
        ops_router,
        "_checklist_progress",
        lambda db, org_id, property_id: ops_router.ChecklistProgress(
            total=2,
            todo=1,
            in_progress=1,
            blocked=0,
            done=0,
        ),
    )
    monkeypatch.setattr(ops_router, "_latest_inspection", lambda db, org_id, property_id: None)
    monkeypatch.setattr(ops_router, "_open_failed_inspection_items", lambda db, org_id, property_id: 0)
    monkeypatch.setattr(
        ops_router,
        "_rehab_summary",
        lambda db, org_id, property_id: {
            "total": 3,
            "todo": 2,
            "in_progress": 1,
            "blocked": 0,
            "done": 0,
            "cost_estimate_sum": 4500.0,
            "is_complete": False,
        },
    )
    monkeypatch.setattr(
        ops_router,
        "_tenant_summary",
        lambda db, org_id, property_id: {
            "occupancy_status": "vacant",
            "lease_count": 0,
            "active_lease_count": 0,
            "upcoming_lease_count": 0,
            "ended_lease_count": 0,
            "active_lease": None,
        },
    )
    monkeypatch.setattr(ops_router, "_active_lease", lambda db, org_id, property_id: None)
    monkeypatch.setattr(
        ops_router,
        "_cash_rollup",
        lambda db, org_id, property_id, days: {
            "income": 0.0,
            "expense": 0.0,
            "capex": 0.0,
            "net": 0.0,
        },
    )
    monkeypatch.setattr(ops_router, "_equity_summary", lambda db, org_id, property_id: None)
    monkeypatch.setattr(ops_router, "_latest_underwriting", lambda db, org_id, property_id: None)

    result = ops_router.property_ops_summary(
        property_id=44,
        cash_days=90,
        db=FakeDB(),
        p=SimpleNamespace(org_id=1, user_id=99),
    )

    assert result["stage"] == "rehab"
    assert result["normalized_decision"] == "GOOD"
    assert result["gate_status"] == "BLOCKED"
    assert result["next_actions"] == [
        "Complete rehab tasks (3 still open).",
        "Schedule inspection after rehab is complete.",
    ]
    assert result["outstanding_tasks"]["blockers"] == ["rehab_open_tasks"]