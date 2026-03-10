from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import ops as ops_router


@pytest.fixture
def test_app(monkeypatch):
    app = FastAPI()
    app.include_router(ops_router.router, prefix="/api")

    def fake_db():
        yield SimpleNamespace()

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_principal] = lambda: SimpleNamespace(
        org_id=1,
        user_id=99,
        role="operator",
        email="austin@demo.local",
    )

    monkeypatch.setattr(
        ops_router,
        "compute_and_persist_stage",
        lambda db, org_id, property: SimpleNamespace(
            current_stage="deal",
            updated_at=None,
        ),
    )
    monkeypatch.setattr(
        ops_router,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "current_stage": "deal",
            "constraints": {"can_advance": False},
            "outstanding_tasks": {"deal": ["run underwriting"]},
            "next_actions": [
                "Run underwriting before moving to decision.",
                "Confirm rent assumptions.",
            ],
        },
    )
    monkeypatch.setattr(
        ops_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "deal",
            "current_stage_label": "Deal",
            "primary_action": {"kind": "advance", "title": "advance"},
            "gate": {"ok": False, "blocked_reason": "Run underwriting first"},
            "stages": [],
        },
    )
    monkeypatch.setattr(
        ops_router,
        "_checklist_progress",
        lambda db, org_id, property_id: ops_router.ChecklistProgress(
            total=2,
            todo=2,
            in_progress=0,
            blocked=0,
            done=0,
        ),
    )
    monkeypatch.setattr(ops_router, "_latest_inspection", lambda *args, **kwargs: None)
    monkeypatch.setattr(ops_router, "_open_failed_inspection_items", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        ops_router,
        "_rehab_summary",
        lambda *args, **kwargs: {
            "total": 0,
            "todo": 0,
            "in_progress": 0,
            "blocked": 0,
            "done": 0,
            "cost_estimate_sum": 0.0,
            "is_complete": False,
        },
    )
    monkeypatch.setattr(ops_router, "_active_lease", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ops_router,
        "_cash_rollup",
        lambda *args, **kwargs: {
            "income": 0.0,
            "expense": 0.0,
            "capex": 0.0,
            "other": 0.0,
            "net": 0.0,
        },
    )
    monkeypatch.setattr(ops_router, "_latest_valuation", lambda *args, **kwargs: None)
    monkeypatch.setattr(ops_router, "_latest_underwriting", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ops_router,
        "select",
        lambda *args, **kwargs: "fake-select",
    )

    class FakeDB:
        def scalar(self, stmt):
            return SimpleNamespace(
                id=7,
                org_id=1,
                address="123 Logic Ave",
                city="Detroit",
                state="MI",
                zip="48201",
                county="Wayne",
                bedrooms=3,
                bathrooms=1.0,
                square_feet=1200,
                year_built=1950,
                lat=42.33,
                lng=-83.04,
                crime_score=55.0,
                offender_count=4,
                is_red_zone=True,
            )

    app.dependency_overrides[get_db] = lambda: iter([FakeDB()])
    return app


def test_property_ops_summary_exposes_next_actions(test_app):
    client = TestClient(test_app)
    r = client.get("/api/property/7/summary")
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["stage"] == "deal"
    assert body["workflow"]["current_stage_label"] == "Deal"
    assert len(body["next_actions"]) == 2
    assert "underwriting" in body["next_actions"][0].lower()