from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import workflow as workflow_router


class FakeDB:
    def __init__(self):
        self.added = []
        self.commits = 0

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def refresh(self, obj):
        return obj

    def flush(self):
        return None


@pytest.fixture
def principal():
    return SimpleNamespace(
        org_id=1,
        org_slug="iss",
        user_id=99,
        role="operator",
        email="austin@demo.local",
        plan_code="pro",
    )


@pytest.fixture
def fake_db():
    return FakeDB()


@pytest.fixture
def test_app(fake_db, principal):
    app = FastAPI()
    app.include_router(workflow_router.router, prefix="/api")

    def _fake_db_dep():
        yield fake_db

    app.dependency_overrides[get_db] = _fake_db_dep
    app.dependency_overrides[get_principal] = lambda: principal
    return app


@pytest.fixture
def client(test_app):
    return TestClient(test_app)


def test_workflow_catalog_returns_six_stage_model(client):
    res = client.get("/api/workflow/catalog")
    assert res.status_code == 200, res.text

    body = res.json()
    assert [row["key"] for row in body["stages"]] == [
        "deal",
        "rehab",
        "compliance",
        "tenant",
        "cash",
        "equity",
    ]
    assert body["decision_states"] == ["GOOD", "REVIEW", "REJECT"]


def test_get_state_returns_normalized_workflow_payload(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "tenant",
            "suggested_stage": "tenant",
            "current_stage_label": "Tenant",
            "normalized_decision": "GOOD",
            "gate_status": "BLOCKED",
            "gate": {"ok": False, "allowed_next_stage": "cash"},
            "constraints": {"lease": {"active": False}},
            "outstanding_tasks": {"blockers": ["missing_active_lease"]},
            "next_actions": ["Create or activate the lease"],
            "stage_completion_summary": {"completed_count": 3, "total_count": 6},
        },
    )
    monkeypatch.setattr(
        workflow_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "property_id": property_id,
            "current_stage": "tenant",
            "gate_status": "BLOCKED",
        },
    )

    res = client.get("/api/workflow/state/55")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["state"]["property_id"] == 55
    assert body["state"]["current_stage"] == "tenant"
    assert body["state"]["normalized_decision"] == "GOOD"
    assert body["state"]["gate_status"] == "BLOCKED"
    assert body["state"]["next_actions"] == ["Create or activate the lease"]
    assert body["workflow"]["current_stage"] == "tenant"


def test_get_transition_returns_gate_payload(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "deal",
            "current_stage_label": "Deal",
            "decision_bucket": "GOOD",
            "gate": {
                "ok": True,
                "allowed_next_stage": "rehab",
                "allowed_next_stage_label": "Rehab",
            },
            "gate_status": "OPEN",
            "constraints": {},
            "next_actions": [],
            "stage_completion_summary": {"completed_count": 1, "total_count": 6},
        },
    )
    monkeypatch.setattr(
        workflow_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "deal",
            "next_stage": "rehab",
        },
    )

    res = client.get("/api/workflow/transition/12")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["transition"]["current_stage"] == "deal"
    assert body["transition"]["gate"]["ok"] is True
    assert body["transition"]["gate"]["allowed_next_stage"] == "rehab"
    assert body["workflow"]["next_stage"] == "rehab"


def test_advance_returns_409_when_gate_is_closed(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "compliance",
            "gate": {
                "ok": False,
                "allowed_next_stage": "tenant",
                "blocked_reason": "Pass inspection first.",
            },
            "constraints": {"inspection": {"passed": False}},
            "next_actions": ["Resolve failed inspection items"],
        },
    )
    monkeypatch.setattr(
        workflow_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "current_stage": "compliance",
            "gate_status": "BLOCKED",
        },
    )

    res = client.post("/api/workflow/advance/12")
    assert res.status_code == 409, res.text

    body = res.json()["detail"]
    assert body["error"] == "stage_transition_blocked"
    assert body["current_stage"] == "compliance"
    assert body["allowed_next_stage"] == "tenant"
    assert body["why"] == "Pass inspection first."


def test_advance_returns_state_and_workflow_when_gate_is_open(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "deal",
            "gate": {
                "ok": True,
                "allowed_next_stage": "rehab",
                "allowed_next_stage_label": "Rehab",
            },
            "constraints": {},
            "next_actions": [],
        },
    )

    calls = {"synced": 0}

    def _sync_property_state(db, org_id, property_id):
        calls["synced"] += 1
        return None

    monkeypatch.setattr(workflow_router, "sync_property_state", _sync_property_state)
    monkeypatch.setattr(
        workflow_router,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "deal",
            "suggested_stage": "deal",
            "current_stage_label": "Deal",
            "normalized_decision": "GOOD",
            "gate_status": "OPEN",
            "gate": {"ok": True, "allowed_next_stage": "rehab"},
            "constraints": {},
            "outstanding_tasks": {},
            "next_actions": [],
            "stage_completion_summary": {"completed_count": 1, "total_count": 6},
        },
    )
    monkeypatch.setattr(
        workflow_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, recompute=False: {
            "property_id": property_id,
            "current_stage": "deal",
            "next_stage": "rehab",
        },
    )

    res = client.post("/api/workflow/advance/12")
    assert res.status_code == 200, res.text

    body = res.json()
    assert body["ok"] is True
    assert body["property_id"] == 12
    assert body["advanced_to"] == "rehab"
    assert body["advanced_to_label"] == "Rehab"
    assert body["workflow"]["next_stage"] == "rehab"
    assert calls["synced"] == 1
    