import json
from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import workflow as workflow_router_module


@pytest.fixture
def test_app():
    app = FastAPI()
    app.include_router(workflow_router_module.router, prefix="/api")

    def fake_db():
        yield SimpleNamespace()

    def fake_principal():
        return SimpleNamespace(
            org_id=1,
            user_id=99,
            email="tester@example.com",
            role="owner",
        )

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_principal] = fake_principal
    return app


@pytest.fixture
def client(test_app):
    return TestClient(test_app)


def test_get_state_returns_payload(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "compliance",
            "suggested_stage": "compliance",
            "next_actions": ["Create or record inspection."],
        },
    )

    res = client.get("/api/workflow/state/123")
    assert res.status_code == 200
    body = res.json()
    assert body["property_id"] == 123
    assert body["current_stage"] == "compliance"
    assert body["next_actions"] == ["Create or record inspection."]


def test_get_transition_returns_payload(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "deal",
            "suggested_stage": "deal",
            "gate": {
                "ok": False,
                "blocked_reason": "Run underwriting evaluation first.",
                "allowed_next_stage": "decision",
            },
            "next_actions": ["Run underwriting evaluation for the deal."],
            "constraints": {"missing_underwriting": True},
            "outstanding_tasks": {},
        },
    )

    res = client.get("/api/workflow/transition/123")
    assert res.status_code == 200
    body = res.json()
    assert body["current_stage"] == "deal"
    assert body["gate"]["ok"] is False
    assert body["gate"]["allowed_next_stage"] == "decision"


def test_advance_returns_409_when_transition_is_blocked(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "rehab_exec",
            "gate": {
                "ok": False,
                "blocked_reason": "Complete rehab execution tasks first.",
                "allowed_next_stage": "compliance",
            },
            "constraints": {"rehab_open_tasks": 2},
            "next_actions": ["Complete rehab execution tasks (2 open)."],
        },
    )

    res = client.post("/api/workflow/advance/123")
    assert res.status_code == 409
    body = res.json()["detail"]
    assert body["error"] == "stage_transition_blocked"
    assert body["current_stage"] == "rehab_exec"
    assert body["allowed_next_stage"] == "compliance"
    assert body["constraints"] == {"rehab_open_tasks": 2}


def test_advance_returns_ok_when_transition_is_open(client, monkeypatch):
    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_transition_payload",
        lambda db, org_id, property_id: {
            "property_id": property_id,
            "current_stage": "tenant",
            "gate": {
                "ok": True,
                "blocked_reason": None,
                "allowed_next_stage": "lease",
            },
            "constraints": {},
            "next_actions": ["Create lease."],
        },
    )
    monkeypatch.setattr(
        workflow_router_module,
        "sync_property_state",
        lambda db, org_id, property_id: None,
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "lease",
            "suggested_stage": "lease",
            "next_actions": ["Activate a lease for this property."],
        },
    )

    res = client.post("/api/workflow/advance/123")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["advanced_to"] == "lease"
    assert body["state"]["current_stage"] == "lease"


def test_upsert_state_blocks_manual_stage_jump_past_computed_truth(client, monkeypatch):
    row = SimpleNamespace(
        current_stage="deal",
        constraints_json=json.dumps({}),
        outstanding_tasks_json=json.dumps({}),
        updated_at=datetime.utcnow(),
    )

    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "ensure_state_row",
        lambda db, org_id, property_id: row,
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "deal",
            "suggested_stage": "deal",
            "next_actions": ["Run underwriting evaluation for the deal."],
        },
    )

    res = client.post(
        "/api/workflow/state",
        json={
            "property_id": 123,
            "current_stage": "cash",
            "constraints": {},
            "outstanding_tasks": {},
        },
    )

    assert res.status_code == 409
    detail = res.json()["detail"]
    assert detail["error"] == "manual_stage_override_blocked"
    assert detail["requested_stage"] == "cash"
    assert detail["suggested_stage"] == "deal"


def test_upsert_state_allows_metadata_merge_and_syncs_back_to_truth(client, monkeypatch):
    row = SimpleNamespace(
        current_stage="deal",
        constraints_json=json.dumps({"existing_flag": True}),
        outstanding_tasks_json=json.dumps({"rehab": {"open": 1}}),
        updated_at=datetime.utcnow(),
    )

    monkeypatch.setattr(
        workflow_router_module,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(
        workflow_router_module,
        "ensure_state_row",
        lambda db, org_id, property_id: row,
    )
    monkeypatch.setattr(
        workflow_router_module,
        "sync_property_state",
        lambda db, org_id, property_id: row,
    )
    monkeypatch.setattr(
        workflow_router_module,
        "get_state_payload",
        lambda db, org_id, property_id, recompute=True: {
            "property_id": property_id,
            "current_stage": "deal",
            "suggested_stage": "deal",
            "constraints": {"existing_flag": True, "new_flag": "x"},
            "outstanding_tasks": {"rehab": {"open": 1}, "deal": {"missing": False}},
            "next_actions": ["Run underwriting evaluation for the deal."],
        },
    )

    res = client.post(
        "/api/workflow/state",
        json={
            "property_id": 123,
            "constraints": {"new_flag": "x"},
            "outstanding_tasks": {"deal": {"missing": False}},
        },
    )

    assert res.status_code == 200
    body = res.json()
    assert body["current_stage"] == "deal"
    assert body["constraints"]["existing_flag"] is True
    assert body["constraints"]["new_flag"] == "x"