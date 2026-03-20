from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.automation import router as automation_router
from app.routers.evaluate import router as evaluate_router
from app.auth import get_principal, require_operator
from app.db import get_db


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(automation_router)
    app.include_router(evaluate_router)

    app.dependency_overrides[get_principal] = lambda: SimpleNamespace(
        org_id=1,
        user_id=123,
    )
    app.dependency_overrides[require_operator] = lambda: SimpleNamespace(
        org_id=1,
        user_id=123,
        role="operator",
    )
    app.dependency_overrides[get_db] = lambda: None
    return app


def test_legacy_automation_ingest_route_returns_410():
    app = _build_app()
    client = TestClient(app)

    res = client.post("/automation/ingest/run")
    assert res.status_code == 410

    body = res.json()
    assert body["detail"]["code"] == "legacy_ingest_route_removed"
    assert "/ingestion/" in body["detail"]["message"]


def test_legacy_snapshot_evaluate_route_returns_410():
    app = _build_app()
    client = TestClient(app)

    res = client.post("/evaluate/snapshot/77")
    assert res.status_code == 410

    body = res.json()
    assert body["detail"]["code"] == "legacy_snapshot_evaluation_removed"
    assert body["detail"]["snapshot_id"] == 77


def test_legacy_evaluate_run_route_returns_410():
    app = _build_app()
    client = TestClient(app)

    res = client.post("/evaluate/run")
    assert res.status_code == 410

    body = res.json()
    assert body["detail"]["code"] == "legacy_snapshot_evaluation_removed"
    assert "property-first" in body["detail"]["message"].lower()
    