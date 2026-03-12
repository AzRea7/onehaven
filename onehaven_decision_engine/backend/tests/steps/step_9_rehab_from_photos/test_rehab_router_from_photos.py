from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import rehab as rehab_router


@pytest.fixture
def app(monkeypatch):
    app = FastAPI()
    app.include_router(rehab_router.router, prefix="/api")

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
        rehab_router,
        "_get_property_or_404",
        lambda db, org_id, property_id: SimpleNamespace(id=property_id, org_id=org_id),
    )
    monkeypatch.setattr(rehab_router, "require_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        rehab_router,
        "analyze_property_photos",
        lambda db, org_id, property_id: {
            "ok": True,
            "property_id": property_id,
            "photo_count": 4,
            "summary": {"interior": 3, "exterior": 1, "unknown": 0},
            "issues": [
                {
                    "title": "Life-safety interior punch list",
                    "category": "safety",
                    "severity": "critical",
                    "estimated_cost": 1200.0,
                    "blocker": True,
                    "notes": "test",
                    "evidence_photo_ids": [1, 2],
                }
            ],
            "created_task_ids": [],
        },
    )
    monkeypatch.setattr(
        rehab_router,
        "analyze_and_create_rehab_tasks",
        lambda db, org_id, property_id: {
            "ok": True,
            "property_id": property_id,
            "photo_count": 4,
            "summary": {"interior": 3, "exterior": 1, "unknown": 0},
            "issues": [],
            "created": 2,
            "created_task_ids": [11, 12],
        },
    )

    return app


def test_preview_rehab_from_photos(app):
    client = TestClient(app)
    res = client.get("/api/rehab/from-photos/123")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["property_id"] == 123
    assert body["photo_count"] == 4


def test_generate_rehab_from_photos(app):
    client = TestClient(app)
    res = client.post("/api/rehab/from-photos/123")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["created"] == 2
    assert body["created_task_ids"] == [11, 12]