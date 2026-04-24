from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import require_owner
from app.db import get_db
from app.routers import policy as policy_router


@pytest.fixture
def app(monkeypatch):
    app = FastAPI()
    app.include_router(policy_router.router, prefix="/api")

    def fake_db():
        yield SimpleNamespace()

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[require_owner] = lambda: SimpleNamespace(
        org_id=1,
        user_id=99,
        role="owner",
        email="owner@test.local",
    )

    monkeypatch.setattr(
        policy_router,
        "cleanup_market",
        lambda db, org_id, reviewer_user_id, state, county, city, pha_name=None, archive_extracted_duplicates=True, focus="se_mi_extended": {
            "ok": True,
            "market": {
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
            },
            "org_id": org_id,
            "reviewer_user_id": reviewer_user_id,
            "archive_extracted_duplicates": archive_extracted_duplicates,
            "focus": focus,
            "cleanup": {
                "sources_reviewed": 12,
                "archived_duplicates": 4,
                "stale_marked": 3,
            },
        },
    )

    return app


def test_market_cleanup_route_returns_cleanup_summary(app):
    client = TestClient(app)

    res = client.post(
        "/api/policy/market/cleanup-stale",
        json={
            "state": "MI",
            "county": "macomb",
            "city": "warren",
            "org_scope": False,
            "archive_extracted_duplicates": True,
        },
    )
    assert res.status_code == 200, res.text

    out = res.json()
    assert out["ok"] is True
    assert out["market"]["state"] == "MI"
    assert out["market"]["county"] == "macomb"
    assert out["market"]["city"] == "warren"
    assert out["archive_extracted_duplicates"] is True
    assert out["cleanup"]["archived_duplicates"] == 4
    