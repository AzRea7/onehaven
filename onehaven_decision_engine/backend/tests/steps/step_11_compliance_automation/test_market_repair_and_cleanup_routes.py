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
        "repair_market",
        lambda db, org_id, reviewer_user_id, state, county, city, pha_name=None, focus="se_mi_extended", archive_extracted_duplicates=True: {
            "ok": True,
            "market": {
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
            },
            "sources_refreshed": {
                "count": 4,
                "ok_count": 4,
                "failed_count": 0,
                "source_ids": [1, 2, 3, 4],
            },
            "assertions_created": {
                "count": 7,
                "results": [],
            },
            "assertions_verified": {
                "updated_count": 3,
                "updated_ids": [11, 12, 13],
            },
            "duplicates_superseded": {
                "superseded_count": 2,
                "superseded_ids": [14, 15],
            },
            "cleanup": {
                "cleaned_count": 2,
                "cleaned_ids": [14, 15],
                "stale_resolved_count": 1,
                "stale_resolved_ids": [14],
                "archived_duplicate_count": 1,
                "archived_duplicate_ids": [15],
                "stale_items_remaining": 0,
                "stale_item_ids_remaining": [],
            },
            "profile": {
                "id": 22,
                "org_id": 1,
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
                "friction_multiplier": 1.15,
                "notes": "test profile",
            },
            "coverage": {
                "id": 33,
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
                "coverage_status": "verified_extended",
                "production_readiness": "ready",
                "confidence_label": "high",
                "verified_rule_count": 6,
                "source_count": 4,
                "fetch_failure_count": 0,
                "stale_warning_count": 0,
                "verified_rule_keys": [
                    "rental_registration_required",
                    "inspection_program_exists",
                    "mi_statute_anchor",
                    "federal_hcv_regulations_anchor",
                    "federal_nspire_anchor",
                ],
                "municipal_core_ok": True,
                "state_federal_core_ok": True,
                "pha_core_ok": True,
            },
            "brief": {"ok": True},
            "stale_items_remaining": 0,
            "unresolved_rule_gaps": [],
            "issues_remaining": [],
        },
    )

    monkeypatch.setattr(
        policy_router,
        "cleanup_market",
        lambda db, org_id, reviewer_user_id, state, county, city, pha_name=None, archive_extracted_duplicates=True: {
            "ok": True,
            "market": {
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
            },
            "cleanup": {
                "cleaned_count": 3,
                "cleaned_ids": [1, 2, 3],
                "stale_resolved_count": 2,
                "stale_resolved_ids": [1, 2],
                "archived_duplicate_count": 1,
                "archived_duplicate_ids": [3],
                "stale_items_remaining": 0,
                "stale_item_ids_remaining": [],
            },
            "coverage": {
                "id": 9,
                "coverage_status": "verified_extended",
                "production_readiness": "ready",
                "stale_warning_count": 0,
            },
        },
    )

    return app


def test_market_repair_route_returns_structured_summary(app):
    client = TestClient(app)

    res = client.post(
      "/api/policy/market/repair",
      json={
          "state": "MI",
          "county": "macomb",
          "city": "warren",
          "org_scope": False,
          "focus": "se_mi_extended",
          "archive_extracted_duplicates": True,
      },
    )

    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["market"]["city"] == "warren"
    assert body["sources_refreshed"]["count"] == 4
    assert body["assertions_created"]["count"] == 7
    assert body["assertions_verified"]["updated_count"] == 3
    assert body["stale_items_remaining"] == 0
    assert body["unresolved_rule_gaps"] == []


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

    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["cleanup"]["cleaned_count"] == 3
    assert body["cleanup"]["stale_items_remaining"] == 0
    assert body["coverage"]["stale_warning_count"] == 0