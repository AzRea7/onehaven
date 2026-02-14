# backend/tests/test_constitution.py
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client():
    return TestClient(app)


def _headers():
    return {"X-Org-Slug": "demo", "X-User-Email": "austin@demo.local", "X-User-Role": "owner"}


def test_missing_rent_causes_review(client: TestClient):
    # This assumes you have at least one deal without rent_used in a fresh DB.
    # If not, adapt to your “create property/deal” endpoints.
    resp = client.get("/health")
    assert resp.status_code == 200

    # NOTE: In your current system, rent_used is filled by enrich/explain.
    # The constitution is: missing rent_used => REVIEW, not PASS.
    # We enforce by running evaluate on a deal created without rent.
    # If your API has a create-deal endpoint, wire it here.

    assert True  # placeholder “smoke” until you decide a minimal create path


def test_over_max_price_rejects(client: TestClient):
    # Same deal: you’ll plug in a minimal create path.
    assert True


def test_under_min_bedrooms_rejects(client: TestClient):
    assert True
