from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal, require_owner
from app.db import get_db
from app.routers.compliance import router as compliance_router
from app.routers.policy import router as policy_router
from app.routers.policy_evidence import router as policy_evidence_router
from app.routers.jurisdiction_profiles import router as jurisdiction_profiles_router


class _FakeScalarResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)


class _FakeQuery:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def count(self):
        return len(self._rows)


class _FakeDB:
    def scalar(self, *args, **kwargs):
        return None

    def scalars(self, *args, **kwargs):
        return _FakeScalarResult([])

    def execute(self, *args, **kwargs):
        return []

    def query(self, *args, **kwargs):
        # Return empty queryable results for smoke tests
        return _FakeQuery([])


@pytest.fixture
def fake_principal():
    return SimpleNamespace(
        org_id=1,
        org_slug="iss",
        user_id=99,
        role="owner",
        email="owner@test.local",
        plan_code="pro",
    )


@pytest.fixture
def app(fake_principal):
    app = FastAPI()

    app.include_router(compliance_router, prefix="/api")
    app.include_router(policy_router, prefix="/api")
    app.include_router(policy_evidence_router, prefix="/api")
    app.include_router(jurisdiction_profiles_router, prefix="/api")

    def _fake_db():
        yield _FakeDB()

    app.dependency_overrides[get_db] = _fake_db
    app.dependency_overrides[get_principal] = lambda: fake_principal
    app.dependency_overrides[require_owner] = lambda: fake_principal

    return app


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def auth_headers():
    return {
        "X-Org-Slug": "iss",
    }
