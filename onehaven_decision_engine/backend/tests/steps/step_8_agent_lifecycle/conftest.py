from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import agent_runs as agent_runs_router


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
    return SimpleNamespace()


@pytest.fixture
def test_app(fake_db, principal):
    app = FastAPI()
    app.include_router(agent_runs_router.router, prefix="/api")

    def _fake_db_dep():
        yield fake_db

    app.dependency_overrides[get_db] = _fake_db_dep
    app.dependency_overrides[get_principal] = lambda: principal
    return app


@pytest.fixture
def client(test_app):
    return TestClient(test_app)