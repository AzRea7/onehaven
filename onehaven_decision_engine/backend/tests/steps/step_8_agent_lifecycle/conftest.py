from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import get_db
from app.routers import agent_runs as agent_runs_router


class FakeScalarResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)


class FakeDB:
    """
    Queue-backed fake DB:
    - scalar(...) pops from scalar_queue
    - scalars(...) pops from scalars_queue and wraps in FakeScalarResult
    """

    def __init__(self):
        self.scalar_queue = []
        self.scalars_queue = []
        self.added = []
        self.commits = 0
        self.refreshed = []

    def queue_scalar(self, *items):
        self.scalar_queue.extend(items)

    def queue_scalars(self, *items):
        for rows in items:
            self.scalars_queue.append(list(rows))

    def scalar(self, stmt):
        if self.scalar_queue:
            return self.scalar_queue.pop(0)
        return None

    def scalars(self, stmt):
        if self.scalars_queue:
            return FakeScalarResult(self.scalars_queue.pop(0))
        return FakeScalarResult([])

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def refresh(self, obj):
        self.refreshed.append(obj)
        return obj

    def flush(self):
        return None


@pytest.fixture
def principal():
    return SimpleNamespace(
        org_id=1,
        org_slug="iss",
        user_id=99,
        role="owner",
        email="austin@demo.local",
        plan_code="pro",
    )


@pytest.fixture
def fake_db():
    return FakeDB()


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
