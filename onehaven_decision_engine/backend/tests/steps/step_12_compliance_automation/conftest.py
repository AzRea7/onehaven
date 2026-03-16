from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import SessionLocal, get_db
from app.models import (
    AppUser,
    Organization,
    OrgMembership,
    Property,
)
from app.routers import compliance as compliance_router


@pytest.fixture
def principal():
    return SimpleNamespace(
        org_id=1,
        org_slug="step12-org",
        user_id=1,
        role="owner",
        email="austin@demo.local",
        plan_code="pro",
    )


@pytest.fixture
def db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def seed_org_user(db):
    org = Organization(slug="step12-org", name="Step 12 Org")
    user = AppUser(email="austin@demo.local", display_name="Austin")

    db.add(org)
    db.add(user)
    db.commit()
    db.refresh(org)
    db.refresh(user)

    db.add(
        OrgMembership(
            org_id=org.id,
            user_id=user.id,
            role="owner",
        )
    )
    db.commit()

    return {"org": org, "user": user}


@pytest.fixture
def seed_property(db, seed_org_user):
    org = seed_org_user["org"]

    prop = Property(
        org_id=org.id,
        address="123 Readiness Ave",
        city="Warren",
        county="macomb",
        state="MI",
        zip="48093",
        bedrooms=3,
        bathrooms=1.5,
        square_feet=1200,
        year_built=1955,
        property_type="single_family",
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)
    return prop


@pytest.fixture
def test_app(monkeypatch, principal):
    app = FastAPI()
    app.include_router(compliance_router.router, prefix="/api")

    def _db_dep():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _db_dep
    app.dependency_overrides[get_principal] = lambda: principal

    # Step-12 router tests should validate automation logic, not get blocked by stage gating noise.
    if hasattr(compliance_router, "require_stage"):
        monkeypatch.setattr(compliance_router, "require_stage", lambda *args, **kwargs: None)

    if hasattr(compliance_router, "sync_property_state"):
        monkeypatch.setattr(compliance_router, "sync_property_state", lambda *args, **kwargs: None)

    if hasattr(compliance_router, "build_workflow_summary"):
        monkeypatch.setattr(
            compliance_router,
            "build_workflow_summary",
            lambda *args, **kwargs: {
                "stage": "compliance",
                "status": "ok",
                "health": "green",
            },
        )

    return app


@pytest.fixture
def client(test_app):
    return TestClient(test_app)