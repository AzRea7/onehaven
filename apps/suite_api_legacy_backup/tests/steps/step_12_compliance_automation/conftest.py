from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.auth import get_principal
from app.db import engine, get_db
from app.models import AppUser, Organization, Property
from app.routers.compliance import router as compliance_router


@pytest.fixture
def db():
    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
    session: Session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture
def seed_org_user(db):
    unique = uuid.uuid4().hex[:10]

    org = Organization(slug=f"step12-org-{unique}", name="Step 12 Org")
    user = AppUser(
        email=f"austin+step12-{unique}@demo.local",
        display_name="Austin",
    )

    db.add(org)
    db.add(user)
    db.commit()
    db.refresh(org)
    db.refresh(user)

    return {"org": org, "user": user}


@pytest.fixture
def seed_property(db, seed_org_user):
    org = seed_org_user["org"]

    prop = Property(
        org_id=org.id,
        address="123 Compliance St",
        city="Warren",
        county="Macomb",
        state="MI",
        zip="48088",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1955,
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)
    return prop


@pytest.fixture
def app(db, seed_org_user):
    app = FastAPI()
    app.include_router(compliance_router, prefix="/api")

    org = seed_org_user["org"]
    user = seed_org_user["user"]

    def _db_override():
        yield db

    app.dependency_overrides[get_db] = _db_override
    app.dependency_overrides[get_principal] = lambda: SimpleNamespace(
        org_id=org.id,
        org_slug=org.slug,
        user_id=user.id,
        role="owner",
        email=user.email,
        plan_code="pro",
    )

    return app


@pytest.fixture
def client(app):
    return TestClient(app)
    