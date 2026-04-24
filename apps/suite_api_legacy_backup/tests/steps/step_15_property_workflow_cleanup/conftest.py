# backend/tests/steps/step_15_property_workflow_cleanup/conftest.py
from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

BACKEND_ROOT = Path(__file__).resolve().parents[3]

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.auth import Principal, get_principal
from app.db import SessionLocal
from app.main import app
from app.models import AppUser, OrgMembership, Organization


@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def auth_context(db_session):
    unique = uuid4().hex[:8]
    org_slug = f"step15-{unique}"
    user_email = f"step15-{unique}@demo.local"

    org = Organization(slug=org_slug, name=f"Step 15 Org {unique}")
    user = AppUser(email=user_email, display_name="Step 15 User")

    db_session.add(org)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(org)
    db_session.refresh(user)

    membership = OrgMembership(
        org_id=org.id,
        user_id=user.id,
        role="owner",
    )
    db_session.add(membership)
    db_session.commit()
    db_session.refresh(membership)

    principal = Principal(
        org_id=org.id,
        org_slug=org.slug,
        user_id=user.id,
        email=user.email,
        role="owner",
        plan_code=None,
    )

    def override_get_principal():
        return principal

    app.dependency_overrides[get_principal] = override_get_principal

    headers = {
        "X-Org-Slug": org.slug,
        "X-User-Email": user.email,
        "X-User-Role": "owner",
    }

    client = TestClient(app)

    try:
        yield {
            "client": client,
            "headers": headers,
            "org": org,
            "user": user,
            "membership": membership,
            "principal": principal,
        }
    finally:
        app.dependency_overrides.pop(get_principal, None)


@pytest.fixture
def client_with_auth_headers(auth_context):
    return auth_context["client"], auth_context["headers"]