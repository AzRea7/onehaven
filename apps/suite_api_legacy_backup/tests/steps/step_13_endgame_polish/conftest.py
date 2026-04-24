from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.auth import get_principal
from app.db import engine, get_db
from app.models import (
    AppUser,
    Deal,
    Lease,
    Organization,
    Property,
    PropertyChecklistItem,
    PropertyState,
    RehabTask,
    Tenant,
    Transaction,
    UnderwritingResult,
    Valuation,
)
from app.routers.cash import router as cash_router
from app.routers.equity import router as equity_router
from app.routers.ops import router as ops_router
from app.routers.tenants import router as tenants_router


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

    org = Organization(slug=f"step13-org-{unique}", name="Step 13 Org")
    user = AppUser(
        email=f"austin+step13-{unique}@demo.local",
        display_name="Austin",
    )

    db.add(org)
    db.add(user)
    db.commit()
    db.refresh(org)
    db.refresh(user)

    return {"org": org, "user": user}


def _build_tenant(*, org_id: int, property_id: int) -> Tenant:
    """
    Build a tenant row without assuming your exact Tenant model shape.
    We only set fields that actually exist on the mapped model.
    """
    unique = f"{org_id}-{property_id}-{uuid.uuid4().hex[:8]}"

    tenant = Tenant(org_id=org_id)

    # Common tenant field variants across schemas
    candidates = {
        "property_id": property_id,
        "first_name": "Test",
        "last_name": "Tenant",
        "full_name": "Test Tenant",
        "name": "Test Tenant",
        "display_name": "Test Tenant",
        "email": f"tenant-{unique}@demo.local",
        "phone": "3135550101",
        "phone_number": "3135550101",
        "mobile_phone": "3135550101",
        "status": "active",
        "lease_status": "active",
        "notes": "Seeded test tenant",
    }

    for attr, value in candidates.items():
        if hasattr(Tenant, attr):
            setattr(tenant, attr, value)

    return tenant


@pytest.fixture
def seed_endgame_data(db, seed_org_user):
    org = seed_org_user["org"]
    now = datetime.utcnow()

    prop = Property(
        org_id=org.id,
        address="456 Endgame Ave",
        city="Detroit",
        county="Wayne",
        state="MI",
        zip="48201",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1300,
        year_built=1948,
        crime_score=35.0,
        offender_count=1,
        is_red_zone=False,
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)

    deal = Deal(
        org_id=org.id,
        property_id=prop.id,
        strategy="section8",
        asking_price=85000,
    )
    db.add(deal)
    db.commit()
    db.refresh(deal)

    uw = UnderwritingResult(
        org_id=org.id,
        deal_id=deal.id,
        decision="PASS",
        score=87,
        dscr=1.32,
        cash_flow=420,
        gross_rent_used=1650,
    )
    db.add(uw)

    db.add(
        PropertyState(
            org_id=org.id,
            property_id=prop.id,
            current_stage="equity",
        )
    )

    db.add_all(
        [
            PropertyChecklistItem(
                org_id=org.id,
                property_id=prop.id,
                item_code="CHK-1",
                category="safety",
                description="Smoke detectors",
                status="done",
            ),
            PropertyChecklistItem(
                org_id=org.id,
                property_id=prop.id,
                item_code="CHK-2",
                category="safety",
                description="Handrails",
                status="done",
            ),
        ]
    )

    db.add_all(
        [
            RehabTask(
                org_id=org.id,
                property_id=prop.id,
                title="Patch drywall",
                status="done",
                cost_estimate=300,
            ),
            RehabTask(
                org_id=org.id,
                property_id=prop.id,
                title="Replace GFCI",
                status="done",
                cost_estimate=120,
            ),
        ]
    )

    tenant = _build_tenant(org_id=org.id, property_id=prop.id)
    db.add(tenant)
    db.commit()
    db.refresh(tenant)

    lease = Lease(
        org_id=org.id,
        property_id=prop.id,
        tenant_id=tenant.id,
        start_date=now - timedelta(days=20),
        end_date=now + timedelta(days=345),
        total_rent=1650,
        tenant_portion=250,
        housing_authority_portion=1400,
        hap_contract_status="active",
    )
    if hasattr(lease, "status"):
        lease.status = "active"
    db.add(lease)

    db.add_all(
        [
            Transaction(
                org_id=org.id,
                property_id=prop.id,
                txn_type="rent",
                amount=1650,
                txn_date=now - timedelta(days=10),
            ),
            Transaction(
                org_id=org.id,
                property_id=prop.id,
                txn_type="expense",
                amount=300,
                txn_date=now - timedelta(days=8),
            ),
            Transaction(
                org_id=org.id,
                property_id=prop.id,
                txn_type="capex",
                amount=1800,
                txn_date=now - timedelta(days=30),
            ),
        ]
    )

    db.add(
        Valuation(
            org_id=org.id,
            property_id=prop.id,
            as_of=now - timedelta(days=1),
            estimated_value=152000,
            loan_balance=101500,
            notes="Recent comp set",
        )
    )

    db.commit()
    db.refresh(prop)
    db.refresh(deal)

    return {
        "org": org,
        "property": prop,
        "deal": deal,
        "tenant": tenant,
    }


@pytest.fixture
def app(db, seed_org_user):
    app = FastAPI()

    app.include_router(cash_router, prefix="/api")
    app.include_router(equity_router, prefix="/api")
    app.include_router(ops_router, prefix="/api")
    app.include_router(tenants_router, prefix="/api")

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
