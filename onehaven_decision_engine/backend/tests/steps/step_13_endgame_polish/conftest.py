from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import get_principal
from app.db import SessionLocal, get_db
from app.models import (
    AppUser,
    Deal,
    Lease,
    OrgMembership,
    Organization,
    Property,
    PropertyChecklistItem,
    Transaction,
    UnderwritingResult,
    Valuation,
)
from app.routers import cash as cash_router
from app.routers import equity as equity_router
from app.routers import ops as ops_router
from app.routers import tenants as tenants_router


@pytest.fixture
def principal():
    return SimpleNamespace(
        org_id=1,
        org_slug="step13-org",
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
    org = Organization(slug="step13-org", name="Step 13 Org")
    user = AppUser(email="austin@demo.local", display_name="Austin")
    db.add(org)
    db.add(user)
    db.commit()
    db.refresh(org)
    db.refresh(user)

    membership = OrgMembership(org_id=org.id, user_id=user.id, role="owner")
    db.add(membership)
    db.commit()
    return {"org": org, "user": user}


@pytest.fixture
def seed_property(db, seed_org_user):
    org = seed_org_user["org"]
    prop = Property(
        org_id=org.id,
        address="123 Endgame Ave",
        city="Detroit",
        county="wayne",
        state="MI",
        zip="48201",
        bedrooms=3,
        bathrooms=1.5,
        square_feet=1200,
        year_built=1950,
        property_type="single_family",
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)
    return prop


@pytest.fixture
def seed_deal_underwriting(db, seed_org_user, seed_property):
    org = seed_org_user["org"]
    prop = seed_property

    deal = Deal(
        org_id=org.id,
        property_id=prop.id,
        asking_price=110000,
        rehab_estimate=12000,
        strategy="section8",
    )
    db.add(deal)
    db.commit()
    db.refresh(deal)

    uw = UnderwritingResult(
        org_id=org.id,
        deal_id=deal.id,
        decision="PASS",
        score=89,
        dscr=1.34,
        cash_flow=325.0,
        gross_rent_used=1650.0,
        mortgage_payment=845.0,
        operating_expenses=480.0,
        noi=1170.0,
        cash_on_cash=0.112,
        break_even_rent=1325.0,
    )
    db.add(uw)
    db.commit()
    db.refresh(uw)
    return {"deal": deal, "uw": uw}


@pytest.fixture
def app(monkeypatch, principal):
    app = FastAPI()
    app.include_router(tenants_router.router, prefix="/api")
    app.include_router(cash_router.router, prefix="/api")
    app.include_router(equity_router.router, prefix="/api")
    app.include_router(ops_router.router, prefix="/api")

    def _db_dep():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _db_dep
    app.dependency_overrides[get_principal] = lambda: principal

    for mod in (tenants_router, cash_router, equity_router):
        if hasattr(mod, "require_stage"):
            monkeypatch.setattr(mod, "require_stage", lambda *args, **kwargs: None)
        if hasattr(mod, "sync_property_state"):
            monkeypatch.setattr(mod, "sync_property_state", lambda *args, **kwargs: None)

    if hasattr(ops_router, "compute_and_persist_stage"):
        monkeypatch.setattr(
            ops_router,
            "compute_and_persist_stage",
            lambda db, org_id, property: SimpleNamespace(
                current_stage="equity",
                updated_at=datetime.utcnow(),
            ),
        )

    if hasattr(ops_router, "get_state_payload"):
        monkeypatch.setattr(
            ops_router,
            "get_state_payload",
            lambda db, org_id, property_id, recompute=False: {
                "current_stage": "equity",
                "constraints": {},
                "outstanding_tasks": {},
                "next_actions": [
                    "Confirm tenant is active",
                    "Review trailing 90-day cashflow",
                    "Update valuation after lease stabilization",
                ],
            },
        )

    if hasattr(ops_router, "build_workflow_summary"):
        monkeypatch.setattr(
            ops_router,
            "build_workflow_summary",
            lambda db, org_id, property_id, recompute=False: {
                "current_stage": "equity",
                "current_stage_label": "Equity",
                "next_actions": [
                    "Confirm tenant is active",
                    "Review trailing 90-day cashflow",
                    "Update valuation after lease stabilization",
                ],
                "primary_action": {
                    "kind": "review",
                    "title": "Review tenant, cash, and equity loop",
                },
            },
        )

    return app


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def seed_endgame_data(db, seed_org_user, seed_property, seed_deal_underwriting):
    org = seed_org_user["org"]
    prop = seed_property

    from app.models import Tenant

    tenant = Tenant(
        org_id=org.id,
        full_name="Dorina Example",
        phone="555-1010",
        email="dorina@example.com",
        voucher_status="approved",
        notes="Strong applicant",
    )
    db.add(tenant)
    db.commit()
    db.refresh(tenant)

    now = datetime.utcnow()

    lease = Lease(
        org_id=org.id,
        property_id=prop.id,
        tenant_id=tenant.id,
        start_date=now - timedelta(days=15),
        end_date=now + timedelta(days=350),
        total_rent=1650.0,
        tenant_portion=250.0,
        housing_authority_portion=1400.0,
        hap_contract_status="active",
        notes="Initial 12-month lease",
    )
    db.add(lease)

    txns = [
        Transaction(
            org_id=org.id,
            property_id=prop.id,
            txn_date=now - timedelta(days=10),
            txn_type="income",
            amount=1650.0,
            memo="April rent",
        ),
        Transaction(
            org_id=org.id,
            property_id=prop.id,
            txn_date=now - timedelta(days=8),
            txn_type="expense",
            amount=220.0,
            memo="Plumbing repair",
        ),
        Transaction(
            org_id=org.id,
            property_id=prop.id,
            txn_date=now - timedelta(days=6),
            txn_type="capex",
            amount=1800.0,
            memo="Water heater replacement",
        ),
    ]
    db.add_all(txns)

    val = Valuation(
        org_id=org.id,
        property_id=prop.id,
        as_of=now - timedelta(days=2),
        estimated_value=152000.0,
        loan_balance=101500.0,
        notes="Broker opinion",
    )
    db.add(val)

    checklist = [
        PropertyChecklistItem(
            org_id=org.id,
            property_id=prop.id,
            item_code="SMOKE_DETECTORS",
            category="safety",
            description="Smoke detectors installed",
            severity=3,
            common_fail=True,
            status="done",
            is_completed=True,
        ),
        PropertyChecklistItem(
            org_id=org.id,
            property_id=prop.id,
            item_code="HANDRAILS",
            category="safety",
            description="Handrails secured",
            severity=2,
            common_fail=True,
            status="in_progress",
            is_completed=False,
        ),
    ]
    db.add_all(checklist)

    db.commit()
    return {
        "tenant": tenant,
        "lease": lease,
        "transactions": txns,
        "valuation": val,
        "property": prop,
    }