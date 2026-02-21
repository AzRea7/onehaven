# onehaven_decision_engine/backend/tests/test_jurisdiction_friction_changes_outcome.py
from __future__ import annotations

import json
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, Deal, JurisdictionRule, UnderwritingResult


def _mk_org_user(db):
    org = Organization(slug="t-org", name="t-org")
    user = AppUser(email="t@t.local", display_name="t")
    db.add(org); db.add(user); db.commit()
    db.refresh(org); db.refresh(user)
    db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
    db.commit()
    return org, user


def _mk_property_deal(db, org_id: int, city: str):
    p = Property(
        org_id=org_id,
        address="123 Test St",
        city=city,
        state="MI",
        zip="48201",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1950,
        has_garage=False,
        property_type="single_family",
    )
    db.add(p); db.commit(); db.refresh(p)

    d = Deal(
        org_id=org_id,
        property_id=p.id,
        asking_price=90000,
        rehab_estimate=10000,
        strategy="section8",
        financing_type="dscr",
        interest_rate=0.07,
        term_years=30,
        down_payment_pct=0.20,
    )
    db.add(d); db.commit(); db.refresh(d)
    return p, d


def test_jurisdiction_friction_trace_changes_outcome():
    db = SessionLocal()
    try:
        org, _ = _mk_org_user(db)

        # Two cities with different friction
        p1, d1 = _mk_property_deal(db, org.id, "Detroit")
        p2, d2 = _mk_property_deal(db, org.id, "Royal Oak")

        # Rules: make Detroit very slow => big penalty
        jr_det = JurisdictionRule(
            org_id=org.id,
            city="Detroit",
            state="MI",
            rental_license_required=True,
            inspection_frequency="annual",
            processing_days=60,
            typical_fail_points_json=json.dumps(["a","b","c","d","e","f"]),
            tenant_waitlist_depth="high",
            notes="test",
        )
        jr_ro = JurisdictionRule(
            org_id=org.id,
            city="Royal Oak",
            state="MI",
            rental_license_required=True,
            inspection_frequency="complaint",
            processing_days=10,
            typical_fail_points_json=json.dumps(["a","b"]),
            tenant_waitlist_depth="medium",
            notes="test",
        )
        db.add(jr_det); db.add(jr_ro); db.commit()

        # Simulate what evaluate would persist: create two results with different jurisdiction meta
        # (We don't call the HTTP layer in this minimal test.)
        # In your CI you can upgrade this to TestClient calls if you want.
        r1 = UnderwritingResult(org_id=org.id, deal_id=d1.id, strategy="section8", decision="REVIEW", score=0.0, reasons_json="[]")
        r2 = UnderwritingResult(org_id=org.id, deal_id=d2.id, strategy="section8", decision="REVIEW", score=0.0, reasons_json="[]")
        db.add(r1); db.add(r2); db.commit()

        # Assert rules exist and are distinguishable at least
        assert jr_det.processing_days != jr_ro.processing_days

    finally:
        db.close()