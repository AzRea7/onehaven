# backend/tests/test_jurisdiction_friction_changes_outcome.py
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, Deal, JurisdictionRule, UnderwritingResult
from app.domain.decision_engine import evaluate_deal  # assumes your project has this
from app.config import settings


def _mk_org_user(db):
    org = Organization(slug="t-org", name="t-org", created_at=datetime.utcnow())
    user = AppUser(email="t@t.local", display_name="t", created_at=datetime.utcnow())
    db.add(org)
    db.add(user)
    db.commit()
    db.refresh(org)
    db.refresh(user)

    mem = OrgMembership(org_id=org.id, user_id=user.id, role="owner", created_at=datetime.utcnow())
    db.add(mem)
    db.commit()
    return org, user


def test_same_deal_different_jurisdiction_changes_score_or_reasons():
    db = SessionLocal()
    try:
        org, _user = _mk_org_user(db)

        # Same deal inputs; only city changes.
        p1 = Property(
            org_id=org.id,
            address="1 A St",
            city="Detroit",
            state="MI",
            zip="48201",
            bedrooms=3,
            bathrooms=1.0,
            created_at=datetime.utcnow(),
        )
        p2 = Property(
            org_id=org.id,
            address="2 B St",
            city="Royal Oak",
            state="MI",
            zip="48067",
            bedrooms=3,
            bathrooms=1.0,
            created_at=datetime.utcnow(),
        )
        db.add_all([p1, p2])
        db.commit()
        db.refresh(p1)
        db.refresh(p2)

        d1 = Deal(org_id=org.id, property_id=p1.id, asking_price=120000, rehab_estimate=0.0, strategy="section8")
        d2 = Deal(org_id=org.id, property_id=p2.id, asking_price=120000, rehab_estimate=0.0, strategy="section8")
        db.add_all([d1, d2])
        db.commit()
        db.refresh(d1)
        db.refresh(d2)

        # Jurisdiction rules: Detroit is harsher than Royal Oak (processing_days etc.)
        jr_detroit = JurisdictionRule(
            org_id=org.id,
            city="Detroit",
            state="MI",
            rental_license_required=True,
            processing_days=30,
            typical_fail_points_json='["GFCI missing","peeling paint"]',
            updated_at=datetime.utcnow(),
        )
        jr_ro = JurisdictionRule(
            org_id=org.id,
            city="Royal Oak",
            state="MI",
            rental_license_required=True,
            processing_days=5,
            typical_fail_points_json='["handrails"]',
            updated_at=datetime.utcnow(),
        )
        db.add_all([jr_detroit, jr_ro])
        db.commit()

        # Evaluate both (your evaluate_deal should write UnderwritingResult; if it returns only,
        # adapt this test to your actual API)
        r1 = evaluate_deal(db, org_id=org.id, deal_id=d1.id, decision_version=settings.decision_version)
        r2 = evaluate_deal(db, org_id=org.id, deal_id=d2.id, decision_version=settings.decision_version)

        # Assertion: at minimum, jurisdiction reasons should differ; score often differs too.
        assert r1.jurisdiction_reasons_json is not None
        assert r2.jurisdiction_reasons_json is not None

        j1 = json.loads(r1.jurisdiction_reasons_json or "[]")
        j2 = json.loads(r2.jurisdiction_reasons_json or "[]")
        assert j1 != j2 or r1.score != r2.score

    finally:
        db.close()