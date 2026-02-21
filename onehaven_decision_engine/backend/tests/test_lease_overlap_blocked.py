from __future__ import annotations

import pytest
from datetime import datetime

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, Tenant, Lease
from app.services.lease_rules import ensure_no_lease_overlap


def _mk_org_user(db):
    org = Organization(slug="t-org2", name="t-org2", created_at=datetime.utcnow())
    user = AppUser(email="t2@t.local", display_name="t2", created_at=datetime.utcnow())
    db.add(org); db.add(user); db.commit()
    db.refresh(org); db.refresh(user)
    db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner", created_at=datetime.utcnow()))
    db.commit()
    return org


def test_overlap_blocked():
    db = SessionLocal()
    try:
        org = _mk_org_user(db)
        p = Property(org_id=org.id, address="1", city="Detroit", state="MI", zip="48201", bedrooms=3, bathrooms=1.0, created_at=datetime.utcnow())
        t = Tenant(org_id=org.id, full_name="X", created_at=datetime.utcnow())
        db.add_all([p, t]); db.commit(); db.refresh(p); db.refresh(t)

        l1 = Lease(
            org_id=org.id,
            property_id=p.id,
            tenant_id=t.id,
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 12, 31),
            total_rent=1200.0,
            created_at=datetime.utcnow(),
        )
        db.add(l1); db.commit(); db.refresh(l1)

        with pytest.raises(Exception):
            ensure_no_lease_overlap(
                db,
                org_id=org.id,
                property_id=p.id,
                start_date=datetime(2026, 6, 1),
                end_date=datetime(2026, 6, 30),
            )
    finally:
        db.close()