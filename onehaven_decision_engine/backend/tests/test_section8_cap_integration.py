# backend/tests/test_section8_cap_integration.py
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, RentAssumption


@pytest.mark.usefixtures("db_session")
def test_section8_enrich_caps_rent_used_below_approved_ceiling():
    """
    End-to-end constitution test:
      /rent/enrich/{id}?strategy=section8 => rent_used <= approved_rent_ceiling (when both present)
    """
    c = TestClient(app)

    # Create org/user/property directly (fast + deterministic)
    db = SessionLocal()
    try:
        org = Organization(slug="cap-org", name="cap-org")
        user = AppUser(email="cap@t.local", display_name="cap")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        prop = Property(
            org_id=org.id,
            address="1 Cap St",
            city="Detroit",
            state="MI",
            zip="48201",
            bedrooms=3,
            bathrooms=1.0,
            square_feet=1100,
        )
        db.add(prop); db.commit(); db.refresh(prop)

        # Seed rent assumption values so enrich doesn't depend on external APIs in test
        ra = RentAssumption(
            org_id=org.id,
            property_id=prop.id,
            market_rent_estimate=1800.0,
            section8_fmr=1500.0,
            rent_reasonableness_comp=1600.0,
            approved_rent_ceiling=None,  # computed = min(1650, 1600) => 1600
        )
        db.add(ra); db.commit()

        headers = {
            "X-Org-Slug": org.slug,
            "X-User-Email": user.email,
            "X-User-Role": "owner",
        }

        r = c.post(f"/rent/enrich/{prop.id}?strategy=section8", headers=headers)
        assert r.status_code in (200, 201), r.text
        data = r.json()

        assert data.get("approved_rent_ceiling") is not None
        assert data.get("rent_used") is not None
        assert float(data["rent_used"]) <= float(data["approved_rent_ceiling"])
        assert data.get("cap_reason") in ("capped", "uncapped")
    finally:
        db.close()