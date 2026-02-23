from __future__ import annotations

import json

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, PropertyState
from app.services.agent_orchestrator import plan_agent_runs


def test_agent_orchestrator_routes_by_stage():
    db = SessionLocal()
    try:
        org = Organization(slug="o-org", name="o-org")
        user = AppUser(email="o@t.local", display_name="o")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        p = Property(
            org_id=org.id,
            address="123 Test",
            city="Detroit",
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

        ps = PropertyState(
            org_id=org.id,
            property_id=p.id,
            current_stage="compliance",
            constraints_json=json.dumps({}),
            outstanding_tasks_json=json.dumps([{"code": "valuation_due"}]),
        )
        db.add(ps); db.commit()

        plan = plan_agent_runs(db, org_id=org.id, property_id=p.id)
        keys = [x.agent_key for x in plan]
        assert "hqs_precheck" in keys
    finally:
        db.close()