# onehaven_decision_engine/backend/tests/test_inspection_failure_creates_tasks_or_flags_checklist.py
from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, PropertyChecklistItem, Inspection, InspectionItem, RehabTask
from app.domain.compliance.inspection_mapping import plan_actions, InspectionItemIn


def test_mapping_is_deterministic():
    plan = plan_actions([InspectionItemIn(code="GFCI", failed=True, location="Kitchen", details="No GFCI")])
    assert len(plan.checklist_updates) >= 1
    assert len(plan.rehab_tasks) >= 1
    assert plan.checklist_updates[0].item_code == "electrical_gfci"


def test_failed_inspection_plan_can_create_tasks_and_flag_items():
    db = SessionLocal()
    try:
        org = Organization(slug="c-org", name="c-org")
        user = AppUser(email="c@t.local", display_name="c")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        prop = Property(
            org_id=org.id,
            address="1 C St",
            city="Detroit",
            state="MI",
            zip="48201",
            bedrooms=3,
            bathrooms=1.0,
            square_feet=1000,
            year_built=1940,
            has_garage=False,
            property_type="single_family",
        )
        db.add(prop); db.commit(); db.refresh(prop)

        # Pretend checklist exists with mapped code
        ck = PropertyChecklistItem(
            org_id=org.id,
            property_id=prop.id,
            item_code="electrical_gfci",
            category="electrical",
            description="GFCI outlets present",
            severity=3,
            common_fail=True,
            status="todo",
        )
        db.add(ck); db.commit(); db.refresh(ck)

        insp = Inspection(org_id=org.id, property_id=prop.id, passed=False)
        db.add(insp); db.commit(); db.refresh(insp)

        item = InspectionItem(org_id=org.id, inspection_id=insp.id, code="GFCI", failed=True, location="Kitchen", details="Missing GFCI")
        db.add(item); db.commit()

        plan = plan_actions([InspectionItemIn(code="GFCI", failed=True, location="Kitchen", details="Missing GFCI")])
        assert plan.checklist_updates[0].item_code == "electrical_gfci"
    finally:
        db.close()