# onehaven_decision_engine/backend/tests/test_jurisdiction_rule_audit_trail.py
from __future__ import annotations

import json
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, AuditEvent
from app.services.jurisdiction_rules_service import create_rule, update_rule, delete_rule


def test_jurisdiction_rule_audit_trail():
    db = SessionLocal()
    try:
        org = Organization(slug="audit-org", name="audit-org")
        user = AppUser(email="audit@t.local", display_name="audit")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        jr = create_rule(
            db,
            org_id=org.id,
            actor_user_id=user.id,
            payload={
                "city": "Detroit",
                "state": "MI",
                "rental_license_required": True,
                "inspection_frequency": "annual",
                "processing_days": 30,
                "typical_fail_points_json": json.dumps(["GFCI", "Handrails"]),
            },
        )

        jr = update_rule(
            db,
            org_id=org.id,
            actor_user_id=user.id,
            rule_id=jr.id,
            payload={"processing_days": 60},
        )

        delete_rule(db, org_id=org.id, actor_user_id=user.id, rule_id=jr.id)

        evts = db.execute(
            select(AuditEvent)
            .where(AuditEvent.org_id == org.id)
            .where(AuditEvent.entity_type == "jurisdiction_rule")
            .order_by(AuditEvent.id.asc())
        ).scalars().all()

        assert len(evts) == 3
        assert evts[0].action == "jurisdiction_rule_created"
        assert evts[1].action == "jurisdiction_rule_updated"
        assert evts[2].action == "jurisdiction_rule_deleted"

        assert evts[0].before_json is None
        assert evts[0].after_json is not None
        assert evts[2].before_json is not None
        assert evts[2].after_json is None

    finally:
        db.close()