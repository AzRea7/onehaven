# onehaven_decision_engine/backend/tests/test_agent_apply_actions_idempotent.py
from __future__ import annotations

import json
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, Property, AgentRun, RehabTask
from app.services.agent_engine import create_run, mark_approved
from app.services.agent_actions import apply_run_actions


def test_apply_actions_is_idempotent():
    db = SessionLocal()
    try:
        org = Organization(slug="idem-org", name="idem-org")
        user = AppUser(email="idem@t.local", display_name="idem")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        prop = Property(org_id=org.id, address="1 Main", city="Detroit", state="MI", zip="48201")
        db.add(prop); db.commit()
        db.refresh(prop)

        # Create a blocked run that requires approval and has proposed rehab task action
        r = create_run(
            db,
            org_id=org.id,
            actor_user_id=user.id,
            agent_key="hqs_precheck",
            property_id=prop.id,
            input_payload={"property_id": prop.id},
            idempotency_key="apply-idem-1",
        )

        # Simulate a finished agent output with actions
        output = {
            "summary": "precheck",
            "actions": [
                {
                    "entity_type": "rehab_task",
                    "op": "create",
                    "data": {"title": "Install GFCI", "category": "electrical", "status": "todo", "inspection_relevant": True},
                }
            ],
        }
        r.output_json = json.dumps(output)
        r.proposed_actions_json = json.dumps(output["actions"])
        r.status = "blocked"
        r.approval_status = "pending"
        db.add(r); db.commit()
        db.refresh(r)

        # Approve
        mark_approved(db, org_id=org.id, actor_user_id=user.id, run_id=r.id)

        # Apply #1
        res1 = apply_run_actions(db, org_id=org.id, actor_user_id=user.id, run_id=r.id)
        assert res1.ok is True
        assert res1.applied_count == 1

        tasks1 = db.scalars(select(RehabTask).where(RehabTask.org_id == org.id, RehabTask.property_id == prop.id)).all()
        assert len(tasks1) == 1

        # Apply #2 should do nothing (idempotent)
        res2 = apply_run_actions(db, org_id=org.id, actor_user_id=user.id, run_id=r.id)
        assert res2.ok is True
        assert res2.applied_count == 0

        tasks2 = db.scalars(select(RehabTask).where(RehabTask.org_id == org.id, RehabTask.property_id == prop.id)).all()
        assert len(tasks2) == 1
    finally:
        db.close()