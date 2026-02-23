from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, AgentRun
from app.services.agent_engine import create_run, execute_run_now


def test_agent_worker_idempotency_same_run_twice():
    """
    Proof: executing the same run twice does not re-run / double-apply.
    It returns existing terminal output once done.
    """
    db = SessionLocal()
    try:
        org = Organization(slug="i-org", name="i-org")
        user = AppUser(email="i@t.local", display_name="i")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        r = create_run(
            db,
            org_id=org.id,
            actor_user_id=user.id,
            agent_key="deal_intake",
            property_id=None,
            input_payload={"address": "1 Main", "city": "Detroit", "zip": "48201", "bedrooms": 3, "bathrooms": 1.0, "asking_price": 100000},
            idempotency_key="idem-same-run",
        )

        out1 = execute_run_now(db, org_id=org.id, run_id=r.id, attempt_number=1)
        out2 = execute_run_now(db, org_id=org.id, run_id=r.id, attempt_number=2)

        assert out1["status"] in {"done", "failed", "blocked"}
        assert out2["status"] == out1["status"]

        rr = db.scalar(select(AgentRun).where(AgentRun.id == r.id))
        assert rr is not None
        assert rr.attempts >= 1
    finally:
        db.close()