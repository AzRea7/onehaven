from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Organization, AppUser, OrgMembership, AgentRun
from app.services.agent_engine import create_run, execute_run_now
from app.domain.agents.contracts import CONTRACTS, AgentContract


def test_agent_contract_rejects_illegal_mutation():
    """
    Proof: if an agent output contains actions but contract is recommend_only, the run fails.
    """
    db = SessionLocal()
    try:
        org = Organization(slug="a-org", name="a-org")
        user = AppUser(email="a@t.local", display_name="a")
        db.add(org); db.add(user); db.commit()
        db.refresh(org); db.refresh(user)
        db.add(OrgMembership(org_id=org.id, user_id=user.id, role="owner"))
        db.commit()

        # Ensure this agent is recommend_only
        CONTRACTS["deal_intake"] = AgentContract(
            agent_key="deal_intake",
            mode="recommend_only",
            allowed_entity_types=[],
            allowed_operations=[],
            required_fields={},
        )

        # Create run with input that will cause deterministic agent to succeed
        r = create_run(
            db,
            org_id=org.id,
            actor_user_id=user.id,
            agent_key="deal_intake",
            property_id=None,
            input_payload={"address": "1 Main", "city": "Detroit", "zip": "48201", "bedrooms": 3, "bathrooms": 1.0, "asking_price": 100000},
            idempotency_key="idem-test-1",
        )

        # Hack: inject an illegal output payload (simulate a compromised agent / LLM)
        r.output_json = '{"summary":"x","actions":[{"entity_type":"rehab_task","op":"create","data":{"title":"bad"}}]}'
        db.add(r); db.commit()

        out = execute_run_now(db, org_id=org.id, run_id=r.id, attempt_number=1)
        assert out["status"] == "failed"

        rr = db.scalar(select(AgentRun).where(AgentRun.id == r.id))
        assert rr is not None
        assert rr.status == "failed"
        assert "Contract validation failed" in (rr.last_error or "")
    finally:
        db.close()