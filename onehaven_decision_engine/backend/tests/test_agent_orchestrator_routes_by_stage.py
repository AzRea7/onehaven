# backend/tests/test_agent_orchestrator_routes_by_stage.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.services.agent_orchestrator import plan_agent_runs


class Dummy:
    pass


def test_orchestrator_plans_deal_stage(db_session: Session):
    # This test assumes you already have fixtures creating org/property/state in your suite.
    # If not, adapt to your existing test patterns.
    org_id = 1
    property_id = 1

    planned = plan_agent_runs(db_session, org_id=org_id, property_id=property_id)
    keys = {p.agent_key for p in planned}

    # Deal stage should include intake + records + packet
    assert "deal_intake" in keys
    assert "public_records_check" in keys
    assert "packet_builder" in keys