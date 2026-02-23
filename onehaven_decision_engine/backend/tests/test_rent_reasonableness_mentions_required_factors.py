# backend/tests/test_rent_reasonableness_mentions_required_factors.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.domain.agents.registry import agent_rent_reasonableness, AgentContext


def test_rent_reasonableness_mentions_required_factors(db_session: Session):
    org_id = 1
    property_id = 1

    out = agent_rent_reasonableness(db_session, AgentContext(org_id=org_id, property_id=property_id, run_id=1))
    payload = out["actions"][0]["data"]["payload"]

    factors = payload.get("comparability_factors_considered") or []
    must = {"location", "quality/condition", "unit size", "unit type", "age of unit", "amenities", "services/maintenance", "utilities included"}
    assert must.issubset(set(factors))