# backend/tests/test_phase4_ops_enforcement_next_actions.py
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy.orm import Session

from app.services.property_state_machine import get_state_payload
from app.models import Lease, Transaction, Valuation


@pytest.mark.usefixtures("db_session")
def test_next_actions_include_valuation_due_and_rent_reconciliation_gap(db_session: Session):
    """
    Phase 4 DoD:
    - stale/missing valuation => valuation_due next action
    - rent collected < expected proxy => rent_reconciliation_gap next action
    """
    org_id = 1
    property_id = 1

    # Ensure expected rent exists
    db_session.add(
        Lease(
            org_id=org_id,
            property_id=property_id,
            tenant_id=1,
            start_date=date.today() - timedelta(days=40),
            end_date=None,
            total_rent=1500.0,
            created_at=None,
        )
    )
    db_session.commit()

    # Collected is less than expected
    db_session.add(
        Transaction(
            org_id=org_id,
            property_id=property_id,
            txn_date=date.today() - timedelta(days=10),
            txn_type="rent",
            amount=500.0,
            memo="partial rent",
            created_at=None,
        )
    )

    # Stale valuation (older than 180 days)
    db_session.add(
        Valuation(
            org_id=org_id,
            property_id=property_id,
            as_of=date.today() - timedelta(days=365),
            value=200000.0,
            notes="old valuation",
            created_at=None,
        )
    )
    db_session.commit()

    st = get_state_payload(db_session, org_id=org_id, property_id=property_id, recompute=True)
    acts = st.get("next_actions") or []

    types = {a.get("type") for a in acts if isinstance(a, dict)}
    assert "rent_reconciliation_gap" in types
    assert "valuation_due" in types