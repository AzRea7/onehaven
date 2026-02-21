# onehaven_decision_engine/backend/app/services/lease_rules.py
from __future__ import annotations

from datetime import date
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from ..models import Lease


def assert_no_overlap(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    start: date,
    end: date,
    exclude_lease_id: int | None = None,
) -> None:
    """
    No overlapping leases for the same property within an org.

    Overlap condition:
      existing.start <= new.end AND existing.end >= new.start
    """
    q = (
        select(Lease)
        .where(Lease.org_id == org_id)
        .where(Lease.property_id == property_id)
        .where(Lease.start_date <= end)
        .where(Lease.end_date >= start)
    )
    if exclude_lease_id is not None:
        q = q.where(Lease.id != exclude_lease_id)

    hit = db.scalar(q)
    if hit is not None:
        raise ValueError("lease overlap detected")