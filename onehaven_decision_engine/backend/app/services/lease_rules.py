# backend/app/services/lease_rules.py
from __future__ import annotations

from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Lease


def _end_or_max(dt: datetime | None) -> datetime:
    return dt or datetime(2100, 1, 1)


def ensure_no_lease_overlap(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    start_date: datetime,
    end_date: datetime | None,
    exclude_lease_id: int | None = None,
) -> None:
    """
    Blocks overlapping leases for the same property.

    Overlap condition:
      existing.start <= new.end AND new.start <= existing.end
    """
    new_start = start_date
    new_end = _end_or_max(end_date)

    q = select(Lease).where(
        Lease.org_id == org_id,
        Lease.property_id == property_id,
    )

    if exclude_lease_id is not None:
        q = q.where(Lease.id != exclude_lease_id)

    rows = db.scalars(q).all()

    for r in rows:
        r_start = r.start_date
        r_end = _end_or_max(r.end_date)
        if (r_start <= new_end) and (new_start <= r_end):
            raise HTTPException(
                status_code=409,
                detail=f"Lease overlap blocked: existing lease {r.id} conflicts with requested dates.",
            )