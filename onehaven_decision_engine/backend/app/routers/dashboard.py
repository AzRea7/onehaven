# backend/app/routers/dashboard.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Property, Deal
from .properties import property_view  # reuse “single source of truth”

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/properties", response_model=list[dict])
def dashboard_properties(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    strategy: Optional[str] = Query(default=None, description="section8|market"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Returns list of PropertyView objects (dicts) for UI dashboards.

    Key invariants:
    - Tenant scoped: only properties in p.org_id
    - Uses property_view() as the single truth builder
    - Resilient: skip rows that fail
    """
    q = select(Property.id).where(Property.org_id == p.org_id).order_by(desc(Property.id))

    if city:
        q = q.where(Property.city == city, Property.state == state)
    else:
        q = q.where(Property.state == state)

    prop_ids = [row[0] for row in db.execute(q.limit(limit)).all()]

    out: list[dict] = []
    for pid in prop_ids:
        try:
            view = property_view(property_id=pid, db=db, p=p)  # ✅ pass principal
            v = view.model_dump() if hasattr(view, "model_dump") else dict(view)

            if strategy:
                d = db.scalar(
                    select(Deal)
                    .where(Deal.org_id == p.org_id, Deal.property_id == pid)
                    .order_by(desc(Deal.id))
                    .limit(1)
                )
                if not d or (d.strategy or "").strip().lower() != strategy.strip().lower():
                    continue

            out.append(v)
        except Exception:
            continue

    return out
