# backend/app/routers/dashboard.py
from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Property, Deal
from .properties import property_view  # reuse your existing “single source of truth” builder

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/properties", response_model=list[dict])
def dashboard_properties(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    strategy: Optional[str] = Query(default=None, description="section8|market"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Returns a list of PropertyView objects (as dicts) for UI dashboards.

    Why dicts instead of a strict schema?
    - PropertyViewOut is already defined and returned by /properties/{id}/view
    - This endpoint simply aggregates that “truth” across many properties.
    """
    q = select(Property.id).order_by(desc(Property.id))

    if city:
        q = q.where(Property.city == city, Property.state == state)
    else:
        q = q.where(Property.state == state)

    prop_ids = [row[0] for row in db.execute(q.limit(limit)).all()]

    out: list[dict] = []
    for pid in prop_ids:
        try:
            view = property_view(property_id=pid, db=db)  # returns PropertyViewOut
            v = view.model_dump() if hasattr(view, "model_dump") else dict(view)
            if strategy:
                # Filter by latest deal strategy if requested
                d = db.scalar(select(Deal).where(Deal.property_id == pid).order_by(desc(Deal.id)).limit(1))
                if not d or (d.strategy or "").strip().lower() != strategy.strip().lower():
                    continue
            out.append(v)
        except Exception:
            # Dashboard should be resilient; skip broken rows rather than fail whole page
            continue

    return out
