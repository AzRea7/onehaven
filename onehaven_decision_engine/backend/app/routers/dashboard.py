# backend/app/routers/dashboard.py
from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Deal, Property
from ..services.dashboard_rollups import compute_rollups
from ..services.property_state_machine import get_state_payload
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
    q = select(Property.id).where(Property.org_id == p.org_id).order_by(desc(Property.id))

    if city:
        q = q.where(Property.city == city, Property.state == state)
    else:
        q = q.where(Property.state == state)

    prop_ids = [row[0] for row in db.execute(q.limit(limit)).all()]

    out: list[dict] = []
    for pid in prop_ids:
        try:
            if strategy:
                d = db.scalar(
                    select(Deal)
                    .where(Deal.org_id == p.org_id, Deal.property_id == pid)
                    .order_by(desc(Deal.id))
                    .limit(1)
                )
                if not d or (d.strategy or "").strip().lower() != strategy.strip().lower():
                    continue

            view = property_view(property_id=pid, db=db, p=p)
            v = view.model_dump() if hasattr(view, "model_dump") else dict(view)
            out.append(v)
        except Exception:
            continue

    return out


@router.get("/portfolio_rollup", response_model=dict)
def portfolio_rollup(
    state: str = Query(default="MI"),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop_ids = [
        r[0]
        for r in db.execute(
            select(Property.id)
            .where(Property.org_id == p.org_id, Property.state == state)
            .order_by(desc(Property.id))
            .limit(limit)
        ).all()
    ]

    stage_counts: dict[str, int] = {}
    has_next_action = 0

    for pid in prop_ids:
        try:
            st = get_state_payload(db, org_id=p.org_id, property_id=pid, recompute=True)
            stage = str(st.get("current_stage") or "deal")
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
            if (st.get("next_actions") or []) and len(st.get("next_actions") or []) > 0:
                has_next_action += 1
        except Exception:
            continue

    return {
        "properties": len(prop_ids),
        "stage_counts": stage_counts,
        "properties_with_next_actions": has_next_action,
    }


@router.get("/next_actions", response_model=dict)
def next_actions(
    state: str = Query(default="MI"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop_ids = [
        r[0]
        for r in db.execute(
            select(Property.id)
            .where(Property.org_id == p.org_id, Property.state == state)
            .order_by(desc(Property.id))
            .limit(1000)
        ).all()
    ]

    rows: list[dict[str, Any]] = []
    for pid in prop_ids:
        try:
            st = get_state_payload(db, org_id=p.org_id, property_id=pid, recompute=True)
            actions = st.get("next_actions") or []
            if not actions:
                continue

            prop = db.scalar(select(Property).where(Property.id == pid, Property.org_id == p.org_id))
            if not prop:
                continue

            for a in actions[:3]:
                rows.append(
                    {
                        "property_id": pid,
                        "address": prop.address,
                        "city": prop.city,
                        "stage": st.get("current_stage"),
                        "action": a,
                    }
                )
        except Exception:
            continue

    order = {"compliance": 0, "rehab": 1, "tenant": 2, "cash": 3, "equity": 4, "deal": 5}
    rows = sorted(rows, key=lambda r: (order.get(str(r.get("stage") or ""), 9), r["city"] or "", r["address"] or ""))[
        :limit
    ]
    return {"rows": rows, "count": len(rows)}


@router.get("/rollups", response_model=dict)
def dashboard_rollups(
    state: str = Query(default="MI"),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return compute_rollups(db, org_id=p.org_id, state=state, limit=limit)
