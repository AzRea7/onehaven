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
from .properties import _build_property_list_item

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
    q = select(Property).where(Property.org_id == p.org_id).order_by(desc(Property.id))

    if city:
        q = q.where(Property.city == city, Property.state == state)
    else:
        q = q.where(Property.state == state)

    props = list(db.scalars(q.limit(limit)).all())

    out: list[dict] = []
    for prop in props:
        try:
            if strategy:
                d = db.scalar(
                    select(Deal)
                    .where(Deal.org_id == p.org_id, Deal.property_id == prop.id)
                    .order_by(desc(Deal.id))
                    .limit(1)
                )
                if not d or (d.strategy or "").strip().lower() != strategy.strip().lower():
                    continue

            out.append(_build_property_list_item(db, org_id=p.org_id, prop=prop))
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
    data = compute_rollups(db, org_id=p.org_id, state=state, limit=limit)

    has_next_action = 0
    for row in data.get("rows", []):
        try:
            payload = get_state_payload(db, org_id=p.org_id, property_id=int(row["property_id"]), recompute=True)
            if payload.get("next_actions"):
                has_next_action += 1
        except Exception:
            continue

    return {
        "properties": data.get("summary", {}).get("property_count", 0),
        "stage_counts": data.get("buckets", {}).get("stages", {}),
        "decision_counts": data.get("buckets", {}).get("decisions", {}),
        "properties_with_next_actions": has_next_action,
        "averages": {
            "asking_price": data.get("summary", {}).get("avg_asking_price", 0.0),
            "projected_monthly_cashflow": data.get("summary", {}).get("avg_projected_monthly_cashflow", 0.0),
            "dscr": data.get("summary", {}).get("avg_dscr", 0.0),
        },
    }


@router.get("/next_actions", response_model=dict)
def next_actions(
    state: str = Query(default="MI"),
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop_ids = [
        row[0]
        for row in db.execute(
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

            for action in actions[:3]:
                rows.append(
                    {
                        "property_id": pid,
                        "address": prop.address,
                        "city": prop.city,
                        "stage": st.get("current_stage"),
                        "decision": st.get("normalized_decision"),
                        "action": action,
                    }
                )
        except Exception:
            continue

    order = {"deal": 0, "rehab": 1, "compliance": 2, "tenant": 3, "cash": 4, "equity": 5}
    rows = sorted(
        rows,
        key=lambda row: (
            order.get(str(row.get("stage") or ""), 99),
            str(row.get("city") or ""),
            str(row.get("address") or ""),
        ),
    )[:limit]

    return {"rows": rows, "count": len(rows)}


@router.get("/rollups", response_model=dict)
def dashboard_rollups(
    state: str = Query(default="MI"),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return compute_rollups(
        db,
        org_id=p.org_id,
        state=state,
        county=county,
        city=city,
        decision=decision,
        stage=stage,
        limit=limit,
    )