from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.workflow.panes import clamp_pane, pane_catalog
from ..models import Deal, Property
from ..services.dashboard_rollups import compute_rollups
from ..services.pane_dashboard_service import (
    build_all_pane_summaries,
    build_pane_dashboard,
    build_portfolio_rollup_with_panes,
)
from ..services.property_state_machine import get_state_payload
from .properties import _build_property_list_item

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/catalog", response_model=dict)
def dashboard_catalog():
    return {
        "panes": pane_catalog(),
        "filters": {
            "status": True,
            "jurisdiction": True,
            "city": True,
            "county": True,
            "assigned_user_id": True,
            "state": True,
            "q": True,
        },
    }


@router.get("/panes", response_model=dict)
def pane_dashboard_overview(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    jurisdiction: Optional[str] = Query(default=None, description="missing|stale|incomplete|complete"),
    status: Optional[str] = Query(default=None),
    assigned_user_id: Optional[int] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return build_all_pane_summaries(
        db,
        org_id=p.org_id,
        principal=p,
        state=state,
        county=county,
        city=city,
        jurisdiction=jurisdiction,
        status=status,
        assigned_user_id=assigned_user_id,
        q=q,
        limit=limit,
    )


@router.get("/panes/{pane}", response_model=dict)
def pane_dashboard(
    pane: str,
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    jurisdiction: Optional[str] = Query(default=None, description="missing|stale|incomplete|complete"),
    status: Optional[str] = Query(default=None),
    assigned_user_id: Optional[int] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return build_pane_dashboard(
        db,
        org_id=p.org_id,
        pane=clamp_pane(pane),
        principal=p,
        state=state,
        county=county,
        city=city,
        jurisdiction=jurisdiction,
        status=status,
        assigned_user_id=assigned_user_id,
        q=q,
        limit=limit,
    )


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
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return build_portfolio_rollup_with_panes(
        db,
        org_id=p.org_id,
        principal=p,
        state=state,
        county=county,
        city=city,
        decision=decision,
        stage=stage,
        limit=limit,
    )


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
                        "pane": st.get("current_pane"),
                        "decision": st.get("normalized_decision"),
                        "action": action,
                    }
                )
        except Exception:
            continue

    order = {
        "investor": 0,
        "acquisition": 1,
        "compliance": 2,
        "tenants": 3,
        "management": 4,
        "admin": 5,
    }
    rows = sorted(
        rows,
        key=lambda row: (
            order.get(str(row.get("pane") or ""), 99),
            str(row.get("city") or ""),
            str(row.get("address") or ""),
        ),
    )[:limit]

    return {"rows": rows, "count": len(rows)}


@router.get("/rollups", response_model=dict)
def dashboard_rollups(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    pane: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    assigned_user_id: Optional[int] = Query(default=None),
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
        pane=pane,
        status=status,
        assigned_user_id=assigned_user_id,
        limit=limit,
    )