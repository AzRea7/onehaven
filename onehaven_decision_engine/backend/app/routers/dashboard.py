from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..domain.workflow.panes import clamp_pane, pane_catalog
from ..services.dashboard_rollups import compute_rollups
from ..services.ownership import ensure_pane_access
from ..services.pane_dashboard_service import (
    build_all_pane_summaries,
    build_pane_dashboard,
    build_portfolio_rollup_with_panes,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
log = logging.getLogger("onehaven.dashboard")


STANDARD_FILTERS = {
    "org": True,
    "city": True,
    "county": True,
    "assigned_user": True,
    "status": True,
    "stage": True,
    "urgency": True,
    "deals_only": True,
    "include_suppressed": True,
    "include_hidden": True,
    "q": True,
    "state": True,
}


@router.get("/catalog", response_model=dict)
def dashboard_catalog():
    return {
        "panes": pane_catalog(),
        "filters": STANDARD_FILTERS,
        "sections": [
            "kpis",
            "blockers",
            "recent_actions",
            "next_actions",
            "stale_items",
            "queue_counts",
            "rows",
        ],
    }


@router.get("/panes", response_model=dict)
def pane_dashboard_overview(
    city: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    assigned_user: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    urgency: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=250),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    include_hidden: bool = Query(default=False),
    deals_only: bool = Query(default=False),
    include_suppressed: bool = Query(default=False),
):
    t0 = time.perf_counter()

    result = build_all_pane_summaries(
        db,
        org_id=p.org_id,
        principal=p,
        state=state,
        county=county,
        city=city,
        status=status,
        stage=stage,
        urgency=urgency,
        assigned_user=assigned_user,
        q=q,
        limit=limit,
        include_hidden=include_hidden,
        deals_only=deals_only,
        include_suppressed=include_suppressed,
    )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "dashboard_panes_complete",
        extra={
            "org_id": p.org_id,
            "city": city,
            "county": county,
            "assigned_user": assigned_user,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "limit": limit,
            "include_hidden": include_hidden,
            "deals_only": deals_only,
            "include_suppressed": include_suppressed,
            "total_ms": total_ms,
        },
    )
    return result


@router.get("/panes/{pane}", response_model=dict)
def pane_dashboard(
    pane: str,
    city: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    assigned_user: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    urgency: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=250),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    include_hidden: bool = Query(default=False),
    deals_only: bool = Query(default=False),
    include_suppressed: bool = Query(default=False),
):
    pane_name = ensure_pane_access(principal=p, pane=clamp_pane(pane))
    t0 = time.perf_counter()

    result = build_pane_dashboard(
        db,
        org_id=p.org_id,
        pane=pane_name,
        principal=p,
        state=state,
        county=county,
        city=city,
        status=status,
        stage=stage,
        urgency=urgency,
        assigned_user=assigned_user,
        q=q,
        limit=limit,
        include_hidden=include_hidden,
        deals_only=deals_only,
        include_suppressed=include_suppressed,
    )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "dashboard_single_pane_complete",
        extra={
            "org_id": p.org_id,
            "pane": pane_name,
            "city": city,
            "county": county,
            "assigned_user": assigned_user,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "limit": limit,
            "include_hidden": include_hidden,
            "deals_only": deals_only,
            "include_suppressed": include_suppressed,
            "total_ms": total_ms,
        },
    )
    return result


@router.get("/portfolio_rollup", response_model=dict)
def portfolio_rollup(
    city: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    assigned_user: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    urgency: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    include_hidden: bool = Query(default=False),
):
    t0 = time.perf_counter()

    result = build_portfolio_rollup_with_panes(
        db,
        org_id=p.org_id,
        principal=p,
        state=state,
        county=county,
        city=city,
        status=status,
        stage=stage,
        urgency=urgency,
        assigned_user=assigned_user,
        limit=limit,
        include_hidden=include_hidden,
    )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "dashboard_portfolio_rollup_complete",
        extra={
            "org_id": p.org_id,
            "city": city,
            "county": county,
            "assigned_user": assigned_user,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "limit": limit,
            "include_hidden": include_hidden,
            "total_ms": total_ms,
        },
    )
    return result


@router.get("/rollups", response_model=dict)
def dashboard_rollups(
    city: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    assigned_user: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    urgency: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    pane: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    include_hidden: bool = Query(default=False),
    deals_only: bool = Query(default=False),
    include_suppressed: bool = Query(default=False),
):
    t0 = time.perf_counter()

    pane_value = clamp_pane(pane) if pane else None
    if pane_value:
        ensure_pane_access(principal=p, pane=pane_value)

    result = compute_rollups(
        db,
        org_id=p.org_id,
        state=state,
        county=county,
        city=city,
        q=q,
        stage=stage,
        decision=decision,
        pane=pane_value,
        status=status,
        assigned_user=assigned_user,
        urgency=urgency,
        limit=limit,
        include_hidden=include_hidden,
        deals_only=deals_only,
        include_suppressed=include_suppressed,
    )

    total_ms = round((time.perf_counter() - t0) * 1000, 2)
    log.info(
        "dashboard_rollups_complete",
        extra={
            "org_id": p.org_id,
            "city": city,
            "county": county,
            "assigned_user": assigned_user,
            "status": status,
            "stage": stage,
            "urgency": urgency,
            "pane": pane_value,
            "limit": limit,
            "include_hidden": include_hidden,
            "deals_only": deals_only,
            "include_suppressed": include_suppressed,
            "total_ms": total_ms,
        },
    )
    return result