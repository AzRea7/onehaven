from __future__ import annotations

import logging
import time
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
log = logging.getLogger("onehaven.dashboard")


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
    limit: int = Query(default=50, ge=1, le=250),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    t0 = time.perf_counter()
    result = build_all_pane_summaries(
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
    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    log.info(
        "dashboard_panes_complete",
        extra={
            "org_id": p.org_id,
            "state": state,
            "county": county,
            "city": city,
            "jurisdiction": jurisdiction,
            "status": status,
            "assigned_user_id": assigned_user_id,
            "q": q,
            "limit": limit,
            "total_ms": total_ms,
        },
    )
    return result


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
    limit: int = Query(default=50, ge=1, le=250),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    pane_name = clamp_pane(pane)
    t0 = time.perf_counter()

    result = build_pane_dashboard(
        db,
        org_id=p.org_id,
        pane=pane_name,
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

    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    log.info(
        "dashboard_single_pane_complete",
        extra={
            "org_id": p.org_id,
            "pane": pane_name,
            "state": state,
            "county": county,
            "city": city,
            "jurisdiction": jurisdiction,
            "status": status,
            "assigned_user_id": assigned_user_id,
            "q": q,
            "limit": limit,
            "total_ms": total_ms,
        },
    )
    return result


@router.get("/properties", response_model=list[dict])
def dashboard_properties(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    strategy: Optional[str] = Query(default=None, description="section8|market"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Cheap dashboard-facing property list.

    Key changes:
    - lower default/max limit
    - no live state recompute while building rows
    - detailed timing + failure logs
    """
    req_t0 = time.perf_counter()

    stmt = select(Property).where(Property.org_id == p.org_id).order_by(desc(Property.id))
    if city:
        stmt = stmt.where(Property.city == city, Property.state == state)
    else:
        stmt = stmt.where(Property.state == state)

    query_t0 = time.perf_counter()
    props = list(db.scalars(stmt.limit(limit)).all())
    query_ms = round((time.perf_counter() - query_t0) * 1000, 2)

    out: list[dict] = []
    skipped_strategy = 0
    skipped_errors = 0

    build_t0 = time.perf_counter()
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
                    skipped_strategy += 1
                    continue

            out.append(
                _build_property_list_item(
                    db,
                    org_id=p.org_id,
                    prop=prop,
                    recompute_state=False,
                )
            )
        except Exception:
            skipped_errors += 1
            log.exception(
                "dashboard_property_row_failed",
                extra={"org_id": p.org_id, "property_id": int(getattr(prop, "id", 0) or 0)},
            )
            continue

    build_ms = round((time.perf_counter() - build_t0) * 1000, 2)
    total_ms = round((time.perf_counter() - req_t0) * 1000, 2)

    log.info(
        "dashboard_properties_complete",
        extra={
            "org_id": p.org_id,
            "city": city,
            "state": state,
            "strategy": strategy,
            "limit": limit,
            "query_rows": len(props),
            "returned_rows": len(out),
            "skipped_strategy": skipped_strategy,
            "skipped_errors": skipped_errors,
            "query_ms": query_ms,
            "build_ms": build_ms,
            "total_ms": total_ms,
        },
    )

    return out


@router.get("/portfolio_rollup", response_model=dict)
def portfolio_rollup(
    state: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    city: Optional[str] = Query(default=None),
    decision: Optional[str] = Query(default=None),
    stage: Optional[str] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    t0 = time.perf_counter()
    result = build_portfolio_rollup_with_panes(
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
    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    log.info(
        "dashboard_portfolio_rollup_complete",
        extra={
            "org_id": p.org_id,
            "state": state,
            "county": county,
            "city": city,
            "decision": decision,
            "stage": stage,
            "limit": limit,
            "total_ms": total_ms,
        },
    )
    return result


@router.get("/next_actions", response_model=dict)
def next_actions(
    state: str = Query(default="MI"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Keep this route functional, but do not force recompute for every property.
    Read persisted state and surface next actions from existing state payload.
    """
    req_t0 = time.perf_counter()

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
    skipped_errors = 0

    for pid in prop_ids:
        try:
            st = get_state_payload(db, org_id=p.org_id, property_id=pid, recompute=False)
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
            skipped_errors += 1
            log.exception(
                "dashboard_next_actions_row_failed",
                extra={"org_id": p.org_id, "property_id": pid},
            )
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

    total_ms = round((time.perf_counter() - req_t0) * 1000, 2)

    log.info(
        "dashboard_next_actions_complete",
        extra={
            "org_id": p.org_id,
            "state": state,
            "limit": limit,
            "property_scan_count": len(prop_ids),
            "returned_rows": len(rows),
            "skipped_errors": skipped_errors,
            "total_ms": total_ms,
        },
    )

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
    limit: int = Query(default=250, ge=1, le=1000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    t0 = time.perf_counter()
    result = compute_rollups(
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
    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    log.info(
        "dashboard_rollups_complete",
        extra={
            "org_id": p.org_id,
            "state": state,
            "county": county,
            "city": city,
            "decision": decision,
            "stage": stage,
            "pane": pane,
            "status": status,
            "assigned_user_id": assigned_user_id,
            "limit": limit,
            "total_ms": total_ms,
        },
    )
    return result
