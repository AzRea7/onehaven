from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import get_principal, require_operator
from ..services.market_catalog_service import (
    find_market_by_city,
    get_market,
    list_active_supported_markets,
)
from ..services.market_sync_service import build_supported_market_sync_plan
from ..tasks.ingestion_tasks import sync_source_task

router = APIRouter(prefix="/markets", tags=["markets"])


@router.get("/supported", response_model=list[dict])
def list_supported_markets_route(
    _p=Depends(get_principal),
):
    return list_active_supported_markets()


@router.get("/coverage", response_model=dict)
def get_market_coverage(
    city: str = Query(...),
    state: str = Query("MI"),
    _p=Depends(get_principal),
):
    market = find_market_by_city(city=city, state=state)
    return {
        "ok": True,
        "covered": market is not None,
        "city": city,
        "state": state,
        "market": market,
    }


@router.post("/sync-market", response_model=dict)
def sync_supported_market_route(
    market_slug: str | None = Query(default=None),
    city: str | None = Query(default=None),
    state: str = Query("MI"),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    plan = build_supported_market_sync_plan(
        org_id=int(p.org_id),
        market_slug=market_slug,
        city=city,
        state=state,
    )

    if not plan["covered"]:
        raise HTTPException(status_code=404, detail="Supported market not found")

    task_ids: list[str] = []
    for dispatch in plan["dispatches"]:
        job = sync_source_task.delay(
            int(p.org_id),
            int(dispatch["source_id"]),
            str(dispatch["trigger_type"]),
            dict(dispatch["runtime_config"]),
        )
        task_ids.append(str(job.id))

    return {
        "ok": True,
        "covered": True,
        "queued": True,
        "market": plan["market"],
        "queued_count": len(task_ids),
        "task_ids": task_ids,
        "dispatches": plan["dispatches"],
    }


@router.post("/sync-city", response_model=dict)
def sync_supported_city_route(
    city: str = Query(...),
    state: str = Query("MI"),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    market = find_market_by_city(city=city, state=state)
    if market is None:
        raise HTTPException(status_code=404, detail="Supported market not found")

    plan = build_supported_market_sync_plan(
        org_id=int(p.org_id),
        market_slug=str(market["slug"]),
    )

    task_ids: list[str] = []
    for dispatch in plan["dispatches"]:
        job = sync_source_task.delay(
            int(p.org_id),
            int(dispatch["source_id"]),
            str(dispatch["trigger_type"]),
            dict(dispatch["runtime_config"]),
        )
        task_ids.append(str(job.id))

    return {
        "ok": True,
        "queued": True,
        "city": city,
        "state": state,
        "market": market,
        "task_ids": task_ids,
        "queued_count": len(task_ids),
    }