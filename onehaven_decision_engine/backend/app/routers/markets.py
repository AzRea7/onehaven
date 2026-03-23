from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from ..auth import get_principal, require_operator
from ..services.market_catalog_service import (
    find_market_by_city,
    list_active_supported_markets,
)
from ..tasks.market_sync_tasks import sync_supported_city_task

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


@router.post("/sync-city", response_model=dict)
def sync_supported_city_route(
    city: str = Query(...),
    state: str = Query("MI"),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    task = sync_supported_city_task.delay(int(p.org_id), city, state)
    return {
        "ok": True,
        "queued": True,
        "city": city,
        "state": state,
        "task_id": str(task.id),
    }