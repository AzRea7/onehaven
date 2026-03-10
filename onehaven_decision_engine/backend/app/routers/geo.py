from __future__ import annotations

import os

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import get_principal, require_operator
from app.db import get_db
from app.models import Property
from app.services.geo_enrichment import enrich_property_geo, is_in_redzone

router = APIRouter(prefix="/geo", tags=["geo"])


@router.post("/enrich", response_model=dict)
async def enrich(
    property_id: int = Query(..., ge=1),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    return await enrich_property_geo(
        db,
        org_id=p.org_id,
        property_id=int(property_id),
        google_api_key=key,
        force=bool(force),
    )


@router.post("/enrich_missing", response_model=dict)
async def enrich_missing(
    state: str = Query(default="MI"),
    limit: int = Query(default=50, ge=1, le=500),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    key = os.getenv("GOOGLE_MAPS_API_KEY")

    rows = db.scalars(
        select(Property.id)
        .where(
            Property.org_id == p.org_id,
            Property.state == state,
        )
        .order_by(Property.id.desc())
        .limit(2000)
    ).all()

    scanned = 0
    enriched = 0
    results: list[dict] = []

    for pid in rows:
        scanned += 1
        if enriched >= limit:
            break

        prop = db.scalar(
            select(Property).where(
                Property.org_id == p.org_id,
                Property.id == int(pid),
            )
        )
        if not prop:
            continue

        needs_geo = (
            force
            or getattr(prop, "lat", None) is None
            or getattr(prop, "lng", None) is None
            or not getattr(prop, "county", None)
        )

        if not needs_geo:
            continue

        out = await enrich_property_geo(
            db,
            org_id=p.org_id,
            property_id=int(pid),
            google_api_key=key,
            force=bool(force),
        )
        results.append(out)
        enriched += 1

    return {
        "ok": True,
        "state": state,
        "scanned": scanned,
        "enriched": enriched,
        "results": results,
    }


@router.get("/redzone_check", response_model=dict)
def redzone_check(
    lat: float = Query(...),
    lng: float = Query(...),
    _p=Depends(get_principal),
):
    return {
        "ok": True,
        "lat": lat,
        "lng": lng,
        "is_red_zone": bool(is_in_redzone(lat=float(lat), lng=float(lng))),
    }