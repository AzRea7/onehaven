# backend/app/routers/geo.py
from __future__ import annotations

import os

from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_operator
from ..db import get_db
from ..models import Property
from ..services.geo_enrichment import enrich_property_geo, is_in_redzone
from ..services.risk_scoring import compute_property_risk

router = APIRouter(prefix="/geo", tags=["geo"])


class GeoEnrichBatchIn(BaseModel):
    property_ids: list[int] = Field(default_factory=list)
    force: bool = False


@router.post("/enrich", response_model=dict)
async def enrich(
    property_id: int = Query(..., ge=1),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    """
    Admin/backfill endpoint only.

    The normal ingestion path should already call geo enrichment and risk scoring
    as part of the property-first ingestion pipeline.
    """
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    return await enrich_property_geo(
        db,
        org_id=p.org_id,
        property_id=int(property_id),
        google_api_key=key,
        force=bool(force),
    )


@router.post("/enrich/batch", response_model=dict)
async def enrich_batch(
    payload: GeoEnrichBatchIn = Body(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    """
    Admin/backfill batch endpoint only.
    """
    key = os.getenv("GOOGLE_MAPS_API_KEY")

    seen: set[int] = set()
    property_ids: list[int] = []
    for pid in payload.property_ids:
        if int(pid) in seen:
            continue
        seen.add(int(pid))
        property_ids.append(int(pid))

    results: list[dict] = []
    errors: list[dict] = []

    for pid in property_ids:
        prop = db.scalar(
            select(Property).where(
                Property.org_id == p.org_id,
                Property.id == int(pid),
            )
        )
        if not prop:
            errors.append({"property_id": int(pid), "error": "property_not_found"})
            continue

        try:
            out = await enrich_property_geo(
                db,
                org_id=p.org_id,
                property_id=int(pid),
                google_api_key=key,
                force=bool(payload.force),
            )
            results.append(out)
        except Exception as e:
            errors.append({"property_id": int(pid), "error": f"{type(e).__name__}: {e}"})

    return {
        "ok": True,
        "attempted": len(property_ids),
        "enriched": len(results),
        "results": results,
        "errors": errors,
    }


@router.post("/enrich_missing", response_model=dict)
async def enrich_missing(
    state: str = Query(default="MI"),
    limit: int = Query(default=50, ge=1, le=500),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    """
    Admin/backfill endpoint only.
    """
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
            or getattr(prop, "crime_score", None) is None
            or getattr(prop, "offender_count", None) is None
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


@router.get("/risk_check", response_model=dict)
def risk_check(
    lat: float = Query(...),
    lng: float = Query(...),
    city: str | None = Query(default=None),
    county: str | None = Query(default=None),
    is_red_zone: bool | None = Query(default=None),
    _p=Depends(get_principal),
):
    red_zone = bool(is_red_zone) if is_red_zone is not None else bool(
        is_in_redzone(lat=float(lat), lng=float(lng))
    )

    risk = compute_property_risk(
        lat=float(lat),
        lng=float(lng),
        city=city,
        county=county,
        is_red_zone=red_zone,
    )

    return {
        "ok": True,
        "lat": float(lat),
        "lng": float(lng),
        "city": city,
        "county": county,
        "is_red_zone": red_zone,
        **risk,
    }
