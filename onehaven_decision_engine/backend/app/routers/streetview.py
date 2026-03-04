# onehaven_decision_engine/backend/app/routers/streetview.py
from __future__ import annotations

import os
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.auth import get_principal
from app.db import get_db
from app.models import Property

router = APIRouter(prefix="/streetview", tags=["streetview"])


@router.get("/img")
def streetview_img(
    property_id: int = Query(..., ge=1),
    w: int = Query(default=900, ge=200, le=2000),
    h: int = Query(default=520, ge=200, le=2000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not key:
        # graceful placeholder: redirect to a tiny 1x1 transparent gif data URL (or serve local static later)
        raise HTTPException(status_code=503, detail="GOOGLE_MAPS_API_KEY not set")

    prop = db.scalar(select(Property).where(Property.org_id == p.org_id, Property.id == int(property_id)))
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    lat = getattr(prop, "lat", None)
    lng = getattr(prop, "lng", None)
    if lat is None or lng is None:
        # Still return something stable: use address if coords missing
        loc = f"{prop.address}, {prop.city}, {prop.state} {prop.zip}"
        params = {
            "size": f"{w}x{h}",
            "location": loc,
            "key": key,
        }
    else:
        params = {
            "size": f"{w}x{h}",
            "location": f"{float(lat)},{float(lng)}",
            "key": key,
        }

    url = "https://maps.googleapis.com/maps/api/streetview?" + urlencode(params)
    return RedirectResponse(url=url, status_code=302)