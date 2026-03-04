# onehaven_decision_engine/backend/app/services/geo_enrichment.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Tuple

import httpx
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import Property

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
REDZONES_PATH = DATA_DIR / "red_zones_detroit.geojson"


def _load_geojson(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"type": "FeatureCollection", "features": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _point_in_polygon(lng: float, lat: float, polygon_coords: list[list[float]]) -> bool:
    """
    Ray casting algorithm.
    polygon_coords: [[lng,lat], [lng,lat], ...] (no holes for v1)
    """
    inside = False
    n = len(polygon_coords)
    if n < 3:
        return False
    j = n - 1
    for i in range(n):
        xi, yi = polygon_coords[i]
        xj, yj = polygon_coords[j]
        intersect = ((yi > lat) != (yj > lat)) and (
            lng < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersect:
            inside = not inside
        j = i
    return inside


def is_in_redzone(*, lat: float, lng: float) -> bool:
    gj = _load_geojson(REDZONES_PATH)
    feats = gj.get("features") or []
    for f in feats:
        geom = (f or {}).get("geometry") or {}
        if geom.get("type") == "Polygon":
            rings = geom.get("coordinates") or []
            if rings and isinstance(rings[0], list):
                outer = rings[0]
                if _point_in_polygon(lng, lat, outer):
                    return True
        if geom.get("type") == "MultiPolygon":
            polys = geom.get("coordinates") or []
            for poly in polys:
                if poly and isinstance(poly[0], list):
                    outer = poly[0]
                    if _point_in_polygon(lng, lat, outer):
                        return True
    return False


async def geocode_address_google(*, api_key: str, address: str) -> Optional[Tuple[float, float]]:
    """
    Returns (lat, lng) using Google Geocoding API.
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    if (data.get("status") or "").upper() != "OK":
        return None
    results = data.get("results") or []
    if not results:
        return None
    loc = ((results[0] or {}).get("geometry") or {}).get("location") or {}
    lat = loc.get("lat")
    lng = loc.get("lng")
    if lat is None or lng is None:
        return None
    return float(lat), float(lng)


async def reverse_geocode_county_google(*, api_key: str, lat: float, lng: float) -> Optional[str]:
    """
    Returns county name using Google reverse geocode.
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"latlng": f"{lat},{lng}", "key": api_key}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    if (data.get("status") or "").upper() != "OK":
        return None
    results = data.get("results") or []
    for res in results:
        comps = res.get("address_components") or []
        for c in comps:
            types = c.get("types") or []
            if "administrative_area_level_2" in types:
                # usually "Wayne County"
                return str(c.get("long_name") or "").replace(" County", "").strip() or None
    return None


async def enrich_property_geo(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    google_api_key: Optional[str],
) -> dict[str, Any]:
    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == int(property_id)))
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    address = f"{prop.address}, {prop.city}, {prop.state} {prop.zip}"
    lat = getattr(prop, "lat", None)
    lng = getattr(prop, "lng", None)

    # Step 1: geocode if missing
    if (lat is None or lng is None) and google_api_key:
        coords = await geocode_address_google(api_key=google_api_key, address=address)
        if coords:
            lat, lng = coords
            setattr(prop, "lat", float(lat))
            setattr(prop, "lng", float(lng))

    # Step 2: county if missing
    county = getattr(prop, "county", None)
    if (not county) and lat is not None and lng is not None and google_api_key:
        c = await reverse_geocode_county_google(api_key=google_api_key, lat=float(lat), lng=float(lng))
        if c:
            setattr(prop, "county", c)

    # Step 3: redzone classification
    if lat is not None and lng is not None:
        rz = is_in_redzone(lat=float(lat), lng=float(lng))
        setattr(prop, "is_red_zone", bool(rz))

    db.add(prop)
    db.commit()
    db.refresh(prop)

    return {
        "ok": True,
        "property_id": int(prop.id),
        "lat": getattr(prop, "lat", None),
        "lng": getattr(prop, "lng", None),
        "county": getattr(prop, "county", None),
        "is_red_zone": bool(getattr(prop, "is_red_zone", False)),
    }