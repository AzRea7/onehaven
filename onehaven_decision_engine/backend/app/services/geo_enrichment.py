from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Tuple

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
REDZONES_PATH = DATA_DIR / "red_zones_detroit.geojson"


def _load_geojson(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"type": "FeatureCollection", "features": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"type": "FeatureCollection", "features": []}


def _point_in_ring(lng: float, lat: float, ring: list[list[float]]) -> bool:
    """
    Ray casting.
    ring: [[lng, lat], ...]
    """
    inside = False
    n = len(ring)
    if n < 3:
        return False

    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]

        intersects = ((yi > lat) != (yj > lat)) and (
            lng < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i

    return inside


def _point_in_polygon_geom(lng: float, lat: float, coords: list[Any]) -> bool:
    """
    Polygon coordinates:
      [
        [outer ring],
        [hole1],
        [hole2]
      ]
    """
    if not coords or not isinstance(coords, list):
        return False

    outer = coords[0]
    if not outer or not _point_in_ring(lng, lat, outer):
        return False

    # If point is inside any hole, it is NOT inside polygon
    for hole in coords[1:]:
        if hole and _point_in_ring(lng, lat, hole):
            return False

    return True


def is_in_redzone(*, lat: float, lng: float) -> bool:
    gj = _load_geojson(REDZONES_PATH)
    feats = gj.get("features") or []

    for feat in feats:
        geom = (feat or {}).get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates") or []

        if gtype == "Polygon":
            if _point_in_polygon_geom(lng, lat, coords):
                return True

        elif gtype == "MultiPolygon":
            for poly in coords:
                if _point_in_polygon_geom(lng, lat, poly):
                    return True

    return False


def _normalize_county(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.lower().endswith(" county"):
        s = s[:-7].strip()
    return s or None


async def geocode_address_google(*, api_key: str, address: str) -> Optional[Tuple[float, float]]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}

    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    if str(data.get("status") or "").upper() != "OK":
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
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"latlng": f"{lat},{lng}", "key": api_key}

    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()

    if str(data.get("status") or "").upper() != "OK":
        return None

    results = data.get("results") or []
    for res in results:
        comps = res.get("address_components") or []
        for c in comps:
            types = c.get("types") or []
            if "administrative_area_level_2" in types:
                return _normalize_county(c.get("long_name"))

    return None


async def enrich_property_geo(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    google_api_key: Optional[str],
    force: bool = False,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == int(property_id),
        )
    )
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    warnings: list[str] = []

    current_lat = getattr(prop, "lat", None)
    current_lng = getattr(prop, "lng", None)
    current_county = getattr(prop, "county", None)

    geocoded = False
    reverse_geocoded = False

    address = f"{prop.address}, {prop.city}, {prop.state} {prop.zip}".strip()

    # Step 1: geocode if needed (or force)
    if force or current_lat is None or current_lng is None:
        if google_api_key:
            coords = await geocode_address_google(api_key=google_api_key, address=address)
            if coords:
                prop.lat = float(coords[0])
                prop.lng = float(coords[1])
                geocoded = True
            else:
                warnings.append("google_geocode_failed")
        else:
            warnings.append("missing_google_maps_api_key")

    # refresh locals after possible geocode
    current_lat = getattr(prop, "lat", None)
    current_lng = getattr(prop, "lng", None)

    # Step 2: reverse geocode county if needed (or force)
    if current_lat is not None and current_lng is not None and (force or not current_county):
        if google_api_key:
            county = await reverse_geocode_county_google(
                api_key=google_api_key,
                lat=float(current_lat),
                lng=float(current_lng),
            )
            if county:
                prop.county = county
                reverse_geocoded = True
            else:
                warnings.append("google_reverse_geocode_failed")
        else:
            warnings.append("missing_google_maps_api_key")

    # Step 3: red-zone classification if we have coordinates
    if current_lat is not None and current_lng is not None:
        prop.is_red_zone = bool(is_in_redzone(lat=float(current_lat), lng=float(current_lng)))
    else:
        warnings.append("missing_coordinates_for_redzone_check")

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
        "geocoded": geocoded,
        "reverse_geocoded": reverse_geocoded,
        "warnings": warnings,
    }
