from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Property
from app.services.geocoding_service import GeocodingService
from app.services.risk_scoring import compute_property_risk, refresh_property_risk

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
    if not coords or not isinstance(coords, list):
        return False

    outer = coords[0]
    if not outer or not _point_in_ring(lng, lat, outer):
        return False

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


def _apply_redzone_flag(prop: Property) -> bool:
    lat = getattr(prop, "lat", None)
    lng = getattr(prop, "lng", None)

    if lat is None or lng is None:
        prop.is_red_zone = False
        return False

    prop.is_red_zone = bool(is_in_redzone(lat=float(lat), lng=float(lng)))
    return bool(prop.is_red_zone)


def _apply_risk_fields(prop: Property) -> dict[str, Any]:
    lat = getattr(prop, "lat", None)
    lng = getattr(prop, "lng", None)
    county = getattr(prop, "county", None)
    city = getattr(prop, "city", None)
    is_red_zone = bool(getattr(prop, "is_red_zone", False))

    risk = compute_property_risk(
        lat=float(lat) if lat is not None else None,
        lng=float(lng) if lng is not None else None,
        city=city,
        county=county,
        is_red_zone=is_red_zone,
    )

    if hasattr(prop, "crime_density"):
        prop.crime_density = risk.get("crime_density")
    if hasattr(prop, "crime_score"):
        prop.crime_score = risk.get("crime_score")
    if hasattr(prop, "offender_count"):
        prop.offender_count = risk.get("offender_count")
    if hasattr(prop, "risk_score"):
        prop.risk_score = risk.get("risk_score")
    if hasattr(prop, "risk_band"):
        prop.risk_band = risk.get("risk_band")

    return risk


def enrich_property_risk(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    if getattr(prop, "lat", None) is None or getattr(prop, "lng", None) is None:
        return {
            "ok": False,
            "error": "missing_coordinates",
            "property_id": int(property_id),
        }

    _apply_redzone_flag(prop)
    risk = _apply_risk_fields(prop)

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
        "crime_density": getattr(prop, "crime_density", None),
        "crime_score": getattr(prop, "crime_score", None),
        "offender_count": getattr(prop, "offender_count", None),
        "crime_band": risk.get("crime_band"),
        "crime_source": risk.get("crime_source"),
        "offender_band": risk.get("offender_band"),
        "offender_source": risk.get("offender_source"),
        "offender_radius_miles": risk.get("offender_radius_miles"),
        "nearest_offender_miles": risk.get("nearest_offender_miles"),
        "risk_score": risk.get("risk_score"),
        "risk_band": risk.get("risk_band"),
        "risk_summary": risk.get("risk_summary"),
        "risk_factors": risk.get("risk_factors"),
    }


async def enrich_property_geo_async(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    google_api_key: Optional[str] = None,
    force: bool = False,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
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
    risk_computed = False
    cache_hit = False

    if not settings.geocoding_enabled:
        warnings.append("geocoding_disabled")
    else:
        geocoding_service = GeocodingService(db)

        result = geocoding_service.geocode(
            address=getattr(prop, "address", None),
            city=getattr(prop, "city", None),
            state=getattr(prop, "state", None),
            postal_code=getattr(prop, "zip", None),
            force_refresh=bool(force),
        )

        if result is None:
            warnings.append("geocode_no_result")
        else:
            cache_hit = bool(result.cache_hit)

            previous_lat = current_lat
            previous_lng = current_lng
            previous_county = current_county

            prop.normalized_address = result.normalized_address
            prop.geocode_source = result.source
            prop.geocode_confidence = result.confidence
            prop.geocode_last_refreshed = None if result.cache_hit and getattr(prop, "geocode_last_refreshed", None) else result.raw_json and __import__("datetime").datetime.utcnow()

            if result.lat is not None and result.lng is not None:
                prop.lat = float(result.lat)
                prop.lng = float(result.lng)

            if result.county:
                prop.county = _normalize_county(result.county)

            geocoded = (
                (previous_lat is None or previous_lng is None)
                and result.lat is not None
                and result.lng is not None
            ) or bool(force and result.lat is not None and result.lng is not None)

            reverse_geocoded = (
                (not previous_county and bool(result.county))
                or (bool(force) and bool(result.county))
            )

            if not result.is_success:
                warnings.append(f"geocode_provider_status:{result.provider_status or 'unknown'}")

    current_lat = getattr(prop, "lat", None)
    current_lng = getattr(prop, "lng", None)

    if current_lat is not None and current_lng is not None:
        _apply_redzone_flag(prop)
    else:
        warnings.append("missing_coordinates_for_redzone_check")

    risk: dict[str, Any] = {
        "crime_density": None,
        "crime_score": None,
        "crime_band": "unknown",
        "crime_source": None,
        "offender_count": None,
        "offender_band": "unknown",
        "offender_source": None,
        "offender_radius_miles": 1.0,
        "nearest_offender_miles": None,
        "risk_score": None,
        "risk_band": "unknown",
        "risk_summary": "missing_coordinates",
        "risk_factors": ["missing_coordinates"],
    }

    if current_lat is not None and current_lng is not None:
        risk = _apply_risk_fields(prop)
        risk_computed = True
    else:
        warnings.append("missing_coordinates_for_risk_scoring")

    db.add(prop)
    db.commit()
    db.refresh(prop)

    return {
        "ok": True,
        "property_id": int(prop.id),
        "normalized_address": getattr(prop, "normalized_address", None),
        "lat": getattr(prop, "lat", None),
        "lng": getattr(prop, "lng", None),
        "county": getattr(prop, "county", None),
        "geocode_source": getattr(prop, "geocode_source", None),
        "geocode_confidence": getattr(prop, "geocode_confidence", None),
        "geocode_last_refreshed": getattr(prop, "geocode_last_refreshed", None),
        "cache_hit": cache_hit,
        "is_red_zone": bool(getattr(prop, "is_red_zone", False)),
        "crime_density": getattr(prop, "crime_density", None),
        "crime_score": getattr(prop, "crime_score", None),
        "offender_count": getattr(prop, "offender_count", None),
        "crime_band": risk.get("crime_band"),
        "crime_source": risk.get("crime_source"),
        "offender_band": risk.get("offender_band"),
        "offender_source": risk.get("offender_source"),
        "offender_radius_miles": risk.get("offender_radius_miles"),
        "nearest_offender_miles": risk.get("nearest_offender_miles"),
        "risk_score": risk.get("risk_score"),
        "risk_band": risk.get("risk_band"),
        "risk_summary": risk.get("risk_summary"),
        "risk_factors": risk.get("risk_factors"),
        "geocoded": geocoded,
        "reverse_geocoded": reverse_geocoded,
        "risk_computed": risk_computed,
        "warnings": warnings,
    }


def enrich_property_geo(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    google_api_key: Optional[str] = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Sync-safe wrapper used by ingestion and other normal service code.
    """
    return asyncio.run(
        enrich_property_geo_async(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            google_api_key=google_api_key,
            force=force,
        )
    )