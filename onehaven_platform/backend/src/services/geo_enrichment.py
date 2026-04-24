from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.models import Property
from onehaven_platform.backend.src.services.address_normalization import normalize_full_address
from onehaven_platform.backend.src.services.geocoding_service import GeocodingService
from onehaven_platform.backend.src.adapters.intelligence_adapter import compute_property_risk

log = logging.getLogger("onehaven.geo_enrichment")

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
REDZONES_PATH = DATA_DIR / "red_zones_detroit.geojson"


def _utcnow() -> datetime:
    return datetime.utcnow()


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


_STATE_ABBREVIATIONS: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


def _normalize_state(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 2:
        return s.upper()
    return _STATE_ABBREVIATIONS.get(s.lower()) or s[:2].upper()


def _nonblank(value: Any) -> str | None:
    s = str(value or "").strip()
    return s or None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _clean_address_text(value: Optional[str]) -> Optional[str]:
    s = _nonblank(value)
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r",\s*,+", ", ", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" ,")
    return s or None


def _looks_like_test_address(address: Optional[str]) -> bool:
    s = (_clean_address_text(address) or "").lower()
    return bool(s and (" test " in f" {s} " or s.startswith("123 test st")))


def _address_contains_city_state_zip(
    address: Optional[str],
    *,
    city: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
) -> bool:
    s = (_clean_address_text(address) or "").lower()
    if not s:
        return False
    city_ok = bool(city and city.lower() in s)
    state_ok = bool(state and state.lower() in s)
    zip_ok = bool(postal_code and postal_code in s)
    return (city_ok and state_ok) or zip_ok


def _extract_street_only(
    address: Optional[str],
    *,
    city: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
) -> Optional[str]:
    s = _clean_address_text(address)
    if not s:
        return None

    if city:
        city_pattern = re.escape(city)
        s = re.sub(
            rf",?\s+\b{city_pattern}\b(?:\s*,\s*|\s+)",
            ", ",
            s,
            flags=re.IGNORECASE,
        )
    if state:
        state_pattern = re.escape(state)
        s = re.sub(
            rf",?\s+\b{state_pattern}\b(?:\s*,\s*|\s+)",
            ", ",
            s,
            flags=re.IGNORECASE,
        )
    if postal_code:
        s = re.sub(rf",?\s*\b{re.escape(postal_code)}\b", "", s, flags=re.IGNORECASE)

    s = re.sub(r",\s*(east|west|north|south)\s*$", "", s, flags=re.IGNORECASE)

    if "," in s:
        s = s.split(",", 1)[0]

    return _clean_address_text(s)


def _address_line1(address: Optional[str]) -> Optional[str]:
    s = _clean_address_text(address)
    if not s:
        return None
    if "," in s:
        return _clean_address_text(s.split(",", 1)[0])
    return s


def _compact_full_address(
    *,
    street: Optional[str],
    city: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
    with_commas: bool,
) -> Optional[str]:
    street = _clean_address_text(street)
    city = _nonblank(city)
    state = _normalize_state(state)
    postal_code = _nonblank(postal_code)
    if not street:
        return None
    parts = [street]
    locality = " ".join([p for p in [city, state, postal_code] if p])
    if locality:
        if with_commas:
            parts.append(locality)
            return ", ".join(parts)
        return f"{street} {locality}".strip()
    return street


def _apply_redzone_flag(prop: Property) -> bool:
    lat = _to_float(getattr(prop, "lat", None))
    lng = _to_float(getattr(prop, "lng", None))
    if lat is None or lng is None:
        prop.is_red_zone = False
        return False
    prop.is_red_zone = bool(is_in_redzone(lat=lat, lng=lng))
    return bool(prop.is_red_zone)


def _apply_risk_fields(prop: Property) -> dict[str, Any]:
    lat = _to_float(getattr(prop, "lat", None))
    lng = _to_float(getattr(prop, "lng", None))
    county = getattr(prop, "county", None)
    city = getattr(prop, "city", None)
    is_red_zone = bool(getattr(prop, "is_red_zone", False))

    risk = compute_property_risk(
        lat=lat,
        lng=lng,
        city=city,
        county=county,
        is_red_zone=is_red_zone,
    )

    for field in [
        "crime_density",
        "crime_score",
        "crime_band",
        "crime_source",
        "crime_method",
        "crime_radius_miles",
        "crime_area_sq_miles",
        "crime_area_type",
        "crime_incident_count",
        "crime_weighted_incident_count",
        "crime_nearest_incident_miles",
        "crime_dataset_version",
        "crime_confidence",
        "investment_area_band",
        "offender_count",
        "offender_band",
        "offender_source",
        "offender_radius_miles",
        "nearest_offender_miles",
        "risk_score",
        "risk_band",
        "risk_summary",
        "risk_confidence",
    ]:
        if hasattr(prop, field):
            setattr(prop, field, risk.get(field))

    if hasattr(prop, "risk_last_computed_at"):
        raw = risk.get("risk_last_computed_at")
        if raw:
            try:
                prop.risk_last_computed_at = datetime.fromisoformat(str(raw))
            except Exception:
                prop.risk_last_computed_at = _utcnow()

    return risk


def _geo_snapshot(prop: Property) -> dict[str, Any]:
    return {
        "address": _nonblank(getattr(prop, "address", None)),
        "city": _nonblank(getattr(prop, "city", None)),
        "state": _nonblank(getattr(prop, "state", None)),
        "zip": _nonblank(getattr(prop, "zip", None)),
        "normalized_address": _nonblank(getattr(prop, "normalized_address", None)),
        "lat": _to_float(getattr(prop, "lat", None)),
        "lng": _to_float(getattr(prop, "lng", None)),
        "county": _nonblank(getattr(prop, "county", None)),
        "geocode_source": _nonblank(getattr(prop, "geocode_source", None)),
        "geocode_confidence": _to_float(getattr(prop, "geocode_confidence", None)),
        "crime_density": _to_float(getattr(prop, "crime_density", None)),
        "crime_score": _to_float(getattr(prop, "crime_score", None)),
        "crime_band": _nonblank(getattr(prop, "crime_band", None)),
        "crime_source": _nonblank(getattr(prop, "crime_source", None)),
        "crime_method": _nonblank(getattr(prop, "crime_method", None)),
        "crime_radius_miles": _to_float(getattr(prop, "crime_radius_miles", None)),
        "crime_area_sq_miles": _to_float(getattr(prop, "crime_area_sq_miles", None)),
        "crime_incident_count": getattr(prop, "crime_incident_count", None),
        "crime_weighted_incident_count": _to_float(getattr(prop, "crime_weighted_incident_count", None)),
        "crime_nearest_incident_miles": _to_float(getattr(prop, "crime_nearest_incident_miles", None)),
        "crime_confidence": _to_float(getattr(prop, "crime_confidence", None)),
        "investment_area_band": _nonblank(getattr(prop, "investment_area_band", None)),
        "offender_count": getattr(prop, "offender_count", None),
        "offender_band": _nonblank(getattr(prop, "offender_band", None)),
        "offender_source": _nonblank(getattr(prop, "offender_source", None)),
        "offender_radius_miles": _to_float(getattr(prop, "offender_radius_miles", None)),
        "nearest_offender_miles": _to_float(getattr(prop, "nearest_offender_miles", None)),
        "risk_score": _to_float(getattr(prop, "risk_score", None)),
        "risk_band": _nonblank(getattr(prop, "risk_band", None)),
        "risk_summary": _nonblank(getattr(prop, "risk_summary", None)),
        "risk_confidence": _to_float(getattr(prop, "risk_confidence", None)),
        "is_red_zone": bool(getattr(prop, "is_red_zone", False)),
        "geocode_last_refreshed": getattr(prop, "geocode_last_refreshed", None),
    }


def _is_geo_complete(snapshot: dict[str, Any]) -> bool:
    return bool(
        snapshot.get("normalized_address")
        and snapshot.get("lat") is not None
        and snapshot.get("lng") is not None
        and snapshot.get("geocode_source")
    )


def _is_risk_complete(snapshot: dict[str, Any]) -> bool:
    return bool(
        snapshot.get("lat") is not None
        and snapshot.get("lng") is not None
        and snapshot.get("crime_score") is not None
        and snapshot.get("offender_count") is not None
    )


def _geo_reason(snapshot: dict[str, Any]) -> str | None:
    if snapshot.get("lat") is None or snapshot.get("lng") is None:
        return "missing_coordinates"
    if not snapshot.get("normalized_address"):
        return "missing_normalized_address"
    if not snapshot.get("geocode_source"):
        return "missing_geocode_source"
    return None


def _address_candidates(prop: Property) -> list[dict[str, Any]]:
    raw_address = _clean_address_text(getattr(prop, "address", None))
    city = _nonblank(getattr(prop, "city", None))
    state = _normalize_state(getattr(prop, "state", None)) or "MI"
    postal_code = _nonblank(getattr(prop, "zip", None))
    county = _normalize_county(getattr(prop, "county", None))
    normalized = _clean_address_text(getattr(prop, "normalized_address", None))
    canonical_full = normalize_full_address(raw_address, city, state, postal_code).full_address if raw_address else ""

    if _looks_like_test_address(raw_address):
        return []

    street_only = _extract_street_only(
        raw_address,
        city=city,
        state=state,
        postal_code=postal_code,
    )
    address_line1 = _address_line1(raw_address)

    address_already_full = _address_contains_city_state_zip(
        raw_address,
        city=city,
        state=state,
        postal_code=postal_code,
    )

    candidates: list[dict[str, Any]] = []

    def add_candidate(*, label: str, address_value: str | None, city_value: str | None, state_value: str | None, postal_code_value: str | None) -> None:
        address_value = _clean_address_text(address_value)
        city_value = _nonblank(city_value)
        state_value = _normalize_state(state_value)
        postal_code_value = _nonblank(postal_code_value)
        if not address_value:
            return

        fingerprint = (
            address_value.lower(),
            (city_value or "").lower(),
            (state_value or "").lower(),
            (postal_code_value or "").lower(),
        )
        for existing in candidates:
            existing_fp = (
                (_clean_address_text(existing.get("address")) or "").lower(),
                (_nonblank(existing.get("city")) or "").lower(),
                (_normalize_state(existing.get("state")) or "").lower(),
                (_nonblank(existing.get("postal_code")) or "").lower(),
            )
            if existing_fp == fingerprint:
                return

        candidates.append(
            {
                "label": label,
                "address": address_value,
                "city": city_value,
                "state": state_value,
                "postal_code": postal_code_value,
            }
        )

    if canonical_full:
        add_candidate(label="canonical_full_address", address_value=canonical_full, city_value=None, state_value=None, postal_code_value=None)
    if normalized and normalized != canonical_full and normalized.count((city or '').strip()) <= 1:
        add_candidate(label="normalized_address", address_value=normalized, city_value=None, state_value=None, postal_code_value=None)

    if raw_address:
        if address_already_full:
            add_candidate(label="raw_full_address", address_value=raw_address, city_value=None, state_value=None, postal_code_value=None)
            add_candidate(
                label="raw_full_address_compact",
                address_value=_compact_full_address(
                    street=address_line1,
                    city=city,
                    state=state,
                    postal_code=postal_code,
                    with_commas=False,
                ),
                city_value=None,
                state_value=None,
                postal_code_value=None,
            )
        else:
            add_candidate(label="raw_address_with_parts", address_value=raw_address, city_value=city, state_value=state, postal_code_value=postal_code)

    if address_line1 and city and state and postal_code:
        add_candidate(label="line1_city_state_zip", address_value=address_line1, city_value=city, state_value=state, postal_code_value=postal_code)
        add_candidate(
            label="line1_compact_full",
            address_value=_compact_full_address(
                street=address_line1,
                city=city,
                state=state,
                postal_code=postal_code,
                with_commas=False,
            ),
            city_value=None,
            state_value=None,
            postal_code_value=None,
        )
    if address_line1 and city and state:
        add_candidate(label="line1_city_state", address_value=address_line1, city_value=city, state_value=state, postal_code_value=None)
    if address_line1 and city:
        add_candidate(label="line1_city", address_value=address_line1, city_value=city, state_value=state, postal_code_value=None)

    if street_only and city and state and postal_code:
        add_candidate(label="street_city_state_zip", address_value=street_only, city_value=city, state_value=state, postal_code_value=postal_code)
        add_candidate(
            label="street_compact_full",
            address_value=_compact_full_address(
                street=street_only,
                city=city,
                state=state,
                postal_code=postal_code,
                with_commas=False,
            ),
            city_value=None,
            state_value=None,
            postal_code_value=None,
        )
    if street_only and city and state:
        add_candidate(label="street_city_state", address_value=street_only, city_value=city, state_value=state, postal_code_value=None)
    if street_only and state and postal_code:
        add_candidate(label="street_state_zip", address_value=street_only, city_value=None, state_value=state, postal_code_value=postal_code)
    if street_only and city:
        add_candidate(label="street_city", address_value=street_only, city_value=city, state_value=state, postal_code_value=None)
    if street_only:
        add_candidate(label="street_only_fallback", address_value=street_only, city_value=city or county, state_value=state, postal_code_value=postal_code)

    return candidates




def _property_metadata_attr_name(prop: Property) -> str:
    if hasattr(prop, "acquisition_metadata_json"):
        return "acquisition_metadata_json"
    if hasattr(prop, "raw_json"):
        return "raw_json"
    return "acquisition_metadata_json"


def _property_metadata_dict(prop: Property) -> dict[str, Any]:
    raw = getattr(prop, _property_metadata_attr_name(prop), None)
    return dict(raw) if isinstance(raw, dict) else {}


def _set_property_metadata_dict(prop: Property, value: dict[str, Any]) -> None:
    setattr(prop, _property_metadata_attr_name(prop), dict(value or {}))


def _raw_json_dict(prop: Property) -> dict[str, Any]:
    return _property_metadata_dict(prop)


def _extract_payload_geo_candidate(prop: Property) -> dict[str, Any] | None:
    raw = _raw_json_dict(prop)
    if not raw:
        return None

    payloads: list[dict[str, Any]] = [raw]
    nested = raw.get("raw")
    if isinstance(nested, dict):
        payloads.append(nested)

    for payload in payloads:
        lat = _to_float(
            payload.get("latitude")
            or payload.get("lat")
            or payload.get("locationLat")
            or payload.get("geoLat")
        )
        lng = _to_float(
            payload.get("longitude")
            or payload.get("lng")
            or payload.get("lon")
            or payload.get("locationLng")
            or payload.get("geoLng")
        )
        if lat is None or lng is None:
            continue

        county = _normalize_county(
            payload.get("county")
            or payload.get("countyName")
            or payload.get("county_name")
        )
        state = _normalize_state(payload.get("state")) or _normalize_state(getattr(prop, "state", None))
        city = _nonblank(payload.get("city")) or _nonblank(getattr(prop, "city", None))
        postal_code = _nonblank(
            payload.get("zipCode")
            or payload.get("postalCode")
            or payload.get("zip")
            or getattr(prop, "zip", None)
        )
        formatted_address = _clean_address_text(
            payload.get("formattedAddress")
            or payload.get("address")
            or getattr(prop, "address", None)
        )
        normalized_address = normalize_full_address(
            formatted_address or getattr(prop, "address", None),
            city,
            state,
            postal_code,
        ).full_address

        return {
            "normalized_address": normalized_address,
            "source": "source_payload",
            "confidence": 0.72,
            "lat": lat,
            "lng": lng,
            "county": county,
            "state": state,
            "city": city,
            "postal_code": postal_code,
            "provider_status": "SOURCE_PAYLOAD_COORDINATES",
            "cache_hit": False,
            "is_success": True,
            "raw_json": payload,
        }

    return None


def _apply_payload_geo_candidate(prop: Property, candidate: dict[str, Any], before: dict[str, Any]) -> dict[str, Any]:
    changed = False

    normalized_address = normalize_full_address(candidate.get("normalized_address") or getattr(prop, 'address', None), candidate.get('city') or getattr(prop, 'city', None), candidate.get('state') or getattr(prop, 'state', None), candidate.get('postal_code') or getattr(prop, 'zip', None)).full_address or _clean_address_text(candidate.get("normalized_address"))
    if normalized_address and normalized_address != before.get("normalized_address"):
        prop.normalized_address = normalized_address
        changed = True

    lat = _to_float(candidate.get("lat"))
    lng = _to_float(candidate.get("lng"))
    if lat is not None and lng is not None:
        if lat != before.get("lat"):
            prop.lat = lat
            changed = True
        if lng != before.get("lng"):
            prop.lng = lng
            changed = True

    county = _normalize_county(candidate.get("county"))
    if county and county != before.get("county"):
        prop.county = county
        changed = True

    state = _normalize_state(candidate.get("state"))
    if state and _nonblank(getattr(prop, "state", None)) != state:
        prop.state = state
        changed = True

    city = _nonblank(candidate.get("city"))
    if city and _nonblank(getattr(prop, "city", None)) != city:
        prop.city = city
        changed = True

    postal_code = _nonblank(candidate.get("postal_code"))
    if postal_code and _nonblank(getattr(prop, "zip", None)) != postal_code:
        prop.zip = postal_code
        changed = True

    confidence = _to_float(candidate.get("confidence"))
    if confidence is not None and confidence != before.get("geocode_confidence"):
        prop.geocode_confidence = confidence
        changed = True

    source = _nonblank(candidate.get("source"))
    if source and source != before.get("geocode_source"):
        prop.geocode_source = source
        changed = True

    if changed:
        prop.geocode_last_refreshed = _utcnow()

    after = _geo_snapshot(prop)
    return {
        "normalized_address": normalized_address,
        "source": source,
        "confidence": confidence,
        "lat": lat,
        "lng": lng,
        "county": county,
        "state": state,
        "provider_status": _nonblank(candidate.get("provider_status")),
        "cache_hit": False,
        "is_success": True,
        "changed": changed,
        "geo_complete": _is_geo_complete(after),
    }


def _get_geo_retry_meta(prop: Property) -> dict[str, Any]:
    raw = _property_metadata_dict(prop)
    meta = raw.get("_geo_retry_meta")
    return dict(meta) if isinstance(meta, dict) else {}


def _get_retry_limit_for_type(retry_type: str) -> int:
    env_key = f"GEO_{str(retry_type).upper()}_RETRY_LIMIT"
    raw_value = os.getenv(env_key, "").strip()
    if raw_value:
        try:
            return max(0, int(raw_value))
        except Exception:
            return 1
    return 1


def _remaining_retry_budget(prop: Property, retry_type: str) -> int:
    meta = _get_geo_retry_meta(prop)
    used = 0
    try:
        used = int((meta.get("used") or {}).get(retry_type) or 0)
    except Exception:
        used = 0
    return max(0, _get_retry_limit_for_type(retry_type) - used)


def _consume_retry_budget(prop: Property, retry_type: str) -> dict[str, Any]:
    raw = _property_metadata_dict(prop)
    meta = _get_geo_retry_meta(prop)
    used = dict(meta.get("used") or {})
    used[retry_type] = int(used.get(retry_type) or 0) + 1
    meta["used"] = used
    meta["last_retry_type"] = retry_type
    meta["last_retry_at"] = _utcnow().isoformat()
    raw["_geo_retry_meta"] = meta
    _set_property_metadata_dict(prop, raw)
    return meta

def _apply_geocode_result(prop: Property, result: Any, before: dict[str, Any]) -> dict[str, Any]:
    normalized_address = normalize_full_address(getattr(result, 'formatted_address', None) or getattr(prop, 'address', None), getattr(result, 'city', None) or getattr(prop, 'city', None), getattr(result, 'state', None) or getattr(prop, 'state', None), getattr(result, 'postal_code', None) or getattr(prop, 'zip', None)).full_address or _nonblank(getattr(result, 'normalized_address', None))
    source = _nonblank(getattr(result, "source", None))
    confidence = _to_float(getattr(result, "confidence", None))
    lat = _to_float(getattr(result, "lat", None))
    lng = _to_float(getattr(result, "lng", None))
    county = _normalize_county(getattr(result, "county", None))
    state = _normalize_state(getattr(result, "state", None))

    changed = False
    if normalized_address and normalized_address != before.get("normalized_address"):
        prop.normalized_address = normalized_address
        changed = True
    if source and source != before.get("geocode_source"):
        prop.geocode_source = source
        changed = True
    if confidence is not None and confidence != before.get("geocode_confidence"):
        prop.geocode_confidence = confidence
        changed = True
    if lat is not None and lng is not None:
        if lat != before.get("lat"):
            prop.lat = lat
            changed = True
        if lng != before.get("lng"):
            prop.lng = lng
            changed = True
    if county and county != before.get("county"):
        prop.county = county
        changed = True
    if state and _nonblank(getattr(prop, "state", None)) != state:
        prop.state = state
        changed = True
    if changed:
        prop.geocode_last_refreshed = _utcnow()

    after = _geo_snapshot(prop)
    return {
        "normalized_address": normalized_address,
        "source": source,
        "confidence": confidence,
        "lat": lat,
        "lng": lng,
        "county": county,
        "state": state,
        "provider_status": _nonblank(getattr(result, "provider_status", None)),
        "cache_hit": bool(getattr(result, "cache_hit", False)),
        "is_success": bool(getattr(result, "is_success", True)),
        "changed": changed,
        "geo_complete": _is_geo_complete(after),
    }


def enrich_property_risk(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    lat = _to_float(getattr(prop, "lat", None))
    lng = _to_float(getattr(prop, "lng", None))
    if lat is None or lng is None:
        return {"ok": False, "error": "missing_coordinates", "property_id": int(property_id)}

    _apply_redzone_flag(prop)
    risk = _apply_risk_fields(prop)

    db.add(prop)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    db.refresh(prop)

    snapshot = _geo_snapshot(prop)
    risk_ok = _is_risk_complete(snapshot)
    return {
        "ok": risk_ok,
        "property_id": int(prop.id),
        "lat": snapshot["lat"],
        "lng": snapshot["lng"],
        "county": snapshot["county"],
        "is_red_zone": snapshot["is_red_zone"],
        "crime_density": snapshot["crime_density"],
        "crime_score": snapshot["crime_score"],
        "offender_count": snapshot["offender_count"],
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
        "risk_computed": risk_ok,
    }


async def enrich_property_geo_async(db: Session, *, org_id: int, property_id: int, google_api_key: Optional[str] = None, force: bool = False) -> dict[str, Any]:
    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if not prop:
        return {"ok": False, "error": "property_not_found"}

    warnings: list[str] = []
    attempt_log: list[dict[str, Any]] = []
    before = _geo_snapshot(prop)
    geocoded = False
    reverse_geocoded = False
    risk_computed = False
    cache_hit = False
    geocode_attempted = False
    geocode_provider_status: str | None = None

    needs_geocode = bool(force) or not _is_geo_complete(before)

    if needs_geocode:
        payload_candidate = _extract_payload_geo_candidate(prop)
        if payload_candidate is not None:
            applied_payload = _apply_payload_geo_candidate(prop, payload_candidate, before)
            geocode_provider_status = applied_payload.get("provider_status")
            geocoded = bool(applied_payload.get("lat") is not None and applied_payload.get("lng") is not None)
            reverse_geocoded = bool(applied_payload.get("county") and not before.get("county"))
            attempt_log.append(
                {
                    "attempt": 0,
                    "label": "source_payload_coordinates",
                    "ok": bool(applied_payload.get("geo_complete")),
                    "provider_status": applied_payload.get("provider_status"),
                    "changed": bool(applied_payload.get("changed")),
                    "lat": applied_payload.get("lat"),
                    "lng": applied_payload.get("lng"),
                    "normalized_address": applied_payload.get("normalized_address"),
                }
            )
            before = _geo_snapshot(prop)
            if _is_geo_complete(before):
                needs_geocode = False

    if not settings.geocoding_enabled:
        warnings.append("geocoding_disabled")
    elif needs_geocode:
        geocoding_service = GeocodingService(db)
        candidates = _address_candidates(prop)
        if not candidates:
            warnings.append("missing_input_address")
        else:
            geocode_attempted = True
            for idx, candidate in enumerate(candidates, start=1):
                try:
                    result = geocoding_service.geocode(
                        address=candidate.get("address"),
                        city=candidate.get("city"),
                        state=candidate.get("state"),
                        postal_code=candidate.get("postal_code"),
                        force_refresh=bool(force),
                    )
                except Exception as exc:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    attempt_log.append({
                        "attempt": idx,
                        "label": candidate.get("label"),
                        "address": candidate.get("address"),
                        "city": candidate.get("city"),
                        "state": candidate.get("state"),
                        "postal_code": candidate.get("postal_code"),
                        "ok": False,
                        "error": f"{type(exc).__name__}:{exc}",
                    })
                    log.exception(
                        "property_geocode_attempt_failed",
                        extra={
                            "org_id": int(org_id),
                            "property_id": int(property_id),
                            "attempt": idx,
                            "candidate_label": candidate.get("label"),
                        },
                    )
                    continue

                if result is None:
                    attempt_log.append({
                        "attempt": idx,
                        "label": candidate.get("label"),
                        "address": candidate.get("address"),
                        "city": candidate.get("city"),
                        "state": candidate.get("state"),
                        "postal_code": candidate.get("postal_code"),
                        "ok": False,
                        "provider_status": None,
                        "cache_hit": False,
                        "reason": "no_result",
                    })
                    continue

                applied = _apply_geocode_result(prop, result, before)
                geocode_provider_status = applied.get("provider_status")
                cache_hit = bool(applied.get("cache_hit"))
                geocoded = bool(applied.get("lat") is not None and applied.get("lng") is not None)
                reverse_geocoded = bool(applied.get("county") and not before.get("county"))
                attempt_log.append({
                    "attempt": idx,
                    "label": candidate.get("label"),
                    "address": candidate.get("address"),
                    "city": candidate.get("city"),
                    "state": candidate.get("state"),
                    "postal_code": candidate.get("postal_code"),
                    "ok": bool(applied.get("geo_complete")),
                    "provider_status": applied.get("provider_status"),
                    "cache_hit": bool(applied.get("cache_hit")),
                    "changed": bool(applied.get("changed")),
                    "lat": applied.get("lat"),
                    "lng": applied.get("lng"),
                    "normalized_address": applied.get("normalized_address"),
                })
                if applied.get("geo_complete"):
                    break

            if not attempt_log:
                warnings.append("geocode_no_attempts")
            elif not any(bool(item.get("ok")) for item in attempt_log):
                warnings.append("geocode_no_result")
    else:
        warnings.append("geocode_skipped_already_complete")

    current_lat = _to_float(getattr(prop, "lat", None))
    current_lng = _to_float(getattr(prop, "lng", None))
    if current_lat is not None and current_lng is not None:
        _apply_redzone_flag(prop)
        risk = _apply_risk_fields(prop)
        risk_computed = True
    else:
        risk = {
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
        warnings.append("missing_coordinates_for_risk_scoring")

    db.add(prop)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    db.refresh(prop)

    snapshot = _geo_snapshot(prop)
    geo_complete = _is_geo_complete(snapshot)
    risk_complete = _is_risk_complete(snapshot)
    geo_reason = None if geo_complete else _geo_reason(snapshot)
    if geo_reason and geo_reason not in warnings:
        warnings.append(geo_reason)

    if geocode_attempted and not geo_complete and not geocode_provider_status:
        geocode_provider_status = "no_match"

    return {
        "ok": geo_complete,
        "property_id": int(prop.id),
        "normalized_address": snapshot["normalized_address"],
        "lat": snapshot["lat"],
        "lng": snapshot["lng"],
        "county": snapshot["county"],
        "geocode_source": snapshot["geocode_source"],
        "geocode_confidence": getattr(prop, "geocode_confidence", None),
        "geocode_last_refreshed": snapshot["geocode_last_refreshed"],
        "cache_hit": cache_hit,
        "is_red_zone": snapshot["is_red_zone"],
        "crime_density": snapshot["crime_density"],
        "crime_score": snapshot["crime_score"],
        "offender_count": snapshot["offender_count"],
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
        "geocode_attempted": geocode_attempted,
        "geocoded": geocoded,
        "reverse_geocoded": reverse_geocoded,
        "risk_computed": risk_computed,
        "geo_complete": geo_complete,
        "risk_complete": risk_complete,
        "geo_reason": geo_reason,
        "provider_status": geocode_provider_status,
        "warnings": warnings,
        "attempt_log": attempt_log,
    }


def enrich_property_geo(db: Session, *, org_id: int, property_id: int, google_api_key: Optional[str] = None, force: bool = False) -> dict[str, Any]:
    try:
        return asyncio.run(
            enrich_property_geo_async(
                db,
                org_id=int(org_id),
                property_id=int(property_id),
                google_api_key=google_api_key,
                force=force,
            )
        )
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                enrich_property_geo_async(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_id),
                    google_api_key=google_api_key,
                    force=force,
                )
            )
        finally:
            loop.close()
