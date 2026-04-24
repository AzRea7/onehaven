from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "crime"

DEFAULT_CRIME_RADIUS_MILES = 0.75
DEFAULT_CRIME_AREA_TYPE = "radius_circle"
DEFAULT_DATASET_VERSION = "local_crime_dataset_v1"

_CATEGORY_WEIGHT_OVERRIDES: dict[str, float] = {
    "homicide": 6.0,
    "murder": 6.0,
    "shooting": 5.5,
    "assault": 4.5,
    "aggravated_assault": 4.5,
    "robbery": 4.0,
    "burglary": 3.0,
    "carjacking": 4.5,
    "vehicle_theft": 3.5,
    "auto_theft": 3.5,
    "larceny": 2.0,
    "theft": 2.0,
    "vandalism": 1.5,
    "drugs": 1.5,
    "narcotics": 1.5,
    "other": 1.0,
}

@dataclass(frozen=True)
class CrimePoint:
    lat: float
    lng: float
    weight: float = 1.0
    category: str | None = None
    occurred_at: str | None = None

def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(a))

def _iter_candidate_files() -> list[Path]:
    return [
        DATA_DIR / "crime_points.csv",
        DATA_DIR / "crime_index.csv",
        DATA_DIR / "crime_points.json",
        DATA_DIR / "crime_index.json",
    ]

def _normalize_category(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    return raw or None

def _effective_weight(category: str | None, explicit_weight: float | None) -> float:
    cat_weight = _CATEGORY_WEIGHT_OVERRIDES.get(str(category or "").strip().lower(), 1.0)
    if explicit_weight is None:
        return cat_weight
    # Respect explicit weight, but anchor it to category severity so weak rows do not dominate.
    return max(0.25, min(8.0, float(explicit_weight) * max(cat_weight, 0.75)))

def _row_to_point(row: dict[str, Any]) -> CrimePoint | None:
    lat = _safe_float(row.get("lat"))
    lng = _safe_float(row.get("lng"))
    if lat is None or lng is None:
        return None

    category = _normalize_category(row.get("category") or row.get("type") or row.get("offense"))
    explicit_weight = _safe_float(row.get("weight"), None)
    weight = _effective_weight(category, explicit_weight)
    occurred_at = str(row.get("occurred_at") or row.get("date") or row.get("timestamp") or "").strip() or None
    return CrimePoint(lat=lat, lng=lng, weight=weight, category=category, occurred_at=occurred_at)

@lru_cache(maxsize=1)
def load_crime_points() -> tuple[CrimePoint, ...]:
    points: list[CrimePoint] = []

    for path in _iter_candidate_files():
        if not path.exists():
            continue

        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    point = _row_to_point(dict(row))
                    if point is not None:
                        points.append(point)

        elif path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            if isinstance(payload, dict):
                payload = payload.get("rows") or payload.get("points") or payload.get("items") or []

            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        point = _row_to_point(item)
                        if point is not None:
                            points.append(point)

    return tuple(points)

def _area_sq_miles(radius_miles: float) -> float:
    return math.pi * (radius_miles ** 2)

def _distance_decay(distance_miles: float, radius_miles: float) -> float:
    if radius_miles <= 0:
        return 0.0
    # Strong local emphasis: block-level incidents matter much more than edge-of-radius incidents.
    normalized = max(0.0, min(1.0, distance_miles / radius_miles))
    return max(0.15, 1.0 - (normalized ** 1.35) * 0.85)

def _estimate_from_points(
    lat: float,
    lng: float,
    *,
    points: tuple[CrimePoint, ...],
    radius_miles: float,
) -> dict[str, Any]:
    if not points:
        return {
            "crime_density": 0.0,
            "crime_incident_count": 0,
            "crime_weighted_incident_count": 0.0,
            "crime_nearest_incident_miles": None,
        }

    incident_count = 0
    weighted = 0.0
    nearest = None

    for p in points:
        dist = _haversine_miles(lat, lng, p.lat, p.lng)
        if nearest is None or dist < nearest:
            nearest = dist
        if dist <= radius_miles:
            incident_count += 1
            weighted += p.weight * _distance_decay(dist, radius_miles)

    area_sq_miles = _area_sq_miles(radius_miles)
    density = round(weighted / area_sq_miles, 4) if area_sq_miles > 0 else 0.0

    return {
        "crime_density": density,
        "crime_incident_count": int(incident_count),
        "crime_weighted_incident_count": round(weighted, 4),
        "crime_nearest_incident_miles": round(nearest, 3) if nearest is not None else None,
    }

def _fallback_density(
    *,
    lat: float,
    lng: float,
    city: str | None,
    county: str | None,
    is_red_zone: bool,
    radius_miles: float,
) -> dict[str, Any]:
    city_l = (city or "").strip().lower()
    county_l = (county or "").strip().lower()

    base = 1.35
    if county_l == "wayne":
        base += 1.0
    elif county_l in {"oakland", "macomb"}:
        base += 0.35

    if city_l == "detroit":
        base += 1.35
    elif city_l in {"pontiac", "inkster", "highland park"}:
        base += 0.75
    elif city_l in {"warren", "southfield"}:
        base += 0.30

    if is_red_zone:
        base += 1.75

    geo_variation = abs(math.sin(lat * 7.31) + math.cos(lng * 5.17)) * 0.45
    density = round(base + geo_variation, 4)
    area_sq_miles = _area_sq_miles(radius_miles)
    estimated_weighted = round(density * area_sq_miles, 4)

    return {
        "crime_density": density,
        "crime_incident_count": max(0, int(round(estimated_weighted / 1.6))),
        "crime_weighted_incident_count": estimated_weighted,
        "crime_nearest_incident_miles": None,
    }

def density_to_score(density: float) -> float:
    if density <= 0:
        return 0.0

    # Tuned to penalize clearly bad neighborhoods harder for investing use-cases.
    if density >= 8.0:
        return 97.0
    if density >= 6.0:
        return round(88.0 + min(9.0, (density - 6.0) * 4.0), 2)
    if density >= 4.0:
        return round(72.0 + (density - 4.0) * 8.0, 2)
    if density >= 2.5:
        return round(50.0 + (density - 2.5) * 14.5, 2)
    if density >= 1.25:
        return round(24.0 + (density - 1.25) * 20.8, 2)
    return round(max(4.0, density * 18.0), 2)

def neighborhood_investment_band(score: float) -> str:
    if score >= 85:
        return "avoid"
    if score >= 65:
        return "caution"
    if score >= 45:
        return "watch"
    if score >= 25:
        return "stable"
    return "preferred"

def crime_band(score: float) -> str:
    if score >= 80:
        return "very_high"
    if score >= 60:
        return "high"
    if score >= 35:
        return "moderate"
    if score >= 15:
        return "low"
    return "very_low"

def _crime_confidence(source: str, incident_count: int, weighted_incidents: float) -> float:
    if source != "local_dataset":
        return 0.45
    if incident_count >= 12 or weighted_incidents >= 18:
        return 0.95
    if incident_count >= 6 or weighted_incidents >= 9:
        return 0.85
    if incident_count >= 2 or weighted_incidents >= 3:
        return 0.72
    return 0.6

def compute_crime_metrics(
    *,
    lat: float,
    lng: float,
    city: str | None = None,
    county: str | None = None,
    is_red_zone: bool = False,
    radius_miles: float = DEFAULT_CRIME_RADIUS_MILES,
) -> dict[str, Any]:
    points = load_crime_points()
    radius_miles = max(0.25, float(radius_miles or DEFAULT_CRIME_RADIUS_MILES))
    area_sq_miles = round(_area_sq_miles(radius_miles), 4)

    if points:
        metrics = _estimate_from_points(lat, lng, points=points, radius_miles=radius_miles)
        source = "local_dataset"
        method = "weighted_radius_density"
        dataset_version = DEFAULT_DATASET_VERSION
    else:
        metrics = _fallback_density(
            lat=lat,
            lng=lng,
            city=city,
            county=county,
            is_red_zone=is_red_zone,
            radius_miles=radius_miles,
        )
        source = "heuristic_fallback"
        method = "fallback_market_heuristic"
        dataset_version = None

    density = float(metrics["crime_density"])
    score = density_to_score(density)
    incident_count = int(metrics["crime_incident_count"])
    weighted_incidents = float(metrics["crime_weighted_incident_count"])

    return {
        "crime_density": round(density, 4),
        "crime_score": round(score, 2),
        "crime_band": crime_band(score),
        "crime_source": source,
        "crime_method": method,
        "crime_radius_miles": radius_miles,
        "crime_area_sq_miles": area_sq_miles,
        "crime_area_type": DEFAULT_CRIME_AREA_TYPE,
        "crime_incident_count": incident_count,
        "crime_weighted_incident_count": round(weighted_incidents, 4),
        "crime_nearest_incident_miles": metrics.get("crime_nearest_incident_miles"),
        "crime_dataset_version": dataset_version,
        "crime_confidence": round(_crime_confidence(source, incident_count, weighted_incidents), 2),
        "investment_area_band": neighborhood_investment_band(score),
    }
