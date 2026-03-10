# crime index.py
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


@dataclass(frozen=True)
class CrimePoint:
    lat: float
    lng: float
    weight: float = 1.0
    category: str | None = None


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


def _row_to_point(row: dict[str, Any]) -> CrimePoint | None:
    lat = _safe_float(row.get("lat"))
    lng = _safe_float(row.get("lng"))
    if lat is None or lng is None:
        return None

    weight = _safe_float(row.get("weight"), 1.0) or 1.0
    category = row.get("category")
    return CrimePoint(lat=lat, lng=lng, weight=weight, category=str(category) if category else None)


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
                payload = payload.get("rows") or payload.get("points") or []

            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        point = _row_to_point(item)
                        if point is not None:
                            points.append(point)

    return tuple(points)


def _estimate_density_from_points(
    lat: float,
    lng: float,
    *,
    points: tuple[CrimePoint, ...],
    radius_miles: float = 1.0,
) -> float:
    if not points:
        return 0.0

    weighted = 0.0
    for p in points:
        dist = _haversine_miles(lat, lng, p.lat, p.lng)
        if dist <= radius_miles:
            # nearer incidents count more
            weighted += p.weight * (1.0 - (dist / radius_miles) * 0.6)

    area = math.pi * (radius_miles ** 2)
    return round(weighted / area, 4)


def _fallback_density(
    *,
    lat: float,
    lng: float,
    city: str | None,
    county: str | None,
    is_red_zone: bool,
) -> float:
    city_l = (city or "").strip().lower()
    county_l = (county or "").strip().lower()

    density = 1.0

    if county_l == "wayne":
        density += 0.7
    if city_l == "detroit":
        density += 1.0
    if is_red_zone:
        density += 1.5

    # add tiny deterministic geo variation so all houses do not look cloned
    density += abs(math.sin(lat * 7.31) + math.cos(lng * 5.17)) * 0.35
    return round(density, 4)


def density_to_score(density: float) -> float:
    if density <= 0:
        return 0.0

    # compress to a 0-100-ish band with diminishing returns
    score = 100.0 * (1.0 - math.exp(-density / 2.25))
    return round(min(max(score, 0.0), 100.0), 2)


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


def compute_crime_metrics(
    *,
    lat: float,
    lng: float,
    city: str | None = None,
    county: str | None = None,
    is_red_zone: bool = False,
) -> dict[str, Any]:
    points = load_crime_points()

    if points:
        density = _estimate_density_from_points(lat, lng, points=points, radius_miles=1.0)
        source = "local_dataset"
    else:
        density = _fallback_density(
            lat=lat,
            lng=lng,
            city=city,
            county=county,
            is_red_zone=is_red_zone,
        )
        source = "heuristic_fallback"

    score = density_to_score(density)

    return {
        "crime_density": round(density, 4),
        "crime_score": score,
        "crime_band": crime_band(score),
        "crime_source": source,
    }