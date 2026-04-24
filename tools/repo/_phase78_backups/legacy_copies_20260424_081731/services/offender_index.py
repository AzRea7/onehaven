# offender_index.py
from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "offenders"


@dataclass(frozen=True)
class OffenderPoint:
    lat: float
    lng: float


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
        DATA_DIR / "offenders.csv",
        DATA_DIR / "registry_points.csv",
        DATA_DIR / "offenders.json",
        DATA_DIR / "registry_points.json",
    ]


def _row_to_point(row: dict[str, Any]) -> OffenderPoint | None:
    lat = _safe_float(row.get("lat"))
    lng = _safe_float(row.get("lng"))
    if lat is None or lng is None:
        return None
    return OffenderPoint(lat=lat, lng=lng)


@lru_cache(maxsize=1)
def load_offender_points() -> tuple[OffenderPoint, ...]:
    points: list[OffenderPoint] = []

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


def _fallback_count(
    *,
    lat: float,
    lng: float,
    city: str | None,
    county: str | None,
    radius_miles: float,
) -> int:
    city_l = (city or "").strip().lower()
    county_l = (county or "").strip().lower()

    base = 0
    if county_l == "wayne":
        base += 2
    if city_l == "detroit":
        base += 2

    variation = int(abs(math.sin(lat * 11.17) + math.cos(lng * 8.91)) * 2.5)
    radius_factor = 2 if radius_miles >= 1.0 else 1
    return max(0, base + variation * radius_factor)


def offender_band(count: int) -> str:
    if count >= 10:
        return "very_high"
    if count >= 6:
        return "high"
    if count >= 3:
        return "moderate"
    if count >= 1:
        return "low"
    return "very_low"


def compute_offender_metrics(
    *,
    lat: float,
    lng: float,
    city: str | None = None,
    county: str | None = None,
    radius_miles: float = 1.0,
) -> dict[str, Any]:
    points = load_offender_points()

    if points:
        count = 0
        nearest = None
        for p in points:
            dist = _haversine_miles(lat, lng, p.lat, p.lng)
            if dist <= radius_miles:
                count += 1
            if nearest is None or dist < nearest:
                nearest = dist

        source = "local_dataset"
        nearest_miles = round(nearest, 3) if nearest is not None else None
    else:
        count = _fallback_count(
            lat=lat,
            lng=lng,
            city=city,
            county=county,
            radius_miles=radius_miles,
        )
        source = "heuristic_fallback"
        nearest_miles = None

    return {
        "offender_count": int(count),
        "offender_band": offender_band(int(count)),
        "offender_source": source,
        "offender_radius_miles": radius_miles,
        "nearest_offender_miles": nearest_miles,
    }