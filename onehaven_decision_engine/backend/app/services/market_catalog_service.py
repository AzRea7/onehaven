# backend/app/services/market_catalog_service.py
from __future__ import annotations

from typing import Any


"""
This file is the central source of truth for market coverage.

Why this exists:
- Today: Southeast Michigan only
- Later: Michigan statewide
- Later: multi-state / national

Future scaling:
- Replace the in-code list with a DB table
- Add per-market source overrides
- Add market health / freshness metadata
- Add "hot / warm / cold" coverage tiers
"""


def list_supported_markets() -> list[dict[str, Any]]:
    return [
        # HOT markets: highest frequency, deepest enrichment
        {
            "slug": "detroit-wayne",
            "label": "Detroit / Wayne County",
            "state": "MI",
            "county": "wayne",
            "city": "detroit",
            "priority": 100,
            "coverage_tier": "hot",
            "is_active": True,
            "sync_limit": 250,
            "sync_every_hours": 24,
        },
        {
            "slug": "dearborn-wayne",
            "label": "Dearborn / Wayne County",
            "state": "MI",
            "county": "wayne",
            "city": "dearborn",
            "priority": 96,
            "coverage_tier": "hot",
            "is_active": True,
            "sync_limit": 250,
            "sync_every_hours": 24,
        },
        {
            "slug": "southfield-oakland",
            "label": "Southfield / Oakland County",
            "state": "MI",
            "county": "oakland",
            "city": "southfield",
            "priority": 92,
            "coverage_tier": "hot",
            "is_active": True,
            "sync_limit": 250,
            "sync_every_hours": 24,
        },
        {
            "slug": "pontiac-oakland",
            "label": "Pontiac / Oakland County",
            "state": "MI",
            "county": "oakland",
            "city": "pontiac",
            "priority": 86,
            "coverage_tier": "warm",
            "is_active": True,
            "sync_limit": 200,
            "sync_every_hours": 24,
        },
        {
            "slug": "warren-macomb",
            "label": "Warren / Macomb County",
            "state": "MI",
            "county": "macomb",
            "city": "warren",
            "priority": 90,
            "coverage_tier": "hot",
            "is_active": True,
            "sync_limit": 250,
            "sync_every_hours": 24,
        },
        {
            "slug": "sterling-heights-macomb",
            "label": "Sterling Heights / Macomb County",
            "state": "MI",
            "county": "macomb",
            "city": "sterling heights",
            "priority": 82,
            "coverage_tier": "warm",
            "is_active": True,
            "sync_limit": 200,
            "sync_every_hours": 24,
        },
        # Future expansion examples:
        # {
        #     "slug": "grand-rapids-kent",
        #     "label": "Grand Rapids / Kent County",
        #     "state": "MI",
        #     "county": "kent",
        #     "city": "grand rapids",
        #     "priority": 70,
        #     "coverage_tier": "warm",
        #     "is_active": False,
        #     "sync_limit": 150,
        #     "sync_every_hours": 24,
        # },
    ]


def normalize_market_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "slug": str(row.get("slug") or "").strip(),
        "label": str(row.get("label") or "").strip(),
        "state": str(row.get("state") or "MI").strip().upper(),
        "county": str(row.get("county") or "").strip().lower() or None,
        "city": str(row.get("city") or "").strip().lower() or None,
        "priority": int(row.get("priority", 0) or 0),
        "coverage_tier": str(row.get("coverage_tier") or "warm").strip().lower(),
        "is_active": bool(row.get("is_active", True)),
        "sync_limit": int(row.get("sync_limit", 250) or 250),
        "sync_every_hours": int(row.get("sync_every_hours", 24) or 24),
    }


def list_active_supported_markets() -> list[dict[str, Any]]:
    rows = [normalize_market_row(m) for m in list_supported_markets()]
    rows = [m for m in rows if bool(m.get("is_active"))]
    rows.sort(key=lambda m: (-int(m["priority"]), m["slug"]))
    return rows


def list_markets_by_tier(tier: str) -> list[dict[str, Any]]:
    wanted = str(tier or "").strip().lower()
    return [m for m in list_active_supported_markets() if m["coverage_tier"] == wanted]


def find_market_by_city(city: str | None, state: str | None = "MI") -> dict[str, Any] | None:
    city_norm = str(city or "").strip().lower()
    state_norm = str(state or "MI").strip().upper()

    if not city_norm:
        return None

    for market in list_active_supported_markets():
        if market["state"] != state_norm:
            continue
        if market["city"] == city_norm:
            return market
    return None


def is_supported_city(city: str | None, state: str | None = "MI") -> bool:
    return find_market_by_city(city=city, state=state) is not None
