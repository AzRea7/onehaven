# backend/app/services/market_sync_service.py
from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from ..config import settings
from .ingestion_scheduler_service import build_runtime_payload
from .ingestion_source_service import ensure_default_manual_sources, list_sources
from .market_catalog_service import (
    find_market_by_city,
    list_active_supported_markets,
    list_markets_by_tier,
)


"""
This service prepares market sync plans.

Why this exists:
- Keeps router logic thin
- Keeps Celery task logic thin
- Makes it easy to swap market catalog from code -> DB later
- Gives one place to implement scaling rules

Future scaling:
- Add per-market source preferences
- Add freshness / staleness based scheduling
- Add demand-based boosts from user searches or favorites
- Add org-specific market enablement
"""


def _normalize_tier_limit(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    return value if value in {"hot", "warm", "cold"} else "all"


def get_daily_market_limit() -> int:
    return int(getattr(settings, "market_sync_daily_market_limit", 6) or 6)


def get_default_market_limit_per_sync() -> int:
    return int(getattr(settings, "market_sync_default_limit_per_market", 250) or 250)


def list_selected_daily_markets() -> list[dict[str, Any]]:
    tier = _normalize_tier_limit(getattr(settings, "market_sync_daily_tier_filter", "all"))

    if tier == "all":
        markets = list_active_supported_markets()
    else:
        markets = list_markets_by_tier(tier)

    return markets[: get_daily_market_limit()]


def build_market_runtime_payload(market: dict[str, Any]) -> dict[str, Any]:
    return build_runtime_payload(
        state=str(market.get("state") or "MI"),
        county=str(market.get("county") or "") or None,
        city=str(market.get("city") or "") or None,
        limit=int(market.get("sync_limit") or get_default_market_limit_per_sync()),
    )


def get_enabled_sources_for_org(db: Session, *, org_id: int):
    ensure_default_manual_sources(db, org_id=int(org_id))
    return [
        source
        for source in list_sources(db, org_id=int(org_id))
        if bool(getattr(source, "is_enabled", False))
    ]


def build_daily_dispatch_plan(db: Session, *, org_id: int) -> list[dict[str, Any]]:
    markets = list_selected_daily_markets()
    sources = get_enabled_sources_for_org(db, org_id=int(org_id))

    dispatches: list[dict[str, Any]] = []
    for market in markets:
        runtime_config = build_market_runtime_payload(market)
        for source in sources:
            dispatches.append(
                {
                    "market": market,
                    "source_id": int(source.id),
                    "source_slug": str(getattr(source, "slug", "")),
                    "provider": str(getattr(source, "provider", "")),
                    "trigger_type": "daily_refresh",
                    "runtime_config": runtime_config,
                }
            )
    return dispatches


def build_city_dispatch_plan(
    db: Session,
    *,
    org_id: int,
    city: str,
    state: str = "MI",
) -> dict[str, Any]:
    market = find_market_by_city(city=city, state=state)
    if market is None:
        return {
            "ok": False,
            "covered": False,
            "city": city,
            "state": state,
            "market": None,
            "dispatches": [],
        }

    sources = get_enabled_sources_for_org(db, org_id=int(org_id))
    runtime_config = build_market_runtime_payload(market)

    dispatches = [
        {
            "market": market,
            "source_id": int(source.id),
            "source_slug": str(getattr(source, "slug", "")),
            "provider": str(getattr(source, "provider", "")),
            "trigger_type": "manual_market_sync",
            "runtime_config": runtime_config,
        }
        for source in sources
    ]

    return {
        "ok": True,
        "covered": True,
        "city": city,
        "state": state,
        "market": market,
        "dispatches": dispatches,
    }