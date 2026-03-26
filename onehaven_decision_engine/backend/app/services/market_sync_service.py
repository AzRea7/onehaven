from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from ..config import settings
from ..db import SessionLocal
from .ingestion_scheduler_service import build_runtime_payload
from .ingestion_source_service import ensure_default_manual_sources, list_sources
from .market_catalog_service import (
    find_market_by_city,
    get_market,
    list_active_supported_markets,
    list_markets_by_tier,
)

"""
This service prepares supported-market sync plans.

Principles:
- frontend only chooses a supported market
- backend resolves that market from a single source of truth
- runtime payload is bounded to the market definition
- matching sources are reused from the same logic used by daily sync
"""


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_tier_limit(raw: str | None) -> str:
    value = _norm_text(raw)
    return value if value in {"hot", "warm", "cold"} else "all"


def get_daily_market_limit() -> int:
    return int(getattr(settings, "market_sync_daily_market_limit", 6) or 6)


def get_default_market_limit_per_sync() -> int:
    return int(getattr(settings, "market_sync_default_limit_per_market", 125) or 125)


def list_selected_daily_markets() -> list[dict[str, Any]]:
    tier = _normalize_tier_limit(
        getattr(settings, "market_sync_daily_tier_filter", "all")
    )

    if tier == "all":
        markets = list_active_supported_markets()
    else:
        markets = list_markets_by_tier(tier)

    return markets[: get_daily_market_limit()]


def build_market_runtime_payload(
    market: dict[str, Any],
    *,
    trigger_type: str = "daily_refresh",
) -> dict[str, Any]:
    return build_runtime_payload(
        state=str(market.get("state") or "MI"),
        county=str(market.get("county") or "") or None,
        city=str(market.get("city") or "") or None,
        limit=int(market.get("sync_limit") or get_default_market_limit_per_sync()),
        market_slug=str(market.get("slug") or "") or None,
        trigger_type=trigger_type,
        property_types=list(
            market.get("property_types") or ["single_family", "multi_family"]
        ),
        max_price=int(
            market.get("max_price")
            or getattr(settings, "investor_buy_box_max_price", 200_000)
        ),
        max_units=int(
            market.get("max_units")
            or getattr(settings, "investor_buy_box_max_units", 4)
        ),
    )


def get_enabled_sources_for_org(db: Session, *, org_id: int):
    ensure_default_manual_sources(db, org_id=int(org_id))
    return [
        source
        for source in list_sources(db, org_id=int(org_id))
        if bool(getattr(source, "is_enabled", False))
    ]


def _source_matches_market(source: Any, market: dict[str, Any]) -> bool:
    config = dict(getattr(source, "config_json", None) or {})

    source_market_slug = _norm_text(config.get("market_slug"))
    market_slug = _norm_text(market.get("slug"))
    if source_market_slug and market_slug and source_market_slug == market_slug:
        return True

    source_city = _norm_text(config.get("city"))
    market_city = _norm_text(market.get("city"))
    source_state = _norm_text(config.get("state") or "MI")
    market_state = _norm_text(market.get("state") or "MI")

    if (
        source_city
        and market_city
        and source_city == market_city
        and source_state == market_state
    ):
        return True

    source_slug = _norm_text(getattr(source, "slug", ""))
    if market_slug and market_slug in source_slug:
        return True

    return False


def _matching_sources_for_market(
    sources: list[Any],
    market: dict[str, Any],
) -> list[Any]:
    exact = [source for source in sources if _source_matches_market(source, market)]
    if exact:
        return exact

    return [
        source
        for source in sources
        if _norm_text(getattr(source, "provider", "")) == "rentcast"
    ]


def resolve_supported_market(
    *,
    market_slug: str | None = None,
    city: str | None = None,
    state: str = "MI",
) -> dict[str, Any] | None:
    if market_slug:
        return get_market(str(market_slug).strip().lower())
    if city:
        return find_market_by_city(city=city, state=state)
    return None


def build_daily_dispatch_plan(db: Session, *, org_id: int) -> list[dict[str, Any]]:
    markets = list_selected_daily_markets()
    sources = get_enabled_sources_for_org(db, org_id=int(org_id))

    dispatches: list[dict[str, Any]] = []
    for market in markets:
        runtime_config = build_market_runtime_payload(
            market,
            trigger_type="daily_refresh",
        )
        matched_sources = _matching_sources_for_market(sources, market)

        for source in matched_sources:
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


# def build_city_dispatch_plan(
#     db: Session,
#     *,
#     org_id: int,
#     city: str,
#     state: str = "MI",
# ) -> dict[str, Any]:
#     """
#     Compatibility function for market_sync_tasks.py.

#     The task module expects a city-scoped dispatch planner. We already have the
#     broader supported-market planner, so this function resolves the city to a
#     supported market and returns the same dispatch shape the task expects.
#     """
#     market = resolve_supported_market(city=city, state=state)

#     if market is None:
#         return {
#             "ok": False,
#             "covered": False,
#             "city": city,
#             "state": state,
#             "market": None,
#             "dispatches": [],
#         }

#     sources = get_enabled_sources_for_org(db, org_id=int(org_id))
#     matched_sources = _matching_sources_for_market(sources, market)

#     runtime_config = build_market_runtime_payload(
#         market,
#         trigger_type="manual_market_sync",
#     )

#     dispatches = [
#         {
#             "market": market,
#             "source_id": int(source.id),
#             "source_slug": str(getattr(source, "slug", "")),
#             "provider": str(getattr(source, "provider", "")),
#             "trigger_type": "manual_market_sync",
#             "runtime_config": dict(runtime_config),
#         }
#         for source in matched_sources
#     ]

#     return {
#         "ok": True,
#         "covered": True,
#         "city": market.get("city"),
#         "state": market.get("state"),
#         "market": market,
#         "dispatches": dispatches,
#     }


def build_supported_market_sync_plan_for_db(
    db: Session,
    *,
    org_id: int,
    market_slug: str | None = None,
    city: str | None = None,
    state: str = "MI",
) -> dict[str, Any]:
    market = resolve_supported_market(
        market_slug=market_slug,
        city=city,
        state=state,
    )

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
    runtime_config = build_market_runtime_payload(
        market,
        trigger_type="manual_market_sync",
    )

    matched_sources = _matching_sources_for_market(sources, market)

    dispatches = [
        {
            "market": market,
            "source_id": int(source.id),
            "source_slug": str(getattr(source, "slug", "")),
            "provider": str(getattr(source, "provider", "")),
            "trigger_type": "manual_market_sync",
            "runtime_config": dict(runtime_config),
        }
        for source in matched_sources
    ]

    return {
        "ok": True,
        "covered": True,
        "city": market.get("city"),
        "state": market.get("state"),
        "market": market,
        "dispatches": dispatches,
    }


def build_supported_market_sync_plan(
    *,
    org_id: int,
    market_slug: str | None = None,
    city: str | None = None,
    state: str = "MI",
) -> dict[str, Any]:
    db = SessionLocal()
    try:
        return build_supported_market_sync_plan_for_db(
            db,
            org_id=org_id,
            market_slug=market_slug,
            city=city,
            state=state,
        )
    finally:
        db.close()