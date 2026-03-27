from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _norm_county(value: Any) -> str:
    county = _norm_text(value)
    if county.endswith(" county"):
        county = county[:-7].strip()
    return county


def _norm_property_types(values: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    for value in values or ():
        raw = _norm_text(value).replace("-", " ").replace("_", " ").strip()
        if raw in {"single family", "single family home", "house", "sfh", "residential"}:
            out.append("single_family")
        elif raw in {
            "multi family",
            "multifamily",
            "duplex",
            "triplex",
            "fourplex",
            "2 family",
            "3 family",
            "4 family",
        }:
            out.append("multi_family")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return tuple(deduped)


@dataclass(frozen=True)
class MarketDefinition:
    slug: str
    label: str
    state: str
    county: str | None
    city: str | None
    zip_codes: tuple[str, ...] = ()
    coverage_tier: str = "warm"
    priority: int = 50
    is_active: bool = True
    sync_limit: int = 125
    sync_every_hours: int = 24

    min_price: int | None = None
    max_price: int | None = 200_000
    property_types: tuple[str, ...] = ("single_family", "multi_family")

    max_units: int | None = 4
    notes: str | None = None


_STATIC_MARKETS: tuple[MarketDefinition, ...] = (
    MarketDefinition(
        slug="detroit-wayne",
        label="Detroit / Wayne County",
        state="MI",
        county="wayne",
        city="detroit",
        zip_codes=("48204", "48205", "48206", "48209", "48210", "48213", "48219", "48221", "48224", "48227", "48228", "48235", "48238"),
        coverage_tier="hot",
        priority=100,
        sync_limit=175,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
    MarketDefinition(
        slug="dearborn-wayne",
        label="Dearborn / Wayne County",
        state="MI",
        county="wayne",
        city="dearborn",
        zip_codes=("48120", "48124", "48126", "48127", "48128"),
        coverage_tier="hot",
        priority=96,
        sync_limit=110,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
    MarketDefinition(
        slug="southfield-oakland",
        label="Southfield / Oakland County",
        state="MI",
        county="oakland",
        city="southfield",
        zip_codes=("48033", "48034", "48075", "48076"),
        coverage_tier="hot",
        priority=92,
        sync_limit=100,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
    MarketDefinition(
        slug="pontiac-oakland",
        label="Pontiac / Oakland County",
        state="MI",
        county="oakland",
        city="pontiac",
        zip_codes=("48340", "48341", "48342"),
        coverage_tier="warm",
        priority=86,
        sync_limit=85,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
    MarketDefinition(
        slug="warren-macomb",
        label="Warren / Macomb County",
        state="MI",
        county="macomb",
        city="warren",
        zip_codes=("48088", "48089", "48091", "48092", "48093"),
        coverage_tier="hot",
        priority=90,
        sync_limit=100,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
    MarketDefinition(
        slug="sterling-heights-macomb",
        label="Sterling Heights / Macomb County",
        state="MI",
        county="macomb",
        city="sterling heights",
        zip_codes=("48310", "48312", "48313", "48314"),
        coverage_tier="warm",
        priority=82,
        sync_limit=75,
        sync_every_hours=24,
        max_price=200_000,
        property_types=("single_family", "multi_family"),
    ),
)


def _serialize_market(market: MarketDefinition) -> dict[str, Any]:
    payload = asdict(market)
    payload["state"] = str(payload.get("state") or "MI").strip().upper()
    payload["county"] = _norm_county(payload.get("county")) or None
    payload["city"] = _norm_text(payload.get("city")) or None
    payload["coverage_tier"] = _norm_text(payload.get("coverage_tier")) or "warm"
    payload["property_types"] = list(_norm_property_types(payload.get("property_types")))
    payload["zip_codes"] = [str(z).strip() for z in (payload.get("zip_codes") or []) if str(z).strip()]
    return payload


def all_markets() -> list[dict[str, Any]]:
    return [_serialize_market(x) for x in _STATIC_MARKETS]


def get_market(slug: str) -> dict[str, Any] | None:
    wanted = _norm_text(slug)
    for market in _STATIC_MARKETS:
        if market.slug == wanted:
            return _serialize_market(market)
    return None


def get_active_supported_market_by_slug(slug: str) -> dict[str, Any] | None:
    wanted = _norm_text(slug)
    if not wanted:
        return None
    for market in _STATIC_MARKETS:
        if _norm_text(market.slug) != wanted:
            continue
        if not bool(market.is_active):
            return None
        return _serialize_market(market)
    return None


def list_supported_markets(
    *,
    tier_filter: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    tier = _norm_text(tier_filter)
    out: list[dict[str, Any]] = []

    for market in _STATIC_MARKETS:
        if active_only and not market.is_active:
            continue
        if tier and tier != "all" and _norm_text(market.coverage_tier) != tier:
            continue
        out.append(_serialize_market(market))

    out.sort(key=lambda m: (-int(m["priority"]), str(m["label"])))
    return out


def list_active_supported_markets() -> list[dict[str, Any]]:
    return list_supported_markets(tier_filter="all", active_only=True)


def list_markets_by_tier(tier: str) -> list[dict[str, Any]]:
    normalized = _norm_text(tier)
    return list_supported_markets(tier_filter=normalized or "all", active_only=True)


def find_market_by_city(*, city: str, state: str = "MI") -> Optional[dict[str, Any]]:
    wanted_city = _norm_text(city)
    wanted_state = _norm_text(state or "MI")

    if not wanted_city:
        return None

    for market in _STATIC_MARKETS:
        if not bool(market.is_active):
            continue
        if _norm_text(market.state) != wanted_state:
            continue
        if _norm_text(market.city) == wanted_city:
            return _serialize_market(market)

    return None


def markets_for_daily_sync(
    *,
    daily_market_limit: int,
    tier_filter: str | None = None,
) -> list[dict[str, Any]]:
    markets = list_supported_markets(
        tier_filter=tier_filter or "all",
        active_only=True,
    )
    return markets[: max(1, int(daily_market_limit))]
