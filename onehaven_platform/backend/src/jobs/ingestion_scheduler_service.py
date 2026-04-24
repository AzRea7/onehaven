from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import distinct, or_, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.middleware.structured_logging import emit_structured_log
from onehaven_platform.backend.src.models import IngestionSource, Property
from onehaven_platform.backend.src.policy_models import JurisdictionProfile
from onehaven_platform.backend.src.adapters.acquire_adapter import ensure_default_manual_sources, list_sources
from onehaven_platform.backend.src.adapters.compliance_adapter import (
    DEFAULT_JURISDICTION_STALE_DAYS,
    build_jurisdiction_refresh_payload as _build_jurisdiction_refresh_payload,
    list_jurisdictions_needing_refresh as _list_jurisdictions_needing_refresh,
)
from onehaven_platform.backend.src.services.locks_service import LockResult, acquire_lock, get_lock
from onehaven_platform.backend.src.adapters.intelligence_adapter import list_active_supported_markets

logger = logging.getLogger(__name__)

DEFAULT_DAILY_SYNC_LOCK_TTL_SECONDS = 60 * 60 * 2
DEFAULT_DISPATCH_DEDUPE_TTL_SECONDS = 60 * 60 * 36


def _utcnow() -> datetime:
    return datetime.utcnow()


def _emit(payload: dict[str, Any], level: int = logging.INFO) -> None:
    emit_structured_log("onehaven.scheduler", payload, level=level)


def build_lock_owner(*, prefix: str = "scheduler") -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"{prefix}:{host}:{pid}"


def list_default_daily_markets() -> list[dict[str, Any]]:
    markets = list_active_supported_markets()
    limit = int(getattr(settings, "market_sync_daily_market_limit", 6) or 6)
    return [dict(x) for x in markets[: max(1, limit)]]


def compute_next_daily_sync(now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    next_run = now.replace(hour=9, minute=10, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return next_run

def collapse_dispatches_to_primary_source(dispatches: list[dict]):
    seen = set()
    out = []

    for d in dispatches:
        slug = d["market"]["slug"]

        if slug in seen:
            continue

        seen.add(slug)
        out.append(d)

    return out

def build_runtime_payload(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    limit: int | None = None,
    market_slug: str | None = None,
    trigger_type: str = "daily_refresh",
    property_types: list[str] | tuple[str, ...] | None = None,
    max_price: int | None = None,
    max_units: int | None = None,
    market_cursor: dict[str, Any] | None = None,
    market_sync_state_id: int | None = None,
    market_sync_status: str | None = None,
    market_exhausted: bool | None = None,
    sync_mode: str | None = None,
    max_pages_budget: int | None = None,
    backfill_target_records: int | None = None,
    reset_market_cursor_on_start: bool | None = None,
    supported_market: bool | None = None,
    mark_backfill_complete_on_exhaustion: bool | None = None,
) -> dict[str, Any]:
    normalized_property_types = [
        str(x).strip()
        for x in (property_types or ["single_family", "multi_family"])
        if str(x).strip()
    ]

    payload = {
        "trigger_type": str(trigger_type or "daily_refresh"),
        "state": (state or "MI").strip().upper(),
        "limit": int(
            limit
            or getattr(settings, "market_sync_default_limit_per_market", 125)
            or 125
        ),
        "max_price": int(
            max_price
            or getattr(settings, "investor_buy_box_max_price", 200_000)
            or 200_000
        ),
        "max_units": int(
            max_units
            or getattr(settings, "investor_buy_box_max_units", 4)
            or 4
        ),
        "property_types": normalized_property_types or ["single_family", "multi_family"],
    }

    if county:
        payload["county"] = county.strip().lower()
    if city:
        payload["city"] = city.strip().lower()
    if market_slug:
        payload["market_slug"] = str(market_slug).strip().lower()
        payload["idempotency_context"] = {"market_slug": str(market_slug).strip().lower()}

    if isinstance(market_cursor, dict) and market_cursor:
        payload["market_cursor"] = dict(market_cursor)
    if market_sync_state_id is not None:
        payload["market_sync_state_id"] = int(market_sync_state_id)
    if market_sync_status:
        payload["market_sync_status"] = str(market_sync_status)
    if market_exhausted is not None:
        payload["market_exhausted"] = bool(market_exhausted)
    if sync_mode:
        payload["sync_mode"] = str(sync_mode).strip().lower()
    if max_pages_budget is not None:
        payload["max_pages_budget"] = int(max_pages_budget)
    if backfill_target_records is not None:
        payload["backfill_target_records"] = int(backfill_target_records)
    if reset_market_cursor_on_start is not None:
        payload["reset_market_cursor_on_start"] = bool(reset_market_cursor_on_start)
    if supported_market is not None:
        payload["supported_market"] = bool(supported_market)
    if mark_backfill_complete_on_exhaustion is not None:
        payload["mark_backfill_complete_on_exhaustion"] = bool(mark_backfill_complete_on_exhaustion)

    return payload


def build_location_refresh_payload(
    *,
    force: bool = False,
    batch_size: int | None = None,
) -> dict[str, Any]:
    payload = {
        "trigger_type": "location_refresh",
        "force": bool(force),
        "batch_size": int(batch_size or settings.location_refresh_batch_size),
    }
    return payload


def build_jurisdiction_refresh_payload(
    *,
    org_id: int | None,
    jurisdiction_profile_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    reason: str | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> dict[str, Any]:
    return _build_jurisdiction_refresh_payload(
        org_id=org_id,
        jurisdiction_profile_id=jurisdiction_profile_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reason=reason,
        force=force,
        stale_days=stale_days,
    )


def compute_stale_location_cutoff(now: datetime | None = None) -> datetime:
    now = now or _utcnow()
    return now - timedelta(hours=int(settings.geocode_stale_after_hours))


def list_properties_needing_location_refresh(
    db: Session,
    *,
    org_id: int,
    batch_size: int | None = None,
) -> list[Property]:
    batch = int(batch_size or settings.location_refresh_batch_size)
    cutoff = compute_stale_location_cutoff()

    stmt = (
        select(Property)
        .where(
            Property.org_id == int(org_id),
            or_(
                Property.lat.is_(None),
                Property.lng.is_(None),
                Property.normalized_address.is_(None),
                Property.geocode_last_refreshed.is_(None),
                Property.geocode_last_refreshed < cutoff,
            ),
        )
        .order_by(Property.id.asc())
        .limit(max(1, batch))
    )

    return list(db.scalars(stmt).all())


def list_jurisdictions_needing_refresh(
    db: Session,
    *,
    org_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> list[JurisdictionProfile]:
    targets = _list_jurisdictions_needing_refresh(
        db,
        org_id=org_id,
        batch_size=batch_size,
        stale_days=stale_days,
    )
    if not targets:
        return []

    ids = [int(t.jurisdiction_profile_id) for t in targets]
    stmt = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.id.in_(ids))
        .order_by(JurisdictionProfile.id.asc())
    )
    return list(db.scalars(stmt).all())


def daily_sync_lock_key(org_id: int) -> str:
    return f"daily_sync:{int(org_id)}"


def ingestion_run_lock_key(org_id: int, source_key: str) -> str:
    return f"ingestion_run:{int(org_id)}:{source_key}"


def property_enrichment_lock_key(org_id: int, property_id: int) -> str:
    return f"property_enrichment:{int(org_id)}:{int(property_id)}"


def _dispatch_dedupe_key(
    *,
    org_id: int,
    day_key: str,
    source_key: str,
    market: dict[str, Any],
    sync_mode: str | None = None,
) -> str:
    state = str(market.get("state") or "MI").strip().lower()
    county = str(market.get("county") or "").strip().lower()
    city = str(market.get("city") or "").strip().lower()
    market_slug = str(market.get("slug") or "").strip().lower()
    normalized_mode = str(sync_mode or "refresh").strip().lower()
    return (
        f"daily_sync_dispatch:{int(org_id)}:{day_key}:{source_key}:"
        f"{state}:{county}:{city}:{market_slug}:{normalized_mode}"
    )


def _source_key(source: Any) -> str:
    provider = str(getattr(source, "provider", "") or "").strip().lower()
    slug = str(getattr(source, "slug", "") or "").strip().lower()
    source_id = int(getattr(source, "id", 0) or 0)
    if provider and slug:
        return f"{provider}:{slug}"
    if provider:
        return f"{provider}:{source_id}"
    return str(source_id)


def build_scheduler_idempotency_context(
    *,
    org_id: int,
    source: Any,
    market: dict[str, Any],
    day_key: str,
    dispatch_key: str,
    sync_mode: str | None = None,
) -> dict[str, Any]:
    source_id = int(getattr(source, "id"))
    source_key = _source_key(source)
    return {
        "mode": "scheduler",
        "scope": "daily_sync",
        "sync_mode": str(sync_mode or "refresh").strip().lower(),
        "org_id": int(org_id),
        "source_id": source_id,
        "source_key": source_key,
        "schedule_day": str(day_key),
        "state": str(market.get("state") or "MI").strip(),
        "county": str(market.get("county") or "").strip(),
        "city": str(market.get("city") or "").strip(),
        "market_slug": str(market.get("slug") or "").strip(),
        "dispatch_key": str(dispatch_key),
    }


def list_org_ids_with_enabled_sources(db: Session) -> list[int]:
    stmt = (
        select(distinct(IngestionSource.org_id))
        .where(IngestionSource.is_enabled.is_(True))
        .order_by(IngestionSource.org_id.asc())
    )
    return [int(x) for x in db.scalars(stmt).all() if x is not None]


def get_daily_sync_lock_state(
    db: Session,
    *,
    org_id: int,
) -> LockResult:
    return get_lock(
        db,
        org_id=int(org_id),
        lock_key=daily_sync_lock_key(int(org_id)),
    )


def _source_matches_market(source: Any, market: dict[str, Any]) -> bool:
    config = dict(getattr(source, "config_json", None) or {})

    source_market_slug = str(config.get("market_slug") or "").strip().lower()
    market_slug = str(market.get("slug") or "").strip().lower()
    if source_market_slug and market_slug and source_market_slug == market_slug:
        return True

    source_city = str(config.get("city") or "").strip().lower()
    source_state = str(config.get("state") or "MI").strip().lower()
    market_city = str(market.get("city") or "").strip().lower()
    market_state = str(market.get("state") or "MI").strip().lower()

    if source_city and market_city and source_city == market_city and source_state == market_state:
        return True

    source_slug = str(getattr(source, "slug", "") or "").strip().lower()
    if market_slug and market_slug in source_slug:
        return True

    return False


def _matching_sources_for_market(
    sources: list[Any],
    market: dict[str, Any],
) -> list[Any]:
    exact = [s for s in sources if _source_matches_market(s, market)]
    if exact:
        return exact

    return [
        s for s in sources
        if str(getattr(s, "provider", "") or "").strip().lower() == "rentcast"
    ]





def _market_identity(market: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(market.get("slug") or "").strip().lower(),
        str(market.get("state") or "MI").strip().lower(),
        str(market.get("county") or "").strip().lower(),
        str(market.get("city") or "").strip().lower(),
    )


def _source_match_score(source: Any, market: dict[str, Any]) -> tuple[int, int, str]:
    config = dict(getattr(source, "config_json", None) or {})
    market_slug = str(market.get("slug") or "").strip().lower()
    market_city = str(market.get("city") or "").strip().lower()
    market_state = str(market.get("state") or "MI").strip().lower()
    source_slug = str(getattr(source, "slug", "") or "").strip().lower()
    source_market_slug = str(config.get("market_slug") or "").strip().lower()
    source_city = str(config.get("city") or "").strip().lower()
    source_state = str(config.get("state") or "MI").strip().lower()

    exact_market_slug = int(bool(source_market_slug and market_slug and source_market_slug == market_slug))
    exact_city = int(bool(source_city and market_city and source_city == market_city and source_state == market_state))
    slug_contains_market = int(bool(market_slug and market_slug in source_slug))
    provider_is_rentcast = int(str(getattr(source, "provider", "") or "").strip().lower() == "rentcast")
    source_id = int(getattr(source, "id", 0) or 0)

    # Higher is better for first 4 components; lower source_id wins tie.
    return (
        exact_market_slug * 100 + exact_city * 10 + slug_contains_market,
        provider_is_rentcast,
        -source_id,
    )


def pick_primary_source_for_market(
    sources: list[Any],
    market: dict[str, Any],
) -> Any | None:
    matched = _matching_sources_for_market(sources, market)
    if not matched:
        return None
    ranked = sorted(
        matched,
        key=lambda source: _source_match_score(source, market),
        reverse=True,
    )
    return ranked[0]


def collapse_dispatches_to_primary_source(
    dispatches: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> list[dict[str, Any]]:
    items = [dict(item) for item in (dispatches or []) if isinstance(item, dict)]
    if not items:
        return []

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for item in items:
        market = dict(item.get("market") or {})
        grouped.setdefault(_market_identity(market), []).append(item)

    collapsed: list[dict[str, Any]] = []
    for _, group in grouped.items():
        group = sorted(
            group,
            key=lambda item: (
                int(bool(str((item.get("runtime_config") or {}).get("market_slug") or "").strip())),
                int(bool(str(item.get("source_slug") or "").strip())),
                -int(item.get("source_id") or 0),
            ),
            reverse=True,
        )
        selected = dict(group[0])
        selected["dispatch_candidates"] = len(group)
        selected["dispatch_source_ids"] = [int(x.get("source_id") or 0) for x in group]
        collapsed.append(selected)
    return collapsed

def dispatch_daily_sync_for_org(
    db: Session,
    *,
    org_id: int,
    enqueue_sync: Callable[[int, int, str, dict[str, Any]], Any],
    sync_mode: str | None = "refresh",
) -> dict[str, Any]:
    ensure_default_manual_sources(db, org_id=int(org_id))
    sources = [
        s for s in list_sources(db, org_id=int(org_id))
        if bool(getattr(s, "is_enabled", False))
    ]
    markets = list_default_daily_markets()
    normalized_mode = str(sync_mode or "refresh").strip().lower()

    owner = build_lock_owner(prefix="daily_sync")
    lock = acquire_lock(
        db,
        org_id=int(org_id),
        lock_key=daily_sync_lock_key(int(org_id)),
        owner=owner,
        ttl_seconds=int(
            getattr(
                settings,
                "daily_sync_lock_ttl_seconds",
                DEFAULT_DAILY_SYNC_LOCK_TTL_SECONDS,
            )
        ),
    )
    if not lock.acquired:
        return {"ok": False, "reason": "daily_sync_lock_not_acquired", "org_id": int(org_id)}

    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    queued = 0
    results: list[dict[str, Any]] = []

    from onehaven_platform.backend.src.adapters.intelligence_adapter import build_market_runtime_payload, get_or_create_market_sync_state

    for market in markets:
        source = pick_primary_source_for_market(sources, market)
        if source is None:
            results.append(
                {
                    "market_slug": market.get("slug"),
                    "city": market.get("city"),
                    "county": market.get("county"),
                    "state": market.get("state"),
                    "sync_mode": normalized_mode,
                    "queued": False,
                    "reason": "no_matching_enabled_source",
                }
            )
            continue

        source_key = _source_key(source)
        dispatch_key = _dispatch_dedupe_key(
            org_id=int(org_id),
            day_key=day_key,
            source_key=source_key,
            market=market,
            sync_mode=normalized_mode,
        )

        acquire = acquire_lock(
            db,
            org_id=int(org_id),
            lock_key=dispatch_key,
            owner=owner,
            ttl_seconds=int(
                getattr(
                    settings,
                    "dispatch_dedupe_ttl_seconds",
                    DEFAULT_DISPATCH_DEDUPE_TTL_SECONDS,
                )
            ),
        )
        if not acquire.acquired:
            results.append(
                {
                    "market_slug": market.get("slug"),
                    "city": market.get("city"),
                    "county": market.get("county"),
                    "state": market.get("state"),
                    "sync_mode": normalized_mode,
                    "queued": False,
                    "reason": "dispatch_dedupe_lock_not_acquired",
                    "source_id": int(source.id),
                    "source_slug": getattr(source, "slug", None),
                }
            )
            continue

        sync_state = get_or_create_market_sync_state(
            db,
            org_id=int(org_id),
            source=source,
            market=market,
        )

        payload = build_market_runtime_payload(
            market,
            trigger_type="daily_refresh" if normalized_mode == "refresh" else "market_backfill",
            sync_state=sync_state,
            sync_mode=normalized_mode,
        )
        payload["idempotency_context"] = build_scheduler_idempotency_context(
            org_id=int(org_id),
            source=source,
            market=market,
            day_key=day_key,
            dispatch_key=dispatch_key,
            sync_mode=normalized_mode,
        )

        task = enqueue_sync(
            int(org_id),
            int(source.id),
            str(payload.get("trigger_type") or "daily_refresh"),
            payload,
        )
        queued += 1
        results.append(
            {
                "source_id": int(source.id),
                "source_slug": getattr(source, "slug", None),
                "provider": getattr(source, "provider", None),
                "market_slug": market.get("slug"),
                "city": market.get("city"),
                "county": market.get("county"),
                "state": market.get("state"),
                "sync_mode": normalized_mode,
                "market_sync_state_id": int(sync_state.id),
                "market_cursor": dict(payload.get("market_cursor") or {}),
                "task_id": str(getattr(task, "id", None)),
                "queued": True,
            }
        )

    return {
        "ok": True,
        "org_id": int(org_id),
        "sync_mode": normalized_mode,
        "queued": queued,
        "results": results,
        "markets": markets,
    }


def build_runtime_payload_from_saved_filters(filters_json: dict[str, Any] | None) -> dict[str, Any]:
    filters_json = dict(filters_json or {})
    payload = build_runtime_payload(
        state=filters_json.get("state"),
        county=filters_json.get("county"),
        city=filters_json.get("city"),
        limit=filters_json.get("limit"),
        market_slug=filters_json.get("market_slug"),
        trigger_type=filters_json.get("trigger_type") or "daily_refresh",
        property_types=filters_json.get("property_types"),
        max_price=filters_json.get("max_price"),
        max_units=filters_json.get("max_units"),
        market_cursor=filters_json.get("market_cursor"),
        market_sync_state_id=filters_json.get("market_sync_state_id"),
        market_sync_status=filters_json.get("market_sync_status"),
        market_exhausted=filters_json.get("market_exhausted"),
        sync_mode=filters_json.get("sync_mode"),
        max_pages_budget=filters_json.get("max_pages_budget"),
        backfill_target_records=filters_json.get("backfill_target_records"),
        reset_market_cursor_on_start=filters_json.get("reset_market_cursor_on_start"),
        supported_market=filters_json.get("supported_market"),
        mark_backfill_complete_on_exhaustion=filters_json.get("mark_backfill_complete_on_exhaustion"),
    )
    for key in [
        "min_price",
        "max_price",
        "min_bedrooms",
        "min_bathrooms",
        "q",
        "zip_code",
        "zip_codes",
    ]:
        if filters_json.get(key) is not None:
            payload[key] = filters_json.get(key)

    return payload
