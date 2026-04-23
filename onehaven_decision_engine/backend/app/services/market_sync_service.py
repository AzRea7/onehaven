from __future__ import annotations

from datetime import datetime
import hashlib
import json
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import SessionLocal, rollback_quietly
from app.models import MarketSyncState
from app.services.ingestion_scheduler_service import build_runtime_payload
from .ingestion_source_service import (
    ensure_default_manual_sources,
    ensure_market_slug_on_sources,
    ensure_sources_for_supported_markets,
    list_sources,
    resolve_sources_for_market,
)
from .market_catalog_service import (
    canonical_source_slug_for_market_slug,
    get_active_supported_market_by_slug,
    list_active_supported_markets,
    list_markets_by_tier,
)

MarketSyncMode = Literal["refresh", "backfill"]

DEFAULT_CURSOR_SORT_MODE = "newest"
DEFAULT_CURSOR_PAGE = 1
DEFAULT_CURSOR_SHARD = 1
DEFAULT_REFRESH_WINDOW_PAGES = 2

DEFAULT_REFRESH_MAX_PAGES_BUDGET = 3
DEFAULT_BACKFILL_MAX_PAGES_BUDGET = 12

DEFAULT_REFRESH_LIMIT_FALLBACK = 125
DEFAULT_BACKFILL_LIMIT_FALLBACK = 250
DEFAULT_BACKFILL_TARGET_RECORDS_FALLBACK = 500


def _utcnow() -> datetime:
    return datetime.utcnow()


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except Exception:
        return default


def _safe_optional_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        parsed = int(value)
        return parsed if parsed > 0 else None
    except Exception:
        return None


def _stable_json_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _normalize_tier_limit(raw: str | None) -> str:
    value = _norm_text(raw)
    return value if value in {"hot", "warm", "cold"} else "all"


def _coerce_zip_codes(value: Any) -> list[str]:
    if value is None:
        return []

    raw_items: list[Any]
    if isinstance(value, str):
        raw_items = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [value]

    out: list[str] = []
    seen: set[str] = set()

    for item in raw_items:
        zip_code = str(item or "").strip()
        if not zip_code:
            continue
        if zip_code in seen:
            continue
        seen.add(zip_code)
        out.append(zip_code)

    return out


def normalize_sync_mode(sync_mode: str | None) -> MarketSyncMode:
    value = _norm_text(sync_mode)
    return "backfill" if value == "backfill" else "refresh"


def get_daily_market_limit() -> int:
    return int(getattr(settings, "market_sync_daily_market_limit", 6) or 6)


def get_default_market_limit_per_sync() -> int:
    return int(
        getattr(settings, "market_sync_default_limit_per_market", DEFAULT_REFRESH_LIMIT_FALLBACK)
        or DEFAULT_REFRESH_LIMIT_FALLBACK
    )


def get_default_backfill_limit_per_sync() -> int:
    return int(
        getattr(settings, "market_sync_backfill_limit_per_market", DEFAULT_BACKFILL_LIMIT_FALLBACK)
        or DEFAULT_BACKFILL_LIMIT_FALLBACK
    )


def get_default_backfill_target_records() -> int:
    return int(
        getattr(settings, "market_sync_backfill_target_records", DEFAULT_BACKFILL_TARGET_RECORDS_FALLBACK)
        or DEFAULT_BACKFILL_TARGET_RECORDS_FALLBACK
    )


def get_default_refresh_window_pages() -> int:
    return int(
        getattr(settings, "market_sync_refresh_window_pages", DEFAULT_REFRESH_WINDOW_PAGES)
        or DEFAULT_REFRESH_WINDOW_PAGES
    )


def get_default_refresh_max_pages_budget() -> int:
    return int(
        getattr(settings, "market_sync_refresh_max_pages_budget", DEFAULT_REFRESH_MAX_PAGES_BUDGET)
        or DEFAULT_REFRESH_MAX_PAGES_BUDGET
    )


def get_default_backfill_max_pages_budget() -> int:
    return int(
        getattr(settings, "market_sync_backfill_max_pages_budget", DEFAULT_BACKFILL_MAX_PAGES_BUDGET)
        or DEFAULT_BACKFILL_MAX_PAGES_BUDGET
    )


def _default_cursor(*, market_slug: str | None = None) -> dict[str, Any]:
    return {
        "market_slug": str(market_slug or "").strip().lower() or None,
        "page": DEFAULT_CURSOR_PAGE,
        "shard": DEFAULT_CURSOR_SHARD,
        "sort_mode": DEFAULT_CURSOR_SORT_MODE,
        "refresh_window_pages": get_default_refresh_window_pages(),
        "page_fingerprint": None,
        "page_changed": True,
        "provider_cursor": None,
    }


def normalize_market_cursor(
    cursor: dict[str, Any] | None,
    *,
    market_slug: str | None = None,
) -> dict[str, Any]:
    payload = dict(_default_cursor(market_slug=market_slug))
    raw = dict(cursor or {})

    payload["market_slug"] = str(
        raw.get("market_slug") or payload.get("market_slug") or market_slug or ""
    ).strip().lower() or None
    payload["page"] = _safe_int(raw.get("page"), DEFAULT_CURSOR_PAGE)
    payload["shard"] = _safe_int(raw.get("shard"), DEFAULT_CURSOR_SHARD)
    payload["sort_mode"] = (
        str(raw.get("sort_mode") or DEFAULT_CURSOR_SORT_MODE).strip().lower()
        or DEFAULT_CURSOR_SORT_MODE
    )
    payload["refresh_window_pages"] = _safe_int(
        raw.get("refresh_window_pages"),
        get_default_refresh_window_pages(),
    )
    payload["page_fingerprint"] = str(raw.get("page_fingerprint") or "").strip() or None
    payload["page_changed"] = bool(raw.get("page_changed", True))

    provider_cursor = raw.get("provider_cursor")
    if isinstance(provider_cursor, dict) and provider_cursor:
        payload["provider_cursor"] = dict(provider_cursor)
    else:
        payload["provider_cursor"] = None

    return payload


def build_market_dataset_key(
    *,
    org_id: int,
    provider: str,
    source_id: int,
    market_slug: str | None,
    page: int | None,
    shard: int | None,
    sort_mode: str | None,
    cursor_json: dict[str, Any] | None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
) -> str:
    normalized_cursor = normalize_market_cursor(
        dict(cursor_json or {}),
        market_slug=market_slug,
    )
    payload = {
        "org_id": int(org_id),
        "provider": str(provider or "").strip().lower(),
        "source_id": int(source_id),
        "market_slug": str(
            market_slug or normalized_cursor.get("market_slug") or ""
        ).strip().lower() or None,
        "state": str(state or "").strip().upper() or None,
        "county": str(county or "").strip().lower() or None,
        "city": str(city or "").strip().lower() or None,
        "page": _safe_int(
            page if page is not None else normalized_cursor.get("page"),
            DEFAULT_CURSOR_PAGE,
        ),
        "shard": _safe_int(
            shard if shard is not None else normalized_cursor.get("shard"),
            DEFAULT_CURSOR_SHARD,
        ),
        "sort_mode": (
            str(sort_mode or normalized_cursor.get("sort_mode") or DEFAULT_CURSOR_SORT_MODE)
            .strip()
            .lower()
            or DEFAULT_CURSOR_SORT_MODE
        ),
        "provider_cursor": (
            dict(normalized_cursor.get("provider_cursor") or {})
            if isinstance(normalized_cursor.get("provider_cursor"), dict)
            else None
        ),
        "page_fingerprint": str(normalized_cursor.get("page_fingerprint") or "").strip() or None,
    }
    return _stable_json_hash(payload)


def build_market_dataset_identity(
    *,
    org_id: int,
    source: Any,
    runtime_config: dict[str, Any] | None,
    sync_state: MarketSyncState | None = None,
    cursor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_config = dict(runtime_config or {})
    base_cursor = normalize_market_cursor(
        dict(cursor or runtime_config.get("market_cursor") or {}),
        market_slug=runtime_config.get("market_slug") or (sync_state.market_slug if sync_state is not None else None),
    )

    market_slug = (
        str(
            runtime_config.get("market_slug")
            or base_cursor.get("market_slug")
            or (sync_state.market_slug if sync_state is not None else "")
        ).strip().lower()
        or None
    )
    state = str(
        runtime_config.get("state")
        or (sync_state.state if sync_state is not None else "")
        or "MI"
    ).strip().upper()
    county = str(
        runtime_config.get("county")
        or (sync_state.county if sync_state is not None else "")
        or ""
    ).strip().lower() or None
    city = str(
        runtime_config.get("city")
        or (sync_state.city if sync_state is not None else "")
        or ""
    ).strip().lower() or None
    provider = str(getattr(source, "provider", "") or "").strip().lower()
    source_id = int(getattr(source, "id"))

    page = _safe_int(base_cursor.get("page"), DEFAULT_CURSOR_PAGE)
    shard = _safe_int(base_cursor.get("shard"), DEFAULT_CURSOR_SHARD)
    sort_mode = str(base_cursor.get("sort_mode") or DEFAULT_CURSOR_SORT_MODE).strip().lower() or DEFAULT_CURSOR_SORT_MODE

    dataset_key = build_market_dataset_key(
        org_id=int(org_id),
        provider=provider,
        source_id=source_id,
        market_slug=market_slug,
        page=page,
        shard=shard,
        sort_mode=sort_mode,
        cursor_json=base_cursor,
        state=state,
        county=county,
        city=city,
    )

    return {
        "dataset_key": dataset_key,
        "provider": provider,
        "source_id": source_id,
        "market_slug": market_slug,
        "state": state,
        "county": county,
        "city": city,
        "page": page,
        "shard": shard,
        "sort_mode": sort_mode,
        "cursor": base_cursor,
    }


def build_fresh_backfill_cursor(*, market_slug: str | None) -> dict[str, Any]:
    return normalize_market_cursor(
        {
            "market_slug": market_slug,
            "page": 1,
            "shard": 1,
            "sort_mode": DEFAULT_CURSOR_SORT_MODE,
            "refresh_window_pages": get_default_refresh_window_pages(),
            "page_fingerprint": None,
            "page_changed": True,
            "provider_cursor": None,
        },
        market_slug=market_slug,
    )


def build_fresh_refresh_cursor(*, market_slug: str | None) -> dict[str, Any]:
    return normalize_market_cursor(
        {
            "market_slug": market_slug,
            "page": 1,
            "shard": 1,
            "sort_mode": DEFAULT_CURSOR_SORT_MODE,
            "refresh_window_pages": get_default_refresh_window_pages(),
            "page_fingerprint": None,
            "page_changed": True,
            "provider_cursor": None,
            "refresh_resume": True,
        },
        market_slug=market_slug,
    )


def get_resume_cursor(sync_state: MarketSyncState | None) -> dict[str, Any]:
    if sync_state is None:
        return build_fresh_refresh_cursor(market_slug=None)

    saved = normalize_market_cursor(
        dict(sync_state.cursor_json or {}),
        market_slug=sync_state.market_slug,
    )

    last_page = _safe_int(
        sync_state.last_page if sync_state.last_page is not None else saved.get("page"),
        DEFAULT_CURSOR_PAGE,
    )
    refresh_window_pages = _safe_int(
        saved.get("refresh_window_pages"),
        get_default_refresh_window_pages(),
    )

    if bool(sync_state.market_exhausted):
        resume_page = 1
    else:
        resume_page = max(1, last_page - refresh_window_pages + 1)

    saved["page"] = resume_page
    saved["last_page"] = last_page
    saved["market_exhausted"] = bool(sync_state.market_exhausted)
    saved["status"] = str(sync_state.status or "idle")
    saved["refresh_resume"] = True

    return saved




def _state_write(db: Session, fn):
    try:
        return fn()
    except Exception:
        rollback_quietly(db)
        raise

def mark_backfill_completed(
    db: Session,
    *,
    sync_state: MarketSyncState,
    completed_at: datetime | None = None,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.backfill_completed_at = completed_at or _utcnow()
        sync_state.updated_at = _utcnow()
        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def clear_backfill_completed(
    db: Session,
    *,
    sync_state: MarketSyncState,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.backfill_completed_at = None
        sync_state.updated_at = _utcnow()
        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def list_selected_daily_markets() -> list[dict[str, Any]]:
    tier = _normalize_tier_limit(
        getattr(settings, "market_sync_daily_tier_filter", "all")
    )

    if tier == "all":
        markets = list_active_supported_markets()
    else:
        markets = list_markets_by_tier(tier)

    return markets[: get_daily_market_limit()]


def get_market_sync_state(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    market_slug: str,
) -> MarketSyncState | None:
    stmt = select(MarketSyncState).where(
        MarketSyncState.org_id == int(org_id),
        MarketSyncState.source_id == int(source_id),
        MarketSyncState.market_slug == str(market_slug).strip().lower(),
    )
    return db.scalar(stmt)


def get_market_sync_state_by_id(
    db: Session,
    *,
    sync_state_id: int | None,
    org_id: int | None = None,
) -> MarketSyncState | None:
    if not sync_state_id:
        return None

    stmt = select(MarketSyncState).where(MarketSyncState.id == int(sync_state_id))
    if org_id is not None:
        stmt = stmt.where(MarketSyncState.org_id == int(org_id))
    return db.scalar(stmt)


def get_or_create_market_sync_state(
    db: Session,
    *,
    org_id: int,
    source: Any,
    market: dict[str, Any],
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        market_slug = str(market.get("slug") or "").strip().lower()
        existing = get_market_sync_state(
            db,
            org_id=int(org_id),
            source_id=int(getattr(source, "id")),
            market_slug=market_slug,
        )
        if existing is not None:
            if not existing.cursor_json:
                existing.cursor_json = normalize_market_cursor(None, market_slug=market_slug)
                db.add(existing)
                db.flush()
            return existing

        row = MarketSyncState(
            org_id=int(org_id),
            source_id=int(getattr(source, "id")),
            provider=str(getattr(source, "provider", "") or "").strip().lower(),
            market_slug=market_slug,
            state=str(market.get("state") or "MI").strip().upper(),
            county=str(market.get("county") or "").strip().lower() or None,
            city=str(market.get("city") or "").strip().lower() or None,
            status="idle",
            cursor_json=normalize_market_cursor(None, market_slug=market_slug),
            last_page=1,
            last_shard=1,
            last_sort_mode=DEFAULT_CURSOR_SORT_MODE,
            market_exhausted=False,
        )
        db.add(row)
        db.flush()
        return row



    return _state_write(db, _op)
def mark_market_sync_started(
    db: Session,
    *,
    sync_state: MarketSyncState,
    requested_limit: int | None = None,
    status: str = "running",
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.status = str(status or "running")
        sync_state.last_requested_limit = (
            int(requested_limit) if requested_limit is not None else sync_state.last_requested_limit
        )
        sync_state.last_sync_started_at = _utcnow()
        sync_state.updated_at = _utcnow()
        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def mark_market_sync_completed(
    db: Session,
    *,
    sync_state: MarketSyncState,
    market_exhausted: bool | None = None,
    seen_provider_record_at: datetime | None = None,
    status: str = "idle",
    sync_mode: str | None = None,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.status = str(status or "idle")
        sync_state.last_sync_completed_at = _utcnow()
        sync_state.updated_at = _utcnow()

        normalized_mode = normalize_sync_mode(sync_mode)
        if market_exhausted is not None:
            if normalized_mode == "refresh":
                sync_state.market_exhausted = False
            else:
                sync_state.market_exhausted = bool(market_exhausted)

        if seen_provider_record_at is not None:
            sync_state.last_seen_provider_record_at = seen_provider_record_at

        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def advance_market_cursor(
    db: Session,
    *,
    sync_state: MarketSyncState,
    next_cursor: dict[str, Any] | None,
    page_scanned: int | None = None,
    shard_scanned: int | None = None,
    sort_mode: str | None = None,
    page_fingerprint: str | None = None,
    page_changed: bool | None = None,
    exhausted: bool | None = None,
    seen_provider_record_at: datetime | None = None,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        merged = normalize_market_cursor(
            {
                **dict(sync_state.cursor_json or {}),
                **dict(next_cursor or {}),
                "page_fingerprint": page_fingerprint
                or dict(next_cursor or {}).get("page_fingerprint"),
                "page_changed": (
                    bool(page_changed)
                    if page_changed is not None
                    else dict(next_cursor or {}).get("page_changed", True)
                ),
                "sort_mode": sort_mode
                or dict(next_cursor or {}).get("sort_mode")
                or sync_state.last_sort_mode
                or DEFAULT_CURSOR_SORT_MODE,
            },
            market_slug=sync_state.market_slug,
        )

        sync_state.cursor_json = merged
        sync_state.last_page = _safe_int(
            page_scanned if page_scanned is not None else merged.get("page"),
            DEFAULT_CURSOR_PAGE,
        )
        sync_state.last_shard = _safe_int(
            shard_scanned if shard_scanned is not None else merged.get("shard"),
            DEFAULT_CURSOR_SHARD,
        )
        sync_state.last_sort_mode = str(merged.get("sort_mode") or DEFAULT_CURSOR_SORT_MODE)
        sync_state.last_page_fingerprint = (
            str(page_fingerprint or merged.get("page_fingerprint") or "").strip() or None
        )
        sync_state.market_exhausted = (
            bool(exhausted) if exhausted is not None else bool(sync_state.market_exhausted)
        )
        sync_state.updated_at = _utcnow()

        if seen_provider_record_at is not None:
            sync_state.last_seen_provider_record_at = seen_provider_record_at

        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def reset_market_cursor(
    db: Session,
    *,
    sync_state: MarketSyncState,
    page: int = 1,
    shard: int = 1,
    sort_mode: str = DEFAULT_CURSOR_SORT_MODE,
    clear_exhausted: bool = True,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.cursor_json = normalize_market_cursor(
            {
                "market_slug": sync_state.market_slug,
                "page": max(1, int(page)),
                "shard": max(1, int(shard)),
                "sort_mode": (
                    str(sort_mode or DEFAULT_CURSOR_SORT_MODE).strip().lower()
                    or DEFAULT_CURSOR_SORT_MODE
                ),
                "refresh_window_pages": get_default_refresh_window_pages(),
                "page_fingerprint": None,
                "page_changed": True,
                "provider_cursor": None,
            },
            market_slug=sync_state.market_slug,
        )
        sync_state.last_page = max(1, int(page))
        sync_state.last_shard = max(1, int(shard))
        sync_state.last_sort_mode = (
            str(sort_mode or DEFAULT_CURSOR_SORT_MODE).strip().lower()
            or DEFAULT_CURSOR_SORT_MODE
        )
        sync_state.last_page_fingerprint = None
        sync_state.updated_at = _utcnow()

        if clear_exhausted:
            sync_state.market_exhausted = False

        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def mark_market_exhausted(
    db: Session,
    *,
    sync_state: MarketSyncState,
    exhausted: bool = True,
) -> MarketSyncState:
    def _op() -> MarketSyncState:
        sync_state.market_exhausted = bool(exhausted)
        sync_state.updated_at = _utcnow()
        db.add(sync_state)
        db.flush()
        return sync_state



    return _state_write(db, _op)
def get_sync_mode_runtime_overrides(
    *,
    sync_mode: str | None,
    market: dict[str, Any],
    sync_state: MarketSyncState | None,
    limit_override: int | None = None,
    trigger_type: str | None = None,
) -> dict[str, Any]:
    normalized_mode = normalize_sync_mode(sync_mode)
    normalized_trigger = _norm_text(trigger_type)

    if normalized_mode == "backfill":
        market_slug = str(market.get("slug") or "").strip().lower() or None
        limit = _safe_int(
            limit_override
            or market.get("backfill_limit")
            or get_default_backfill_limit_per_sync(),
            get_default_backfill_limit_per_sync(),
        )
        max_pages_budget = _safe_int(
            market.get("backfill_max_pages_budget")
            or get_default_backfill_max_pages_budget(),
            get_default_backfill_max_pages_budget(),
        )
        backfill_target_records = _safe_int(
            market.get("backfill_target_records")
            or get_default_backfill_target_records(),
            get_default_backfill_target_records(),
        )

        return {
            "sync_mode": "backfill",
            "limit": limit,
            "market_cursor": build_fresh_backfill_cursor(market_slug=market_slug),
            "reset_market_cursor_on_start": True,
            "max_pages_budget": max_pages_budget,
            "backfill_target_records": backfill_target_records,
            "mark_backfill_complete_on_exhaustion": True,
        }

    if normalized_trigger == "manual_market_sync":
        market_slug = str(market.get("slug") or "").strip().lower() or None
        limit = _safe_int(
            limit_override
            or market.get("backfill_limit")
            or market.get("sync_limit")
            or get_default_backfill_limit_per_sync(),
            get_default_backfill_limit_per_sync(),
        )
        max_pages_budget = _safe_int(
            market.get("manual_max_pages_budget")
            or market.get("backfill_max_pages_budget")
            or get_default_backfill_max_pages_budget(),
            get_default_backfill_max_pages_budget(),
        )

        return {
            "sync_mode": "refresh",
            "limit": limit,
            "market_cursor": build_fresh_refresh_cursor(market_slug=market_slug),
            "reset_market_cursor_on_start": True,
            "max_pages_budget": max_pages_budget,
            "backfill_target_records": None,
            "mark_backfill_complete_on_exhaustion": False,
        }

    limit = _safe_int(
        limit_override
        or market.get("sync_limit")
        or get_default_market_limit_per_sync(),
        get_default_market_LIMIT_per_sync() if False else get_default_market_limit_per_sync(),
    )
    max_pages_budget = _safe_int(
        market.get("refresh_max_pages_budget")
        or get_default_refresh_max_pages_budget(),
        get_default_refresh_max_pages_budget(),
    )

    return {
        "sync_mode": "refresh",
        "limit": limit,
        "market_cursor": get_resume_cursor(sync_state),
        "reset_market_cursor_on_start": False,
        "max_pages_budget": max_pages_budget,
        "backfill_target_records": None,
        "mark_backfill_complete_on_exhaustion": False,
    }


def build_market_runtime_payload(
    market: dict[str, Any],
    *,
    trigger_type: str = "daily_refresh",
    sync_state: MarketSyncState | None = None,
    limit_override: int | None = None,
    sync_mode: str | None = "refresh",
) -> dict[str, Any]:
    mode_overrides = get_sync_mode_runtime_overrides(
        sync_mode=sync_mode,
        market=market,
        sync_state=sync_state,
        limit_override=limit_override,
        trigger_type=trigger_type,
    )

    payload = build_runtime_payload(
        state=str(market.get("state") or "MI"),
        county=str(market.get("county") or "") or None,
        city=str(market.get("city") or "") or None,
        limit=int(mode_overrides["limit"]),
        market_slug=str(market.get("slug") or "") or None,
        trigger_type=trigger_type,
        property_types=["single_family", "multi_family"],
        max_price=200000,
        max_units=int(
            market.get("max_units")
            or getattr(settings, "investor_buy_box_max_units", 4)
        ),
        market_cursor=dict(mode_overrides["market_cursor"] or {}),
        market_sync_state_id=int(sync_state.id) if sync_state is not None else None,
        market_sync_status=(
            str(sync_state.status or "idle") if sync_state is not None else "idle"
        ),
        market_exhausted=bool(sync_state.market_exhausted) if sync_state is not None else False,
        sync_mode=str(mode_overrides["sync_mode"]),
        max_pages_budget=int(mode_overrides["max_pages_budget"]),
        backfill_target_records=mode_overrides["backfill_target_records"],
        reset_market_cursor_on_start=bool(mode_overrides["reset_market_cursor_on_start"]),
        supported_market=True,
        mark_backfill_complete_on_exhaustion=bool(mode_overrides["mark_backfill_complete_on_exhaustion"]),
    )

    zip_codes = _coerce_zip_codes(
        market.get("zip_codes") or market.get("zips") or market.get("zipCodes")
    )

    if str(market.get("slug") or "").strip().lower() == "pontiac-oakland":
        forced_pontiac_zips = ["48340", "48341", "48342", "48343"]
        zip_codes = forced_pontiac_zips

    if zip_codes:
        payload["zip_codes"] = zip_codes

    payload["query_strategy"] = "city_plus_zip_sweep"
    payload["max_price"] = 200000
    payload["property_types"] = ["single_family", "multi_family"]

    return payload


def _source_config(source: Any) -> dict[str, Any]:
    raw = getattr(source, "config_json", None) or {}
    return dict(raw) if isinstance(raw, dict) else {}


def _source_matches_market_slug(source: Any, market: dict[str, Any]) -> bool:
    config = _source_config(source)
    return _norm_text(config.get("market_slug")) == _norm_text(market.get("slug"))


def _source_matches_city_state(source: Any, market: dict[str, Any]) -> bool:
    config = _source_config(source)
    return (
        _norm_text(config.get("city")) == _norm_text(market.get("city"))
        and str(config.get("state") or "").strip().upper()
        == str(market.get("state") or "").strip().upper()
    )


def _source_matches_county_state(source: Any, market: dict[str, Any]) -> bool:
    config = _source_config(source)
    return (
        _norm_text(config.get("county")) == _norm_text(market.get("county"))
        and str(config.get("state") or "").strip().upper()
        == str(market.get("state") or "").strip().upper()
    )


def _source_slug_contains_market(source: Any, market: dict[str, Any]) -> bool:
    source_slug = _norm_text(getattr(source, "slug", ""))
    market_slug = _norm_text(market.get("slug"))
    return bool(source_slug and market_slug and market_slug in source_slug)


def _source_match_rank(source: Any, market: dict[str, Any]) -> int:
    if _source_matches_market_slug(source, market):
        return 100
    if _source_matches_city_state(source, market):
        return 80
    if _source_matches_county_state(source, market):
        return 50
    if _source_slug_contains_market(source, market):
        return 30
    return 0


def _source_sort_key(source: Any, market: dict[str, Any]) -> tuple[Any, ...]:
    config = _source_config(source)
    return (
        -_source_match_rank(source, market),
        0 if bool(config.get("market_slug")) else 1,
        0 if bool(config.get("city")) else 1,
        0 if bool(config.get("county")) else 1,
        str(getattr(source, "slug", "") or "").strip().lower(),
        int(getattr(source, "id", 0) or 0),
    )


def _select_primary_source_for_market(
    sources: list[Any],
    market: dict[str, Any],
) -> Any | None:
    if not sources:
        return None

    ranked = [source for source in sources if _source_match_rank(source, market) > 0]
    if ranked:
        ranked.sort(key=lambda s: _source_sort_key(s, market))
        return ranked[0]

    rentcast_sources = [
        source
        for source in sources
        if _norm_text(getattr(source, "provider", "")) == "rentcast"
    ]
    if not rentcast_sources:
        return None

    canonical_slug = canonical_source_slug_for_market_slug(market.get("slug"))
    if canonical_slug:
        for source in rentcast_sources:
            if _norm_text(getattr(source, "slug", "")) == _norm_text(canonical_slug):
                return source

    rentcast_sources.sort(key=lambda s: _source_sort_key(s, market))
    return rentcast_sources[0]


def resolve_supported_market(*, market_slug: str) -> dict[str, Any] | None:
    if not market_slug:
        return None
    return get_active_supported_market_by_slug(str(market_slug).strip().lower())


def get_enabled_sources_for_org(db: Session, *, org_id: int) -> list[Any]:
    ensure_market_slug_on_sources(db, org_id=int(org_id))
    ensure_sources_for_supported_markets(db, org_id=int(org_id))
    ensure_default_manual_sources(db, org_id=int(org_id))
    db.flush()
    db.commit()

    return [
        source
        for source in list_sources(db, org_id=int(org_id))
        if bool(getattr(source, "is_enabled", False))
        and _norm_text(getattr(source, "provider", "")) == "rentcast"
    ]


def build_daily_dispatch_plan(
    db: Session,
    *,
    org_id: int,
    sync_mode: str | None = "refresh",
) -> list[dict[str, Any]]:
    markets = list_selected_daily_markets()
    sources = get_enabled_sources_for_org(db, org_id=int(org_id))
    normalized_mode = normalize_sync_mode(sync_mode)

    dispatches: list[dict[str, Any]] = []
    seen_market_slugs: set[str] = set()

    for market in markets:
        market_slug = str(market.get("slug") or "").strip().lower()
        if not market_slug or market_slug in seen_market_slugs:
            continue

        matched_sources = resolve_sources_for_market(
            db,
            org_id=int(org_id),
            market_slug=market_slug,
        )

        source = None
        if matched_sources:
            matched_sources.sort(key=lambda s: _source_sort_key(s, market))
            source = matched_sources[0]
        else:
            source = _select_primary_source_for_market(sources, market)

        if source is None:
            continue

        sync_state = get_or_create_market_sync_state(
            db,
            org_id=int(org_id),
            source=source,
            market=market,
        )
        runtime_config = build_market_runtime_payload(
            market,
            trigger_type="daily_refresh",
            sync_state=sync_state,
            sync_mode=normalized_mode,
        )
        dispatches.append(
            {
                "market": market,
                "source_id": int(source.id),
                "source_slug": str(getattr(source, "slug", "")),
                "provider": str(getattr(source, "provider", "")),
                "trigger_type": "daily_refresh",
                "runtime_config": runtime_config,
                "market_sync_state_id": int(sync_state.id),
                "market_cursor": dict(runtime_config.get("market_cursor") or {}),
                "sync_mode": normalized_mode,
                "dataset_identity": build_market_dataset_identity(
                    org_id=int(org_id),
                    source=source,
                    runtime_config=runtime_config,
                    sync_state=sync_state,
                ),
            }
        )
        seen_market_slugs.add(market_slug)

    return dispatches


def build_supported_market_sync_plan_for_db(
    db: Session,
    *,
    org_id: int,
    market_slug: str,
    limit: int | None = None,
    sync_mode: str | None = "refresh",
) -> dict[str, Any]:
    market = resolve_supported_market(market_slug=market_slug)
    normalized_mode = normalize_sync_mode(sync_mode)

    if market is None:
        return {
            "ok": False,
            "covered": False,
            "market": None,
            "dispatches": [],
            "sync_mode": normalized_mode,
        }

    if str(market.get("slug") or "").strip().lower() == "pontiac-oakland":
        market = {
            **market,
            "zip_codes": ["48340", "48341", "48342", "48343"],
            "city": "pontiac",
            "state": "MI",
            "county": "oakland",
            "max_price": 200000,
            "property_types": ["single_family", "multi_family"],
        }

    get_enabled_sources_for_org(db, org_id=int(org_id))

    matched_sources = resolve_sources_for_market(
        db,
        org_id=int(org_id),
        market_slug=str(market.get("slug") or ""),
    )

    if not matched_sources:
        return {
            "ok": True,
            "covered": True,
            "market": market,
            "dispatches": [],
            "sync_mode": normalized_mode,
        }

    matched_sources.sort(key=lambda s: _source_sort_key(s, market))
    source = matched_sources[0]
    sync_state = get_or_create_market_sync_state(
        db,
        org_id=int(org_id),
        source=source,
        market=market,
    )

    trigger_type = "manual_market_sync"

    runtime_config = build_market_runtime_payload(
        market,
        trigger_type=trigger_type,
        sync_state=sync_state,
        limit_override=limit,
        sync_mode=normalized_mode,
    )

    dispatches = [
        {
            "market": market,
            "source_id": int(source.id),
            "source_slug": str(getattr(source, "slug", "")),
            "provider": str(getattr(source, "provider", "")),
            "trigger_type": trigger_type,
            "runtime_config": dict(runtime_config),
            "market_sync_state_id": int(sync_state.id),
            "market_cursor": dict(runtime_config.get("market_cursor") or {}),
            "sync_mode": normalized_mode,
            "dataset_identity": build_market_dataset_identity(
                org_id=int(org_id),
                source=source,
                runtime_config=runtime_config,
                sync_state=sync_state,
            ),
        }
    ]

    return {
        "ok": True,
        "covered": True,
        "market": market,
        "dispatches": dispatches,
        "sync_mode": normalized_mode,
    }


def build_supported_market_sync_plan(
    *,
    org_id: int,
    market_slug: str,
    limit: int | None = None,
    sync_mode: str | None = "refresh",
) -> dict[str, Any]:
    db = SessionLocal()
    try:
        plan = build_supported_market_sync_plan_for_db(
            db,
            org_id=int(org_id),
            market_slug=market_slug,
            limit=limit,
            sync_mode=sync_mode,
        )
        db.commit()
        return plan
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
        

