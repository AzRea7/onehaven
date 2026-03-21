# backend/app/services/ingestion_scheduler_service.py
from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import distinct, or_, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import IngestionSource, Property
from ..policy_models import JurisdictionProfile
from ..middleware.structured_logging import emit_structured_log
from .ingestion_source_service import ensure_default_manual_sources, list_sources
from .jurisdiction_refresh_service import (
    DEFAULT_JURISDICTION_STALE_DAYS,
    build_jurisdiction_refresh_payload as _build_jurisdiction_refresh_payload,
    list_jurisdictions_needing_refresh as _list_jurisdictions_needing_refresh,
)
from .locks_service import LockResult, acquire_lock, get_lock

logger = logging.getLogger(__name__)

DEFAULT_DAILY_MARKETS: list[dict[str, Any]] = [
    {"state": "MI", "county": "wayne", "city": "detroit"},
    {"state": "MI", "county": "wayne", "city": "dearborn"},
    {"state": "MI", "county": "oakland", "city": "pontiac"},
    {"state": "MI", "county": "oakland", "city": "southfield"},
    {"state": "MI", "county": "macomb", "city": "warren"},
    {"state": "MI", "county": "macomb", "city": "sterling heights"},
]

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
    return [dict(x) for x in DEFAULT_DAILY_MARKETS]


def compute_next_daily_sync(now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    next_run = now.replace(hour=9, minute=10, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return next_run


def build_runtime_payload(*, state: str | None, county: str | None, city: str | None) -> dict[str, Any]:
    payload = {
        "trigger_type": "daily_refresh",
        "state": (state or "MI").strip(),
        "limit": 250,
    }
    if county:
        payload["county"] = county.strip()
    if city:
        payload["city"] = city.strip()
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
    stmt = select(JurisdictionProfile).where(JurisdictionProfile.id.in_(ids)).order_by(JurisdictionProfile.id.asc())
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
) -> str:
    state = str(market.get("state") or "MI").strip().lower()
    county = str(market.get("county") or "").strip().lower()
    city = str(market.get("city") or "").strip().lower()
    return f"daily_sync_dispatch:{int(org_id)}:{day_key}:{source_key}:{state}:{county}:{city}"


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
) -> dict[str, Any]:
    source_id = int(getattr(source, "id"))
    source_key = _source_key(source)
    return {
        "mode": "scheduler",
        "scope": "daily_sync",
        "org_id": int(org_id),
        "source_id": source_id,
        "source_key": source_key,
        "schedule_day": str(day_key),
        "state": str(market.get("state") or "MI").strip(),
        "county": str(market.get("county") or "").strip(),
        "city": str(market.get("city") or "").strip(),
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


def dispatch_daily_sync_for_org(
    db: Session,
    *,
    org_id: int,
    enqueue_sync: Callable[[int, int, str, dict[str, Any]], Any],
    now: datetime | None = None,
    owner: str | None = None,
    markets: list[dict[str, Any]] | None = None,
    lock_ttl_seconds: int | None = None,
    dedupe_ttl_seconds: int | None = None,
) -> dict[str, Any]:
    now = now or _utcnow()
    owner = owner or build_lock_owner(prefix="daily-sync")
    lock_ttl_seconds = int(lock_ttl_seconds or DEFAULT_DAILY_SYNC_LOCK_TTL_SECONDS)
    dedupe_ttl_seconds = int(dedupe_ttl_seconds or DEFAULT_DISPATCH_DEDUPE_TTL_SECONDS)
    markets = markets or list_default_daily_markets()

    scheduler_lock = acquire_lock(
        db,
        org_id=int(org_id),
        lock_key=daily_sync_lock_key(int(org_id)),
        owner=owner,
        ttl_seconds=lock_ttl_seconds,
        now=now,
    )
    db.commit()

    if not scheduler_lock.acquired:
        _emit(
            {
                "event": "daily_sync_skipped_locked",
                "job_type": "daily_sync",
                "org_id": int(org_id),
                "outcome": "skipped_locked",
                "lock_key": scheduler_lock.lock_key,
                "holder": scheduler_lock.holder,
                "expires_at": scheduler_lock.expires_at.isoformat() if scheduler_lock.expires_at else None,
            }
        )
        return {
            "ok": True,
            "org_id": int(org_id),
            "status": "skipped_locked",
            "lock": {
                "acquired": scheduler_lock.acquired,
                "lock_key": scheduler_lock.lock_key,
                "holder": scheduler_lock.holder,
                "expires_at": scheduler_lock.expires_at.isoformat() if scheduler_lock.expires_at else None,
                "stale": scheduler_lock.stale,
            },
            "queued": 0,
            "skipped": 0,
            "results": [],
        }

    ensure_default_manual_sources(db, org_id=int(org_id))
    sources = [
        s
        for s in list_sources(db, org_id=int(org_id))
        if bool(getattr(s, "is_enabled", False))
    ]

    day_key = now.date().isoformat()
    queued = 0
    skipped = 0
    results: list[dict[str, Any]] = []

    _emit(
        {
            "event": "daily_sync_dispatch_start",
            "job_type": "daily_sync",
            "org_id": int(org_id),
            "lock_key": scheduler_lock.lock_key,
            "outcome": "running",
            "source_count": len(sources),
            "market_count": len(markets),
        }
    )

    for source in sources:
        source_id = int(getattr(source, "id"))
        source_key = _source_key(source)

        for market in markets:
            dedupe_key = _dispatch_dedupe_key(
                org_id=int(org_id),
                day_key=day_key,
                source_key=source_key,
                market=market,
            )
            dedupe_lock = acquire_lock(
                db,
                org_id=int(org_id),
                lock_key=dedupe_key,
                owner=owner,
                ttl_seconds=dedupe_ttl_seconds,
                now=now,
            )
            db.commit()

            if not dedupe_lock.acquired:
                skipped += 1
                _emit(
                    {
                        "event": "daily_sync_dispatch_duplicate_skip",
                        "job_type": "daily_sync",
                        "org_id": int(org_id),
                        "source": source_key,
                        "source_id": source_id,
                        "lock_key": dedupe_key,
                        "outcome": "skipped_duplicate",
                        "holder": dedupe_lock.holder,
                        "state": market.get("state"),
                        "county": market.get("county"),
                        "city": market.get("city"),
                    }
                )
                results.append(
                    {
                        "queued": False,
                        "reason": "duplicate_for_day",
                        "source_id": source_id,
                        "source_key": source_key,
                        "market": dict(market),
                        "lock_key": dedupe_key,
                        "holder": dedupe_lock.holder,
                        "expires_at": dedupe_lock.expires_at.isoformat() if dedupe_lock.expires_at else None,
                    }
                )
                continue

            payload = build_runtime_payload(
                state=market.get("state"),
                county=market.get("county"),
                city=market.get("city"),
            )
            payload["idempotency_context"] = build_scheduler_idempotency_context(
                org_id=int(org_id),
                source=source,
                market=market,
                day_key=day_key,
                dispatch_key=dedupe_key,
            )

            enqueue_sync(int(org_id), source_id, "daily_refresh", payload)
            queued += 1
            _emit(
                {
                    "event": "daily_sync_enqueued",
                    "job_type": "daily_sync",
                    "org_id": int(org_id),
                    "source": source_key,
                    "source_id": source_id,
                    "lock_key": dedupe_key,
                    "outcome": "queued",
                    "state": market.get("state"),
                    "county": market.get("county"),
                    "city": market.get("city"),
                }
            )
            results.append(
                {
                    "queued": True,
                    "reason": "enqueued",
                    "source_id": source_id,
                    "source_key": source_key,
                    "market": dict(market),
                    "runtime_payload": payload,
                    "lock_key": dedupe_key,
                }
            )

    _emit(
        {
            "event": "daily_sync_dispatch_complete",
            "job_type": "daily_sync",
            "org_id": int(org_id),
            "queued": queued,
            "skipped": skipped,
            "source_count": len(sources),
            "market_count": len(markets),
            "outcome": "completed",
        }
    )

    return {
        "ok": True,
        "org_id": int(org_id),
        "status": "dispatched",
        "lock": {
            "acquired": scheduler_lock.acquired,
            "lock_key": scheduler_lock.lock_key,
            "holder": scheduler_lock.holder,
            "expires_at": scheduler_lock.expires_at.isoformat() if scheduler_lock.expires_at else None,
            "stale": scheduler_lock.stale,
        },
        "queued": queued,
        "skipped": skipped,
        "results": results,
    }


def dispatch_daily_sync_for_all_orgs(
    db: Session,
    *,
    enqueue_sync: Callable[[int, int, str, dict[str, Any]], Any],
    org_ids: list[int] | None = None,
    now: datetime | None = None,
    owner: str | None = None,
    markets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    now = now or _utcnow()
    owner = owner or build_lock_owner(prefix="daily-sync")
    org_ids = org_ids or list_org_ids_with_enabled_sources(db)

    per_org: list[dict[str, Any]] = []
    total_queued = 0
    total_skipped = 0

    for org_id in org_ids:
        result = dispatch_daily_sync_for_org(
            db,
            org_id=int(org_id),
            enqueue_sync=enqueue_sync,
            now=now,
            owner=owner,
            markets=markets,
        )
        total_queued += int(result.get("queued", 0) or 0)
        total_skipped += int(result.get("skipped", 0) or 0)
        per_org.append(result)

    return {
        "ok": True,
        "org_count": len(org_ids),
        "queued": total_queued,
        "skipped": total_skipped,
        "results": per_org,
    }