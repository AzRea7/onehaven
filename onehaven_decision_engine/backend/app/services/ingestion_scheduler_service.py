from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Property

DEFAULT_DAILY_MARKETS: list[dict[str, Any]] = [
    {"state": "MI", "county": "wayne", "city": "detroit"},
    {"state": "MI", "county": "wayne", "city": "dearborn"},
    {"state": "MI", "county": "oakland", "city": "pontiac"},
    {"state": "MI", "county": "oakland", "city": "southfield"},
    {"state": "MI", "county": "macomb", "city": "warren"},
    {"state": "MI", "county": "macomb", "city": "sterling heights"},
]


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


def compute_stale_location_cutoff(now: datetime | None = None) -> datetime:
    now = now or datetime.utcnow()
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