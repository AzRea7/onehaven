from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

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
