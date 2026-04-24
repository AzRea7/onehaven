from __future__ import annotations

from typing import Any


def evaluate_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    stale = payload.get("stale_authoritative_sources") or []
    overdue = payload.get("overdue_refresh_categories") or []

    return {
        "passed": not stale,
        "stale_authoritative_sources": stale,
        "overdue_refresh_categories": overdue,
        "reason": "stale_authoritative_sources" if stale else None,
    }
