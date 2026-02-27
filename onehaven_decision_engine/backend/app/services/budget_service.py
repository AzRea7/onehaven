# backend/app/services/budget_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models_saas import ExternalBudgetLedger
from app.services.plan_service import get_limits, record_usage


@dataclass(frozen=True)
class BudgetStatus:
    metric: str
    provider: str
    limit: int
    used: int
    remaining: int
    reset_at: str  # ISO timestamp


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _day_window(now: datetime) -> tuple[datetime, datetime]:
    # Daily window in UTC to avoid DST weirdness.
    start = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def get_external_budget_status(db: Session, *, org_id: int, provider: str, metric_key: str = "external_calls_per_day") -> BudgetStatus:
    limits = get_limits(db, org_id=org_id)
    cap = int(limits.get(metric_key, 0) or 0)

    now = _utcnow()
    start, end = _day_window(now)

    used = db.scalar(
        select(func.coalesce(func.sum(ExternalBudgetLedger.cost_units), 0))
        .where(ExternalBudgetLedger.org_id == int(org_id))
        .where(ExternalBudgetLedger.provider == str(provider))
        .where(ExternalBudgetLedger.created_at >= start)
        .where(ExternalBudgetLedger.created_at < end)
    )
    used_i = int(used or 0)

    remaining = max(0, cap - used_i) if cap else 10**9  # "unlimited" if cap=0
    return BudgetStatus(
        metric=metric_key,
        provider=str(provider),
        limit=int(cap),
        used=used_i,
        remaining=int(remaining),
        reset_at=end.isoformat(),
    )


def consume_external_budget(
    db: Session,
    *,
    org_id: int,
    provider: str,
    units: int = 1,
    meta: dict[str, Any] | None = None,
    metric_key: str = "external_calls_per_day",
) -> BudgetStatus:
    """
    Single enforcement point for your "50 calls max" rule (per day), per org.

    Plan limit key (recommended):
      external_calls_per_day: 50

    If cap is 0 or missing => treated as "unlimited" (dev / internal plans).
    """
    status = get_external_budget_status(db, org_id=org_id, provider=provider, metric_key=metric_key)

    # If limit is 0 => no enforcement.
    if status.limit <= 0:
        return status

    if status.used + int(units) > status.limit:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "plan_limit_exceeded",
                "metric": metric_key,
                "provider": provider,
                "limit": status.limit,
                "used": status.used,
                "remaining": status.remaining,
                "reset_at": status.reset_at,
            },
        )

    now = _utcnow()
    db.add(
        ExternalBudgetLedger(
            org_id=int(org_id),
            provider=str(provider),
            cost_units=int(units),
            meta_json=json.dumps(meta or {}),
            created_at=now,
        )
    )

    # mirror into generic usage meter as well
    record_usage(db, org_id=org_id, metric="external_calls", units=int(units), meta={"provider": provider})

    # caller commits (or you can commit here if you want "always counted" semantics)
    return get_external_budget_status(db, org_id=org_id, provider=provider, metric_key=metric_key)
