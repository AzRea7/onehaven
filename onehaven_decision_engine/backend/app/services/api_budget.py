from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import ApiUsage


class ApiBudgetExceeded(RuntimeError):
    pass


def get_calls_used(db: Session, *, provider: str, day: date) -> int:
    row = db.scalar(select(ApiUsage).where(ApiUsage.provider == provider, ApiUsage.day == day))
    return int(row.calls) if row else 0


def get_remaining(db: Session, *, provider: str, day: date, daily_limit: int) -> int:
    used = get_calls_used(db, provider=provider, day=day)
    return max(int(daily_limit) - used, 0)


def consume(db: Session, *, provider: str, day: date, daily_limit: int, calls: int = 1) -> int:
    """
    Atomically-ish increments usage (within a transaction).
    Returns remaining calls after consume.
    Raises ApiBudgetExceeded if limit would be exceeded.
    """
    calls = int(calls)
    if calls <= 0:
        return get_remaining(db, provider=provider, day=day, daily_limit=daily_limit)

    row = db.scalar(select(ApiUsage).where(ApiUsage.provider == provider, ApiUsage.day == day))
    if row is None:
        row = ApiUsage(provider=provider, day=day, calls=0, updated_at=datetime.utcnow())
        db.add(row)
        db.flush()

    new_total = int(row.calls) + calls
    if new_total > int(daily_limit):
        raise ApiBudgetExceeded(
            f"API budget exceeded for provider={provider} day={day.isoformat()} "
            f"(used={row.calls}, requested={calls}, limit={daily_limit})"
        )

    row.calls = new_total
    row.updated_at = datetime.utcnow()
    db.flush()

    return max(int(daily_limit) - int(row.calls), 0)
