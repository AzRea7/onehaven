# backend/app/services/budget_service.py
from __future__ import annotations

import json
from datetime import datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models_saas import ExternalBudgetLedger
from app.services.plan_service import get_limits, record_usage


def _day_window(now: datetime) -> tuple[datetime, datetime]:
    start = datetime(year=now.year, month=now.month, day=now.day)
    end = start + timedelta(days=1)
    return start, end


def consume_external_budget(db: Session, *, org_id: int, provider: str, units: int = 1, meta: dict | None = None) -> None:
    """
    Single enforcement point for your "50 calls max" rule (per day), per org.
    This is where every external API wrapper must call first.
    """
    limits = get_limits(db, org_id=org_id)
    cap = int(limits.get("external_calls_per_day", 0))
    if not cap:
        return

    now = datetime.utcnow()
    start, end = _day_window(now)

    used = db.scalar(
        select(func.coalesce(func.sum(ExternalBudgetLedger.cost_units), 0))
        .where(ExternalBudgetLedger.org_id == int(org_id))
        .where(ExternalBudgetLedger.created_at >= start)
        .where(ExternalBudgetLedger.created_at < end)
    )
    used = int(used or 0)

    if used + int(units) > cap:
        raise HTTPException(status_code=402, detail=f"external_budget_exceeded:{used}/{cap}")

    db.add(
        ExternalBudgetLedger(
            org_id=int(org_id),
            provider=str(provider),
            cost_units=int(units),
            meta_json=json.dumps(meta or {}),
            created_at=now,
        )
    )

    # also mirror into generic usage
    record_usage(db, org_id=org_id, metric="external_calls", units=int(units), meta={"provider": provider})