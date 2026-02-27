# backend/app/services/external_budget.py
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import ExternalBudgetLedger


def record_external_cost(
    db: Session,
    *,
    org_id: int,
    provider: str,
    cost_units: int = 1,
    meta: Optional[dict[str, Any]] = None,
) -> ExternalBudgetLedger:
    row = ExternalBudgetLedger(
        org_id=int(org_id),
        provider=str(provider),
        cost_units=int(cost_units),
        meta_json=json.dumps(meta or {}),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def external_cost_used_today(
    db: Session,
    *,
    org_id: int,
    provider: str,
) -> int:
    today = date.today()
    start = datetime(today.year, today.month, today.day)
    q = (
        select(func.coalesce(func.sum(ExternalBudgetLedger.cost_units), 0))
        .where(ExternalBudgetLedger.org_id == int(org_id))
        .where(ExternalBudgetLedger.provider == str(provider))
        .where(ExternalBudgetLedger.created_at >= start)
    )
    v = db.scalar(q)
    try:
        return int(v or 0)
    except Exception:
        return 0


def external_cost_remaining_today(
    db: Session,
    *,
    org_id: int,
    provider: str,
    daily_limit_units: int,
) -> int:
    used = external_cost_used_today(db, org_id=org_id, provider=provider)
    remaining = int(daily_limit_units) - int(used)
    return remaining if remaining > 0 else 0
