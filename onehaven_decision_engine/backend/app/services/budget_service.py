# backend/app/services/budget_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import ExternalBudgetLedger, OrgSubscription, Plan


@dataclass(frozen=True)
class BudgetStatus:
    provider: str
    metric: str
    limit: int
    used: int
    remaining: int
    reset_at: str


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def _today_utc() -> date:
    return datetime.utcnow().date()


def _reset_at_iso(day: date) -> str:
    # Next midnight UTC
    return datetime.combine(day + timedelta(days=1), datetime.min.time()).isoformat() + "Z"


def _plan_limit_for_metric(plan_limits: dict[str, Any], metric_key: str) -> int:
    """
    plan_limits_json is a dict. We expect keys like:
      - external_calls_per_day
      - agent_runs_per_day
      - properties
    """
    v = plan_limits.get(metric_key)
    if v is None:
        # sensible default if missing
        return int(getattr(settings, "default_external_calls_per_day", 50) or 50)
    try:
        return int(v)
    except Exception:
        return int(getattr(settings, "default_external_calls_per_day", 50) or 50)


def _get_org_plan_limits(db: Session, *, org_id: int) -> dict[str, Any]:
    sub = db.scalar(select(OrgSubscription).where(OrgSubscription.org_id == int(org_id)))
    plan_code = (sub.plan_code if sub else None) or "free"

    plan = db.scalar(select(Plan).where(Plan.code == plan_code))
    if not plan:
        return {}

    limits = _loads(getattr(plan, "limits_json", None), {})
    return limits if isinstance(limits, dict) else {}


def get_external_budget_status(
    db: Session,
    *,
    org_id: int,
    provider: str,
    metric_key: str = "external_calls_per_day",
) -> BudgetStatus:
    """
    Returns used/remaining for today's UTC budget for the provider.
    Ledger is append-only; we sum cost_units for today.
    """
    day = _today_utc()
    limits = _get_org_plan_limits(db, org_id=int(org_id))
    limit = _plan_limit_for_metric(limits, metric_key)

    used = int(
        db.scalar(
            select(func.coalesce(func.sum(ExternalBudgetLedger.cost_units), 0))
            .where(ExternalBudgetLedger.org_id == int(org_id))
            .where(ExternalBudgetLedger.provider == str(provider))
            .where(func.date(ExternalBudgetLedger.created_at) == day)
        )
        or 0
    )

    remaining = max(0, int(limit) - int(used))
    return BudgetStatus(
        provider=str(provider),
        metric=str(metric_key),
        limit=int(limit),
        used=int(used),
        remaining=int(remaining),
        reset_at=_reset_at_iso(day),
    )


def consume_external_budget(
    db: Session,
    *,
    org_id: int,
    provider: str,
    units: int = 1,
    meta: Optional[dict[str, Any]] = None,
    metric_key: str = "external_calls_per_day",
) -> BudgetStatus:
    """
    Atomically enforces daily budget by:
      1) reading status
      2) if remaining < units => raise HTTP-ish error payload (handled by router)
      3) inserting ledger row
    """
    from fastapi import HTTPException

    units_i = max(1, int(units))

    status = get_external_budget_status(db, org_id=int(org_id), provider=str(provider), metric_key=metric_key)

    if status.remaining < units_i:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "plan_limit_exceeded",
                "provider": status.provider,
                "metric": status.metric,
                "limit": status.limit,
                "used": status.used,
                "remaining": status.remaining,
                "reset_at": status.reset_at,
            },
        )

    row = ExternalBudgetLedger(
        org_id=int(org_id),
        provider=str(provider),
        cost_units=int(units_i),
        meta_json=_dumps(meta) if meta else None,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()

    return get_external_budget_status(db, org_id=int(org_id), provider=str(provider), metric_key=metric_key)
