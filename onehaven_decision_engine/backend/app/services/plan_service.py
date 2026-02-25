# backend/app/services/plan_service.py
from __future__ import annotations

import json
from datetime import datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models_saas import Plan, OrgSubscription, UsageLedger


DEFAULT_PLANS = {
    "free": {"properties_max": 3, "agent_runs_per_day": 20, "external_calls_per_day": 50},
    "starter": {"properties_max": 25, "agent_runs_per_day": 200, "external_calls_per_day": 500},
    "pro": {"properties_max": 250, "agent_runs_per_day": 5000, "external_calls_per_day": 20000},
}


def ensure_default_plans(db: Session) -> None:
    existing = {p.code for p in db.scalars(select(Plan)).all()}
    for code, limits in DEFAULT_PLANS.items():
        if code in existing:
            continue
        db.add(Plan(code=code, name=code.title(), limits_json=json.dumps(limits)))
    db.commit()


def get_limits(db: Session, *, org_id: int) -> dict:
    ensure_default_plans(db)

    sub = db.scalar(select(OrgSubscription).where(OrgSubscription.org_id == int(org_id)))
    plan_code = sub.plan_code if sub else "free"

    plan = db.scalar(select(Plan).where(Plan.code == str(plan_code)))
    if not plan:
        plan = db.scalar(select(Plan).where(Plan.code == "free"))

    try:
        return json.loads(plan.limits_json or "{}")
    except Exception:
        return {}


def _day_window(now: datetime) -> tuple[datetime, datetime]:
    start = datetime(year=now.year, month=now.month, day=now.day)
    end = start + timedelta(days=1)
    return start, end


def enforce_daily_limit(db: Session, *, org_id: int, metric: str, add_units: int = 1) -> None:
    limits = get_limits(db, org_id=org_id)
    now = datetime.utcnow()
    start, end = _day_window(now)

    cap = None
    if metric == "agent_runs":
        cap = int(limits.get("agent_runs_per_day", 0))
    elif metric == "external_calls":
        cap = int(limits.get("external_calls_per_day", 0))

    if not cap:
        return

    used = db.scalar(
        select(func.coalesce(func.sum(UsageLedger.units), 0))
        .where(UsageLedger.org_id == int(org_id))
        .where(UsageLedger.metric == str(metric))
        .where(UsageLedger.created_at >= start)
        .where(UsageLedger.created_at < end)
    )
    used = int(used or 0)

    if used + int(add_units) > cap:
        raise HTTPException(status_code=402, detail=f"plan_limit_exceeded:{metric}:{used}/{cap}")


def record_usage(db: Session, *, org_id: int, metric: str, units: int = 1, meta: dict | None = None) -> None:
    db.add(
        UsageLedger(
            org_id=int(org_id),
            metric=str(metric),
            units=int(units),
            meta_json=json.dumps(meta or {}),
            created_at=datetime.utcnow(),
        )
    )
    