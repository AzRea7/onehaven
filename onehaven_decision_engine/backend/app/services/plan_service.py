from __future__ import annotations

import json
from datetime import datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import OrgSubscription, Plan, UsageLedger


DEFAULT_PLANS = {
    "free": {
        "properties_max": 3,
        "agent_runs_per_day": 20,
        "external_calls_per_day": 50,
        "jurisdiction_tasks_per_property": 10,
    },
    "starter": {
        "properties_max": 25,
        "agent_runs_per_day": 200,
        "external_calls_per_day": 500,
        "jurisdiction_tasks_per_property": 25,
    },
    "pro": {
        "properties_max": 250,
        "agent_runs_per_day": 5000,
        "external_calls_per_day": 20000,
        "jurisdiction_tasks_per_property": 100,
    },
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


def jurisdiction_task_limit(db: Session, *, org_id: int) -> int:
    limits = get_limits(db, org_id=org_id)
    try:
        return int(limits.get("jurisdiction_tasks_per_property", 0) or 0)
    except Exception:
        return 0


def clip_jurisdiction_tasks_for_plan(
    db: Session,
    *,
    org_id: int,
    tasks: list[dict],
) -> dict:
    """
    Optional helper for UI/service layers that want to avoid flooding lower-tier plans
    with an excessive number of generated jurisdiction tasks.
    """
    cap = jurisdiction_task_limit(db, org_id=org_id)
    if cap <= 0:
        return {
            "tasks": tasks,
            "task_count": len(tasks),
            "task_limit": cap,
            "truncated": False,
            "truncated_count": 0,
        }

    clipped = list(tasks[:cap])
    truncated_count = max(0, len(tasks) - len(clipped))

    return {
        "tasks": clipped,
        "task_count": len(clipped),
        "task_limit": cap,
        "truncated": truncated_count > 0,
        "truncated_count": truncated_count,
    }
