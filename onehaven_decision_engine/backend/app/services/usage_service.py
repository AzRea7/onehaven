# backend/app/services/usage_service.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.models import UsageLedger, Plan, Subscription, Property, AgentRun


def _day_key_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _loads_json(s: str | None) -> dict[str, Any]:
    import json

    if not s:
        return {}
    try:
        v = json.loads(s)
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}


def _get_plan_limits(db: Session, org_id: int) -> dict[str, Any]:
    sub = db.scalar(select(Subscription).where(Subscription.org_id == int(org_id)).order_by(Subscription.id.desc()))
    plan_code = str(sub.plan_code) if sub and sub.status == "active" else "free"

    plan = db.scalar(select(Plan).where(Plan.code == plan_code))
    if not plan:
        # safe fallback
        return {"max_properties": 3, "agent_runs_per_day": 20, "external_calls_per_day": 50, "max_concurrent_runs": 2}

    return _loads_json(getattr(plan, "limits_json", "{}"))


def _count_usage(db: Session, org_id: int, *, day_key: str, kind: str, provider: Optional[str] = None) -> int:
    q = select(func.coalesce(func.sum(UsageLedger.units), 0)).where(
        UsageLedger.org_id == int(org_id),
        UsageLedger.day_key == day_key,
        UsageLedger.kind == kind,
    )
    if provider:
        q = q.where(UsageLedger.provider == provider)
    return int(db.scalar(q) or 0)


def record_usage(db: Session, *, org_id: int, kind: str, units: int = 1, provider: str | None = None, ref_id: str | None = None) -> None:
    db.add(
        UsageLedger(
            org_id=int(org_id),
            kind=str(kind),
            provider=str(provider) if provider else None,
            units=int(units),
            ref_id=str(ref_id) if ref_id else None,
            day_key=_day_key_utc(),
            created_at=datetime.utcnow(),
        )
    )


def assert_can_create_property(db: Session, org_id: int) -> None:
    limits = _get_plan_limits(db, org_id=int(org_id))
    max_props = int(limits.get("max_properties") or 0)

    if max_props > 0:
        props = db.scalar(select(func.count(Property.id)).where(Property.org_id == int(org_id)))
        if int(props or 0) >= max_props:
            raise HTTPException(status_code=402, detail=f"Plan limit reached: max_properties={max_props}")


def assert_can_start_agent_run(db: Session, org_id: int, agent_key: str) -> None:
    limits = _get_plan_limits(db, org_id=int(org_id))
    max_runs_day = int(limits.get("agent_runs_per_day") or 0)
    max_concurrent = int(limits.get("max_concurrent_runs") or 0)

    day = _day_key_utc()

    if max_runs_day > 0:
        used = _count_usage(db, org_id=int(org_id), day_key=day, kind="agent_run")
        if used >= max_runs_day:
            raise HTTPException(status_code=402, detail=f"Plan limit reached: agent_runs_per_day={max_runs_day}")

    if max_concurrent > 0:
        active = db.scalar(
            select(func.count(AgentRun.id)).where(
                AgentRun.org_id == int(org_id),
                AgentRun.status.in_(["queued", "running", "blocked"]),
            )
        )
        if int(active or 0) >= max_concurrent:
            raise HTTPException(status_code=429, detail=f"Concurrency limit reached: max_concurrent_runs={max_concurrent}")


def record_agent_run_started(db: Session, org_id: int, run_id: int, agent_key: str) -> None:
    record_usage(db, org_id=int(org_id), kind="agent_run", units=1, provider=None, ref_id=f"run:{int(run_id)}")


def consume_external_budget(db: Session, *, org_id: int, provider: str, units: int, ref_id: str | None = None) -> None:
    """
    ✅ This is your single enforcement point for “50 calls max”.
    Count budget by day; optionally count by provider too.
    """
    limits = _get_plan_limits(db, org_id=int(org_id))
    max_calls_day = int(limits.get("external_calls_per_day") or 0)
    day = _day_key_utc()

    if max_calls_day > 0:
        used = _count_usage(db, org_id=int(org_id), day_key=day, kind="external_call")
        if used + int(units) > max_calls_day:
            raise HTTPException(status_code=402, detail=f"External API budget exceeded: {used}/{max_calls_day} calls today")

    record_usage(db, org_id=int(org_id), kind="external_call", units=int(units), provider=str(provider), ref_id=ref_id)