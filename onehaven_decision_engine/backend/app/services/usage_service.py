# backend/app/services/usage_service.py
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import AgentRun, ApiKey, Property, UsageLedger
from app.services import plan_service


@dataclass(frozen=True)
class UsageSnapshot:
    metric: str
    used: int
    limit: int | None
    remaining: int | None
    allowed: bool
    window_start: datetime | None
    window_end: datetime | None
    plan_code: str


def _now() -> datetime:
    return datetime.utcnow()


def _day_key_utc(now: datetime | None = None) -> str:
    now = now or _now()
    return now.strftime("%Y-%m-%d")


def _month_key_utc(now: datetime | None = None) -> str:
    now = now or _now()
    return now.strftime("%Y-%m")


def _loads_json(s: str | None) -> dict[str, Any]:
    if not s:
        return {}
    try:
        v = json.loads(s)
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}


def _metric_column_name() -> str:
    return "kind" if hasattr(UsageLedger, "kind") else "metric"


def _provider_column_name() -> str | None:
    if hasattr(UsageLedger, "provider"):
        return "provider"
    return None


def _ref_id_column_name() -> str | None:
    if hasattr(UsageLedger, "ref_id"):
        return "ref_id"
    return None


def _meta_json_column_name() -> str | None:
    if hasattr(UsageLedger, "meta_json"):
        return "meta_json"
    return None


def _day_key_column_name() -> str | None:
    if hasattr(UsageLedger, "day_key"):
        return "day_key"
    return None


def _count_usage(
    db: Session,
    org_id: int,
    *,
    metric: str,
    start: datetime | None = None,
    end: datetime | None = None,
    provider: Optional[str] = None,
    day_key: str | None = None,
) -> int:
    metric_col = getattr(UsageLedger, _metric_column_name())

    q = select(func.coalesce(func.sum(UsageLedger.units), 0)).where(
        UsageLedger.org_id == int(org_id),
        metric_col == str(metric),
    )

    day_key_col_name = _day_key_column_name()
    if day_key and day_key_col_name:
        q = q.where(getattr(UsageLedger, day_key_col_name) == str(day_key))

    if start is not None:
        q = q.where(UsageLedger.created_at >= start)
    if end is not None:
        q = q.where(UsageLedger.created_at < end)

    provider_col_name = _provider_column_name()
    if provider and provider_col_name:
        q = q.where(getattr(UsageLedger, provider_col_name) == str(provider))

    return int(db.scalar(q) or 0)


def record_usage(
    db: Session,
    *,
    org_id: int,
    metric: str,
    units: int = 1,
    provider: str | None = None,
    ref_id: str | None = None,
    meta: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> None:
    created_at = created_at or _now()

    kwargs: dict[str, Any] = {
        "org_id": int(org_id),
        "units": int(units),
        "created_at": created_at,
    }

    metric_col_name = _metric_column_name()
    kwargs[metric_col_name] = str(metric)

    provider_col_name = _provider_column_name()
    if provider_col_name:
        kwargs[provider_col_name] = str(provider) if provider else None

    ref_id_col_name = _ref_id_column_name()
    if ref_id_col_name:
        kwargs[ref_id_col_name] = str(ref_id) if ref_id else None

    meta_json_col_name = _meta_json_column_name()
    if meta_json_col_name:
        kwargs[meta_json_col_name] = json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)

    day_key_col_name = _day_key_column_name()
    if day_key_col_name:
        kwargs[day_key_col_name] = _day_key_utc(created_at)

    db.add(UsageLedger(**kwargs))
    db.flush()


def increment_usage(
    db: Session,
    *,
    org_id: int,
    metric: str,
    units: int = 1,
    provider: str | None = None,
    ref_id: str | None = None,
    meta: dict[str, Any] | None = None,
) -> UsageSnapshot:
    record_usage(
        db,
        org_id=int(org_id),
        metric=str(metric),
        units=int(units),
        provider=provider,
        ref_id=ref_id,
        meta=meta,
    )
    return get_usage_snapshot(db, org_id=int(org_id), metric=str(metric), provider=provider)


def _count_properties_under_management(db: Session, *, org_id: int) -> int:
    return int(db.scalar(select(func.count(Property.id)).where(Property.org_id == int(org_id))) or 0)


def _count_active_api_keys(db: Session, *, org_id: int) -> int:
    q = select(func.count(ApiKey.id)).where(ApiKey.org_id == int(org_id))
    if hasattr(ApiKey, "revoked_at"):
        q = q.where(ApiKey.revoked_at.is_(None))
    if hasattr(ApiKey, "disabled_at"):
        q = q.where(ApiKey.disabled_at.is_(None))
    return int(db.scalar(q) or 0)


def _count_active_agent_runs(db: Session, *, org_id: int) -> int:
    return int(
        db.scalar(
            select(func.count(AgentRun.id)).where(
                AgentRun.org_id == int(org_id),
                AgentRun.status.in_(["queued", "running", "blocked"]),
            )
        )
        or 0
    )


def _current_count_for_metric(db: Session, *, org_id: int, metric: str, provider: str | None = None) -> tuple[int, datetime | None, datetime | None]:
    metric = str(metric)
    now = _now()

    if metric in {"max_properties", "properties_max", "properties_under_management"}:
        return _count_properties_under_management(db, org_id=int(org_id)), None, None

    if metric in {"max_api_keys", "active_api_keys"}:
        return _count_active_api_keys(db, org_id=int(org_id)), None, None

    if metric in {"max_concurrent_runs", "active_agent_runs"}:
        return _count_active_agent_runs(db, org_id=int(org_id)), None, None

    if metric in {"agent_run", "external_call", "automation_run", "premium_action"}:
        start, end = plan_service.usage_window_for_metric(db, org_id=int(org_id), metric=metric, now=now)
        return _count_usage(
            db,
            int(org_id),
            metric=metric,
            start=start,
            end=end,
            provider=provider,
            day_key=_day_key_utc(now),
        ), start, end

    if metric in {"ingestion_run"}:
        start, end = plan_service.usage_window_for_metric(db, org_id=int(org_id), metric="ingestion_runs_per_month", now=now)
        return _count_usage(
            db,
            int(org_id),
            metric=metric,
            start=start,
            end=end,
            provider=provider,
        ), start, end

    start, end = plan_service.usage_window_for_metric(db, org_id=int(org_id), metric=metric, now=now)
    return _count_usage(db, int(org_id), metric=metric, start=start, end=end, provider=provider), start, end


def _limit_for_usage_metric(db: Session, *, org_id: int, metric: str) -> int | None:
    mapping = {
        "properties_under_management": "max_properties",
        "max_properties": "max_properties",
        "properties_max": "max_properties",
        "active_api_keys": "max_api_keys",
        "max_api_keys": "max_api_keys",
        "active_agent_runs": "max_concurrent_runs",
        "max_concurrent_runs": "max_concurrent_runs",
        "agent_run": "agent_runs_per_day",
        "external_call": "external_calls_per_day",
        "automation_run": "automation_runs_per_day",
        "ingestion_run": "ingestion_runs_per_month",
    }
    limit_metric = mapping.get(str(metric), str(metric))
    limit = plan_service.get_limit(db, org_id=int(org_id), metric=limit_metric, default=0)
    return None if limit <= 0 else int(limit)


def get_usage_snapshot(
    db: Session,
    *,
    org_id: int,
    metric: str,
    provider: str | None = None,
) -> UsageSnapshot:
    plan_code = plan_service.get_plan_code(db, org_id=int(org_id))
    used, window_start, window_end = _current_count_for_metric(
        db,
        org_id=int(org_id),
        metric=str(metric),
        provider=provider,
    )
    limit = _limit_for_usage_metric(db, org_id=int(org_id), metric=str(metric))
    remaining = None if limit is None else max(0, int(limit) - int(used))
    allowed = True if limit is None else int(used) < int(limit) or remaining > 0

    return UsageSnapshot(
        metric=str(metric),
        used=int(used),
        limit=limit,
        remaining=remaining,
        allowed=bool(allowed),
        window_start=window_start,
        window_end=window_end,
        plan_code=plan_code,
    )


def get_usage_snapshots(
    db: Session,
    *,
    org_id: int,
) -> list[UsageSnapshot]:
    metrics = [
        "properties_under_management",
        "active_api_keys",
        "active_agent_runs",
        "agent_run",
        "external_call",
        "automation_run",
        "ingestion_run",
    ]
    return [get_usage_snapshot(db, org_id=int(org_id), metric=m) for m in metrics]


def get_usage_snapshot_payload(
    db: Session,
    *,
    org_id: int,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for snap in get_usage_snapshots(db, org_id=int(org_id)):
        row = asdict(snap)
        row["window_start"] = snap.window_start.isoformat() if snap.window_start else None
        row["window_end"] = snap.window_end.isoformat() if snap.window_end else None
        payload.append(row)
    return payload


def assert_can_create_property(db: Session, org_id: int) -> None:
    snap = get_usage_snapshot(db, org_id=int(org_id), metric="properties_under_management")
    if snap.limit is not None and int(snap.used) >= int(snap.limit):
        raise HTTPException(
            status_code=402,
            detail={
                "error": "plan_limit_exceeded",
                "metric": "max_properties",
                "used": int(snap.used),
                "limit": int(snap.limit),
                "remaining": int(snap.remaining or 0),
                "plan_code": snap.plan_code,
            },
        )


def assert_can_start_agent_run(db: Session, org_id: int, agent_key: str) -> None:
    daily = get_usage_snapshot(db, org_id=int(org_id), metric="agent_run")
    if daily.limit is not None and int(daily.used) >= int(daily.limit):
        raise HTTPException(
            status_code=402,
            detail={
                "error": "plan_limit_exceeded",
                "metric": "agent_runs_per_day",
                "used": int(daily.used),
                "limit": int(daily.limit),
                "remaining": int(daily.remaining or 0),
                "plan_code": daily.plan_code,
                "agent_key": str(agent_key),
            },
        )

    concurrent = get_usage_snapshot(db, org_id=int(org_id), metric="active_agent_runs")
    if concurrent.limit is not None and int(concurrent.used) >= int(concurrent.limit):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "concurrency_limit_reached",
                "metric": "max_concurrent_runs",
                "used": int(concurrent.used),
                "limit": int(concurrent.limit),
                "remaining": int(concurrent.remaining or 0),
                "plan_code": concurrent.plan_code,
                "agent_key": str(agent_key),
            },
        )


def record_agent_run_started(db: Session, org_id: int, run_id: int, agent_key: str) -> None:
    record_usage(
        db,
        org_id=int(org_id),
        metric="agent_run",
        units=1,
        provider=None,
        ref_id=f"run:{int(run_id)}",
        meta={"agent_key": str(agent_key)},
    )


def assert_can_run_daily_sync(db: Session, *, org_id: int) -> None:
    plan_service.assert_can_run_daily_sync(db, org_id=int(org_id))


def assert_can_create_api_key(db: Session, *, org_id: int) -> None:
    plan_service.assert_can_create_api_key(db, org_id=int(org_id))


def assert_premium_feature(db: Session, *, org_id: int, feature: str) -> None:
    plan_service.assert_feature_enabled(db, org_id=int(org_id), feature=str(feature))


def consume_external_budget(
    db: Session,
    *,
    org_id: int,
    provider: str,
    units: int,
    ref_id: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    snap = get_usage_snapshot(db, org_id=int(org_id), metric="external_call", provider=str(provider))
    next_used = int(snap.used) + int(units)

    if snap.limit is not None and next_used > int(snap.limit):
        raise HTTPException(
            status_code=402,
            detail={
                "error": "external_api_budget_exceeded",
                "metric": "external_calls_per_day",
                "provider": str(provider),
                "used": int(snap.used),
                "requested_units": int(units),
                "limit": int(snap.limit),
                "remaining": int(snap.remaining or 0),
                "plan_code": snap.plan_code,
            },
        )

    record_usage(
        db,
        org_id=int(org_id),
        metric="external_call",
        units=int(units),
        provider=str(provider),
        ref_id=ref_id,
        meta=meta or {},
    )