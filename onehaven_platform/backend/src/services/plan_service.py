# backend/app/services/plan_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import ApiKey, Plan, Property, UsageLedger

try:
    from onehaven_platform.backend.src.models import Subscription as OrgSubscription  # type: ignore
except Exception:
    from onehaven_platform.backend.src.models import OrgSubscription  # type: ignore


DEFAULT_PLANS: dict[str, dict[str, Any]] = {
    "free": {
        "limits": {
            "max_properties": 3,
            "properties_max": 3,
            "agent_runs_per_day": 20,
            "external_calls_per_day": 10000,
            "automation_runs_per_day": 10,
            "ingestion_runs_per_month": 30,
            "max_concurrent_runs": 2,
            "max_api_keys": 1,
            "jurisdiction_tasks_per_property": 10,
        },
        "features": {
            "daily_sync": False,
            "api_keys": True,
            "premium_automation": False,
            "premium_reporting": False,
            "partner_exports": False,
            "admin_api_scopes": False,
        },
        "soft_limits": {},
    },
    "starter": {
        "limits": {
            "max_properties": 25,
            "properties_max": 25,
            "agent_runs_per_day": 200,
            "external_calls_per_day": 500,
            "automation_runs_per_day": 150,
            "ingestion_runs_per_month": 300,
            "max_concurrent_runs": 5,
            "max_api_keys": 5,
            "jurisdiction_tasks_per_property": 25,
        },
        "features": {
            "daily_sync": True,
            "api_keys": True,
            "premium_automation": False,
            "premium_reporting": False,
            "partner_exports": False,
            "admin_api_scopes": False,
        },
        "soft_limits": {},
    },
    "pro": {
        "limits": {
            "max_properties": 250,
            "properties_max": 250,
            "agent_runs_per_day": 5000,
            "external_calls_per_day": 20000,
            "automation_runs_per_day": 2500,
            "ingestion_runs_per_month": 5000,
            "max_concurrent_runs": 25,
            "max_api_keys": 25,
            "jurisdiction_tasks_per_property": 100,
        },
        "features": {
            "daily_sync": True,
            "api_keys": True,
            "premium_automation": True,
            "premium_reporting": True,
            "partner_exports": True,
            "admin_api_scopes": True,
        },
        "soft_limits": {},
    },
}


@dataclass(frozen=True)
class PlanCheckResult:
    allowed: bool
    reason: str | None
    feature: str | None
    metric: str | None
    used: int | None
    limit: int | None
    remaining: int | None
    plan_code: str


def _now() -> datetime:
    return datetime.utcnow()


def _loads_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return dict(value) if isinstance(value, dict) else {}
    except Exception:
        return {}


def _dumps_json(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _normalize_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    limits = dict(payload.get("limits") or {})
    features = dict(payload.get("features") or {})
    soft_limits = dict(payload.get("soft_limits") or {})

    # backward compatibility with older flat limits_json shape
    for k, v in payload.items():
        if k in {"limits", "features", "soft_limits"}:
            continue
        if k.endswith("_per_day") or k.endswith("_per_month") or k.startswith("max_") or k.endswith("_max"):
            limits.setdefault(k, v)

    if "max_properties" not in limits and "properties_max" in limits:
        limits["max_properties"] = limits["properties_max"]
    if "properties_max" not in limits and "max_properties" in limits:
        limits["properties_max"] = limits["max_properties"]

    return {
        "limits": limits,
        "features": features,
        "soft_limits": soft_limits,
    }


def ensure_default_plans(db: Session) -> None:
    existing = {str(p.code) for p in db.scalars(select(Plan)).all()}
    changed = False

    for code, payload in DEFAULT_PLANS.items():
        if code in existing:
            continue

        normalized = _normalize_plan_payload(payload)
        row_kwargs: dict[str, Any] = {
            "code": code,
            "name": code.title(),
            "limits_json": _dumps_json(normalized),
        }
        if hasattr(Plan, "created_at"):
            row_kwargs["created_at"] = _now()

        db.add(Plan(**row_kwargs))
        changed = True

    if changed:
        db.commit()


def _get_active_subscription(db: Session, *, org_id: int) -> Any | None:
    stmt = select(OrgSubscription).where(OrgSubscription.org_id == int(org_id))
    if hasattr(OrgSubscription, "id"):
        stmt = stmt.order_by(OrgSubscription.id.desc())
    row = db.scalar(stmt)
    if row is None:
        return None

    status = str(getattr(row, "status", "active") or "active").lower()
    if status not in {"active", "trialing", "trial"}:
        return None
    return row


def get_plan_code(db: Session, *, org_id: int) -> str:
    ensure_default_plans(db)
    sub = _get_active_subscription(db, org_id=int(org_id))
    plan_code = str(getattr(sub, "plan_code", "") or "").strip().lower()
    return plan_code or "free"


def get_plan_payload(db: Session, *, org_id: int) -> dict[str, Any]:
    ensure_default_plans(db)

    plan_code = get_plan_code(db, org_id=int(org_id))
    plan = db.scalar(select(Plan).where(Plan.code == str(plan_code)))
    if not plan:
        plan = db.scalar(select(Plan).where(Plan.code == "free"))

    payload = _normalize_plan_payload(_loads_json(getattr(plan, "limits_json", None)))
    return payload


def get_limits(db: Session, *, org_id: int) -> dict[str, Any]:
    return dict(get_plan_payload(db, org_id=int(org_id)).get("limits") or {})


def get_features(db: Session, *, org_id: int) -> dict[str, bool]:
    raw = dict(get_plan_payload(db, org_id=int(org_id)).get("features") or {})
    return {str(k): bool(v) for k, v in raw.items()}


def get_soft_limits(db: Session, *, org_id: int) -> dict[str, Any]:
    return dict(get_plan_payload(db, org_id=int(org_id)).get("soft_limits") or {})


def feature_enabled(db: Session, *, org_id: int, feature: str) -> bool:
    return bool(get_features(db, org_id=int(org_id)).get(str(feature), False))


def get_limit(db: Session, *, org_id: int, metric: str, default: int = 0) -> int:
    limits = get_limits(db, org_id=int(org_id))
    aliases = {
        "max_properties": ["max_properties", "properties_max"],
        "properties_max": ["properties_max", "max_properties"],
    }
    keys = aliases.get(metric, [metric])

    for key in keys:
        try:
            raw = limits.get(key)
            if raw is not None:
                return int(raw or 0)
        except Exception:
            continue
    return int(default)


def _raise_feature_denied(*, plan_code: str, feature: str) -> None:
    raise HTTPException(
        status_code=402,
        detail={
            "error": "feature_not_available",
            "feature": str(feature),
            "plan_code": str(plan_code),
        },
    )


def _raise_limit_exceeded(
    *,
    plan_code: str,
    metric: str,
    used: int,
    limit: int,
    status_code: int = 402,
) -> None:
    raise HTTPException(
        status_code=status_code,
        detail={
            "error": "plan_limit_exceeded",
            "metric": str(metric),
            "used": int(used),
            "limit": int(limit),
            "remaining": max(0, int(limit) - int(used)),
            "plan_code": str(plan_code),
        },
    )


def assert_feature_enabled(db: Session, *, org_id: int, feature: str) -> None:
    plan_code = get_plan_code(db, org_id=int(org_id))
    if not feature_enabled(db, org_id=int(org_id), feature=str(feature)):
        _raise_feature_denied(plan_code=plan_code, feature=str(feature))


def usage_window_for_metric(
    db: Session,
    *,
    org_id: int,
    metric: str,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    now = now or _now()
    metric = str(metric)

    if metric.endswith("_per_day") or metric in {
        "agent_run",
        "external_call",
        "automation_run",
        "premium_action",
    }:
        start = datetime(year=now.year, month=now.month, day=now.day)
        end = start + timedelta(days=1)
        return start, end

    # default to monthly window
    start = datetime(year=now.year, month=now.month, day=1)
    if start.month == 12:
        end = datetime(year=start.year + 1, month=1, day=1)
    else:
        end = datetime(year=start.year, month=start.month + 1, day=1)
    return start, end


def _metric_column_name() -> str:
    return "kind" if hasattr(UsageLedger, "kind") else "metric"


def _provider_column_name() -> str | None:
    if hasattr(UsageLedger, "provider"):
        return "provider"
    return None


def _count_active_api_keys(db: Session, *, org_id: int) -> int:
    q = select(func.count(ApiKey.id)).where(ApiKey.org_id == int(org_id))
    if hasattr(ApiKey, "revoked_at"):
        q = q.where(ApiKey.revoked_at.is_(None))
    if hasattr(ApiKey, "disabled_at"):
        q = q.where(ApiKey.disabled_at.is_(None))
    return int(db.scalar(q) or 0)


def _count_properties(db: Session, *, org_id: int) -> int:
    return int(db.scalar(select(func.count(Property.id)).where(Property.org_id == int(org_id))) or 0)


def _count_ledger_metric(
    db: Session,
    *,
    org_id: int,
    metric: str,
    start: datetime,
    end: datetime,
    provider: str | None = None,
) -> int:
    metric_col = getattr(UsageLedger, _metric_column_name())

    q = select(func.coalesce(func.sum(UsageLedger.units), 0)).where(
        UsageLedger.org_id == int(org_id),
        metric_col == str(metric),
        UsageLedger.created_at >= start,
        UsageLedger.created_at < end,
    )
    provider_col_name = _provider_column_name()
    if provider and provider_col_name:
        q = q.where(getattr(UsageLedger, provider_col_name) == str(provider))

    return int(db.scalar(q) or 0)


def enforce_limit(
    db: Session,
    *,
    org_id: int,
    metric: str,
    used: int,
    add_units: int = 1,
    status_code: int = 402,
) -> PlanCheckResult:
    plan_code = get_plan_code(db, org_id=int(org_id))
    limit = get_limit(db, org_id=int(org_id), metric=str(metric), default=0)

    if limit <= 0:
        return PlanCheckResult(
            allowed=True,
            reason=None,
            feature=None,
            metric=str(metric),
            used=int(used),
            limit=0,
            remaining=None,
            plan_code=plan_code,
        )

    remaining = max(0, int(limit) - int(used))
    allowed = int(used) + int(add_units) <= int(limit)

    if not allowed:
        _raise_limit_exceeded(
            plan_code=plan_code,
            metric=str(metric),
            used=int(used),
            limit=int(limit),
            status_code=status_code,
        )

    return PlanCheckResult(
        allowed=True,
        reason=None,
        feature=None,
        metric=str(metric),
        used=int(used),
        limit=int(limit),
        remaining=max(0, int(limit) - int(used) - int(add_units)),
        plan_code=plan_code,
    )


def can_run_daily_sync(db: Session, *, org_id: int) -> PlanCheckResult:
    plan_code = get_plan_code(db, org_id=int(org_id))
    enabled = feature_enabled(db, org_id=int(org_id), feature="daily_sync")
    return PlanCheckResult(
        allowed=enabled,
        reason=None if enabled else "feature_not_available",
        feature="daily_sync",
        metric=None,
        used=None,
        limit=None,
        remaining=None,
        plan_code=plan_code,
    )


def assert_can_run_daily_sync(db: Session, *, org_id: int) -> None:
    assert_feature_enabled(db, org_id=int(org_id), feature="daily_sync")


def can_create_api_key(db: Session, *, org_id: int) -> PlanCheckResult:
    plan_code = get_plan_code(db, org_id=int(org_id))
    if not feature_enabled(db, org_id=int(org_id), feature="api_keys"):
        return PlanCheckResult(
            allowed=False,
            reason="feature_not_available",
            feature="api_keys",
            metric=None,
            used=None,
            limit=None,
            remaining=None,
            plan_code=plan_code,
        )

    used = _count_active_api_keys(db, org_id=int(org_id))
    limit = get_limit(db, org_id=int(org_id), metric="max_api_keys", default=0)
    remaining = None if limit <= 0 else max(0, limit - used)
    allowed = limit <= 0 or used < limit

    return PlanCheckResult(
        allowed=allowed,
        reason=None if allowed else "plan_limit_exceeded",
        feature="api_keys",
        metric="max_api_keys",
        used=used,
        limit=limit,
        remaining=remaining,
        plan_code=plan_code,
    )


def assert_can_create_api_key(db: Session, *, org_id: int) -> None:
    check = can_create_api_key(db, org_id=int(org_id))
    if not check.allowed:
        if check.reason == "feature_not_available":
            _raise_feature_denied(plan_code=check.plan_code, feature="api_keys")
        _raise_limit_exceeded(
            plan_code=check.plan_code,
            metric="max_api_keys",
            used=int(check.used or 0),
            limit=int(check.limit or 0),
        )


def can_use_premium_automation(db: Session, *, org_id: int) -> PlanCheckResult:
    plan_code = get_plan_code(db, org_id=int(org_id))
    enabled = feature_enabled(db, org_id=int(org_id), feature="premium_automation")
    return PlanCheckResult(
        allowed=enabled,
        reason=None if enabled else "feature_not_available",
        feature="premium_automation",
        metric=None,
        used=None,
        limit=None,
        remaining=None,
        plan_code=plan_code,
    )


def assert_can_use_premium_automation(db: Session, *, org_id: int) -> None:
    assert_feature_enabled(db, org_id=int(org_id), feature="premium_automation")


def required_feature_for_scope(scope: str) -> str | None:
    scope = str(scope).strip().lower()

    premium_scope_map = {
        "reports:premium": "premium_reporting",
        "automation:premium": "premium_automation",
        "partners:export": "partner_exports",
        "admin:apikeys": "admin_api_scopes",
    }
    return premium_scope_map.get(scope)


def assert_scopes_allowed_for_plan(
    db: Session,
    *,
    org_id: int,
    scopes: list[str],
) -> None:
    for scope in scopes:
        feature = required_feature_for_scope(scope)
        if feature:
            assert_feature_enabled(db, org_id=int(org_id), feature=feature)


def jurisdiction_task_limit(db: Session, *, org_id: int) -> int:
    return get_limit(db, org_id=int(org_id), metric="jurisdiction_tasks_per_property", default=0)


def clip_jurisdiction_tasks_for_plan(
    db: Session,
    *,
    org_id: int,
    tasks: list[dict],
) -> dict[str, Any]:
    cap = jurisdiction_task_limit(db, org_id=int(org_id))
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