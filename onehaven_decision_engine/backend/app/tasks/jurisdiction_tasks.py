from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from app.config import settings
from app.db import SessionLocal
from app.policy_models import JurisdictionProfile
from app.services.jurisdiction_health_service import get_jurisdiction_health
from app.services.jurisdiction_notification_service import notify_if_jurisdiction_stale
from app.services.jurisdiction_refresh_service import refresh_due_jurisdictions
from app.workers.celery_app import celery_app

log = logging.getLogger("onehaven.jurisdiction.tasks")


def _flags() -> dict[str, bool]:
    return {
        "automation_enabled": bool(getattr(settings, "jurisdiction_automation_enabled", True)),
        "notification_enabled": bool(getattr(settings, "jurisdiction_notification_enabled", True)),
        "critical_stale_lockout_enabled": bool(
            getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)
        ),
    }


def _skip(task_name: str, reason: str) -> dict[str, Any]:
    payload = {
        "ok": True,
        "skipped": True,
        "task": task_name,
        "reason": reason,
        **_flags(),
    }
    log.info("jurisdiction task skipped", extra={"payload": payload})
    return payload


def _run_refresh(task_name: str) -> dict[str, Any]:
    flags = _flags()
    if not flags["automation_enabled"]:
        return _skip(task_name, "jurisdiction automation disabled")

    db = SessionLocal()
    try:
        result = refresh_due_jurisdictions(db, focus="se_mi_extended")
        if isinstance(result, dict):
            result = {**result, "task": task_name, **flags}
        return result
    finally:
        db.close()


@celery_app.task(name="jurisdiction.refresh_stale_profiles")
def refresh_stale_profiles() -> dict:
    return _run_refresh("jurisdiction.refresh_stale_profiles")


@celery_app.task(name="jurisdiction.notify_stale_profiles")
def notify_stale_profiles() -> dict:
    flags = _flags()
    if not flags["automation_enabled"]:
        return _skip("jurisdiction.notify_stale_profiles", "jurisdiction automation disabled")
    if not flags["notification_enabled"]:
        return _skip("jurisdiction.notify_stale_profiles", "jurisdiction notifications disabled")

    db = SessionLocal()
    try:
        rows = list(db.scalars(select(JurisdictionProfile)).all())
        results = []
        for row in rows:
            if bool(getattr(row, "is_stale", False)) or getattr(row, "refresh_state", None) in {
                "blocked",
                "degraded",
                "failed",
            }:
                results.append(notify_if_jurisdiction_stale(db, profile=row))
        return {
            "ok": True,
            "count": len(results),
            "results": results,
            "task": "jurisdiction.notify_stale_profiles",
            **flags,
        }
    finally:
        db.close()


@celery_app.task(name="jurisdiction.retry_discovery")
def retry_discovery() -> dict:
    return _run_refresh("jurisdiction.retry_discovery")


@celery_app.task(name="jurisdiction.retry_validation")
def retry_validation() -> dict:
    return _run_refresh("jurisdiction.retry_validation")


@celery_app.task(name="jurisdiction.recompute_due_profiles")
def recompute_due_profiles() -> dict:
    return _run_refresh("jurisdiction.recompute_due_profiles")


@celery_app.task(name="jurisdiction.health_snapshot")
def jurisdiction_health_snapshot() -> dict:
    flags = _flags()
    if not flags["automation_enabled"]:
        return _skip("jurisdiction.health_snapshot", "jurisdiction automation disabled")

    db = SessionLocal()
    try:
        items = [
            get_jurisdiction_health(db, profile_id=int(row.id))
            for row in list(db.scalars(select(JurisdictionProfile)).all())
        ]
        return {
            "ok": True,
            "count": len(items),
            "items": items[:200],
            "blocked_count": sum(1 for item in items if bool((item.get("lockout") or {}).get("lockout_active"))),
            "degraded_count": sum(1 for item in items if item.get("refresh_state") == "degraded"),
            "task": "jurisdiction.health_snapshot",
            **flags,
        }
    finally:
        db.close()
