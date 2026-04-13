
from __future__ import annotations

from app.db import SessionLocal
from app.services.jurisdiction_health_service import get_jurisdiction_health
from app.services.jurisdiction_notification_service import notify_if_jurisdiction_stale
from app.services.jurisdiction_refresh_service import refresh_due_jurisdictions
from app.workers.celery_app import celery_app


@celery_app.task(name="jurisdiction.refresh_stale_profiles")
def refresh_stale_profiles() -> dict:
    db = SessionLocal()
    try:
        return refresh_due_jurisdictions(db, focus="se_mi_extended")
    finally:
        db.close()


@celery_app.task(name="jurisdiction.notify_stale_profiles")
def notify_stale_profiles() -> dict:
    db = SessionLocal()
    try:
        from sqlalchemy import select
        from app.policy_models import JurisdictionProfile
        rows = list(db.scalars(select(JurisdictionProfile)).all())
        results = []
        for row in rows:
            if bool(getattr(row, "is_stale", False)) or getattr(row, "refresh_state", None) in {"blocked", "degraded", "failed"}:
                results.append(notify_if_jurisdiction_stale(db, profile=row))
        return {"ok": True, "count": len(results), "results": results}
    finally:
        db.close()


@celery_app.task(name="jurisdiction.retry_discovery")
def retry_discovery() -> dict:
    db = SessionLocal()
    try:
        return refresh_due_jurisdictions(db, focus="se_mi_extended")
    finally:
        db.close()


@celery_app.task(name="jurisdiction.retry_validation")
def retry_validation() -> dict:
    db = SessionLocal()
    try:
        return refresh_due_jurisdictions(db, focus="se_mi_extended")
    finally:
        db.close()


@celery_app.task(name="jurisdiction.recompute_due_profiles")
def recompute_due_profiles() -> dict:
    db = SessionLocal()
    try:
        return refresh_due_jurisdictions(db, focus="se_mi_extended")
    finally:
        db.close()


@celery_app.task(name="jurisdiction.health_snapshot")
def jurisdiction_health_snapshot() -> dict:
    db = SessionLocal()
    try:
        from sqlalchemy import select
        from app.policy_models import JurisdictionProfile
        items = [get_jurisdiction_health(db, profile_id=int(row.id)) for row in list(db.scalars(select(JurisdictionProfile)).all())]
        return {"ok": True, "count": len(items), "items": items[:200]}
    finally:
        db.close()
