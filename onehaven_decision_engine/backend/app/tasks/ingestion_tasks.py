from __future__ import annotations

from app.db import SessionLocal
from app.models import IngestionSource
from app.services.ingestion_run_execute import execute_source_sync
from app.services.ingestion_scheduler_service import due_sources
from app.services.ingestion_source_service import ensure_default_manual_sources, list_sources
from app.workers.celery_app import celery_app


@celery_app.task(name="ingestion.sync_source")
def sync_source_task(
    org_id: int,
    source_id: int,
    trigger_type: str = "scheduled",
    runtime_config: dict | None = None,
):
    db = SessionLocal()
    try:
        source = db.get(IngestionSource, int(source_id))
        if source is None or int(source.org_id) != int(org_id):
            return {"ok": False, "error": "source_not_found"}

        run = execute_source_sync(
            db,
            org_id=int(org_id),
            source=source,
            trigger_type=trigger_type,
            runtime_config=runtime_config or {},
        )
        return {"ok": True, "run_id": int(run.id), "status": run.status}
    finally:
        db.close()


@celery_app.task(name="ingestion.sync_due_sources")
def sync_due_sources_task():
    db = SessionLocal()
    try:
        rows = due_sources(db)
        ids = []
        for source in rows:
            sync_source_task.delay(int(source.org_id), int(source.id), "scheduled", {})
            ids.append(int(source.id))
        return {"ok": True, "count": len(ids), "source_ids": ids}
    finally:
        db.close()


@celery_app.task(name="ingestion.daily_market_refresh")
def daily_market_refresh_task():
    """
    Ensures the default southeast Michigan market sources exist, then queues
    a sync for enabled sources that are intended to stay warm daily.

    This gives you a database that is continuously refreshed without requiring
    a user to manually kick off imports for common markets.
    """
    db = SessionLocal()
    try:
        # Right now this assumes org_id=1 for daily warm sync.
        # If you later want multi-tenant scheduled syncs, move this to a per-org scheduler.
        org_id = 1
        ensure_default_manual_sources(db, org_id=org_id)

        sources = list_sources(db, org_id=org_id)
        queued_ids: list[int] = []

        for source in sources:
            if not bool(source.is_enabled):
                continue

            sync_source_task.delay(
                int(source.org_id),
                int(source.id),
                "daily_refresh",
                {},
            )
            queued_ids.append(int(source.id))

        return {"ok": True, "queued": len(queued_ids), "source_ids": queued_ids}
    finally:
        db.close()
        