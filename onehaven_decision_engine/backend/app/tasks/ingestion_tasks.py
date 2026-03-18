from __future__ import annotations

from app.db import SessionLocal
from app.models import IngestionSource
from app.services.ingestion_run_execute import execute_source_sync
from app.services.ingestion_scheduler_service import due_sources
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