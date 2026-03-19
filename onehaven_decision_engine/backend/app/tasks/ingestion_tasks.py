from __future__ import annotations

from ..db import SessionLocal
from ..services.ingestion_run_execute import execute_source_sync
from ..services.ingestion_scheduler_service import build_runtime_payload, list_default_daily_markets
from ..services.ingestion_source_service import ensure_default_manual_sources, list_sources
from ..workers.agent_worker import celery_app


@celery_app.task(name="ingestion.sync_source")
def sync_source_task(org_id: int, source_id: int, trigger_type: str = "manual", runtime_config: dict | None = None):
    db = SessionLocal()
    try:
        from ..services.ingestion_source_service import get_source

        source = get_source(db, org_id=int(org_id), source_id=int(source_id))
        if source is None:
            return {"ok": False, "error": "source_not_found", "source_id": source_id}

        run = execute_source_sync(
            db,
            org_id=int(org_id),
            source=source,
            trigger_type=str(trigger_type or "manual"),
            runtime_config=runtime_config or {},
        )
        return {
            "ok": True,
            "run_id": getattr(run, "id", None),
            "status": getattr(run, "status", None),
            "summary_json": getattr(run, "summary_json", None),
        }
    finally:
        db.close()


@celery_app.task(name="ingestion.sync_due_sources")
def sync_due_sources_task():
    db = SessionLocal()
    try:
        org_ids = [1]
        queued = 0
        for org_id in org_ids:
            ensure_default_manual_sources(db, org_id=int(org_id))
            for source in list_sources(db, org_id=int(org_id)):
                if not bool(getattr(source, "is_enabled", False)):
                    continue
                sync_source_task.delay(int(org_id), int(source.id), "scheduled", {})
                queued += 1
        return {"ok": True, "queued": queued}
    finally:
        db.close()


@celery_app.task(name="ingestion.daily_market_refresh")
def daily_market_refresh_task():
    db = SessionLocal()
    try:
        org_ids = [1]
        queued = 0
        markets = list_default_daily_markets()
        for org_id in org_ids:
            ensure_default_manual_sources(db, org_id=int(org_id))
            sources = [s for s in list_sources(db, org_id=int(org_id)) if bool(getattr(s, "is_enabled", False))]
            for market in markets:
                for source in sources:
                    payload = build_runtime_payload(
                        state=market.get("state"),
                        county=market.get("county"),
                        city=market.get("city"),
                    )
                    sync_source_task.delay(int(org_id), int(source.id), "daily_refresh", payload)
                    queued += 1
        return {"ok": True, "queued": queued, "markets": markets}
    finally:
        db.close()


celery_app.conf.beat_schedule.setdefault(
    "ingestion-sync-due-hourly",
    {
        "task": "ingestion.sync_due_sources",
        "schedule": 60 * 60,
    },
)

celery_app.conf.beat_schedule.setdefault(
    "ingestion-daily-market-refresh",
    {
        "task": "ingestion.daily_market_refresh",
        "schedule": 24 * 60 * 60,
    },
)
