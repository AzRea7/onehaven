from __future__ import annotations

from app.db import SessionLocal
from app.workers.celery_app import celery_app

from ..services.market_sync_service import (
    build_daily_dispatch_plan,
    build_supported_market_sync_plan_for_db,
)
from .ingestion_tasks import sync_source_task

"""
Separate task module for market scheduling.

Why separate from ingestion_tasks.py:
- avoids making the existing file even bigger
- keeps market coverage orchestration distinct from raw source sync
- easier to expand to nationwide region batching later

Chunk 5 additions:
- explicit refresh vs backfill orchestration
- backfill can seed a supported market deeply
- refresh can remain light and incremental
"""


@celery_app.task(name="market_sync.daily_supported_markets")
def daily_supported_markets_task(
    org_id: int = 1,
    *,
    sync_mode: str = "refresh",
):
    db = SessionLocal()
    try:
        dispatches = build_daily_dispatch_plan(
            db,
            org_id=int(org_id),
            sync_mode=sync_mode,
        )

        queued = []
        for item in dispatches:
            job = sync_source_task.delay(
                int(org_id),
                int(item["source_id"]),
                str(item["trigger_type"]),
                dict(item["runtime_config"]),
            )
            queued.append(
                {
                    "task_id": str(job.id),
                    "source_id": int(item["source_id"]),
                    "source_slug": str(item["source_slug"]),
                    "provider": str(item["provider"]),
                    "market_slug": str(item["market"]["slug"]),
                    "sync_mode": str(item.get("sync_mode") or sync_mode),
                    "runtime_config": dict(item["runtime_config"]),
                }
            )

        return {
            "ok": True,
            "org_id": int(org_id),
            "sync_mode": str(sync_mode),
            "queued": len(queued),
            "jobs": queued,
        }
    finally:
        db.close()


@celery_app.task(name="market_sync.sync_supported_market")
def sync_supported_market_task(
    org_id: int,
    market_slug: str,
    *,
    limit: int | None = None,
    sync_mode: str = "refresh",
):
    db = SessionLocal()
    try:
        plan = build_supported_market_sync_plan_for_db(
            db,
            org_id=int(org_id),
            market_slug=str(market_slug),
            limit=limit,
            sync_mode=sync_mode,
        )
        if not bool(plan.get("covered")):
            return plan

        queued = []
        for item in plan["dispatches"]:
            job = sync_source_task.delay(
                int(org_id),
                int(item["source_id"]),
                str(item["trigger_type"]),
                dict(item["runtime_config"]),
            )
            queued.append(
                {
                    "task_id": str(job.id),
                    "source_id": int(item["source_id"]),
                    "source_slug": str(item["source_slug"]),
                    "provider": str(item["provider"]),
                    "market_slug": str(item["market"]["slug"]),
                    "sync_mode": str(item.get("sync_mode") or sync_mode),
                    "runtime_config": dict(item["runtime_config"]),
                }
            )

        plan["queued_jobs"] = queued
        plan["queued"] = len(queued)
        return plan
    finally:
        db.close()


@celery_app.task(name="market_sync.backfill_supported_market")
def backfill_supported_market_task(
    org_id: int,
    market_slug: str,
    *,
    limit: int | None = None,
):
    return sync_supported_market_task.apply(
        args=(int(org_id), str(market_slug)),
        kwargs={"limit": limit, "sync_mode": "backfill"},
    ).get()