# backend/app/tasks/market_sync_tasks.py
from __future__ import annotations

from app.db import SessionLocal
from app.workers.celery_app import celery_app

from ..services.market_sync_service import (
    build_city_dispatch_plan,
    build_daily_dispatch_plan,
)
from .ingestion_tasks import sync_source_task


"""
Separate task module for market scheduling.

Why separate from ingestion_tasks.py:
- avoids making the existing file even bigger
- keeps market coverage orchestration distinct from raw source sync
- easier to expand to nationwide region batching later

Future scaling:
- Add state-level fanout tasks
- Add "refresh only stale markets" task
- Add separate warm/cold cadence tasks
"""


@celery_app.task(name="market_sync.daily_supported_markets")
def daily_supported_markets_task(org_id: int = 1):
    db = SessionLocal()
    try:
        dispatches = build_daily_dispatch_plan(db, org_id=int(org_id))

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
                    "runtime_config": dict(item["runtime_config"]),
                }
            )

        return {
            "ok": True,
            "org_id": int(org_id),
            "queued": len(queued),
            "jobs": queued,
        }
    finally:
        db.close()


@celery_app.task(name="market_sync.sync_city")
def sync_supported_city_task(
    org_id: int,
    city: str,
    state: str = "MI",
):
    db = SessionLocal()
    try:
        plan = build_city_dispatch_plan(db, org_id=int(org_id), city=city, state=state)
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
                    "runtime_config": dict(item["runtime_config"]),
                }
            )

        plan["queued_jobs"] = queued
        plan["queued"] = len(queued)
        return plan
    finally:
        db.close()
