from __future__ import annotations

from app.db import SessionLocal
from app.workers.celery_app import celery_app
from app.services.ingestion_scheduler_service import collapse_dispatches_to_primary_source
from app.services.market_sync_service import (
    build_daily_dispatch_plan,
    build_supported_market_sync_plan_for_db,
)
from .ingestion_tasks import sync_source_task


@celery_app.task(name="market_sync.daily_supported_markets")
def daily_supported_markets_task(
    org_id: int = 1,
    *,
    sync_mode: str = "refresh",
):
    db = SessionLocal()
    try:
        raw_dispatches = build_daily_dispatch_plan(
            db,
            org_id=int(org_id),
            sync_mode=sync_mode,
        )
        dispatches = collapse_dispatches_to_primary_source(raw_dispatches)

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
                    "dispatch_candidates": int(item.get("dispatch_candidates") or 1),
                    "dispatch_source_ids": [int(x) for x in item.get("dispatch_source_ids") or [item["source_id"]]],
                }
            )

        return {
            "ok": True,
            "org_id": int(org_id),
            "sync_mode": str(sync_mode),
            "dispatches_seen": len(raw_dispatches),
            "dispatches_selected": len(dispatches),
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

        raw_dispatches = list(plan.get("dispatches") or [])
        dispatches = collapse_dispatches_to_primary_source(raw_dispatches)

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
                    "dispatch_candidates": int(item.get("dispatch_candidates") or 1),
                    "dispatch_source_ids": [int(x) for x in item.get("dispatch_source_ids") or [item["source_id"]]],
                }
            )

        plan["dispatches_seen"] = len(raw_dispatches)
        plan["dispatches"] = dispatches
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
    return sync_supported_market_task(
        int(org_id),
        str(market_slug),
        limit=limit,
        sync_mode="backfill",
    )
