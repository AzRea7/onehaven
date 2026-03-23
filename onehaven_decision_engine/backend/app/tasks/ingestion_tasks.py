from __future__ import annotations

from ..db import SessionLocal
from ..services.geo_enrichment import enrich_property_geo
from ..services.ingestion_run_execute import execute_source_sync
from ..services.ingestion_scheduler_service import (
    build_jurisdiction_refresh_payload,
    build_location_refresh_payload,
    build_runtime_payload,
    list_default_daily_markets,
    list_jurisdictions_needing_refresh,
    list_properties_needing_location_refresh,
)
from ..services.ingestion_source_service import (
    ensure_default_manual_sources,
    list_sources,
)
from ..services.jurisdiction_notification_service import notify_stale_jurisdictions
from ..services.jurisdiction_refresh_service import (
    DEFAULT_JURISDICTION_STALE_DAYS,
    refresh_jurisdiction_profile,
)
from ..workers.celery_app import celery_app

from ..config import settings
from ..services.ingestion_enrichment_service import refresh_property_rent_assumptions
from ..services.rent_refresh_queue_service import (
    list_properties_for_budgeted_rent_refresh,
    should_queue_rent_refresh_after_sync,
)

def _pipeline_outcome(summary_json: dict | None) -> dict:
    summary = dict(summary_json or {})
    return {
        "records_seen": int(summary.get("records_seen", 0) or 0),
        "records_imported": int(summary.get("records_imported", 0) or 0),
        "properties_created": int(summary.get("properties_created", 0) or 0),
        "properties_updated": int(summary.get("properties_updated", 0) or 0),
        "deals_created": int(summary.get("deals_created", 0) or 0),
        "deals_updated": int(summary.get("deals_updated", 0) or 0),
        "rent_rows_upserted": int(summary.get("rent_rows_upserted", 0) or 0),
        "photos_upserted": int(summary.get("photos_upserted", 0) or 0),
        "duplicates_skipped": int(summary.get("duplicates_skipped", 0) or 0),
        "invalid_rows": int(summary.get("invalid_rows", 0) or 0),
        "filtered_out": int(summary.get("filtered_out", 0) or 0),
        "geo_enriched": int(summary.get("geo_enriched", 0) or 0),
        "risk_scored": int(summary.get("risk_scored", 0) or 0),
        "rent_refreshed": int(summary.get("rent_refreshed", 0) or 0),
        "evaluated": int(summary.get("evaluated", 0) or 0),
        "state_synced": int(summary.get("state_synced", 0) or 0),
        "workflow_synced": int(summary.get("workflow_synced", 0) or 0),
        "next_actions_seeded": int(summary.get("next_actions_seeded", 0) or 0),
        "post_import_failures": int(summary.get("post_import_failures", 0) or 0),
        "post_import_partials": int(summary.get("post_import_partials", 0) or 0),
        "post_import_errors": list(summary.get("post_import_errors") or []),
        "filter_reason_counts": dict(summary.get("filter_reason_counts") or {}),
        "location_automation_enabled": bool(summary.get("location_automation_enabled", False)),
        "normal_path": bool(summary.get("normal_path", True)),
    }


@celery_app.task(name="ingestion.sync_source")
def sync_source_task(
    org_id: int,
    source_id: int,
    trigger_type: str = "manual",
    runtime_config: dict | None = None,
):
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
        summary = dict(getattr(run, "summary_json", None) or {})

        post_sync_rent_queued = 0
        post_sync_rent_property_ids: list[int] = []

        if should_queue_rent_refresh_after_sync():
            burst_limit = int(getattr(settings, "ingestion_post_sync_rent_budget", 5) or 5)
            property_ids = list_properties_for_budgeted_rent_refresh(
                db,
                org_id=int(org_id),
                limit=burst_limit,
            )
            for property_id in property_ids:
                refresh_property_rent_task.delay(int(org_id), int(property_id))
                post_sync_rent_queued += 1
                post_sync_rent_property_ids.append(int(property_id))

        return {
            "ok": True,
            "run_id": getattr(run, "id", None),
            "status": getattr(run, "status", None),
            "post_sync_rent_queued": post_sync_rent_queued,
            "post_sync_rent_property_ids": post_sync_rent_property_ids,
            "summary_json": summary,
            "pipeline_outcome": _pipeline_outcome(summary),
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
            sources = [
                s
                for s in list_sources(db, org_id=int(org_id))
                if bool(getattr(s, "is_enabled", False))
            ]
            for market in markets:
                for source in sources:
                    payload = build_runtime_payload(
                        state=market.get("state"),
                        county=market.get("county"),
                        city=market.get("city"),
                    )
                    sync_source_task.delay(
                        int(org_id),
                        int(source.id),
                        "daily_refresh",
                        payload,
                    )
                    queued += 1
        return {"ok": True, "queued": queued, "markets": markets}
    finally:
        db.close()

@celery_app.task(name="rent.refresh_property")
def refresh_property_rent_task(
    org_id: int,
    property_id: int,
):
    db = SessionLocal()
    try:
        result = refresh_property_rent_assumptions(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        return {
            "ok": bool(result.get("ok")),
            "org_id": int(org_id),
            "property_id": int(property_id),
            "result": result,
        }
    finally:
        db.close()


@celery_app.task(name="rent.refresh_budgeted_batch")
def refresh_budgeted_rent_batch_task(
    org_id: int = 1,
    *,
    limit: int | None = None,
):
    db = SessionLocal()
    try:
        effective_limit = int(
            limit
            or getattr(settings, "ingestion_daily_rent_refresh_limit", 25)
            or 25
        )

        property_ids = list_properties_for_budgeted_rent_refresh(
            db,
            org_id=int(org_id),
            limit=effective_limit,
        )

        queued = 0
        for property_id in property_ids:
            refresh_property_rent_task.delay(int(org_id), int(property_id))
            queued += 1

        return {
            "ok": True,
            "org_id": int(org_id),
            "queued": queued,
            "property_ids": property_ids,
            "limit": effective_limit,
        }
    finally:
        db.close()

@celery_app.task(name="location.refresh_property")
def refresh_property_location_task(
    org_id: int,
    property_id: int,
    force: bool = False,
):
    db = SessionLocal()
    try:
        result = enrich_property_geo(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            force=bool(force),
        )
        return {
            "ok": bool(result.get("ok")),
            "property_id": int(property_id),
            "org_id": int(org_id),
            "force": bool(force),
            "result": result,
        }
    finally:
        db.close()


@celery_app.task(name="location.refresh_stale_properties")
def refresh_stale_locations_task(
    org_id: int = 1,
    *,
    force: bool = False,
    batch_size: int | None = None,
):
    db = SessionLocal()
    try:
        rows = list_properties_needing_location_refresh(
            db,
            org_id=int(org_id),
            batch_size=batch_size,
        )

        queued = 0
        property_ids: list[int] = []
        for row in rows:
            property_id = int(getattr(row, "id"))
            refresh_property_location_task.delay(
                int(org_id),
                property_id,
                bool(force),
            )
            queued += 1
            property_ids.append(property_id)

        return {
            "ok": True,
            "org_id": int(org_id),
            "queued": queued,
            "property_ids": property_ids,
            "force": bool(force),
            "payload": build_location_refresh_payload(force=force, batch_size=batch_size),
        }
    finally:
        db.close()


@celery_app.task(name="jurisdiction.refresh_profile")
def refresh_jurisdiction_profile_task(
    jurisdiction_profile_id: int,
    *,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
):
    db = SessionLocal()
    try:
        result = refresh_jurisdiction_profile(
            db,
            jurisdiction_profile_id=int(jurisdiction_profile_id),
            reviewer_user_id=reviewer_user_id,
            force=bool(force),
            stale_days=int(stale_days),
        )
        return result
    finally:
        db.close()


@celery_app.task(name="jurisdiction.refresh_stale_profiles")
def refresh_stale_jurisdictions_task(
    org_id: int = 1,
    *,
    reviewer_user_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
):
    db = SessionLocal()
    try:
        rows = list_jurisdictions_needing_refresh(
            db,
            org_id=int(org_id),
            batch_size=batch_size,
            stale_days=int(stale_days),
        )

        queued = 0
        jurisdiction_profile_ids: list[int] = []
        payloads: list[dict] = []

        for row in rows:
            jurisdiction_profile_id = int(getattr(row, "id"))
            refresh_jurisdiction_profile_task.delay(
                jurisdiction_profile_id,
                reviewer_user_id=reviewer_user_id,
                force=False,
                stale_days=int(stale_days),
            )
            queued += 1
            jurisdiction_profile_ids.append(jurisdiction_profile_id)
            payloads.append(
                build_jurisdiction_refresh_payload(
                    org_id=getattr(row, "org_id", None),
                    jurisdiction_profile_id=jurisdiction_profile_id,
                    state=getattr(row, "state", None),
                    county=getattr(row, "county", None),
                    city=getattr(row, "city", None),
                    pha_name=getattr(row, "pha_name", None),
                    reason=getattr(row, "stale_reason", None),
                    force=False,
                    stale_days=int(stale_days),
                )
            )

        return {
            "ok": True,
            "org_id": int(org_id),
            "queued": queued,
            "jurisdiction_profile_ids": jurisdiction_profile_ids,
            "payloads": payloads,
        }
    finally:
        db.close()


@celery_app.task(name="jurisdiction.notify_stale_profiles")
def notify_stale_jurisdictions_task(
    org_id: int = 1,
    *,
    force: bool = False,
    limit: int | None = None,
):
    db = SessionLocal()
    try:
        result = notify_stale_jurisdictions(
            db,
            org_id=int(org_id),
            force=bool(force),
            limit=limit,
        )
        return result
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

celery_app.conf.beat_schedule.setdefault(
    "location-refresh-stale-properties",
    {
        "task": "location.refresh_stale_properties",
        "schedule": 6 * 60 * 60,
    },
)

celery_app.conf.beat_schedule.setdefault(
    "jurisdiction-refresh-stale-profiles",
    {
        "task": "jurisdiction.refresh_stale_profiles",
        "schedule": 12 * 60 * 60,
    },
)

celery_app.conf.beat_schedule.setdefault(
    "jurisdiction-notify-stale-profiles",
    {
        "task": "jurisdiction.notify_stale_profiles",
        "schedule": 12 * 60 * 60,
    },
)
