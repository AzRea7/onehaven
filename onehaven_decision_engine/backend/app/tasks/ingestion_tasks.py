from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.exc import DBAPIError, OperationalError

from ..db import SessionLocal
from ..services.geo_enrichment import enrich_property_geo
from ..services.ingestion_run_execute import execute_source_sync
from ..services.ingestion_scheduler_service import (
    build_jurisdiction_refresh_payload,
    build_location_refresh_payload,
    dispatch_daily_sync_for_org,
    list_jurisdictions_needing_refresh,
    list_org_ids_with_enabled_sources,
    list_properties_needing_location_refresh,
)
from ..services.ingestion_source_service import (
    ensure_default_manual_sources,
    get_source,
    list_sources,
    resolve_sources_for_market,
)
from ..services.jurisdiction_notification_service import notify_stale_jurisdictions
from ..services.jurisdiction_refresh_service import (
    DEFAULT_JURISDICTION_STALE_DAYS,
    refresh_jurisdiction_profile,
)
from ..workers.celery_app import celery_app

from ..config import settings
from ..services.ingestion_enrichment_service import refresh_property_rent_assumptions
from ..services.market_sync_service import (
    get_market_sync_state_by_id,
    mark_backfill_completed,
)
from ..services.rent_refresh_queue_service import (
    list_properties_for_budgeted_rent_refresh,
    should_queue_rent_refresh_after_sync,
)

logger = logging.getLogger(__name__)


def _retry_enabled() -> bool:
    return bool(getattr(settings, "ingestion_retry_transient_failures", False))


def _max_retries() -> int:
    return max(0, int(getattr(settings, "ingestion_sync_task_max_retries", 1) or 0))


def _retry_delay_seconds() -> int:
    return max(1, int(getattr(settings, "ingestion_retry_delay_seconds", 30) or 30))


def _is_transient_error(exc: Exception) -> bool:
    return isinstance(exc, (OperationalError, DBAPIError, TimeoutError, ConnectionError))


def _org_scope(org_id: int | None, discovered: list[int]) -> list[int]:
    if org_id is not None:
        return [int(org_id)]
    return discovered or [1]


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
        "market_exhausted": bool(summary.get("market_exhausted", False)),
        "stop_reason": summary.get("stop_reason"),
    }


def _recover_source_for_task(
    db,
    *,
    org_id: int,
    source_id: int,
    runtime_config: dict | None,
):
    source = get_source(db, org_id=int(org_id), source_id=int(source_id))
    if source is not None:
        return source

    cfg = dict(runtime_config or {})
    market_slug = str(cfg.get("market_slug") or "").strip().lower()
    if not market_slug:
        return None

    matches = resolve_sources_for_market(
        db,
        org_id=int(org_id),
        market_slug=market_slug,
    )
    if not matches:
        return None

    enabled = [m for m in matches if bool(getattr(m, "is_enabled", False))]
    if not enabled:
        return None

    enabled.sort(
        key=lambda s: (
            str(getattr(s, "slug", "") or ""),
            int(getattr(s, "id", 0) or 0),
        )
    )
    return enabled[0]


@celery_app.task(
    name="ingestion.sync_source",
    bind=True,
    max_retries=int(getattr(settings, "ingestion_sync_task_max_retries", 1) or 1),
    default_retry_delay=int(getattr(settings, "ingestion_retry_delay_seconds", 30) or 30),
    soft_time_limit=int(getattr(settings, "ingestion_task_soft_time_limit_seconds", 240) or 240),
    time_limit=int(getattr(settings, "ingestion_task_hard_time_limit_seconds", 300) or 300),
)
def sync_source_task(
    self,
    org_id: int,
    source_id: int,
    trigger_type: str = "manual",
    runtime_config: dict | None = None,
):
    db = SessionLocal()
    try:
        source = _recover_source_for_task(
            db,
            org_id=int(org_id),
            source_id=int(source_id),
            runtime_config=runtime_config,
        )
        if source is None:
            return {
                "ok": False,
                "error": "source_not_found",
                "source_id": int(source_id),
                "market_slug": str((runtime_config or {}).get("market_slug") or "").strip().lower() or None,
            }

        effective_runtime_config = dict(runtime_config or {})
        sync_mode = str(effective_runtime_config.get("sync_mode") or "refresh").strip().lower()

        run = execute_source_sync(
            db,
            org_id=int(org_id),
            source=source,
            trigger_type=str(trigger_type or "manual"),
            runtime_config=effective_runtime_config,
        )
        summary = dict(getattr(run, "summary_json", None) or {})

        if sync_mode == "backfill" and bool(effective_runtime_config.get("mark_backfill_complete_on_exhaustion")):
            sync_state = get_market_sync_state_by_id(
                db,
                sync_state_id=effective_runtime_config.get("market_sync_state_id"),
                org_id=int(org_id),
            )
            if sync_state is not None and bool(summary.get("market_exhausted")):
                mark_backfill_completed(db, sync_state=sync_state)

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
            "source_id": int(getattr(source, "id")),
            "sync_mode": sync_mode,
            "post_sync_rent_queued": post_sync_rent_queued,
            "post_sync_rent_property_ids": post_sync_rent_property_ids,
            "summary_json": summary,
            "pipeline_outcome": _pipeline_outcome(summary),
        }
    except Exception as exc:
        logger.exception("sync_source_task_failed", extra={"org_id": int(org_id), "source_id": int(source_id)})
        if _retry_enabled() and _is_transient_error(exc) and int(getattr(self.request, "retries", 0) or 0) < _max_retries():
            raise self.retry(exc=exc, countdown=_retry_delay_seconds())
        raise
    finally:
        db.close()


@celery_app.task(name="ingestion.sync_due_sources")
def sync_due_sources_task(org_id: int | None = None):
    db = SessionLocal()
    try:
        org_ids = _org_scope(org_id, list_org_ids_with_enabled_sources(db))
        queued = 0
        for scoped_org_id in org_ids:
            ensure_default_manual_sources(db, org_id=int(scoped_org_id))
            db.commit()
            for source in list_sources(db, org_id=int(scoped_org_id)):
                if not bool(getattr(source, "is_enabled", False)):
                    continue
                sync_source_task.delay(int(scoped_org_id), int(source.id), "scheduled", {})
                queued += 1
        return {"ok": True, "queued": queued}
    finally:
        db.close()


@celery_app.task(name="ingestion.daily_market_refresh")
def daily_market_refresh_task(org_id: int | None = None):
    db = SessionLocal()
    try:
        org_ids = _org_scope(org_id, list_org_ids_with_enabled_sources(db))
        queued = 0
        runs: list[dict] = []

        for scoped_org_id in org_ids:
            result = dispatch_daily_sync_for_org(
                db,
                org_id=int(scoped_org_id),
                sync_mode="refresh",
                enqueue_sync=lambda _org_id, _source_id, _trigger_type, _payload: sync_source_task.delay(
                    int(_org_id),
                    int(_source_id),
                    _trigger_type,
                    _payload,
                ),
            )
            queued += int(result.get("queued", 0) or 0)
            runs.append(result)

        return {"ok": True, "queued": queued, "sync_mode": "refresh", "runs": runs}
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
            "property_ids": [int(x) for x in property_ids],
        }
    finally:
        db.close()


@celery_app.task(name="location.refresh_stale_properties")
def refresh_stale_locations_task(org_id: int = 1, force: bool = False, batch_size: int | None = None):
    db = SessionLocal()
    try:
        payload = build_location_refresh_payload(force=force, batch_size=batch_size)
        rows = list_properties_needing_location_refresh(
            db,
            org_id=int(org_id),
            batch_size=int(payload["batch_size"]),
        )
        refreshed = 0
        property_ids: list[int] = []

        for row in rows:
            result = enrich_property_geo(
                db,
                org_id=int(org_id),
                property_id=int(row.id),
                force=bool(payload["force"]),
            )
            if bool(result.get("updated")):
                refreshed += 1
            property_ids.append(int(row.id))

        return {
            "ok": True,
            "org_id": int(org_id),
            "refreshed": refreshed,
            "property_ids": property_ids,
        }
    finally:
        db.close()


@celery_app.task(name="jurisdiction.refresh_stale_profiles")
def refresh_stale_jurisdictions_task(org_id: int = 1, stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS):
    db = SessionLocal()
    try:
        payload = build_jurisdiction_refresh_payload(stale_days=stale_days)
        rows = list_jurisdictions_needing_refresh(
            db,
            org_id=int(org_id),
            stale_days=int(payload["stale_days"]),
        )
        refreshed = 0
        results: list[dict[str, Any]] = []

        for row in rows:
            result = refresh_jurisdiction_profile(
                db,
                org_id=int(org_id),
                jurisdiction_code=str(row.jurisdiction_code),
            )
            results.append(result)
            if bool(result.get("ok")):
                refreshed += 1

        return {"ok": True, "org_id": int(org_id), "refreshed": refreshed, "results": results}
    finally:
        db.close()


@celery_app.task(name="jurisdiction.notify_stale_profiles")
def notify_stale_jurisdictions_task(org_id: int = 1, stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS):
    db = SessionLocal()
    try:
        return notify_stale_jurisdictions(
            db,
            org_id=int(org_id),
            stale_days=int(stale_days),
        )
    finally:
        db.close()