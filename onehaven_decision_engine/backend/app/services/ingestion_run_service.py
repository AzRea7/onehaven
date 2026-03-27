from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..db import SessionLocal, rollback_quietly
from ..models import IngestionRun, IngestionSource


def _json_safe(value: Any) -> Any:
    """
    Convert nested Python objects into JSON-safe values for JSON/JSONB columns.
    Datetime and other unsupported types are stringified.
    """
    return json.loads(json.dumps(value, default=str))


def start_run(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    trigger_type: str,
    runtime_config: dict[str, Any] | None = None,
    status: str = "running",
    summary_json: dict[str, Any] | None = None,
) -> IngestionRun:
    summary = _json_safe(dict(summary_json or {}))
    runtime = _json_safe(dict(runtime_config or {}))

    if runtime:
        summary.setdefault("runtime_config", runtime)

        if runtime.get("market_slug") is not None:
            summary.setdefault("market_slug", runtime.get("market_slug"))
        if runtime.get("sync_mode") is not None:
            summary.setdefault("sync_mode", runtime.get("sync_mode"))
        if runtime.get("trigger_type") is not None:
            summary.setdefault("trigger_type", runtime.get("trigger_type"))
        if runtime.get("market_exhausted") is not None:
            summary.setdefault("market_exhausted", runtime.get("market_exhausted"))
        if runtime.get("max_pages_budget") is not None:
            summary.setdefault("max_pages_budget", runtime.get("max_pages_budget"))
        if runtime.get("market_cursor") is not None:
            summary.setdefault("market_cursor", runtime.get("market_cursor"))
        if runtime.get("idempotency_context") is not None:
            summary.setdefault("idempotency_context", runtime.get("idempotency_context"))

    row = IngestionRun(
        org_id=int(org_id),
        source_id=int(source_id),
        trigger_type=str(trigger_type or runtime.get("trigger_type") or "manual"),
        status=str(status or "running"),
        started_at=datetime.utcnow(),
        records_seen=0,
        records_imported=0,
        properties_created=0,
        properties_updated=0,
        deals_created=0,
        deals_updated=0,
        rent_rows_upserted=0,
        photos_upserted=0,
        duplicates_skipped=0,
        invalid_rows=0,
        retry_count=0,
        summary_json=summary,
    )
    db.add(row)
    try:
        db.commit()
    except Exception:
        rollback_quietly(db)
        raise

    db.refresh(row)
    return row


def _apply_run_finish_state(
    row: IngestionRun,
    *,
    status: str,
    final_summary: dict[str, Any],
    error_summary: str | None,
    error_json: dict[str, Any] | None,
) -> None:
    row.status = str(status)
    row.finished_at = datetime.utcnow()
    row.records_seen = int(final_summary.get("records_seen") or 0)
    row.records_imported = int(final_summary.get("records_imported") or 0)
    row.properties_created = int(final_summary.get("properties_created") or 0)
    row.properties_updated = int(final_summary.get("properties_updated") or 0)
    row.deals_created = int(final_summary.get("deals_created") or 0)
    row.deals_updated = int(final_summary.get("deals_updated") or 0)
    row.rent_rows_upserted = int(final_summary.get("rent_rows_upserted") or 0)
    row.photos_upserted = int(final_summary.get("photos_upserted") or 0)
    row.duplicates_skipped = int(final_summary.get("duplicates_skipped") or 0)
    row.invalid_rows = int(final_summary.get("invalid_rows") or 0)
    row.summary_json = final_summary
    row.error_summary = error_summary
    row.error_json = _json_safe(error_json) if error_json is not None else None


def finish_run_in_new_session(
    *,
    run_id: int,
    status: str,
    summary: dict[str, Any] | None = None,
    summary_json: dict[str, Any] | None = None,
    error_summary: str | None = None,
    error_json: dict[str, Any] | None = None,
) -> IngestionRun:
    db = SessionLocal()
    try:
        row = db.get(IngestionRun, int(run_id))
        if row is None:
            raise ValueError(f"IngestionRun not found: run_id={int(run_id)}")
        return finish_run(
            db,
            row,
            status=status,
            summary=summary,
            summary_json=summary_json,
            error_summary=error_summary,
            error_json=error_json,
        )
    except Exception:
        rollback_quietly(db)
        raise
    finally:
        db.close()


def finish_run(
    db: Session,
    row: IngestionRun,
    *,
    status: str,
    summary: dict[str, Any] | None = None,
    summary_json: dict[str, Any] | None = None,
    error_summary: str | None = None,
    error_json: dict[str, Any] | None = None,
) -> IngestionRun:
    final_summary = dict(summary or {})
    if summary_json is not None:
        final_summary = dict(summary_json or {})

    final_summary = _json_safe(final_summary)

    existing_summary = _json_safe(dict(getattr(row, "summary_json", None) or {}))
    if existing_summary:
        final_summary.setdefault("runtime_config", existing_summary.get("runtime_config"))
        final_summary.setdefault("market_slug", existing_summary.get("market_slug"))
        final_summary.setdefault("sync_mode", existing_summary.get("sync_mode"))
        final_summary.setdefault("trigger_type", existing_summary.get("trigger_type"))
        final_summary.setdefault("market_cursor", existing_summary.get("market_cursor"))
        final_summary.setdefault("idempotency_context", existing_summary.get("idempotency_context"))
        final_summary.setdefault("max_pages_budget", existing_summary.get("max_pages_budget"))
        final_summary.setdefault("market_exhausted", existing_summary.get("market_exhausted"))

    _apply_run_finish_state(
        row,
        status=status,
        final_summary=final_summary,
        error_summary=error_summary,
        error_json=error_json,
    )
    db.add(row)

    source = db.get(IngestionSource, int(row.source_id))
    if source is not None:
        source.last_synced_at = row.finished_at
        if status in {"success", "partial", "completed"}:
            source.last_success_at = row.finished_at
            source.last_error_summary = None
            source.status = "healthy"
        else:
            source.last_failure_at = row.finished_at
            source.last_error_summary = error_summary or final_summary.get("error")
            source.status = "error"
        db.add(source)

    db.commit()
    db.refresh(row)
    return row


def _summary_for_list(run: IngestionRun) -> dict[str, Any]:
    summary = dict(getattr(run, "summary_json", None) or {})
    runtime_config = dict(summary.get("runtime_config") or {})
    cursor_advanced_to = dict(summary.get("cursor_advanced_to") or {})
    market_cursor = dict(summary.get("market_cursor") or runtime_config.get("market_cursor") or {})

    return {
        "new_listings_imported": int(
            summary.get("new_listings_imported", summary.get("new_records_imported", 0)) or 0
        ),
        "already_seen_skipped": int(summary.get("already_seen_skipped") or 0),
        "provider_pages_scanned": int(summary.get("provider_pages_scanned") or 0),
        "market_slug": summary.get("market_slug") or runtime_config.get("market_slug"),
        "cursor_advanced_to": {
            "market_slug": cursor_advanced_to.get("market_slug"),
            "page": cursor_advanced_to.get("page"),
            "shard": cursor_advanced_to.get("shard"),
            "sort_mode": cursor_advanced_to.get("sort_mode"),
            "page_changed": cursor_advanced_to.get("page_changed"),
        },
        "market_cursor": {
            "market_slug": market_cursor.get("market_slug"),
            "page": market_cursor.get("page"),
            "shard": market_cursor.get("shard"),
            "sort_mode": market_cursor.get("sort_mode"),
            "refresh_window_pages": market_cursor.get("refresh_window_pages"),
            "page_fingerprint": market_cursor.get("page_fingerprint"),
            "page_changed": market_cursor.get("page_changed"),
            "provider_cursor": market_cursor.get("provider_cursor"),
            "last_page": market_cursor.get("last_page"),
            "market_exhausted": market_cursor.get("market_exhausted"),
            "status": market_cursor.get("status"),
        },
        "market_exhausted": bool(
            summary.get(
                "market_exhausted",
                runtime_config.get("market_exhausted", False),
            )
        ),
        "sync_mode": summary.get("sync_mode") or runtime_config.get("sync_mode") or "refresh",
        "stop_reason": summary.get("stop_reason"),
        "max_pages_budget": summary.get("max_pages_budget") or runtime_config.get("max_pages_budget"),
    }


def list_runs(db: Session, *, org_id: int, limit: int = 25) -> list[dict[str, Any]]:
    rows = db.execute(
        select(IngestionRun, IngestionSource)
        .join(IngestionSource, IngestionSource.id == IngestionRun.source_id)
        .where(IngestionRun.org_id == int(org_id))
        .order_by(IngestionRun.started_at.desc(), IngestionRun.id.desc())
        .limit(int(limit))
    ).all()

    out: list[dict[str, Any]] = []
    for run, source in rows:
        summary = dict(getattr(run, "summary_json", None) or {})
        summary_extras = _summary_for_list(run)

        out.append(
            {
                "id": run.id,
                "source_id": run.source_id,
                "source_label": getattr(source, "display_name", f"Source {run.source_id}"),
                "provider": getattr(source, "provider", "unknown"),
                "trigger_type": run.trigger_type,
                "status": run.status,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "records_seen": run.records_seen,
                "records_imported": run.records_imported,
                "properties_created": getattr(run, "properties_created", 0),
                "properties_updated": getattr(run, "properties_updated", 0),
                "duplicates_skipped": run.duplicates_skipped,
                "invalid_rows": run.invalid_rows,
                "error_summary": run.error_summary,
                "summary_json": summary,
                **summary_extras,
            }
        )
    return out


def get_ingestion_overview(db: Session, *, org_id: int) -> dict[str, Any]:
    now = datetime.utcnow()
    cutoff_24h = now - timedelta(hours=24)
    cutoff_7d = now - timedelta(days=7)

    total_sources = db.scalar(
        select(func.count()).select_from(IngestionSource).where(IngestionSource.org_id == int(org_id))
    ) or 0
    sources_enabled = db.scalar(
        select(func.count()).select_from(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.is_enabled.is_(True),
        )
    ) or 0
    success_runs_24h = db.scalar(
        select(func.count()).select_from(IngestionRun).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
            IngestionRun.status.in_(["success", "partial", "completed"]),
        )
    ) or 0
    failed_runs_24h = db.scalar(
        select(func.count()).select_from(IngestionRun).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
            IngestionRun.status == "failed",
        )
    ) or 0
    records_imported_24h = db.scalar(
        select(func.coalesce(func.sum(IngestionRun.records_imported), 0)).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
        )
    ) or 0
    duplicates_skipped_24h = db.scalar(
        select(func.coalesce(func.sum(IngestionRun.duplicates_skipped), 0)).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
        )
    ) or 0
    properties_created_7d = db.scalar(
        select(func.coalesce(func.sum(IngestionRun.properties_created), 0)).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_7d,
        )
    ) or 0
    properties_updated_7d = db.scalar(
        select(func.coalesce(func.sum(IngestionRun.properties_updated), 0)).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_7d,
        )
    ) or 0
    last_sync_at = db.scalar(
        select(func.max(IngestionRun.finished_at)).where(IngestionRun.org_id == int(org_id))
    )

    return {
        "sources_connected": int(total_sources),
        "sources_enabled": int(sources_enabled),
        "total_sources": int(total_sources),
        "last_sync_at": last_sync_at,
        "success_runs_24h": int(success_runs_24h),
        "failed_runs_24h": int(failed_runs_24h),
        "records_imported_24h": int(records_imported_24h),
        "duplicates_skipped_24h": int(duplicates_skipped_24h),
        "properties_created_7d": int(properties_created_7d),
        "properties_updated_7d": int(properties_updated_7d),
    }


