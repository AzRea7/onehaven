from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import IngestionRun, IngestionSource


def start_run(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    trigger_type: str,
    status: str = "running",
    summary_json: dict[str, Any] | None = None,
) -> IngestionRun:
    row = IngestionRun(
        org_id=int(org_id),
        source_id=int(source_id),
        trigger_type=str(trigger_type or "manual"),
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
        summary_json=dict(summary_json or {}),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def finish_run(
    db: Session,
    *,
    row: IngestionRun,
    status: str,
    summary: dict[str, Any] | None = None,
    error_summary: str | None = None,
    error_json: dict[str, Any] | None = None,
) -> IngestionRun:
    summary = dict(summary or {})
    row.status = str(status)
    row.finished_at = datetime.utcnow()
    row.records_seen = int(summary.get("records_seen") or 0)
    row.records_imported = int(summary.get("records_imported") or 0)
    row.properties_created = int(summary.get("properties_created") or 0)
    row.properties_updated = int(summary.get("properties_updated") or 0)
    row.deals_created = int(summary.get("deals_created") or 0)
    row.deals_updated = int(summary.get("deals_updated") or 0)
    row.rent_rows_upserted = int(summary.get("rent_rows_upserted") or 0)
    row.photos_upserted = int(summary.get("photos_upserted") or 0)
    row.duplicates_skipped = int(summary.get("duplicates_skipped") or 0)
    row.invalid_rows = int(summary.get("invalid_rows") or 0)
    row.summary_json = summary
    row.error_summary = error_summary
    row.error_json = error_json
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
            source.last_error_summary = error_summary
            source.status = "error"
        db.add(source)

    db.commit()
    db.refresh(row)
    return row


def _summary_for_list(run: IngestionRun) -> dict[str, Any]:
    summary = dict(getattr(run, "summary_json", None) or {})
    cursor_advanced_to = dict(summary.get("cursor_advanced_to") or {})

    return {
        "new_listings_imported": int(
            summary.get("new_listings_imported", summary.get("new_records_imported", 0)) or 0
        ),
        "already_seen_skipped": int(summary.get("already_seen_skipped") or 0),
        "provider_pages_scanned": int(summary.get("provider_pages_scanned") or 0),
        "market_slug": summary.get("market_slug"),
        "cursor_advanced_to": {
            "market_slug": cursor_advanced_to.get("market_slug"),
            "page": cursor_advanced_to.get("page"),
            "shard": cursor_advanced_to.get("shard"),
            "sort_mode": cursor_advanced_to.get("sort_mode"),
            "page_changed": cursor_advanced_to.get("page_changed"),
        },
        "market_exhausted": bool(summary.get("market_exhausted", False)),
        "sync_mode": summary.get("sync_mode") or "refresh",
        "stop_reason": summary.get("stop_reason"),
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
    last_sync_at = db.scalar(select(func.max(IngestionRun.finished_at)).where(IngestionRun.org_id == int(org_id)))

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