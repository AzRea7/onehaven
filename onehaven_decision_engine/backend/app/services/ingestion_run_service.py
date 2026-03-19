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
) -> IngestionRun:
    row = IngestionRun(
        org_id=int(org_id),
        source_id=int(source_id),
        trigger_type=str(trigger_type),
        status="running",
        started_at=datetime.utcnow(),
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
    summary: dict[str, Any],
    error_summary: str | None = None,
    error_json: dict[str, Any] | None = None,
) -> IngestionRun:
    finished_at = datetime.utcnow()

    row.status = status
    row.finished_at = finished_at
    row.records_seen = int(summary.get("records_seen", 0))
    row.records_imported = int(summary.get("records_imported", 0))
    row.properties_created = int(summary.get("properties_created", 0))
    row.properties_updated = int(summary.get("properties_updated", 0))
    row.deals_created = int(summary.get("deals_created", 0))
    row.deals_updated = int(summary.get("deals_updated", 0))
    row.rent_rows_upserted = int(summary.get("rent_rows_upserted", 0))
    row.photos_upserted = int(summary.get("photos_upserted", 0))
    row.duplicates_skipped = int(summary.get("duplicates_skipped", 0))
    row.invalid_rows = int(summary.get("invalid_rows", 0))
    row.summary_json = summary
    row.error_summary = error_summary
    row.error_json = error_json

    source = db.get(IngestionSource, int(row.source_id))
    if source:
        source.last_synced_at = finished_at

        if status in {"success", "partial"}:
            source.last_success_at = finished_at
            source.status = "connected"
            source.last_error_summary = None
            source.last_error_json = None
        else:
            source.last_failure_at = finished_at
            source.status = "error"
            source.last_error_summary = error_summary
            source.last_error_json = error_json

        mins = int(source.sync_interval_minutes or 1440)
        source.next_scheduled_at = finished_at + timedelta(minutes=mins)
        source.updated_at = datetime.utcnow()
        db.add(source)

    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def list_runs(db: Session, *, org_id: int, limit: int = 50) -> list[dict[str, Any]]:
    rows = db.execute(
        select(IngestionRun, IngestionSource)
        .join(IngestionSource, IngestionSource.id == IngestionRun.source_id)
        .where(IngestionRun.org_id == int(org_id))
        .order_by(IngestionRun.started_at.desc())
        .limit(int(limit))
    ).all()

    out: list[dict[str, Any]] = []
    for run, source in rows:
        out.append(
            {
                "id": run.id,
                "source_id": run.source_id,
                "source_label": source.display_name,
                "provider": source.provider,
                "trigger_type": run.trigger_type,
                "status": run.status,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "records_seen": run.records_seen,
                "records_imported": run.records_imported,
                "properties_created": run.properties_created,
                "properties_updated": run.properties_updated,
                "duplicates_skipped": run.duplicates_skipped,
                "invalid_rows": run.invalid_rows,
                "error_summary": run.error_summary,
            }
        )
    return out


def get_ingestion_overview(db: Session, *, org_id: int) -> dict[str, Any]:
    now = datetime.utcnow()
    cutoff_24h = now - timedelta(hours=24)
    cutoff_7d = now - timedelta(days=7)

    total_sources = db.scalar(
        select(func.count()).select_from(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
        )
    ) or 0

    enabled_sources = db.scalar(
        select(func.count()).select_from(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.is_enabled.is_(True),
        )
    ) or 0

    success_runs_24h = db.scalar(
        select(func.count()).select_from(IngestionRun).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
            IngestionRun.status.in_(["success", "partial"]),
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

    duplicates_skipped_24h = db.scalar(
        select(func.coalesce(func.sum(IngestionRun.duplicates_skipped), 0)).where(
            IngestionRun.org_id == int(org_id),
            IngestionRun.started_at >= cutoff_24h,
        )
    ) or 0

    last_sync_at = db.scalar(
        select(func.max(IngestionRun.finished_at)).where(
            IngestionRun.org_id == int(org_id)
        )
    )

    return {
        "sources_connected": int(enabled_sources),
        "sources_enabled": int(enabled_sources),
        "last_sync_at": last_sync_at,
        "success_runs_24h": int(success_runs_24h),
        "failed_runs_24h": int(failed_runs_24h),
        "records_imported_24h": int(records_imported_24h),
        "duplicates_skipped_24h": int(duplicates_skipped_24h),
        "total_sources": int(total_sources),
        "properties_created_7d": int(properties_created_7d),
        "properties_updated_7d": int(properties_updated_7d),
    }
