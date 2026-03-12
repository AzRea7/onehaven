from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import IngestionSource


DEFAULT_SOURCES = [
    {
        "provider": "rentcast",
        "slug": "rentcast-sale-listings",
        "display_name": "RentCast Sale Listings",
        "source_type": "api",
        "status": "disconnected",
        "sync_interval_minutes": 60,
        "config_json": {
            "state": "MI",
            "city": "Detroit",
            "limit": 50,
        },
    },
]


def compute_next_scheduled_at(source: IngestionSource) -> Optional[datetime]:
    if not source.is_enabled:
        return None
    mins = int(source.sync_interval_minutes or 60)
    base = source.last_synced_at or datetime.utcnow()
    return base + timedelta(minutes=mins)


def ensure_default_manual_sources(db: Session, *, org_id: int) -> list[IngestionSource]:
    out: list[IngestionSource] = []
    for row in DEFAULT_SOURCES:
        existing = db.scalar(
            select(IngestionSource).where(
                IngestionSource.org_id == int(org_id),
                IngestionSource.provider == row["provider"],
                IngestionSource.slug == row["slug"],
            )
        )
        if existing is None:
            existing = IngestionSource(
                org_id=int(org_id),
                provider=row["provider"],
                slug=row["slug"],
                display_name=row["display_name"],
                source_type=row["source_type"],
                status=row["status"],
                is_enabled=True,
                sync_interval_minutes=row["sync_interval_minutes"],
                config_json=row.get("config_json") or {},
                credentials_json={},
                cursor_json={},
                next_scheduled_at=datetime.utcnow() + timedelta(minutes=row["sync_interval_minutes"]),
            )
            db.add(existing)
            db.flush()
        out.append(existing)
    db.commit()
    return out


def list_sources(db: Session, *, org_id: int) -> list[IngestionSource]:
    return list(
        db.scalars(
            select(IngestionSource)
            .where(IngestionSource.org_id == int(org_id))
            .order_by(IngestionSource.provider.asc(), IngestionSource.id.asc())
        ).all()
    )


def get_source(db: Session, *, org_id: int, source_id: int) -> Optional[IngestionSource]:
    return db.scalar(
        select(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.id == int(source_id),
        )
    )


def create_source(db: Session, *, org_id: int, payload) -> IngestionSource:
    row = IngestionSource(
        org_id=int(org_id),
        provider=payload.provider,
        slug=payload.slug,
        display_name=payload.display_name,
        source_type=payload.source_type,
        is_enabled=payload.is_enabled,
        status="connected" if payload.credentials_json else "disconnected",
        base_url=payload.base_url,
        schedule_cron=payload.schedule_cron,
        sync_interval_minutes=payload.sync_interval_minutes,
        config_json=payload.config_json or {},
        credentials_json=payload.credentials_json or {},
        cursor_json={},
    )
    row.next_scheduled_at = compute_next_scheduled_at(row)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_source(db: Session, *, row: IngestionSource, payload) -> IngestionSource:
    for field in [
        "display_name",
        "is_enabled",
        "status",
        "base_url",
        "schedule_cron",
        "sync_interval_minutes",
        "config_json",
        "credentials_json",
    ]:
        value = getattr(payload, field, None)
        if value is not None:
            setattr(row, field, value)

    # Auto-upgrade status if credentials now exist.
    if (row.credentials_json or {}).get("api_key"):
        if row.status == "disconnected":
            row.status = "connected"

    row.updated_at = datetime.utcnow()
    row.next_scheduled_at = compute_next_scheduled_at(row)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
