from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import IngestionSource


def _has_rentcast_credentials(payload: dict | None = None) -> bool:
    payload = payload or {}
    api_key = (
        payload.get("api_key")
        or os.getenv("RENTCAST_INGESTION_API_KEY")
        or os.getenv("RENTCAST_API_KEY")
        or ""
    ).strip()
    return bool(api_key)


DEFAULT_SOURCES = [
    {
        "provider": "rentcast",
        "slug": "rentcast-sale-listings",
        "display_name": "RentCast Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 60,
        "config_json": {
            "state": "MI",
            "city": "Detroit",
            "limit": 50,
            "photo_mode": "placeholder_until_connected",
            "image_backfill_status": "pending",
        },
    },
]


def compute_next_scheduled_at(source: IngestionSource) -> Optional[datetime]:
    if not source.is_enabled:
        return None
    mins = int(source.sync_interval_minutes or 60)
    base = source.last_synced_at or datetime.utcnow()
    return base + timedelta(minutes=mins)


def _derive_status(*, provider: str, credentials_json: dict | None) -> str:
    provider = (provider or "").strip().lower()
    credentials_json = credentials_json or {}

    if provider == "rentcast":
        return "connected" if _has_rentcast_credentials(credentials_json) else "disconnected"

    return "connected" if credentials_json else "disconnected"


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

        desired_status = _derive_status(
            provider=row["provider"],
            credentials_json={},
        )

        desired_config = row.get("config_json") or {}

        if existing is None:
            existing = IngestionSource(
                org_id=int(org_id),
                provider=row["provider"],
                slug=row["slug"],
                display_name=row["display_name"],
                source_type=row["source_type"],
                status=desired_status,
                is_enabled=True,
                sync_interval_minutes=row["sync_interval_minutes"],
                config_json=desired_config,
                credentials_json={},
                cursor_json={},
                next_scheduled_at=datetime.utcnow() + timedelta(minutes=row["sync_interval_minutes"]),
            )
            db.add(existing)
            db.flush()
        else:
            # Keep old rows aligned with current product truth.
            changed = False

            if existing.display_name != row["display_name"]:
                existing.display_name = row["display_name"]
                changed = True

            if existing.source_type != row["source_type"]:
                existing.source_type = row["source_type"]
                changed = True

            if int(existing.sync_interval_minutes or 0) != int(row["sync_interval_minutes"]):
                existing.sync_interval_minutes = int(row["sync_interval_minutes"])
                changed = True

            merged_config = dict(existing.config_json or {})
            for k, v in desired_config.items():
                if merged_config.get(k) != v:
                    merged_config[k] = v
                    changed = True
            if changed:
                existing.config_json = merged_config

            derived_status = _derive_status(
                provider=existing.provider,
                credentials_json=existing.credentials_json or {},
            )
            if existing.status != derived_status:
                existing.status = derived_status
                changed = True

            if changed:
                existing.updated_at = datetime.utcnow()
                existing.next_scheduled_at = compute_next_scheduled_at(existing)
                db.add(existing)

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
        status=_derive_status(
            provider=payload.provider,
            credentials_json=payload.credentials_json or {},
        ),
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
        "base_url",
        "schedule_cron",
        "sync_interval_minutes",
        "config_json",
        "credentials_json",
    ]:
        value = getattr(payload, field, None)
        if value is not None:
            setattr(row, field, value)

    # If caller explicitly set status, honor it only temporarily, then normalize below.
    incoming_status = getattr(payload, "status", None)
    if incoming_status is not None:
        row.status = incoming_status

    row.status = _derive_status(
        provider=row.provider,
        credentials_json=row.credentials_json or {},
    )
    row.updated_at = datetime.utcnow()
    row.next_scheduled_at = compute_next_scheduled_at(row)

    db.add(row)
    db.commit()
    db.refresh(row)
    return row