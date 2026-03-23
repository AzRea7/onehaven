from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..models import IngestionSource


def _utcnow() -> datetime:
    return datetime.utcnow()


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
        "slug": "rentcast-detroit-sale-listings",
        "display_name": "RentCast Detroit Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 1440,
        "config_json": {
            "state": "MI",
            "city": "Detroit",
            "property_type": None,
            "photo_mode": "placeholder_until_connected",
            "image_backfill_status": "pending",
        },
    },
    {
        "provider": "rentcast",
        "slug": "rentcast-wayne-county-sale-listings",
        "display_name": "RentCast Wayne County Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 1440,
        "config_json": {
            "state": "MI",
            "county": "Wayne",
            "property_type": None,
            "photo_mode": "placeholder_until_connected",
            "image_backfill_status": "pending",
        },
    },
    {
        "provider": "rentcast",
        "slug": "rentcast-oakland-county-sale-listings",
        "display_name": "RentCast Oakland County Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 1440,
        "config_json": {
            "state": "MI",
            "county": "Oakland",
            "property_type": None,
            "photo_mode": "placeholder_until_connected",
            "image_backfill_status": "pending",
        },
    },
    {
        "provider": "rentcast",
        "slug": "rentcast-macomb-county-sale-listings",
        "display_name": "RentCast Macomb County Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 1440,
        "config_json": {
            "state": "MI",
            "county": "Macomb",
            "property_type": None,
            "photo_mode": "placeholder_until_connected",
            "image_backfill_status": "pending",
        },
    },
]


def compute_next_scheduled_at(source: IngestionSource) -> Optional[datetime]:
    if not bool(getattr(source, "is_enabled", False)):
        return None
    mins = int(getattr(source, "sync_interval_minutes", None) or 1440)
    base = getattr(source, "last_synced_at", None) or _utcnow()
    return base + timedelta(minutes=mins)


def _derive_status(*, provider: str, credentials_json: dict | None) -> str:
    provider = (provider or "").strip().lower()
    credentials_json = credentials_json or {}

    if provider == "rentcast":
        return "connected" if _has_rentcast_credentials(credentials_json) else "disconnected"

    return "connected" if credentials_json else "disconnected"


def _get_source_by_identity(
    db: Session,
    *,
    org_id: int,
    provider: str,
    slug: str,
) -> Optional[IngestionSource]:
    return db.scalar(
        select(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.provider == str(provider),
            IngestionSource.slug == str(slug),
        )
    )


def _build_default_source(org_id: int, row: dict) -> IngestionSource:
    source = IngestionSource(
        org_id=int(org_id),
        provider=row["provider"],
        slug=row["slug"],
        display_name=row["display_name"],
        source_type=row["source_type"],
        status=_derive_status(provider=row["provider"], credentials_json={}),
        is_enabled=True,
        sync_interval_minutes=int(row["sync_interval_minutes"]),
        config_json=dict(row.get("config_json") or {}),
        credentials_json={},
        cursor_json={},
    )
    source.next_scheduled_at = compute_next_scheduled_at(source)
    return source


def ensure_default_manual_sources(db: Session, *, org_id: int) -> list[IngestionSource]:
    out: list[IngestionSource] = []

    for row in DEFAULT_SOURCES:
        existing = _get_source_by_identity(
            db,
            org_id=int(org_id),
            provider=row["provider"],
            slug=row["slug"],
        )

        desired_status = _derive_status(
            provider=row["provider"],
            credentials_json=(existing.credentials_json if existing is not None else {}) or {},
        )
        desired_config = dict(row.get("config_json") or {})

        if existing is None:
            existing = _build_default_source(int(org_id), row)
            db.add(existing)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                existing = _get_source_by_identity(
                    db,
                    org_id=int(org_id),
                    provider=row["provider"],
                    slug=row["slug"],
                )
                if existing is None:
                    raise
        else:
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

            desired_enabled = True
            if bool(existing.is_enabled) != desired_enabled:
                existing.is_enabled = desired_enabled
                changed = True

            if existing.status != desired_status:
                existing.status = desired_status
                changed = True

            next_scheduled_at = compute_next_scheduled_at(existing)
            if existing.next_scheduled_at != next_scheduled_at:
                existing.next_scheduled_at = next_scheduled_at
                changed = True

            if changed:
                if hasattr(existing, "updated_at"):
                    existing.updated_at = _utcnow()
                db.add(existing)

        out.append(existing)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        out = []
        for row in DEFAULT_SOURCES:
            existing = _get_source_by_identity(
                db,
                org_id=int(org_id),
                provider=row["provider"],
                slug=row["slug"],
            )
            if existing is None:
                raise
            out.append(existing)

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
    existing = _get_source_by_identity(
        db,
        org_id=int(org_id),
        provider=payload.provider,
        slug=payload.slug,
    )
    if existing is not None:
        return existing

    row = IngestionSource(
        org_id=int(org_id),
        provider=payload.provider,
        slug=payload.slug,
        display_name=payload.display_name,
        source_type=payload.source_type,
        is_enabled=getattr(payload, "is_enabled", True),
        status=_derive_status(
            provider=payload.provider,
            credentials_json=getattr(payload, "credentials_json", None) or {},
        ),
        base_url=getattr(payload, "base_url", None),
        schedule_cron=getattr(payload, "schedule_cron", None),
        sync_interval_minutes=getattr(payload, "sync_interval_minutes", None),
        config_json=getattr(payload, "config_json", None) or {},
        credentials_json=getattr(payload, "credentials_json", None) or {},
        cursor_json={},
    )
    row.next_scheduled_at = compute_next_scheduled_at(row)

    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = _get_source_by_identity(
            db,
            org_id=int(org_id),
            provider=payload.provider,
            slug=payload.slug,
        )
        if existing is not None:
            return existing
        raise

    db.refresh(row)
    return row


def update_source(db: Session, *, row: IngestionSource, payload) -> IngestionSource:
    for field in [
        "display_name",
        "source_type",
        "is_enabled",
        "base_url",
        "schedule_cron",
        "sync_interval_minutes",
        "config_json",
        "credentials_json",
    ]:
        if hasattr(payload, field):
            value = getattr(payload, field)
            if value is not None:
                setattr(row, field, value)

    incoming_status = getattr(payload, "status", None)
    if incoming_status is not None:
        row.status = incoming_status

    row.status = _derive_status(
        provider=row.provider,
        credentials_json=row.credentials_json or {},
    )

    if hasattr(row, "updated_at"):
        row.updated_at = _utcnow()

    row.next_scheduled_at = compute_next_scheduled_at(row)

    db.add(row)
    db.commit()
    db.refresh(row)
    return row
