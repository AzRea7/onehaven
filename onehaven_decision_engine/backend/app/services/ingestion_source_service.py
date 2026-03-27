from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..models import IngestionSource


def _utcnow() -> datetime:
    return datetime.utcnow()


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_lower(value: Any) -> str:
    return _norm_text(value).lower()


def _norm_upper(value: Any) -> str:
    return _norm_text(value).upper()


def _slugify(value: Any) -> str:
    text = _norm_lower(value)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _has_rentcast_credentials(payload: dict | None = None) -> bool:
    payload = payload or {}
    api_key = (
        payload.get("api_key")
        or os.getenv("RENTCAST_INGESTION_API_KEY")
        or os.getenv("RENTCAST_API_KEY")
        or ""
    ).strip()
    return bool(api_key)


# Hardened default policy:
# - keep a single safe city-scoped default instead of multiple county-wide defaults
# - broad county defaults are exactly what can fan out into overlapping supported-market dispatch
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
            "market_slug": "detroit-wayne",
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
    provider = _norm_lower(provider)
    credentials_json = credentials_json or {}

    if provider == "rentcast":
        return "connected" if _has_rentcast_credentials(credentials_json) else "disconnected"

    return "connected" if credentials_json else "disconnected"


def ensure_sources_for_supported_markets(db: Session, org_id: int) -> list[IngestionSource]:
    from types import SimpleNamespace
    from .market_catalog_service import list_active_supported_markets

    created = []
    markets = list_active_supported_markets()
    existing = list_sources(db, org_id=org_id)
    existing_slugs = {s.slug for s in existing}

    for m in markets:
        slug = f"rentcast-{m['slug']}"

        if slug in existing_slugs:
            continue

        try:
            src = create_source(
                db,
                org_id=org_id,
                payload=SimpleNamespace(
                    provider="rentcast",
                    slug=slug,
                    display_name=f"RentCast {m['label']}",
                    source_type="api",
                    is_enabled=True,
                    sync_interval_minutes=m.get("sync_every_hours", 24) * 60,
                    config_json={
                        "state": m["state"],
                        "city": m.get("city"),
                        "county": m.get("county"),
                        "market_slug": m["slug"],
                        "property_types": m.get("property_types"),
                        "max_price": m.get("max_price"),
                    },
                ),
            )
            created.append(src)
        except Exception:
            # ignore conflicts (already protected by uniqueness)
            continue

    return created

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


def _source_config(source: IngestionSource | None) -> dict[str, Any]:
    if source is None:
        return {}
    raw = getattr(source, "config_json", None) or {}
    return dict(raw) if isinstance(raw, dict) else {}


def _clean_config_json(config_json: dict | None) -> dict[str, Any]:
    payload = dict(config_json or {})
    state = _norm_upper(payload.get("state") or "MI") or "MI"
    county = _norm_text(payload.get("county")) or None
    city = _norm_text(payload.get("city")) or None
    market_slug = _norm_lower(payload.get("market_slug")) or None

    if county:
        county = county.replace(" County", "").replace(" county", "").strip()
    if city:
        city = city.strip()

    payload["state"] = state
    payload["county"] = county
    payload["city"] = city
    payload["market_slug"] = market_slug

    return payload


def _derive_rentcast_scope(config_json: dict | None) -> dict[str, Any]:
    cfg = _clean_config_json(config_json)
    state = _norm_upper(cfg.get("state") or "MI") or "MI"
    county = _norm_lower(cfg.get("county")) or None
    city = _norm_lower(cfg.get("city")) or None
    market_slug = _norm_lower(cfg.get("market_slug")) or None

    if market_slug:
        return {
            "scope_type": "market",
            "scope_key": f"market:{state}:{market_slug}",
            "state": state,
            "county": county,
            "city": city,
            "market_slug": market_slug,
            "is_broad": False,
        }

    if city and county:
        return {
            "scope_type": "city_county",
            "scope_key": f"city_county:{state}:{county}:{city}",
            "state": state,
            "county": county,
            "city": city,
            "market_slug": None,
            "is_broad": False,
        }

    if city:
        return {
            "scope_type": "city",
            "scope_key": f"city:{state}:{city}",
            "state": state,
            "county": None,
            "city": city,
            "market_slug": None,
            "is_broad": False,
        }

    if county:
        return {
            "scope_type": "county",
            "scope_key": f"county:{state}:{county}",
            "state": state,
            "county": county,
            "city": None,
            "market_slug": None,
            "is_broad": True,
        }

    return {
        "scope_type": "state",
        "scope_key": f"state:{state}",
        "state": state,
        "county": None,
        "city": None,
        "market_slug": None,
        "is_broad": True,
    }


def _rentcast_overlap_reason(
    *,
    incoming_config: dict | None,
    existing_config: dict | None,
) -> str | None:
    incoming = _derive_rentcast_scope(incoming_config)
    existing = _derive_rentcast_scope(existing_config)

    if incoming["state"] != existing["state"]:
        return None

    if incoming["scope_key"] == existing["scope_key"]:
        return "same_scope"

    # market slug exact overlap
    if incoming["market_slug"] and existing["market_slug"]:
        if incoming["market_slug"] == existing["market_slug"]:
            return "same_market_slug"

    # county-wide source overlaps any city/city_county/market inside same county
    if incoming["scope_type"] == "county":
        if existing["county"] and incoming["county"] == existing["county"]:
            return "county_overlaps_narrower_scope"

    if existing["scope_type"] == "county":
        if incoming["county"] and incoming["county"] == existing["county"]:
            return "narrower_scope_overlaps_county"

    # state-wide is too broad and overlaps everything in state
    if incoming["scope_type"] == "state":
        return "state_overlaps_existing_scope"

    if existing["scope_type"] == "state":
        return "incoming_scope_overlaps_existing_state"

    # exact city overlap
    if incoming["city"] and existing["city"] and incoming["city"] == existing["city"]:
        if (incoming["county"] or "") == (existing["county"] or ""):
            return "same_city_scope"

    return None


def _list_sources_for_org(db: Session, *, org_id: int) -> list[IngestionSource]:
    return list(
        db.scalars(
            select(IngestionSource)
            .where(IngestionSource.org_id == int(org_id))
            .order_by(IngestionSource.provider.asc(), IngestionSource.id.asc())
        ).all()
    )


def _validate_rentcast_scope_uniqueness(
    db: Session,
    *,
    org_id: int,
    provider: str,
    slug: str,
    config_json: dict | None,
    ignore_source_id: int | None = None,
) -> None:
    if _norm_lower(provider) != "rentcast":
        return

    incoming_config = _clean_config_json(config_json)
    for existing in _list_sources_for_org(db, org_id=int(org_id)):
        if int(getattr(existing, "id", 0) or 0) == int(ignore_source_id or 0):
            continue
        if _norm_lower(getattr(existing, "provider", "")) != "rentcast":
            continue

        reason = _rentcast_overlap_reason(
            incoming_config=incoming_config,
            existing_config=_source_config(existing),
        )
        if not reason:
            continue

        raise ValueError(
            "Conflicting RentCast source scope detected: "
            f"new slug='{slug}' overlaps existing slug='{existing.slug}' "
            f"(reason={reason}). Use one canonical source per logical market."
        )


def _suggest_rentcast_slug(provider: str, config_json: dict | None) -> str:
    cfg = _clean_config_json(config_json)
    provider_slug = _slugify(provider or "source") or "source"
    city = _slugify(cfg.get("city"))
    county = _slugify(cfg.get("county"))
    market_slug = _slugify(cfg.get("market_slug"))
    state = _slugify(cfg.get("state") or "MI") or "mi"

    if market_slug:
        return f"{provider_slug}-{market_slug}-sale-listings"
    if city and county:
        return f"{provider_slug}-{city}-{county}-sale-listings"
    if city:
        return f"{provider_slug}-{city}-sale-listings"
    if county:
        return f"{provider_slug}-{county}-county-sale-listings"
    return f"{provider_slug}-{state}-sale-listings"


def _build_default_source(org_id: int, row: dict) -> IngestionSource:
    clean_config = _clean_config_json(dict(row.get("config_json") or {}))
    source = IngestionSource(
        org_id=int(org_id),
        provider=row["provider"],
        slug=row["slug"],
        display_name=row["display_name"],
        source_type=row["source_type"],
        status=_derive_status(provider=row["provider"], credentials_json={}),
        is_enabled=True,
        sync_interval_minutes=int(row["sync_interval_minutes"]),
        config_json=clean_config,
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
        desired_config = _clean_config_json(dict(row.get("config_json") or {}))

        if existing is None:
            _validate_rentcast_scope_uniqueness(
                db,
                org_id=int(org_id),
                provider=row["provider"],
                slug=row["slug"],
                config_json=desired_config,
            )
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
                _validate_rentcast_scope_uniqueness(
                    db,
                    org_id=int(org_id),
                    provider=existing.provider,
                    slug=existing.slug,
                    config_json=merged_config,
                    ignore_source_id=int(existing.id),
                )
                existing.config_json = merged_config

            # HARDENING:
            # do not force-enable a source the operator intentionally disabled
            # old behavior always set desired_enabled = True
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
    return _list_sources_for_org(db, org_id=int(org_id))


def get_source(db: Session, *, org_id: int, source_id: int) -> Optional[IngestionSource]:
    return db.scalar(
        select(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.id == int(source_id),
        )
    )


def create_source(db: Session, *, org_id: int, payload) -> IngestionSource:
    provider = _norm_lower(getattr(payload, "provider", ""))
    config_json = _clean_config_json(getattr(payload, "config_json", None) or {})

    slug = _norm_text(getattr(payload, "slug", ""))
    if not slug and provider == "rentcast":
        slug = _suggest_rentcast_slug(provider, config_json)

    existing = _get_source_by_identity(
        db,
        org_id=int(org_id),
        provider=provider,
        slug=slug,
    )
    if existing is not None:
        return existing

    _validate_rentcast_scope_uniqueness(
        db,
        org_id=int(org_id),
        provider=provider,
        slug=slug,
        config_json=config_json,
    )

    row = IngestionSource(
        org_id=int(org_id),
        provider=provider,
        slug=slug,
        display_name=getattr(payload, "display_name", None) or slug,
        source_type=getattr(payload, "source_type", None),
        is_enabled=getattr(payload, "is_enabled", True),
        status=_derive_status(
            provider=provider,
            credentials_json=getattr(payload, "credentials_json", None) or {},
        ),
        base_url=getattr(payload, "base_url", None),
        schedule_cron=getattr(payload, "schedule_cron", None),
        sync_interval_minutes=getattr(payload, "sync_interval_minutes", None),
        config_json=config_json,
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
            provider=provider,
            slug=slug,
        )
        if existing is not None:
            return existing
        raise

    db.refresh(row)
    return row


def update_source(db: Session, *, row: IngestionSource, payload) -> IngestionSource:
    next_provider = _norm_lower(getattr(payload, "provider", None) or row.provider)
    next_slug = _norm_text(getattr(payload, "slug", None) or row.slug)
    next_config = _clean_config_json(
        getattr(payload, "config_json", None)
        if hasattr(payload, "config_json") and getattr(payload, "config_json", None) is not None
        else (row.config_json or {})
    )

    for field in [
        "display_name",
        "source_type",
        "is_enabled",
        "base_url",
        "schedule_cron",
        "sync_interval_minutes",
        "credentials_json",
    ]:
        if hasattr(payload, field):
            value = getattr(payload, field)
            if value is not None:
                setattr(row, field, value)

    row.provider = next_provider
    row.slug = next_slug
    row.config_json = next_config

    _validate_rentcast_scope_uniqueness(
        db,
        org_id=int(row.org_id),
        provider=row.provider,
        slug=row.slug,
        config_json=row.config_json,
        ignore_source_id=int(row.id),
    )

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
