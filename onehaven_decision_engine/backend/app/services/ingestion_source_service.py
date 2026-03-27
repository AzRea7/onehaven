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


# Keep one safe manual default so a brand-new org still has something dispatchable.
DEFAULT_SOURCES = [
    {
        "provider": "rentcast",
        "slug": "rentcast-detroit-sale-listings",
        "display_name": "RentCast Detroit Sale Listings",
        "source_type": "api",
        "sync_interval_minutes": 1440,
        "config_json": {
            "state": "MI",
            "county": "wayne",
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

    # Derive market_slug when missing.
    if not market_slug and city and county:
        market_slug = f"{_slugify(city)}-{_slugify(county)}"
    elif not market_slug and city:
        market_slug = _slugify(city)

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

    if incoming["market_slug"] and existing["market_slug"]:
        if incoming["market_slug"] == existing["market_slug"]:
            return "same_market_slug"

    if incoming["scope_type"] == "county":
        if existing["county"] and incoming["county"] == existing["county"]:
            return "county_overlaps_narrower_scope"

    if existing["scope_type"] == "county":
        if incoming["county"] and incoming["county"] == existing["county"]:
            return "narrower_scope_overlaps_county"

    if incoming["scope_type"] == "state":
        return "state_overlaps_existing_scope"

    if existing["scope_type"] == "state":
        return "incoming_scope_overlaps_existing_state"

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


def list_sources(db: Session, *, org_id: int) -> list[IngestionSource]:
    return _list_sources_for_org(db, org_id=org_id)


def get_source(
    db: Session,
    *,
    org_id: int,
    source_id: int,
) -> Optional[IngestionSource]:
    return db.scalar(
        select(IngestionSource).where(
            IngestionSource.org_id == int(org_id),
            IngestionSource.id == int(source_id),
        )
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

            if dict(existing.config_json or {}) != desired_config:
                existing.config_json = desired_config
                changed = True

            if not bool(existing.is_enabled):
                existing.is_enabled = True
                changed = True

            if (existing.status or "") != desired_status:
                existing.status = desired_status
                changed = True

            if changed:
                existing.next_scheduled_at = compute_next_scheduled_at(existing)
                db.add(existing)
                db.flush()

        out.append(existing)

    return out


def ensure_market_slug_on_sources(db: Session, org_id: int) -> list[IngestionSource]:
    """
    Backfills market_slug for legacy rows that only have city/county.
    """
    sources = list_sources(db, org_id=org_id)
    changed_rows: list[IngestionSource] = []

    for source in sources:
        cfg = _clean_config_json(dict(source.config_json or {}))
        old_cfg = dict(source.config_json or {})

        if cfg != old_cfg:
            source.config_json = cfg
            db.add(source)
            changed_rows.append(source)

    if changed_rows:
        db.flush()

    return changed_rows


def ensure_sources_for_supported_markets(db: Session, org_id: int) -> list[IngestionSource]:
    """
    Seeds one canonical RentCast source per supported market when missing.
    This is the main fix that prevents no_dispatchable_sources for market sync.
    """
    from types import SimpleNamespace

    from .market_catalog_service import list_active_supported_markets

    created: list[IngestionSource] = []
    markets = list_active_supported_markets()
    existing = list_sources(db, org_id=org_id)
    existing_slugs = {str(s.slug or "").strip().lower() for s in existing}

    for market in markets:
        slug = f"rentcast-{_slugify(market['slug'])}-sale-listings"
        if slug in existing_slugs:
            continue

        payload = SimpleNamespace(
            provider="rentcast",
            slug=slug,
            display_name=f"RentCast {market['label']} Sale Listings",
            source_type="api",
            is_enabled=True,
            sync_interval_minutes=int(market.get("sync_every_hours", 24) or 24) * 60,
            config_json={
                "state": market["state"],
                "city": market.get("city"),
                "county": market.get("county"),
                "market_slug": market["slug"],
                "property_types": market.get("property_types"),
                "max_price": market.get("max_price"),
                "max_units": market.get("max_units"),
            },
            credentials_json={},
            cursor_json={},
        )

        try:
            src = create_source(db, org_id=org_id, payload=payload)
            created.append(src)
            existing_slugs.add(slug)
        except ValueError:
            # Existing broader/narrower logical equivalent already protects this market.
            continue
        except IntegrityError:
            db.rollback()
            existing_source = _get_source_by_identity(
                db,
                org_id=org_id,
                provider="rentcast",
                slug=slug,
            )
            if existing_source is not None:
                continue
            raise

    return created


def resolve_sources_for_market(db: Session, org_id: int, market_slug: str) -> list[IngestionSource]:
    """
    Resolve enabled sources for a market.

    Match order:
    1. exact config_json.market_slug
    2. slug contains market slug
    3. legacy city+county fallback from supported market catalog
    4. legacy city-only fallback
    """
    from .market_catalog_service import get_active_supported_market_by_slug

    normalized_market_slug = _norm_lower(market_slug)
    if not normalized_market_slug:
        return []

    # Make legacy rows usable before matching.
    ensure_market_slug_on_sources(db, org_id=int(org_id))

    market = get_active_supported_market_by_slug(normalized_market_slug)
    city = _norm_lower((market or {}).get("city"))
    county = _norm_lower((market or {}).get("county"))
    state = _norm_upper((market or {}).get("state") or "MI")

    sources = [
        s
        for s in list_sources(db, org_id=org_id)
        if bool(getattr(s, "is_enabled", False))
    ]

    exact_market: list[IngestionSource] = []
    slug_match: list[IngestionSource] = []
    legacy_city_county: list[IngestionSource] = []
    legacy_city_only: list[IngestionSource] = []

    for source in sources:
        cfg = _clean_config_json(dict(source.config_json or {}))
        source_market_slug = _norm_lower(cfg.get("market_slug"))
        source_city = _norm_lower(cfg.get("city"))
        source_county = _norm_lower(cfg.get("county"))
        source_state = _norm_upper(cfg.get("state") or "MI")
        source_slug = _norm_lower(getattr(source, "slug", ""))

        if source_market_slug == normalized_market_slug:
            exact_market.append(source)
            continue

        if normalized_market_slug and normalized_market_slug in source_slug:
            slug_match.append(source)
            continue

        if (
            city
            and county
            and source_state == state
            and source_city == city
            and source_county == county
        ):
            legacy_city_county.append(source)
            continue

        if city and source_state == state and source_city == city:
            legacy_city_only.append(source)
            continue

    if exact_market:
        return sorted(exact_market, key=lambda s: (0, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)))
    if slug_match:
        return sorted(slug_match, key=lambda s: (1, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)))
    if legacy_city_county:
        return sorted(legacy_city_county, key=lambda s: (2, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)))
    if legacy_city_only:
        return sorted(legacy_city_only, key=lambda s: (3, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)))

    return []


def create_source(db: Session, *, org_id: int, payload: Any) -> IngestionSource:
    provider = _norm_lower(getattr(payload, "provider", ""))
    config_json = _clean_config_json(dict(getattr(payload, "config_json", {}) or {}))
    slug = _norm_text(getattr(payload, "slug", "")) or _suggest_rentcast_slug(provider, config_json)

    _validate_rentcast_scope_uniqueness(
        db,
        org_id=int(org_id),
        provider=provider,
        slug=slug,
        config_json=config_json,
    )

    credentials_json = dict(getattr(payload, "credentials_json", {}) or {})
    cursor_json = dict(getattr(payload, "cursor_json", {}) or {})

    source = IngestionSource(
        org_id=int(org_id),
        provider=provider,
        slug=slug,
        display_name=_norm_text(getattr(payload, "display_name", "")) or slug,
        source_type=_norm_text(getattr(payload, "source_type", "")) or "api",
        status=_derive_status(provider=provider, credentials_json=credentials_json),
        is_enabled=bool(getattr(payload, "is_enabled", True)),
        sync_interval_minutes=int(getattr(payload, "sync_interval_minutes", 1440) or 1440),
        config_json=config_json,
        credentials_json=credentials_json,
        cursor_json=cursor_json,
    )
    source.next_scheduled_at = compute_next_scheduled_at(source)

    db.add(source)
    db.flush()
    return source


def update_source(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    payload: Any,
) -> IngestionSource:
    source = get_source(db, org_id=org_id, source_id=source_id)
    if source is None:
        raise ValueError("source_not_found")

    provider = _norm_lower(getattr(payload, "provider", source.provider))
    slug = _norm_text(getattr(payload, "slug", source.slug)) or source.slug
    config_json = _clean_config_json(
        dict(getattr(payload, "config_json", source.config_json) or {})
    )

    _validate_rentcast_scope_uniqueness(
        db,
        org_id=int(org_id),
        provider=provider,
        slug=slug,
        config_json=config_json,
        ignore_source_id=int(source.id),
    )

    source.provider = provider
    source.slug = slug
    source.display_name = _norm_text(getattr(payload, "display_name", source.display_name)) or source.display_name
    source.source_type = _norm_text(getattr(payload, "source_type", source.source_type)) or source.source_type
    source.is_enabled = bool(getattr(payload, "is_enabled", source.is_enabled))
    source.sync_interval_minutes = int(getattr(payload, "sync_interval_minutes", source.sync_interval_minutes) or 1440)
    source.config_json = config_json
    source.credentials_json = dict(getattr(payload, "credentials_json", source.credentials_json) or {})
    source.cursor_json = dict(getattr(payload, "cursor_json", source.cursor_json) or {})
    source.status = _derive_status(provider=provider, credentials_json=source.credentials_json)
    source.next_scheduled_at = compute_next_scheduled_at(source)

    db.add(source)
    db.flush()
    return source


def delete_source(
    db: Session,
    *,
    org_id: int,
    source_id: int,
) -> bool:
    source = get_source(db, org_id=org_id, source_id=source_id)
    if source is None:
        return False
    db.delete(source)
    db.flush()
    return True