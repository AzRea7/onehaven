from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import IngestionSource
from products.intelligence.backend.src.services.market_catalog_service import (
    canonical_source_slug_for_market_slug,
    get_active_supported_market_by_slug,
)


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


DEFAULT_SOURCES = [
    {
        "provider": "rentcast",
        "slug": "rentcast-detroit-wayne-sale-listings",
        "display_name": "RentCast Detroit / Wayne County Sale Listings",
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

    if not market_slug and city and county:
        market_slug = f"{_slugify(city)}-{_slugify(county)}"
    elif not market_slug and city:
        market_slug = _slugify(city)

    raw_zip_codes = payload.get("zip_codes") or payload.get("zips") or payload.get("zipCodes") or []
    if isinstance(raw_zip_codes, str):
        zip_codes = [part.strip() for part in raw_zip_codes.split(",") if part.strip()]
    elif isinstance(raw_zip_codes, (list, tuple, set)):
        zip_codes = [str(part).strip() for part in raw_zip_codes if str(part).strip()]
    else:
        zip_codes = []

    query_strategy = _norm_lower(payload.get("query_strategy") or "") or None

    payload["state"] = state
    payload["county"] = county.lower() if county else None
    payload["city"] = city
    payload["market_slug"] = market_slug
    payload["zip_codes"] = list(dict.fromkeys(zip_codes))
    payload["query_strategy"] = query_strategy

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
    incoming_slug = _norm_lower(slug)

    for existing in _list_sources_for_org(db, org_id=int(org_id)):
        if int(getattr(existing, "id", 0) or 0) == int(ignore_source_id or 0):
            continue
        if _norm_lower(getattr(existing, "provider", "")) != "rentcast":
            continue

        existing_slug = _norm_lower(getattr(existing, "slug", ""))
        if incoming_slug and existing_slug and incoming_slug == existing_slug:
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


def _find_equivalent_source_for_default(
    db: Session,
    *,
    org_id: int,
    provider: str,
    desired_config: dict[str, Any],
) -> Optional[IngestionSource]:
    desired_scope = _derive_rentcast_scope(desired_config)
    for existing in _list_sources_for_org(db, org_id=int(org_id)):
        if _norm_lower(getattr(existing, "provider", "")) != _norm_lower(provider):
            continue
        existing_scope = _derive_rentcast_scope(_source_config(existing))
        if desired_scope["scope_key"] == existing_scope["scope_key"]:
            return existing
    return None


def ensure_default_manual_sources(db: Session, *, org_id: int) -> list[IngestionSource]:
    out: list[IngestionSource] = []

    for row in DEFAULT_SOURCES:
        desired_config = _clean_config_json(dict(row.get("config_json") or {}))
        existing = _get_source_by_identity(
            db,
            org_id=int(org_id),
            provider=row["provider"],
            slug=row["slug"],
        )

        if existing is None:
            existing = _find_equivalent_source_for_default(
                db,
                org_id=int(org_id),
                provider=row["provider"],
                desired_config=desired_config,
            )

        desired_status = _derive_status(
            provider=row["provider"],
            credentials_json=(existing.credentials_json if existing is not None else {}) or {},
        )

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
                    existing = _find_equivalent_source_for_default(
                        db,
                        org_id=int(org_id),
                        provider=row["provider"],
                        desired_config=desired_config,
                    )
                if existing is None:
                    raise
        changed = False

        if existing.slug != row["slug"]:
            existing.slug = row["slug"]
            changed = True

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


def _supported_market_source_defaults(market: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": "rentcast",
        "slug": canonical_source_slug_for_market_slug(market["slug"]) or f"rentcast-{_slugify(market['slug'])}-sale-listings",
        "display_name": f"RentCast {market['label']} Sale Listings",
        "source_type": "api",
        "is_enabled": True,
        "sync_interval_minutes": int(market.get("sync_every_hours", 24) or 24) * 60,
        "config_json": _clean_config_json(
            {
                "state": market["state"],
                "city": market.get("city"),
                "county": market.get("county"),
                "market_slug": market["slug"],
                "property_types": market.get("property_types"),
                "max_price": market.get("max_price"),
                "max_units": market.get("max_units"),
                "zip_codes": market.get("zip_codes") or market.get("zips") or market.get("zipCodes"),
                "query_strategy": market.get("query_strategy") or "city_plus_zip_sweep",
            }
        ),
    }


def _repair_supported_market_source(
    db: Session,
    *,
    existing: IngestionSource,
    desired: dict[str, Any],
) -> bool:
    changed = False

    if _norm_lower(existing.provider) != _norm_lower(desired["provider"]):
        existing.provider = desired["provider"]
        changed = True

    if _norm_text(existing.slug) != _norm_text(desired["slug"]):
        existing.slug = desired["slug"]
        changed = True

    if _norm_text(existing.display_name) != _norm_text(desired["display_name"]):
        existing.display_name = desired["display_name"]
        changed = True

    if _norm_text(existing.source_type) != _norm_text(desired["source_type"]):
        existing.source_type = desired["source_type"]
        changed = True

    desired_interval = int(desired["sync_interval_minutes"])
    if int(existing.sync_interval_minutes or 0) != desired_interval:
        existing.sync_interval_minutes = desired_interval
        changed = True

    desired_config = _clean_config_json(dict(desired["config_json"] or {}))
    existing_config = _clean_config_json(dict(existing.config_json or {}))
    if existing_config != desired_config:
        existing.config_json = desired_config
        changed = True

    if not bool(existing.is_enabled):
        existing.is_enabled = True
        changed = True

    desired_status = _derive_status(
        provider=desired["provider"],
        credentials_json=dict(existing.credentials_json or {}),
    )
    if _norm_text(existing.status) != _norm_text(desired_status):
        existing.status = desired_status
        changed = True

    if changed:
        existing.next_scheduled_at = compute_next_scheduled_at(existing)
        db.add(existing)
        db.flush()

    return changed


def _find_overlapping_rentcast_source_for_market(
    db: Session,
    *,
    org_id: int,
    desired: dict[str, Any],
) -> Optional[IngestionSource]:
    desired_cfg = _clean_config_json(dict(desired.get("config_json") or {}))
    desired_scope = _derive_rentcast_scope(desired_cfg)
    desired_market_slug = _norm_lower(desired_cfg.get("market_slug"))
    desired_city = _norm_lower(desired_cfg.get("city"))
    desired_county = _norm_lower(desired_cfg.get("county"))
    desired_state = _norm_upper(desired_cfg.get("state") or "MI")

    candidates: list[tuple[int, IngestionSource]] = []

    for source in _list_sources_for_org(db, org_id=int(org_id)):
        if _norm_lower(getattr(source, "provider", "")) != "rentcast":
            continue

        cfg = _clean_config_json(dict(source.config_json or {}))
        source_scope = _derive_rentcast_scope(cfg)
        source_market_slug = _norm_lower(cfg.get("market_slug"))
        source_city = _norm_lower(cfg.get("city"))
        source_county = _norm_lower(cfg.get("county"))
        source_state = _norm_upper(cfg.get("state") or "MI")
        source_slug = _norm_lower(getattr(source, "slug", ""))

        if source_state != desired_state:
            continue

        score = -1

        if source_market_slug and source_market_slug == desired_market_slug:
            score = 100
        elif desired_market_slug and desired_market_slug in source_slug:
            score = 90
        elif desired_city and desired_county and source_city == desired_city and source_county == desired_county:
            score = 80
        elif desired_city and source_city == desired_city:
            score = 70
        else:
            overlap_reason = _rentcast_overlap_reason(
                incoming_config=desired_cfg,
                existing_config=cfg,
            )
            if overlap_reason:
                if source_scope["scope_type"] == "county" and source_county == desired_county:
                    score = 60
                elif source_scope["scope_type"] == "state":
                    score = 50
                else:
                    score = 40

        if score >= 0:
            candidates.append((score, source))

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            -item[0],
            0 if bool(getattr(item[1], "is_enabled", False)) else 1,
            int(getattr(item[1], "id", 0) or 0),
        )
    )
    return candidates[0][1]


def ensure_sources_for_supported_markets(db: Session, org_id: int) -> dict[str, list[IngestionSource]]:
    from types import SimpleNamespace

    from products.intelligence.backend.src.services.market_catalog_service import list_active_supported_markets

    created: list[IngestionSource] = []
    repaired: list[IngestionSource] = []
    adopted: list[IngestionSource] = []

    markets = list_active_supported_markets()

    for market in markets:
        desired = _supported_market_source_defaults(market)
        slug = desired["slug"]

        existing = _get_source_by_identity(
            db,
            org_id=int(org_id),
            provider=desired["provider"],
            slug=slug,
        )

        if existing is None:
            payload = SimpleNamespace(
                provider=desired["provider"],
                slug=slug,
                display_name=desired["display_name"],
                source_type=desired["source_type"],
                is_enabled=desired["is_enabled"],
                sync_interval_minutes=desired["sync_interval_minutes"],
                config_json=desired["config_json"],
                credentials_json={},
                cursor_json={},
            )
            try:
                src = create_source(db, org_id=org_id, payload=payload)
                created.append(src)
            except ValueError:
                overlapping = _find_overlapping_rentcast_source_for_market(
                    db,
                    org_id=int(org_id),
                    desired=desired,
                )
                if overlapping is None:
                    continue

                if _repair_supported_market_source(
                    db,
                    existing=overlapping,
                    desired=desired,
                ):
                    adopted.append(overlapping)
            except IntegrityError:
                db.rollback()
                existing_after_conflict = _get_source_by_identity(
                    db,
                    org_id=int(org_id),
                    provider=desired["provider"],
                    slug=slug,
                )
                if existing_after_conflict is not None:
                    if _repair_supported_market_source(
                        db,
                        existing=existing_after_conflict,
                        desired=desired,
                    ):
                        repaired.append(existing_after_conflict)
                    continue

                overlapping = _find_overlapping_rentcast_source_for_market(
                    db,
                    org_id=int(org_id),
                    desired=desired,
                )
                if overlapping is None:
                    raise

                if _repair_supported_market_source(
                    db,
                    existing=overlapping,
                    desired=desired,
                ):
                    adopted.append(overlapping)
            continue

        if _repair_supported_market_source(
            db,
            existing=existing,
            desired=desired,
        ):
            repaired.append(existing)

    return {
        "created": created,
        "repaired": repaired,
        "adopted": adopted,
        "touched": [*created, *repaired, *adopted],
    }


def resolve_sources_for_market(db: Session, org_id: int, market_slug: str) -> list[IngestionSource]:
    normalized_market_slug = _norm_lower(market_slug)
    if not normalized_market_slug:
        return []

    ensure_market_slug_on_sources(db, org_id=int(org_id))

    market = get_active_supported_market_by_slug(normalized_market_slug)
    city = _norm_lower((market or {}).get("city"))
    county = _norm_lower((market or {}).get("county"))
    state = _norm_upper((market or {}).get("state") or "MI")
    canonical_slug = canonical_source_slug_for_market_slug(normalized_market_slug)

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

        source_zip_codes = set(str(z).strip() for z in (cfg.get("zip_codes") or []) if str(z).strip())
        market_zip_codes = set(str(z).strip() for z in (((market or {}).get("zip_codes") or [])) if str(z).strip())

        if source_market_slug == normalized_market_slug:
            if market_zip_codes and source_zip_codes and source_zip_codes.isdisjoint(market_zip_codes):
                continue
            exact_market.append(source)
            continue

        if canonical_slug and source_slug == canonical_slug:
            slug_match.append(source)
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
        return sorted(
            exact_market,
            key=lambda s: (0, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)),
        )
    if slug_match:
        return sorted(
            slug_match,
            key=lambda s: (1, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)),
        )
    if legacy_city_county:
        return sorted(
            legacy_city_county,
            key=lambda s: (2, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)),
        )
    if legacy_city_only:
        return sorted(
            legacy_city_only,
            key=lambda s: (3, str(getattr(s, "slug", "") or ""), int(getattr(s, "id", 0) or 0)),
        )

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
