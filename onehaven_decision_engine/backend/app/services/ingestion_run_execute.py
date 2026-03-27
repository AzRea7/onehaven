from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, select, text
from sqlalchemy.orm import Session

from ..config import settings
from ..middleware.structured_logging import emit_structured_log
from ..models import Deal, Property, PropertyPhoto, RentAssumption
from ..services.ingestion_dedupe_service import (
    build_property_fingerprint,
    find_existing_by_external_id,
    find_existing_property,
    upsert_record_link,
)
from ..services.ingestion_enrichment_service import (
    apply_pipeline_summary,
    canonical_listing_payload,
    derive_photo_kind,
    execute_post_ingestion_pipeline,
)
from ..services.ingestion_run_service import finish_run, start_run
from ..services.locks_service import (
    acquire_ingestion_execution_lock,
    build_ingestion_execution_lock_key,
    clear_stale_lock,
    has_completed_ingestion_dataset,
    mark_ingestion_dataset_completed,
    release_ingestion_execution_lock,
)
from ..services.market_sync_service import (
    advance_market_cursor,
    build_market_dataset_identity,
    get_market_sync_state_by_id,
    mark_market_sync_completed,
    mark_market_sync_started,
)
from ..services.rentcast_listing_source import (
    RentCastListingFetchResult,
    RentCastListingSource,
)

logger = logging.getLogger(__name__)

PROVIDER_ADAPTERS = {"rentcast": RentCastListingSource()}

DEFAULT_EXECUTION_LOCK_TTL_SECONDS = 60 * 60 * 3
DEFAULT_COMPLETION_LOCK_TTL_SECONDS = 60 * 60 * 24 * 14


def _emit(payload: dict[str, Any], level: int = logging.INFO) -> None:
    emit_structured_log("onehaven.ingestion", payload, level=level)


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_optional_filter_value(value: Any) -> str | None:
    s = str(value or "").strip()
    if not s:
        return None
    if s.lower() in {"any", "all", "none", "null"}:
        return None
    return s


def _normalize_county_text(value: Any) -> str:
    s = _norm_text(value)
    if s.endswith(" county"):
        s = s[:-7].strip()
    return s


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _utcnow_naive() -> datetime:
    return datetime.utcnow()


def _normalize_runtime_config(runtime_config: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(runtime_config or {})
    limit = _safe_int(payload.get("limit")) or int(
        getattr(settings, "market_sync_default_limit_per_market", 125)
    )
    payload["limit"] = max(1, limit)

    payload["state"] = _normalize_optional_filter_value(payload.get("state")) or "MI"
    payload["county"] = _normalize_optional_filter_value(payload.get("county"))
    payload["city"] = _normalize_optional_filter_value(payload.get("city"))

    if payload.get("market_slug") or payload.get("city"):
        payload["county"] = None

    if bool(getattr(settings, "ingestion_disable_county_fallback_variants", True)) and (
        payload.get("market_slug") or payload.get("city")
    ):
        payload["county"] = None

    property_types = payload.get("property_types")
    if isinstance(property_types, str):
        property_types = [x.strip() for x in property_types.split(",") if x.strip()]
    if not payload.get("city") and not payload.get("market_slug"):
        raise ValueError("Ingestion requires city or market_slug (county-only disabled)")
    if not property_types:
        property_types = ["single_family", "multi_family"]
    payload["property_types"] = [str(x).strip() for x in property_types if str(x).strip()]

    payload["max_price"] = _safe_float(payload.get("max_price"))
    if payload.get("max_price") is None:
        payload["max_price"] = float(getattr(settings, "investor_buy_box_max_price", 200_000))

    payload["sync_mode"] = str(payload.get("sync_mode") or "refresh").strip().lower() or "refresh"
    payload["market_slug"] = _normalize_optional_filter_value(payload.get("market_slug"))
    payload["max_pages_budget"] = _safe_int(payload.get("max_pages_budget"))

    payload["defer_optional_post_pipeline"] = bool(
        payload.get(
            "defer_optional_post_pipeline",
            getattr(settings, "ingestion_defer_optional_post_pipeline", True),
        )
    )

    payload.pop("zip_code", None)
    payload.pop("address", None)
    payload.pop("max_units", None)
    payload.pop("min_price", None)
    payload.pop("min_bedrooms", None)
    payload.pop("min_bathrooms", None)
    return payload


def _normalize_property_type(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""

    cleaned = re.sub(r"[\s\-]+", "_", raw)
    cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)

    single_family_aliases = {
        "single_family",
        "singlefamily",
        "single_family_home",
        "single_family_residential",
        "house",
        "detached",
        "sfh",
        "residential",
    }
    multi_family_aliases = {
        "multi_family",
        "multifamily",
        "multi_family_home",
        "duplex",
        "triplex",
        "fourplex",
        "2_family",
        "3_family",
        "4_family",
    }

    if cleaned in single_family_aliases:
        return "single_family"
    if cleaned in multi_family_aliases:
        return "multi_family"
    return cleaned


def _runtime_for_idempotency(runtime_config: dict[str, Any]) -> dict[str, Any]:
    stable = {
        "trigger_type": runtime_config.get("trigger_type"),
        "state": runtime_config.get("state"),
        "county": runtime_config.get("county"),
        "city": runtime_config.get("city"),
        "property_types": runtime_config.get("property_types"),
        "max_price": runtime_config.get("max_price"),
        "limit": runtime_config.get("limit"),
        "idempotency_context": runtime_config.get("idempotency_context") or {},
        "market_cursor": runtime_config.get("market_cursor") or {},
        "market_slug": runtime_config.get("market_slug"),
        "market_sync_state_id": runtime_config.get("market_sync_state_id"),
        "sync_mode": runtime_config.get("sync_mode"),
    }
    return stable


def _json_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def build_run_idempotency_key(
    *,
    org_id: int,
    source: Any,
    trigger_type: str,
    runtime_config: dict[str, Any],
) -> str:
    source_provider = str(getattr(source, "provider", "") or "").strip().lower()
    source_slug = str(getattr(source, "slug", "") or "").strip().lower()
    source_cursor = dict(getattr(source, "cursor_json", {}) or {})
    context = dict(runtime_config.get("idempotency_context") or {})

    if context:
        base = {
            "org_id": int(org_id),
            "source_id": int(getattr(source, "id")),
            "trigger_type": str(trigger_type),
            "provider": source_provider,
            "slug": source_slug,
            "runtime": _runtime_for_idempotency(runtime_config),
        }
        return _json_hash(base)

    base = {
        "org_id": int(org_id),
        "source_id": int(getattr(source, "id")),
        "trigger_type": str(trigger_type),
        "provider": source_provider,
        "slug": source_slug,
        "cursor": source_cursor,
        "runtime": _runtime_for_idempotency(runtime_config),
    }
    return _json_hash(base)


def _build_lock_owner(*, org_id: int, source_id: int, dataset_key: str) -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"ingestion:{host}:{pid}:{int(org_id)}:{int(source_id)}:{dataset_key}"


def _persist_property_acquisition_metadata(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    source: Any,
    payload: dict[str, Any],
    trigger_type: str,
) -> None:
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            """
            UPDATE properties
            SET acquisition_first_seen_at = COALESCE(acquisition_first_seen_at, :now_ts),
                acquisition_last_seen_at = :now_ts,
                acquisition_source_provider = :provider,
                acquisition_source_slug = :slug,
                acquisition_source_record_id = :record_id,
                acquisition_source_url = COALESCE(:source_url, acquisition_source_url),
                acquisition_metadata_json = COALESCE(acquisition_metadata_json, '{}'::jsonb) || CAST(:metadata_json AS JSONB)
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "now_ts": now,
            "provider": str(getattr(source, "provider", "") or "").strip() or None,
            "slug": str(getattr(source, "slug", "") or "").strip() or None,
            "record_id": str(payload.get("external_record_id") or "").strip() or None,
            "source_url": payload.get("external_url"),
            "metadata_json": json.dumps(
                {
                    "trigger_type": trigger_type,
                    "last_payload_address": payload.get("address"),
                    "last_payload_city": payload.get("city"),
                    "inventory_count": payload.get("inventory_count"),
                },
                default=str,
            ),
        },
    )


def _seed_missing_completeness_columns(db: Session, *, org_id: int, property_id: int) -> None:
    db.execute(
        text(
            """
            UPDATE properties
            SET completeness_geo_status = COALESCE(completeness_geo_status, 'missing'),
                completeness_rent_status = COALESCE(completeness_rent_status, 'missing'),
                completeness_rehab_status = COALESCE(completeness_rehab_status, 'missing'),
                completeness_risk_status = COALESCE(completeness_risk_status, 'missing'),
                completeness_jurisdiction_status = COALESCE(completeness_jurisdiction_status, 'missing'),
                completeness_cashflow_status = COALESCE(completeness_cashflow_status, 'missing')
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    )


def _upsert_property(db: Session, *, org_id: int, payload: dict[str, Any]):
    existing = find_existing_property(
        db,
        org_id=org_id,
        address=payload["address"],
        city=payload["city"],
        state=payload["state"],
        zip_code=payload["zip"],
    )

    created = False
    if existing is None:
        existing = Property(
            org_id=int(org_id),
            address=payload["address"],
            city=payload["city"],
            county=payload.get("county"),
            state=payload["state"],
            zip=payload["zip"],
            bedrooms=int(payload["bedrooms"] or 0),
            bathrooms=float(payload["bathrooms"] or 1),
            square_feet=payload.get("square_feet"),
            year_built=payload.get("year_built"),
            property_type=payload.get("property_type") or "single_family",
        )
        db.add(existing)
        db.flush()
        created = True
    else:
        existing.county = payload.get("county") or getattr(existing, "county", None)
        existing.bedrooms = int(payload["bedrooms"] or existing.bedrooms or 0)
        existing.bathrooms = float(payload["bathrooms"] or existing.bathrooms or 1)
        existing.square_feet = payload.get("square_feet") or existing.square_feet
        existing.year_built = payload.get("year_built") or existing.year_built
        existing.property_type = payload.get("property_type") or existing.property_type
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_deal(db: Session, *, org_id: int, property_id: int, payload: dict[str, Any]):
    existing = db.scalar(
        select(Deal)
        .where(
            Deal.org_id == int(org_id),
            Deal.property_id == int(property_id),
        )
        .order_by(desc(Deal.id))
    )

    created = False
    if existing is None:
        existing = Deal(
            org_id=int(org_id),
            property_id=int(property_id),
            snapshot_id=None,
            asking_price=float(payload.get("asking_price") or 0),
            estimated_purchase_price=payload.get("estimated_purchase_price"),
            rehab_estimate=float(payload.get("rehab_estimate") or 0),
            source=payload.get("source", "ingestion"),
            strategy="section8",
        )
        db.add(existing)
        db.flush()
        created = True
    else:
        existing.asking_price = float(payload.get("asking_price") or existing.asking_price or 0)
        if payload.get("estimated_purchase_price") is not None:
            existing.estimated_purchase_price = payload.get("estimated_purchase_price")
        existing.rehab_estimate = float(payload.get("rehab_estimate") or existing.rehab_estimate or 0)
        existing.source = payload.get("source", existing.source)
        existing.snapshot_id = None
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_rent_assumption(db: Session, *, org_id: int, property_id: int, payload: dict[str, Any]):
    existing = db.scalar(
        select(RentAssumption)
        .where(
            RentAssumption.org_id == int(org_id),
            RentAssumption.property_id == int(property_id),
        )
        .order_by(desc(RentAssumption.id))
    )

    created = False
    if existing is None:
        existing = RentAssumption(
            org_id=int(org_id),
            property_id=int(property_id),
            market_rent_estimate=payload.get("market_rent_estimate"),
            section8_fmr=payload.get("section8_fmr"),
            approved_rent_ceiling=payload.get("approved_rent_ceiling"),
            inventory_count=payload.get("inventory_count"),
        )
        db.add(existing)
        db.flush()
        created = True
    else:
        for key in ["market_rent_estimate", "section8_fmr", "approved_rent_ceiling", "inventory_count"]:
            if payload.get(key) is not None:
                setattr(existing, key, payload.get(key))
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_photos(db: Session, *, org_id: int, property_id: int, provider: str, photos: list[Any]) -> int:
    count = 0
    for p in photos:
        url = p["url"] if isinstance(p, dict) else str(p)
        kind = p.get("kind") if isinstance(p, dict) else derive_photo_kind(url)

        existing = db.scalar(
            select(PropertyPhoto).where(
                PropertyPhoto.org_id == int(org_id),
                PropertyPhoto.property_id == int(property_id),
                PropertyPhoto.url == url,
            )
        )
        if existing is None:
            db.add(
                PropertyPhoto(
                    org_id=int(org_id),
                    property_id=int(property_id),
                    url=url,
                    kind=kind or "unknown",
                    source=provider,
                )
            )
            count += 1

    db.flush()
    return count


def _starting_cursor(source, trigger_type: str, runtime_config: dict[str, Any] | None = None) -> dict[str, Any]:
    runtime_config = dict(runtime_config or {})
    market_cursor = runtime_config.get("market_cursor")
    if isinstance(market_cursor, dict) and market_cursor:
        return dict(market_cursor)

    if trigger_type in {"manual", "daily_refresh", "scheduled", "manual_market_sync", "market_backfill"}:
        return dict(getattr(source, "cursor_json", None) or {"page": 1})

    return dict(getattr(source, "cursor_json", None) or {})


def _filter_reason(
    payload: dict[str, Any],
    runtime_config: dict[str, Any],
    *,
    query_variant: dict[str, Any] | None = None,
) -> str | None:
    if not runtime_config:
        return None

    state = _normalize_optional_filter_value(runtime_config.get("state"))
    runtime_city = _normalize_optional_filter_value(runtime_config.get("city"))
    active_variant = dict(query_variant or {})

    query_city = _normalize_optional_filter_value(active_variant.get("city"))
    query_county = _normalize_optional_filter_value(active_variant.get("county"))

    payload_state = _normalize_optional_filter_value(payload.get("state"))
    payload_city = _normalize_optional_filter_value(payload.get("city"))
    payload_county = _normalize_optional_filter_value(payload.get("county"))

    if state and _norm_text(payload_state) != _norm_text(state):
        return "state"

    if query_city:
        if _norm_text(payload_city) != _norm_text(query_city):
            return "city"
    elif query_county and payload_county:
        if _normalize_county_text(payload_county) != _normalize_county_text(query_county):
            return "county"
    elif runtime_city:
        if _norm_text(payload_city) != _norm_text(runtime_city):
            return "city"

    asking_price = _safe_float(payload.get("asking_price")) or 0.0
    max_price = runtime_config.get("max_price")
    if max_price is not None and asking_price > float(max_price):
        return "max_price"

    requested_types = runtime_config.get("property_types") or []
    requested_types = {_normalize_property_type(x) for x in requested_types if str(x).strip()}
    actual_type = _normalize_property_type(payload.get("property_type"))
    if requested_types and actual_type not in requested_types:
        return "property_type"

    return None


def _is_valid_payload(payload: dict[str, Any]) -> bool:
    return bool(
        payload.get("external_record_id")
        and payload.get("address")
        and payload.get("city")
        and payload.get("state")
        and payload.get("zip")
    )


def _title_city_variants(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []

    variants: list[str] = []
    seen: set[str] = set()

    candidates = [raw, raw.lower(), raw.title()]

    for item in candidates:
        normalized = str(item or "").strip()
        if normalized and normalized.lower() not in seen:
            seen.add(normalized.lower())
            variants.append(normalized)

    return variants


def _build_query_variants(
    *,
    source: Any,
    runtime_config: dict[str, Any],
    provider_fetch_limit: int,
) -> list[dict[str, Any]]:
    base_config = dict(source.config_json or {})
    merged = {**base_config, **dict(runtime_config or {})}
    merged["limit"] = int(provider_fetch_limit)

    city = _normalize_optional_filter_value(merged.get("city"))
    county = _normalize_optional_filter_value(merged.get("county"))
    state = _normalize_optional_filter_value(merged.get("state")) or "MI"
    market_slug = _normalize_optional_filter_value(merged.get("market_slug"))
    max_price = _safe_float(merged.get("max_price"))
    property_types = list(merged.get("property_types") or [])

    city_variants = _title_city_variants(city)

    variants: list[dict[str, Any]] = []
    seen: set[str] = set()

    is_market_scoped = bool(market_slug or city)
    allow_county_variant = bool(county and not is_market_scoped)
    allow_statewide_fallback = bool(
        runtime_config.get("allow_statewide_fallback", False)
    ) and not is_market_scoped and not county

    def add_variant(
        *,
        city_value: str | None,
        county_value: str | None,
        include_property_type: bool,
    ) -> None:
        cfg = dict(merged)

        cfg["state"] = state
        cfg["city"] = city_value
        cfg["county"] = county_value
        cfg["max_price"] = max_price

        if include_property_type:
            cfg["property_types"] = list(property_types)
        else:
            cfg["property_types"] = []
            cfg.pop("property_type", None)

        variant_label = {
            "state": cfg.get("state"),
            "city": cfg.get("city"),
            "county": cfg.get("county"),
            "max_price": cfg.get("max_price"),
            "property_types": list(cfg.get("property_types") or []),
        }

        key = json.dumps(variant_label, sort_keys=True, default=str)
        if key in seen:
            return

        cfg["_query_variant"] = dict(variant_label)
        seen.add(key)
        variants.append(cfg)

    if city_variants:
        for city_value in city_variants:
            add_variant(city_value=city_value, county_value=None, include_property_type=True)
        for city_value in city_variants:
            add_variant(city_value=city_value, county_value=None, include_property_type=False)

    if allow_county_variant:
        add_variant(city_value=None, county_value=county, include_property_type=True)
        add_variant(city_value=None, county_value=county, include_property_type=False)

    if allow_statewide_fallback:
        add_variant(city_value=None, county_value=None, include_property_type=True)
        add_variant(city_value=None, county_value=None, include_property_type=False)

    return variants


def _load_rows_page(
    source,
    *,
    trigger_type: str,
    runtime_config: dict[str, Any],
    cursor: dict[str, Any],
    provider_fetch_limit: int,
) -> RentCastListingFetchResult:
    base_config = dict(source.config_json or {})
    merged_config = {**base_config, **dict(runtime_config or {})}
    merged_config["limit"] = int(provider_fetch_limit)
    if bool(getattr(settings, "ingestion_disable_county_fallback_variants", True)):
        merged_config["county"] = None

    sample_rows = merged_config.get("sample_rows")
    if isinstance(sample_rows, list):
        rows = [x for x in sample_rows if isinstance(x, dict)]
        return RentCastListingFetchResult(
            rows=rows,
            next_cursor={"page": (_safe_int(cursor.get("page")) or 1) + 1},
            raw_count=len(rows),
            page_scanned=_safe_int(cursor.get("page")) or 1,
            shard_scanned=_safe_int(cursor.get("shard")) or 1,
            sort_mode=str(cursor.get("sort_mode") or "newest"),
            exhausted=len(rows) < int(provider_fetch_limit),
            page_fingerprint=None,
            page_changed=True,
            provider_cursor=None,
            query_variant=None,
        )

    adapter = PROVIDER_ADAPTERS.get(source.provider)
    if adapter is None:
        raise ValueError(f"No adapter registered for provider={source.provider}")

    if str(getattr(source, "provider", "") or "").strip().lower() != "rentcast":
        return adapter.load_rows_page(
            credentials=dict(getattr(source, "credentials_json", None) or {}),
            runtime_config=merged_config,
            cursor=cursor,
        )

    query_variants = _build_query_variants(
        source=source,
        runtime_config=runtime_config,
        provider_fetch_limit=provider_fetch_limit,
    )

    last_result: RentCastListingFetchResult | None = None

    for idx, variant_config in enumerate(query_variants, start=1):
        variant_label = dict(variant_config.get("_query_variant") or {})
        variant_label["attempt"] = idx

        _emit(
            {
                "event": "ingestion_query_variant_attempt",
                "provider": str(getattr(source, "provider", "") or ""),
                "source_id": int(getattr(source, "id")),
                "variant": variant_label,
                "cursor": dict(cursor or {}),
            }
        )

        result = adapter.load_rows_page(
            credentials=dict(getattr(source, "credentials_json", None) or {}),
            runtime_config=variant_config,
            cursor=cursor,
        )
        last_result = result

        rows = list(result.rows or [])
        raw_count = int(result.raw_count or 0)

        if raw_count > 0 or rows:
            runtime_city = _normalize_optional_filter_value(runtime_config.get("city"))
            runtime_county = _normalize_optional_filter_value(runtime_config.get("county"))
            runtime_market_slug = _normalize_optional_filter_value(runtime_config.get("market_slug"))

            selected_city = _normalize_optional_filter_value(variant_config.get("city"))
            selected_county = _normalize_optional_filter_value(variant_config.get("county"))

            is_scoped_sync = bool(runtime_market_slug or runtime_city or runtime_county)
            selected_is_broad = not selected_city and not selected_county

            if is_scoped_sync and selected_is_broad:
                _emit(
                    {
                        "event": "ingestion_query_variant_rejected",
                        "provider": str(getattr(source, "provider", "") or ""),
                        "source_id": int(getattr(source, "id")),
                        "variant": variant_label,
                        "reason": "broad_fallback_not_allowed_for_scoped_sync",
                        "runtime_market_slug": runtime_market_slug,
                        "runtime_city": runtime_city,
                        "runtime_county": runtime_county,
                        "raw_count": raw_count,
                        "rows_returned": len(rows),
                    }
                )
                continue

            _emit(
                {
                    "event": "ingestion_query_variant_selected",
                    "provider": str(getattr(source, "provider", "") or ""),
                    "source_id": int(getattr(source, "id")),
                    "variant": variant_label,
                    "raw_count": raw_count,
                    "rows_returned": len(rows),
                }
            )
            return result

    assert last_result is not None
    return last_result


def _append_property_error(summary: dict[str, Any], *, property_id: int | None, external_record_id: str | None, reason: str) -> None:
    errors = list(summary.get("property_errors") or [])
    errors.append(
        {
            "property_id": property_id,
            "external_record_id": external_record_id,
            "reason": reason,
        }
    )
    summary["property_errors"] = errors[:200]


def _budget_exhausted(
    *,
    provider_pages_scanned: int,
    max_pages_budget: int,
) -> bool:
    return provider_pages_scanned >= max_pages_budget


def _cursor_summary(next_cursor: dict[str, Any] | None) -> dict[str, Any]:
    cursor = dict(next_cursor or {})
    return {
        "market_slug": cursor.get("market_slug"),
        "page": _safe_int(cursor.get("page")),
        "shard": _safe_int(cursor.get("shard")),
        "sort_mode": cursor.get("sort_mode"),
        "page_changed": cursor.get("page_changed"),
        "page_fingerprint": cursor.get("page_fingerprint"),
        "provider_cursor": cursor.get("provider_cursor"),
    }


def _run_optional_post_pipeline(
    db: Session,
    *,
    org_id: int,
    property_row: Any,
    deal_row: Any,
    payload: dict[str, Any],
    runtime_config: dict[str, Any],
    summary: dict[str, Any],
) -> tuple[bool, list[str], float]:
    """
    Returns: (was_partial, pipeline_errors, elapsed_ms)

    This function intentionally allows the caller to skip expensive optional stages
    for faster ingestion. The import path remains property-first either way.
    """
    if bool(runtime_config.get("defer_optional_post_pipeline", False)):
        return True, ["optional_post_pipeline_deferred"], 0.0

    t0 = time.perf_counter()
    pipeline_errors: list[str] = []
    was_partial = False

    try:
        result = execute_post_ingestion_pipeline(
            db,
            org_id=int(org_id),
            property_id=int(property_row.id),
            deal_id=int(deal_row.id),
            payload=payload,
        )
        apply_pipeline_summary(summary, result or {})
        was_partial = bool((result or {}).get("partial", False))
        pipeline_errors = list((result or {}).get("errors") or [])
    except ModuleNotFoundError as exc:
        logger.info(
            "optional_post_pipeline_module_missing",
            extra={"org_id": int(org_id), "property_id": int(property_row.id), "module": getattr(exc, "name", None)},
        )
        was_partial = True
        pipeline_errors = [f"optional_module_missing:{getattr(exc, 'name', 'unknown')}"]
        summary["post_import_partials"] = int(summary.get("post_import_partials", 0) or 0) + 1
    except Exception as exc:
        logger.exception(
            "optional_post_pipeline_failed",
            extra={"org_id": int(org_id), "property_id": int(property_row.id)},
        )
        was_partial = True
        pipeline_errors = [str(exc)]
        summary["post_import_failures"] = int(summary.get("post_import_failures", 0) or 0) + 1
        errors = list(summary.get("post_import_errors") or [])
        errors.append({"property_id": int(property_row.id), "error": str(exc)})
        summary["post_import_errors"] = errors[:200]

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    return was_partial, pipeline_errors, elapsed_ms


def execute_source_sync(
    db: Session,
    *,
    org_id: int,
    source: Any,
    trigger_type: str = "manual",
    runtime_config: dict[str, Any] | None = None,
):
    run_t0 = time.perf_counter()

    runtime_config = _normalize_runtime_config(runtime_config)
    runtime_config["trigger_type"] = str(trigger_type or "manual")

    source_id = int(getattr(source, "id"))

    requested_new_records = int(runtime_config.get("limit") or 1)
    provider_fetch_limit = min(
        max(1, requested_new_records),
        int(getattr(settings, "market_sync_default_limit_per_market", 125) or 125),
    )
    max_pages_budget = _safe_int(runtime_config.get("max_pages_budget")) or max(
        1,
        int(getattr(settings, "ingestion_provider_max_pages_per_shard", 3) or 3),
    )

    market_sync_state = get_market_sync_state_by_id(
        db,
        sync_state_id=_safe_int(runtime_config.get("market_sync_state_id")),
        org_id=int(org_id),
    )

    starting_cursor = _starting_cursor(source, trigger_type, runtime_config)
    dataset_identity = build_market_dataset_identity(
        org_id=int(org_id),
        source=source,
        runtime_config=runtime_config,
        sync_state=market_sync_state,
        cursor=starting_cursor,
    )
    dataset_key = str(dataset_identity["dataset_key"])

    fallback_idempotency_key = build_run_idempotency_key(
        org_id=int(org_id),
        source=source,
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )

    lock_owner = _build_lock_owner(
        org_id=int(org_id),
        source_id=source_id,
        dataset_key=dataset_key,
    )

    _emit(
        {
            "event": "ingestion_sync_start",
            "org_id": int(org_id),
            "source_id": source_id,
            "provider": str(getattr(source, "provider", "") or ""),
            "trigger_type": str(trigger_type),
            "provider_fetch_limit": provider_fetch_limit,
            "requested_new_records": requested_new_records,
            "max_pages_budget": max_pages_budget,
            "runtime_config": runtime_config,
            "dataset_identity": dataset_identity,
            "dataset_key": dataset_key,
            "fallback_idempotency_key": fallback_idempotency_key,
        }
    )

    execution_lock_ttl_seconds = int(
        getattr(
            settings,
            "ingestion_execution_lock_ttl_seconds",
            DEFAULT_EXECUTION_LOCK_TTL_SECONDS,
        )
    )

    execution_lock = acquire_ingestion_execution_lock(
        db,
        org_id=int(org_id),
        source_id=source_id,
        dataset_key=dataset_key,
        owner=lock_owner,
        ttl_seconds=execution_lock_ttl_seconds,
    )

    if not execution_lock.acquired:
        execution_lock_key = build_ingestion_execution_lock_key(
            org_id=int(org_id),
            source_id=source_id,
            dataset_key=dataset_key,
        )

        stale_clear_result = clear_stale_lock(
            db,
            org_id=int(org_id),
            lock_key=execution_lock_key,
        )

        if stale_clear_result.acquired:
            db.commit()
            execution_lock = acquire_ingestion_execution_lock(
                db,
                org_id=int(org_id),
                source_id=source_id,
                dataset_key=dataset_key,
                owner=lock_owner,
                ttl_seconds=execution_lock_ttl_seconds,
            )

    if execution_lock.acquired and bool(getattr(settings, "ingestion_commit_execution_lock_on_acquire", True)):
        try:
            db.commit()
        except Exception:
            logger.exception("commit_after_execution_lock_acquire_failed")
            try:
                db.rollback()
            except Exception:
                logger.exception("rollback_after_execution_lock_acquire_commit_failed")
            raise

    if not execution_lock.acquired:
        _emit(
            {
                "event": "ingestion_sync_skipped_locked",
                "org_id": int(org_id),
                "source_id": source_id,
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
                "lock_holder": getattr(execution_lock, "holder", None),
                "lock_expires_at": getattr(execution_lock, "expires_at", None),
                "lock_stale": getattr(execution_lock, "stale", None),
            },
            level=logging.WARNING,
        )
        return start_run(
            db,
            org_id=int(org_id),
            source_id=source_id,
            trigger_type=str(trigger_type),
            runtime_config=runtime_config,
            status="skipped_locked",
            summary_json={
                "reason": "execution_lock_not_acquired",
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
                "market_slug": runtime_config.get("market_slug") or dataset_identity.get("market_slug"),
                "sync_mode": runtime_config.get("sync_mode") or "refresh",
                "runtime_config": dict(runtime_config or {}),
                "lock_holder": getattr(execution_lock, "holder", None),
                "lock_expires_at": getattr(execution_lock, "expires_at", None),
                "lock_stale": getattr(execution_lock, "stale", None),
            },
        )

    if has_completed_ingestion_dataset(
        db,
        org_id=int(org_id),
        source_id=source_id,
        dataset_key=dataset_key,
    ):
        release_ingestion_execution_lock(
            db,
            org_id=int(org_id),
            source_id=source_id,
            dataset_key=dataset_key,
            owner=lock_owner,
        )
        _emit(
            {
                "event": "ingestion_sync_skipped_duplicate_dataset",
                "org_id": int(org_id),
                "source_id": source_id,
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
            }
        )
        return start_run(
            db,
            org_id=int(org_id),
            source_id=source_id,
            trigger_type=str(trigger_type),
            runtime_config=runtime_config,
            status="skipped_duplicate_dataset",
            summary_json={
                "reason": "already_completed",
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
                "market_slug": runtime_config.get("market_slug") or dataset_identity.get("market_slug"),
                "sync_mode": runtime_config.get("sync_mode") or "refresh",
            },
        )

    run = start_run(
        db,
        org_id=int(org_id),
        source_id=source_id,
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )

    summary = {
        "records_seen": 0,
        "records_seen_from_provider": 0,
        "records_candidate_after_filtering": 0,
        "records_imported": 0,
        "new_records_imported": 0,
        "new_listings_imported": 0,
        "already_seen_skipped": 0,
        "properties_created": 0,
        "properties_updated": 0,
        "deals_created": 0,
        "deals_updated": 0,
        "rent_rows_upserted": 0,
        "photos_upserted": 0,
        "duplicates_skipped": 0,
        "invalid_rows": 0,
        "filtered_out": 0,
        "unchanged_pages_skipped": 0,
        "post_import_failures": 0,
        "post_import_partials": 0,
        "post_import_errors": [],
        "next_actions_seeded": 0,
        "filter_reason_counts": {},
        "filter_reason_examples": {},
        "normal_path": True,
        "pages_scanned": 0,
        "provider_pages_scanned": 0,
        "page_stats": [],
        "requested_new_records": requested_new_records,
        "provider_fetch_limit": provider_fetch_limit,
        "max_pages_budget": max_pages_budget,
        "budget_boundary_hit": False,
        "market_exhausted": False,
        "stop_reason": None,
        "dataset_key": dataset_key,
        "dataset_identity": dataset_identity,
        "market_slug": runtime_config.get("market_slug") or dataset_identity.get("market_slug"),
        "sync_mode": runtime_config.get("sync_mode") or "refresh",
        "cursor_started_at": _cursor_summary(starting_cursor),
        "cursor_advanced_to": _cursor_summary(starting_cursor),
        "timings_ms": {
            "provider_load_total": 0.0,
            "db_upsert_total": 0.0,
            "post_pipeline_total": 0.0,
            "run_total": 0.0,
        },
    }

    try:
        if market_sync_state is not None:
            mark_market_sync_started(
                db,
                sync_state=market_sync_state,
                requested_limit=requested_new_records,
                status="running",
            )

        cursor = dict(starting_cursor or {})
        provider_pages_scanned = 0
        market_exhausted = False
        last_seen_provider_record_at: datetime | None = None

        while True:
            if summary["new_records_imported"] >= requested_new_records:
                summary["stop_reason"] = "requested_new_records_satisfied"
                break

            if market_exhausted:
                summary["stop_reason"] = "market_exhausted"
                break

            if _budget_exhausted(
                provider_pages_scanned=provider_pages_scanned,
                max_pages_budget=max_pages_budget,
            ):
                summary["budget_boundary_hit"] = True
                summary["stop_reason"] = "provider_page_budget_exhausted"
                break

            page_t0 = time.perf_counter()

            provider_t0 = time.perf_counter()
            fetch_result = _load_rows_page(
                source,
                trigger_type=str(trigger_type),
                runtime_config=runtime_config,
                cursor=cursor,
                provider_fetch_limit=provider_fetch_limit,
            )
            provider_ms = round((time.perf_counter() - provider_t0) * 1000, 2)
            summary["timings_ms"]["provider_load_total"] = round(
                float(summary["timings_ms"].get("provider_load_total", 0.0) or 0.0) + provider_ms,
                2,
            )

            provider_pages_scanned += 1
            summary["provider_pages_scanned"] = provider_pages_scanned
            summary["pages_scanned"] = provider_pages_scanned

            rows = list(fetch_result.rows or [])
            raw_count = int(fetch_result.raw_count or 0)
            summary["records_seen_from_provider"] += raw_count
            summary["records_seen"] += raw_count

            page_stat = {
                "page_number": int(fetch_result.page_scanned or _safe_int(cursor.get("page")) or 1),
                "cursor_in": _cursor_summary(cursor),
                "cursor_out": _cursor_summary(fetch_result.next_cursor),
                "raw_count": raw_count,
                "rows_returned": len(rows),
                "provider_ms": provider_ms,
                "page_scanned": int(fetch_result.page_scanned or _safe_int(cursor.get("page")) or 1),
                "shard_scanned": int(fetch_result.shard_scanned or _safe_int(cursor.get("shard")) or 1),
                "sort_mode": str(fetch_result.sort_mode or cursor.get("sort_mode") or "newest"),
                "page_fingerprint": fetch_result.page_fingerprint,
                "page_changed": bool(fetch_result.page_changed),
                "market_exhausted": bool(fetch_result.exhausted),
                "records_candidate_after_filtering": 0,
                "imported": 0,
                "new_records_imported": 0,
                "new_listings_imported": 0,
                "already_seen_skipped": 0,
                "duplicates_skipped": 0,
                "invalid_rows": 0,
                "filtered_out": 0,
                "pipeline_failures": 0,
                "skipped_unchanged_page": False,
                "query_variant": dict(fetch_result.query_variant or {}),
                "runtime_city": runtime_config.get("city"),
                "runtime_county": runtime_config.get("county"),
            }

            if not bool(fetch_result.page_changed) and raw_count > 0:
                summary["unchanged_pages_skipped"] += 1
                page_stat["skipped_unchanged_page"] = True
                page_stat["page_total_ms"] = round((time.perf_counter() - page_t0) * 1000, 2)
                summary["page_stats"].append(page_stat)
                summary["stop_reason"] = "unchanged_page_repeat"
                cursor = dict(fetch_result.next_cursor or cursor)
                break

            for raw_row in rows:
                payload = canonical_listing_payload(raw_row)
                query_variant = dict(fetch_result.query_variant or {})

                if not _is_valid_payload(payload):
                    summary["invalid_rows"] += 1
                    page_stat["invalid_rows"] += 1
                    continue

                reason = _filter_reason(payload, runtime_config, query_variant=query_variant)
                if reason:
                    summary["filtered_out"] += 1
                    page_stat["filtered_out"] += 1
                    counts = dict(summary.get("filter_reason_counts") or {})
                    counts[reason] = int(counts.get(reason, 0) or 0) + 1
                    summary["filter_reason_counts"] = counts
                    continue

                summary["records_candidate_after_filtering"] += 1
                page_stat["records_candidate_after_filtering"] += 1

                existing_link = find_existing_by_external_id(
                    db,
                    org_id=int(org_id),
                    provider=str(getattr(source, "provider", "") or ""),
                    external_record_id=str(payload.get("external_record_id") or ""),
                )
                already_linked = existing_link is not None

                db_t0 = time.perf_counter()

                fingerprint = build_property_fingerprint(
                    address=payload["address"],
                    city=payload["city"],
                    state=payload["state"],
                    zip_code=payload["zip"],
                )

                property_row, property_created = _upsert_property(
                    db,
                    org_id=int(org_id),
                    payload=payload,
                )
                _seed_missing_completeness_columns(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_row.id),
                )
                _persist_property_acquisition_metadata(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_row.id),
                    source=source,
                    payload=payload,
                    trigger_type=str(trigger_type),
                )
                upsert_record_link(
                    db,
                    org_id=int(org_id),
                    provider=str(getattr(source, "provider", "") or ""),
                    external_record_id=str(payload.get("external_record_id") or ""),
                    property_id=int(property_row.id),
                    fingerprint=fingerprint,
                )

                deal_row, deal_created = _upsert_deal(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_row.id),
                    payload=payload,
                )
                _, rent_created = _upsert_rent_assumption(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_row.id),
                    payload=payload,
                )
                photos_added = _upsert_photos(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_row.id),
                    provider=str(getattr(source, "provider", "") or ""),
                    photos=list(payload.get("photos") or []),
                )

                db_upsert_ms = round((time.perf_counter() - db_t0) * 1000, 2)
                summary["timings_ms"]["db_upsert_total"] = round(
                    float(summary["timings_ms"].get("db_upsert_total", 0.0) or 0.0) + db_upsert_ms,
                    2,
                )

                summary["records_imported"] += 1
                page_stat["imported"] += 1

                if not already_linked:
                    summary["new_records_imported"] += 1
                    page_stat["new_records_imported"] += 1

                if property_created:
                    summary["properties_created"] += 1
                    summary["new_listings_imported"] += 1
                    page_stat["new_listings_imported"] += 1
                else:
                    summary["properties_updated"] += 1

                if deal_created:
                    summary["deals_created"] += 1
                else:
                    summary["deals_updated"] += 1

                if rent_created:
                    summary["rent_rows_upserted"] += 1

                if photos_added:
                    summary["photos_upserted"] += int(photos_added)

                pipeline_partial, pipeline_errors, pipeline_ms = _run_optional_post_pipeline(
                    db,
                    org_id=int(org_id),
                    property_row=property_row,
                    deal_row=deal_row,
                    payload=payload,
                    runtime_config=runtime_config,
                    summary=summary,
                )
                summary["timings_ms"]["post_pipeline_total"] = round(
                    float(summary["timings_ms"].get("post_pipeline_total", 0.0) or 0.0) + pipeline_ms,
                    2,
                )

                _emit(
                    {
                        "event": "ingestion_property_pipeline_complete",
                        "org_id": int(org_id),
                        "source_id": source_id,
                        "property_id": int(property_row.id),
                        "external_record_id": payload.get("external_record_id"),
                        "prop_created": bool(property_created),
                        "deal_created": bool(deal_created),
                        "photos_added": int(photos_added),
                        "db_upsert_ms": db_upsert_ms,
                        "pipeline_ms": pipeline_ms,
                        "pipeline_partial": bool(pipeline_partial),
                        "pipeline_errors": pipeline_errors,
                        "dataset_key": dataset_key,
                    }
                )

                last_seen_provider_record_at = _utcnow_naive()

                if summary["new_records_imported"] >= requested_new_records:
                    break

            market_exhausted = bool(fetch_result.exhausted) or raw_count == 0
            page_stat["market_exhausted"] = bool(market_exhausted)
            page_stat["page_total_ms"] = round((time.perf_counter() - page_t0) * 1000, 2)
            summary["page_stats"].append(page_stat)

            cursor = dict(fetch_result.next_cursor or cursor)
            summary["cursor_advanced_to"] = _cursor_summary(cursor)

            if summary["new_records_imported"] >= requested_new_records:
                summary["stop_reason"] = "requested_new_records_satisfied"
                break

        summary["market_exhausted"] = bool(market_exhausted)

        if market_sync_state is not None:
            advance_market_cursor(
                db,
                sync_state=market_sync_state,
                next_cursor=cursor,
                page_scanned=_safe_int(cursor.get("page")) or 1,
                shard_scanned=_safe_int(cursor.get("shard")) or 1,
                sort_mode=str(cursor.get("sort_mode") or "newest"),
                page_fingerprint=cursor.get("page_fingerprint"),
                page_changed=bool(cursor.get("page_changed", True)),
                exhausted=bool(market_exhausted),
                seen_provider_record_at=last_seen_provider_record_at,
            )
            mark_market_sync_completed(
                db,
                sync_state=market_sync_state,
                market_exhausted=bool(market_exhausted),
                seen_provider_record_at=last_seen_provider_record_at,
                status="idle",
            )

        if int(summary.get("records_imported", 0) or 0) > 0:
            completion_ttl = int(
                getattr(settings, "ingestion_completion_lock_ttl_seconds", DEFAULT_COMPLETION_LOCK_TTL_SECONDS)
                or DEFAULT_COMPLETION_LOCK_TTL_SECONDS
            )
            mark_ingestion_dataset_completed(
                db,
                org_id=int(org_id),
                source_id=source_id,
                dataset_key=dataset_key,
                ttl_seconds=completion_ttl,
            )

        summary["timings_ms"]["run_total"] = round((time.perf_counter() - run_t0) * 1000, 2)

        finish_run(
            db,
            run,
            status="completed",
            summary_json=summary,
        )

        _emit(
            {
                "event": "ingestion_sync_completed",
                "org_id": int(org_id),
                "source_id": source_id,
                "run_id": int(getattr(run, "id")),
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
                "summary": summary,
            }
        )

        return run
    except Exception:
        logger.exception("execute_source_sync_failed", extra={"org_id": int(org_id), "source_id": source_id})
        summary["timings_ms"]["run_total"] = round((time.perf_counter() - run_t0) * 1000, 2)
        finish_run(
            db,
            run,
            status="failed",
            summary_json=summary,
        )
        raise
    finally:
        try:
            if bool(getattr(settings, "ingestion_force_release_lock_on_finish", True)):
                release_ingestion_execution_lock(
                    db,
                    org_id=int(org_id),
                    source_id=source_id,
                    dataset_key=dataset_key,
                    owner=lock_owner,
                )
        except Exception:
            logger.exception("release_ingestion_execution_lock_failed")
            