from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import time
from typing import Any
from datetime import datetime, timezone

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
    
    # HARD STOP COUNTY FALLBACK
    if payload.get("market_slug") or payload.get("city"):
        payload["county"] = None

    if bool(getattr(settings, "ingestion_disable_county_fallback_variants", True)) and (payload.get("market_slug") or payload.get("city")):
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


def _set_run_status(row: Any, status: str) -> None:
    if hasattr(row, "status"):
        setattr(row, "status", status)


def _set_run_summary(row: Any, summary: dict[str, Any]) -> None:
    if hasattr(row, "summary_json"):
        setattr(row, "summary_json", summary)


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

    # State is always hard.
    if state and _norm_text(payload_state) != _norm_text(state):
        return "state"

    # If the active provider query is city-based, enforce that city strictly.
    if query_city:
        if _norm_text(payload_city) != _norm_text(query_city):
            return "city"

    # County is secondary/optional and should only matter if we ever explicitly
    # re-enable county-only provider fallback in the future.
    elif query_county and payload_county:
        if _normalize_county_text(payload_county) != _normalize_county_text(query_county):
            return "county"

    # If the active query is state-only, but the original request had a city,
    # still require the payload city to match the original requested city.
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
    state = _normalize_optional_filter_value(merged.get("state")) or "MI"
    max_price = _safe_float(merged.get("max_price"))
    property_types = list(merged.get("property_types") or [])

    city_variants = _title_city_variants(city)

    variants: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_variant(
        *,
        city_value: str | None,
        include_property_type: bool,
    ) -> None:
        cfg = dict(merged)

        cfg["state"] = state
        cfg["city"] = city_value
        cfg["county"] = None
        cfg["max_price"] = max_price

        if include_property_type:
            cfg["property_types"] = list(property_types)
        else:
            cfg["property_types"] = []
            cfg.pop("property_type", None)

        variant_label = {
            "state": cfg.get("state"),
            "city": cfg.get("city"),
            "county": None,
            "max_price": cfg.get("max_price"),
            "property_types": list(cfg.get("property_types") or []),
        }

        key = json.dumps(variant_label, sort_keys=True, default=str)
        if key in seen:
            return

        cfg["_query_variant"] = dict(variant_label)
        seen.add(key)
        variants.append(cfg)

    # 1) state + city + propertyType + price
    for city_value in city_variants:
        add_variant(city_value=city_value, include_property_type=True)

    # 2) state + city + price
    for city_value in city_variants:
        add_variant(city_value=city_value, include_property_type=False)

    # 3) state + propertyType + price
    add_variant(city_value=None, include_property_type=True)

    # 4) state + price
    add_variant(city_value=None, include_property_type=False)

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
            exhausted = bool(fetch_result.exhausted or not rows)
            page_changed = bool(fetch_result.page_changed)
            next_cursor = dict(fetch_result.next_cursor or {})
            selected_query_variant = dict(getattr(fetch_result, "query_variant", None) or {})

            if raw_count > 0:
                last_seen_provider_record_at = _utcnow_naive()

            summary["records_seen_from_provider"] += raw_count
            summary["records_seen"] = summary["records_seen_from_provider"]
            summary["cursor_advanced_to"] = _cursor_summary(next_cursor)
            summary["market_slug"] = (
                next_cursor.get("market_slug")
                or runtime_config.get("market_slug")
                or summary.get("market_slug")
            )

            page_stat = {
                "page_number": provider_pages_scanned,
                "cursor_in": dict(cursor or {}),
                "cursor_out": dict(next_cursor or {}),
                "raw_count": raw_count,
                "rows_returned": len(rows),
                "provider_ms": provider_ms,
                "page_scanned": int(fetch_result.page_scanned or provider_pages_scanned),
                "shard_scanned": int(fetch_result.shard_scanned or 1),
                "sort_mode": str(fetch_result.sort_mode or "newest"),
                "page_fingerprint": fetch_result.page_fingerprint,
                "page_changed": page_changed,
                "market_exhausted": exhausted,
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
                "query_variant": selected_query_variant,
                "runtime_city": runtime_config.get("city"),
                "runtime_county": runtime_config.get("county"),
            }

            _emit(
                {
                    "event": "ingestion_page_loaded",
                    "org_id": int(org_id),
                    "source_id": source_id,
                    "page_number": provider_pages_scanned,
                    "page_scanned": int(fetch_result.page_scanned or provider_pages_scanned),
                    "raw_count": raw_count,
                    "rows_returned": len(rows),
                    "provider_ms": provider_ms,
                    "page_changed": page_changed,
                    "market_exhausted": exhausted,
                    "dataset_key": dataset_key,
                    "query_variant": selected_query_variant,
                }
            )

            if not page_changed:
                summary["unchanged_pages_skipped"] += 1
                page_stat["skipped_unchanged_page"] = True

                if market_sync_state is not None:
                    advance_market_cursor(
                        db,
                        sync_state=market_sync_state,
                        next_cursor=next_cursor,
                        page_scanned=int(fetch_result.page_scanned or provider_pages_scanned),
                        shard_scanned=int(fetch_result.shard_scanned or 1),
                        sort_mode=str(fetch_result.sort_mode or "newest"),
                        page_fingerprint=fetch_result.page_fingerprint,
                        page_changed=page_changed,
                        exhausted=exhausted,
                        seen_provider_record_at=last_seen_provider_record_at,
                    )

                source.cursor_json = dict(next_cursor or {})
                db.add(source)
                db.flush()

                page_stat["page_total_ms"] = round((time.perf_counter() - page_t0) * 1000, 2)
                summary["page_stats"].append(page_stat)

                cursor = dict(next_cursor or {})
                market_exhausted = exhausted
                summary["market_exhausted"] = market_exhausted

                if provider_pages_scanned >= 2:
                    summary["stop_reason"] = "unchanged_page_repeat"
                    break

                continue

            for raw in rows:
                payload = canonical_listing_payload(raw)
                external_record_id = str(payload.get("external_record_id") or "").strip() or None

                if not _is_valid_payload(payload):
                    summary["invalid_rows"] += 1
                    page_stat["invalid_rows"] += 1
                    _append_property_error(
                        summary,
                        property_id=None,
                        external_record_id=external_record_id,
                        reason="invalid_payload",
                    )
                    continue

                reason = _filter_reason(
                    payload,
                    runtime_config,
                    query_variant=selected_query_variant,
                )
                if reason:
                    summary["filtered_out"] += 1
                    page_stat["filtered_out"] += 1
                    summary["filter_reason_counts"][reason] = int(
                        summary["filter_reason_counts"].get(reason, 0)
                    ) + 1

                    if len(summary["filter_reason_examples"].setdefault(reason, [])) < 10:
                        summary["filter_reason_examples"][reason].append(
                            {
                                "external_record_id": external_record_id,
                                "payload_city": payload.get("city"),
                                "payload_county": payload.get("county"),
                                "runtime_city": runtime_config.get("city"),
                                "runtime_county": runtime_config.get("county"),
                                "query_variant": selected_query_variant,
                            }
                        )
                    continue

                summary["records_candidate_after_filtering"] += 1
                page_stat["records_candidate_after_filtering"] += 1

                external_link = find_existing_by_external_id(
                    db,
                    org_id=int(org_id),
                    provider=str(source.provider),
                    external_record_id=str(payload["external_record_id"]),
                )
                if external_link is not None:
                    summary["already_seen_skipped"] += 1
                    summary["duplicates_skipped"] += 1
                    page_stat["already_seen_skipped"] += 1
                    page_stat["duplicates_skipped"] += 1
                    continue

                fingerprint = build_property_fingerprint(
                    address=payload["address"],
                    city=payload["city"],
                    state=payload["state"],
                    zip_code=payload["zip"],
                )

                prop_before = find_existing_property(
                    db,
                    org_id=int(org_id),
                    address=payload["address"],
                    city=payload["city"],
                    state=payload["state"],
                    zip_code=payload["zip"],
                )

                db_t0 = time.perf_counter()
                prop, prop_created = _upsert_property(db, org_id=int(org_id), payload=payload)
                deal, deal_created = _upsert_deal(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                    payload=payload,
                )
                rent, _rent_created = _upsert_rent_assumption(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                    payload=payload,
                )
                photos_added = _upsert_photos(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                    provider=str(source.provider),
                    photos=list(payload.get("photos") or []),
                )

                upsert_record_link(
                    db,
                    org_id=int(org_id),
                    provider=str(source.provider),
                    source_id=source_id,
                    external_record_id=str(payload["external_record_id"]),
                    external_url=payload.get("external_url"),
                    property_id=int(prop.id),
                    deal_id=int(deal.id) if getattr(deal, "id", None) else None,
                    raw_json=payload.get("raw_json") or payload,
                    fingerprint=fingerprint,
                )
                _persist_property_acquisition_metadata(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                    source=source,
                    payload=payload,
                    trigger_type=str(trigger_type),
                )
                _seed_missing_completeness_columns(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                )
                db_upsert_ms = round((time.perf_counter() - db_t0) * 1000, 2)
                summary["timings_ms"]["db_upsert_total"] = round(
                    float(summary["timings_ms"].get("db_upsert_total", 0.0) or 0.0) + db_upsert_ms,
                    2,
                )

                summary["records_imported"] += 1
                summary["new_records_imported"] += 1
                summary["new_listings_imported"] += 1
                page_stat["imported"] += 1
                page_stat["new_records_imported"] += 1
                page_stat["new_listings_imported"] += 1
                summary["properties_created"] += 1 if prop_created else 0
                summary["properties_updated"] += 1 if (not prop_created and prop_before is not None) else 0
                summary["deals_created"] += 1 if deal_created else 0
                summary["deals_updated"] += 1 if not deal_created else 0
                summary["rent_rows_upserted"] += 1 if rent is not None else 0
                summary["photos_upserted"] += int(photos_added)

                try:
                    pipeline_t0 = time.perf_counter()
                    pipeline_summary = execute_post_ingestion_pipeline(
                        db,
                        org_id=int(org_id),
                        property_id=int(prop.id),
                        actor_user_id=None,
                        emit_events=False,
                    )
                    pipeline_ms = round((time.perf_counter() - pipeline_t0) * 1000, 2)
                    summary["timings_ms"]["post_pipeline_total"] = round(
                        float(summary["timings_ms"].get("post_pipeline_total", 0.0) or 0.0) + pipeline_ms,
                        2,
                    )

                    if isinstance(pipeline_summary, dict):
                        apply_pipeline_summary(summary, pipeline_summary, int(prop.id))
                        if pipeline_summary.get("errors"):
                            page_stat["pipeline_failures"] += 1

                    _emit(
                        {
                            "event": "ingestion_property_pipeline_complete",
                            "org_id": int(org_id),
                            "source_id": source_id,
                            "property_id": int(prop.id),
                            "external_record_id": external_record_id,
                            "prop_created": bool(prop_created),
                            "deal_created": bool(deal_created),
                            "photos_added": int(photos_added),
                            "db_upsert_ms": db_upsert_ms,
                            "pipeline_ms": pipeline_ms,
                            "pipeline_partial": bool((pipeline_summary or {}).get("partial"))
                            if isinstance(pipeline_summary, dict)
                            else None,
                            "pipeline_errors": list((pipeline_summary or {}).get("errors") or [])
                            if isinstance(pipeline_summary, dict)
                            else [],
                            "dataset_key": dataset_key,
                        }
                    )
                except Exception as exc:
                    page_stat["pipeline_failures"] += 1
                    logger.exception(
                        "post_ingestion_pipeline_failed property_id=%s",
                        getattr(prop, "id", None),
                    )
                    summary.setdefault("post_import_failures", 0)
                    summary["post_import_failures"] = int(summary.get("post_import_failures", 0)) + 1
                    summary.setdefault("post_import_errors", [])
                    summary["post_import_errors"].append(
                        {
                            "property_id": int(getattr(prop, "id", 0) or 0),
                            "external_record_id": external_record_id,
                            "errors": [str(exc)],
                        }
                    )

            if market_sync_state is not None:
                advance_market_cursor(
                    db,
                    sync_state=market_sync_state,
                    next_cursor=next_cursor,
                    page_scanned=int(fetch_result.page_scanned or provider_pages_scanned),
                    shard_scanned=int(fetch_result.shard_scanned or 1),
                    sort_mode=str(fetch_result.sort_mode or "newest"),
                    page_fingerprint=fetch_result.page_fingerprint,
                    page_changed=page_changed,
                    exhausted=exhausted,
                    seen_provider_record_at=last_seen_provider_record_at,
                )

            source.cursor_json = dict(next_cursor or {})
            db.add(source)
            db.flush()

            page_stat["page_total_ms"] = round((time.perf_counter() - page_t0) * 1000, 2)
            summary["page_stats"].append(page_stat)

            cursor = dict(next_cursor or {})
            market_exhausted = exhausted
            summary["market_exhausted"] = market_exhausted

        if market_sync_state is not None:
            mark_market_sync_completed(
                db,
                sync_state=market_sync_state,
                market_exhausted=market_exhausted,
                seen_provider_record_at=last_seen_provider_record_at,
                status="idle",
            )

        should_mark_completed = bool(
            summary.get("new_records_imported", 0) > 0
            or summary.get("records_imported", 0) > 0
        )

        if should_mark_completed:
            mark_ingestion_dataset_completed(
                db,
                org_id=int(org_id),
                source_id=source_id,
                dataset_key=dataset_key,
                owner=lock_owner,
                ttl_seconds=int(
                    getattr(
                        settings,
                        "ingestion_completion_lock_ttl_seconds",
                        DEFAULT_COMPLETION_LOCK_TTL_SECONDS,
                    )
                ),
            )
        else:
            _emit(
                {
                    "event": "ingestion_dataset_not_marked_completed",
                    "org_id": int(org_id),
                    "source_id": source_id,
                    "dataset_key": dataset_key,
                    "reason": "zero_import_run",
                    "records_imported": int(summary.get("records_imported", 0) or 0),
                    "new_records_imported": int(summary.get("new_records_imported", 0) or 0),
                    "stop_reason": summary.get("stop_reason"),
                    "market_exhausted": bool(summary.get("market_exhausted")),
                }
            )

        summary["timings_ms"]["run_total"] = round((time.perf_counter() - run_t0) * 1000, 2)

        _set_run_summary(run, summary)
        _set_run_status(run, "completed")
        finish_run(db, run, status="completed", summary_json=summary)

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

    except Exception as exc:
        logger.exception("execute_source_sync_failed")

        try:
            db.rollback()
        except Exception:
            logger.exception("execute_source_sync_rollback_failed")

        summary["timings_ms"]["run_total"] = round((time.perf_counter() - run_t0) * 1000, 2)

        if market_sync_state is not None:
            try:
                mark_market_sync_completed(
                    db,
                    sync_state=market_sync_state,
                    market_exhausted=summary.get("market_exhausted"),
                    status="failed",
                )
            except Exception:
                logger.exception("mark_market_sync_completed_failed")
                try:
                    db.rollback()
                except Exception:
                    logger.exception("rollback_after_mark_market_sync_completed_failed")

        try:
            _set_run_summary(run, {**summary, "error": str(exc)})
            _set_run_status(run, "failed")
            finish_run(db, run, status="failed", summary_json={**summary, "error": str(exc)})
        except Exception:
            logger.exception("finish_run_failed_after_ingestion_error")
            try:
                db.rollback()
            except Exception:
                logger.exception("rollback_after_finish_run_failed")

        try:
            _emit(
                {
                    "event": "ingestion_sync_failed",
                    "org_id": int(org_id),
                    "source_id": source_id,
                    "run_id": int(getattr(run, "id")) if getattr(run, "id", None) else None,
                    "dataset_key": dataset_key,
                    "dataset_identity": dataset_identity,
                    "error": str(exc),
                    "summary": summary,
                },
                level=logging.ERROR,
            )
        except Exception:
            logger.exception("emit_ingestion_sync_failed_event_failed")

        raise

    finally:
        try:
            db.rollback()
        except Exception:
            logger.exception("execute_source_sync_finally_rollback_failed")

        try:
            release_ingestion_execution_lock(
                db,
                org_id=int(org_id),
                source_id=source_id,
                dataset_key=dataset_key,
                owner=lock_owner,
                force=bool(getattr(settings, "ingestion_force_release_lock_on_finish", True)),
            )
            db.commit()
        except Exception:
            logger.exception(
                "release_ingestion_execution_lock_failed",
                extra={
                    "org_id": int(org_id),
                    "source_id": source_id,
                    "dataset_key": dataset_key,
                },
            )
            try:
                db.rollback()
            except Exception:
                logger.exception("rollback_after_release_ingestion_execution_lock_failed")
