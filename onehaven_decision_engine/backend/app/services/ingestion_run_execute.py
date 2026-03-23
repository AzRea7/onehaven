from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
from typing import Any

from sqlalchemy import desc, select
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
    has_completed_ingestion_dataset,
    mark_ingestion_dataset_completed,
    release_lock,
)
from ..services.rentcast_listing_source import RentCastListingSource

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


def _normalize_runtime_config(runtime_config: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(runtime_config or {})
    limit = _safe_int(payload.get("limit")) or int(getattr(settings, "market_sync_default_limit_per_market", 125))
    payload["limit"] = max(1, limit)

    payload["state"] = _normalize_optional_filter_value(payload.get("state")) or "MI"
    payload["county"] = _normalize_optional_filter_value(payload.get("county"))
    payload["city"] = _normalize_optional_filter_value(payload.get("city"))

    property_types = payload.get("property_types")
    if isinstance(property_types, str):
        property_types = [x.strip() for x in property_types.split(",") if x.strip()]
    if not property_types:
        property_types = ["single_family", "multi_family"]
    payload["property_types"] = [str(x).strip() for x in property_types if str(x).strip()]

    for key in ["min_price", "max_price", "min_bedrooms", "min_bathrooms", "max_units"]:
        payload[key] = _safe_float(payload.get(key))

    if payload.get("max_price") is None:
        payload["max_price"] = float(getattr(settings, "investor_buy_box_max_price", 200_000))
    if payload.get("max_units") is None:
        payload["max_units"] = float(getattr(settings, "investor_buy_box_max_units", 4))

    payload.pop("zip_code", None)
    payload.pop("address", None)
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
        "min_price": runtime_config.get("min_price"),
        "max_price": runtime_config.get("max_price"),
        "min_bedrooms": runtime_config.get("min_bedrooms"),
        "min_bathrooms": runtime_config.get("min_bathrooms"),
        "max_units": runtime_config.get("max_units"),
        "limit": runtime_config.get("limit"),
        "idempotency_context": runtime_config.get("idempotency_context") or {},
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


def _build_lock_owner(*, org_id: int, source_id: int, idempotency_key: str) -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"ingestion:{host}:{pid}:{int(org_id)}:{int(source_id)}:{idempotency_key}"


def _set_run_status(row: Any, status: str) -> None:
    if hasattr(row, "status"):
        setattr(row, "status", status)


def _set_run_summary(row: Any, summary: dict[str, Any]) -> None:
    if hasattr(row, "summary_json"):
        setattr(row, "summary_json", summary)


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


def _starting_cursor(source, trigger_type: str) -> dict[str, Any]:
    if trigger_type in {"manual", "daily_refresh", "scheduled"}:
        return {"page": 1}
    return dict(source.cursor_json or {})


def _filter_reason(payload: dict[str, Any], runtime_config: dict[str, Any]) -> str | None:
    if not runtime_config:
        return None

    state = _normalize_optional_filter_value(runtime_config.get("state"))
    county = _normalize_optional_filter_value(runtime_config.get("county"))
    city = _normalize_optional_filter_value(runtime_config.get("city"))

    if state and _norm_text(payload.get("state")) != _norm_text(state):
        return "state"

    payload_county = payload.get("county")
    if county and payload_county:
        if _normalize_county_text(payload_county) != _normalize_county_text(county):
            return "county"

    if city and _norm_text(payload.get("city")) != _norm_text(city):
        return "city"

    asking_price = _safe_float(payload.get("asking_price")) or 0.0
    min_price = runtime_config.get("min_price")
    max_price = runtime_config.get("max_price")
    if min_price is not None and asking_price < float(min_price):
        return "min_price"
    if max_price is not None and asking_price > float(max_price):
        return "max_price"

    bedrooms = _safe_float(payload.get("bedrooms")) or 0.0
    bathrooms = _safe_float(payload.get("bathrooms")) or 0.0
    min_bedrooms = runtime_config.get("min_bedrooms")
    min_bathrooms = runtime_config.get("min_bathrooms")
    if min_bedrooms is not None and bedrooms < float(min_bedrooms):
        return "min_bedrooms"
    if min_bathrooms is not None and bathrooms < float(min_bathrooms):
        return "min_bathrooms"

    max_units = runtime_config.get("max_units")
    if max_units is not None:
        units = _safe_float(payload.get("units"))
        if units is not None and units > float(max_units):
            return "max_units"

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


def _load_rows_page(
    source,
    *,
    trigger_type: str,
    runtime_config: dict[str, Any],
    cursor: dict[str, Any],
    provider_fetch_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], int]:
    base_config = dict(source.config_json or {})
    merged_config = {**base_config, **dict(runtime_config or {})}
    merged_config["limit"] = int(provider_fetch_limit)

    sample_rows = merged_config.get("sample_rows")
    if isinstance(sample_rows, list):
        rows = [x for x in sample_rows if isinstance(x, dict)]
        return rows, {"page": 1}, len(rows)

    adapter = PROVIDER_ADAPTERS.get(source.provider)
    if adapter is None:
        raise ValueError(f"No adapter registered for provider={source.provider}")

    result = adapter.load_rows_page(
        credentials=dict(getattr(source, "credentials_json", None) or {}),
        runtime_config=merged_config,
        cursor=cursor,
    )
    return result.rows, dict(result.next_cursor or {}), int(result.raw_count or 0)


def execute_source_sync(
    db: Session,
    *,
    org_id: int,
    source: Any,
    trigger_type: str = "manual",
    runtime_config: dict[str, Any] | None = None,
):
    runtime_config = _normalize_runtime_config(runtime_config)
    runtime_config["trigger_type"] = str(trigger_type or "manual")

    provider_fetch_limit = min(
        int(runtime_config.get("limit") or 100),
        int(getattr(settings, "market_sync_default_limit_per_market", 125) or 125),
    )

    idempotency_key = build_run_idempotency_key(
        org_id=int(org_id),
        source=source,
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )

    lock_owner = _build_lock_owner(
        org_id=int(org_id),
        source_id=int(getattr(source, "id")),
        idempotency_key=idempotency_key,
    )

    execution_lock = acquire_ingestion_execution_lock(
        db,
        org_id=int(org_id),
        lock_key=f"ingestion:source:{int(getattr(source, 'id'))}",
        owner=lock_owner,
        ttl_seconds=int(getattr(settings, "ingestion_execution_lock_ttl_seconds", DEFAULT_EXECUTION_LOCK_TTL_SECONDS)),
    )
    if not execution_lock.acquired:
        return start_run(
            db,
            org_id=int(org_id),
            source_id=int(getattr(source, "id")),
            trigger_type=str(trigger_type),
            runtime_config=runtime_config,
            status="skipped_locked",
            summary_json={"reason": "execution_lock_not_acquired"},
        )

    if has_completed_ingestion_dataset(
        db,
        org_id=int(org_id),
        key=idempotency_key,
    ):
        release_lock(db, org_id=int(org_id), lock_key=f"ingestion:source:{int(getattr(source, 'id'))}", owner=lock_owner)
        return start_run(
            db,
            org_id=int(org_id),
            source_id=int(getattr(source, "id")),
            trigger_type=str(trigger_type),
            runtime_config=runtime_config,
            status="skipped_duplicate_dataset",
            summary_json={"reason": "already_completed"},
        )

    run = start_run(
        db,
        org_id=int(org_id),
        source_id=int(getattr(source, "id")),
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )

    summary = {
        "records_seen": 0,
        "records_imported": 0,
        "properties_created": 0,
        "properties_updated": 0,
        "deals_created": 0,
        "deals_updated": 0,
        "rent_rows_upserted": 0,
        "photos_upserted": 0,
        "duplicates_skipped": 0,
        "invalid_rows": 0,
        "filtered_out": 0,
        "filter_reason_counts": {},
        "normal_path": True,
    }

    try:
        cursor = _starting_cursor(source, trigger_type)
        max_pages = max(1, int(getattr(settings, "ingestion_provider_max_pages_per_shard", 3) or 3))
        pages_scanned = 0

        while cursor and pages_scanned < max_pages:
            rows, next_cursor, raw_count = _load_rows_page(
                source,
                trigger_type=str(trigger_type),
                runtime_config=runtime_config,
                cursor=cursor,
                provider_fetch_limit=provider_fetch_limit,
            )
            pages_scanned += 1

            for raw in rows:
                summary["records_seen"] += 1

                payload = canonical_listing_payload(raw)
                if not _is_valid_payload(payload):
                    summary["invalid_rows"] += 1
                    continue

                reason = _filter_reason(payload, runtime_config)
                if reason:
                    summary["filtered_out"] += 1
                    summary["filter_reason_counts"][reason] = int(summary["filter_reason_counts"].get(reason, 0)) + 1
                    continue

                external_link = find_existing_by_external_id(
                    db,
                    org_id=int(org_id),
                    provider=str(source.provider),
                    external_record_id=str(payload["external_record_id"]),
                )
                if external_link is not None:
                    summary["duplicates_skipped"] += 1
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

                prop, prop_created = _upsert_property(db, org_id=int(org_id), payload=payload)
                deal, deal_created = _upsert_deal(db, org_id=int(org_id), property_id=int(prop.id), payload=payload)
                rent, rent_created = _upsert_rent_assumption(db, org_id=int(org_id), property_id=int(prop.id), payload=payload)
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
                    source_id=int(getattr(source, "id")),
                    external_record_id=str(payload["external_record_id"]),
                    external_url=payload.get("external_url"),
                    property_id=int(prop.id),
                    deal_id=int(deal.id) if getattr(deal, "id", None) else None,
                    raw_json=payload.get("raw_json") or payload,
                    fingerprint=fingerprint,
                )

                summary["records_imported"] += 1
                summary["properties_created"] += 1 if prop_created else 0
                summary["properties_updated"] += 1 if (not prop_created and prop_before is not None) else 0
                summary["deals_created"] += 1 if deal_created else 0
                summary["deals_updated"] += 1 if not deal_created else 0
                summary["rent_rows_upserted"] += 1 if rent is not None else 0
                summary["photos_upserted"] += int(photos_added)

                try:
                    pipeline_summary = execute_post_ingestion_pipeline(
                        db,
                        org_id=int(org_id),
                        property_id=int(prop.id),
                        actor_user_id=None,
                        emit_events=False,
                    )
                    if isinstance(pipeline_summary, dict):
                        apply_pipeline_summary(summary, pipeline_summary)
                except Exception as exc:
                    logger.exception("post_ingestion_pipeline_failed property_id=%s", getattr(prop, "id", None))
                    summary.setdefault("post_import_failures", 0)
                    summary["post_import_failures"] = int(summary.get("post_import_failures", 0)) + 1
                    summary.setdefault("post_import_errors", [])
                    summary["post_import_errors"].append(str(exc))

            cursor = dict(next_cursor or {})

        source.cursor_json = cursor or {}
        db.add(source)

        mark_ingestion_dataset_completed(
            db,
            org_id=int(org_id),
            key=idempotency_key,
            ttl_seconds=int(getattr(settings, "ingestion_completion_lock_ttl_seconds", DEFAULT_COMPLETION_LOCK_TTL_SECONDS)),
        )

        _set_run_summary(run, summary)
        _set_run_status(run, "completed")
        finish_run(db, run, status="completed", summary_json=summary)
        return run

    except Exception as exc:
        logger.exception("execute_source_sync_failed")
        _set_run_summary(run, {**summary, "error": str(exc)})
        _set_run_status(run, "failed")
        finish_run(db, run, status="failed", summary_json={**summary, "error": str(exc)})
        raise
    finally:
        release_lock(
            db,
            org_id=int(org_id),
            lock_key=f"ingestion:source:{int(getattr(source, 'id'))}",
            owner=lock_owner,
        )