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

from app.db import rollback_quietly

from app.config import settings
from ..middleware.structured_logging import emit_structured_log
from app.models import Deal, Property, PropertyPhoto, RentAssumption
from app.services.ingestion_dedupe_service import (
    build_property_fingerprint,
    find_existing_by_external_id,
    find_existing_property,
    upsert_record_link,
)
from app.services.address_normalization import normalize_full_address
from app.services.ingestion_enrichment_service import (
    apply_pipeline_summary,
    canonical_listing_payload,
    derive_photo_kind,
    execute_post_ingestion_pipeline,
)
from app.services.ingestion_run_service import finish_run, finish_run_in_new_session, start_run
from app.services.locks_service import (
    acquire_ingestion_execution_lock,
    build_ingestion_execution_lock_key,
    clear_stale_lock,
    has_completed_ingestion_dataset,
    mark_ingestion_dataset_completed,
    release_ingestion_execution_lock,
    release_ingestion_execution_lock_in_new_session,
)
from app.services.market_sync_service import (
    advance_market_cursor,
    build_market_dataset_identity,
    get_market_sync_state_by_id,
    mark_market_sync_completed,
    mark_market_sync_started,
)
from app.services.rentcast_listing_source import (
    RentCastListingFetchResult,
    RentCastListingSource,
)
from ..workers.celery_app import celery_app

logger = logging.getLogger(__name__)

PROVIDER_ADAPTERS = {"rentcast": RentCastListingSource()}

DEFAULT_EXECUTION_LOCK_TTL_SECONDS = 60 * 60 * 3
DEFAULT_COMPLETION_LOCK_TTL_SECONDS = 60 * 60 * 24 * 14


def _emit(payload: dict[str, Any], level: int = logging.INFO) -> None:
    emit_structured_log("onehaven.ingestion", payload, level=level)




def _probe_session(db: Session, *, phase: str, org_id: int, source_id: int) -> None:
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        logger.exception(
            "ingestion_session_probe_failed",
            extra={"org_id": int(org_id), "source_id": int(source_id), "phase": phase},
        )
        rollback_quietly(db)
        raise

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


def _coerce_zip_codes(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, str):
        raw = [x.strip() for x in value.split(",") if x.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw = [str(x).strip() for x in value if str(x).strip()]
    else:
        raw = [str(value).strip()] if str(value).strip() else []

    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


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

    if not property_types:
        property_types = ["single_family", "multi_family"]
    payload["property_types"] = [str(x).strip() for x in property_types if str(x).strip()]

    payload["max_price"] = _safe_float(payload.get("max_price"))
    if payload.get("max_price") is None:
        payload["max_price"] = 200000.0

    payload["sync_mode"] = str(payload.get("sync_mode") or "refresh").strip().lower() or "refresh"
    payload["market_slug"] = _normalize_optional_filter_value(payload.get("market_slug"))
    payload["max_pages_budget"] = _safe_int(payload.get("max_pages_budget"))

    payload["defer_optional_post_pipeline"] = bool(
        payload.get(
            "defer_optional_post_pipeline",
            getattr(settings, "ingestion_defer_optional_post_pipeline", True),
        )
    )

    zip_codes = _coerce_zip_codes(payload.get("zip_codes"))
    if payload.get("market_slug") == "pontiac-oakland" and not zip_codes:
        zip_codes = ["48340", "48341", "48342", "48343"]
    payload["zip_codes"] = zip_codes

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
        "zip_codes": runtime_config.get("zip_codes"),
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

def _parse_dt_utc(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        raw = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _normalize_listing_status(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    if raw == "active":
        return "Active"
    if raw == "inactive":
        return "Inactive"
    return str(value).strip() or None


def _build_listing_reconciliation_metadata(payload: dict[str, Any], *, trigger_type: str) -> dict[str, Any]:
    raw = dict(payload.get("raw") or {})
    listing_status = _normalize_listing_status(raw.get("status") or payload.get("listing_status"))
    removed_at = _parse_dt_utc(raw.get("removedDate") or raw.get("removed_date"))
    listed_at = _parse_dt_utc(raw.get("listedDate") or raw.get("listed_date"))
    created_at = _parse_dt_utc(raw.get("createdDate") or raw.get("created_date"))
    last_seen_at = _parse_dt_utc(raw.get("lastSeenDate") or raw.get("last_seen_date"))

    listing_hidden = listing_status == "Inactive"

    return {
        "trigger_type": trigger_type,
        "listing_status": listing_status,
        "listing_hidden": bool(listing_hidden),
        "listing_listed_at": listed_at.isoformat() if listed_at else None,
        "listing_created_at": created_at.isoformat() if created_at else None,
        "listing_removed_at": removed_at.isoformat() if removed_at else None,
        "listing_last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
        "listing_days_on_market": _safe_int(raw.get("daysOnMarket") or raw.get("days_on_market")),
        "listing_price": _safe_float(raw.get("price") or payload.get("asking_price")),
        "listing_mls_name": str(raw.get("mlsName") or "").strip() or None,
        "listing_mls_number": str(raw.get("mlsNumber") or "").strip() or None,
        "listing_type": str(raw.get("listingType") or "").strip() or None,
        "listing_hidden_reason": "inactive_listing" if listing_hidden else None,
        "listing_agent": dict(raw.get("listingAgent") or {}) if isinstance(raw.get("listingAgent"), dict) else None,
        "listing_office": dict(raw.get("listingOffice") or {}) if isinstance(raw.get("listingOffice"), dict) else None,
        "raw": raw,
    }


def _read_property_metadata(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT acquisition_metadata_json
            FROM properties
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).fetchone()
    if row is None:
        return {}
    raw = row._mapping.get("acquisition_metadata_json")
    return dict(raw or {}) if isinstance(raw, dict) else {}


def _mark_deal_visibility_from_listing(db: Session, *, org_id: int, property_id: int, listing_hidden: bool) -> None:
    db.execute(
        text(
            """
            UPDATE deals
            SET updated_at = NOW(),
                metadata_json = COALESCE(metadata_json, '{}'::jsonb) || CAST(:meta AS JSONB)
            WHERE org_id = :org_id AND property_id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "meta": json.dumps(
                {
                    "listing_hidden": bool(listing_hidden),
                    "listing_hidden_reason": "inactive_listing" if listing_hidden else None,
                },
                default=str,
            ),
        },
    )

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

    existing_meta = _read_property_metadata(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
    )

    reconciled = _build_listing_reconciliation_metadata(
        payload,
        trigger_type=trigger_type,
    )

    prior_price = _safe_float(existing_meta.get("listing_price"))
    next_price = _safe_float(reconciled.get("listing_price"))
    if prior_price is not None and next_price is not None and prior_price != next_price:
        reconciled["listing_price_changed"] = True
        reconciled["listing_previous_price"] = prior_price
    else:
        reconciled["listing_price_changed"] = False
        reconciled["listing_previous_price"] = prior_price

    prior_status = _normalize_listing_status(existing_meta.get("listing_status"))
    next_status = _normalize_listing_status(reconciled.get("listing_status"))
    reconciled["listing_status_changed"] = bool(prior_status and next_status and prior_status != next_status)
    reconciled["listing_previous_status"] = prior_status

    agent = dict(reconciled.get("listing_agent") or {})
    office = dict(reconciled.get("listing_office") or {})
    raw = dict(reconciled.get("raw") or payload.get("raw") or {})

    zillow_url = (
        raw.get("zillowUrl")
        or raw.get("zillow_url")
        or raw.get("listingUrl")
        or payload.get("external_url")
    )

    metadata_json = {
        "trigger_type": trigger_type,
        "last_payload_address": payload.get("address"),
        "last_payload_city": payload.get("city"),
        "inventory_count": payload.get("inventory_count"),
        **reconciled,
    }

    db.execute(
        text(
            """
            UPDATE properties
            SET acquisition_first_seen_at = COALESCE(acquisition_first_seen_at, :now_ts),
                acquisition_last_seen_at = COALESCE(:listing_last_seen_at, :now_ts),
                acquisition_source_provider = :provider,
                acquisition_source_slug = :slug,
                acquisition_source_record_id = :record_id,
                acquisition_source_url = COALESCE(:source_url, acquisition_source_url),

                listing_status = :listing_status,
                listing_hidden = :listing_hidden,
                listing_hidden_reason = :listing_hidden_reason,
                listing_last_seen_at = :listing_last_seen_at,
                listing_removed_at = :listing_removed_at,
                listing_listed_at = :listing_listed_at,
                listing_created_at = :listing_created_at,
                listing_days_on_market = :listing_days_on_market,
                listing_price = :listing_price,
                listing_mls_name = :listing_mls_name,
                listing_mls_number = :listing_mls_number,
                listing_type = :listing_type,
                listing_zillow_url = COALESCE(:listing_zillow_url, listing_zillow_url),
                listing_agent_name = :listing_agent_name,
                listing_agent_phone = :listing_agent_phone,
                listing_agent_email = :listing_agent_email,
                listing_agent_website = :listing_agent_website,
                listing_office_name = :listing_office_name,
                listing_office_phone = :listing_office_phone,
                listing_office_email = :listing_office_email,

                acquisition_metadata_json = COALESCE(acquisition_metadata_json, '{}'::jsonb) || CAST(:metadata_json AS JSONB)
            WHERE org_id = :org_id AND id = :property_id
            """
        ),
        {
            "org_id": int(org_id),
            "property_id": int(property_id),
            "now_ts": now,
            "listing_last_seen_at": _parse_dt_utc(reconciled.get("listing_last_seen_at")) or now,
            "listing_removed_at": _parse_dt_utc(reconciled.get("listing_removed_at")),
            "listing_listed_at": _parse_dt_utc(reconciled.get("listing_listed_at")),
            "listing_created_at": _parse_dt_utc(reconciled.get("listing_created_at")),
            "provider": str(getattr(source, "provider", "") or "").strip() or None,
            "slug": str(getattr(source, "slug", "") or "").strip() or None,
            "record_id": str(payload.get("external_record_id") or "").strip() or None,
            "source_url": payload.get("external_url"),

            "listing_status": reconciled.get("listing_status"),
            "listing_hidden": bool(reconciled.get("listing_hidden")),
            "listing_hidden_reason": reconciled.get("listing_hidden_reason"),
            "listing_days_on_market": _safe_int(reconciled.get("listing_days_on_market")),
            "listing_price": _safe_float(reconciled.get("listing_price")),
            "listing_mls_name": reconciled.get("listing_mls_name"),
            "listing_mls_number": reconciled.get("listing_mls_number"),
            "listing_type": reconciled.get("listing_type"),
            "listing_zillow_url": zillow_url,
            "listing_agent_name": str(agent.get("name") or "").strip() or None,
            "listing_agent_phone": str(agent.get("phone") or "").strip() or None,
            "listing_agent_email": str(agent.get("email") or "").strip() or None,
            "listing_agent_website": str(agent.get("website") or "").strip() or None,
            "listing_office_name": str(office.get("name") or "").strip() or None,
            "listing_office_phone": str(office.get("phone") or "").strip() or None,
            "listing_office_email": str(office.get("email") or "").strip() or None,

            "metadata_json": json.dumps(metadata_json, default=str),
        },
    )

    _mark_deal_visibility_from_listing(
        db,
        org_id=int(org_id),
        property_id=int(property_id),
        listing_hidden=bool(reconciled.get("listing_hidden")),
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


def _mark_property_enrichment_queued(db: Session, *, org_id: int, property_id: int) -> None:
   return


def _canonicalize_property_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_full_address(
        payload.get("address"),
        payload.get("city"),
        payload.get("state"),
        payload.get("zip"),
    )
    out = dict(payload)
    if normalized.address_line1:
        out["address"] = normalized.address_line1
    if normalized.city:
        out["city"] = normalized.city
    if normalized.state:
        out["state"] = normalized.state
    if normalized.postal_code:
        out["zip"] = normalized.postal_code
    out["normalized_address"] = normalized.full_address
    return out


def _upsert_property(db: Session, *, org_id: int, payload: dict[str, Any]):
    payload = _canonicalize_property_payload(payload)
    existing = find_existing_property(
        db,
        org_id=org_id,
        address=payload["address"],
        city=payload["city"],
        state=payload["state"],
        zip_code=payload["zip"],
    )

    raw = dict(payload.get("raw") or {})
    incoming_price = _safe_float(raw.get("price") or payload.get("asking_price"))
    incoming_status = _normalize_listing_status(raw.get("status") or payload.get("listing_status"))

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
        if hasattr(existing, "normalized_address"):
            existing.normalized_address = normalize_full_address(
                payload.get("address"),
                payload.get("city"),
                payload.get("state"),
                payload.get("zip"),
            ).full_address
        db.add(existing)
        db.flush()
        created = True
    else:
        existing.county = payload.get("county") or existing.county
        existing.bedrooms = int(payload["bedrooms"] or existing.bedrooms or 0)
        existing.bathrooms = float(payload["bathrooms"] or existing.bathrooms or 1)
        existing.square_feet = payload.get("square_feet") or existing.square_feet
        existing.year_built = payload.get("year_built") or existing.year_built
        existing.property_type = payload.get("property_type") or existing.property_type
        if hasattr(existing, "normalized_address"):
            existing.normalized_address = normalize_full_address(
                payload.get("address"),
                payload.get("city"),
                payload.get("state"),
                payload.get("zip"),
            ).full_address
        db.add(existing)
        db.flush()

    # Keep raw listing truth on the property row if your model already has these economics fields.
    # No hard-delete behavior here. Inactive hiding is handled through metadata.
    if incoming_price is not None and hasattr(existing, "asking_price"):
        try:
            existing.asking_price = incoming_price
            db.add(existing)
            db.flush()
        except Exception:
            rollback_quietly(db)
            raise

    if incoming_status and hasattr(existing, "raw_json"):
        current_raw = dict(getattr(existing, "raw_json", None) or {})
        current_raw["listing_status"] = incoming_status
        setattr(existing, "raw_json", current_raw)
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_deal(db: Session, *, org_id: int, property_id: int, payload: dict[str, Any]):
    payload = _canonicalize_property_payload(payload)
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
        if hasattr(existing, "normalized_address"):
            existing.normalized_address = normalize_full_address(
                payload.get("address"),
                payload.get("city"),
                payload.get("state"),
                payload.get("zip"),
            ).full_address
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
        if hasattr(existing, "normalized_address"):
            existing.normalized_address = normalize_full_address(
                payload.get("address"),
                payload.get("city"),
                payload.get("state"),
                payload.get("zip"),
            ).full_address
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_rent_assumption(db: Session, *, org_id: int, property_id: int, payload: dict[str, Any]):
    payload = _canonicalize_property_payload(payload)
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
        if hasattr(existing, "normalized_address"):
            existing.normalized_address = normalize_full_address(
                payload.get("address"),
                payload.get("city"),
                payload.get("state"),
                payload.get("zip"),
            ).full_address
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
        return dict(getattr(source, "cursor_json", None) or {"page": 1, "shard": 1, "sort_mode": "newest"})

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
    runtime_zip_codes = set(_coerce_zip_codes(runtime_config.get("zip_codes")))
    active_variant = dict(query_variant or {})

    query_city = _normalize_optional_filter_value(active_variant.get("city"))
    query_county = _normalize_optional_filter_value(active_variant.get("county"))
    query_zip_code = _normalize_optional_filter_value(active_variant.get("zip_code"))

    payload_state = _normalize_optional_filter_value(payload.get("state"))
    payload_city = _normalize_optional_filter_value(payload.get("city"))
    payload_county = _normalize_optional_filter_value(payload.get("county"))
    payload_zip = _normalize_optional_filter_value(payload.get("zip"))

    if state and _norm_text(payload_state) != _norm_text(state):
        return "state"

    if query_zip_code:
        if payload_zip != query_zip_code:
            return "zip_code"
    elif query_city:
        if _norm_text(payload_city) != _norm_text(query_city):
            return "city"
    elif query_county and payload_county:
        if _normalize_county_text(payload_county) != _normalize_county_text(query_county):
            return "county"
    elif runtime_city:
        if _norm_text(payload_city) != _norm_text(runtime_city):
            return "city"

    if runtime_zip_codes and payload_zip and payload_zip not in runtime_zip_codes:
        return "zip_scope"

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

    adapter = PROVIDER_ADAPTERS.get(source.provider)
    if adapter is None:
        raise ValueError(f"No adapter registered for provider={source.provider}")

    return adapter.load_rows_page(
        credentials=dict(getattr(source, "credentials_json", None) or {}),
        runtime_config=merged_config,
        cursor=cursor,
    )


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


def _should_skip_completed_dataset(*, trigger_type: str, runtime_config: dict[str, Any]) -> bool:
    normalized_trigger = str(trigger_type or "").strip().lower()
    normalized_mode = str(runtime_config.get("sync_mode") or "refresh").strip().lower()

    if bool(runtime_config.get("allow_completed_dataset_skip")):
        return True
    if bool(runtime_config.get("disable_completed_dataset_skip")):
        return False
    if normalized_mode == "refresh":
        return False
    if normalized_trigger in {"manual", "manual_market_sync", "sync_now", "refresh", "daily_refresh"}:
        return False
    return normalized_mode == "backfill"


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

    Important: the optional pipeline is isolated behind a savepoint so any failure
    inside it cannot leave the shared ingestion session in an aborted state.
    """
    if bool(runtime_config.get("defer_optional_post_pipeline", False)):
        return True, ["optional_post_pipeline_deferred"], 0.0

    t0 = time.perf_counter()
    pipeline_errors: list[str] = []
    was_partial = False

    try:
        with db.begin_nested():
            result = execute_post_ingestion_pipeline(
                db,
                org_id=int(org_id),
                property_id=int(property_row.id),
                actor_user_id=None,
                emit_events=False,
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
        rollback_quietly(db)
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
        rollback_quietly(db)

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    return was_partial, pipeline_errors, elapsed_ms


def _queue_post_sync_enrichment(
    *,
    org_id: int,
    source_id: int,
    run_id: int,
    property_ids: list[int],
) -> int:
    queued = 0
    seen: set[int] = set()

    for property_id in property_ids:
        pid = int(property_id)
        if pid <= 0 or pid in seen:
            continue
        seen.add(pid)

        celery_app.send_task(
            "ingestion.enrich_property_after_sync",
            kwargs={
                "org_id": int(org_id),
                "property_id": pid,
                "source_id": int(source_id),
                "run_id": int(run_id),
            },
        )
        queued += 1

    return queued


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
    provider_fetch_limit = min(max(1, requested_new_records), 500)
    max_pages_budget = _safe_int(runtime_config.get("max_pages_budget")) or max(
        1,
        int(getattr(settings, "ingestion_provider_max_pages_per_shard", 12) or 12),
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
        or DEFAULT_EXECUTION_LOCK_TTL_SECONDS
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

    if execution_lock.acquired and bool(
        getattr(settings, "ingestion_commit_execution_lock_on_acquire", True)
    ):
        try:
            db.commit()
        except Exception:
            logger.exception("commit_after_execution_lock_acquire_failed")
            rollback_quietly(db)
            raise

    _probe_session(db, phase="after_execution_lock_setup", org_id=int(org_id), source_id=source_id)

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

    skip_completed_dataset = _should_skip_completed_dataset(
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )
    if skip_completed_dataset:
        if has_completed_ingestion_dataset(
            db,
            org_id=int(org_id),
            source_id=source_id,
            dataset_key=dataset_key,
        ):
            return start_run(
                db,
                org_id=int(org_id),
                source_id=source_id,
                trigger_type=str(trigger_type),
                runtime_config=runtime_config,
                status="skipped_duplicate_dataset",
                summary_json={
                    "reason": "dataset_already_completed",
                    "dataset_key": dataset_key,
                    "dataset_identity": dataset_identity,
                    "market_slug": runtime_config.get("market_slug") or dataset_identity.get("market_slug"),
                    "sync_mode": runtime_config.get("sync_mode") or "refresh",
                    "completed_dataset_skip_applied": True,
                },
            )
    else:
        _emit(
            {
                "event": "ingestion_completed_dataset_skip_disabled",
                "org_id": int(org_id),
                "source_id": source_id,
                "dataset_key": dataset_key,
                "dataset_identity": dataset_identity,
                "market_slug": runtime_config.get("market_slug") or dataset_identity.get("market_slug"),
                "sync_mode": runtime_config.get("sync_mode") or "refresh",
                "trigger_type": str(trigger_type),
            }
        )

    run = start_run(
        db,
        org_id=int(org_id),
        source_id=source_id,
        trigger_type=str(trigger_type),
        runtime_config=runtime_config,
    )
    _probe_session(db, phase="after_start_run", org_id=int(org_id), source_id=source_id)

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
        "post_sync_enrichment_queued": 0,
        "post_sync_property_ids": [],
    }

    properties_to_enrich: list[int] = []

    try:
        if market_sync_state is not None:
            mark_market_sync_started(
                db,
                sync_state=market_sync_state,
                requested_limit=requested_new_records,
                status="running",
            )
            _probe_session(db, phase="after_mark_market_sync_started", org_id=int(org_id), source_id=source_id)

        cursor = dict(starting_cursor or {})
        provider_pages_scanned = 0
        market_exhausted = False
        last_seen_provider_record_at: datetime | None = None
        empty_page_grace = max(1, _safe_int(runtime_config.get("empty_page_grace")) or 2)
        consecutive_empty_pages = 0

        while True:
            if summary["new_records_imported"] >= requested_new_records:
                summary["stop_reason"] = "requested_new_records_satisfied"
                break

            if _budget_exhausted(
                provider_pages_scanned=provider_pages_scanned,
                max_pages_budget=max_pages_budget,
            ):
                summary["budget_boundary_hit"] = True
                summary["stop_reason"] = "provider_page_budget_exhausted"
                break

            if market_exhausted:
                summary["stop_reason"] = "market_exhausted"
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
            _probe_session(db, phase="after_provider_fetch_before_row_loop", org_id=int(org_id), source_id=source_id)
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

            page_is_empty = raw_count <= 0 and len(rows) <= 0
            if page_is_empty:
                consecutive_empty_pages += 1
            else:
                consecutive_empty_pages = 0

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
                "empty_page": bool(page_is_empty),
                "consecutive_empty_pages": int(consecutive_empty_pages),
                "empty_page_grace": int(empty_page_grace),
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

            for row in rows:
                payload = canonical_listing_payload(row)
                reason = _filter_reason(
                    payload,
                    runtime_config,
                    query_variant=dict(fetch_result.query_variant or {}).get("post_filter"),
                )
                if reason:
                    summary["filtered_out"] += 1
                    counts = dict(summary.get("filter_reason_counts") or {})
                    counts[reason] = int(counts.get(reason, 0) or 0) + 1
                    summary["filter_reason_counts"] = counts
                    examples = dict(summary.get("filter_reason_examples") or {})
                    if reason not in examples:
                        examples[reason] = payload.get("address")
                    summary["filter_reason_examples"] = examples
                    page_stat["filtered_out"] += 1
                    continue

                summary["records_candidate_after_filtering"] += 1
                page_stat["records_candidate_after_filtering"] += 1

                if not _is_valid_payload(payload):
                    summary["invalid_rows"] += 1
                    page_stat["invalid_rows"] += 1
                    _append_property_error(
                        summary,
                        property_id=None,
                        external_record_id=payload.get("external_record_id"),
                        reason="invalid_payload",
                    )
                    continue

                _probe_session(
                    db,
                    phase="before_find_existing_by_external_id",
                    org_id=int(org_id),
                    source_id=source_id,
                )
                existing_record = find_existing_by_external_id(
                    db,
                    org_id=int(org_id),
                    provider=str(getattr(source, "provider", "") or ""),
                    external_record_id=str(payload.get("external_record_id") or ""),
                )

                prop_row, prop_created = _upsert_property(db, org_id=int(org_id), payload=payload)
                deal_row, deal_created = _upsert_deal(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                    payload=payload,
                )
                _, rent_created = _upsert_rent_assumption(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                    payload=payload,
                )
                photos_upserted = _upsert_photos(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                    provider=str(getattr(source, "provider", "") or "rentcast"),
                    photos=list(payload.get("photos") or []),
                )

                fingerprint = build_property_fingerprint(
                    address=payload["address"],
                    city=payload["city"],
                    state=payload["state"],
                    zip_code=payload["zip"],
                )

                upsert_record_link(
                    db,
                    org_id=int(org_id),
                    provider=str(getattr(source, "provider", "") or ""),
                    source_id=int(getattr(source, "id")),
                    external_record_id=str(payload.get("external_record_id") or ""),
                    external_url=payload.get("external_url"),
                    property_id=int(prop_row.id),
                    deal_id=int(deal_row.id) if deal_row is not None else None,
                    raw_json=payload.get("raw_json") or payload,
                    fingerprint=fingerprint,
                )

                _persist_property_acquisition_metadata(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                    source=source,
                    payload=payload,
                    trigger_type=str(trigger_type),
                )

                meta_after = _read_property_metadata(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                )

                if bool(meta_after.get("listing_hidden")):
                    summary["inactive_listings_hidden"] = int(summary.get("inactive_listings_hidden") or 0) + 1
                    page_stat["inactive_listings_hidden"] = int(page_stat.get("inactive_listings_hidden") or 0) + 1

                if bool(meta_after.get("listing_status_changed")):
                    summary["listing_status_changes"] = int(summary.get("listing_status_changes") or 0) + 1
                    page_stat["listing_status_changes"] = int(page_stat.get("listing_status_changes") or 0) + 1

                if bool(meta_after.get("listing_price_changed")):
                    summary["listing_price_changes"] = int(summary.get("listing_price_changes") or 0) + 1
                    page_stat["listing_price_changes"] = int(page_stat.get("listing_price_changes") or 0) + 1

                _seed_missing_completeness_columns(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                )
                _mark_property_enrichment_queued(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop_row.id),
                )
                properties_to_enrich.append(int(prop_row.id))

                if prop_created:
                    summary["properties_created"] += 1
                else:
                    summary["properties_updated"] += 1

                if deal_created:
                    summary["deals_created"] += 1
                else:
                    summary["deals_updated"] += 1

                if rent_created:
                    summary["rent_rows_upserted"] += 1

                summary["photos_upserted"] += int(photos_upserted or 0)

                if existing_record is None:
                    summary["new_records_imported"] += 1
                    summary["new_listings_imported"] += 1
                    page_stat["new_records_imported"] += 1
                    page_stat["new_listings_imported"] += 1
                else:
                    summary["already_seen_skipped"] += 1
                    page_stat["already_seen_skipped"] += 1

                summary["records_imported"] += 1
                page_stat["imported"] += 1

                _, pipeline_errors, post_pipeline_ms = _run_optional_post_pipeline(
                    db,
                    org_id=int(org_id),
                    property_row=prop_row,
                    deal_row=deal_row,
                    payload=payload,
                    runtime_config=runtime_config,
                    summary=summary,
                )
                summary["timings_ms"]["post_pipeline_total"] = round(
                    float(summary["timings_ms"].get("post_pipeline_total", 0.0) or 0.0) + post_pipeline_ms,
                    2,
                )
                if pipeline_errors:
                    page_stat["pipeline_failures"] += 1

                last_seen_provider_record_at = datetime.now(timezone.utc).replace(tzinfo=None)

            next_cursor = dict(fetch_result.next_cursor or {})
            page_stat["page_total_ms"] = round((time.perf_counter() - page_t0) * 1000, 2)
            summary["page_stats"].append(page_stat)
            summary["cursor_advanced_to"] = _cursor_summary(next_cursor)

            current_page = _safe_int(cursor.get("page")) or 1
            current_shard = _safe_int(cursor.get("shard")) or 1
            next_page = _safe_int(next_cursor.get("page")) or current_page
            next_shard = _safe_int(next_cursor.get("shard")) or current_shard

            advanced_to_new_variant = next_shard != current_shard
            advanced_within_variant = next_page != current_page
            advanced_cursor = advanced_to_new_variant or advanced_within_variant
            market_exhausted = bool(fetch_result.exhausted)

            if page_is_empty and advanced_cursor and consecutive_empty_pages < empty_page_grace:
                market_exhausted = False
                page_stat["market_exhausted"] = False
                page_stat["empty_page_grace_used"] = True
            else:
                page_stat["empty_page_grace_used"] = False

            cursor = next_cursor

            if market_exhausted and advanced_cursor:
                market_exhausted = False
                page_stat["market_exhausted"] = False

        summary["market_exhausted"] = bool(market_exhausted)

        if int(summary.get("records_imported", 0) or 0) > 0:
            queued_count = _queue_post_sync_enrichment(
                org_id=int(org_id),
                source_id=source_id,
                run_id=int(getattr(run, "id")),
                property_ids=properties_to_enrich,
            )
            summary["post_sync_enrichment_queued"] = int(queued_count)
            summary["post_sync_property_ids"] = sorted(set(int(x) for x in properties_to_enrich))

        if market_sync_state is not None:
            advance_market_cursor(
                db,
                sync_state=market_sync_state,
                next_cursor=cursor,
                page_scanned=current_page,
                shard_scanned=current_shard,
                sort_mode=str(fetch_result.sort_mode or cursor.get("sort_mode") or "newest"),
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

        if int(summary.get("records_imported", 0) or 0) > 0 and _should_skip_completed_dataset(
            trigger_type=str(trigger_type),
            runtime_config=runtime_config,
        ):
            completion_ttl = int(
                getattr(settings, "ingestion_completion_lock_ttl_seconds", DEFAULT_COMPLETION_LOCK_TTL_SECONDS)
                or DEFAULT_COMPLETION_LOCK_TTL_SECONDS
            )
            mark_ingestion_dataset_completed(
                db,
                org_id=int(org_id),
                source_id=source_id,
                dataset_key=dataset_key,
                owner=lock_owner,
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
    except Exception as exc:
        logger.exception("execute_source_sync_failed", extra={"org_id": int(org_id), "source_id": source_id})
        summary["timings_ms"]["run_total"] = round((time.perf_counter() - run_t0) * 1000, 2)
        summary.setdefault("error", str(exc))
        error_summary = str(exc)
        error_json = {
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }

        rollback_quietly(db)

        try:
            finish_run(
                db,
                run,
                status="failed",
                summary_json=summary,
                error_summary=error_summary,
                error_json=error_json,
            )
        except Exception:
            logger.exception(
                "execute_source_sync_finish_run_failed",
                extra={"org_id": int(org_id), "source_id": source_id, "run_id": int(getattr(run, "id"))},
            )
            finish_run_in_new_session(
                run_id=int(getattr(run, "id")),
                status="failed",
                summary_json=summary,
                error_summary=error_summary,
                error_json=error_json,
            )
        raise
    finally:
        if bool(getattr(settings, "ingestion_force_release_lock_on_finish", True)):
            released = False
            try:
                release_ingestion_execution_lock(
                    db,
                    org_id=int(org_id),
                    source_id=source_id,
                    dataset_key=dataset_key,
                    owner=lock_owner,
                    force=True,
                )
                try:
                    db.commit()
                except Exception:
                    rollback_quietly(db)
                released = True
            except Exception:
                rollback_quietly(db)
                logger.exception("release_ingestion_execution_lock_inline_failed")

            if not released:
                try:
                    release_ingestion_execution_lock_in_new_session(
                        org_id=int(org_id),
                        source_id=source_id,
                        dataset_key=dataset_key,
                        owner=lock_owner,
                        force=True,
                    )
                except Exception:
                    logger.exception("release_ingestion_execution_lock_failed")
