from __future__ import annotations

import re
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..models import Deal, Property, PropertyPhoto, RentAssumption
from ..services.ingestion_dedupe_service import (
    build_property_fingerprint,
    find_existing_by_external_id,
    find_existing_property,
    upsert_record_link,
)
from ..services.ingestion_enrichment_service import (
    canonical_listing_payload,
    derive_photo_kind,
    get_google_maps_api_key,
    refresh_property_rent_assumptions,
)
from ..services.ingestion_run_service import finish_run, start_run
from ..services.rentcast_listing_source import RentCastListingSource

PROVIDER_ADAPTERS = {"rentcast": RentCastListingSource()}


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
    limit = _safe_int(payload.get("limit")) or 100
    payload["limit"] = max(1, limit)

    payload["state"] = _normalize_optional_filter_value(payload.get("state"))
    payload["county"] = _normalize_optional_filter_value(payload.get("county"))
    payload["city"] = _normalize_optional_filter_value(payload.get("city"))
    payload["property_type"] = _normalize_optional_filter_value(payload.get("property_type"))

    for key in ["min_price", "max_price", "min_bedrooms", "min_bathrooms"]:
        payload[key] = _safe_float(payload.get(key))

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
    if trigger_type in {"manual", "daily_refresh"}:
        return {"page": 1}
    return dict(source.cursor_json or {})


def _filter_reason(payload: dict[str, Any], runtime_config: dict[str, Any]) -> str | None:
    if not runtime_config:
        return None

    state = _normalize_optional_filter_value(runtime_config.get("state"))
    county = _normalize_optional_filter_value(runtime_config.get("county"))
    city = _normalize_optional_filter_value(runtime_config.get("city"))
    property_type = _normalize_optional_filter_value(runtime_config.get("property_type"))

    if state and _norm_text(payload.get("state")) != _norm_text(state):
        return "state"

    # County is optional:
    # - if caller didn't provide county -> ignore
    # - if caller provided county but provider row has no county -> ignore
    # - only enforce when BOTH exist
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

    if property_type:
        requested_type = _normalize_property_type(property_type)
        actual_type = _normalize_property_type(payload.get("property_type"))
        if requested_type and actual_type and requested_type != actual_type:
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

    fetched = adapter.fetch_incremental(
        credentials=source.credentials_json or {},
        config=merged_config,
        cursor=cursor,
    )
    rows = fetched.get("rows") or []
    next_cursor = fetched.get("next_cursor") or {"page": 1}
    raw_count = int(fetched.get("raw_count") or len(rows))
    return rows, next_cursor, raw_count


def _collect_matching_rows(
    source,
    *,
    trigger_type: str,
    runtime_config: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    requested_limit = _safe_int(runtime_config.get("limit")) or 100

    provider_fetch_limit = max(100, min(250, requested_limit * 4))
    max_pages = max(5, min(20, requested_limit * 2))

    matched_rows: list[dict[str, Any]] = []
    seen_external_ids: set[str] = set()
    seen_fingerprints: set[str] = set()

    filter_reason_counts: dict[str, int] = {}
    raw_rows_seen = 0
    invalid_rows = 0
    duplicates_in_scan = 0
    pages_scanned = 0

    cursor = _starting_cursor(source, trigger_type)
    final_next_cursor = dict(cursor)

    for _ in range(max_pages):
        rows, next_cursor, raw_count = _load_rows_page(
            source,
            trigger_type=trigger_type,
            runtime_config=runtime_config,
            cursor=cursor,
            provider_fetch_limit=provider_fetch_limit,
        )
        pages_scanned += 1
        final_next_cursor = next_cursor

        if not rows:
            break

        raw_rows_seen += len(rows)

        for raw_row in rows:
            payload = canonical_listing_payload(raw_row)

            reason = _filter_reason(payload, runtime_config)
            if reason is not None:
                filter_reason_counts[reason] = filter_reason_counts.get(reason, 0) + 1
                continue

            if not _is_valid_payload(payload):
                invalid_rows += 1
                continue

            ext_id = str(payload["external_record_id"])
            fingerprint = build_property_fingerprint(
                address=payload["address"],
                city=payload["city"],
                state=payload["state"],
                zip_code=payload["zip"],
            )

            if ext_id in seen_external_ids or fingerprint in seen_fingerprints:
                duplicates_in_scan += 1
                continue

            seen_external_ids.add(ext_id)
            seen_fingerprints.add(fingerprint)
            matched_rows.append(raw_row)

            if len(matched_rows) >= requested_limit:
                return matched_rows, final_next_cursor, {
                    "records_seen": raw_rows_seen,
                    "invalid_rows": invalid_rows,
                    "filtered_out": sum(filter_reason_counts.values()),
                    "duplicates_skipped": duplicates_in_scan,
                    "filter_reason_counts": filter_reason_counts,
                    "provider_pages_scanned": pages_scanned,
                    "provider_fetch_limit": provider_fetch_limit,
                }

        current_page = _safe_int(cursor.get("page")) or 1
        next_page = _safe_int(next_cursor.get("page")) or 1
        if next_page <= current_page:
            break

        cursor = next_cursor

    return matched_rows, final_next_cursor, {
        "records_seen": raw_rows_seen,
        "invalid_rows": invalid_rows,
        "filtered_out": sum(filter_reason_counts.values()),
        "duplicates_skipped": duplicates_in_scan,
        "filter_reason_counts": filter_reason_counts,
        "provider_pages_scanned": pages_scanned,
        "provider_fetch_limit": provider_fetch_limit,
    }


def _run_post_import_pipeline(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "geo_ok": False,
        "rent_ok": False,
        "evaluate_ok": False,
        "state_ok": False,
        "workflow_ok": False,
        "errors": [],
    }

    try:
        from ..services.geo_enrichment import enrich_property_geo

        geo_res = enrich_property_geo(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            google_api_key=get_google_maps_api_key(),
            force=False,
        )
        result["geo_ok"] = bool(geo_res.get("ok"))
        result["geo"] = geo_res
    except Exception as e:
        result["errors"].append(f"geo:{type(e).__name__}:{e}")

    try:
        rent_res = refresh_property_rent_assumptions(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        result["rent_ok"] = bool(rent_res.get("ok"))
        result["rent"] = rent_res
    except Exception as e:
        result["errors"].append(f"rent:{type(e).__name__}:{e}")

    try:
        from ..routers.evaluate import evaluate_property_core

        eval_res = evaluate_property_core(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
            emit_events=False,
            commit=True,
        )
        result["evaluate_ok"] = bool(eval_res.get("ok"))
        result["evaluate"] = eval_res
    except Exception as e:
        result["errors"].append(f"evaluate:{type(e).__name__}:{e}")

    try:
        from ..services.property_state_machine import sync_property_state

        sync_property_state(db, org_id=int(org_id), property_id=int(property_id))
        result["state_ok"] = True
    except Exception as e:
        result["errors"].append(f"state:{type(e).__name__}:{e}")

    try:
        from ..services.workflow_gate_service import build_workflow_summary

        build_workflow_summary(db, org_id=int(org_id), property_id=int(property_id), recompute=True)
        result["workflow_ok"] = True
    except Exception as e:
        result["errors"].append(f"workflow:{type(e).__name__}:{e}")

    return result


def _apply_pipeline_summary(summary: dict[str, Any], pipeline_res: dict[str, Any], property_id: int) -> None:
    summary["post_import_pipeline_attempted"] += 1

    if pipeline_res.get("geo_ok"):
        summary["geo_enriched"] += 1
    if pipeline_res.get("rent_ok"):
        summary["rent_refreshed"] += 1
    if pipeline_res.get("evaluate_ok"):
        summary["evaluated"] += 1
    if pipeline_res.get("state_ok"):
        summary["state_synced"] += 1
    if pipeline_res.get("workflow_ok"):
        summary["workflow_synced"] += 1

    errors = list(pipeline_res.get("errors") or [])
    if errors:
        summary["post_import_failures"] += 1
        summary["post_import_errors"].append(
            {
                "property_id": int(property_id),
                "errors": errors,
            }
        )


def execute_source_sync(
    db: Session,
    *,
    org_id: int,
    source,
    trigger_type: str = "manual",
    runtime_config: dict[str, Any] | None = None,
):
    normalized_runtime = _normalize_runtime_config(runtime_config)
    run = start_run(db, org_id=org_id, source_id=source.id, trigger_type=trigger_type)

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
        "matched_before_limit": 0,
        "launch": normalized_runtime,
        "post_import_pipeline_attempted": 0,
        "geo_enriched": 0,
        "rent_refreshed": 0,
        "evaluated": 0,
        "state_synced": 0,
        "workflow_synced": 0,
        "post_import_failures": 0,
        "post_import_errors": [],
        "filter_reason_counts": {},
        "provider_pages_scanned": 0,
        "provider_fetch_limit": 0,
    }

    try:
        matched_rows, next_cursor, scan_stats = _collect_matching_rows(
            source,
            trigger_type=trigger_type,
            runtime_config=normalized_runtime,
        )

        if trigger_type != "manual":
            source.cursor_json = next_cursor or {"page": 1}
            db.add(source)
            db.commit()
            db.refresh(source)

        summary["records_seen"] = int(scan_stats.get("records_seen") or 0)
        summary["invalid_rows"] = int(scan_stats.get("invalid_rows") or 0)
        summary["filtered_out"] = int(scan_stats.get("filtered_out") or 0)
        summary["duplicates_skipped"] = int(scan_stats.get("duplicates_skipped") or 0)
        summary["filter_reason_counts"] = dict(scan_stats.get("filter_reason_counts") or {})
        summary["provider_pages_scanned"] = int(scan_stats.get("provider_pages_scanned") or 0)
        summary["provider_fetch_limit"] = int(scan_stats.get("provider_fetch_limit") or 0)

        summary["matched_before_limit"] = len(matched_rows)
        capped_rows = matched_rows[: int(normalized_runtime.get("limit") or len(matched_rows) or 100)]

        for raw_row in capped_rows:
            payload = canonical_listing_payload(raw_row)
            payload["source"] = source.provider
            ext_id = payload["external_record_id"]

            fingerprint = build_property_fingerprint(
                address=payload["address"],
                city=payload["city"],
                state=payload["state"],
                zip_code=payload["zip"],
            )

            existing_link = find_existing_by_external_id(
                db,
                org_id=org_id,
                provider=source.provider,
                external_record_id=ext_id,
            )

            if existing_link is not None and getattr(existing_link, "property_id", None):
                prop = db.get(Property, int(existing_link.property_id))
                if prop is None or int(getattr(prop, "org_id", 0)) != int(org_id):
                    summary["duplicates_skipped"] += 1
                    continue

                prop.address = payload["address"]
                prop.city = payload["city"]
                prop.state = payload["state"]
                prop.zip = payload["zip"]
                prop.county = payload.get("county") or getattr(prop, "county", None)
                prop.bedrooms = int(payload["bedrooms"] or getattr(prop, "bedrooms", 0) or 0)
                prop.bathrooms = float(payload["bathrooms"] or getattr(prop, "bathrooms", 1) or 1)
                prop.square_feet = payload.get("square_feet") or getattr(prop, "square_feet", None)
                prop.year_built = payload.get("year_built") or getattr(prop, "year_built", None)
                prop.property_type = payload.get("property_type") or getattr(prop, "property_type", None)
                db.add(prop)
                db.flush()

                deal, deal_created = _upsert_deal(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                    payload=payload,
                )

                _upsert_rent_assumption(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                    payload=payload,
                )

                photo_count = _upsert_photos(
                    db,
                    org_id=org_id,
                    property_id=int(prop.id),
                    provider=source.provider,
                    photos=payload.get("photos") or [],
                )

                upsert_record_link(
                    db,
                    org_id=org_id,
                    provider=source.provider,
                    source_id=source.id,
                    external_record_id=ext_id,
                    external_url=payload.get("external_url"),
                    property_id=int(prop.id),
                    deal_id=int(deal.id) if deal else None,
                    raw_json=payload.get("raw") or raw_row,
                    fingerprint=fingerprint,
                )

                summary["records_imported"] += 1
                summary["properties_updated"] += 1
                summary["deals_created"] += 1 if deal_created else 0
                summary["deals_updated"] += 0 if deal_created else 1
                summary["rent_rows_upserted"] += 1
                summary["photos_upserted"] += photo_count

                pipeline_res = _run_post_import_pipeline(
                    db,
                    org_id=int(org_id),
                    property_id=int(prop.id),
                )
                _apply_pipeline_summary(summary, pipeline_res, int(prop.id))
                continue

            prop, prop_created = _upsert_property(db, org_id=org_id, payload=payload)
            deal, deal_created = _upsert_deal(
                db,
                org_id=org_id,
                property_id=prop.id,
                payload=payload,
            )

            _upsert_rent_assumption(
                db,
                org_id=org_id,
                property_id=prop.id,
                payload=payload,
            )

            photo_count = _upsert_photos(
                db,
                org_id=org_id,
                property_id=prop.id,
                provider=source.provider,
                photos=payload.get("photos") or [],
            )

            upsert_record_link(
                db,
                org_id=org_id,
                provider=source.provider,
                source_id=source.id,
                external_record_id=ext_id,
                external_url=payload.get("external_url"),
                property_id=prop.id,
                deal_id=deal.id if deal else None,
                raw_json=payload.get("raw") or raw_row,
                fingerprint=fingerprint,
            )

            summary["records_imported"] += 1
            summary["properties_created"] += 1 if prop_created else 0
            summary["properties_updated"] += 0 if prop_created else 1
            summary["deals_created"] += 1 if deal_created else 0
            summary["deals_updated"] += 0 if deal_created else 1
            summary["rent_rows_upserted"] += 1
            summary["photos_upserted"] += photo_count

            pipeline_res = _run_post_import_pipeline(
                db,
                org_id=int(org_id),
                property_id=int(prop.id),
            )
            _apply_pipeline_summary(summary, pipeline_res, int(prop.id))

        db.commit()
        return finish_run(db, row=run, status="success", summary=summary)

    except Exception as e:
        db.rollback()
        return finish_run(
            db,
            row=run,
            status="failed",
            summary=summary,
            error_summary=str(e),
            error_json={"type": type(e).__name__},
        )
    