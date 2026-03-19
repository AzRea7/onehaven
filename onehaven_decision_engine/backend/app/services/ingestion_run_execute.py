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
from ..services.ingestion_enrichment_service import canonical_listing_payload, derive_photo_kind
from ..services.ingestion_run_service import finish_run, start_run
from ..services.rentcast_listing_source import RentCastListingSource


PROVIDER_ADAPTERS = {
    "rentcast": RentCastListingSource(),
}


def _norm_text(value: Any) -> str:
    return str(value or "").strip().lower()


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

    # keep manual intake simple and avoid over-filtering
    limit = _safe_int(payload.get("limit")) or 100
    payload["limit"] = max(1, limit)

    for key in ["state", "county", "city", "property_type"]:
        if key in payload and payload[key] is not None:
            payload[key] = str(payload[key]).strip()

    for key in ["min_price", "max_price", "min_bedrooms", "min_bathrooms"]:
        payload[key] = _safe_float(payload.get(key))

    # ZIP and address intentionally removed from manual intake flow
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
    condo_aliases = {
        "condo",
        "condominium",
    }
    townhouse_aliases = {
        "townhouse",
        "townhome",
        "town_house",
        "rowhouse",
        "row_home",
    }

    if cleaned in single_family_aliases:
        return "single_family"
    if cleaned in multi_family_aliases:
        return "multi_family"
    if cleaned in condo_aliases:
        return "condo"
    if cleaned in townhouse_aliases:
        return "townhouse"

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
        existing.bedrooms = int(payload["bedrooms"] or existing.bedrooms or 0)
        existing.bathrooms = float(payload["bathrooms"] or existing.bathrooms or 1)
        existing.square_feet = payload.get("square_feet") or existing.square_feet
        existing.year_built = payload.get("year_built") or existing.year_built
        existing.property_type = payload.get("property_type") or existing.property_type
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_deal(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    snapshot_id: int | None,
    payload: dict[str, Any],
):
    existing = db.scalar(
        select(Deal)
        .where(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
        .order_by(desc(Deal.id))
    )
    created = False

    if existing is None:
        existing = Deal(
            org_id=int(org_id),
            property_id=int(property_id),
            snapshot_id=snapshot_id,
            asking_price=float(payload.get("asking_price") or 0),
            estimated_purchase_price=payload.get("estimated_purchase_price"),
            rehab_estimate=float(payload.get("rehab_estimate") or 0),
            source=payload.get("source", "ingestion"),
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
        db.add(existing)
        db.flush()

    return existing, created


def _upsert_rent_assumption(db: Session, *, org_id: int, property_id: int, payload: dict[str, Any]):
    existing = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.org_id == int(org_id), RentAssumption.property_id == int(property_id))
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
        if payload.get("market_rent_estimate") is not None:
            existing.market_rent_estimate = payload.get("market_rent_estimate")
        if payload.get("section8_fmr") is not None:
            existing.section8_fmr = payload.get("section8_fmr")
        if payload.get("approved_rent_ceiling") is not None:
            existing.approved_rent_ceiling = payload.get("approved_rent_ceiling")
        if payload.get("inventory_count") is not None:
            existing.inventory_count = payload.get("inventory_count")
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
            existing = PropertyPhoto(
                org_id=int(org_id),
                property_id=int(property_id),
                url=url,
                kind=kind or "unknown",
                source=provider,
                label=None,
            )
            db.add(existing)
            count += 1

    db.flush()
    return count


def _starting_cursor(source, trigger_type: str) -> dict[str, Any]:
    if trigger_type in {"manual", "daily_refresh"}:
        return {"page": 1}
    return dict(source.cursor_json or {})


def _load_rows(
    source,
    trigger_type: str,
    runtime_config: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base_config = dict(source.config_json or {})
    merged_config = {**base_config, **(runtime_config or {})}

    sample_rows = merged_config.get("sample_rows")
    if isinstance(sample_rows, list):
        rows = [x for x in sample_rows if isinstance(x, dict)]
        return rows, {"page": 1}

    adapter = PROVIDER_ADAPTERS.get(source.provider)
    if adapter is None:
        raise ValueError(f"No adapter registered for provider={source.provider}")

    fetched = adapter.fetch_incremental(
        credentials=source.credentials_json or {},
        config=merged_config,
        cursor=_starting_cursor(source, trigger_type),
    )
    return fetched.get("rows") or [], fetched.get("next_cursor") or {"page": 1}


def _is_valid_payload(payload: dict[str, Any]) -> bool:
    return bool(
        payload.get("external_record_id")
        and payload.get("address")
        and payload.get("city")
        and payload.get("state")
        and payload.get("zip")
    )


def _matches_runtime_filters(payload: dict[str, Any], runtime_config: dict[str, Any]) -> bool:
    if not runtime_config:
        return True

    state = str(runtime_config.get("state") or "").strip()
    county = str(runtime_config.get("county") or "").strip()
    city = str(runtime_config.get("city") or "").strip()
    property_type = str(runtime_config.get("property_type") or "").strip()

    if state and _norm_text(payload.get("state")) != _norm_text(state):
        return False

    # county is optional because not every listing row will normalize to county
    payload_county = _norm_text(payload.get("county"))
    if county and payload_county and payload_county != _norm_text(county):
        return False

    if city and _norm_text(payload.get("city")) != _norm_text(city):
        return False

    asking_price = _safe_float(payload.get("asking_price")) or 0.0
    min_price = runtime_config.get("min_price")
    max_price = runtime_config.get("max_price")
    if min_price is not None and asking_price < float(min_price):
        return False
    if max_price is not None and asking_price > float(max_price):
        return False

    bedrooms = _safe_float(payload.get("bedrooms")) or 0.0
    bathrooms = _safe_float(payload.get("bathrooms")) or 0.0
    min_bedrooms = runtime_config.get("min_bedrooms")
    min_bathrooms = runtime_config.get("min_bathrooms")

    if min_bedrooms is not None and bedrooms < float(min_bedrooms):
        return False
    if min_bathrooms is not None and bathrooms < float(min_bathrooms):
        return False

    if property_type:
        requested_type = _normalize_property_type(property_type)
        actual_type = _normalize_property_type(payload.get("property_type"))
        if requested_type and actual_type and requested_type != actual_type:
            return False

    return True


def _run_post_import_hooks(db: Session, *, org_id: int, property_id: int, deal_id: int | None) -> None:
    """
    Tries to advance the imported property automatically through the early
    pipeline so users do not need to manually click enrich/evaluate actions.
    This is intentionally best-effort and does not fail the ingestion run.
    """
    # geo / enrichment hooks
    try:
        from ..services.geo_enrichment import enrich_property_geo

        enrich_property_geo(db, property_id=int(property_id))
    except Exception:
        pass

    # property state / stage hooks
    try:
        from ..services.property_state_machine import compute_and_persist_stage

        compute_and_persist_stage(db, int(property_id), int(org_id))
    except Exception:
        pass

    # underwriting hooks when a service exists and can operate from a deal id
    if deal_id is not None:
        try:
            from ..services.underwriting_service import run_underwriting_for_deal

            run_underwriting_for_deal(db, org_id=int(org_id), deal_id=int(deal_id))
        except Exception:
            pass


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
        "post_import_hooks_attempted": 0,
    }

    try:
        rows, next_cursor = _load_rows(source, trigger_type, normalized_runtime)

        if trigger_type != "manual":
            source.cursor_json = next_cursor or {"page": 1}
            db.add(source)
            db.commit()
            db.refresh(source)

        matched_rows: list[dict[str, Any]] = []
        seen_external_ids: set[str] = set()
        seen_fingerprints: set[str] = set()

        for raw_row in rows:
            summary["records_seen"] += 1
            payload = canonical_listing_payload(raw_row)

            if not _matches_runtime_filters(payload, normalized_runtime):
                summary["filtered_out"] += 1
                continue

            if not _is_valid_payload(payload):
                summary["invalid_rows"] += 1
                continue

            ext_id = str(payload["external_record_id"])
            fingerprint = build_property_fingerprint(
                address=payload["address"],
                city=payload["city"],
                state=payload["state"],
                zip_code=payload["zip"],
            )

            if ext_id in seen_external_ids or fingerprint in seen_fingerprints:
                summary["duplicates_skipped"] += 1
                continue

            seen_external_ids.add(ext_id)
            seen_fingerprints.add(fingerprint)
            matched_rows.append(raw_row)

        summary["matched_before_limit"] = len(matched_rows)
        capped_rows = matched_rows[: int(normalized_runtime.get("limit") or len(matched_rows) or 100)]

        for raw_row in capped_rows:
            payload = canonical_listing_payload(raw_row)
            payload["source"] = source.provider

            ext_id = payload["external_record_id"]
            existing_link = find_existing_by_external_id(
                db,
                org_id=org_id,
                provider=source.provider,
                external_record_id=ext_id,
            )

            if existing_link is not None:
                summary["duplicates_skipped"] += 1
                continue

            fingerprint = build_property_fingerprint(
                address=payload["address"],
                city=payload["city"],
                state=payload["state"],
                zip_code=payload["zip"],
            )

            prop, prop_created = _upsert_property(db, org_id=org_id, payload=payload)
            deal, deal_created = _upsert_deal(
                db,
                org_id=org_id,
                property_id=prop.id,
                snapshot_id=None,
                payload=payload,
            )
            _rent, _ = _upsert_rent_assumption(
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

            _run_post_import_hooks(
                db,
                org_id=int(org_id),
                property_id=int(prop.id),
                deal_id=int(deal.id) if deal else None,
            )
            summary["post_import_hooks_attempted"] += 1

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
    