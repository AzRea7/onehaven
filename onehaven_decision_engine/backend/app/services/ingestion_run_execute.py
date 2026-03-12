from __future__ import annotations

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


def _upsert_deal(db: Session, *, org_id: int, property_id: int, snapshot_id: int | None, payload: dict[str, Any]):
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


def _load_rows(source) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = source.config_json or {}

    # Local/test/webhook override path. This makes the ingestion pipeline fully testable
    # without requiring a live upstream API.
    sample_rows = config.get("sample_rows")
    if isinstance(sample_rows, list):
        rows = [x for x in sample_rows if isinstance(x, dict)]
        next_cursor = source.cursor_json or {}
        return rows, next_cursor

    adapter = PROVIDER_ADAPTERS.get(source.provider)
    if adapter is None:
        raise ValueError(f"No adapter registered for provider={source.provider}")

    fetched = adapter.fetch_incremental(
        credentials=source.credentials_json or {},
        config=config,
        cursor=source.cursor_json or {},
    )
    return fetched.get("rows") or [], fetched.get("next_cursor") or {}


def _is_valid_payload(payload: dict[str, Any]) -> bool:
    return bool(
        payload.get("external_record_id")
        and payload.get("address")
        and payload.get("city")
        and payload.get("state")
        and payload.get("zip")
    )


def execute_source_sync(db: Session, *, org_id: int, source, trigger_type: str = "manual"):
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
    }

    try:
        rows, next_cursor = _load_rows(source)

        source.cursor_json = next_cursor or {}
        db.add(source)
        db.commit()
        db.refresh(source)

        for raw_row in rows:
            summary["records_seen"] += 1

            payload = canonical_listing_payload(raw_row)
            payload["source"] = source.provider

            if not _is_valid_payload(payload):
                summary["invalid_rows"] += 1
                continue

            ext_id = payload["external_record_id"]

            existing_link = find_existing_by_external_id(
                db,
                org_id=org_id,
                provider=source.provider,
                external_record_id=ext_id,
            )

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

            if existing_link is not None:
                summary["duplicates_skipped"] += 1

            summary["records_imported"] += 1
            summary["properties_created"] += 1 if prop_created else 0
            summary["properties_updated"] += 0 if prop_created else 1
            summary["deals_created"] += 1 if deal_created else 0
            summary["deals_updated"] += 0 if deal_created else 1
            summary["rent_rows_upserted"] += 1
            summary["photos_upserted"] += photo_count

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
    