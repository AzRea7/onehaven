from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal
from products.intelligence.backend.src.services.rentcast_listing_source import RentCastListingSource

BATCH_SIZE = 50


def clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def to_int(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def to_float(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except Exception:
        return None


def to_dict(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def build_merged_metadata(
    *,
    existing_meta: dict[str, Any],
    property_row: dict[str, Any],
    match: dict[str, Any],
) -> dict[str, Any]:
    existing_raw = existing_meta.get("raw")
    if not isinstance(existing_raw, dict):
        existing_raw = {}

    merged_raw = {
        **existing_raw,
        "source": "rentcast",
        "address": property_row.get("address"),
        "city": property_row.get("city"),
        "state": property_row.get("state"),
        "zip": property_row.get("zip"),
        "external_record_id": clean(match.get("id")) or property_row.get("acquisition_source_record_id"),
        "raw_json": match,
    }

    return {
        **existing_meta,
        "raw": merged_raw,
        "repair_type": "exact_address_lookup_backfill",
    }


def main() -> None:
    db = SessionLocal()
    source = RentCastListingSource()

    try:
        rows = db.execute(
            text(
                """
                SELECT
                    id,
                    org_id,
                    address,
                    city,
                    state,
                    zip,
                    acquisition_source_record_id,
                    acquisition_metadata_json
                FROM properties
                WHERE acquisition_source_provider = 'rentcast'
                  AND (
                    listing_status IS NULL
                    OR listing_listed_at IS NULL
                    OR listing_mls_number IS NULL
                    OR listing_agent_name IS NULL
                    OR listing_office_name IS NULL
                  )
                ORDER BY id
                LIMIT :batch_size
                """
            ),
            {"batch_size": BATCH_SIZE},
        ).fetchall()

        if not rows:
            print("done: repaired=0 skipped=0 (no rentcast properties currently need repair)")
            return

        repaired = 0
        skipped = 0

        print(f"selected {len(rows)} properties for repair (batch size {BATCH_SIZE})")

        for row_obj in rows:
            row = dict(row_obj._mapping)

            try:
                match = source.lookup_exact_address(
                    credentials={},
                    address=row["address"],
                    city=row.get("city"),
                    state=row.get("state"),
                    zip_code=row.get("zip"),
                    limit=10,
                    status="Active",
                    allow_status_fallback=True,
                    allow_location_fallback=True,
                )
            except Exception as exc:
                print(f"lookup failed for property {row['id']}: {exc}")
                db.rollback()
                skipped += 1
                continue

            if not match:
                print(f"no exact-address match for property {row['id']} {row['address']}")
                skipped += 1
                continue

            listing_agent = match.get("listingAgent") if isinstance(match.get("listingAgent"), dict) else {}
            listing_office = match.get("listingOffice") if isinstance(match.get("listingOffice"), dict) else {}

            existing_meta = to_dict(row.get("acquisition_metadata_json"))
            merged_meta = build_merged_metadata(
                existing_meta=existing_meta,
                property_row=row,
                match=match,
            )

            try:
                db.execute(
                    text(
                        """
                        UPDATE properties
                        SET
                          acquisition_metadata_json = CAST(:metadata_json AS jsonb),
                          listing_status = COALESCE(:listing_status, listing_status),
                          listing_last_seen_at = COALESCE(NULLIF(:listing_last_seen_at, '')::timestamp, listing_last_seen_at),
                          listing_removed_at = COALESCE(NULLIF(:listing_removed_at, '')::timestamp, listing_removed_at),
                          listing_listed_at = COALESCE(NULLIF(:listing_listed_at, '')::timestamp, listing_listed_at),
                          listing_created_at = COALESCE(NULLIF(:listing_created_at, '')::timestamp, listing_created_at),
                          listing_days_on_market = COALESCE(:listing_days_on_market, listing_days_on_market),
                          listing_price = COALESCE(:listing_price, listing_price),
                          listing_mls_name = COALESCE(:listing_mls_name, listing_mls_name),
                          listing_mls_number = COALESCE(:listing_mls_number, listing_mls_number),
                          listing_type = COALESCE(:listing_type, listing_type),
                          listing_agent_name = COALESCE(:listing_agent_name, listing_agent_name),
                          listing_agent_phone = COALESCE(:listing_agent_phone, listing_agent_phone),
                          listing_agent_email = COALESCE(:listing_agent_email, listing_agent_email),
                          listing_office_name = COALESCE(:listing_office_name, listing_office_name),
                          listing_office_phone = COALESCE(:listing_office_phone, listing_office_phone),
                          listing_office_email = COALESCE(:listing_office_email, listing_office_email),
                          updated_at = now()
                        WHERE id = :property_id
                          AND org_id = :org_id
                        """
                    ),
                    {
                        "property_id": int(row["id"]),
                        "org_id": int(row["org_id"]),
                        "metadata_json": json.dumps(merged_meta),
                        "listing_status": clean(match.get("status") or match.get("statusText")),
                        "listing_last_seen_at": clean(match.get("lastSeenDate") or match.get("lastSeen")),
                        "listing_removed_at": clean(match.get("removedDate")),
                        "listing_listed_at": clean(match.get("listedDate")),
                        "listing_created_at": clean(match.get("createdDate")),
                        "listing_days_on_market": to_int(match.get("daysOnMarket")),
                        "listing_price": to_float(match.get("price")),
                        "listing_mls_name": clean(match.get("mlsName")),
                        "listing_mls_number": clean(match.get("mlsNumber")),
                        "listing_type": clean(match.get("listingType")),
                        "listing_agent_name": clean(listing_agent.get("name")),
                        "listing_agent_phone": clean(listing_agent.get("phone")),
                        "listing_agent_email": clean(listing_agent.get("email")),
                        "listing_office_name": clean(listing_office.get("name")),
                        "listing_office_phone": clean(listing_office.get("phone")),
                        "listing_office_email": clean(listing_office.get("email")),
                    },
                )
                repaired += 1
                print(f"repaired property {row['id']} {row['address']}")
            except Exception as exc:
                db.rollback()
                print(f"update failed for property {row['id']}: {exc}")
                skipped += 1
                continue

        db.commit()
        print(f"done: repaired={repaired} skipped={skipped}")
        print("rerun the script to process the next batch of remaining broken properties")
    finally:
        db.close()


if __name__ == "__main__":
    main()
