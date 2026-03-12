from __future__ import annotations

import hashlib
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import IngestionRecordLink, Property


def normalize_address(address: Optional[str]) -> str:
    return " ".join((address or "").strip().lower().replace(",", " ").split())


def build_property_fingerprint(*, address: str, city: str, state: str, zip_code: str) -> str:
    raw = f"{normalize_address(address)}|{(city or '').strip().lower()}|{(state or '').strip().lower()}|{(zip_code or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def find_existing_property(
    db: Session,
    *,
    org_id: int,
    address: str,
    city: str,
    state: str,
    zip_code: str,
) -> Optional[Property]:
    return db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.address == address,
            Property.city == city,
            Property.state == state,
            Property.zip == zip_code,
        )
    )


def find_existing_by_external_id(
    db: Session,
    *,
    org_id: int,
    provider: str,
    external_record_id: str,
) -> Optional[IngestionRecordLink]:
    return db.scalar(
        select(IngestionRecordLink).where(
            IngestionRecordLink.org_id == int(org_id),
            IngestionRecordLink.provider == str(provider),
            IngestionRecordLink.external_record_id == str(external_record_id),
        )
    )


def upsert_record_link(
    db: Session,
    *,
    org_id: int,
    provider: str,
    source_id: Optional[int],
    external_record_id: str,
    external_url: Optional[str],
    property_id: Optional[int],
    deal_id: Optional[int],
    raw_json: Optional[dict[str, Any]],
    fingerprint: Optional[str],
) -> IngestionRecordLink:
    row = find_existing_by_external_id(
        db,
        org_id=org_id,
        provider=provider,
        external_record_id=external_record_id,
    )
    if row is None:
        row = IngestionRecordLink(
            org_id=int(org_id),
            provider=str(provider),
            source_id=source_id,
            external_record_id=str(external_record_id),
            external_url=external_url,
            property_id=property_id,
            deal_id=deal_id,
            raw_json=raw_json or {},
            fingerprint=fingerprint,
        )
        db.add(row)
        db.flush()
        return row

    row.external_url = external_url
    row.property_id = property_id
    row.deal_id = deal_id
    row.raw_json = raw_json or row.raw_json
    row.fingerprint = fingerprint or row.fingerprint
    from datetime import datetime
    row.last_seen_at = datetime.utcnow()
    db.add(row)
    db.flush()
    return row
