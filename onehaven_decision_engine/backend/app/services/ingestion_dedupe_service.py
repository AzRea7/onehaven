from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import IngestionRecordLink, Property


_ADDR_ABBREV = {
    "street": "st",
    "st.": "st",
    "avenue": "ave",
    "ave.": "ave",
    "road": "rd",
    "rd.": "rd",
    "drive": "dr",
    "dr.": "dr",
    "boulevard": "blvd",
    "lane": "ln",
    "court": "ct",
    "place": "pl",
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
}


def normalize_address(address: Optional[str]) -> str:
    raw = str(address or "").strip().lower()
    raw = raw.replace(",", " ")
    raw = re.sub(r"\s+", " ", raw)
    parts = []
    for token in raw.split(" "):
        if not token:
            continue
        parts.append(_ADDR_ABBREV.get(token, token))
    return " ".join(parts).strip()


def normalize_zip(zip_code: Optional[str]) -> str:
    raw = re.sub(r"[^0-9]", "", str(zip_code or ""))
    return raw[:5]


def build_property_fingerprint(*, address: str, city: str, state: str, zip_code: str) -> str:
    raw = "|".join(
        [
            normalize_address(address),
            str(city or "").strip().lower(),
            str(state or "").strip().lower(),
            normalize_zip(zip_code),
        ]
    )
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
    normalized = normalize_address(address)
    normalized_zip = normalize_zip(zip_code)

    rows = db.scalars(
        select(Property).where(
            Property.org_id == int(org_id),
            func.lower(Property.city) == str(city or "").strip().lower(),
            func.lower(Property.state) == str(state or "").strip().lower(),
        )
    ).all()

    for row in rows:
        row_addr = normalize_address(getattr(row, "address", None))
        row_zip = normalize_zip(getattr(row, "zip", None))
        if row_addr == normalized and row_zip == normalized_zip:
            return row
    return None


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


def find_existing_by_fingerprint(
    db: Session,
    *,
    org_id: int,
    fingerprint: str,
) -> Optional[IngestionRecordLink]:
    return db.scalar(
        select(IngestionRecordLink).where(
            IngestionRecordLink.org_id == int(org_id),
            IngestionRecordLink.fingerprint == str(fingerprint),
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
    if row is None and fingerprint:
        row = find_existing_by_fingerprint(db, org_id=org_id, fingerprint=fingerprint)

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

    row.provider = str(provider)
    row.source_id = source_id
    row.external_record_id = str(external_record_id)
    row.external_url = external_url
    row.property_id = property_id
    row.deal_id = deal_id
    row.raw_json = raw_json or row.raw_json
    row.fingerprint = fingerprint or row.fingerprint
    row.last_seen_at = datetime.utcnow()
    db.add(row)
    db.flush()
    return row