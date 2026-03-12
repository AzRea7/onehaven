# backend/app/services/property_photo_service.py
from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property, PropertyPhoto
from app.services.zillow_photo_source import classify_photo_kind


def _now() -> datetime:
    return datetime.utcnow()


def list_property_photos(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> list[PropertyPhoto]:
    return list(
        db.scalars(
            select(PropertyPhoto)
            .where(
                PropertyPhoto.org_id == org_id,
                PropertyPhoto.property_id == property_id,
            )
            .order_by(
                PropertyPhoto.sort_order.asc(),
                PropertyPhoto.id.asc(),
            )
        ).all()
    )


def ensure_property_exists(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.id == property_id,
            Property.org_id == org_id,
        )
    )
    if not prop:
        raise ValueError("property not found")
    return prop


def upsert_zillow_photos(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    urls: Iterable[str],
) -> dict:
    ensure_property_exists(db, org_id=org_id, property_id=property_id)

    clean_urls = [(u or "").strip() for u in urls if (u or "").strip()]
    if not clean_urls:
        return {"created": 0, "existing": 0, "total": 0}

    existing_rows = list(
        db.scalars(
            select(PropertyPhoto).where(
                PropertyPhoto.org_id == org_id,
                PropertyPhoto.property_id == property_id,
                PropertyPhoto.source == "zillow",
            )
        ).all()
    )
    existing_by_url = {str(r.url): r for r in existing_rows}

    created = 0
    existing = 0

    for idx, url in enumerate(clean_urls):
        row = existing_by_url.get(url)
        if row:
            existing += 1
            if row.sort_order is None:
                row.sort_order = idx
                row.updated_at = _now()
                db.add(row)
            continue

        db.add(
            PropertyPhoto(
                org_id=org_id,
                property_id=property_id,
                source="zillow",
                kind=classify_photo_kind(url),
                label=None,
                url=url,
                storage_key=None,
                content_type=None,
                sort_order=idx,
                created_at=_now(),
                updated_at=_now(),
            )
        )
        created += 1

    db.commit()

    total = db.scalar(
        select(PropertyPhoto)
        .where(
            PropertyPhoto.org_id == org_id,
            PropertyPhoto.property_id == property_id,
        )
    )
    return {"created": created, "existing": existing, "total": len(list_property_photos(db, org_id=org_id, property_id=property_id))}


def create_uploaded_photo(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    url: str,
    storage_key: str | None,
    kind: str = "unknown",
    label: str | None = None,
    content_type: str | None = None,
) -> PropertyPhoto:
    ensure_property_exists(db, org_id=org_id, property_id=property_id)

    current_count = len(list_property_photos(db, org_id=org_id, property_id=property_id))

    row = PropertyPhoto(
        org_id=org_id,
        property_id=property_id,
        source="upload",
        kind=kind or "unknown",
        label=label,
        url=url,
        storage_key=storage_key,
        content_type=content_type,
        sort_order=current_count,
        created_at=_now(),
        updated_at=_now(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
