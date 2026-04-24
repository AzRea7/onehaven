from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import Property, PropertyPhoto
from onehaven_platform.backend.src.adapters.intelligence_adapter import classify_photo_kind


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


def summarize_property_photo_inventory(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    ensure_property_exists(db, org_id=org_id, property_id=property_id)
    rows = list_property_photos(db, org_id=org_id, property_id=property_id)
    by_kind: Counter[str] = Counter()
    by_source: Counter[str] = Counter()

    for row in rows:
        by_kind[str(getattr(row, "kind", None) or "unknown").strip().lower() or "unknown"] += 1
        by_source[str(getattr(row, "source", None) or "unknown").strip().lower() or "unknown"] += 1

    return {
        "property_id": int(property_id),
        "count": len(rows),
        "by_kind": dict(sorted(by_kind.items())),
        "by_source": dict(sorted(by_source.items())),
        "has_interior": by_kind.get("interior", 0) > 0,
        "has_exterior": by_kind.get("exterior", 0) > 0,
        "rows": [serialize_property_photo(row) for row in rows],
    }


def serialize_property_photo(row: PropertyPhoto) -> dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "property_id": int(getattr(row, "property_id", 0) or 0),
        "source": getattr(row, "source", None),
        "kind": getattr(row, "kind", None),
        "label": getattr(row, "label", None),
        "url": getattr(row, "url", None),
        "storage_key": getattr(row, "storage_key", None),
        "content_type": getattr(row, "content_type", None),
        "sort_order": getattr(row, "sort_order", None),
        "created_at": getattr(row, "created_at", None),
        "updated_at": getattr(row, "updated_at", None),
    }


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

    return {
        "created": created,
        "existing": existing,
        "total": len(list_property_photos(db, org_id=org_id, property_id=property_id)),
    }


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
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
) -> PropertyPhoto:
    ensure_property_exists(db, org_id=org_id, property_id=property_id)

    current_count = len(list_property_photos(db, org_id=org_id, property_id=property_id))

    extra_bits: list[str] = []
    if inspection_id is not None:
        extra_bits.append(f"inspection:{int(inspection_id)}")
    if checklist_item_id is not None:
        extra_bits.append(f"checklist_item:{int(checklist_item_id)}")

    final_label = label
    if extra_bits:
        suffix = " | ".join(extra_bits)
        final_label = f"{label} [{suffix}]" if label else suffix

    row = PropertyPhoto(
        org_id=org_id,
        property_id=property_id,
        source="upload",
        kind=kind or "unknown",
        label=final_label,
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
