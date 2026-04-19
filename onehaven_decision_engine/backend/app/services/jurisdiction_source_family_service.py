from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import MetaData, Table, and_, func, select, update
from sqlalchemy.orm import Session


SOURCE_STATUS_ACTIVE = "active"
SOURCE_STATUS_BLOCKED = "blocked"
SOURCE_STATUS_MANUAL = "manual"
SOURCE_STATUS_INACTIVE = "inactive"

FETCH_MODE_API = "api"
FETCH_MODE_HTML = "html"
FETCH_MODE_PDF = "pdf"
FETCH_MODE_RENDER = "render"
FETCH_MODE_MANUAL = "manual"

CATEGORY_RENTAL_REGISTRATION = "rental_registration"
CATEGORY_RENTAL_INSPECTION = "rental_inspection"
CATEGORY_CERTIFICATE_OF_OCCUPANCY = "certificate_of_occupancy"
CATEGORY_PERMITS_BUILDING = "permits_building"
CATEGORY_FEES_FORMS = "fees_forms"
CATEGORY_LOCAL_CODE = "local_code"
CATEGORY_PROGRAM_OVERLAY = "program_overlay"
CATEGORY_CONTACT = "contact"


@dataclass(frozen=True)
class JurisdictionSourceFamilyRecord:
    id: int
    jurisdiction_id: int
    category: str
    source_label: str | None
    source_url: str | None
    source_kind: str | None
    publisher_name: str | None
    publisher_type: str | None
    authority_level: str | None
    fetch_mode: str | None
    status: str
    is_official: bool
    is_active: bool
    notes: str | None
    coverage_hint: str | None
    review_state: str | None
    last_checked_at: datetime | None
    last_reviewed_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _table(db: Session) -> Table:
    metadata = MetaData()
    return Table("jurisdiction_source_families", metadata, autoload_with=db.bind)


def _row_to_record(row: Any) -> JurisdictionSourceFamilyRecord:
    data = dict(row._mapping)
    return JurisdictionSourceFamilyRecord(
        id=int(data["id"]),
        jurisdiction_id=int(data["jurisdiction_id"]),
        category=str(data.get("category") or ""),
        source_label=data.get("source_label"),
        source_url=data.get("source_url"),
        source_kind=data.get("source_kind"),
        publisher_name=data.get("publisher_name"),
        publisher_type=data.get("publisher_type"),
        authority_level=data.get("authority_level"),
        fetch_mode=data.get("fetch_mode"),
        status=str(data.get("status") or SOURCE_STATUS_ACTIVE),
        is_official=bool(data.get("is_official", False)),
        is_active=bool(data.get("is_active", True)),
        notes=data.get("notes"),
        coverage_hint=data.get("coverage_hint"),
        review_state=data.get("review_state"),
        last_checked_at=data.get("last_checked_at"),
        last_reviewed_at=data.get("last_reviewed_at"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
    )


def get_source_family_by_id(db: Session, *, source_family_id: int) -> JurisdictionSourceFamilyRecord | None:
    table = _table(db)
    row = db.execute(select(table).where(table.c.id == int(source_family_id)).limit(1)).first()
    return _row_to_record(row) if row else None


def get_source_families_for_jurisdiction(
    db: Session,
    *,
    jurisdiction_id: int,
    include_inactive: bool = False,
) -> list[JurisdictionSourceFamilyRecord]:
    table = _table(db)
    conditions = [table.c.jurisdiction_id == int(jurisdiction_id)]
    if not include_inactive:
        conditions.append(table.c.is_active.is_(True))
    rows = db.execute(
        select(table)
        .where(and_(*conditions))
        .order_by(func.lower(table.c.category), func.lower(table.c.source_label))
    ).all()
    return [_row_to_record(row) for row in rows]


def get_active_source_family_for_category(
    db: Session,
    *,
    jurisdiction_id: int,
    category: str,
) -> JurisdictionSourceFamilyRecord | None:
    table = _table(db)
    row = db.execute(
        select(table)
        .where(
            and_(
                table.c.jurisdiction_id == int(jurisdiction_id),
                table.c.category == (_norm_lower(category) or ""),
                table.c.is_active.is_(True),
            )
        )
        .order_by(table.c.is_official.desc(), table.c.updated_at.desc().nullslast())
        .limit(1)
    ).first()
    return _row_to_record(row) if row else None


def create_source_family(
    db: Session,
    *,
    jurisdiction_id: int,
    category: str,
    source_label: str | None = None,
    source_url: str | None = None,
    source_kind: str | None = None,
    publisher_name: str | None = None,
    publisher_type: str | None = None,
    authority_level: str | None = None,
    fetch_mode: str | None = None,
    status: str = SOURCE_STATUS_ACTIVE,
    is_official: bool = False,
    is_active: bool = True,
    notes: str | None = None,
    coverage_hint: str | None = None,
    review_state: str | None = None,
    last_checked_at: datetime | None = None,
    last_reviewed_at: datetime | None = None,
) -> JurisdictionSourceFamilyRecord:
    table = _table(db)
    payload = {
        "jurisdiction_id": int(jurisdiction_id),
        "category": _norm_lower(category),
        "source_label": _norm_text(source_label),
        "source_url": _norm_text(source_url),
        "source_kind": _norm_lower(source_kind),
        "publisher_name": _norm_text(publisher_name),
        "publisher_type": _norm_lower(publisher_type),
        "authority_level": _norm_lower(authority_level),
        "fetch_mode": _norm_lower(fetch_mode),
        "status": _norm_lower(status) or SOURCE_STATUS_ACTIVE,
        "is_official": bool(is_official),
        "is_active": bool(is_active),
        "notes": _norm_text(notes),
        "coverage_hint": _norm_text(coverage_hint),
        "review_state": _norm_lower(review_state),
        "last_checked_at": last_checked_at,
        "last_reviewed_at": last_reviewed_at,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    inserted = db.execute(table.insert().values(**payload))
    db.flush()
    return get_source_family_by_id(db, source_family_id=int(inserted.inserted_primary_key[0]))  # type: ignore[index]


def upsert_source_family(
    db: Session,
    *,
    jurisdiction_id: int,
    category: str,
    source_url: str | None = None,
    source_label: str | None = None,
    source_kind: str | None = None,
    publisher_name: str | None = None,
    publisher_type: str | None = None,
    authority_level: str | None = None,
    fetch_mode: str | None = None,
    status: str = SOURCE_STATUS_ACTIVE,
    is_official: bool = False,
    is_active: bool = True,
    notes: str | None = None,
    coverage_hint: str | None = None,
    review_state: str | None = None,
    last_checked_at: datetime | None = None,
    last_reviewed_at: datetime | None = None,
) -> JurisdictionSourceFamilyRecord:
    table = _table(db)
    category_norm = _norm_lower(category) or ""
    source_url_norm = _norm_text(source_url)

    row = db.execute(
        select(table)
        .where(
            and_(
                table.c.jurisdiction_id == int(jurisdiction_id),
                table.c.category == category_norm,
                table.c.source_url == source_url_norm,
            )
        )
        .limit(1)
    ).first()

    if row is None:
        return create_source_family(
            db,
            jurisdiction_id=jurisdiction_id,
            category=category_norm,
            source_label=source_label,
            source_url=source_url_norm,
            source_kind=source_kind,
            publisher_name=publisher_name,
            publisher_type=publisher_type,
            authority_level=authority_level,
            fetch_mode=fetch_mode,
            status=status,
            is_official=is_official,
            is_active=is_active,
            notes=notes,
            coverage_hint=coverage_hint,
            review_state=review_state,
            last_checked_at=last_checked_at,
            last_reviewed_at=last_reviewed_at,
        )

    existing = _row_to_record(row)
    db.execute(
        update(table)
        .where(table.c.id == existing.id)
        .values(
            source_label=_norm_text(source_label),
            source_kind=_norm_lower(source_kind),
            publisher_name=_norm_text(publisher_name),
            publisher_type=_norm_lower(publisher_type),
            authority_level=_norm_lower(authority_level),
            fetch_mode=_norm_lower(fetch_mode),
            status=_norm_lower(status) or SOURCE_STATUS_ACTIVE,
            is_official=bool(is_official),
            is_active=bool(is_active),
            notes=_norm_text(notes),
            coverage_hint=_norm_text(coverage_hint),
            review_state=_norm_lower(review_state),
            last_checked_at=last_checked_at,
            last_reviewed_at=last_reviewed_at,
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return get_source_family_by_id(db, source_family_id=existing.id)  # type: ignore[return-value]


def mark_source_family_blocked(
    db: Session,
    *,
    source_family_id: int,
    notes: str | None = None,
) -> JurisdictionSourceFamilyRecord | None:
    table = _table(db)
    db.execute(
        update(table)
        .where(table.c.id == int(source_family_id))
        .values(
            status=SOURCE_STATUS_BLOCKED,
            fetch_mode=FETCH_MODE_MANUAL,
            notes=_norm_text(notes),
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return get_source_family_by_id(db, source_family_id=source_family_id)


def mark_source_family_manual_required(
    db: Session,
    *,
    source_family_id: int,
    notes: str | None = None,
) -> JurisdictionSourceFamilyRecord | None:
    table = _table(db)
    db.execute(
        update(table)
        .where(table.c.id == int(source_family_id))
        .values(
            status=SOURCE_STATUS_MANUAL,
            fetch_mode=FETCH_MODE_MANUAL,
            notes=_norm_text(notes),
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return get_source_family_by_id(db, source_family_id=source_family_id)


def mark_source_family_reviewed(
    db: Session,
    *,
    source_family_id: int,
    review_state: str = "reviewed",
    notes: str | None = None,
    last_reviewed_at: datetime | None = None,
) -> JurisdictionSourceFamilyRecord | None:
    table = _table(db)
    db.execute(
        update(table)
        .where(table.c.id == int(source_family_id))
        .values(
            review_state=_norm_lower(review_state),
            notes=_norm_text(notes),
            last_reviewed_at=last_reviewed_at or datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return get_source_family_by_id(db, source_family_id=source_family_id)