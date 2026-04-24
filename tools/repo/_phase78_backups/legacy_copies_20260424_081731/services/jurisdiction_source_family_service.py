from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

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

CATEGORY_ALIASES: dict[str, str] = {
    "registration": CATEGORY_RENTAL_REGISTRATION,
    "rental_registration": CATEGORY_RENTAL_REGISTRATION,
    "rental_license": CATEGORY_RENTAL_REGISTRATION,
    "inspection": CATEGORY_RENTAL_INSPECTION,
    "rental_inspection": CATEGORY_RENTAL_INSPECTION,
    "occupancy": CATEGORY_CERTIFICATE_OF_OCCUPANCY,
    "certificate_of_occupancy": CATEGORY_CERTIFICATE_OF_OCCUPANCY,
    "certificate_of_compliance": CATEGORY_CERTIFICATE_OF_OCCUPANCY,
    "permits": CATEGORY_PERMITS_BUILDING,
    "permits_building": CATEGORY_PERMITS_BUILDING,
    "fees": CATEGORY_FEES_FORMS,
    "fees_forms": CATEGORY_FEES_FORMS,
    "documents": CATEGORY_FEES_FORMS,
    "local_code": CATEGORY_LOCAL_CODE,
    "safety": CATEGORY_LOCAL_CODE,
    "lead": CATEGORY_LOCAL_CODE,
    "program_overlay": CATEGORY_PROGRAM_OVERLAY,
    "section8": CATEGORY_PROGRAM_OVERLAY,
    "contact": CATEGORY_CONTACT,
    "contacts": CATEGORY_CONTACT,
}

AUTHORITY_RANKS: dict[str, int] = {
    "authoritative_official": 100,
    "approved_official_supporting": 85,
    "semi_authoritative_operational": 60,
    "derived_or_inferred": 25,
}


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


@dataclass(frozen=True)
class JurisdictionCoverageGapSummary:
    jurisdiction_id: int
    required_categories: list[str]
    covered_categories: list[str]
    missing_categories: list[str]
    official_categories: list[str]
    binding_categories: list[str]
    active_source_count: int
    category_map: dict[str, list[dict[str, Any]]]


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _normalize_category(value: Any) -> str | None:
    raw = _norm_lower(value)
    if not raw:
        return None
    raw = raw.replace("-", "_").replace(" ", "_").replace("/", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")
    return CATEGORY_ALIASES.get(raw, raw)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        norm = _normalize_category(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _host_from_url(url: Any) -> str:
    host = urlparse(str(url or "").strip()).netloc.strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    return host


def _looks_guessed_host(host: str) -> bool:
    host = str(host or "").strip().lower()
    if not host:
        return True
    return host.startswith("ci.") or host.startswith("co.") or ".ci." in host or ".co." in host


def _is_official_url(url: Any) -> bool:
    host = _host_from_url(url)
    if not host or _looks_guessed_host(host):
        return False
    return host.endswith(".gov") or host.endswith(".mi.us")


def _authority_rank(value: Any) -> int:
    raw = _norm_lower(value) or "derived_or_inferred"
    return int(AUTHORITY_RANKS.get(raw, 25))


def _is_binding_authority(value: Any) -> bool:
    return _authority_rank(value) >= AUTHORITY_RANKS["authoritative_official"]


def _is_category_covered(record: JurisdictionSourceFamilyRecord) -> bool:
    if not bool(record.is_active):
        return False
    if str(record.status or "").strip().lower() in {SOURCE_STATUS_BLOCKED, SOURCE_STATUS_INACTIVE}:
        return False
    if not bool(record.is_official):
        return False
    return True


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
                table.c.category == (_normalize_category(category) or ""),
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
    category_norm = _normalize_category(category)
    source_url_norm = _norm_text(source_url)
    is_official_norm = bool(is_official and _is_official_url(source_url_norm))
    fetch_mode_norm = _norm_lower(fetch_mode)
    status_norm = _norm_lower(status) or SOURCE_STATUS_ACTIVE
    if source_url_norm and not is_official_norm and bool(is_official):
        status_norm = SOURCE_STATUS_MANUAL
        fetch_mode_norm = FETCH_MODE_MANUAL
    payload = {
        "jurisdiction_id": int(jurisdiction_id),
        "category": category_norm,
        "source_label": _norm_text(source_label),
        "source_url": source_url_norm,
        "source_kind": _norm_lower(source_kind),
        "publisher_name": _norm_text(publisher_name),
        "publisher_type": _norm_lower(publisher_type),
        "authority_level": _norm_lower(authority_level),
        "fetch_mode": fetch_mode_norm,
        "status": status_norm,
        "is_official": is_official_norm,
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
    category_norm = _normalize_category(category) or ""
    source_url_norm = _norm_text(source_url)

    if source_url_norm:
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
    else:
        row = db.execute(
            select(table)
            .where(
                and_(
                    table.c.jurisdiction_id == int(jurisdiction_id),
                    table.c.category == category_norm,
                    table.c.source_url.is_(None),
                )
            )
            .order_by(table.c.updated_at.desc().nullslast())
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
    is_official_norm = bool(is_official and _is_official_url(source_url_norm or existing.source_url))
    fetch_mode_norm = _norm_lower(fetch_mode) or existing.fetch_mode
    status_norm = _norm_lower(status) or SOURCE_STATUS_ACTIVE
    if (source_url_norm or existing.source_url) and bool(is_official) and not is_official_norm:
        status_norm = SOURCE_STATUS_MANUAL
        fetch_mode_norm = FETCH_MODE_MANUAL
    db.execute(
        update(table)
        .where(table.c.id == existing.id)
        .values(
            source_label=_norm_text(source_label) or existing.source_label,
            source_kind=_norm_lower(source_kind) or existing.source_kind,
            publisher_name=_norm_text(publisher_name) or existing.publisher_name,
            publisher_type=_norm_lower(publisher_type) or existing.publisher_type,
            source_url=source_url_norm or existing.source_url,
            authority_level=_norm_lower(authority_level) or existing.authority_level,
            fetch_mode=fetch_mode_norm,
            status=status_norm,
            is_official=is_official_norm,
            is_active=bool(is_active),
            notes=_norm_text(notes) or existing.notes,
            coverage_hint=_norm_text(coverage_hint) or existing.coverage_hint,
            review_state=_norm_lower(review_state) or existing.review_state,
            last_checked_at=last_checked_at or existing.last_checked_at,
            last_reviewed_at=last_reviewed_at or existing.last_reviewed_at,
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return get_source_family_by_id(db, source_family_id=existing.id)  # type: ignore[return-value]


def summarize_source_family_coverage(
    db: Session,
    *,
    jurisdiction_id: int,
    required_categories: list[str] | None = None,
    include_inactive: bool = False,
) -> JurisdictionCoverageGapSummary:
    rows = get_source_families_for_jurisdiction(
        db,
        jurisdiction_id=jurisdiction_id,
        include_inactive=include_inactive,
    )
    required = _dedupe(list(required_categories or []))
    category_map: dict[str, list[dict[str, Any]]] = {}
    covered: list[str] = []
    official_categories: list[str] = []
    binding_categories: list[str] = []

    for record in rows:
        category = _normalize_category(record.category)
        if not category:
            continue
        entry = {
            "source_family_id": int(record.id),
            "source_label": record.source_label,
            "source_url": record.source_url,
            "source_kind": record.source_kind,
            "publisher_name": record.publisher_name,
            "publisher_type": record.publisher_type,
            "authority_level": record.authority_level,
            "authority_rank": _authority_rank(record.authority_level),
            "fetch_mode": record.fetch_mode,
            "status": record.status,
            "is_official": bool(record.is_official),
            "is_active": bool(record.is_active),
            "coverage_hint": record.coverage_hint,
            "review_state": record.review_state,
            "last_checked_at": record.last_checked_at.isoformat() if record.last_checked_at else None,
            "last_reviewed_at": record.last_reviewed_at.isoformat() if record.last_reviewed_at else None,
            "usable_for_coverage": _is_category_covered(record),
            "binding_sufficient": _is_binding_authority(record.authority_level) and bool(record.is_official) and bool(record.is_active),
        }
        category_map.setdefault(category, []).append(entry)
        if entry["usable_for_coverage"]:
            covered.append(category)
        if entry["is_official"]:
            official_categories.append(category)
        if entry["binding_sufficient"]:
            binding_categories.append(category)

    for items in category_map.values():
        items.sort(
            key=lambda x: (
                -int(x["usable_for_coverage"]),
                -int(x["binding_sufficient"]),
                -int(x["is_official"]),
                -int(x["authority_rank"]),
                str(x.get("source_label") or ""),
            )
        )

    covered_norm = _dedupe(covered)
    official_norm = _dedupe(official_categories)
    binding_norm = _dedupe(binding_categories)
    missing = [c for c in required if c not in set(covered_norm)]

    return JurisdictionCoverageGapSummary(
        jurisdiction_id=int(jurisdiction_id),
        required_categories=required,
        covered_categories=covered_norm,
        missing_categories=_dedupe(missing),
        official_categories=official_norm,
        binding_categories=binding_norm,
        active_source_count=sum(1 for row in rows if bool(row.is_active)),
        category_map=category_map,
    )


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
