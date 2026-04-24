from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse

from sqlalchemy import MetaData, Table, and_, func, select, update
from sqlalchemy.orm import Session


JURISDICTION_TYPE_STATE = "state"
JURISDICTION_TYPE_COUNTY = "county"
JURISDICTION_TYPE_CITY = "city"
JURISDICTION_TYPE_TOWNSHIP = "township"
JURISDICTION_TYPE_VILLAGE = "village"
JURISDICTION_TYPE_PHA = "pha"

ONBOARDING_DISCOVERED = "discovered"
ONBOARDING_SITE_CONFIRMED = "official_site_confirmed"
ONBOARDING_SOURCE_MAPPED = "source_family_mapped"
ONBOARDING_RULES_EXTRACTED = "rules_extracted"
ONBOARDING_HUMAN_REVIEWED = "human_reviewed"
ONBOARDING_TRUSTED = "trusted"
ONBOARDING_BLOCKED = "blocked_manual_only"


@dataclass(frozen=True)
class JurisdictionRegistryRecord:
    id: int
    org_id: int | None
    jurisdiction_type: str
    state_code: str | None
    state_name: str | None
    county_name: str | None
    city_name: str | None
    display_name: str
    slug: str
    geoid: str | None
    lsad: str | None
    census_class: str | None
    parent_jurisdiction_id: int | None
    official_website: str | None
    onboarding_status: str
    source_confidence: float | None
    is_active: bool
    last_reviewed_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_upper(value: Any) -> str | None:
    text = _norm_text(value)
    return text.upper() if text else None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _slugify(*parts: Any) -> str:
    normalized: list[str] = []
    for part in parts:
        text = _norm_text(part)
        if not text:
            continue
        text = text.lower().replace("&", "and")
        for ch in ["/", ",", ".", "(", ")", "'", '"']:
            text = text.replace(ch, " ")
        text = "-".join(filter(None, text.split()))
        if text:
            normalized.append(text)
    return "-".join(normalized)




def _host_from_url(url: Any) -> str:
    host = urlparse(str(url or "").strip()).netloc.strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    return host


def _host_looks_guessed(host: str) -> bool:
    host = str(host or "").strip().lower()
    if not host:
        return True
    return any(part in host for part in (".ci.", ".co.")) or host.startswith("ci.") or host.startswith("co.")


def _is_official_website(url: Any) -> bool:
    host = _host_from_url(url)
    if not host or _host_looks_guessed(host):
        return False
    return host.endswith(".gov") or host.endswith(".mi.us") or host in {
        "ecfr.gov", "www.ecfr.gov",
        "federalregister.gov", "www.federalregister.gov",
        "hud.gov", "www.hud.gov",
        "michigan.gov", "www.michigan.gov",
        "legislature.mi.gov", "www.legislature.mi.gov",
        "courts.michigan.gov", "www.courts.michigan.gov",
    }


def _sanitize_official_website(url: Any) -> str | None:
    text = _norm_text(url)
    if not text:
        return None
    return text if _is_official_website(text) else None

def _display_name(
    *,
    jurisdiction_type: str,
    state_code: str | None,
    county_name: str | None,
    city_name: str | None,
    state_name: str | None = None,
) -> str:
    jt = _norm_lower(jurisdiction_type) or "jurisdiction"
    if jt == JURISDICTION_TYPE_STATE:
        return state_name or state_code or "Unknown state"
    if jt == JURISDICTION_TYPE_COUNTY:
        if county_name and state_code:
            return f"{county_name.title()} County, {state_code}"
        return county_name.title() if county_name else "Unknown county"
    if jt in {JURISDICTION_TYPE_CITY, JURISDICTION_TYPE_TOWNSHIP, JURISDICTION_TYPE_VILLAGE}:
        if city_name and state_code:
            return f"{city_name.title()}, {state_code}"
        return city_name.title() if city_name else "Unknown locality"
    if jt == JURISDICTION_TYPE_PHA:
        if city_name and state_code:
            return f"{city_name.title()} PHA, {state_code}"
        return "PHA"
    return city_name or county_name or state_name or state_code or "Jurisdiction"


def _jurisdictions_table(db: Session) -> Table:
    metadata = MetaData()
    return Table("jurisdiction_registry", metadata, autoload_with=db.bind)


def _row_to_record(row: Any) -> JurisdictionRegistryRecord:
    data = dict(row._mapping)
    return JurisdictionRegistryRecord(
        id=int(data["id"]),
        org_id=data.get("org_id"),
        jurisdiction_type=str(data.get("jurisdiction_type") or ""),
        state_code=data.get("state_code"),
        state_name=data.get("state_name"),
        county_name=data.get("county_name"),
        city_name=data.get("city_name"),
        display_name=str(data.get("display_name") or ""),
        slug=str(data.get("slug") or ""),
        geoid=data.get("geoid"),
        lsad=data.get("lsad"),
        census_class=data.get("census_class"),
        parent_jurisdiction_id=data.get("parent_jurisdiction_id"),
        official_website=data.get("official_website"),
        onboarding_status=str(data.get("onboarding_status") or ONBOARDING_DISCOVERED),
        source_confidence=(
            float(data["source_confidence"]) if data.get("source_confidence") is not None else None
        ),
        is_active=bool(data.get("is_active", True)),
        last_reviewed_at=data.get("last_reviewed_at"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
    )


def build_registry_slug(
    *,
    jurisdiction_type: str,
    state_code: str | None,
    county_name: str | None = None,
    city_name: str | None = None,
    geoid: str | None = None,
) -> str:
    jt = _norm_lower(jurisdiction_type) or "jurisdiction"
    if geoid:
        return _slugify(jt, state_code, county_name, city_name, geoid)
    return _slugify(jt, state_code, county_name, city_name)


def find_jurisdiction_by_id(db: Session, *, jurisdiction_id: int) -> JurisdictionRegistryRecord | None:
    table = _jurisdictions_table(db)
    row = db.execute(
        select(table).where(table.c.id == int(jurisdiction_id)).limit(1)
    ).first()
    return _row_to_record(row) if row else None


def find_jurisdiction_by_slug(db: Session, *, slug: str) -> JurisdictionRegistryRecord | None:
    table = _jurisdictions_table(db)
    row = db.execute(
        select(table).where(table.c.slug == str(slug).strip().lower()).limit(1)
    ).first()
    return _row_to_record(row) if row else None


def find_jurisdiction_by_geoid(db: Session, *, geoid: str) -> JurisdictionRegistryRecord | None:
    table = _jurisdictions_table(db)
    row = db.execute(
        select(table).where(table.c.geoid == str(geoid).strip()).limit(1)
    ).first()
    return _row_to_record(row) if row else None


def list_child_jurisdictions(
    db: Session,
    *,
    parent_jurisdiction_id: int,
    include_inactive: bool = False,
) -> list[JurisdictionRegistryRecord]:
    table = _jurisdictions_table(db)
    conditions = [table.c.parent_jurisdiction_id == int(parent_jurisdiction_id)]
    if not include_inactive:
        conditions.append(table.c.is_active.is_(True))
    rows = db.execute(
        select(table).where(and_(*conditions)).order_by(func.lower(table.c.display_name))
    ).all()
    return [_row_to_record(row) for row in rows]


def get_or_create_jurisdiction(
    db: Session,
    *,
    jurisdiction_type: str,
    state_code: str | None,
    state_name: str | None = None,
    county_name: str | None = None,
    city_name: str | None = None,
    geoid: str | None = None,
    lsad: str | None = None,
    census_class: str | None = None,
    parent_jurisdiction_id: int | None = None,
    official_website: str | None = None,
    onboarding_status: str = ONBOARDING_DISCOVERED,
    source_confidence: float | None = None,
    org_id: int | None = None,
    is_active: bool = True,
) -> JurisdictionRegistryRecord:
    table = _jurisdictions_table(db)

    jt = _norm_lower(jurisdiction_type) or "jurisdiction"
    state_code_norm = _norm_upper(state_code)
    state_name_norm = _norm_text(state_name)
    county_norm = _norm_lower(county_name)
    city_norm = _norm_lower(city_name)
    geoid_norm = _norm_text(geoid)
    slug = build_registry_slug(
        jurisdiction_type=jt,
        state_code=state_code_norm,
        county_name=county_norm,
        city_name=city_norm,
        geoid=geoid_norm,
    )
    display_name = _display_name(
        jurisdiction_type=jt,
        state_code=state_code_norm,
        county_name=county_norm,
        city_name=city_norm,
        state_name=state_name_norm,
    )

    existing = None
    if geoid_norm:
        existing = find_jurisdiction_by_geoid(db, geoid=geoid_norm)
    if existing is None:
        existing = find_jurisdiction_by_slug(db, slug=slug)

    official_website_norm = _sanitize_official_website(official_website)
    onboarding_status_norm = _norm_lower(onboarding_status) or ONBOARDING_DISCOVERED
    if onboarding_status_norm == ONBOARDING_SITE_CONFIRMED and not official_website_norm:
        onboarding_status_norm = ONBOARDING_DISCOVERED

    payload = {
        "org_id": org_id,
        "jurisdiction_type": jt,
        "state_code": state_code_norm,
        "state_name": state_name_norm,
        "county_name": county_norm,
        "city_name": city_norm,
        "display_name": display_name,
        "slug": slug,
        "geoid": geoid_norm,
        "lsad": _norm_text(lsad),
        "census_class": _norm_text(census_class),
        "parent_jurisdiction_id": parent_jurisdiction_id,
        "official_website": official_website_norm,
        "onboarding_status": onboarding_status_norm,
        "source_confidence": source_confidence,
        "is_active": bool(is_active),
        "updated_at": datetime.utcnow(),
    }

    if existing is None:
        payload["created_at"] = datetime.utcnow()
        inserted = db.execute(table.insert().values(**payload))
        db.flush()
        return find_jurisdiction_by_id(db, jurisdiction_id=int(inserted.inserted_primary_key[0]))  # type: ignore[index]

    db.execute(update(table).where(table.c.id == existing.id).values(**payload))
    db.flush()
    return find_jurisdiction_by_id(db, jurisdiction_id=existing.id)  # type: ignore[return-value]


def mark_onboarding_status(
    db: Session,
    *,
    jurisdiction_id: int,
    onboarding_status: str,
    last_reviewed_at: datetime | None = None,
) -> JurisdictionRegistryRecord | None:
    table = _jurisdictions_table(db)
    db.execute(
        update(table)
        .where(table.c.id == int(jurisdiction_id))
        .values(
            onboarding_status=_norm_lower(onboarding_status) or ONBOARDING_DISCOVERED,
            last_reviewed_at=last_reviewed_at,
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return find_jurisdiction_by_id(db, jurisdiction_id=jurisdiction_id)


def resolve_jurisdiction_hierarchy(
    db: Session,
    *,
    state_code: str | None,
    county_name: str | None = None,
    city_name: str | None = None,
) -> dict[str, JurisdictionRegistryRecord | None]:
    state_code_norm = _norm_upper(state_code)
    county_norm = _norm_lower(county_name)
    city_norm = _norm_lower(city_name)
    table = _jurisdictions_table(db)

    state_row = None
    county_row = None
    city_row = None

    if state_code_norm:
        state_row = db.execute(
            select(table)
            .where(
                and_(
                    table.c.jurisdiction_type == JURISDICTION_TYPE_STATE,
                    table.c.state_code == state_code_norm,
                    table.c.is_active.is_(True),
                )
            )
            .limit(1)
        ).first()

    if state_code_norm and county_norm:
        county_row = db.execute(
            select(table)
            .where(
                and_(
                    table.c.jurisdiction_type == JURISDICTION_TYPE_COUNTY,
                    table.c.state_code == state_code_norm,
                    table.c.county_name == county_norm,
                    table.c.is_active.is_(True),
                )
            )
            .limit(1)
        ).first()

    if state_code_norm and city_norm:
        city_row = db.execute(
            select(table)
            .where(
                and_(
                    table.c.jurisdiction_type.in_(
                        [
                            JURISDICTION_TYPE_CITY,
                            JURISDICTION_TYPE_TOWNSHIP,
                            JURISDICTION_TYPE_VILLAGE,
                        ]
                    ),
                    table.c.state_code == state_code_norm,
                    table.c.city_name == city_norm,
                    table.c.is_active.is_(True),
                )
            )
            .order_by(table.c.jurisdiction_type)
            .limit(1)
        ).first()

    return {
        "state": _row_to_record(state_row) if state_row else None,
        "county": _row_to_record(county_row) if county_row else None,
        "city": _row_to_record(city_row) if city_row else None,
    }

# --- additive registry-source mapping overlay ---

def _table_has_column(table: Table, name: str) -> bool:
    try:
        return name in table.c
    except Exception:
        return False


def _safe_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = __import__('json').loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_registry_source_links(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _safe_json_dict(payload)
    normalized: dict[str, Any] = {}
    for key, value in data.items():
        k = _norm_lower(key)
        if not k:
            continue
        if isinstance(value, str):
            url = _sanitize_official_website(value)
            if url:
                normalized[k] = {"url": url, "trusted": True}
            continue
        if isinstance(value, dict):
            url = _sanitize_official_website(value.get('url'))
            if not url:
                continue
            row = dict(value)
            row['url'] = url
            row['trusted'] = True
            normalized[k] = row
    return normalized


def _normalize_registry_source_family_map(payload: dict[str, Any] | None) -> dict[str, list[str]]:
    data = _safe_json_dict(payload)
    normalized: dict[str, list[str]] = {}
    for category, families in data.items():
        key = _norm_lower(category)
        if not key:
            continue
        vals: list[str] = []
        if isinstance(families, (list, tuple)):
            vals = [str(x).strip().lower() for x in families if str(x).strip()]
        elif isinstance(families, str):
            vals = [str(families).strip().lower()] if str(families).strip() else []
        if vals:
            normalized[key] = sorted(dict.fromkeys(vals))
    return normalized


def get_jurisdiction_source_configuration(db: Session, *, jurisdiction_id: int) -> dict[str, Any]:
    table = _jurisdictions_table(db)
    row = db.execute(select(table).where(table.c.id == int(jurisdiction_id)).limit(1)).first()
    if not row:
        return {}
    mapping = row._mapping
    return {
        'source_links': _safe_json_dict(mapping.get('source_links_json')),
        'source_family_map': _safe_json_dict(mapping.get('source_family_map_json')),
        'validation_metadata': _safe_json_dict(mapping.get('validation_metadata_json')),
        'notes': mapping.get('notes'),
    }


def update_jurisdiction_source_configuration(
    db: Session,
    *,
    jurisdiction_id: int,
    source_links: dict[str, Any] | None = None,
    source_family_map: dict[str, Any] | None = None,
    validation_metadata: dict[str, Any] | None = None,
    notes: str | None = None,
) -> JurisdictionRegistryRecord | None:
    table = _jurisdictions_table(db)
    values: dict[str, Any] = {'updated_at': datetime.utcnow()}
    if _table_has_column(table, 'source_links_json') and source_links is not None:
        values['source_links_json'] = _normalize_registry_source_links(source_links)
    if _table_has_column(table, 'source_family_map_json') and source_family_map is not None:
        values['source_family_map_json'] = _normalize_registry_source_family_map(source_family_map)
    if _table_has_column(table, 'validation_metadata_json') and validation_metadata is not None:
        values['validation_metadata_json'] = _safe_json_dict(validation_metadata)
    if _table_has_column(table, 'notes') and notes is not None:
        values['notes'] = _norm_text(notes)
    db.execute(update(table).where(table.c.id == int(jurisdiction_id)).values(**values))
    db.flush()
    return find_jurisdiction_by_id(db, jurisdiction_id=jurisdiction_id)


def get_or_create_jurisdiction_with_sources(
    db: Session,
    *,
    jurisdiction_type: str,
    state_code: str | None,
    state_name: str | None = None,
    county_name: str | None = None,
    city_name: str | None = None,
    geoid: str | None = None,
    lsad: str | None = None,
    census_class: str | None = None,
    parent_jurisdiction_id: int | None = None,
    official_website: str | None = None,
    onboarding_status: str = ONBOARDING_DISCOVERED,
    source_confidence: float | None = None,
    org_id: int | None = None,
    is_active: bool = True,
    source_links: dict[str, Any] | None = None,
    source_family_map: dict[str, Any] | None = None,
    validation_metadata: dict[str, Any] | None = None,
    notes: str | None = None,
) -> JurisdictionRegistryRecord:
    record = get_or_create_jurisdiction(
        db,
        jurisdiction_type=jurisdiction_type,
        state_code=state_code,
        state_name=state_name,
        county_name=county_name,
        city_name=city_name,
        geoid=geoid,
        lsad=lsad,
        census_class=census_class,
        parent_jurisdiction_id=parent_jurisdiction_id,
        official_website=official_website,
        onboarding_status=onboarding_status,
        source_confidence=source_confidence,
        org_id=org_id,
        is_active=is_active,
    )
    update_jurisdiction_source_configuration(
        db,
        jurisdiction_id=record.id,
        source_links=source_links,
        source_family_map=source_family_map,
        validation_metadata=validation_metadata,
        notes=notes,
    )
    return find_jurisdiction_by_id(db, jurisdiction_id=record.id) or record
