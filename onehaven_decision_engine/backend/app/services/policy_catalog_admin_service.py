from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.policy_models import PolicyCatalogEntry
from app.services.policy_catalog import PolicyCatalogItem, catalog_for_market


def _norm_state(v: Optional[str]) -> str:
    return (v or "MI").strip().upper()


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().lower()
    return s or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s or None


def _row_to_item(row: PolicyCatalogEntry) -> PolicyCatalogItem:
    return PolicyCatalogItem(
        url=row.url,
        state=row.state,
        county=row.county,
        city=row.city,
        pha_name=row.pha_name,
        program_type=row.program_type,
        publisher=row.publisher,
        title=row.title,
        notes=row.notes,
        source_kind=row.source_kind,
        is_authoritative=bool(row.is_authoritative),
        priority=int(row.priority or 100),
    )


def _market_stmt(
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    return (
        _norm_state(state),
        _norm_lower(county),
        _norm_lower(city),
        _norm_text(pha_name),
    )


def list_catalog_entries_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> list[PolicyCatalogEntry]:
    st, cnty, cty, pha = _market_stmt(
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    q = select(PolicyCatalogEntry).where(PolicyCatalogEntry.state == st)

    if org_id is None:
        q = q.where(PolicyCatalogEntry.org_id.is_(None))
    else:
        q = q.where(
            (PolicyCatalogEntry.org_id == org_id)
            | (PolicyCatalogEntry.org_id.is_(None))
        )

    rows = list(db.scalars(q).all())

    out: list[PolicyCatalogEntry] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
        out.append(row)

    out.sort(key=lambda x: (int(x.priority or 100), x.title or "", x.url or ""))
    return out


def merged_catalog_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCatalogItem]:
    baseline = catalog_for_market(
        state=state,
        county=county,
        city=city,
        focus=focus,
    )
    db_rows = list_catalog_entries_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    # These URLs are explicitly suppressed by inactive editable rows.
    # Important: suppression must work even if baseline_url is missing.
    suppressed_urls: set[str] = set()

    # These are active override rows keyed by URL.
    overrides_by_url: dict[str, PolicyCatalogItem] = {}

    for row in db_rows:
        row_url = (row.url or "").strip()
        baseline_url = (row.baseline_url or "").strip()

        # Inactive rows act as suppressions of:
        # 1) their baseline_url if present
        # 2) their own url always
        if not row.is_active:
            if baseline_url:
                suppressed_urls.add(baseline_url)
            if row_url:
                suppressed_urls.add(row_url)
            continue

        if not row_url:
            continue

        item = _row_to_item(row)
        overrides_by_url[row_url] = item

        # Active override with a different baseline URL replaces the baseline URL.
        if baseline_url and baseline_url != row_url:
            suppressed_urls.add(baseline_url)

    merged: list[PolicyCatalogItem] = []

    # Bring in baseline, minus anything explicitly suppressed
    for item in baseline:
        url = item.url.strip()
        if not url:
            continue
        if url in suppressed_urls:
            continue
        if url in overrides_by_url:
            merged.append(overrides_by_url[url])
        else:
            merged.append(item)

    # Add active override rows that are not already present
    for url, item in overrides_by_url.items():
        if url in suppressed_urls:
            continue
        if all(existing.url.strip() != url for existing in merged):
            merged.append(item)

    merged.sort(key=lambda x: (int(x.priority or 100), x.title or "", x.url or ""))

    deduped: list[PolicyCatalogItem] = []
    seen: set[str] = set()
    for item in merged:
        key = item.url.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def bootstrap_market_catalog_entries(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict:
    baseline = catalog_for_market(
        state=state,
        county=county,
        city=city,
        focus=focus,
    )
    existing = list_catalog_entries_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    existing_urls = {r.url.strip() for r in existing if r.url}

    created = 0
    st, cnty, cty, pha = _market_stmt(
        org_id=org_id, state=state, county=county, city=city, pha_name=pha_name
    )

    for item in baseline:
        url = item.url.strip()
        if not url or url in existing_urls:
            continue

        row = PolicyCatalogEntry(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type=item.program_type,
            url=url,
            publisher=item.publisher,
            title=item.title,
            notes=item.notes,
            source_kind=item.source_kind,
            is_authoritative=bool(item.is_authoritative),
            priority=int(item.priority or 100),
            is_active=True,
            is_override=True,
            baseline_url=url,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(row)
        created += 1

    db.commit()
    return {"created_count": created}


def reset_market_catalog_entries(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict:
    rows = list_catalog_entries_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    count = len(rows)
    for row in rows:
        db.delete(row)
    db.commit()
    return {"deleted_count": count}


def create_catalog_entry(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
    url: str,
    publisher: Optional[str],
    title: Optional[str],
    notes: Optional[str],
    source_kind: Optional[str],
    is_authoritative: bool,
    priority: int,
    baseline_url: Optional[str] = None,
) -> PolicyCatalogEntry:
    st, cnty, cty, pha = _market_stmt(
        org_id=org_id, state=state, county=county, city=city, pha_name=pha_name
    )
    row = PolicyCatalogEntry(
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type=_norm_text(program_type),
        url=url.strip(),
        publisher=_norm_text(publisher),
        title=_norm_text(title),
        notes=_norm_text(notes),
        source_kind=_norm_text(source_kind),
        is_authoritative=bool(is_authoritative),
        priority=int(priority or 100),
        is_active=True,
        is_override=True,
        baseline_url=_norm_text(baseline_url),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_catalog_entry(
    db: Session,
    *,
    item_id: int,
    org_id: Optional[int],
    title: Optional[str],
    publisher: Optional[str],
    notes: Optional[str],
    source_kind: Optional[str],
    is_authoritative: Optional[bool],
    priority: Optional[int],
    url: Optional[str],
    is_active: Optional[bool],
) -> PolicyCatalogEntry | None:
    row = db.get(PolicyCatalogEntry, int(item_id))
    if row is None:
        return None

    if org_id is None:
        if row.org_id is not None:
            return None
    else:
        if row.org_id not in {None, org_id}:
            return None

    if title is not None:
        row.title = _norm_text(title)
    if publisher is not None:
        row.publisher = _norm_text(publisher)
    if notes is not None:
        row.notes = _norm_text(notes)
    if source_kind is not None:
        row.source_kind = _norm_text(source_kind)
    if is_authoritative is not None:
        row.is_authoritative = bool(is_authoritative)
    if priority is not None:
        row.priority = int(priority)
    if url is not None and url.strip():
        row.url = url.strip()
    if is_active is not None:
        row.is_active = bool(is_active)

    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def disable_catalog_entry(
    db: Session,
    *,
    item_id: int,
    org_id: Optional[int],
) -> PolicyCatalogEntry | None:
    row = db.get(PolicyCatalogEntry, int(item_id))
    if row is None:
        return None

    if org_id is None:
        if row.org_id is not None:
            return None
    else:
        if row.org_id not in {None, org_id}:
            return None

    row.is_active = False
    row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def source_kind_coverage_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )

    counts: dict[str, int] = {}
    for item in items:
        key = (item.source_kind or "unknown").strip()
        counts[key] = counts.get(key, 0) + 1

    # Required for operational baseline
    required = [
        "federal_anchor",
        "state_anchor",
        "municipal_registration",
        "municipal_inspection",
    ]

    # Recommended / useful, but not always hard blockers
    recommended = [
        "municipal_certificate",
        "municipal_enforcement",
        "municipal_ordinance",
        "pha_guidance",
        "pha_plan",
        "state_hcv_anchor",
    ]

    missing_required = [k for k in required if counts.get(k, 0) <= 0]
    missing_recommended = [k for k in recommended if counts.get(k, 0) <= 0]

    return {
        "counts": counts,
        "missing": missing_required + missing_recommended,
        "missing_required": missing_required,
        "missing_recommended": missing_recommended,
        "complete_core": len(missing_required) == 0,
    }


# ---- Chunk 5 catalog enrichments ----
_base_source_kind_coverage_for_market = source_kind_coverage_for_market


def source_kind_coverage_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
) -> dict:
    payload = _base_source_kind_coverage_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    payload['resolution_order'] = [
        'michigan_statewide_baseline',
        'county_rules',
        'city_rules',
        'housing_authority_overlays',
        'org_overrides',
    ]
    payload['recommended_source_layers'] = {
        'statewide_baseline': ['state statute', 'state housing program guidance', 'federal anchors'],
        'county_rules': ['county code', 'county health / building authorities'],
        'city_rules': ['city ordinance', 'rental inspection program', 'certificate / occupancy pages'],
        'housing_authority_overlays': ['PHA admin plan', 'landlord packet', 'program notices'],
        'org_overrides': ['internal operations memo', 'approved exception handling'],
    }
    return payload
