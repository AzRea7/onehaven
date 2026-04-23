
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.policy_models import PolicyCatalogEntry
from app.services.policy_catalog import (
    PolicyCatalogItem,
    catalog_for_market,
    filter_official_catalog_items,
    policy_catalog_source_family,
)


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


def _item_url_key(item: PolicyCatalogItem) -> str:
    return (item.url or "").strip().lower()


def _is_truthy_authoritative(item: PolicyCatalogItem) -> bool:
    return bool(getattr(item, "is_authoritative", False))


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
        q = q.where((PolicyCatalogEntry.org_id == org_id) | (PolicyCatalogEntry.org_id.is_(None)))

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


_DEFAULT_EVIDENCE_PRIORITY = [
    "legal_primary",
    "state_program",
    "municipal_operations",
    "program_admin",
    "supporting_guidance",
    "unknown",
]


def _catalog_item_source_family(item: PolicyCatalogItem) -> str:
    try:
        return str(policy_catalog_source_family(item))
    except Exception:
        source_kind = str(getattr(item, "source_kind", None) or "").strip().lower()
        if "anchor" in source_kind or "code" in source_kind or "ordinance" in source_kind:
            return "legal_primary"
        if "state" in source_kind:
            return "state_program"
        if "pha" in source_kind or "housing_authority" in source_kind:
            return "program_admin"
        if "municipal" in source_kind or "city_" in source_kind or "county_" in source_kind:
            return "municipal_operations"
        return "unknown"


def _coverage_category_hints_for_item(item: PolicyCatalogItem) -> list[str]:
    text = " ".join(
        [
            str(getattr(item, "title", "") or ""),
            str(getattr(item, "notes", "") or ""),
            str(getattr(item, "url", "") or ""),
        ]
    ).lower()
    hints: list[str] = []
    checks = {
        "lead": ["lead", "lbp"],
        "source_of_income": ["source of income", "voucher discrimination", "fair housing"],
        "permits": ["permit"],
        "documents": ["document", "application", "packet", "submit"],
        "contacts": ["contact", "office", "department", "division"],
        "rental_license": ["license", "registration", "certificate"],
        "fees": ["fee", "payment"],
        "program_overlay": ["voucher", "hcv", "nspire", "hap", "overlay"],
        "inspection": ["inspection"],
        "occupancy": ["occupancy", "certificate of occupancy", "re-occupancy"],
        "registration": ["registration"],
    }
    for category, pats in checks.items():
        if any(p in text for p in pats):
            hints.append(category)
    return sorted(set(hints))


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
    baseline = filter_official_catalog_items(
        catalog_for_market(state=state, county=county, city=city, focus=focus)
    )
    db_rows = list_catalog_entries_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    suppressed_urls: set[str] = set()
    overrides_by_url: dict[str, PolicyCatalogItem] = {}

    for row in db_rows:
        row_url = (row.url or "").strip()
        baseline_url = (row.baseline_url or "").strip()

        if not row.is_active:
            if baseline_url:
                suppressed_urls.add(baseline_url.strip().lower())
            if row_url:
                suppressed_urls.add(row_url.strip().lower())
            continue

        if not row_url:
            continue

        item = _row_to_item(row)
        vetted = filter_official_catalog_items([item])
        if not vetted:
            continue

        item = vetted[0]
        overrides_by_url[_item_url_key(item)] = item

        if baseline_url and baseline_url != row_url:
            suppressed_urls.add(baseline_url.strip().lower())

    merged: list[PolicyCatalogItem] = []
    for item in baseline:
        url_key = _item_url_key(item)
        if not url_key or url_key in suppressed_urls:
            continue
        merged.append(overrides_by_url.get(url_key, item))

    for url_key, item in overrides_by_url.items():
        if url_key in suppressed_urls:
            continue
        if all(_item_url_key(existing) != url_key for existing in merged):
            merged.append(item)

    merged = filter_official_catalog_items(merged)
    enriched: list[PolicyCatalogItem] = []
    for item in merged:
        notes = str(getattr(item, "notes", "") or "")
        hints = _coverage_category_hints_for_item(item)
        if hints:
            base_parts = [p.strip() for p in notes.split("|") if p.strip() and not p.strip().lower().startswith("category_hints=")]
            base_parts.append("category_hints=" + ",".join(hints))
            notes = " | ".join(base_parts).strip()
        enriched.append(
            PolicyCatalogItem(
                url=item.url,
                state=item.state,
                county=item.county,
                city=item.city,
                pha_name=item.pha_name,
                program_type=item.program_type,
                publisher=item.publisher,
                title=item.title,
                notes=notes,
                source_kind=item.source_kind,
                is_authoritative=item.is_authoritative,
                priority=item.priority,
            )
        )

    enriched.sort(
        key=lambda item: (
            _DEFAULT_EVIDENCE_PRIORITY.index(_catalog_item_source_family(item))
            if _catalog_item_source_family(item) in _DEFAULT_EVIDENCE_PRIORITY
            else len(_DEFAULT_EVIDENCE_PRIORITY),
            0 if _is_truthy_authoritative(item) else 1,
            int(getattr(item, "priority", 100) or 100),
            str(getattr(item, "title", "") or ""),
            str(getattr(item, "url", "") or ""),
        )
    )

    deduped: list[PolicyCatalogItem] = []
    seen: set[str] = set()
    for item in enriched:
        key = _item_url_key(item)
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
) -> dict[str, Any]:
    baseline = catalog_for_market(state=state, county=county, city=city, focus=focus)
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
    st, cnty, cty, pha = _market_stmt(org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)

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
) -> dict[str, Any]:
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
    st, cnty, cty, pha = _market_stmt(org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
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
) -> dict[str, Any]:
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

    required = ["federal_anchor", "state_anchor", "municipal_registration", "municipal_inspection"]
    recommended = ["municipal_certificate", "municipal_enforcement", "municipal_ordinance", "pha_guidance", "pha_plan", "state_hcv_anchor"]

    missing_required = [k for k in required if counts.get(k, 0) <= 0]
    missing_recommended = [k for k in recommended if counts.get(k, 0) <= 0]

    return {
        "counts": counts,
        "missing": missing_required + missing_recommended,
        "missing_required": missing_required,
        "missing_recommended": missing_recommended,
        "complete_core": len(missing_required) == 0,
        "resolution_order": [
            "michigan_statewide_baseline",
            "county_rules",
            "city_rules",
            "housing_authority_overlays",
            "org_overrides",
        ],
    }


def catalog_control_plane_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    rows = list_catalog_entries_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    disabled_urls: list[str] = []
    override_urls: list[str] = []
    source_family_counts: dict[str, int] = {}

    for row in rows:
        item = _row_to_item(row)
        family = _catalog_item_source_family(item)
        source_family_counts[family] = source_family_counts.get(family, 0) + 1
        if not bool(getattr(row, "is_active", True)):
            if getattr(row, "url", None):
                disabled_urls.append(str(row.url).strip())
            if getattr(row, "baseline_url", None):
                disabled_urls.append(str(row.baseline_url).strip())
        else:
            override_urls.append(str(getattr(row, "url", "") or "").strip())

    return {
        "truth_model": "catalog_control_plane",
        "service_role": "operational_control_plane",
        "state": _norm_state(state),
        "county": _norm_lower(county),
        "city": _norm_lower(city),
        "pha_name": _norm_text(pha_name),
        "evidence_priority": list(_DEFAULT_EVIDENCE_PRIORITY),
        "disabled_urls": sorted({u for u in disabled_urls if u}),
        "override_urls": sorted({u for u in override_urls if u}),
        "source_family_counts": source_family_counts,
        "db_entry_count": len(rows),
    }


def catalog_admin_summary_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    families: dict[str, int] = {}
    for item in items:
        family = _catalog_item_source_family(item)
        families[family] = families.get(family, 0) + 1
    control = catalog_control_plane_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return {
        **control,
        "merged_item_count": len(items),
        "merged_source_family_counts": families,
        "top_urls": [str(getattr(item, "url", "") or "") for item in items[:20]],
    }
