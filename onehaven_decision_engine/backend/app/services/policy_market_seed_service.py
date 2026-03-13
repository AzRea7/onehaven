from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_catalog import PolicyCatalogItem, catalog_for_market, catalog_market_packs


def _norm_state(v: Optional[str]) -> Optional[str]:
    return (v or "").strip().upper() or None


def _norm_lower(v: Optional[str]) -> Optional[str]:
    return (v or "").strip().lower() or None


def _find_existing(db: Session, *, org_id: Optional[int], item: PolicyCatalogItem) -> Optional[PolicySource]:
    q = db.query(PolicySource).filter(PolicySource.url == item.url)

    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(PolicySource.org_id == org_id)

    q = q.filter(
        PolicySource.state == (_norm_state(item.state) or "MI"),
        PolicySource.county == _norm_lower(item.county),
        PolicySource.city == _norm_lower(item.city),
    )
    return q.first()


def _apply_item(row: PolicySource, item: PolicyCatalogItem) -> None:
    row.state = _norm_state(item.state) or "MI"
    row.county = _norm_lower(item.county)
    row.city = _norm_lower(item.city)
    row.pha_name = item.pha_name
    row.program_type = item.program_type
    row.publisher = item.publisher
    row.title = item.title
    row.notes = "\n".join(
        x
        for x in [
            item.notes,
            f"catalog_meta={{'source_kind': {item.source_kind!r}, "
            f"'source_scope': {item.source_scope!r}, "
            f"'source_domain': {item.source_domain!r}, "
            f"'extraction_template': {item.extraction_template!r}, "
            f"'priority': {item.priority!r}}}",
        ]
        if x
    )


def seed_market_sources(
    db: Session,
    *,
    org_id: Optional[int],
    state: str = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    items = catalog_for_market(state=state, county=county, city=city, focus=focus)

    created = 0
    updated = 0
    rows: list[PolicySource] = []

    for item in items:
        row = _find_existing(db, org_id=org_id, item=item)
        if row is None:
            row = PolicySource(org_id=org_id, url=item.url)
            db.add(row)
            created += 1
        else:
            updated += 1

        _apply_item(row, item)
        rows.append(row)

    db.commit()
    for row in rows:
        db.refresh(row)

    return {
        "ok": True,
        "created": created,
        "updated": updated,
        "count": len(rows),
        "items": [
            {
                "id": r.id,
                "url": r.url,
                "state": r.state,
                "county": r.county,
                "city": r.city,
                "publisher": r.publisher,
                "title": r.title,
                "notes": r.notes,
            }
            for r in rows
        ],
    }


def seed_focus_markets(
    db: Session,
    *,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    items = []
    for pack in catalog_market_packs(focus=focus):
        items.append(
            {
                **pack,
                **seed_market_sources(
                    db,
                    org_id=org_id,
                    state=pack["state"] or "MI",
                    county=pack.get("county"),
                    city=pack.get("city"),
                    focus=focus,
                ),
            }
        )

    return {"ok": True, "focus": focus, "count": len(items), "items": items}