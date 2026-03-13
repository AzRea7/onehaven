from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_catalog_admin_service import merged_catalog_for_market


ARCHIVE_MARKER = "[archived_stale_source]"


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v or None


def _is_archived_source(src: PolicySource) -> bool:
    notes = (src.notes or "").lower()
    return ARCHIVE_MARKER in notes


def archive_stale_market_sources(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict:
    """
    Soft-archive collected market sources that are no longer in the active merged
    catalog for this market.

    Important behavior:
    - if a source URL has been disabled in editable catalog, it should be absent
      from merged_catalog_for_market()
    - any collected PolicySource rows with that URL should then be archived here
    """
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    active_catalog_items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )
    active_urls = {
        item.url.strip()
        for item in active_catalog_items
        if item.url and item.url.strip()
    }

    q = db.query(PolicySource).filter(PolicySource.state == st)

    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(
            (PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None))
        )

    rows = q.all()

    market_rows: list[PolicySource] = []
    for src in rows:
        if src.county is not None and src.county != cnty:
            continue
        if src.city is not None and src.city != cty:
            continue
        if src.pha_name is not None and src.pha_name != pha:
            continue
        market_rows.append(src)

    archived_ids: list[int] = []
    kept_ids: list[int] = []

    now = datetime.utcnow().isoformat()

    for src in market_rows:
        url = (src.url or "").strip()
        if not url:
            continue

        if url in active_urls:
            kept_ids.append(src.id)
            continue

        if _is_archived_source(src):
            continue

        existing = (src.notes or "").strip()
        extra = (
            f"{ARCHIVE_MARKER} archived_at={now} "
            f"reason=no_longer_in_active_market_catalog"
        )
        src.notes = extra if not existing else f"{existing} | {extra}"
        archived_ids.append(src.id)

    db.commit()

    return {
        "archived_count": len(archived_ids),
        "archived_ids": archived_ids,
        "kept_count": len(kept_ids),
        "kept_ids": kept_ids,
        "active_catalog_url_count": len(active_urls),
    }
