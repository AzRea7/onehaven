# backend/app/services/policy_cleanup_service.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from products.compliance.backend.src.services.policy_sources.source_service import _is_rejected_discovered_source
from app.policy_models import PolicyAssertion, PolicySource
from products.compliance.backend.src.services.policy_sources.catalog_admin_service import merged_catalog_for_market


ARCHIVE_MARKER = "[archived_stale_source]"
NON_PROJECTABLE_NOTE_MARKER = "[governance_excluded]"


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


def _append_note(existing: Optional[str], addition: str) -> str:
    current = (existing or "").strip()
    if addition.lower() in current.lower():
        return current
    return addition if not current else f"{current} | {addition}"

def _is_invalid_discovered_source(src):
    notes = (src.notes or "").lower()

    if "[discovered]" not in notes:
        return False

    if not _is_official_host(src.url):
        return True

    return _looks_like_placeholder_or_access_page(
        title=src.title,
        publisher=src.publisher,
        url=src.url,
        http_status=getattr(src, "http_status", None),
    )

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

        if url in active_urls and not _is_rejected_discovered_source(src):
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

def cleanup_discovered_garbage_sources_for_market(db, state, county, city):
    rows = db.query(PolicySource).filter_by(state=state).all()

    archived = []

    for src in rows:
        if _is_invalid_discovered_source(src):
            src.notes = (src.notes or "") + " [ARCHIVED: rejected_discovery_candidate]"
            archived.append(src.id)

    db.commit()

    return {
        "archived": archived,
        "count": len(archived)
    }

def cleanup_non_projectable_assertions_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

    rows = q.all()

    updated_ids: list[int] = []
    excluded_ids: list[int] = []
    safe_ids: list[int] = []

    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue

        governance_state = (getattr(row, "governance_state", None) or "").strip().lower()
        rule_status = (getattr(row, "rule_status", None) or "").strip().lower()
        coverage_status = (getattr(row, "coverage_status", None) or "").strip().lower()
        review_status = (getattr(row, "review_status", None) or "").strip().lower()
        has_replacement = getattr(row, "replaced_by_assertion_id", None) is not None
        has_superseder = getattr(row, "superseded_by_assertion_id", None) is not None

        if (
            governance_state == "active"
            and rule_status == "active"
            and review_status == "verified"
            and not has_replacement
            and not has_superseder
            and coverage_status not in {"candidate", "partial", "inferred", "conflicting", "stale"}
            and bool(getattr(row, "is_current", False))
        ):
            safe_ids.append(int(row.id))
            continue

        excluded_ids.append(int(row.id))

        if governance_state in {"draft", "replaced"} or rule_status in {"candidate", "draft", "replaced", "superseded", "conflicting", "stale"} or has_replacement or has_superseder:
            row.coverage_status = "stale" if rule_status == "stale" else "candidate"
            row.change_summary = f"{NON_PROJECTABLE_NOTE_MARKER} governance_state={governance_state or 'unknown'} rule_status={rule_status or 'unknown'}"
            updated_ids.append(int(row.id))
            db.add(row)

    db.commit()
    return {
        "safe_count": len(safe_ids),
        "safe_ids": safe_ids,
        "excluded_count": len(excluded_ids),
        "excluded_ids": excluded_ids,
        "updated_count": len(updated_ids),
        "updated_ids": updated_ids,
    }

# --- Story 4.2 additive cleanup overlays ---

def _is_assertion_projectable_now(row: PolicyAssertion) -> bool:
    governance_state = (getattr(row, "governance_state", None) or "").strip().lower()
    rule_status = (getattr(row, "rule_status", None) or "").strip().lower()
    review_status = (getattr(row, "review_status", None) or "").strip().lower()
    validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
    coverage_status = (getattr(row, "coverage_status", None) or "").strip().lower()
    has_replacement = getattr(row, "replaced_by_assertion_id", None) is not None
    has_superseder = getattr(row, "superseded_by_assertion_id", None) is not None
    return bool(
        governance_state == "active"
        and rule_status == "active"
        and review_status == "verified"
        and validation_state == "validated"
        and not has_replacement
        and not has_superseder
        and coverage_status not in {"candidate", "partial", "inferred", "conflicting", "stale", "superseded"}
        and bool(getattr(row, "is_current", False))
    )


def cleanup_non_projectable_assertions_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

    rows = q.all()

    updated_ids: list[int] = []
    excluded_ids: list[int] = []
    safe_ids: list[int] = []
    manual_review_ids: list[int] = []

    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if getattr(row, "pha_name", None) is not None and getattr(row, "pha_name", None) != pha:
            continue

        if _is_assertion_projectable_now(row):
            safe_ids.append(int(row.id))
            continue

        excluded_ids.append(int(row.id))
        validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
        review_status = (getattr(row, "review_status", None) or "").strip().lower()
        if validation_state == "conflicting" or review_status == "needs_manual_review":
            row.coverage_status = "conflicting"
            row.is_current = False
            manual_review_ids.append(int(row.id))
        else:
            rule_status = (getattr(row, "rule_status", None) or "").strip().lower()
            row.coverage_status = "stale" if rule_status == "stale" else ("superseded" if getattr(row, "superseded_by_assertion_id", None) is not None or getattr(row, "replaced_by_assertion_id", None) is not None else "candidate")
        row.change_summary = f"{NON_PROJECTABLE_NOTE_MARKER} governance_state={(getattr(row, 'governance_state', None) or 'unknown')} rule_status={(getattr(row, 'rule_status', None) or 'unknown')}"
        updated_ids.append(int(row.id))
        db.add(row)

    db.commit()
    return {
        "safe_count": len(safe_ids),
        "safe_ids": safe_ids,
        "excluded_count": len(excluded_ids),
        "excluded_ids": excluded_ids,
        "updated_count": len(updated_ids),
        "updated_ids": updated_ids,
        "manual_review_count": len(sorted(set(manual_review_ids))),
        "manual_review_ids": sorted(set(manual_review_ids)),
    }
