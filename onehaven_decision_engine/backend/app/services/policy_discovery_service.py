from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import expected_rule_universe_for_scope, normalize_categories
from app.policy_models import PolicyDiscoveryAttempt, PolicySource, PolicySourceInventory
from app.services.policy_change_detection_service import compute_next_retry_due


DEFAULT_DISCOVERY_RETRY_DAYS = 3


def _utcnow() -> datetime:
    return datetime.utcnow()


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip().lower()
    return out or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip()
    return out or None


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _json_loads_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _json_loads_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def canonicalize_url(url: str) -> str:
    return (url or "").strip()


def source_inventory_scope_key(*, state: str, county: Optional[str], city: Optional[str], pha_name: Optional[str], program_type: Optional[str]) -> str:
    return "|".join([
        _norm_state(state),
        _norm_lower(county) or "-",
        _norm_lower(city) or "-",
        _norm_text(pha_name) or "-",
        _norm_text(program_type) or "-",
    ])


def inventory_dedupe_key(*, scope_key: str, url: str) -> str:
    raw = f"{scope_key}|{canonicalize_url(url).lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def expected_inventory_hints(*, state: str, county: Optional[str], city: Optional[str], pha_name: Optional[str], include_section8: bool = True) -> dict[str, Any]:
    universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
    )
    return {
        "expected_categories": list(universe.required_categories),
        "expected_tiers": list(universe.jurisdiction_types),
    }


def _find_inventory_row(
    db: Session,
    *,
    org_id: Optional[int],
    scope_key: str,
    canonical_url: str,
) -> PolicySourceInventory | None:
    stmt = select(PolicySourceInventory).where(
        PolicySourceInventory.scope_key == scope_key,
        PolicySourceInventory.canonical_url == canonical_url,
    )
    if org_id is None:
        stmt = stmt.where(PolicySourceInventory.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySourceInventory.org_id == org_id, PolicySourceInventory.org_id.is_(None)))
    return db.scalar(stmt.order_by(PolicySourceInventory.id.asc()))


def upsert_source_inventory_record(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
    url: str,
    title: Optional[str],
    publisher: Optional[str],
    source_type: Optional[str],
    publication_type: Optional[str],
    category_hints: list[str] | None,
    search_terms: list[str] | None,
    expected_categories: list[str] | None,
    expected_tiers: list[str] | None,
    authority_tier: Optional[str] = None,
    authority_rank: int = 0,
    authority_score: float = 0.0,
    lifecycle_state: str = "discovered",
    crawl_status: str = "pending",
    inventory_origin: str = "discovered",
    policy_source_id: int | None = None,
    source_version_id: int | None = None,
    is_curated: bool = False,
    is_official_candidate: bool = False,
    probe_result: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> PolicySourceInventory:
    now = _utcnow()
    canonical_url = canonicalize_url(url)
    scope_key = source_inventory_scope_key(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
    )
    row = _find_inventory_row(db, org_id=org_id, scope_key=scope_key, canonical_url=canonical_url)
    merged_meta = dict(metadata or {})
    if probe_result:
        merged_meta["probe_result"] = dict(probe_result)
    dedupe_key = inventory_dedupe_key(scope_key=scope_key, url=canonical_url)
    domain_name = urlparse(canonical_url).netloc.strip().lower() or None
    expected_categories = normalize_categories(expected_categories or [])
    category_hints = normalize_categories(category_hints or [])
    expected_tiers = [str(x).strip().lower() for x in (expected_tiers or []) if str(x).strip()]
    search_terms = [str(x).strip() for x in (search_terms or []) if str(x).strip()]

    if row is None:
        row = PolicySourceInventory(
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
            program_type=_norm_text(program_type),
            scope_key=scope_key,
            canonical_url=canonical_url,
            domain_name=domain_name,
            title=title,
            publisher=publisher,
            source_type=source_type,
            publication_type=publication_type,
            policy_source_id=policy_source_id,
            current_source_version_id=source_version_id,
            lifecycle_state=lifecycle_state,
            crawl_status=crawl_status,
            inventory_origin=inventory_origin,
            is_curated=bool(is_curated),
            is_official_candidate=bool(is_official_candidate),
            dedupe_key=dedupe_key,
            canonical_fingerprint=None,
            authority_tier=authority_tier,
            authority_rank=int(authority_rank or 0),
            authority_score=float(authority_score or 0.0),
            expected_categories_json=_json_dumps(expected_categories),
            expected_tiers_json=_json_dumps(expected_tiers),
            category_hints_json=_json_dumps(category_hints),
            search_terms_json=_json_dumps(search_terms),
            discovered_at=now,
            last_seen_at=now,
            next_crawl_due_at=now,
            inventory_metadata_json=_json_dumps(merged_meta),
        )
        db.add(row)
        db.flush()
        return row

    row.state = _norm_state(state)
    row.county = _norm_lower(county)
    row.city = _norm_lower(city)
    row.pha_name = _norm_text(pha_name)
    row.program_type = _norm_text(program_type)
    row.title = title or row.title
    row.publisher = publisher or row.publisher
    row.source_type = source_type or row.source_type
    row.publication_type = publication_type or row.publication_type
    row.policy_source_id = policy_source_id or row.policy_source_id
    row.current_source_version_id = source_version_id or row.current_source_version_id
    row.lifecycle_state = lifecycle_state or row.lifecycle_state
    row.crawl_status = crawl_status or row.crawl_status
    row.inventory_origin = inventory_origin or row.inventory_origin
    row.is_curated = bool(row.is_curated or is_curated)
    row.is_official_candidate = bool(row.is_official_candidate or is_official_candidate)
    row.authority_tier = authority_tier or row.authority_tier
    row.authority_rank = max(int(row.authority_rank or 0), int(authority_rank or 0))
    row.authority_score = max(float(row.authority_score or 0.0), float(authority_score or 0.0))
    row.last_seen_at = now
    row.next_crawl_due_at = row.next_crawl_due_at or now
    row.expected_categories_json = _json_dumps(sorted(set(_json_loads_list(row.expected_categories_json) + expected_categories)))
    row.expected_tiers_json = _json_dumps(sorted(set([str(x).strip().lower() for x in _json_loads_list(row.expected_tiers_json)] + expected_tiers)))
    row.category_hints_json = _json_dumps(sorted(set(_json_loads_list(row.category_hints_json) + category_hints)))
    row.search_terms_json = _json_dumps(sorted(set(_json_loads_list(row.search_terms_json) + search_terms)))
    row.inventory_metadata_json = _json_dumps({**_json_loads_dict(row.inventory_metadata_json), **merged_meta})
    db.add(row)
    db.flush()
    return row


def sync_policy_source_into_inventory(
    db: Session,
    *,
    source: PolicySource,
    org_id: Optional[int],
    expected_categories: list[str] | None = None,
    expected_tiers: list[str] | None = None,
    inventory_origin: str = "source_sync",
    is_curated: bool | None = None,
) -> PolicySourceInventory:
    hints = _json_loads_list(getattr(source, "category_hints_json", None))
    if not hints:
        hints = _json_loads_list(getattr(source, "normalized_categories_json", None))
    if not hints:
        meta = _json_loads_dict(getattr(source, "source_metadata_json", None))
        hints = _json_loads_list((meta.get("discovery") or {}).get("category_hints"))
    row = upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=getattr(source, "state", None) or "MI",
        county=getattr(source, "county", None),
        city=getattr(source, "city", None),
        pha_name=getattr(source, "pha_name", None),
        program_type=getattr(source, "program_type", None),
        url=getattr(source, "url", None) or "",
        title=getattr(source, "title", None),
        publisher=getattr(source, "publisher", None),
        source_type=getattr(source, "source_type", None),
        publication_type=getattr(source, "publication_type", None),
        category_hints=hints,
        search_terms=[],
        expected_categories=expected_categories,
        expected_tiers=expected_tiers,
        authority_tier=getattr(source, "authority_tier", None),
        authority_rank=int(getattr(source, "authority_rank", 0) or 0),
        authority_score=float(getattr(source, "authority_score", 0.0) or 0.0),
        lifecycle_state=(getattr(source, "registry_status", None) or "active").lower(),
        crawl_status="pending" if not getattr(source, "last_fetched_at", None) else "fetched",
        inventory_origin=inventory_origin,
        policy_source_id=int(getattr(source, "id", 0) or 0) or None,
        is_curated=bool(getattr(source, "source_origin", "") == "curated") if is_curated is None else bool(is_curated),
        is_official_candidate=bool(getattr(source, "is_authoritative", False) or int(getattr(source, "authority_rank", 0) or 0) >= 85),
        metadata={
            "registry_status": getattr(source, "registry_status", None),
            "freshness_status": getattr(source, "freshness_status", None),
        },
    )
    return row


def record_discovery_attempt(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
    query_text: Optional[str],
    searched_categories: list[str] | None,
    searched_tiers: list[str] | None,
    result_urls: list[str] | None,
    inventory_id: int | None = None,
    policy_source_id: int | None = None,
    attempt_type: str = "discovery",
    status: str = "completed",
    not_found: bool = False,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
    next_retry_due_at: datetime | None = None,
) -> PolicyDiscoveryAttempt:
    now = _utcnow()
    row = PolicyDiscoveryAttempt(
        org_id=org_id,
        inventory_id=inventory_id,
        policy_source_id=policy_source_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        program_type=_norm_text(program_type),
        scope_key=source_inventory_scope_key(state=state, county=county, city=city, pha_name=pha_name, program_type=program_type),
        attempt_type=attempt_type,
        status=status,
        query_text=query_text,
        searched_categories_json=_json_dumps(normalize_categories(searched_categories or [])),
        searched_tiers_json=_json_dumps([str(x).strip().lower() for x in (searched_tiers or []) if str(x).strip()]),
        result_urls_json=_json_dumps([canonicalize_url(x) for x in (result_urls or []) if canonicalize_url(x)]),
        not_found=bool(not_found),
        error_message=error_message,
        started_at=now,
        finished_at=now,
        next_retry_due_at=next_retry_due_at,
        attempt_metadata_json=_json_dumps(metadata or {}),
    )
    db.add(row)
    db.flush()
    return row



def update_inventory_after_fetch(
    db: Session,
    *,
    source: PolicySource,
    fetch_result: dict[str, Any],
    source_version_id: int | None = None,
) -> PolicySourceInventory | None:
    url = canonicalize_url(getattr(source, "url", None) or "")
    if not url:
        return None
    scope_key = source_inventory_scope_key(
        state=getattr(source, "state", None) or "MI",
        county=getattr(source, "county", None),
        city=getattr(source, "city", None),
        pha_name=getattr(source, "pha_name", None),
        program_type=getattr(source, "program_type", None),
    )
    row = _find_inventory_row(db, org_id=getattr(source, "org_id", None), scope_key=scope_key, canonical_url=url)
    if row is None:
        row = sync_policy_source_into_inventory(db, source=source, org_id=getattr(source, "org_id", None))
    now = _utcnow()
    ok = bool(fetch_result.get("ok"))
    row.policy_source_id = int(getattr(source, "id", 0) or 0) or row.policy_source_id
    row.current_source_version_id = source_version_id or row.current_source_version_id
    row.last_crawled_at = now
    row.last_seen_at = now
    row.last_http_status = fetch_result.get("http_status")
    row.last_error = fetch_result.get("fetch_error")
    row.canonical_fingerprint = fetch_result.get("current_fingerprint") or row.canonical_fingerprint
    row.refresh_state = str(fetch_result.get("refresh_state") or getattr(source, "refresh_state", None) or row.refresh_state or "pending")
    row.refresh_status_reason = fetch_result.get("status_reason") or getattr(source, "refresh_status_reason", None)
    row.next_refresh_step = fetch_result.get("next_step") or row.next_refresh_step
    row.revalidation_required = bool(fetch_result.get("revalidation_required") or getattr(source, "revalidation_required", False))
    row.validation_due_at = getattr(source, "validation_due_at", None) or row.validation_due_at
    row.last_state_transition_at = getattr(source, "last_state_transition_at", None) or now
    if fetch_result.get("change_detected") or fetch_result.get("changed"):
        row.last_change_detected_at = now
    if ok:
        row.crawl_status = "fetched"
        row.lifecycle_state = "active"
        row.last_success_at = now
        row.next_crawl_due_at = getattr(source, "next_refresh_due_at", None) or (now + timedelta(days=7))
    else:
        row.crawl_status = "failed"
        row.lifecycle_state = "failed"
        row.last_failure_at = now
        row.failure_count = int(row.failure_count or 0) + 1
        row.next_crawl_due_at = getattr(source, "next_refresh_due_at", None) or compute_next_retry_due(retry_count=int(row.failure_count or 0), base_dt=now)
    meta = _json_loads_dict(row.inventory_metadata_json)
    meta["last_fetch_result"] = dict(fetch_result)
    row.inventory_metadata_json = _json_dumps(meta)
    row.last_refresh_outcome_json = _json_dumps(fetch_result)
    row.last_change_summary_json = _json_dumps(fetch_result.get("change_summary") or {})
    db.add(row)
    db.flush()
    return row


def mark_inventory_not_found(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
    expected_categories: list[str] | None,
    expected_tiers: list[str] | None,
    search_terms: list[str] | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    now = _utcnow()
    for category in normalize_categories(expected_categories or []):
        synthetic_url = f"not-found://{source_inventory_scope_key(state=state, county=county, city=city, pha_name=pha_name, program_type=program_type)}/{category}"
        row = upsert_source_inventory_record(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
            url=synthetic_url,
            title=f"Search placeholder for {category}",
            publisher=None,
            source_type="discovery_placeholder",
            publication_type=None,
            category_hints=[category],
            search_terms=search_terms,
            expected_categories=[category],
            expected_tiers=expected_tiers,
            lifecycle_state="ignored",
            crawl_status="not_found",
            inventory_origin="search_placeholder",
            metadata=metadata,
        )
        row.searched_not_found_count = int(row.searched_not_found_count or 0) + 1
        row.last_failure_at = now
        row.last_search_retry_at = now
        row.next_search_retry_due_at = compute_next_retry_due(
            retry_count=int(row.searched_not_found_count or 0),
            base_dt=now,
            min_hours=24,
            max_days=max(DEFAULT_DISCOVERY_RETRY_DAYS, 14),
        )
        row.next_crawl_due_at = row.next_search_retry_due_at
        row.refresh_state = "pending"
        row.refresh_status_reason = "searched_not_found"
        row.next_refresh_step = "search_retry"
        db.add(row)
    db.flush()


def list_inventory_for_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> list[PolicySourceInventory]:
    stmt = select(PolicySourceInventory).where(
        PolicySourceInventory.state == _norm_state(state),
        PolicySourceInventory.county == _norm_lower(county) if county is not None else PolicySourceInventory.county.is_(None),
        PolicySourceInventory.city == _norm_lower(city) if city is not None else PolicySourceInventory.city.is_(None),
    )
    if pha_name is not None:
        stmt = stmt.where(PolicySourceInventory.pha_name == _norm_text(pha_name))
    if program_type is not None:
        stmt = stmt.where(PolicySourceInventory.program_type == _norm_text(program_type))
    if org_id is None:
        stmt = stmt.where(PolicySourceInventory.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySourceInventory.org_id == org_id, PolicySourceInventory.org_id.is_(None)))
    return list(db.scalars(stmt.order_by(PolicySourceInventory.id.asc())).all())


def summarize_inventory_for_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> dict[str, Any]:
    rows = list_inventory_for_scope(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name, program_type=program_type)
    lifecycle_counts: dict[str, int] = {}
    crawl_counts: dict[str, int] = {}
    category_map: dict[str, int] = {}
    source_ids: list[int] = []
    for row in rows:
        lifecycle = (getattr(row, "lifecycle_state", None) or "unknown").strip().lower()
        crawl = (getattr(row, "crawl_status", None) or "unknown").strip().lower()
        lifecycle_counts[lifecycle] = lifecycle_counts.get(lifecycle, 0) + 1
        crawl_counts[crawl] = crawl_counts.get(crawl, 0) + 1
        for cat in normalize_categories(_json_loads_list(getattr(row, "category_hints_json", None))):
            category_map[cat] = category_map.get(cat, 0) + 1
        if getattr(row, "policy_source_id", None) is not None:
            source_ids.append(int(row.policy_source_id))
    return {
        "inventory_count": len(rows),
        "lifecycle_counts": lifecycle_counts,
        "crawl_counts": crawl_counts,
        "categories": category_map,
        "linked_source_ids": sorted(set(source_ids)),
        "rows": [
            {
                "id": int(row.id),
                "url": row.canonical_url,
                "title": row.title,
                "publisher": row.publisher,
                "lifecycle_state": row.lifecycle_state,
                "crawl_status": row.crawl_status,
                "policy_source_id": row.policy_source_id,
                "expected_categories": _json_loads_list(row.expected_categories_json),
                "category_hints": _json_loads_list(row.category_hints_json),
                "searched_not_found_count": int(row.searched_not_found_count or 0),
                "failure_count": int(row.failure_count or 0),
                "next_crawl_due_at": row.next_crawl_due_at.isoformat() if row.next_crawl_due_at else None,
                "refresh_state": getattr(row, "refresh_state", None),
                "refresh_status_reason": getattr(row, "refresh_status_reason", None),
                "next_refresh_step": getattr(row, "next_refresh_step", None),
                "revalidation_required": bool(getattr(row, "revalidation_required", False)),
                "validation_due_at": row.validation_due_at.isoformat() if getattr(row, "validation_due_at", None) else None,
                "next_search_retry_due_at": row.next_search_retry_due_at.isoformat() if getattr(row, "next_search_retry_due_at", None) else None,
            }
            for row in rows
        ],
    }
