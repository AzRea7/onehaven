from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import expected_rule_universe_for_scope, normalize_categories


AUTHORITY_POLICY_BY_TIER: dict[str, dict[str, Any]] = {
    "authoritative_official": {"use_type": "binding", "binding_sufficient": True, "supporting_only": False, "usable": True},
    "approved_official_supporting": {"use_type": "supporting", "binding_sufficient": False, "supporting_only": True, "usable": True},
    "semi_authoritative_operational": {"use_type": "supporting", "binding_sufficient": False, "supporting_only": True, "usable": True},
    "derived_or_inferred": {"use_type": "weak", "binding_sufficient": False, "supporting_only": False, "usable": False},
}
from app.policy_models import PolicyDiscoveryAttempt, PolicySource, PolicySourceInventory
from app.services.policy_change_detection_service import compute_next_retry_due


DEFAULT_DISCOVERY_RETRY_DAYS = 3

INVENTORY_LIFECYCLE_DISCOVERED = "discovered"
INVENTORY_LIFECYCLE_PENDING_CRAWL = "pending_crawl"
INVENTORY_LIFECYCLE_NOT_FOUND = "not_found"
INVENTORY_LIFECYCLE_FAILED = "failed"
INVENTORY_LIFECYCLE_SUPERSEDED = "superseded"
INVENTORY_LIFECYCLE_ACCEPTED = "accepted"

INVENTORY_CRAWL_PENDING = "pending"
INVENTORY_CRAWL_QUEUED = "queued"
INVENTORY_CRAWL_FETCHED = "fetched"
INVENTORY_CRAWL_FAILED = "failed"
INVENTORY_CRAWL_NOT_FOUND = "not_found"


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


def _coerce_dt_like(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _clean_reason(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _resolve_next_refresh_step(fetch_result: dict[str, Any], row: PolicySourceInventory, source: PolicySource) -> str:
    explicit = _clean_reason(fetch_result.get("next_step") or fetch_result.get("next_refresh_step"))
    if explicit:
        return explicit
    source_next = _clean_reason(getattr(source, "next_refresh_step", None))
    if source_next:
        return source_next
    row_next = _clean_reason(getattr(row, "next_refresh_step", None))
    if row_next:
        return row_next
    if bool(fetch_result.get("revalidation_required") or getattr(source, "revalidation_required", False)):
        return "validate"
    if bool(fetch_result.get("change_detected") or fetch_result.get("changed")):
        return "extract"
    return "monitor"

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
        "legally_binding_categories": list(getattr(universe, "legally_binding_categories", []) or []),
        "authority_expectations": dict(getattr(universe, "authority_expectations", {}) or {}),
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
    authority_policy = AUTHORITY_POLICY_BY_TIER.get(str(authority_tier or "derived_or_inferred"), AUTHORITY_POLICY_BY_TIER["derived_or_inferred"])
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
            candidate_origin_type=inventory_origin,
            candidate_status_reason=None,
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
    row.candidate_origin_type = inventory_origin or row.candidate_origin_type
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


def upsert_discovery_candidate_inventory(
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
    probe_result: dict[str, Any] | None = None,
    policy_source_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> PolicySourceInventory:
    now = _utcnow()
    probe = dict(probe_result or {})
    fetch_error = probe.get("fetch_error")
    ok = bool(probe.get("ok"))
    crawl_status = INVENTORY_CRAWL_QUEUED if ok else (INVENTORY_CRAWL_FAILED if fetch_error and fetch_error != "probe_skipped" else INVENTORY_CRAWL_PENDING)
    lifecycle_state = INVENTORY_LIFECYCLE_PENDING_CRAWL if ok else (INVENTORY_LIFECYCLE_DISCOVERED if not fetch_error or fetch_error == "probe_skipped" else INVENTORY_LIFECYCLE_FAILED)
    refresh_state = "healthy" if ok else ("degraded" if fetch_error and fetch_error != "probe_skipped" else "pending")
    next_step = "crawl" if ok else ("retry_probe" if fetch_error and fetch_error != "probe_skipped" else "crawl")
    row = upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
        url=url,
        title=title,
        publisher=publisher,
        source_type=source_type,
        publication_type=publication_type,
        category_hints=category_hints,
        search_terms=search_terms,
        expected_categories=expected_categories,
        expected_tiers=expected_tiers,
        authority_tier=authority_tier,
        authority_rank=authority_rank,
        authority_score=authority_score,
        lifecycle_state=lifecycle_state,
        crawl_status=crawl_status,
        inventory_origin="discovered",
        policy_source_id=policy_source_id,
        is_curated=False,
        is_official_candidate=bool(int(authority_rank or 0) >= 85),
        probe_result=probe,
        metadata=metadata,
    )
    row.last_seen_at = now
    row.candidate_status_reason = None if ok else str(fetch_error or "discovered_not_probed")
    row.refresh_state = refresh_state
    row.refresh_status_reason = None if ok else (str(fetch_error or "discovered_not_probed"))
    row.next_refresh_step = next_step
    row.last_http_status = probe.get("http_status")
    if fetch_error and fetch_error != "probe_skipped":
        row.last_failure_at = now
        row.failure_count = int(row.failure_count or 0) + 1
        row.lifecycle_state = INVENTORY_LIFECYCLE_FAILED
        row.next_search_retry_due_at = compute_next_retry_due(
            retry_count=int(row.failure_count or 0),
            base_dt=now,
            min_hours=12,
            max_days=max(DEFAULT_DISCOVERY_RETRY_DAYS, 14),
        )
        row.next_crawl_due_at = row.next_search_retry_due_at
    else:
        row.next_crawl_due_at = row.next_crawl_due_at or now
        if ok:
            row.lifecycle_state = INVENTORY_LIFECYCLE_PENDING_CRAWL
    meta = _json_loads_dict(getattr(row, "inventory_metadata_json", None))
    meta["last_probe_result"] = probe
    row.inventory_metadata_json = _json_dumps(meta)
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
        lifecycle_state=INVENTORY_LIFECYCLE_ACCEPTED,
        crawl_status=INVENTORY_CRAWL_PENDING if not getattr(source, "last_fetched_at", None) else INVENTORY_CRAWL_FETCHED,
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
    fetch_error = _clean_reason(fetch_result.get("fetch_error") or fetch_result.get("status_reason"))
    changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))
    refresh_state = str(
        fetch_result.get("refresh_state")
        or getattr(source, "refresh_state", None)
        or row.refresh_state
        or ("healthy" if ok else "failed")
    )
    next_step = _resolve_next_refresh_step(fetch_result, row, source)
    validation_due_at = (
        _coerce_dt_like(fetch_result.get("validation_due_at"))
        or _coerce_dt_like(getattr(source, "validation_due_at", None))
        or getattr(row, "validation_due_at", None)
    )
    retry_due_at = (
        _coerce_dt_like(fetch_result.get("retry_due_at"))
        or _coerce_dt_like(getattr(source, "next_refresh_due_at", None))
        or None
    )

    row.policy_source_id = int(getattr(source, "id", 0) or 0) or row.policy_source_id
    row.current_source_version_id = source_version_id or fetch_result.get("source_version_id") or row.current_source_version_id
    row.last_crawled_at = now
    row.last_seen_at = now
    row.last_http_status = fetch_result.get("http_status")
    row.last_error = fetch_error
    row.canonical_fingerprint = (
        fetch_result.get("current_fingerprint")
        or fetch_result.get("content_sha256")
        or row.canonical_fingerprint
    )
    row.refresh_state = refresh_state
    row.refresh_status_reason = fetch_error or getattr(source, "refresh_status_reason", None)
    row.next_refresh_step = next_step
    row.revalidation_required = bool(fetch_result.get("revalidation_required") or getattr(source, "revalidation_required", False))
    row.validation_due_at = validation_due_at
    row.last_state_transition_at = getattr(source, "last_state_transition_at", None) or now
    if changed:
        row.last_change_detected_at = now

    if ok:
        row.crawl_status = INVENTORY_CRAWL_FETCHED
        row.lifecycle_state = INVENTORY_LIFECYCLE_ACCEPTED
        row.accepted_at = row.accepted_at or now
        row.rejected_at = None
        row.last_success_at = now
        row.next_crawl_due_at = retry_due_at or getattr(source, "next_refresh_due_at", None) or (now + timedelta(days=7))
        if row.revalidation_required:
            row.refresh_state = "validating"
            row.next_refresh_step = "validate"
    else:
        row.crawl_status = INVENTORY_CRAWL_FAILED
        row.lifecycle_state = INVENTORY_LIFECYCLE_FAILED
        row.rejected_at = now
        row.last_failure_at = now
        row.failure_count = int(row.failure_count or 0) + 1
        row.next_crawl_due_at = retry_due_at or getattr(source, "next_refresh_due_at", None) or compute_next_retry_due(retry_count=int(row.failure_count or 0), base_dt=now)
        row.next_search_retry_due_at = row.next_crawl_due_at

    meta = _json_loads_dict(row.inventory_metadata_json)
    meta["last_fetch_result"] = dict(fetch_result)
    meta["last_fetch_stateful_summary"] = {
        "ok": ok,
        "fetch_error": fetch_error,
        "changed": changed,
        "refresh_state": row.refresh_state,
        "next_refresh_step": row.next_refresh_step,
        "current_source_version_id": row.current_source_version_id,
        "validation_due_at": row.validation_due_at.isoformat() if row.validation_due_at else None,
    }
    if fetch_result.get("comparison_state"):
        meta["comparison_state"] = fetch_result.get("comparison_state")
    if fetch_result.get("change_kind"):
        meta["change_kind"] = fetch_result.get("change_kind")
    if fetch_result.get("actionable_outcome"):
        meta["actionable_outcome"] = fetch_result.get("actionable_outcome")
    if fetch_result.get("raw_path"):
        meta["raw_path"] = fetch_result.get("raw_path")
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
            lifecycle_state=INVENTORY_LIFECYCLE_NOT_FOUND,
            crawl_status=INVENTORY_CRAWL_NOT_FOUND,
            inventory_origin="search_placeholder",
            metadata=metadata,
        )
        row.searched_not_found_count = int(row.searched_not_found_count or 0) + 1
        row.last_failure_at = now
        row.last_search_retry_at = now
        row.lifecycle_state = INVENTORY_LIFECYCLE_FAILED
        row.next_search_retry_due_at = compute_next_retry_due(
            retry_count=int(row.searched_not_found_count or 0),
            base_dt=now,
            min_hours=24,
            max_days=max(DEFAULT_DISCOVERY_RETRY_DAYS, 14),
        )
        row.next_crawl_due_at = row.next_search_retry_due_at
        row.candidate_status_reason = "searched_not_found"
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
    authority_use_counts: dict[str, int] = {}
    for row in rows:
        lifecycle = (getattr(row, "lifecycle_state", None) or "unknown").strip().lower()
        crawl = (getattr(row, "crawl_status", None) or "unknown").strip().lower()
        lifecycle_counts[lifecycle] = lifecycle_counts.get(lifecycle, 0) + 1
        crawl_counts[crawl] = crawl_counts.get(crawl, 0) + 1
        for cat in normalize_categories(_json_loads_list(getattr(row, "category_hints_json", None))):
            category_map[cat] = category_map.get(cat, 0) + 1
        authority_use = str(getattr(row, "authority_use_type", None) or "weak").strip().lower() or "weak"
        authority_use_counts[authority_use] = authority_use_counts.get(authority_use, 0) + 1
        if getattr(row, "policy_source_id", None) is not None:
            source_ids.append(int(row.policy_source_id))
    return {
        "inventory_count": len(rows),
        "lifecycle_counts": lifecycle_counts,
        "crawl_counts": crawl_counts,
        "categories": category_map,
        "authority_use_counts": {},
        "linked_source_ids": sorted(set(source_ids)),
        "authority_use_counts": authority_use_counts,
        "rows": [
            {
                "id": int(row.id),
                "url": row.canonical_url,
                "title": row.title,
                "publisher": row.publisher,
                "lifecycle_state": row.lifecycle_state,
                "crawl_status": row.crawl_status,
                "inventory_origin": row.inventory_origin,
                "candidate_origin_type": getattr(row, "candidate_origin_type", None),
                "candidate_status_reason": getattr(row, "candidate_status_reason", None),
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
                "authority_use_type": getattr(row, "authority_use_type", None),
                "authority_policy": _json_loads_dict(getattr(row, "authority_policy_json", None)),
                "next_search_retry_due_at": row.next_search_retry_due_at.isoformat() if getattr(row, "next_search_retry_due_at", None) else None,
            }
            for row in rows
        ],
    }

FETCH_MODE_API = "api"
FETCH_MODE_HTML = "html"
FETCH_MODE_PDF = "pdf"
FETCH_MODE_MANUAL_REQUIRED = "manual-required"
FETCH_MODE_UNKNOWN = "unknown"

FETCH_RESOLUTION_FETCHED = "fetched"
FETCH_RESOLUTION_MANUAL_REQUIRED = "manual_required"
FETCH_RESOLUTION_UNRESOLVED = "unresolved"
FETCH_RESOLUTION_BLOCKED = "blocked"
FETCH_RESOLUTION_FAILED = "failed"

_BLOCKED_HTTP_STATUSES = {401, 403, 405, 406, 407, 429, 451}
_PDF_CONTENT_HINTS = ("application/pdf", ".pdf")
_API_PATH_HINTS = ("/api/", "api.")


def _normalize_fetch_mode(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {FETCH_MODE_API, FETCH_MODE_HTML, FETCH_MODE_PDF, FETCH_MODE_MANUAL_REQUIRED}:
        return raw
    return FETCH_MODE_UNKNOWN


def infer_fetch_mode(*, url: str | None, source_kind: str | None = None, publication_type: str | None = None, fetch_mode: str | None = None) -> str:
    explicit = _normalize_fetch_mode(fetch_mode)
    if explicit != FETCH_MODE_UNKNOWN:
        return explicit
    kind = str(source_kind or "").strip().lower()
    publication = str(publication_type or "").strip().lower()
    canonical = canonicalize_url(url or "")
    lower_url = canonical.lower()
    if publication == "pdf" or lower_url.endswith('.pdf'):
        return FETCH_MODE_PDF
    if kind in {"api", "json_api", "rss_api"}:
        return FETCH_MODE_API
    if any(token in lower_url for token in _API_PATH_HINTS):
        return FETCH_MODE_API
    if canonical:
        return FETCH_MODE_HTML
    return FETCH_MODE_UNKNOWN


def classify_fetch_resolution(fetch_result: dict[str, Any]) -> str:
    status = fetch_result.get("http_status")
    error = str(fetch_result.get("fetch_error") or "").lower()
    resolution = str(fetch_result.get("resolution") or "").strip().lower()
    if resolution in {
        FETCH_RESOLUTION_FETCHED,
        FETCH_RESOLUTION_MANUAL_REQUIRED,
        FETCH_RESOLUTION_UNRESOLVED,
        FETCH_RESOLUTION_BLOCKED,
        FETCH_RESOLUTION_FAILED,
    }:
        return resolution
    if fetch_result.get("ok"):
        return FETCH_RESOLUTION_FETCHED
    if status in _BLOCKED_HTTP_STATUSES or any(t in error for t in ["captcha", "forbidden", "blocked", "anti-bot", "manual"]):
        return FETCH_RESOLUTION_MANUAL_REQUIRED if str(fetch_result.get("next_step") or "").strip().lower() == "manual_review" else FETCH_RESOLUTION_BLOCKED
    if error in {"no_source", "missing_url", "not_configured", "manual_required", "probe_skipped"}:
        return FETCH_RESOLUTION_UNRESOLVED if error != "manual_required" else FETCH_RESOLUTION_MANUAL_REQUIRED
    if error:
        return FETCH_RESOLUTION_FAILED
    return FETCH_RESOLUTION_UNRESOLVED


def source_family_expected_modes(category: str | None) -> list[str]:
    normalized = str(category or "").strip().lower()
    mapping = {
        "section8": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "program_overlay": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "inspection": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "rental_license": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "registration": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "occupancy": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "permits": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "fees": [FETCH_MODE_HTML, FETCH_MODE_PDF],
        "contacts": [FETCH_MODE_HTML],
        "documents": [FETCH_MODE_PDF, FETCH_MODE_HTML],
    }
    return list(mapping.get(normalized, [FETCH_MODE_HTML]))


def build_source_family_candidate(
    *,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    program_type: str | None,
    category: str,
    source_url: str | None,
    source_label: str | None,
    source_kind: str | None,
    publisher_name: str | None,
    authority_level: str | None,
    fetch_mode: str | None,
    notes: str | None = None,
    coverage_hint: str | None = None,
    is_official: bool = False,
    is_active: bool = True,
    source_family_id: int | None = None,
    jurisdiction_id: int | None = None,
) -> dict[str, Any]:
    mode = infer_fetch_mode(
        url=source_url,
        source_kind=source_kind,
        publication_type=None,
        fetch_mode=fetch_mode,
    )
    canonical_url = canonicalize_url(source_url or "")
    if not is_active:
        candidate_status = "inactive"
    elif not canonical_url:
        candidate_status = "unresolved"
    elif mode == FETCH_MODE_MANUAL_REQUIRED:
        candidate_status = "manual_required"
    else:
        candidate_status = "candidate"
    return {
        "state": _norm_state(state),
        "county": _norm_lower(county),
        "city": _norm_lower(city),
        "pha_name": _norm_text(pha_name),
        "program_type": _norm_text(program_type),
        "category": str(category or "").strip().lower(),
        "url": canonical_url or None,
        "title": _norm_text(source_label),
        "publisher": _norm_text(publisher_name),
        "source_type": _norm_text(source_kind) or "source_family",
        "publication_type": "pdf" if mode == FETCH_MODE_PDF else ("api" if mode == FETCH_MODE_API else "html"),
        "authority_tier": _norm_text(authority_level),
        "fetch_mode": mode,
        "category_hints": normalize_categories([category]),
        "search_terms": [x for x in [_norm_text(source_label), _norm_text(city), _norm_text(county), _norm_text(state)] if x],
        "candidate_status": candidate_status,
        "notes": _norm_text(notes),
        "coverage_hint": _norm_text(coverage_hint),
        "is_official_candidate": bool(is_official),
        "is_active": bool(is_active),
        "source_family_id": source_family_id,
        "jurisdiction_id": jurisdiction_id,
    }

_DISCOVERY_UNRESOLVED = "unresolved"
_DISCOVERY_MANUAL_REQUIRED = "manual_required"
_DISCOVERY_DISCOVERED = "discovered"


def _safe_import_step2_services():
    try:
        from app.services.jurisdiction_source_family_service import get_source_families_for_jurisdiction
        from app.services.jurisdiction_registry_service import (
            find_jurisdiction_by_id,
            find_jurisdiction_by_slug,
            get_or_create_jurisdiction,
            list_child_jurisdictions,
        )
        return {
            "get_source_families_for_jurisdiction": get_source_families_for_jurisdiction,
            "find_jurisdiction_by_id": find_jurisdiction_by_id,
            "find_jurisdiction_by_slug": find_jurisdiction_by_slug,
            "get_or_create_jurisdiction": get_or_create_jurisdiction,
            "list_child_jurisdictions": list_child_jurisdictions,
        }
    except Exception:
        return {}


def _source_family_rows_for_scope(
    db: Session,
    *,
    jurisdiction_id: int | None = None,
) -> list[Any]:
    services = _safe_import_step2_services()
    getter = services.get("get_source_families_for_jurisdiction")
    if getter is None or jurisdiction_id is None:
        return []
    try:
        return list(getter(db, jurisdiction_id=int(jurisdiction_id), include_inactive=True) or [])
    except Exception:
        return []


def discover_source_family_candidates(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    program_type: str | None = None,
    jurisdiction_id: int | None = None,
    expected_categories: list[str] | None = None,
    expected_tiers: list[str] | None = None,
    commit: bool = False,
) -> dict[str, Any]:
    hints = expected_inventory_hints(state=state, county=county, city=city, pha_name=pha_name, include_section8=True)
    expected_categories = normalize_categories(expected_categories or hints.get("expected_categories") or [])
    expected_tiers = [str(x).strip().lower() for x in (expected_tiers or hints.get("expected_tiers") or []) if str(x).strip()]

    family_rows = _source_family_rows_for_scope(db, jurisdiction_id=jurisdiction_id)
    candidates: list[dict[str, Any]] = []
    discovered_urls: list[str] = []
    missing_categories: list[str] = []
    manual_categories: list[str] = []

    family_by_category: dict[str, list[Any]] = {}
    for row in family_rows:
        category = str(getattr(row, "category", None) or "").strip().lower()
        if not category:
            continue
        family_by_category.setdefault(category, []).append(row)

    for category in expected_categories:
        rows = family_by_category.get(category, [])
        if not rows:
            missing_categories.append(category)
            continue
        active_for_category = False
        for row in rows:
            candidate = build_source_family_candidate(
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                program_type=program_type,
                category=category,
                source_url=getattr(row, "source_url", None),
                source_label=getattr(row, "source_label", None),
                source_kind=getattr(row, "source_kind", None),
                publisher_name=getattr(row, "publisher_name", None),
                authority_level=getattr(row, "authority_level", None),
                fetch_mode=getattr(row, "fetch_mode", None),
                notes=getattr(row, "notes", None),
                coverage_hint=getattr(row, "coverage_hint", None),
                is_official=bool(getattr(row, "is_official", False)),
                is_active=bool(getattr(row, "is_active", True)),
                source_family_id=int(getattr(row, "id", 0) or 0) or None,
                jurisdiction_id=int(getattr(row, "jurisdiction_id", 0) or 0) or jurisdiction_id,
            )
            candidates.append(candidate)
            if candidate.get("url"):
                discovered_urls.append(candidate["url"])
            if candidate.get("candidate_status") == _DISCOVERY_MANUAL_REQUIRED:
                manual_categories.append(category)
            if candidate.get("candidate_status") not in {"inactive", _DISCOVERY_UNRESOLVED}:
                active_for_category = True
                inventory_row = upsert_discovery_candidate_inventory(
                    db,
                    org_id=org_id,
                    state=state,
                    county=county,
                    city=city,
                    pha_name=pha_name,
                    program_type=program_type,
                    url=candidate.get("url") or f"manual-required://{source_inventory_scope_key(state=state, county=county, city=city, pha_name=pha_name, program_type=program_type)}/{category}",
                    title=candidate.get("title"),
                    publisher=candidate.get("publisher"),
                    source_type=candidate.get("source_type"),
                    publication_type=candidate.get("publication_type"),
                    category_hints=candidate.get("category_hints") or [category],
                    search_terms=candidate.get("search_terms") or [],
                    expected_categories=expected_categories,
                    expected_tiers=expected_tiers,
                    authority_tier=candidate.get("authority_tier"),
                    authority_rank=100 if candidate.get("is_official_candidate") else 50,
                    authority_score=1.0 if candidate.get("is_official_candidate") else 0.5,
                    probe_result={
                        "ok": bool(candidate.get("url")) and candidate.get("fetch_mode") != FETCH_MODE_MANUAL_REQUIRED,
                        "fetch_error": "manual_required" if candidate.get("fetch_mode") == FETCH_MODE_MANUAL_REQUIRED else ("missing_url" if not candidate.get("url") else None),
                        "candidate_status": candidate.get("candidate_status"),
                    },
                    metadata={
                        "source_family_id": candidate.get("source_family_id"),
                        "jurisdiction_id": candidate.get("jurisdiction_id"),
                        "fetch_mode": candidate.get("fetch_mode"),
                        "notes": candidate.get("notes"),
                        "coverage_hint": candidate.get("coverage_hint"),
                    },
                )
                if candidate.get("fetch_mode") == FETCH_MODE_MANUAL_REQUIRED:
                    inventory_row.crawl_status = INVENTORY_CRAWL_NOT_FOUND
                    inventory_row.lifecycle_state = INVENTORY_LIFECYCLE_DISCOVERED
                    inventory_row.candidate_status_reason = "manual_required"
                    inventory_row.refresh_state = "manual_only"
                    inventory_row.refresh_status_reason = "manual_required"
                    inventory_row.next_refresh_step = "manual_review"
                    db.add(inventory_row)
        if not active_for_category:
            missing_categories.append(category)

    unresolved_categories = sorted(set(missing_categories))
    if unresolved_categories:
        mark_inventory_not_found(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
            expected_categories=unresolved_categories,
            expected_tiers=expected_tiers,
            search_terms=[x for x in [city, county, state, pha_name] if x],
            metadata={"reason": "no_source_family_mapping", "jurisdiction_id": jurisdiction_id},
        )

    attempt_status = "completed"
    not_found = False
    if not candidates:
        attempt_status = "completed_no_candidates"
        not_found = True
    elif unresolved_categories and len(unresolved_categories) == len(expected_categories):
        attempt_status = "completed_unresolved"
        not_found = True

    record_discovery_attempt(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
        query_text=f"source family discovery for {city or county or state}",
        searched_categories=expected_categories,
        searched_tiers=expected_tiers,
        result_urls=sorted(set(discovered_urls)),
        attempt_type="source_family_discovery",
        status=attempt_status,
        not_found=not_found,
        metadata={
            "jurisdiction_id": jurisdiction_id,
            "manual_categories": sorted(set(manual_categories)),
            "unresolved_categories": unresolved_categories,
            "candidate_count": len(candidates),
        },
    )

    if commit:
        db.commit()
    else:
        db.flush()

    return {
        "ok": True,
        "jurisdiction_id": jurisdiction_id,
        "expected_categories": expected_categories,
        "expected_tiers": expected_tiers,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "manual_required_categories": sorted(set(manual_categories)),
        "unresolved_categories": unresolved_categories,
        "discovered_urls": sorted(set(discovered_urls)),
        "inventory_summary": summarize_inventory_for_scope(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
        ),
    }
