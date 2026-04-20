# backend/app/services/policy_source_service.py
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import or_, select

from app.services.policy_fetch_service import (
    build_fetch_metadata_payload,
    fetch_official_source_with_fallback,
    should_browser_fallback_on_result,
)
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import category_label, expected_rule_universe_for_scope, normalize_categories
from app.policy_models import PolicyCatalogEntry, PolicySource, PolicySourceVersion
from app.services.policy_discovery_service import (
    INVENTORY_CRAWL_PENDING,
    INVENTORY_CRAWL_QUEUED,
    INVENTORY_LIFECYCLE_ACCEPTED,
    INVENTORY_LIFECYCLE_DISCOVERED,
    INVENTORY_LIFECYCLE_PENDING_CRAWL,
    expected_inventory_hints,
    mark_inventory_not_found,
    record_discovery_attempt,
    summarize_inventory_for_scope,
    sync_policy_source_into_inventory,
    upsert_discovery_candidate_inventory,
    upsert_source_inventory_record,
)
from app.services.policy_crawl_service import sync_crawl_result_to_inventory
from app.services.policy_change_detection_service import (
    build_source_change_summary,
    compute_next_retry_due,
    determine_source_refresh_state,
)

from app.services.policy_catalog import catalog_mi_authoritative, catalog_municipalities
from app.services.policy_catalog_admin_service import merged_catalog_for_market as merged_catalog_for_market_admin

AUTHORITY_TIER_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}


DEFAULT_TIMEOUT_SECONDS = 20.0
DISCOVERY_NOTE_MARKER = "[discovered]"
CURATED_NOTE_MARKER = "[curated]"
DEFAULT_DISCOVERY_MAX_CANDIDATES = 24

AUTHORITY_POLICY_BY_TIER: dict[str, dict[str, Any]] = {
    "authoritative_official": {"use_type": "binding", "binding_sufficient": True, "supporting_only": False, "usable": True},
    "approved_official_supporting": {"use_type": "supporting", "binding_sufficient": False, "supporting_only": True, "usable": True},
    "semi_authoritative_operational": {"use_type": "supporting", "binding_sufficient": False, "supporting_only": True, "usable": True},
    "derived_or_inferred": {"use_type": "weak", "binding_sufficient": False, "supporting_only": False, "usable": False},
}

class OfficialSourceValidationError(ValueError):
    pass


def _host_from_url(url: str) -> str:
    host = urlparse(str(url or "").strip()).netloc.strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    return host


def _is_official_host(url: str) -> bool:
    host = _host_from_url(url)
    if not host:
        return False
    if host.endswith(".gov"):
        return True
    if host.endswith(".mi.us"):
        return True
    if host in {
        "ecfr.gov",
        "www.ecfr.gov",
        "federalregister.gov",
        "www.federalregister.gov",
        "hud.gov",
        "www.hud.gov",
        "michigan.gov",
        "www.michigan.gov",
        "legislature.mi.gov",
        "www.legislature.mi.gov",
        "courts.michigan.gov",
        "www.courts.michigan.gov",
    }:
        return True
    return False


def _source_is_validated_official(source: PolicySource) -> bool:
    if source is None:
        return False
    authority_tier = str(getattr(source, "authority_tier", None) or "").strip().lower()
    validation_state = str(getattr(source, "validation_state", None) or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", None) or "").strip().lower()
    http_status = getattr(source, "http_status", None)

    if not _is_official_host(getattr(source, "url", None) or ""):
        return False
    if authority_tier != "authoritative_official":
        return False
    if validation_state in {"unsupported", "conflicting"}:
        return False
    if freshness_status in {"fetch_failed", "error", "blocked"}:
        return False
    if http_status is not None and int(http_status) >= 400:
        return False
    return True


def _catalog_items_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
):
    return merged_catalog_for_market_admin(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )


def _find_catalog_item_for_url(
    db: Session,
    *,
    org_id: Optional[int],
    url: str,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
):
    clean = str(url or "").strip().lower()
    if not clean:
        return None
    for item in _catalog_items_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    ):
        if str(getattr(item, "url", "") or "").strip().lower() == clean:
            return item
    return None


def _catalog_candidates_for_missing_categories(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    missing_categories: list[str],
    focus: str,
) -> list[PolicySourceDiscoveryCandidate]:
    items = _catalog_items_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    wanted = {str(x).strip().lower() for x in (missing_categories or []) if str(x).strip()}
    out: list[PolicySourceDiscoveryCandidate] = []

    for item in items:
        title_text = str(getattr(item, "title", "") or "").lower()
        notes_text = str(getattr(item, "notes", "") or "").lower()
        source_kind = str(getattr(item, "source_kind", "") or "").lower()

        category_hints = sorted({
            category
            for category in wanted
            if (
                category in title_text
                or category in notes_text
                or category_label(category).lower() in title_text
                or category_label(category).lower() in notes_text
                or category.replace("_", " ") in source_kind
            )
        })

        if wanted and not category_hints:
            # keep broad authoritative anchors when nothing narrower exists
            if source_kind not in {"federal_anchor", "state_anchor", "municipal_code", "pha_plan", "municipal_registration"}:
                continue
            category_hints = sorted(wanted)

        source_type = _source_type_from_entry(item)
        authority = _classify_authority_tier(
            url=item.url,
            publisher=item.publisher,
            title=item.title,
            source_type=source_type,
            source_kind=item.source_kind,
        )

        out.append(
            PolicySourceDiscoveryCandidate(
                url=item.url,
                title=str(item.title or category_label(category_hints[0] if category_hints else "policy_source")),
                publisher=_norm_text(item.publisher),
                source_type=source_type,
                category_hints=category_hints or sorted(wanted),
                search_terms=sorted(wanted),
                authority_kind=str(authority["authority_kind"]),
                authority_score=float(authority["authority_score"]),
                authority_tier=str(authority["authority_tier"]),
                authority_rank=int(authority["authority_rank"]),
                authority_class=authority.get("authority_class"),
                authority_reason=authority.get("authority_reason"),
                publication_type=authority.get("publication_type"),
                domain_name=authority.get("domain_name"),
                discovered_via="curated_catalog_selection",
                should_fetch=True,
            )
        )

    deduped: dict[str, PolicySourceDiscoveryCandidate] = {}
    for item in out:
        key = item.url.strip().lower()
        if not key:
            continue
        existing = deduped.get(key)
        if existing is None or item.authority_rank > existing.authority_rank:
            deduped[key] = item

    return sorted(
        deduped.values(),
        key=lambda row: (-int(row.authority_rank), row.url),
    )

@dataclass(frozen=True)
class PolicySourceDiscoveryCandidate:
    url: str
    title: str
    publisher: str | None
    source_type: str
    category_hints: list[str]
    search_terms: list[str]
    authority_kind: str
    authority_score: float
    discovered_via: str
    authority_tier: str = "derived_or_inferred"
    authority_rank: int = 25
    authority_class: str | None = None
    authority_reason: str | None = None
    publication_type: str | None = None
    domain_name: str | None = None
    should_fetch: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "publisher": self.publisher,
            "source_type": self.source_type,
            "category_hints": list(self.category_hints),
            "search_terms": list(self.search_terms),
            "authority_kind": self.authority_kind,
            "authority_score": float(self.authority_score),
            "authority_tier": self.authority_tier,
            "authority_rank": int(self.authority_rank),
            "authority_class": self.authority_class,
            "authority_reason": self.authority_reason,
            "publication_type": self.publication_type,
            "domain_name": self.domain_name,
            "discovered_via": self.discovered_via,
            "should_fetch": bool(self.should_fetch),
        }


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


def _slugify(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw).strip("-")
    return raw or "unknown"


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




def _catalog_entry_key(entry: PolicyCatalogEntry) -> str:
    payload = {
        "url": getattr(entry, "url", None),
        "state": getattr(entry, "state", None),
        "county": getattr(entry, "county", None),
        "city": getattr(entry, "city", None),
        "pha_name": getattr(entry, "pha_name", None),
        "program_type": getattr(entry, "program_type", None),
        "source_kind": getattr(entry, "source_kind", None),
        "title": getattr(entry, "title", None),
        "publisher": getattr(entry, "publisher", None),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()

def _source_name_from_url(url: str) -> str:
    host = urlparse(url).netloc.strip().lower()
    if not host:
        return "unknown_source"
    return host


def _publication_type_from_url(url: str) -> str:
    lower = (url or "").strip().lower()
    if lower.endswith(".pdf"):
        return "official_document"
    if any(token in lower for token in ("ordinance", "code", "statute", "regulation", "ecfr", "federalregister")):
        return "legal_code"
    if any(token in lower for token in ("forms", "packet", "application", "checklist")):
        return "official_form"
    if any(token in lower for token in ("notice", "bulletin", "faq", "program")):
        return "guidance_page"
    return "web_page"


def _authority_rank_for_tier(tier: str) -> int:
    return int(AUTHORITY_TIER_RANKS.get(str(tier or "derived_or_inferred"), 25))


def _classify_authority_tier(*, url: str, publisher: Optional[str] = None, title: Optional[str] = None, source_type: Optional[str] = None, source_kind: Optional[str] = None) -> dict[str, Any]:
    host = urlparse(url).netloc.strip().lower()
    publisher_text = (publisher or "").strip().lower()
    title_text = (title or "").strip().lower()
    source_type_text = (source_type or "").strip().lower()
    source_kind_text = (source_kind or "").strip().lower()
    publication_type = _publication_type_from_url(url)
    authority_tier = "derived_or_inferred"
    authority_class = "private_site"
    authority_kind = "private_site"
    authority_reason = "No government or approved official signal detected."
    authority_score = 0.35
    if host.endswith(".gov") or ".gov." in host or host.endswith(".mi.us") or host.endswith(".us"):
        authority_tier = "authoritative_official"
        authority_class = "official_government"
        authority_kind = "official_government"
        authority_reason = "Official government domain."
        authority_score = 0.99
    elif any(token in source_kind_text for token in ("federal_anchor", "state_anchor", "municipal_code")):
        authority_tier = "authoritative_official"
        authority_class = "legal_primary"
        authority_kind = "catalog_primary"
        authority_reason = "Catalog source kind indicates primary official/legal source."
        authority_score = 0.96
    elif any(token in source_kind_text for token in ("city_program_page", "county_program_page", "state_program_page", "housing_authority", "pha_plan", "pha_guidance")) or "housing authority" in publisher_text or "housing commission" in publisher_text or "housing authority" in title_text:
        authority_tier = "approved_official_supporting"
        authority_class = "official_supporting"
        authority_kind = "official_supporting"
        authority_reason = "Approved supporting official/program source."
        authority_score = 0.86
    elif any(token in source_type_text for token in ("program", "city", "county", "state", "federal")) or host.endswith(".org"):
        authority_tier = "semi_authoritative_operational"
        authority_class = "operational_program" if source_type_text else "organizational"
        authority_kind = authority_class
        authority_reason = "Operational or organizational source, useful but not primary legal authority."
        authority_score = 0.65
    authority_rank = _authority_rank_for_tier(authority_tier)
    return {
        "authority_kind": authority_kind,
        "authority_score": float(round(authority_score, 3)),
        "authority_tier": authority_tier,
        "authority_rank": authority_rank,
        "authority_class": authority_class,
        "authority_reason": authority_reason,
        "publication_type": publication_type,
        "domain_name": host,
        "approved_supporting_source": authority_tier == "approved_official_supporting",
        "semi_authoritative": authority_tier == "semi_authoritative_operational",
        "derived_or_inferred": authority_tier == "derived_or_inferred",
        "is_official": authority_tier == "authoritative_official",
    }

@dataclass(frozen=True)
class PolicyCollectResult:
    source: PolicySource
    changed: bool
    fetch_ok: bool
    fetch_error: str | None = None


def _collect_single_catalog_entry(
    db: Session,
    *,
    entry: PolicyCatalogEntry,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> PolicyCollectResult:
    before = db.scalar(
        select(PolicySource).where(PolicySource.url == entry.url).order_by(PolicySource.id.asc())
    )

    source = ensure_policy_source_from_catalog_entry(
        db,
        entry=entry,
        org_id=org_id,
        focus=focus,
    )

    inventory_hints = expected_inventory_hints(
        state=_norm_state(entry.state),
        county=_norm_lower(entry.county),
        city=_norm_lower(entry.city),
        pha_name=_norm_text(entry.pha_name),
        include_section8=True,
    )
    sync_policy_source_into_inventory(
        db,
        source=source,
        org_id=org_id,
        expected_categories=inventory_hints.get("expected_categories"),
        expected_tiers=inventory_hints.get("expected_tiers"),
        inventory_origin="catalog_sync",
        is_curated=True,
    )

    changed = before is None
    return PolicyCollectResult(
        source=source,
        changed=bool(changed),
        fetch_ok=True,
        fetch_error=None,
    )


def collect_catalog_for_focus(
    db: Session,
    *,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> list[PolicyCollectResult]:
    items = catalog_mi_authoritative(focus=focus)
    out: list[PolicyCollectResult] = []

    for entry in items:
        out.append(
            _collect_single_catalog_entry(
                db,
                entry=entry,
                org_id=org_id,
                focus=focus,
            )
        )

    db.commit()
    return out


def collect_catalog_all_municipalities(
    db: Session,
    *,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    items = catalog_mi_authoritative(focus=focus)
    municipalities = catalog_municipalities(items)

    total_results: list[PolicyCollectResult] = []
    seen_urls: set[str] = set()

    for entry in items:
        url = str(getattr(entry, "url", "") or "").strip().lower()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        total_results.append(
            _collect_single_catalog_entry(
                db,
                entry=entry,
                org_id=org_id,
                focus=focus,
            )
        )

    db.commit()

    return {
        "focus": focus,
        "municipalities": municipalities,
        "count": len(total_results),
        "ok_count": sum(1 for r in total_results if r.fetch_ok),
        "failed_count": sum(1 for r in total_results if not r.fetch_ok),
        "results": [
            {
                "source_id": int(r.source.id),
                "url": r.source.url,
                "changed": bool(r.changed),
                "fetch_ok": bool(r.fetch_ok),
                "fetch_error": r.fetch_error,
            }
            for r in total_results
        ],
    }


def collect_catalog_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCollectResult]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )

    out: list[PolicyCollectResult] = []
    seen_urls: set[str] = set()

    for entry in items:
        url = str(getattr(entry, "url", "") or "").strip().lower()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        out.append(
            _collect_single_catalog_entry(
                db,
                entry=entry,
                org_id=org_id,
                focus=focus,
            )
        )

    db.commit()
    return out


def _expected_universe_for_source_scope(*, state: Optional[str], county: Optional[str], city: Optional[str], pha_name: Optional[str], program_type: Optional[str] = None):
    include_section8 = bool(pha_name or str(program_type or "").strip().lower() == "section8")
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
    )

def collect_url(
    db: Session,
    *,
    org_id: Optional[int],
    url: str,
    state: str,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    notes: Optional[str] = None,
) -> PolicyCollectResult:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    program = _norm_text(program_type)
    clean_url = str(url or "").strip()
    if not clean_url:
        raise ValueError("url is required")

    existing_stmt = select(PolicySource).where(PolicySource.url == clean_url)
    if org_id is None:
        existing_stmt = existing_stmt.where(PolicySource.org_id.is_(None))
    else:
        existing_stmt = existing_stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    existing = db.scalar(existing_stmt.order_by(PolicySource.id.asc()))
    if existing is not None:
        return PolicyCollectResult(source=existing, changed=False, fetch_ok=True, fetch_error=None)

    matched_catalog_item = _find_catalog_item_for_url(
        db,
        org_id=org_id,
        url=clean_url,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    if matched_catalog_item is not None:
        source = ensure_policy_source_from_catalog_entry(
            db,
            entry=matched_catalog_item,
            org_id=org_id,
        )
        db.commit()
        return PolicyCollectResult(source=source, changed=True, fetch_ok=True, fetch_error=None)

    # Allow direct entry only when the host is official and already known through a validated official source.
    if not _is_official_host(clean_url):
        raise OfficialSourceValidationError("direct_source_url_rejected_non_official_host")

    known_scoped_sources = list_sources_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    known_hosts = {
        _host_from_url(getattr(row, "url", "") or "")
        for row in known_scoped_sources
        if _source_is_validated_official(row)
    }
    if _host_from_url(clean_url) not in known_hosts:
        raise OfficialSourceValidationError("direct_source_url_rejected_unapproved_host")

    source_type = "program" if pha or (program and program.lower() == "section8") else ("city" if cty else "county" if cnty else "state")
    authority = _classify_authority_tier(
        url=clean_url,
        publisher=publisher,
        title=title,
        source_type=source_type,
        source_kind=None,
    )

    row = PolicySource(
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type=program,
        publisher=publisher,
        title=title,
        url=clean_url,
        notes=_append_note_marker(notes, CURATED_NOTE_MARKER),
        is_authoritative=bool(authority["authority_tier"] == "authoritative_official"),
        authority_score=float(authority["authority_score"]),
        authority_tier=str(authority["authority_tier"]),
        authority_rank=int(authority["authority_rank"]),
        authority_class=authority.get("authority_class"),
        authority_reason="manual_addition_on_prevalidated_official_host",
        publication_type=authority.get("publication_type"),
        domain_name=authority.get("domain_name"),
        authority_use_type=str(
            AUTHORITY_POLICY_BY_TIER.get(
                str(authority.get("authority_tier") or "derived_or_inferred"),
                AUTHORITY_POLICY_BY_TIER["derived_or_inferred"],
            ).get("use_type") or "weak"
        ),
        freshness_status="unknown",
        freshness_reason="manual_collect_pending_fetch",
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    sync_policy_source_into_inventory(
        db,
        source=row,
        org_id=org_id,
        expected_categories=expected_inventory_hints(
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            include_section8=True,
        ).get("expected_categories"),
        expected_tiers=expected_inventory_hints(
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            include_section8=True,
        ).get("expected_tiers"),
        inventory_origin="manual_collect_prevalidated_host",
        is_curated=True,
    )
    db.commit()

    return PolicyCollectResult(source=row, changed=True, fetch_ok=True, fetch_error=None)

def authority_policy_for_scope(*, authority_tier: Optional[str], state: Optional[str], county: Optional[str], city: Optional[str], pha_name: Optional[str], program_type: Optional[str] = None, normalized_categories: list[str] | None = None) -> dict[str, Any]:
    tier = str(authority_tier or "derived_or_inferred").strip() or "derived_or_inferred"
    tier_policy = dict(AUTHORITY_POLICY_BY_TIER.get(tier, AUTHORITY_POLICY_BY_TIER["derived_or_inferred"]))
    universe = _expected_universe_for_source_scope(state=state, county=county, city=city, pha_name=pha_name, program_type=program_type)
    categories = normalize_categories(list(normalized_categories or []))
    authority_expectations = dict(getattr(universe, "authority_expectations", {}) or {})
    legally_binding = set(normalize_categories(getattr(universe, "legally_binding_categories", []) or []))

    binding_categories: list[str] = []
    supporting_categories: list[str] = []
    unusable_categories: list[str] = []
    for category in categories:
        expected = str(authority_expectations.get(category) or "").strip()
        requires_binding = category in legally_binding or expected == "authoritative_official"
        if tier_policy.get("binding_sufficient"):
            binding_categories.append(category)
        elif tier_policy.get("usable"):
            supporting_categories.append(category)
            if requires_binding:
                unusable_categories.append(category)
        else:
            unusable_categories.append(category)

    return {
        "authority_tier": tier,
        "authority_rank": _authority_rank_for_tier(tier),
        **tier_policy,
        "binding_categories": sorted(set(binding_categories)),
        "supporting_categories": sorted(set(supporting_categories)),
        "unusable_categories": sorted(set(unusable_categories)),
        "legally_binding_categories": sorted(legally_binding.intersection(set(categories))),
        "authority_expectations": {k: authority_expectations[k] for k in categories if k in authority_expectations},
    }


def _apply_authority_policy_to_source(source: PolicySource, *, normalized_categories: list[str] | None = None) -> dict[str, Any]:
    categories = normalize_categories(list(normalized_categories or _json_loads_list(getattr(source, "normalized_categories_json", None))))
    policy = authority_policy_for_scope(
        authority_tier=getattr(source, "authority_tier", None),
        state=getattr(source, "state", None),
        county=getattr(source, "county", None),
        city=getattr(source, "city", None),
        pha_name=getattr(source, "pha_name", None),
        program_type=getattr(source, "program_type", None),
        normalized_categories=categories,
    )
    source.authority_use_type = str(policy.get("use_type") or getattr(source, "authority_use_type", None) or "weak")
    source.authority_policy_json = _json_dumps(policy)
    source.binding_categories_json = _json_dumps(policy.get("binding_categories") or [])
    source.supporting_categories_json = _json_dumps(policy.get("supporting_categories") or [])
    source.unusable_categories_json = _json_dumps(policy.get("unusable_categories") or [])
    return policy

def _fingerprint_for_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _refresh_run_id(*, source_id: int | None = None) -> str:
    suffix = f"{int(source_id)}" if source_id is not None else "unknown"
    return f"source-refresh-{suffix}-{int(_utcnow().timestamp())}"


def _jurisdiction_slug(
    *,
    source_type: str,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    program_type: Optional[str],
) -> str:
    st = state.lower()
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    program = _norm_text(program_type)

    if source_type == "federal":
        return f"federal:{st}"
    if source_type == "state":
        return f"state:{st}"
    if source_type == "county":
        return f"county:{st}:{cnty or 'unknown'}"
    if source_type == "city":
        return f"city:{st}:{cnty or 'unknown'}:{cty or 'unknown'}"
    if source_type == "program":
        base = (pha or program or "program").strip().lower().replace(" ", "-")
        return f"program:{st}:{base}"
    if cnty or cty:
        return f"local:{st}:{cnty or '-'}:{cty or '-'}"
    return st


def _source_type_from_entry(entry: PolicyCatalogEntry) -> str:
    kind = (entry.source_kind or "").strip().lower()
    url = (entry.url or "").strip().lower()
    publisher = (entry.publisher or "").strip().lower()
    title = (entry.title or "").strip().lower()

    if any(token in kind for token in ("federal", "hud", "ecfr", "federal_anchor")):
        return "federal"
    if any(token in kind for token in ("state", "mshda", "state_anchor")):
        return "state"
    if "county" in kind:
        return "county"
    if any(token in kind for token in ("pha", "housing_authority", "voucher", "program")):
        return "program"
    if any(domain in url for domain in ("hud.gov", "ecfr.gov", "federalregister.gov")):
        return "federal"
    if "michigan.gov" in url or "legislature.mi.gov" in url:
        return "state"
    if "housing" in title and "authority" in title:
        return "program"
    if "housing commission" in publisher:
        return "program"
    if entry.city:
        return "city"
    if entry.county:
        return "county"
    return "local"


def _fetch_method_from_url(url: str) -> str:
    lower = (url or "").lower()
    if lower.endswith(".pdf"):
        return "http_pdf"
    if lower.startswith("http://") or lower.startswith("https://"):
        return "http_get"
    return "manual"


def _trust_level(entry: PolicyCatalogEntry) -> float:
    if bool(entry.is_authoritative):
        if entry.priority <= 20:
            return 0.98
        if entry.priority <= 50:
            return 0.92
        return 0.85
    if entry.priority <= 50:
        return 0.70
    return 0.55


def _refresh_interval_days(entry: PolicyCatalogEntry) -> int:
    kind = (entry.source_kind or "").lower()
    if "federal" in kind or "pha" in kind or "voucher" in kind:
        return 14
    if "municipal" in kind or "inspection" in kind or "registration" in kind:
        return 21
    if "state" in kind:
        return 30
    return 30


def _effective_refresh_interval_days(source: PolicySource) -> int:
    try:
        value = int(getattr(source, "refresh_interval_days", 0) or 0)
        if value > 0:
            return value
    except Exception:
        pass
    return 30


def _compute_next_refresh_due_at(source: PolicySource, *, from_dt: Optional[datetime] = None) -> datetime:
    base = from_dt or getattr(source, "last_fetched_at", None) or _utcnow()
    return base + timedelta(days=_effective_refresh_interval_days(source))


def _official_block_retry_due(source: PolicySource, *, base_dt: datetime) -> datetime:
    retry_count = int(getattr(source, "refresh_retry_count", 0) or 0)
    if _is_official_host(getattr(source, "url", "") or ""):
        hours = min(24, max(2, 2 * (retry_count + 1)))
        return base_dt + timedelta(hours=hours)
    return compute_next_retry_due(retry_count=retry_count, base_dt=base_dt)


def _safe_text_from_http_response(resp: httpx.Response) -> str:
    content_type = (resp.headers.get("content-type") or "").lower()
    if "text" in content_type or "json" in content_type or "html" in content_type or "xml" in content_type:
        return resp.text or ""
    try:
        return resp.text or ""
    except Exception:
        return ""


def _sync_registry_defaults(source: PolicySource) -> None:
    if not getattr(source, "source_name", None):
        source.source_name = getattr(source, "publisher", None) or getattr(source, "title", None) or _source_name_from_url(source.url)
    if not getattr(source, "source_type", None):
        source.source_type = "local"
    if not getattr(source, "jurisdiction_slug", None):
        source.jurisdiction_slug = _jurisdiction_slug(
            source_type=str(getattr(source, "source_type", None) or "local"),
            state=_norm_state(getattr(source, "state", None)),
            county=getattr(source, "county", None),
            city=getattr(source, "city", None),
            pha_name=getattr(source, "pha_name", None),
            program_type=getattr(source, "program_type", None),
        )
    if not getattr(source, "fetch_method", None):
        source.fetch_method = _fetch_method_from_url(source.url)
    if not getattr(source, "fingerprint_algo", None):
        source.fingerprint_algo = "sha256"
    if not getattr(source, "registry_status", None):
        source.registry_status = "active"
    authority = _classify_authority_tier(
        url=getattr(source, "url", "") or "",
        publisher=getattr(source, "publisher", None),
        title=getattr(source, "title", None),
        source_type=getattr(source, "source_type", None),
        source_kind=_json_loads_dict(getattr(source, "registry_meta_json", None)).get("source_kind"),
    )
    if not getattr(source, "authority_tier", None):
        source.authority_tier = authority["authority_tier"]
    if not getattr(source, "authority_rank", None):
        source.authority_rank = authority["authority_rank"]
    if not getattr(source, "authority_class", None):
        source.authority_class = authority["authority_class"]
    if not getattr(source, "authority_reason", None):
        source.authority_reason = authority["authority_reason"]
    if not getattr(source, "publication_type", None):
        source.publication_type = authority["publication_type"]
    if not getattr(source, "domain_name", None):
        source.domain_name = authority["domain_name"]
    source.approved_supporting_source = bool(getattr(source, "approved_supporting_source", False) or authority["approved_supporting_source"])
    source.semi_authoritative = bool(getattr(source, "semi_authoritative", False) or authority["semi_authoritative"])
    source.derived_or_inferred = bool(authority["derived_or_inferred"])
    source.authority_score = max(float(getattr(source, "authority_score", 0.0) or 0.0), float(authority["authority_score"]))
    if getattr(source, "next_refresh_due_at", None) is None:
        source.next_refresh_due_at = _compute_next_refresh_due_at(source)
    if getattr(source, "source_metadata_json", None) is None:
        source.source_metadata_json = "{}"
    if getattr(source, "fetch_config_json", None) is None:
        source.fetch_config_json = "{}"
    if getattr(source, "registry_meta_json", None) is None:
        source.registry_meta_json = "{}"
    if getattr(source, "authority_policy_json", None) is None:
        source.authority_policy_json = "{}"
    if getattr(source, "binding_categories_json", None) is None:
        source.binding_categories_json = "[]"
    if getattr(source, "supporting_categories_json", None) is None:
        source.supporting_categories_json = "[]"
    if getattr(source, "unusable_categories_json", None) is None:
        source.unusable_categories_json = "[]"
    _apply_authority_policy_to_source(source)


def _append_note_marker(existing_notes: Optional[str], marker: str) -> str:
    notes = (existing_notes or "").strip()
    if marker.lower() in notes.lower():
        return notes
    if not notes:
        return marker
    return f"{notes} {marker}"


def _base_domains_for_scope(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[dict[str, Any]]:
    state_lower = _norm_state(state).lower()
    county_slug = _slugify(county)
    city_slug = _slugify(city)
    pha_slug = _slugify(pha_name)

    candidates: list[dict[str, Any]] = [
        {"domain": "www.hud.gov", "source_type": "federal", "publisher": "HUD"},
        {"domain": "www.ecfr.gov", "source_type": "federal", "publisher": "eCFR"},
        {"domain": "www.michigan.gov", "source_type": "state", "publisher": "State of Michigan"},
        {"domain": "www.legislature.mi.gov", "source_type": "state", "publisher": "Michigan Legislature"},
    ]

    if county_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.{county_slug}countymi.gov", "source_type": "county", "publisher": f"{county} County"},
                {"domain": f"www.{county_slug}county.gov", "source_type": "county", "publisher": f"{county} County"},
                {"domain": f"www.co.{county_slug}.mi.us", "source_type": "county", "publisher": f"{county} County"},
            ]
        )

    if city_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.cityof{city_slug}.org", "source_type": "city", "publisher": city},
                {"domain": f"www.cityof{city_slug}.com", "source_type": "city", "publisher": city},
                {"domain": f"www.{city_slug}.mi.us", "source_type": "city", "publisher": city},
                {"domain": f"www.{city_slug}{state_lower}.gov", "source_type": "city", "publisher": city},
                {"domain": f"www.ci.{city_slug}.mi.us", "source_type": "city", "publisher": city},
            ]
        )

    if pha_slug != "unknown":
        candidates.extend(
            [
                {"domain": f"www.{pha_slug}.org", "source_type": "program", "publisher": pha_name},
                {"domain": f"www.{pha_slug}.gov", "source_type": "program", "publisher": pha_name},
            ]
        )

    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for candidate in candidates:
        domain = str(candidate.get("domain") or "").strip().lower()
        if not domain or domain in seen:
            continue
        seen.add(domain)
        output.append(candidate)
    return output


def _discovery_terms_for_category(category: str) -> list[str]:
    lookup: dict[str, list[str]] = {
        "registration": ["rental registration", "landlord registration", "registration form"],
        "inspection": ["rental inspection", "inspection checklist", "rental inspection program"],
        "occupancy": ["certificate of occupancy", "occupancy certificate", "occupancy permit"],
        "safety": ["property maintenance code", "housing code", "safety checklist"],
        "lead": ["lead paint", "lead safe", "lead hazard"],
        "permits": ["building permit", "permits", "rehab permit"],
        "section8": ["housing choice voucher", "section 8 landlord", "voucher inspection"],
        "program_overlay": ["administrative plan", "voucher landlord packet", "HAP contract"],
        "documents": ["application packet", "rental forms", "landlord forms"],
        "fees": ["fee schedule", "inspection fees", "license fee"],
        "contacts": ["housing department", "rental inspections", "landlord contact"],
        "rental_license": ["rental license", "rental certificate", "landlord license"],
        "source_of_income": ["source of income", "fair housing", "income discrimination"],
        "zoning": ["zoning code", "land use", "zoning ordinance"],
        "tax": ["property tax", "tax assessor"],
        "utilities": ["water billing", "sewer billing", "utilities"],
    }
    return lookup.get(category, [category_label(category)])


def _paths_for_category(category: str) -> list[str]:
    lookup: dict[str, list[str]] = {
        "registration": [
            "/departments/rental-inspections",
            "/departments/rental-inspections-division",
            "/rental-registration",
            "/landlord-registration",
            "/documents/rental-registration.pdf",
        ],
        "inspection": [
            "/departments/rental-inspections",
            "/inspections/rental",
            "/rental-inspection",
            "/documents/rental-inspection-checklist.pdf",
        ],
        "occupancy": [
            "/certificate-of-occupancy",
            "/occupancy",
            "/building/certificate-of-occupancy",
            "/documents/certificate-of-occupancy.pdf",
        ],
        "safety": [
            "/property-maintenance",
            "/housing-code",
            "/code-enforcement",
            "/documents/property-maintenance-code.pdf",
        ],
        "lead": [
            "/lead",
            "/lead-safe",
            "/documents/lead-safe.pdf",
        ],
        "permits": [
            "/permits",
            "/building/permits",
            "/documents/permit-application.pdf",
        ],
        "section8": [
            "/housing-choice-voucher",
            "/section-8",
            "/landlords/housing-choice-voucher",
            "/documents/hcv-landlord-packet.pdf",
        ],
        "program_overlay": [
            "/administrative-plan",
            "/documents/administrative-plan.pdf",
            "/landlords",
            "/voucher-program",
        ],
        "documents": [
            "/documents",
            "/forms",
            "/landlords/forms",
            "/documents/rental-application-packet.pdf",
        ],
        "fees": [
            "/fees",
            "/documents/fee-schedule.pdf",
            "/rental-fees",
        ],
        "contacts": [
            "/contact",
            "/contact-us",
            "/departments",
            "/housing",
        ],
        "rental_license": [
            "/rental-license",
            "/rental-licensing",
            "/documents/rental-license-application.pdf",
        ],
        "source_of_income": [
            "/fair-housing",
            "/civil-rights",
            "/documents/fair-housing.pdf",
        ],
    }
    return lookup.get(category, ["/"])


def _title_for_candidate(
    *,
    city: Optional[str],
    county: Optional[str],
    pha_name: Optional[str],
    category: str,
    source_type: str,
) -> str:
    scope = city or county or pha_name or "jurisdiction"
    return f"{scope} {category_label(category)} source ({source_type})"


def classify_discovery_authority(
    *,
    url: str,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    source_type: Optional[str] = None,
    source_kind: Optional[str] = None,
) -> dict[str, Any]:
    return _classify_authority_tier(
        url=url,
        publisher=publisher,
        title=title,
        source_type=source_type,
        source_kind=source_kind,
    )


def _probe_discovery_candidate(
    *,
    url: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    fetch_result = fetch_official_source_with_fallback(
        url=url,
        timeout_seconds=timeout_seconds,
        cache_ttl_seconds=900,
    )
    title = fetch_result.get("title")
    if not title:
        title = _extract_title(str(fetch_result.get("html") or ""))
    return {
        "ok": bool(fetch_result.get("ok")),
        "http_status": fetch_result.get("http_status"),
        "content_type": fetch_result.get("content_type"),
        "title": title,
        "fetch_error": fetch_result.get("fetch_error"),
        "fetch_method": fetch_result.get("method"),
        "from_cache": bool(fetch_result.get("from_cache")),
        "final_url": fetch_result.get("url") or url,
    }


def _extract_title(text: str) -> Optional[str]:
    raw = text or ""
    match = re.search(r"<title>\s*(.*?)\s*</title>", raw, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return title or None


def policy_source_origin(source: PolicySource) -> str:
    metadata = _json_loads_dict(getattr(source, "source_metadata_json", None))
    discovery = metadata.get("discovery") if isinstance(metadata.get("discovery"), dict) else {}
    if discovery.get("mode") == "discovered":
        return "discovered"
    notes = (getattr(source, "notes", None) or "").lower()
    if DISCOVERY_NOTE_MARKER.lower() in notes:
        return "discovered"
    return "curated"


def policy_source_discovery_metadata(source: PolicySource) -> dict[str, Any]:
    metadata = _json_loads_dict(getattr(source, "source_metadata_json", None))
    return metadata.get("discovery", {}) if isinstance(metadata.get("discovery"), dict) else {}



def policy_source_needs_refresh(
    source: PolicySource,
    *,
    force: bool = False,
    now: Optional[datetime] = None,
) -> bool:
    if force:
        return True

    now = now or _utcnow()
    status = (getattr(source, "registry_status", None) or "active").strip().lower()
    refresh_state = (getattr(source, "refresh_state", None) or "pending").strip().lower()
    if refresh_state == "blocked":
        return False
    if status not in {"active", "candidate", "warning"}:
        return False

    validation_due_at = getattr(source, "validation_due_at", None)
    if bool(getattr(source, "revalidation_required", False)) and (
        validation_due_at is None or validation_due_at <= now
    ):
        return True

    if getattr(source, "last_fetched_at", None) is None:
        return True

    due_at = getattr(source, "next_refresh_due_at", None)
    if due_at is None:
        due_at = _compute_next_refresh_due_at(source, from_dt=getattr(source, "last_fetched_at", None))
        source.next_refresh_due_at = due_at

    freshness_status = (getattr(source, "freshness_status", None) or "").strip().lower()
    if freshness_status in {"stale", "fetch_failed", "unknown", "error"}:
        return True

    return now >= due_at


def merged_catalog_for_market(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCatalogEntry]:
    from app.services.policy_catalog_admin_service import merged_catalog_for_market as _merged_catalog_for_market

    return _merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )


def ensure_policy_source_from_catalog_entry(
    db: Session,
    *,
    entry: PolicyCatalogEntry,
    org_id: Optional[int],
    focus: str = "se_mi_extended",
) -> PolicySource:
    state = _norm_state(entry.state)
    county = _norm_lower(entry.county)
    city = _norm_lower(entry.city)
    pha_name = _norm_text(entry.pha_name)
    program_type = _norm_text(entry.program_type)

    stmt = select(PolicySource).where(PolicySource.url == entry.url)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    existing = db.scalar(stmt.order_by(PolicySource.id.asc()))

    source_type = _source_type_from_entry(entry)
    authority = _classify_authority_tier(
        url=entry.url,
        publisher=entry.publisher,
        title=entry.title,
        source_type=source_type,
        source_kind=entry.source_kind,
    )
    if existing is None:
        source = PolicySource(
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
            publisher=entry.publisher,
            title=entry.title,
            url=entry.url,
            content_type=None,
            http_status=None,
            retrieved_at=None,
            content_sha256=None,
            raw_path=None,
            extracted_text=None,
            notes=_append_note_marker(entry.notes, CURATED_NOTE_MARKER),
            is_authoritative=bool(entry.is_authoritative),
            authority_score=float(authority["authority_score"]),
            authority_tier=str(authority["authority_tier"]),
            authority_rank=int(authority["authority_rank"]),
            authority_class=authority.get("authority_class"),
            authority_reason=authority.get("authority_reason"),
            publication_type=authority.get("publication_type"),
            domain_name=authority.get("domain_name"),
            approved_supporting_source=bool(authority.get("approved_supporting_source", False)),
            semi_authoritative=bool(authority.get("semi_authoritative", False)),
            derived_or_inferred=bool(authority.get("derived_or_inferred", False)),
            authority_use_type=str(AUTHORITY_POLICY_BY_TIER.get(str(authority.get("authority_tier") or "derived_or_inferred"), AUTHORITY_POLICY_BY_TIER["derived_or_inferred"]).get("use_type") or "weak"),
            authority_policy_json="{}",
            binding_categories_json="[]",
            supporting_categories_json="[]",
            unusable_categories_json="[]",
            normalized_categories_json="[]",
            freshness_status="unknown",
            freshness_reason="not_fetched",
            freshness_checked_at=None,
            published_at=None,
            effective_date=None,
            last_verified_at=None,
            source_name=entry.publisher or entry.title or _source_name_from_url(entry.url),
            source_type=source_type,
            jurisdiction_slug=_jurisdiction_slug(
                source_type=source_type,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                program_type=program_type,
            ),
            fetch_method=_fetch_method_from_url(entry.url),
            trust_level=_trust_level(entry),
            refresh_interval_days=_refresh_interval_days(entry),
            last_fetched_at=None,
            registry_status="active",
            fetch_config_json=_json_dumps({"focus": focus}),
            registry_meta_json=_json_dumps(
                {
                    "catalog_entry_key": _catalog_entry_key(entry),
                    "catalog_entry_url": entry.url,
                    "baseline_url": getattr(entry, "baseline_url", None),
                    "source_kind": entry.source_kind,
                    "priority": entry.priority,
                    "origin_mode": "curated",
                }
            ),
            fingerprint_algo="sha256",
            current_fingerprint=None,
            last_changed_at=None,
            next_refresh_due_at=None,
            last_fetch_error=None,
            last_http_status=None,
            last_seen_same_fingerprint_at=None,
            source_metadata_json=_json_dumps(
                {
                    "discovery": {
                        "mode": "curated",
                        "category_hints": _json_loads_list(getattr(entry, "normalized_categories_json", None)),
                        "authority_kind": "catalog_curated",
                        "authority_tier": authority["authority_tier"],
                        "authority_rank": authority["authority_rank"],
                        "authority_class": authority["authority_class"],
                        "authority_reason": authority["authority_reason"],
                        "publication_type": authority["publication_type"],
                        "domain_name": authority["domain_name"],
                    }
                }
            ),
            last_verified_by_user_id=None,
        )
        _sync_registry_defaults(source)
        _apply_authority_policy_to_source(source, normalized_categories=_json_loads_list(getattr(entry, "normalized_categories_json", None)))
        db.add(source)
        db.flush()
        inventory_hints = expected_inventory_hints(
            state=_norm_state(state),
            county=county,
            city=city,
            pha_name=pha_name,
            include_section8=True,
        )
        normalized_categories = _json_loads_list(getattr(entry, "normalized_categories_json", None))
        upsert_source_inventory_record(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
            url=entry.url,
            title=entry.title,
            publisher=entry.publisher,
            source_type=source_type,
            publication_type=authority.get("publication_type"),
            category_hints=list(normalized_categories),
            search_terms=list(normalized_categories),
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            authority_tier=str(authority.get("authority_tier") or "derived_or_inferred"),
            authority_rank=int(authority.get("authority_rank") or 25),
            authority_score=float(authority.get("authority_score") or 0.35),
            lifecycle_state=INVENTORY_LIFECYCLE_ACCEPTED,
            crawl_status=INVENTORY_CRAWL_QUEUED,
            inventory_origin="catalog_sync",
            policy_source_id=int(source.id),
            is_curated=True,
            is_official_candidate=bool(int(authority.get("authority_rank") or 25) >= 85),
            probe_result={"ok": True, "source": "catalog"},
            metadata={"focus": focus, "catalog_entry_key": _catalog_entry_key(entry)},
        )
        return source

    existing.state = state
    existing.county = county
    existing.city = city
    existing.pha_name = pha_name
    existing.program_type = program_type
    existing.publisher = entry.publisher
    existing.title = entry.title
    existing.notes = _append_note_marker(entry.notes or existing.notes, CURATED_NOTE_MARKER)
    existing.is_authoritative = bool(entry.is_authoritative)
    existing.source_name = entry.publisher or entry.title or existing.source_name or _source_name_from_url(entry.url)
    existing.source_type = source_type
    existing.jurisdiction_slug = _jurisdiction_slug(
        source_type=source_type,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
    )
    existing.fetch_method = existing.fetch_method or _fetch_method_from_url(entry.url)
    existing.trust_level = max(float(existing.trust_level or 0.0), _trust_level(entry))
    existing.authority_score = max(float(existing.authority_score or 0.0), float(authority["authority_score"]))
    existing.authority_tier = authority["authority_tier"]
    existing.authority_rank = authority["authority_rank"]
    existing.authority_class = authority["authority_class"]
    existing.authority_reason = authority["authority_reason"]
    existing.publication_type = authority["publication_type"]
    existing.domain_name = authority["domain_name"]
    existing.approved_supporting_source = bool(authority.get("approved_supporting_source", False))
    existing.semi_authoritative = bool(authority.get("semi_authoritative", False))
    existing.derived_or_inferred = bool(authority.get("derived_or_inferred", False))
    existing.authority_use_type = str(AUTHORITY_POLICY_BY_TIER.get(str(authority.get("authority_tier") or "derived_or_inferred"), AUTHORITY_POLICY_BY_TIER["derived_or_inferred"]).get("use_type") or existing.authority_use_type or "weak")
    existing.refresh_interval_days = max(1, _refresh_interval_days(entry))

    meta = _json_loads_dict(existing.registry_meta_json)
    meta.update(
        {
            "catalog_entry_key": _catalog_entry_key(entry),
                    "catalog_entry_url": entry.url,
            "baseline_url": getattr(entry, "baseline_url", None),
            "source_kind": entry.source_kind,
            "priority": entry.priority,
            "origin_mode": "curated",
        }
    )
    existing.registry_meta_json = _json_dumps(meta)

    source_metadata = _json_loads_dict(existing.source_metadata_json)
    source_metadata["discovery"] = {
        "mode": "curated",
        "category_hints": _json_loads_list(getattr(entry, "normalized_categories_json", None)),
        "authority_kind": "catalog_curated",
    }
    existing.source_metadata_json = _json_dumps(source_metadata)

    _sync_registry_defaults(existing)
    _apply_authority_policy_to_source(existing, normalized_categories=_json_loads_list(getattr(entry, "normalized_categories_json", None)))
    db.flush()
    inventory_hints = expected_inventory_hints(
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )
    normalized_categories = _json_loads_list(getattr(entry, "normalized_categories_json", None))
    upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
        url=existing.url,
        title=existing.title,
        publisher=existing.publisher,
        source_type=existing.source_type,
        publication_type=existing.publication_type,
        category_hints=list(normalized_categories),
        search_terms=list(normalized_categories),
        expected_categories=inventory_hints.get("expected_categories"),
        expected_tiers=inventory_hints.get("expected_tiers"),
        authority_tier=existing.authority_tier,
        authority_rank=existing.authority_rank,
        authority_score=existing.authority_score,
        lifecycle_state=INVENTORY_LIFECYCLE_ACCEPTED,
        crawl_status=INVENTORY_CRAWL_QUEUED,
        inventory_origin="catalog_sync",
        policy_source_id=int(existing.id),
        is_curated=True,
        is_official_candidate=bool(int(existing.authority_rank or 0) >= 85),
        probe_result={"ok": True, "source": "catalog"},
        metadata={"focus": focus, "catalog_entry_key": _catalog_entry_key(entry)},
    )
    return existing


def _existing_source_urls(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> set[str]:
    rows = list_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return {str(row.url or "").strip().lower() for row in rows if getattr(row, "url", None)}


def _candidate_urls_for_scope(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    missing_categories: list[str],
) -> list[PolicySourceDiscoveryCandidate]:
    # URL guessing is intentionally disabled.
    # Discovery now means selecting from curated official catalog entries only.
    return []


def _upsert_discovered_source(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    candidate: PolicySourceDiscoveryCandidate,
    probe_result: dict[str, Any],
    focus: str,
) -> PolicySource:
    stmt = select(PolicySource).where(PolicySource.url == candidate.url)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    existing = db.scalar(stmt.order_by(PolicySource.id.asc()))

    metadata_payload = {
        "discovery": {
            "mode": "discovered",
            "category_hints": list(candidate.category_hints),
            "search_terms": list(candidate.search_terms),
            "authority_kind": candidate.authority_kind,
            "authority_score": candidate.authority_score,
            "authority_tier": candidate.authority_tier,
            "authority_rank": candidate.authority_rank,
            "authority_class": candidate.authority_class,
            "authority_reason": candidate.authority_reason,
            "publication_type": candidate.publication_type,
            "domain_name": candidate.domain_name,
            "discovered_via": candidate.discovered_via,
            "discovered_at": _utcnow().isoformat(),
            "probe_result": {
                "ok": probe_result.get("ok"),
                "http_status": probe_result.get("http_status"),
                "content_type": probe_result.get("content_type"),
                "fetch_error": probe_result.get("fetch_error"),
            },
        }
    }

    registry_meta = {
        "origin_mode": "discovered",
        "focus": focus,
        "category_hints": list(candidate.category_hints),
        "authority_kind": candidate.authority_kind,
        "authority_score": candidate.authority_score,
        "authority_tier": candidate.authority_tier,
        "authority_rank": candidate.authority_rank,
        "authority_class": candidate.authority_class,
        "authority_reason": candidate.authority_reason,
        "publication_type": candidate.publication_type,
        "domain_name": candidate.domain_name,
    }

    if existing is None:
        source = PolicySource(
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
            program_type="section8" if "section8" in set(candidate.category_hints) else None,
            publisher=candidate.publisher,
            title=probe_result.get("title") or candidate.title,
            url=candidate.url,
            content_type=probe_result.get("content_type"),
            http_status=probe_result.get("http_status"),
            retrieved_at=None,
            content_sha256=None,
            raw_path=None,
            extracted_text=None,
            notes=_append_note_marker(None, DISCOVERY_NOTE_MARKER),
            is_authoritative=bool(candidate.authority_score >= 0.85),
            authority_score=float(candidate.authority_score),
            authority_tier=str(candidate.authority_tier),
            authority_rank=int(candidate.authority_rank),
            authority_class=candidate.authority_class,
            authority_reason=candidate.authority_reason,
            publication_type=candidate.publication_type,
            domain_name=candidate.domain_name,
            approved_supporting_source=bool(candidate.authority_tier == "approved_official_supporting"),
            semi_authoritative=bool(candidate.authority_tier == "semi_authoritative_operational"),
            derived_or_inferred=bool(candidate.authority_tier == "derived_or_inferred"),
            authority_use_type=str(AUTHORITY_POLICY_BY_TIER.get(str(candidate.authority_tier or "derived_or_inferred"), AUTHORITY_POLICY_BY_TIER["derived_or_inferred"]).get("use_type") or "weak"),
            authority_policy_json="{}",
            binding_categories_json="[]",
            supporting_categories_json="[]",
            unusable_categories_json="[]",
            normalized_categories_json="[]",
            freshness_status="unknown",
            freshness_reason="discovered_not_fetched",
            freshness_checked_at=_utcnow(),
            published_at=None,
            effective_date=None,
            last_verified_at=None,
            source_name=candidate.publisher or _source_name_from_url(candidate.url),
            source_type=candidate.source_type,
            jurisdiction_slug=_jurisdiction_slug(
                source_type=candidate.source_type,
                state=_norm_state(state),
                county=county,
                city=city,
                pha_name=pha_name,
                program_type="section8" if "section8" in set(candidate.category_hints) else None,
            ),
            fetch_method=_fetch_method_from_url(candidate.url),
            trust_level=float(round(max(0.35, candidate.authority_score), 3)),
            refresh_interval_days=21,
            last_fetched_at=None,
            registry_status="candidate" if not probe_result.get("ok") else "active",
            fetch_config_json=_json_dumps({"focus": focus, "discovery_candidate": True}),
            registry_meta_json=_json_dumps(registry_meta),
            fingerprint_algo="sha256",
            current_fingerprint=None,
            last_changed_at=None,
            next_refresh_due_at=_utcnow(),
            last_fetch_error=probe_result.get("fetch_error"),
            last_http_status=probe_result.get("http_status"),
            last_seen_same_fingerprint_at=None,
            source_metadata_json=_json_dumps(metadata_payload),
            last_verified_by_user_id=None,
        )
        _sync_registry_defaults(source)
        _apply_authority_policy_to_source(source, normalized_categories=list(candidate.category_hints))
        db.add(source)
        db.flush()
        inventory_hints = expected_inventory_hints(
            state=_norm_state(state),
            county=county,
            city=city,
            pha_name=pha_name,
            include_section8=True,
        )
        normalized_categories = _json_loads_list(getattr(source, "normalized_categories_json", None))
        if not normalized_categories:
            normalized_categories = list(candidate.category_hints)
        upsert_source_inventory_record(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
            program_type="section8" if "section8" in set(candidate.category_hints) else None,
            url=source.url,
            title=source.title,
            publisher=source.publisher,
            source_type=source.source_type,
            publication_type=source.publication_type,
            category_hints=list(normalized_categories),
            search_terms=list(candidate.search_terms),
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            authority_tier=str(getattr(source, "authority_tier", None) or candidate.authority_tier or "derived_or_inferred"),
            authority_rank=int(getattr(source, "authority_rank", 0) or candidate.authority_rank or 25),
            authority_score=float(getattr(source, "authority_score", 0.0) or candidate.authority_score or 0.35),
            lifecycle_state=INVENTORY_LIFECYCLE_ACCEPTED,
            crawl_status=INVENTORY_CRAWL_QUEUED,
            inventory_origin="discovered",
            policy_source_id=int(source.id),
            is_curated=False,
            is_official_candidate=bool(int(getattr(source, "authority_rank", 0) or 0) >= 85),
            probe_result=probe_result,
            metadata={"focus": focus, "candidate": candidate.as_dict()},
        )
        return source

    existing.state = _norm_state(state)
    existing.county = _norm_lower(county)
    existing.city = _norm_lower(city)
    existing.pha_name = _norm_text(pha_name)
    existing.publisher = candidate.publisher or existing.publisher
    existing.title = probe_result.get("title") or candidate.title or existing.title
    existing.notes = _append_note_marker(existing.notes, DISCOVERY_NOTE_MARKER)
    existing.source_name = candidate.publisher or existing.source_name or _source_name_from_url(candidate.url)
    existing.source_type = candidate.source_type or existing.source_type
    existing.jurisdiction_slug = _jurisdiction_slug(
        source_type=str(existing.source_type or candidate.source_type or "local"),
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
    )
    existing.fetch_method = existing.fetch_method or _fetch_method_from_url(candidate.url)
    existing.trust_level = max(float(existing.trust_level or 0.0), float(candidate.authority_score))
    existing.is_authoritative = bool(existing.is_authoritative or candidate.authority_score >= 0.85)
    existing.authority_score = max(float(existing.authority_score or 0.0), float(candidate.authority_score))
    existing.authority_tier = candidate.authority_tier or existing.authority_tier
    existing.authority_rank = max(int(getattr(existing, "authority_rank", 0) or 0), int(candidate.authority_rank or 0))
    existing.authority_class = candidate.authority_class or existing.authority_class
    existing.authority_reason = candidate.authority_reason or existing.authority_reason
    existing.publication_type = candidate.publication_type or existing.publication_type
    existing.domain_name = candidate.domain_name or existing.domain_name
    existing.approved_supporting_source = bool(existing.approved_supporting_source or candidate.authority_tier == "approved_official_supporting")
    existing.semi_authoritative = bool(existing.semi_authoritative or candidate.authority_tier == "semi_authoritative_operational")
    existing.derived_or_inferred = bool(candidate.authority_tier == "derived_or_inferred")
    existing.authority_use_type = str(AUTHORITY_POLICY_BY_TIER.get(str(existing.authority_tier or "derived_or_inferred"), AUTHORITY_POLICY_BY_TIER["derived_or_inferred"]).get("use_type") or existing.authority_use_type or "weak")
    existing.registry_status = existing.registry_status or ("candidate" if not probe_result.get("ok") else "active")
    existing.last_fetch_error = probe_result.get("fetch_error") or existing.last_fetch_error
    existing.last_http_status = probe_result.get("http_status") or existing.last_http_status
    existing.content_type = probe_result.get("content_type") or existing.content_type
    existing.freshness_checked_at = _utcnow()
    existing.next_refresh_due_at = _utcnow()

    meta = _json_loads_dict(existing.registry_meta_json)
    meta.update(registry_meta)
    existing.registry_meta_json = _json_dumps(meta)

    source_metadata = _json_loads_dict(existing.source_metadata_json)
    existing_discovery = source_metadata.get("discovery") if isinstance(source_metadata.get("discovery"), dict) else {}
    merged_hints = sorted(set(_json_loads_list(existing_discovery.get("category_hints")) + list(candidate.category_hints)))
    source_metadata["discovery"] = {
        **existing_discovery,
        **metadata_payload["discovery"],
        "category_hints": merged_hints,
    }
    existing.source_metadata_json = _json_dumps(source_metadata)

    _sync_registry_defaults(existing)
    _apply_authority_policy_to_source(existing, normalized_categories=merged_hints)
    db.flush()
    inventory_hints = expected_inventory_hints(
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )
    normalized_categories = _json_loads_list(getattr(entry, "normalized_categories_json", None))
    upsert_source_inventory_record(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=getattr(existing, "program_type", None),
        url=existing.url,
        title=existing.title,
        publisher=existing.publisher,
        source_type=existing.source_type,
        publication_type=existing.publication_type,
        category_hints=list(normalized_categories),
        search_terms=list(normalized_categories),
        expected_categories=inventory_hints.get("expected_categories"),
        expected_tiers=inventory_hints.get("expected_tiers"),
        authority_tier=existing.authority_tier,
        authority_rank=existing.authority_rank,
        authority_score=existing.authority_score,
        lifecycle_state=INVENTORY_LIFECYCLE_ACCEPTED,
        crawl_status=INVENTORY_CRAWL_QUEUED,
        inventory_origin="catalog_sync",
        policy_source_id=int(existing.id),
        is_curated=True,
        is_official_candidate=bool(int(existing.authority_rank or 0) >= 85),
        probe_result={"ok": True, "source": "catalog"},
        metadata={"focus": focus, "catalog_entry_key": _catalog_entry_key(entry)},
    )
    return existing


def build_source_discovery_candidates(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
) -> list[dict[str, Any]]:
    categories = [str(item).strip().lower() for item in (missing_categories or []) if str(item).strip()]
    if not categories:
        return []
    candidates = _candidate_urls_for_scope(
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        missing_categories=categories,
    )
    return [row.as_dict() for row in candidates[: max(1, int(max_candidates))]]


def discover_policy_sources_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    focus: str = "se_mi_extended",
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
    probe: bool = True,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    missing = [str(item).strip().lower() for item in (missing_categories or []) if str(item).strip()]

    inventory_hints = expected_inventory_hints(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )

    if not missing:
        return {
            "ok": True,
            "discovery_triggered": False,
            "reason": "no_missing_categories",
            "mode": "catalog_selection_only",
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
            "missing_categories": [],
            "candidate_count": 0,
            "created_count": 0,
            "existing_count": 0,
            "created_source_ids": [],
            "curated_candidate_count": 0,
            "validated_candidate_count": 0,
            "rejected_candidate_count": 0,
            "guessed_candidate_count": 0,
            "candidates": [],
            "results": [],
            "inventory_summary": summarize_inventory_for_scope(
                db,
                org_id=org_id,
                state=st,
                county=cnty,
                city=cty,
                pha_name=pha,
                program_type="section8" if "section8" in set(missing) else None,
            ),
        }

    # Ensure catalog-backed official sources are present first.
    collected = collect_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )
    collected_by_url = {
        str(getattr(item.source, "url", "") or "").strip().lower(): item
        for item in collected
        if getattr(item, "source", None) is not None
    }

    raw_candidates = _catalog_candidates_for_missing_categories(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        missing_categories=missing,
        focus=focus,
    )

    selected: list[PolicySourceDiscoveryCandidate] = []
    seen_urls: set[str] = set()
    for candidate in raw_candidates:
        key = candidate.url.strip().lower()
        if not key or key in seen_urls:
            continue
        seen_urls.add(key)
        selected.append(candidate)
        if len(selected) >= max(1, int(max_candidates)):
            break

    results: list[dict[str, Any]] = []
    created_source_ids: list[int] = []
    created_count = 0
    existing_count = 0

    for candidate in selected:
        source_row = None
        collected_row = collected_by_url.get(candidate.url.strip().lower())
        if collected_row is not None:
            source_row = collected_row.source
            if collected_row.changed:
                created_count += 1
            else:
                existing_count += 1
        else:
            stmt = select(PolicySource).where(PolicySource.url == candidate.url)
            if org_id is None:
                stmt = stmt.where(PolicySource.org_id.is_(None))
            else:
                stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
            source_row = db.scalar(stmt.order_by(PolicySource.id.asc()))
            if source_row is not None:
                existing_count += 1

        if source_row is not None:
            created_source_ids.append(int(source_row.id))
            sync_policy_source_into_inventory(
                db,
                source=source_row,
                org_id=org_id,
                expected_categories=inventory_hints.get("expected_categories"),
                expected_tiers=inventory_hints.get("expected_tiers"),
                inventory_origin="catalog_selection",
                is_curated=True,
            )

        results.append(
            {
                "url": candidate.url,
                "title": candidate.title,
                "publisher": candidate.publisher,
                "category_hints": list(candidate.category_hints),
                "authority_tier": candidate.authority_tier,
                "authority_rank": candidate.authority_rank,
                "discovered_via": candidate.discovered_via,
                "selected_from": "curated_catalog",
                "accepted": source_row is not None,
                "source_id": int(source_row.id) if source_row is not None else None,
                "fetch_ok": None,
                "fetch_error": None,
            }
        )

    record_discovery_attempt(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type="section8" if "section8" in set(missing) else None,
        query_text=" | ".join(missing) if missing else None,
        searched_categories=missing,
        searched_tiers=inventory_hints.get("expected_tiers"),
        result_urls=[row.url for row in selected],
        attempt_type="catalog_selection",
        status="completed",
        not_found=not bool(selected),
        metadata={
            "focus": focus,
            "probe": False,
            "candidate_count": len(selected),
            "guessed_candidate_count": 0,
            "selection_mode": "curated_only",
        },
    )

    if not selected and missing:
        mark_inventory_not_found(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type="section8" if "section8" in set(missing) else None,
            expected_categories=missing,
            expected_tiers=inventory_hints.get("expected_tiers"),
            search_terms=missing,
            metadata={"focus": focus, "reason": "no_curated_candidates"},
        )

    db.commit()

    return {
        "ok": True,
        "discovery_triggered": True,
        "reason": "missing_categories",
        "mode": "catalog_selection_only",
        "status": "completed",
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "missing_categories": missing,
        "candidate_count": len(selected),
        "created_count": created_count,
        "existing_count": existing_count,
        "created_source_ids": sorted(set(created_source_ids)),
        "curated_candidate_count": len(selected),
        "validated_candidate_count": len(selected),
        "rejected_candidate_count": 0,
        "guessed_candidate_count": 0,
        "candidates": [candidate.as_dict() for candidate in selected],
        "results": results,
        "inventory_summary": summarize_inventory_for_scope(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type="section8" if "section8" in set(missing) else None,
        ),
    }

def queue_policy_source_discovery(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    missing_categories: list[str] | None = None,
    focus: str = "se_mi_extended",
    max_candidates: int = DEFAULT_DISCOVERY_MAX_CANDIDATES,
) -> dict[str, Any]:
    return discover_policy_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        missing_categories=missing_categories,
        focus=focus,
        max_candidates=max_candidates,
        probe=False,
    )


def collect_catalog_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> list[PolicyCollectResult]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )

    results: list[PolicyCollectResult] = []
    seen_urls: set[str] = set()
    inventory_hints = expected_inventory_hints(
        state=_norm_state(state),
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )

    for item in items:
        url = str(getattr(item, "url", "") or "").strip().lower()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        result = _collect_single_catalog_entry(
            db,
            entry=item,
            org_id=org_id,
            focus=focus,
        )
        sync_policy_source_into_inventory(
            db,
            source=result.source,
            org_id=org_id,
            expected_categories=inventory_hints.get("expected_categories"),
            expected_tiers=inventory_hints.get("expected_tiers"),
            inventory_origin="catalog_sync",
            is_curated=True,
        )
        results.append(result)

    db.commit()
    return results

def list_sources_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> list[PolicySource]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicySource).where(PolicySource.state == st)
    if org_id is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))

    rows = list(db.scalars(stmt.order_by(PolicySource.is_authoritative.desc(), PolicySource.id.asc())).all())
    out: list[PolicySource] = []

    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        row_pha = _norm_text(getattr(row, "pha_name", None))

        if row_county is not None and row_county != cnty:
            continue
        if row_city is not None and row_city != cty:
            continue
        if pha is not None and row_pha not in {None, pha}:
            continue

        _sync_registry_defaults(row)
        out.append(row)

    db.commit()
    return out



def get_policy_source_refresh_snapshot(source: PolicySource) -> dict[str, Any]:
    _sync_registry_defaults(source)
    return {
        "source_id": int(getattr(source, "id", 0) or 0),
        "url": getattr(source, "url", None),
        "registry_status": getattr(source, "registry_status", None),
        "freshness_status": getattr(source, "freshness_status", None),
        "authority_tier": getattr(source, "authority_tier", None),
        "authority_rank": getattr(source, "authority_rank", None),
        "authority_class": getattr(source, "authority_class", None),
        "authority_reason": getattr(source, "authority_reason", None),
        "publication_type": getattr(source, "publication_type", None),
        "domain_name": getattr(source, "domain_name", None),
        "approved_supporting_source": bool(getattr(source, "approved_supporting_source", False)),
        "semi_authoritative": bool(getattr(source, "semi_authoritative", False)),
        "derived_or_inferred": bool(getattr(source, "derived_or_inferred", False)),
        "authority_use_type": getattr(source, "authority_use_type", None),
        "authority_policy": _json_loads_dict(getattr(source, "authority_policy_json", None)),
        "binding_categories": _json_loads_list(getattr(source, "binding_categories_json", None)),
        "supporting_categories": _json_loads_list(getattr(source, "supporting_categories_json", None)),
        "unusable_categories": _json_loads_list(getattr(source, "unusable_categories_json", None)),
        "last_fetched_at": getattr(source, "last_fetched_at", None).isoformat() if getattr(source, "last_fetched_at", None) else None,
        "next_refresh_due_at": getattr(source, "next_refresh_due_at", None).isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "current_fingerprint": getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
        "last_changed_at": getattr(source, "last_changed_at", None).isoformat() if getattr(source, "last_changed_at", None) else None,
        "last_http_status": getattr(source, "last_http_status", None),
        "refresh_interval_days": int(getattr(source, "refresh_interval_days", 0) or 0),
        "refresh_state": getattr(source, "refresh_state", None),
        "refresh_status_reason": getattr(source, "refresh_status_reason", None),
        "refresh_blocked_reason": getattr(source, "refresh_blocked_reason", None),
        "revalidation_required": bool(getattr(source, "revalidation_required", False)),
        "validation_due_at": getattr(source, "validation_due_at", None).isoformat() if getattr(source, "validation_due_at", None) else None,
        "last_refresh_attempt_at": getattr(source, "last_refresh_attempt_at", None).isoformat() if getattr(source, "last_refresh_attempt_at", None) else None,
        "last_refresh_completed_at": getattr(source, "last_refresh_completed_at", None).isoformat() if getattr(source, "last_refresh_completed_at", None) else None,
        "current_refresh_run_id": getattr(source, "current_refresh_run_id", None),
        "current_source_version_id": int(getattr(source, "current_source_version_id", 0) or 0) if hasattr(source, "current_source_version_id") and getattr(source, "current_source_version_id", None) is not None else None,
    }



def fetch_policy_source(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    now = _utcnow()
    _sync_registry_defaults(source)
    pre_refresh = get_policy_source_refresh_snapshot(source)
    run_id = _refresh_run_id(source_id=int(getattr(source, "id", 0) or 0))

    if not policy_source_needs_refresh(source, force=force, now=now):
        return {
            "ok": True,
            "source_id": int(source.id),
            "skipped": True,
            "reason": "fresh_enough",
            "changed": False,
            "change_detected": False,
            "change_summary": build_source_change_summary(
                previous_fingerprint=getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
                current_fingerprint=getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None),
                authoritative=bool(int(getattr(source, "authority_rank", 0) or 0) >= 85),
            ),
            "current_fingerprint": getattr(source, "current_fingerprint", None),
            "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
            "refresh_state": getattr(source, "refresh_state", None),
            "status_reason": getattr(source, "refresh_status_reason", None),
            "next_step": "monitor",
            "revalidation_required": bool(getattr(source, "revalidation_required", False)),
            "pre_refresh": pre_refresh,
            "post_refresh": pre_refresh,
        }

    source.current_refresh_run_id = run_id
    source.last_refresh_attempt_at = now
    source.last_state_transition_at = now
    source.refresh_state = "crawling"
    source.refresh_status_reason = "fetch_started"
    source.refresh_blocked_reason = None
    db.add(source)
    db.commit()
    db.refresh(source)

    url = (source.url or "").strip()
    if not url:
        blocked_reason = "missing_url"
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = blocked_reason
        source.freshness_checked_at = now
        source.last_fetch_error = blocked_reason
        source.next_refresh_due_at = _official_block_retry_due(source, base_dt=now)
        source.refresh_state = "blocked"
        source.refresh_status_reason = blocked_reason
        source.refresh_blocked_reason = blocked_reason
        source.last_refresh_completed_at = now
        source.refresh_retry_count = int(getattr(source, "refresh_retry_count", 0) or 0) + 1
        source.last_refresh_outcome_json = _json_dumps({"ok": False, "reason": blocked_reason, "refresh_state": "blocked"})
        db.add(source)
        db.commit()
        post_refresh = get_policy_source_refresh_snapshot(source)
        crawl_sync = sync_crawl_result_to_inventory(
            db,
            source=source,
            fetch_result={
                "ok": False,
                "source_id": int(source.id),
                "fetch_error": blocked_reason,
                "changed": False,
                "change_detected": False,
                "refresh_state": "blocked",
                "status_reason": blocked_reason,
                "next_step": "manual_unblock",
                "revalidation_required": False,
                "comparison_state": "fetch_failed",
                "change_kind": "fetch_failed",
                "raw_path": getattr(source, "raw_path", None),
                "retry_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
            },
        )
        db.commit()
        return {
            "ok": False,
            "source_id": int(source.id),
            "skipped": False,
            "reason": blocked_reason,
            "changed": False,
            "change_detected": False,
            "fetch_error": blocked_reason,
            "refresh_state": "blocked",
            "status_reason": blocked_reason,
            "next_step": "manual_unblock",
            "revalidation_required": False,
            "pre_refresh": pre_refresh,
            "post_refresh": post_refresh,
            "crawl_sync": crawl_sync,
        }

    http_status: int | None = None
    content_type: str | None = None
    extracted_text = ""
    fetch_error: str | None = None
    fetch_meta: dict[str, Any] = {}
    html_body = ""

    previous_current_version = db.scalar(
        select(PolicySourceVersion).where(
            PolicySourceVersion.source_id == int(source.id),
            PolicySourceVersion.is_current.is_(True),
        ).order_by(PolicySourceVersion.retrieved_at.desc(), PolicySourceVersion.id.desc())
    )
    previous_version_id = int(previous_current_version.id) if previous_current_version is not None else None
    previous_fingerprint = getattr(source, "current_fingerprint", None) or getattr(source, "content_sha256", None)

    fetch_result = fetch_official_source_with_fallback(
        url=url,
        timeout_seconds=timeout_seconds,
        cache_ttl_seconds=3600,
    )
    fetch_meta = dict(fetch_result)
    http_status = fetch_result.get("http_status")
    content_type = fetch_result.get("content_type")
    extracted_text = str(fetch_result.get("extracted_text") or "")
    html_body = str(fetch_result.get("html") or "")
    fetch_error = fetch_result.get("fetch_error")

    if not extracted_text and html_body:
        extracted_text = _html_to_text(html_body) if '_html_to_text' in globals() else html_body

    fingerprint = _fingerprint_for_text(extracted_text or "")
    authoritative = bool(int(getattr(source, "authority_rank", 0) or 0) >= 85)

    version = PolicySourceVersion(
        source_id=int(source.id),
        retrieved_at=now,
        http_status=http_status,
        content_sha256=fingerprint[:64] if fingerprint else None,
        raw_path=getattr(source, "raw_path", None),
        content_type=content_type,
        fetch_error=fetch_error,
        extracted_text=extracted_text,
        is_current=True,
    )
    db.add(version)
    db.flush()

    if hasattr(version, "version_metadata_json"):
        version.version_metadata_json = _json_dumps(
            build_fetch_metadata_payload(fetch_meta=fetch_meta, fetched_at=now)
        )
        db.add(version)

    prior_versions = list(
        db.scalars(
            select(PolicySourceVersion).where(
                PolicySourceVersion.source_id == int(source.id),
                PolicySourceVersion.id != int(version.id),
                PolicySourceVersion.is_current.is_(True),
            )
        ).all()
    )
    for row in prior_versions:
        row.is_current = False
        db.add(row)

    retry_due_at = (
        _official_block_retry_due(source, base_dt=now)
        if fetch_error is not None
        else None
    )

    change_summary = build_source_change_summary(
        previous_fingerprint=previous_fingerprint,
        current_fingerprint=fingerprint or None,
        previous_version_id=previous_version_id,
        current_version_id=int(version.id),
        http_status=http_status,
        fetch_error=fetch_error,
        authoritative=authoritative,
        previous_last_changed_at=getattr(source, "last_changed_at", None),
        raw_path=getattr(source, "raw_path", None),
        retry_due_at=retry_due_at,
    )

    state_payload = determine_source_refresh_state(
        fetch_ok=(fetch_error is None),
        change_summary=change_summary,
    )

    if should_browser_fallback_on_result(fetch_meta) and _is_official_host(url):
        state_payload["refresh_state"] = "retrying"
        state_payload["status_reason"] = "blocked_official_requires_retry_or_manual_review"
        state_payload["next_step"] = "retry_fetch"
        state_payload["blocked_reason"] = None
        state_payload["revalidation_required"] = False

    source.http_status = http_status
    source.last_http_status = http_status
    source.content_type = content_type
    source.retrieved_at = now
    source.last_fetched_at = now
    source.extracted_text = extracted_text
    source.content_sha256 = fingerprint[:64] if fingerprint else None
    source.current_fingerprint = fingerprint or None
    if hasattr(source, "current_source_version_id"):
        setattr(source, "current_source_version_id", int(version.id))
    source.freshness_checked_at = now
    source.last_fetch_error = fetch_error
    source.next_refresh_due_at = (
        _compute_next_refresh_due_at(source, from_dt=now)
        if fetch_error is None
        else _official_block_retry_due(source, base_dt=now)
    )
    source.refresh_state = state_payload["refresh_state"]
    source.refresh_status_reason = state_payload["status_reason"]
    source.refresh_blocked_reason = state_payload.get("blocked_reason")
    source.last_refresh_completed_at = now
    source.last_state_transition_at = now
    source.revalidation_required = bool(state_payload.get("revalidation_required", False))
    source.validation_due_at = now if source.revalidation_required else None
    source.refresh_retry_count = 0 if fetch_error is None else int(getattr(source, "refresh_retry_count", 0) or 0) + 1

    source_meta = _json_loads_dict(getattr(source, "source_metadata_json", None))
    source_meta["last_fetch"] = build_fetch_metadata_payload(fetch_meta=fetch_meta, fetched_at=now)
    if html_body and not source_meta.get("last_fetch", {}).get("title"):
        source_meta["last_fetch"]["title"] = _extract_title(html_body)
    source.source_metadata_json = _json_dumps(source_meta)

    if fetch_error is None:
        source.registry_status = "active"
        source.freshness_status = "fresh"
        source.freshness_reason = None
        if change_summary.get("changed"):
            source.last_changed_at = now
        else:
            source.last_seen_same_fingerprint_at = now
    else:
        source.registry_status = "warning"
        source.freshness_status = "fetch_failed"
        source.freshness_reason = fetch_error

    source.last_change_summary_json = _json_dumps(change_summary)
    source.last_refresh_outcome_json = _json_dumps(
        {
            "ok": fetch_error is None,
            "source_version_id": int(version.id),
            "refresh_state": state_payload["refresh_state"],
            "status_reason": state_payload["status_reason"],
            "next_step": state_payload["next_step"],
            "http_status": http_status,
            "fetch_error": fetch_error,
            "change_summary": change_summary,
            "fetch_meta": build_fetch_metadata_payload(fetch_meta=fetch_meta, fetched_at=now),
        }
    )

    db.add(source)
    db.commit()
    db.refresh(source)
    db.refresh(version)

    fetch_payload = {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "source_version_id": int(version.id),
        "previous_version_id": previous_version_id,
        "fetch_error": fetch_error,
        "changed": bool(change_summary.get("changed")),
        "change_detected": bool(change_summary.get("change_detected")),
        "comparison_state": change_summary.get("comparison_state"),
        "change_kind": change_summary.get("change_kind"),
        "actionable_outcome": change_summary.get("actionable_outcome"),
        "change_summary": change_summary,
        "previous_fingerprint": previous_fingerprint,
        "current_fingerprint": fingerprint or None,
        "content_sha256": fingerprint[:64] if fingerprint else None,
        "raw_path": getattr(source, "raw_path", None),
        "http_status": http_status,
        "content_type": content_type,
        "refresh_state": state_payload["refresh_state"],
        "status_reason": state_payload["status_reason"],
        "next_step": state_payload["next_step"],
        "retry_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "revalidation_required": bool(state_payload.get("revalidation_required", False)),
        "fetch_method": fetch_meta.get("method"),
        "from_cache": bool(fetch_meta.get("from_cache")),
        "final_url": fetch_meta.get("url") or url,
    }
    crawl_sync = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_payload)
    db.commit()
    db.refresh(source)
    db.refresh(version)
    post_refresh = get_policy_source_refresh_snapshot(source)

    return {
        "ok": fetch_error is None,
        "source_id": int(source.id),
        "source_version_id": int(version.id),
        "skipped": False,
        "reason": None if fetch_error is None else fetch_error,
        "fetch_error": fetch_error,
        "changed": bool(change_summary.get("changed")),
        "change_detected": bool(change_summary.get("change_detected")),
        "change_summary": change_summary,
        "previous_fingerprint": previous_fingerprint,
        "current_fingerprint": fingerprint or None,
        "comparison_state": change_summary.get("comparison_state"),
        "change_kind": change_summary.get("change_kind"),
        "actionable_outcome": change_summary.get("actionable_outcome"),
        "raw_path": getattr(source, "raw_path", None),
        "http_status": http_status,
        "content_type": content_type,
        "fetch_method": fetch_meta.get("method"),
        "from_cache": bool(fetch_meta.get("from_cache")),
        "next_refresh_due_at": source.next_refresh_due_at.isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "refresh_state": state_payload["refresh_state"],
        "status_reason": state_payload["status_reason"],
        "next_step": state_payload["next_step"],
        "revalidation_required": bool(state_payload.get("revalidation_required", False)),
        "pre_refresh": pre_refresh,
        "post_refresh": post_refresh,
        "crawl_sync": crawl_sync,
    }


def refresh_policy_source_and_detect_changes(
    db: Session,
    *,
    source: PolicySource,
    force: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    result = fetch_policy_source(
        db,
        source=source,
        force=force,
        timeout_seconds=timeout_seconds,
    )
    result["refresh_due"] = policy_source_needs_refresh(source, force=False, now=_utcnow())
    result["self_maintaining"] = bool(result.get("ok")) and ("post_refresh" in result)
    return result

def inventory_summary_for_market(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> dict[str, Any]:
    return summarize_inventory_for_scope(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        program_type=_norm_text(program_type),
    )


# --- Final inventory summary filtering override ---
_inventory_summary_orig = inventory_summary_for_market

def inventory_summary_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> dict[str, Any]:
    payload = dict(_inventory_summary_orig(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
    ) or {})
    rows = list(payload.get("rows") or [])
    filtered = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        lifecycle = str(row.get("lifecycle_state") or "").lower()
        origin = str(row.get("inventory_origin") or "").lower()
        policy_source_id = row.get("policy_source_id")
        refresh_reason = str(row.get("refresh_status_reason") or "").lower()
        url = str(row.get("url") or "")
        if policy_source_id in (None, "", 0) and origin == "discovered":
            continue
        if "name or service not known" in refresh_reason and origin == "discovered":
            continue
        if lifecycle == "failed" and origin == "discovered" and not _is_official_host(url):
            continue
        filtered.append(row)

    if len(filtered) != len(rows):
        payload["rows"] = filtered
        payload["inventory_count"] = len(filtered)
        linked = []
        lifecycle_counts = {}
        crawl_counts = {}
        categories = {}
        authority_use_counts = {}
        for row in filtered:
            psid = row.get("policy_source_id")
            if psid not in (None, "", 0):
                try:
                    linked.append(int(psid))
                except Exception:
                    pass
            lifecycle = str(row.get("lifecycle_state") or "").lower() or "unknown"
            crawl = str(row.get("crawl_status") or "").lower() or "unknown"
            lifecycle_counts[lifecycle] = lifecycle_counts.get(lifecycle, 0) + 1
            crawl_counts[crawl] = crawl_counts.get(crawl, 0) + 1
            aut = str(row.get("authority_use_type") or "").lower() or "unknown"
            authority_use_counts[aut] = authority_use_counts.get(aut, 0) + 1
            for cat in list(row.get("category_hints") or []):
                cat = str(cat).strip().lower()
                if cat:
                    categories[cat] = categories.get(cat, 0) + 1
        payload["linked_source_ids"] = sorted(set(linked))
        payload["lifecycle_counts"] = lifecycle_counts
        payload["crawl_counts"] = crawl_counts
        payload["categories"] = categories
        payload["authority_use_counts"] = authority_use_counts
    return payload


# === Tier 3 support-role overrides ===

def policy_source_service_role() -> dict[str, Any]:
    return {
        "truth_model": "support_role_only",
        "service_role": "source_discovery_refresh_inventory",
        "product_truth_owner": "jurisdiction_health_service",
        "notes": [
            "This service manages source discovery, refresh, crawl health, and inventory snapshots.",
            "It does not decide final compliance truth or safe-to-rely-on status.",
        ],
    }


def policy_source_evidence_family(source: PolicySource) -> str:
    source_type = str(getattr(source, "source_type", None) or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", None) or "").strip().lower()
    notes = str(getattr(source, "notes", None) or "").strip().lower()
    authority_tier = str(getattr(source, "authority_tier", None) or "").strip().lower()
    if "[curated]" in notes or publication_type in {"official_document", "legal_code", "official_form"}:
        return "catalog_or_artifact"
    if source_type in {"federal", "state", "county", "city", "program"} and authority_tier in {"authoritative_official", "approved_official_supporting"}:
        return "official_source"
    if source_type in {"program", "city", "county", "state", "federal"}:
        return "supporting_source"
    return "crawl_discovery"


def policy_source_truth_boundary(source: PolicySource) -> dict[str, Any]:
    family = policy_source_evidence_family(source)
    return {
        "source_id": int(getattr(source, "id", 0) or 0),
        "url": getattr(source, "url", None),
        "source_family": family,
        "truth_model": "support_role_only",
        "service_role": "source_discovery_refresh_inventory",
        "may_inform_freshness": True,
        "may_decide_product_truth": False,
        "requires_validation_elsewhere": True,
    }


_tier3_original_get_policy_source_refresh_snapshot = get_policy_source_refresh_snapshot

def get_policy_source_refresh_snapshot(source: PolicySource) -> dict[str, Any]:
    snapshot = _tier3_original_get_policy_source_refresh_snapshot(source)
    if not isinstance(snapshot, dict):
        snapshot = {"ok": False, "source_id": int(getattr(source, "id", 0) or 0)}
    snapshot["service_role"] = "source_discovery_refresh_inventory"
    snapshot["truth_model"] = "support_role_only"
    snapshot["evidence_family"] = policy_source_evidence_family(source)
    snapshot["truth_boundary"] = policy_source_truth_boundary(source)
    return snapshot


_tier3_original_inventory_summary_for_market = inventory_summary_for_market

def inventory_summary_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
) -> dict[str, Any]:
    summary = _tier3_original_inventory_summary_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
    )
    if not isinstance(summary, dict):
        summary = {"ok": False}
    summary["service_role"] = "source_discovery_refresh_inventory"
    summary["truth_model"] = "support_role_only"
    summary["product_truth_owner"] = "jurisdiction_health_service"
    return summary


def source_support_snapshot_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    rows = list_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    family_counts: dict[str, int] = {}
    refresh_states: dict[str, int] = {}
    for row in rows:
        family = policy_source_evidence_family(row)
        family_counts[family] = family_counts.get(family, 0) + 1
        refresh_state = str(getattr(row, "refresh_state", None) or "unknown").strip().lower()
        refresh_states[refresh_state] = refresh_states.get(refresh_state, 0) + 1
    return {
        "ok": True,
        "truth_model": "support_role_only",
        "service_role": "source_discovery_refresh_inventory",
        "product_truth_owner": "jurisdiction_health_service",
        "state": _norm_state(state),
        "county": _norm_lower(county),
        "city": _norm_lower(city),
        "pha_name": _norm_text(pha_name),
        "source_count": len(rows),
        "source_family_counts": family_counts,
        "refresh_state_counts": refresh_states,
        "sources": [
            {
                "source_id": int(getattr(row, "id", 0) or 0),
                "url": getattr(row, "url", None),
                "source_type": getattr(row, "source_type", None),
                "authority_tier": getattr(row, "authority_tier", None),
                "refresh_state": getattr(row, "refresh_state", None),
                "freshness_status": getattr(row, "freshness_status", None),
                "evidence_family": policy_source_evidence_family(row),
                "truth_boundary": policy_source_truth_boundary(row),
            }
            for row in rows
        ],
    }
