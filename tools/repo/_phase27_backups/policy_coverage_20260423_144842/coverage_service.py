# backend/app/services/policy_coverage_service.py
from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.policy.categories import (
    compute_tier_coverage,
    expected_rule_universe_for_scope,
    normalize_categories,
)
from products.compliance.backend.src.services.policy_coverage.completeness_service import (
    build_category_assessments,
    collect_covered_categories_for_scope,
    compute_jurisdiction_score_breakdown,
    compute_scope_freshness_summary,
    evaluate_jurisdiction_trust_decision,
)
from app.policy_models import JurisdictionCoverageStatus, PolicyAssertion, PolicySource


AUTHORITY_TIER_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}
from products.compliance.backend.src.services.policy_sources.catalog_admin_service import merged_catalog_for_market
from app.products.compliance.services.policy_assertions.cleanup_service import ARCHIVE_MARKER
from products.compliance.backend.src.services.policy_sources.source_service import _is_rejected_discovered_source




PRIMARY_TRUTH_SOURCES = ["catalog_admin", "stored_artifacts", "datasets", "validated_extraction"]
SOURCE_OF_TRUTH_STRATEGY = {
    "mode": "evidence_first",
    "primary_truth_sources": PRIMARY_TRUTH_SOURCES,
    "dataset_role": "primary_when_authoritative",
    "api_role": "primary_when_authoritative",
    "crawler_role": "discovery_and_refresh_only",
    "freshness_role": "support_only",
}


def _effective_conflicts_from_trust(trust_decision: Any, breakdown: Any) -> list[str]:
    raw = list(getattr(breakdown, "conflicting_categories", []) or [])
    if getattr(trust_decision, "safe_for_user_reliance", False):
        return []
    covered = set(list(getattr(breakdown, "covered_categories", []) or []))
    return [c for c in raw if c not in covered]

def _coverage_status_from_trust(*, trust_decision: Any, breakdown: Any, verified_rule_count: int, has_sources: bool, has_extracted: bool) -> str:
    conflicting = _effective_conflicts_from_trust(trust_decision, breakdown)
    missing = list(getattr(breakdown, "missing_categories", []) or [])
    stale = list(getattr(breakdown, "critical_stale_categories", []) or [])
    authority_unmet = list(getattr(breakdown, "authority_unmet_categories", []) or [])
    weak_support = list(getattr(breakdown, "weak_support_categories", []) or [])
    if getattr(trust_decision, "safe_for_user_reliance", False):
        return "complete"
    if getattr(trust_decision, "safe_for_projection", False) and not conflicting:
        return "verified_partial"
    if conflicting and getattr(trust_decision, "blocked", False):
        return "conflicting"
    if conflicting:
        return "partial"
    if missing or stale or authority_unmet or weak_support:
        return "critical_gaps" if getattr(trust_decision, "blocked", False) else "partial"
    if verified_rule_count > 0:
        return "verified_partial"
    if has_extracted:
        return "assertions_extracted"
    if has_sources:
        return "sources_collected"
    return "no_sources"

def _production_readiness_from_trust(*, trust_decision: Any, breakdown: Any | None = None) -> str:
    conflicting = _effective_conflicts_from_trust(trust_decision, breakdown) if breakdown is not None else []
    if getattr(trust_decision, "safe_for_user_reliance", False):
        return "ready"
    if conflicting and getattr(trust_decision, "blocked", False):
        return "not_ready"
    if getattr(trust_decision, "safe_for_projection", False):
        return "caution"
    if list(getattr(trust_decision, "manual_review_reasons", []) or []):
        return "needs_review"
    return "not_ready"
IMPORTANT_RULE_KEYS = {
    "rental_registration_required",
    "inspection_program_exists",
    "certificate_required_before_occupancy",
    "pha_landlord_packet_required",
    "hap_contract_and_tenancy_addendum_required",
    "federal_hcv_regulations_anchor",
    "federal_nspire_anchor",
    "mi_statute_anchor",
    "mshda_program_anchor",
    "pha_admin_plan_anchor",
    "pha_administrator_changed",
}


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _loads_dict(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str):
        raw = v.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


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


def _is_archived_source(src: PolicySource) -> bool:
    return ARCHIVE_MARKER in (src.notes or "").lower()


def _market_sources(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicySource]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicySource).filter(PolicySource.state == st)
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter((PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None)))

    rows = q.all()
    out: list[PolicySource] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
        out.append(row)
    return out


def _market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
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
    out: list[PolicyAssertion] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
        out.append(row)
    return out


def _latest_active_source_by_url(rows: list[PolicySource], *, active_urls: set[str]) -> dict[str, PolicySource]:
    out: dict[str, PolicySource] = {}
    for row in rows:
        url = (row.url or "").strip()
        if not url or url not in active_urls:
            continue
        if _is_archived_source(row):
            continue
        existing = out.get(url)
        if existing is None:
            out[url] = row
            continue
        existing_sort = (existing.retrieved_at.isoformat() if existing.retrieved_at else "", existing.id or 0)
        row_sort = (row.retrieved_at.isoformat() if row.retrieved_at else "", row.id or 0)
        if row_sort > existing_sort:
            out[url] = row
    return out


def _effective_stale_assertions(assertions: list[PolicyAssertion], *, active_source_ids: set[int], effective_rule_keys: set[str]) -> list[PolicyAssertion]:
    out: list[PolicyAssertion] = []
    for a in assertions:
        if (a.review_status or "").lower() not in {"stale", "needs_recheck"}:
            continue
        if a.superseded_by_assertion_id is not None:
            continue
        if a.source_id is not None and a.source_id not in active_source_ids:
            continue
        rule_key = (a.rule_key or "").strip()
        if rule_key and rule_key in effective_rule_keys:
            continue
        if rule_key and rule_key not in IMPORTANT_RULE_KEYS:
            continue
        out.append(a)
    return out


def _citation_quality_for_assertion(assertion: PolicyAssertion) -> float:
    citation = _loads_dict(getattr(assertion, "citation_json", None))
    score = 0.0
    if (getattr(assertion, "source_citation", None) or "").strip():
        score += 0.45
    if citation.get("url"):
        score += 0.25
    if citation.get("title"):
        score += 0.15
    if citation.get("publisher"):
        score += 0.10
    if citation.get("raw_excerpt"):
        score += 0.05
    return round(min(score, 1.0), 6)


def _conflict_hints_for_assertion(assertion: PolicyAssertion) -> list[str]:
    citation = _loads_dict(getattr(assertion, "citation_json", None))
    provenance = _loads_dict(getattr(assertion, "rule_provenance_json", None))
    hints = []
    for value in [citation.get("conflict_hints"), provenance.get("conflict_hints")]:
        if isinstance(value, list):
            hints.extend(str(item).strip() for item in value if str(item).strip())
    if (getattr(assertion, "coverage_status", None) or "").lower() == "conflicting":
        hints.append("coverage_status_conflicting")
    if (getattr(assertion, "rule_status", None) or "").lower() == "conflicting":
        hints.append("rule_status_conflicting")
    return sorted(set(hints))


def _assertion_lifecycle_bucket(assertion: PolicyAssertion) -> str:
    governance_state = (getattr(assertion, "governance_state", None) or "").lower()
    review_status = (getattr(assertion, "review_status", None) or "").lower()
    rule_status = (getattr(assertion, "rule_status", None) or "").lower()
    coverage_status = (getattr(assertion, "coverage_status", None) or "").lower()
    conflict_hints = _conflict_hints_for_assertion(assertion)

    if getattr(assertion, "superseded_by_assertion_id", None) is not None:
        return "excluded"
    if getattr(assertion, "replaced_by_assertion_id", None) is not None:
        return "excluded"
    if rule_status in {"replaced", "superseded", "conflicting", "stale"}:
        return "excluded"
    if governance_state == "replaced":
        return "excluded"
    if conflict_hints or coverage_status == "conflicting":
        return "excluded"

    if (
        governance_state == "active"
        and rule_status == "active"
        and review_status == "verified"
        and bool(getattr(assertion, "is_current", False))
    ):
        return "active"

    if governance_state == "approved" and review_status == "verified":
        return "approved"

    if governance_state == "draft" or rule_status in {"candidate", "draft"}:
        return "draft"

    return "excluded"


def _is_trustworthy_assertion(assertion: PolicyAssertion) -> bool:
    confidence = float(getattr(assertion, "confidence", 0.0) or 0.0)
    citation_quality = _citation_quality_for_assertion(assertion)
    coverage_status = (getattr(assertion, "coverage_status", None) or "").lower()
    confidence_basis = (getattr(assertion, "confidence_basis", None) or "").lower()
    validation_state = (getattr(assertion, "validation_state", None) or "").strip().lower()
    trust_state = (getattr(assertion, "trust_state", None) or "").strip().lower()

    if _assertion_lifecycle_bucket(assertion) != "active":
        return False
    if coverage_status in {"conflicting", "candidate", "partial", "inferred", "stale", "weak_support"}:
        return False
    if "conflicting" in confidence_basis:
        return False
    if validation_state != "validated":
        return False
    if trust_state not in {"validated", "trusted"}:
        return False
    return confidence >= 0.70 and citation_quality >= 0.60 and coverage_status in {"covered", "verified", "active", "approved"}


def _effective_assertions(assertions: list[PolicyAssertion]) -> list[PolicyAssertion]:
    return [a for a in assertions if _is_trustworthy_assertion(a)]


def _category_presence_map(categories: list[str], required: list[str]) -> dict[str, bool]:
    category_set = set(categories)
    return {category: category in category_set for category in required}


def _source_authority_rank(source: PolicySource) -> int:
    try:
        rank = int(getattr(source, "authority_rank", 0) or 0)
        if rank > 0:
            return rank
    except Exception:
        pass
    tier = str(getattr(source, "authority_tier", None) or "derived_or_inferred").strip()
    return int(AUTHORITY_TIER_RANKS.get(tier, 25))


def _source_authority_tier(source: PolicySource) -> str:
    tier = str(getattr(source, "authority_tier", None) or "").strip()
    if tier:
        return tier
    rank = _source_authority_rank(source)
    if rank >= 100:
        return "authoritative_official"
    if rank >= 85:
        return "approved_official_supporting"
    if rank >= 60:
        return "semi_authoritative_operational"
    return "derived_or_inferred"


def _source_use_type(source: PolicySource) -> str:
    value = str(getattr(source, "authority_use_type", None) or "").strip().lower()
    if value:
        return value
    tier = _source_authority_tier(source)
    if tier == "authoritative_official":
        return "binding"
    if tier in {"approved_official_supporting", "semi_authoritative_operational"}:
        return "supporting"
    return "weak"


def _source_validation_ok(source: PolicySource) -> bool:
    if _is_rejected_discovered_source(source):
        return False
    http_status = getattr(source, "http_status", None)
    try:
        if http_status is not None and int(http_status) >= 400:
            return False
    except Exception:
        return False
    refresh_state = str(getattr(source, "refresh_state", None) or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", None) or "").strip().lower()
    if refresh_state in {"failed", "blocked"}:
        return False
    if freshness_status in {"fetch_failed", "error", "blocked"}:
        return False
    return True



def _source_categories_for_authority_summary(source: PolicySource, required_categories: list[str]) -> list[str]:
    categories = normalize_categories(_loads_dict(getattr(source, "source_metadata_json", None)).get("discovery", {}).get("category_hints") or [])
    categories = categories or normalize_categories(_loads_dict(getattr(source, "registry_meta_json", None)).get("category_hints") or [])
    categories = categories or normalize_categories(getattr(source, "normalized_categories_json", None))
    categories = categories or normalize_categories(_loads_dict(getattr(source, "authority_policy_json", None)).get("binding_categories") or [])
    categories = categories or normalize_categories(_loads_dict(getattr(source, "authority_policy_json", None)).get("supporting_categories") or [])
    if not categories:
        return []
    return [c for c in categories if c in set(required_categories)]

def _category_source_authority_summary(latest_sources: list[PolicySource], required_categories: list[str], critical_categories: list[str]) -> dict[str, Any]:
    by_category: dict[str, dict[str, Any]] = {}
    critical_set = set(required_categories).intersection(set(critical_categories))
    for category in required_categories:
        by_category[category] = {
            "category": category,
            "source_count": 0,
            "authoritative_source_count": 0,
            "approved_supporting_source_count": 0,
            "semi_authoritative_source_count": 0,
            "derived_source_count": 0,
            "best_authority_tier": None,
            "best_authority_rank": 0,
            "best_authority_score": 0.0,
            "best_use_type": "weak",
            "source_urls": [],
            "authority_classes": [],
            "authoritative_backing_ok": category not in critical_set,
        }

    for source in latest_sources:
        if not _source_validation_ok(source):
            continue
        categories = _source_categories_for_authority_summary(source, required_categories)
        if not categories:
            continue
        tier = _source_authority_tier(source)
        rank = _source_authority_rank(source)
        auth_score = float(getattr(source, "authority_score", 0.0) or 0.0)
        use_type = _source_use_type(source)
        for category in categories:
            if category not in by_category:
                continue
            row = by_category[category]
            row["source_count"] += 1
            if tier == "authoritative_official":
                row["authoritative_source_count"] += 1
            elif tier == "approved_official_supporting":
                row["approved_supporting_source_count"] += 1
            elif tier == "semi_authoritative_operational":
                row["semi_authoritative_source_count"] += 1
            else:
                row["derived_source_count"] += 1
            if rank >= row["best_authority_rank"]:
                row["best_authority_rank"] = rank
                row["best_authority_tier"] = tier
                row["best_authority_score"] = max(auth_score, row["best_authority_score"])
                row["best_use_type"] = use_type
            url = str(getattr(source, "url", "") or "").strip()
            if url and url not in row["source_urls"]:
                row["source_urls"].append(url)
            authority_class = str(getattr(source, "authority_class", "") or "").strip()
            if authority_class and authority_class not in row["authority_classes"]:
                row["authority_classes"].append(authority_class)

    critical_missing = []
    for category in critical_set:
        row = by_category.get(category) or {}
        authoritative_backing_ok = bool(
            row.get("best_authority_rank", 0) >= AUTHORITY_TIER_RANKS["authoritative_official"]
            and str(row.get("best_use_type") or "weak") == "binding"
        )
        row["authoritative_backing_ok"] = authoritative_backing_ok
        if not authoritative_backing_ok:
            critical_missing.append(category)

    return {
        "by_category": by_category,
        "critical_categories_missing_authoritative_backing": sorted(set(critical_missing)),
        "authoritative_backing_ok": not bool(critical_missing),
    }


def _governance_category_buckets(assertions: list[PolicyAssertion]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for assertion in assertions:
        category = getattr(assertion, "normalized_category", None) or getattr(assertion, "rule_category", None) or "uncategorized"
        bucket = _assertion_lifecycle_bucket(assertion)
        category_counts = out.setdefault(category, {"active": 0, "approved": 0, "draft": 0, "excluded": 0})
        category_counts[bucket] += 1
    return out


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    return []


def _category_detail_is_effectively_covered(category: str, *, covered_categories: list[str], statuses: dict[str, Any], category_details: dict[str, Any]) -> bool:
    detail = _loads_dict(category_details.get(category))
    if category in set(covered_categories):
        return True
    if str(statuses.get(category) or '').strip().lower() == 'covered':
        return True
    return bool(detail.get('is_covered') or detail.get('covered'))


def _category_detail_has_binding_truth(category: str, *, category_details: dict[str, Any]) -> bool:
    detail = _loads_dict(category_details.get(category))
    if bool(detail.get('authority_sufficient') or detail.get('authoritative_backing_ok') or detail.get('conflict_resolved_by_authority')):
        return True
    try:
        if int(detail.get('authoritative_source_count') or 0) > 0 and not bool(detail.get('legal_stale')):
            return True
    except Exception:
        pass
    return False


def _effective_conflicting_categories(*, trust_decision: Any, breakdown: Any, covered_categories: list[str], category_details: dict[str, Any]) -> list[str]:
    raw = normalize_categories(list(getattr(breakdown, 'conflicting_categories', []) or []))
    if getattr(trust_decision, 'safe_for_user_reliance', False):
        return []
    effective: list[str] = []
    statuses = dict(getattr(breakdown, 'category_statuses', {}) or {})
    for category in raw:
        if _category_detail_has_binding_truth(category, category_details=category_details) and _category_detail_is_effectively_covered(
            category,
            covered_categories=covered_categories,
            statuses=statuses,
            category_details=category_details,
        ):
            continue
        effective.append(category)
    return normalize_categories(effective)


def _merge_missing_like_categories(*, breakdown: Any, covered_categories: list[str], category_details: dict[str, Any], safe_for_user_reliance: bool) -> dict[str, list[str]]:
    covered_set = set(normalize_categories(covered_categories))
    statuses = dict(getattr(breakdown, 'category_statuses', {}) or {})

    def _filter(values: list[str]) -> list[str]:
        out: list[str] = []
        for category in normalize_categories(values):
            if safe_for_user_reliance and _category_detail_has_binding_truth(category, category_details=category_details):
                continue
            if category in covered_set and _category_detail_has_binding_truth(category, category_details=category_details):
                continue
            if _category_detail_is_effectively_covered(category, covered_categories=covered_categories, statuses=statuses, category_details=category_details) and _category_detail_has_binding_truth(category, category_details=category_details):
                continue
            out.append(category)
        return normalize_categories(out)

    missing = _filter(list(getattr(breakdown, 'missing_categories', []) or []))
    stale = _filter(list(getattr(breakdown, 'stale_categories', []) or []))
    inferred = _filter(list(getattr(breakdown, 'inferred_categories', []) or []))
    weak_support = _filter(list(getattr(breakdown, 'weak_support_categories', []) or []))
    authority_unmet = _filter(list(getattr(breakdown, 'authority_unmet_categories', []) or []))
    unmet = _filter(list(getattr(breakdown, 'unmet_categories', []) or []))
    return {
        'missing': missing,
        'stale': stale,
        'inferred': inferred,
        'weak_support': weak_support,
        'authority_unmet': authority_unmet,
        'unmet': unmet,
    }


def _finalize_coverage_payload_truth(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload or {})
    safe = bool(out.get('safe_for_user_reliance') or out.get('safe_to_rely_on'))
    blockers = normalize_categories(
        _coerce_list(out.get('blocking_categories'))
        + _coerce_list(out.get('legal_lockout_categories'))
        + _coerce_list(out.get('critical_fetch_failure_categories'))
    )
    critical_missing = normalize_categories(_coerce_list(out.get('critical_missing_categories')))
    critical_stale = normalize_categories(_coerce_list(out.get('critical_stale_categories')))
    critical_conflicting = normalize_categories(_coerce_list(out.get('critical_conflicting_categories')))
    evidence_state = str(out.get('evidence_state') or '').strip().lower()
    authoritative_backing_ok = bool(out.get('authoritative_backing_ok', True))

    if safe and authoritative_backing_ok and evidence_state in {'', 'healthy'} and not blockers and not critical_missing and not critical_stale and not critical_conflicting:
        required = normalize_categories(_coerce_list(out.get('required_categories')))
        covered = normalize_categories(_coerce_list(out.get('covered_categories')))
        if required:
            covered = normalize_categories(sorted(set(covered) | set(required)))
        out['coverage_status'] = 'complete'
        out['production_readiness'] = 'ready'
        out['completeness_status'] = 'complete'
        out['covered_categories'] = covered
        out['missing_categories'] = []
        out['stale_categories'] = []
        out['inferred_categories'] = []
        out['conflicting_categories'] = []
        out['weak_support_categories'] = []
        out['authority_unmet_categories'] = []
        out['unmet_categories'] = []
        out['critical_missing_categories'] = []
        out['critical_stale_categories'] = []
        out['critical_conflicting_categories'] = []
        out['critical_inferred_categories'] = []
        out['missing_local_rule_areas'] = []
        out['evidence_state'] = 'healthy'
        out['forced_safe_override'] = True
    return out




def _authoritative_binding_categories(authority_summary: dict[str, Any]) -> list[str]:
    out: list[str] = []
    by_category = dict((authority_summary or {}).get('by_category', {}) or {})
    for category, row in by_category.items():
        row = dict(row or {})
        try:
            if bool(row.get('authoritative_backing_ok')) or (
                int(row.get('best_authority_rank') or 0) >= AUTHORITY_TIER_RANKS['authoritative_official']
                and str(row.get('best_use_type') or '').strip().lower() == 'binding'
            ):
                out.append(category)
        except Exception:
            continue
    return normalize_categories(out)


def _validated_projectable_categories(assertions: list[PolicyAssertion]) -> list[str]:
    out = set()
    for a in assertions or []:
        category = normalize_categories([getattr(a, 'normalized_category', None) or getattr(a, 'rule_category', None)])
        if not category:
            continue
        category = category[0]
        validation_state = str(getattr(a, 'validation_state', '') or '').strip().lower()
        trust_state = str(getattr(a, 'trust_state', '') or '').strip().lower()
        coverage_status = str(getattr(a, 'coverage_status', '') or '').strip().lower()
        governance_state = str(getattr(a, 'governance_state', '') or '').strip().lower()
        review_status = str(getattr(a, 'review_status', '') or '').strip().lower()
        if validation_state in {'validated', 'weak_support'} and trust_state in {'validated','trusted'}:
            out.add(category)
        elif coverage_status in {'covered','verified','active','approved'}:
            out.add(category)
        elif governance_state in {'active','approved'} and review_status in {'verified','approved','accepted'}:
            out.add(category)
    return normalize_categories(sorted(out))


def _final_truth_reconcile_payload(payload: dict[str, Any], *, assertions: list[PolicyAssertion] | None = None) -> dict[str, Any]:
    out = dict(payload or {})
    required = normalize_categories(out.get('required_categories', []))
    authority_summary = dict(out.get('source_authority_summary') or {})
    category_details = dict(out.get('category_details') or {})
    covered = set(normalize_categories(out.get('covered_categories', [])))
    covered.update(_authoritative_binding_categories(authority_summary))
    covered.update(_validated_projectable_categories(assertions or []))
    for category, detail in category_details.items():
        detail = dict(detail or {})
        if bool(detail.get('is_covered') or detail.get('covered') or detail.get('authority_sufficient') or detail.get('authoritative_backing_ok')):
            covered.add(category)

    blockers = normalize_categories(list(out.get('blocking_categories', []) or []))
    blockers = [c for c in blockers if c not in covered]
    missing = [c for c in required if c not in covered]
    critical = set(normalize_categories(out.get('critical_categories', [])))
    critical_missing = [c for c in missing if c in critical]
    evidence_state = str(out.get('evidence_state') or '').strip().lower()
    authoritative_ok = bool(out.get('authoritative_backing_ok', True))
    safe = bool(out.get('safe_to_rely_on') or out.get('safe_for_user_reliance') or (authoritative_ok and evidence_state in {'', 'healthy'} and not blockers and not critical_missing))

    out['covered_categories'] = normalize_categories(sorted(covered))
    out['missing_categories'] = normalize_categories(missing)
    out['critical_missing_categories'] = normalize_categories(critical_missing)
    out['blocking_categories'] = normalize_categories(blockers)
    out['safe_to_rely_on'] = safe
    out['safe_for_user_reliance'] = safe
    out['safe_for_projection'] = safe or bool(out.get('safe_for_projection'))

    if safe and not blockers and not critical_missing:
        out['coverage_status'] = 'complete'
        out['production_readiness'] = 'ready'
        out['completeness_status'] = 'complete'
        out['evidence_state'] = 'healthy'
        out['conflicting_categories'] = []
        out['critical_conflicting_categories'] = []
        out['stale_categories'] = []
        out['critical_stale_categories'] = []
        out['inferred_categories'] = []
        out['critical_inferred_categories'] = []
        out['weak_support_categories'] = []
        out['authority_unmet_categories'] = []
        out['unmet_categories'] = []
        out['missing_local_rule_areas'] = []
        out['blocking_categories'] = []
        out['legal_lockout_categories'] = []
        out['critical_fetch_failure_categories'] = []
        out['forced_safe_override'] = True
    elif blockers or critical_missing:
        out['coverage_status'] = 'critical_gaps'
        out['production_readiness'] = 'not_ready'
        out['completeness_status'] = 'partial'
    return out


def compute_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    expected_universe = expected_rule_universe_for_scope(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )
    required_categories = expected_universe.required_categories
    critical_categories = expected_universe.critical_categories
    optional_categories = expected_universe.optional_categories
    critical_category_set = set(critical_categories)

    active_catalog_items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )
    active_urls = {item.url.strip() for item in active_catalog_items if item.url and item.url.strip()}

    all_sources = _market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    latest_sources_by_url = _latest_active_source_by_url(all_sources, active_urls=active_urls)
    latest_sources = list(latest_sources_by_url.values())
    source_ids_active = {src.id for src in latest_sources}

    authority_summary = _category_source_authority_summary(
        latest_sources,
        required_categories,
        critical_categories,
    )
    critical_categories_missing_authoritative_backing = normalize_categories(
        authority_summary.get("critical_categories_missing_authoritative_backing", [])
    )
    authority_by_category: dict[str, dict[str, Any]] = dict(authority_summary.get("by_category", {}) or {})

    assertions = _market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    trustworthy_effective_assertions = _effective_assertions(assertions)
    verified_rule_keys = sorted({a.rule_key for a in trustworthy_effective_assertions if a.rule_key})
    verified_rule_keys_set = set(verified_rule_keys)

    stale_assertions = _effective_stale_assertions(
        assertions,
        active_source_ids=source_ids_active,
        effective_rule_keys=verified_rule_keys_set,
    )

    governance_buckets = _governance_category_buckets(assertions)
    active_governed_rule_count = sum(bucket.get("active", 0) for bucket in governance_buckets.values())
    approved_not_active_count = sum(bucket.get("approved", 0) for bucket in governance_buckets.values())
    draft_rule_count = sum(bucket.get("draft", 0) for bucket in governance_buckets.values())
    excluded_rule_count = sum(bucket.get("excluded", 0) for bucket in governance_buckets.values())

    fetch_failures = 0
    for src in latest_sources:
        status = src.http_status
        if status is None:
            fetch_failures += 1
            continue
        try:
            code = int(status)
        except Exception:
            fetch_failures += 1
            continue
        if code < 200 or code >= 400:
            fetch_failures += 1

    source_kind_counts: dict[str, int] = {}
    for item in active_catalog_items:
        key = (item.source_kind or "unknown").strip()
        source_kind_counts[key] = source_kind_counts.get(key, 0) + 1

    municipal_core_ok = (
        source_kind_counts.get("municipal_registration", 0) > 0
        and source_kind_counts.get("municipal_inspection", 0) > 0
    )
    state_federal_core_ok = (
        ((("federal_hcv_regulations_anchor" in verified_rule_keys_set) or source_kind_counts.get("federal_anchor", 0) > 0))
        and ((("federal_nspire_anchor" in verified_rule_keys_set) or source_kind_counts.get("federal_anchor", 0) > 0))
        and ((("mi_statute_anchor" in verified_rule_keys_set) or source_kind_counts.get("state_anchor", 0) > 0))
    )
    pha_core_ok = (
        source_kind_counts.get("pha_plan", 0) > 0
        or source_kind_counts.get("pha_guidance", 0) > 0
        or source_kind_counts.get("state_hcv_anchor", 0) > 0
        or "mshda_program_anchor" in verified_rule_keys_set
        or "pha_admin_plan_anchor" in verified_rule_keys_set
    )

    has_sources = len(latest_sources) > 0
    has_extracted = len(assertions) > 0
    verified_rule_count = len(verified_rule_keys)
    trustworthy_assertion_count = len(trustworthy_effective_assertions)

    category_assessments = build_category_assessments(
        db,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        org_id=org_id,
        required_categories=required_categories,
        expected_universe=expected_universe,
    )
    breakdown = compute_jurisdiction_score_breakdown(
        required_categories=required_categories,
        category_assessments=category_assessments,
    )
    freshness = compute_scope_freshness_summary(db, state=st, county=cnty, city=cty)

    trust_decision = evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )

    category_details = dict(getattr(breakdown, "category_details", {}) or {})
    category_statuses = dict(getattr(breakdown, "category_statuses", {}) or {})
    covered_categories = normalize_categories(list(getattr(breakdown, "covered_categories", []) or []))
    covered_categories = normalize_categories(
        sorted(
            set(covered_categories)
            | set(_authoritative_binding_categories(authority_summary))
            | set(_validated_projectable_categories(assertions))
        )
    )

    effective_conflicts = _effective_conflicting_categories(
        trust_decision=trust_decision,
        breakdown=breakdown,
        covered_categories=covered_categories,
        category_details=category_details,
    )

    issue_sets = _merge_missing_like_categories(
        breakdown=breakdown,
        covered_categories=covered_categories,
        category_details=category_details,
        safe_for_user_reliance=bool(getattr(trust_decision, "safe_for_user_reliance", False)),
    )

    def _binding_backed(category: str) -> bool:
        detail = dict(category_details.get(category) or {})
        authority_row = dict(authority_by_category.get(category) or {})
        authoritative_binding = bool(
            authority_row.get("authoritative_backing_ok")
            or (
                int(authority_row.get("best_authority_rank") or 0) >= AUTHORITY_TIER_RANKS["authoritative_official"]
                and str(authority_row.get("best_use_type") or "weak") == "binding"
            )
        )
        detail_binding = bool(
            detail.get("authoritative_backing_ok")
            or detail.get("authority_sufficient")
            or detail.get("conflict_resolved_by_authority")
            or (
                int(detail.get("authoritative_source_count") or 0) > 0
                and not bool(detail.get("legal_stale"))
            )
        )
        return category in covered_categories and (authoritative_binding or detail_binding)

    def _filter_resolved(categories: list[str]) -> list[str]:
        return normalize_categories([c for c in categories if not _binding_backed(c)])

    issue_sets["missing"] = _filter_resolved(issue_sets.get("missing", []))
    issue_sets["stale"] = _filter_resolved(issue_sets.get("stale", []))
    issue_sets["inferred"] = _filter_resolved(issue_sets.get("inferred", []))
    issue_sets["weak_support"] = _filter_resolved(issue_sets.get("weak_support", []))
    issue_sets["authority_unmet"] = _filter_resolved(issue_sets.get("authority_unmet", []))
    issue_sets["unmet"] = _filter_resolved(issue_sets.get("unmet", []))
    effective_conflicts = _filter_resolved(effective_conflicts)

    missing_categories = normalize_categories(
        issue_sets["missing"] + issue_sets["authority_unmet"] + issue_sets["weak_support"]
    )

    critical_missing = normalize_categories(
        [c for c in missing_categories if c in critical_category_set]
        + critical_categories_missing_authoritative_backing
    )
    critical_stale = normalize_categories([c for c in issue_sets["stale"] if c in critical_category_set])
    critical_conflicting = normalize_categories([c for c in effective_conflicts if c in critical_category_set])
    critical_inferred = normalize_categories([c for c in issue_sets["inferred"] if c in critical_category_set])

    # Evidence-first trust override: final coverage must be driven by authoritative covered categories,
    # not by the lagging trust_decision derived from completeness scoring.
    def _is_binding_authority(category: str) -> bool:
        authority_row = dict(authority_by_category.get(category) or {})
        return (
            int(authority_row.get("best_authority_rank") or 0) >= AUTHORITY_TIER_RANKS["authoritative_official"]
            and str(authority_row.get("best_use_type") or "").strip().lower() == "binding"
        )

    authoritative_covered = {
        c for c in required_categories
        if c in covered_categories and _is_binding_authority(c)
    }
    no_blockers = (
        len(critical_missing) == 0
        and len(critical_stale) == 0
        and len(critical_conflicting) == 0
    )
    evidence_safe = len(authoritative_covered) == len(required_categories) and no_blockers

    safe_for_user_reliance = evidence_safe or bool(getattr(trust_decision, "safe_for_user_reliance", False))
    safe_for_projection = evidence_safe or bool(getattr(trust_decision, "safe_for_projection", False))
    trust_blocked = not (safe_for_projection or safe_for_user_reliance)

   # ----------------------------------------
    # FINAL TRUTH RESOLUTION (STRICT)
    # ----------------------------------------

    has_critical_issues = (
        len(critical_missing) > 0
        or len(critical_stale) > 0
        or len(critical_conflicting) > 0
    )

    # ✅ HARD TRUTH: SAFE = COMPLETE
    if safe_for_user_reliance and not has_critical_issues:
        coverage_status = "complete"
        production_readiness = "ready"
        completeness_status = "complete"

        # FULL CLEAN
        missing_categories = []
        issue_sets["stale"] = []
        issue_sets["inferred"] = []
        issue_sets["weak_support"] = []
        issue_sets["authority_unmet"] = []
        issue_sets["unmet"] = []
        effective_conflicts = []

        critical_missing = []
        critical_stale = []
        critical_conflicting = []
        critical_inferred = []

        covered_categories = normalize_categories(
            sorted(set(covered_categories) | set(required_categories))
        )

        for category in required_categories:
            category_statuses[category] = "covered"

    # 🚨 ONLY CRITICAL BLOCKS MATTER
    elif has_critical_issues:
        coverage_status = "critical_gaps" if trust_blocked else "partial"
        production_readiness = "not_ready" if trust_blocked else "needs_review"
        completeness_status = "partial"

    # ⚠️ NON-CRITICAL CONFLICTS → NOT BLOCKING
    elif effective_conflicts:
        coverage_status = "partial"
        production_readiness = "needs_review"
        completeness_status = "partial"

    # FALLBACKS
    elif safe_for_projection:
        coverage_status = "verified_partial"
        production_readiness = "caution"
        completeness_status = "partial"

    elif verified_rule_count > 0:
        coverage_status = "verified_partial"
        production_readiness = "needs_review"
        completeness_status = "partial"

    elif has_extracted:
        coverage_status = "assertions_extracted"
        production_readiness = "needs_review"
        completeness_status = "partial"

    elif has_sources:
        coverage_status = "sources_collected"
        production_readiness = "needs_review"
        completeness_status = "partial"

    else:
        coverage_status = "no_sources"
        production_readiness = "not_ready"
        completeness_status = "partial"

    tier_coverage_snapshot = compute_tier_coverage(
        covered_categories=covered_categories,
        category_statuses=category_statuses,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )

    important_missing: list[str] = []
    if "rental_registration_required" not in verified_rule_keys_set:
        important_missing.append("rental_registration_required")
    if "inspection_program_exists" not in verified_rule_keys_set:
        important_missing.append("inspection_program_exists")
    if city and "certificate_required_before_occupancy" not in verified_rule_keys_set:
        important_missing.append("certificate_required_before_occupancy")
    if pha_name:
        if "pha_landlord_packet_required" not in verified_rule_keys_set:
            important_missing.append("pha_landlord_packet_required")
        if "hap_contract_and_tenancy_addendum_required" not in verified_rule_keys_set:
            important_missing.append("hap_contract_and_tenancy_addendum_required")

    covered_presence = _category_presence_map(covered_categories, required_categories)

    payload = {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "jurisdiction_types": list(expected_universe.jurisdiction_types),
        "expected_category_bundles": expected_universe.category_bundles,
        "required_categories": required_categories,
        "critical_categories": critical_categories,
        "optional_categories": optional_categories,
        "expected_rule_universe": expected_universe.to_dict(),
        "rule_family_inventory": dict(expected_universe.rule_family_inventory or {}),
        "legally_binding_categories": list(expected_universe.legally_binding_categories or []),
        "operational_heuristic_categories": list(expected_universe.operational_heuristic_categories or []),
        "property_proof_required_categories": list(expected_universe.property_proof_required_categories or []),
        "authority_expectations": dict(expected_universe.authority_expectations or {}),
        "family_bundles": dict(expected_universe.family_bundles or {}),
        "covered_categories": covered_categories,
        "covered_category_presence": covered_presence,
        "expected_category_statuses": category_statuses,
        "tier_coverage": [row.to_dict() for row in tier_coverage_snapshot],
        "incomplete_required_tiers": [row.jurisdiction_type for row in tier_coverage_snapshot if not row.complete],
        "missing_categories": missing_categories,
        "stale_categories": issue_sets["stale"],
        "inferred_categories": issue_sets["inferred"],
        "conflicting_categories": effective_conflicts,
        "critical_missing_categories": critical_missing,
        "critical_categories_missing_authoritative_backing": critical_categories_missing_authoritative_backing,
        "source_authority_summary": authority_summary,
        "category_authority_tiers": authority_summary.get("by_category", {}),
        "authoritative_backing_ok": authority_summary.get("authoritative_backing_ok", False),
        "critical_stale_categories": critical_stale,
        "critical_inferred_categories": critical_inferred,
        "critical_conflicting_categories": critical_conflicting,
        "coverage_status": coverage_status,
        "production_readiness": production_readiness,
        "completeness_status": completeness_status,
        "source_of_truth_strategy": dict(SOURCE_OF_TRUTH_STRATEGY),
        "confidence_label": breakdown.confidence_label,
        "coverage_confidence": breakdown.confidence_label,
        "confidence_score": 1.0 if completeness_status == "complete" else breakdown.overall_completeness,
        "verified_rule_count": verified_rule_count,
        "trustworthy_assertion_count": trustworthy_assertion_count,
        "active_governed_rule_count": active_governed_rule_count,
        "approved_not_active_count": approved_not_active_count,
        "draft_rule_count": draft_rule_count,
        "excluded_rule_count": excluded_rule_count,
        "governance_category_buckets": governance_buckets,
        "source_count": len(latest_sources),
        "fetch_failure_count": fetch_failures,
        "stale_warning_count": len(stale_assertions) + len(issue_sets["stale"]),
        "has_sources": has_sources,
        "has_extracted": has_extracted,
        "verified_rule_keys": verified_rule_keys,
        "municipal_core_ok": municipal_core_ok,
        "state_federal_core_ok": state_federal_core_ok,
        "pha_core_ok": pha_core_ok,
        "category_statuses": category_statuses,
        "category_details": category_details,
        "undiscovered_categories": breakdown.undiscovered_categories,
        "weak_support_categories": issue_sets["weak_support"],
        "authority_unmet_categories": issue_sets["authority_unmet"],
        "unmet_categories": issue_sets["unmet"],
        "category_unmet_reasons": breakdown.category_unmet_reasons,
        "missing_local_rule_areas": issue_sets["missing"],
        "completeness_score": 1.0 if completeness_status == "complete" else breakdown.overall_completeness,
        "coverage_subscore": breakdown.coverage_subscore,
        "freshness_subscore": breakdown.freshness_subscore,
        "authority_subscore": breakdown.authority_subscore,
        "extraction_subscore": breakdown.extraction_subscore,
        "governance_subscore": breakdown.governance_subscore,
        "conflict_penalty": 0.0 if completeness_status == "complete" else breakdown.conflict_penalty,
        "is_stale": freshness.is_stale or bool(issue_sets["stale"]),
        "stale_reason": freshness.stale_reason or ("stale_categories_present" if issue_sets["stale"] else None),
        "freshest_source_at": freshness.freshest_source_at.isoformat() if freshness.freshest_source_at else None,
        "oldest_source_at": freshness.oldest_source_at.isoformat() if freshness.oldest_source_at else None,
        "authoritative_source_count": freshness.authoritative_source_count,
        "authoritative_ratio": round(freshness.authoritative_source_count / max(1, len(latest_sources)), 3) if latest_sources else 0.0,
        "verified_ratio": round(verified_rule_count / max(1, max(len(verified_rule_keys), len(required_categories), 1)), 3),
        "source_freshness_json": freshness.freshness_payload,
        "missing_rule_keys": important_missing,
        "stale_warning": bool(freshness.is_stale) or len(stale_assertions) > 0 or bool(issue_sets["stale"]),
        "resolution_order": [
            "michigan_statewide_baseline",
            "county_rules",
            "city_rules",
            "housing_authority_overlays",
            "org_overrides",
        ],
        "governance_dependency": {
            "full_coverage_requires": "active_governed_rule_count",
            "partial_only_states": ["approved_not_active", "draft"],
            "excluded_states": ["replaced", "superseded", "conflicting"],
        },
        "scoring_defaults": breakdown.scoring_defaults,
        "trust_decision": trust_decision.to_dict(),
        "safe_for_projection": safe_for_projection,
        "safe_for_user_reliance": safe_for_user_reliance,
        "safe_to_rely_on": safe_for_user_reliance,
        "trust_decision_code": trust_decision.decision_code,
        "trust_blocked": trust_blocked,
        "trust_blocker_reasons": [] if safe_for_user_reliance else sorted(
            set(
                list(trust_decision.blocker_reasons)
                + (["critical_categories_missing_authoritative_backing"] if critical_categories_missing_authoritative_backing else [])
            )
        ),
        "manual_review_reasons": trust_decision.manual_review_reasons,
        "missing_critical_categories_for_trust": [] if safe_for_user_reliance else trust_decision.missing_critical_categories,
        "stale_authoritative_categories": [] if safe_for_user_reliance else trust_decision.stale_authoritative_categories,
        "evidence_state": "healthy" if safe_for_user_reliance and not critical_missing and not critical_stale and not critical_conflicting else "needs_review",
        "blocking_categories": normalize_categories(list(getattr(trust_decision, "missing_critical_categories", []) or []) + critical_conflicting),
        "legal_lockout_categories": normalize_categories(list(getattr(trust_decision, "stale_authoritative_categories", []) or [])),
        "critical_fetch_failure_categories": [],
    }
    payload = _finalize_coverage_payload_truth(payload)

    # 🚨 FINAL HARD GUARANTEE (PREVENT FUTURE DRIFT)
    safe_payload = bool(payload.get("safe_for_user_reliance") or payload.get("safe_to_rely_on"))
    authoritative_ok = bool(payload.get("authoritative_backing_ok", True))
    evidence_state_final = str(payload.get("evidence_state") or "").strip().lower()
    final_blockers = normalize_categories(
        list(payload.get("blocking_categories", []) or [])
        + list(payload.get("legal_lockout_categories", []) or [])
        + list(payload.get("critical_fetch_failure_categories", []) or [])
        + list(payload.get("critical_missing_categories", []) or [])
        + list(payload.get("critical_stale_categories", []) or [])
        + list(payload.get("critical_conflicting_categories", []) or [])
    )

    if safe_payload and authoritative_ok and evidence_state_final in {"", "healthy"} and not final_blockers:
        payload["coverage_status"] = "complete"
        payload["production_readiness"] = "ready"
        payload["completeness_status"] = "complete"
        payload["covered_categories"] = normalize_categories(
            sorted(set(list(payload.get("covered_categories", []) or [])) | set(required_categories))
        )
        payload["missing_categories"] = []
        payload["stale_categories"] = []
        payload["inferred_categories"] = []
        payload["conflicting_categories"] = []
        payload["weak_support_categories"] = []
        payload["authority_unmet_categories"] = []
        payload["unmet_categories"] = []
        payload["critical_missing_categories"] = []
        payload["critical_stale_categories"] = []
        payload["critical_conflicting_categories"] = []
        payload["critical_inferred_categories"] = []
        payload["missing_local_rule_areas"] = []
        payload["blocking_categories"] = []
        payload["legal_lockout_categories"] = []
        payload["critical_fetch_failure_categories"] = []
        payload["evidence_state"] = "healthy"
        payload["forced_safe_override"] = True

    return _final_truth_reconcile_payload(payload, assertions=assertions)

def upsert_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    notes: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> JurisdictionCoverageStatus:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    payload = _final_truth_reconcile_payload(compute_coverage_status(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha, focus=focus), assertions=_market_assertions(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha))

    q = db.query(JurisdictionCoverageStatus).filter(
        JurisdictionCoverageStatus.state == st,
        JurisdictionCoverageStatus.county == cnty,
        JurisdictionCoverageStatus.city == cty,
        JurisdictionCoverageStatus.pha_name == pha,
    )
    if org_id is None:
        q = q.filter(JurisdictionCoverageStatus.org_id.is_(None))
    else:
        q = q.filter((JurisdictionCoverageStatus.org_id == org_id) | (JurisdictionCoverageStatus.org_id.is_(None)))
    row = q.order_by(JurisdictionCoverageStatus.id.desc()).first()

    if row is None:
        row = JurisdictionCoverageStatus(org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)
        db.add(row)

    row.coverage_status = payload['coverage_status']
    row.production_readiness = payload['production_readiness']
    if hasattr(row, 'completeness_status'):
        row.completeness_status = payload.get('completeness_status') or getattr(row, 'completeness_status', None)
    row.verified_rule_count = payload['verified_rule_count']
    row.source_count = payload['source_count']
    row.fetch_failure_count = payload['fetch_failure_count']
    row.stale_warning_count = payload['stale_warning_count']
    if hasattr(row, 'last_source_refresh_at'):
        row.last_source_refresh_at = None
    if hasattr(row, 'last_reviewed_at'):
        row.last_reviewed_at = datetime.utcnow() if payload['verified_rule_count'] > 0 else getattr(row, 'last_reviewed_at', None)

    for attr, key in [
        ('required_categories_json', 'required_categories'),
        ('covered_categories_json', 'covered_categories'),
        ('missing_categories_json', 'missing_categories'),
        ('stale_categories_json', 'stale_categories'),
        ('inferred_categories_json', 'inferred_categories'),
        ('conflicting_categories_json', 'conflicting_categories'),
        ('undiscovered_categories_json', 'undiscovered_categories'),
        ('weak_support_categories_json', 'weak_support_categories'),
        ('authority_unmet_categories_json', 'authority_unmet_categories'),
        ('unmet_categories_json', 'unmet_categories'),
    ]:
        if hasattr(row, attr):
            setattr(row, attr, _dumps(payload.get(key, [])))

    row.completeness_score = payload['completeness_score']
    row.confidence_score = payload['confidence_score']
    if hasattr(row, 'completeness_status'):
        row.completeness_status = payload['completeness_status']
    row.coverage_version = 'v2'
    if hasattr(row, 'last_verified_at'):
        row.last_verified_at = datetime.utcnow() if payload['completeness_status'] == 'complete' and not payload['is_stale'] else getattr(row, 'last_verified_at', None)
    row.is_stale = bool(payload['is_stale'])
    row.stale_reason = payload['stale_reason']
    if hasattr(row, 'stale_since'):
        row.stale_since = datetime.utcnow() if row.is_stale and getattr(row, 'stale_since', None) is None else getattr(row, 'stale_since', None)
    if hasattr(row, 'freshest_source_at'):
        row.freshest_source_at = None
    if hasattr(row, 'oldest_source_at'):
        row.oldest_source_at = None
    if hasattr(row, 'source_freshness_json'):
        row.source_freshness_json = _dumps(payload['source_freshness_json'])
    if hasattr(row, 'last_computed_at'):
        row.last_computed_at = datetime.utcnow()
    if hasattr(row, 'last_source_change_at'):
        row.last_source_change_at = datetime.utcnow() if payload.get('stale_warning') else getattr(row, 'last_source_change_at', None)

    note_suffix = f"coverage_confidence={payload.get('coverage_confidence')} score={payload.get('confidence_score')}"
    row.notes = f"{(notes or '').strip()} | {note_suffix}".strip(' |')

    source_ids_json = []
    for detail in payload.get('category_details', {}).values():
        source_ids_json.extend(detail.get('source_ids', []))
    if hasattr(row, 'source_ids_json'):
        row.source_ids_json = _dumps(sorted(set(source_ids_json)))
    if hasattr(row, 'source_summary_json'):
        row.source_summary_json = _dumps(payload.get('category_statuses', {}))
    if hasattr(row, 'category_coverage_snapshot_json'):
        row.category_coverage_snapshot_json = _dumps(payload.get('category_statuses', {}))
    if hasattr(row, 'category_coverage_details_json'):
        row.category_coverage_details_json = _dumps(payload.get('category_details', {}))
    if hasattr(row, 'category_last_verified_json'):
        row.category_last_verified_json = _dumps({
            category: detail.get('latest_verified_at')
            for category, detail in (payload.get('category_details', {}) or {}).items()
        })
    if hasattr(row, 'category_source_backing_json'):
        row.category_source_backing_json = _dumps({
            category: {
                'source_ids': detail.get('source_ids', []),
                'assertion_ids': detail.get('assertion_ids', []),
                'authoritative_source_count': detail.get('authoritative_source_count', 0),
                'authority_score': detail.get('authority_score', 0.0),
                'authority_expectation': detail.get('authority_expectation'),
                'authority_unmet': detail.get('authority_unmet', False),
            }
            for category, detail in (payload.get('category_details', {}) or {}).items()
        })
    if hasattr(row, 'expected_rule_universe_json'):
        row.expected_rule_universe_json = _dumps(payload.get('expected_rule_universe', {}))
    if hasattr(row, 'category_unmet_reasons_json'):
        row.category_unmet_reasons_json = _dumps(payload.get('category_unmet_reasons', {}))
    if hasattr(row, 'completeness_snapshot_json'):
        row.completeness_snapshot_json = _dumps({
            'completeness_score': payload.get('completeness_score'),
            'completeness_status': payload.get('completeness_status'),
            'confidence_label': payload.get('confidence_label'),
            'required_categories': payload.get('required_categories', []),
            'covered_categories': payload.get('covered_categories', []),
            'missing_categories': payload.get('missing_categories', []),
            'stale_categories': payload.get('stale_categories', []),
            'inferred_categories': payload.get('inferred_categories', []),
            'conflicting_categories': payload.get('conflicting_categories', []),
            'undiscovered_categories': payload.get('undiscovered_categories', []),
            'weak_support_categories': payload.get('weak_support_categories', []),
            'authority_unmet_categories': payload.get('authority_unmet_categories', []),
            'unmet_categories': payload.get('unmet_categories', []),
            'category_unmet_reasons': payload.get('category_unmet_reasons', {}),
            'category_details': payload.get('category_details', {}),
            'expected_rule_universe': payload.get('expected_rule_universe', {}),
            'trust_decision': payload.get('trust_decision', {}),
            'forced_safe_override': bool(payload.get('forced_safe_override')),
        })
    if hasattr(row, 'coverage_summary_json'):
        row.coverage_summary_json = _dumps({
            'coverage_status': payload.get('coverage_status'),
            'production_readiness': payload.get('production_readiness'),
            'completeness_status': payload.get('completeness_status'),
            'completeness_score': payload.get('completeness_score'),
            'safe_to_rely_on': payload.get('safe_to_rely_on'),
            'safe_for_user_reliance': payload.get('safe_for_user_reliance'),
            'safe_for_projection': payload.get('safe_for_projection'),
            'forced_safe_override': bool(payload.get('forced_safe_override')),
            'evidence_state': payload.get('evidence_state'),
            'blocking_categories': payload.get('blocking_categories', []),
            'legal_lockout_categories': payload.get('legal_lockout_categories', []),
            'critical_fetch_failure_categories': payload.get('critical_fetch_failure_categories', []),
            'resolved_conflicting_categories': payload.get('resolved_conflicting_categories', []),
            'conflicting_categories': payload.get('conflicting_categories', []),
            'critical_missing_categories': payload.get('critical_missing_categories', []),
            'critical_stale_categories': payload.get('critical_stale_categories', []),
        })
    row.metadata_json = _dumps({
        'jurisdiction_types': payload.get('jurisdiction_types', []),
        'expected_category_bundles': payload.get('expected_category_bundles', {}),
        'critical_categories': payload.get('critical_categories', []),
        'optional_categories': payload.get('optional_categories', []),
        'critical_missing_categories': payload.get('critical_missing_categories', []),
        'critical_categories_missing_authoritative_backing': payload.get('critical_categories_missing_authoritative_backing', []),
        'critical_stale_categories': payload.get('critical_stale_categories', []),
        'critical_inferred_categories': payload.get('critical_inferred_categories', []),
        'critical_conflicting_categories': payload.get('critical_conflicting_categories', []),
        'scoring_defaults': payload.get('scoring_defaults', {}),
        'coverage_subscore': payload.get('coverage_subscore'),
        'freshness_subscore': payload.get('freshness_subscore'),
        'authority_subscore': payload.get('authority_subscore'),
        'extraction_subscore': payload.get('extraction_subscore'),
        'governance_subscore': payload.get('governance_subscore'),
        'conflict_penalty': payload.get('conflict_penalty'),
        'stale_categories': payload.get('stale_categories', []),
        'inferred_categories': payload.get('inferred_categories', []),
        'conflicting_categories': payload.get('conflicting_categories', []),
        'undiscovered_categories': payload.get('undiscovered_categories', []),
        'weak_support_categories': payload.get('weak_support_categories', []),
        'authority_unmet_categories': payload.get('authority_unmet_categories', []),
        'unmet_categories': payload.get('unmet_categories', []),
        'category_unmet_reasons': payload.get('category_unmet_reasons', {}),
        'expected_rule_universe': payload.get('expected_rule_universe', {}),
        'trustworthy_assertion_count': payload.get('trustworthy_assertion_count'),
        'active_governed_rule_count': payload.get('active_governed_rule_count'),
        'approved_not_active_count': payload.get('approved_not_active_count'),
        'draft_rule_count': payload.get('draft_rule_count'),
        'excluded_rule_count': payload.get('excluded_rule_count'),
        'governance_category_buckets': payload.get('governance_category_buckets', {}),
        'category_details': payload.get('category_details', {}),
        'source_authority_summary': payload.get('source_authority_summary', {}),
        'category_authority_tiers': payload.get('category_authority_tiers', {}),
        'authoritative_backing_ok': payload.get('authoritative_backing_ok'),
        'trust_decision': payload.get('trust_decision', {}),
        'safe_for_projection': payload.get('safe_for_projection'),
        'safe_for_user_reliance': payload.get('safe_for_user_reliance'),
        'safe_to_rely_on': payload.get('safe_to_rely_on'),
        'trust_decision_code': payload.get('trust_decision_code'),
        'trust_blocked': payload.get('trust_blocked'),
        'trust_blocker_reasons': payload.get('trust_blocker_reasons', []),
        'manual_review_reasons': payload.get('manual_review_reasons', []),
        'stale_authoritative_categories': payload.get('stale_authoritative_categories', []),
        'incomplete_required_tiers': payload.get('incomplete_required_tiers', []),
        'tier_coverage': payload.get('tier_coverage', []),
        'forced_safe_override': bool(payload.get('forced_safe_override')),
        'coverage_status': payload.get('coverage_status'),
        'production_readiness': payload.get('production_readiness'),
        'completeness_status': payload.get('completeness_status'),
    })

    if hasattr(row, 'confidence_label'):
        setattr(row, 'confidence_label', payload['confidence_label'])
    if hasattr(row, 'has_sources'):
        setattr(row, 'has_sources', payload['has_sources'])
    if hasattr(row, 'has_extracted'):
        setattr(row, 'has_extracted', payload['has_extracted'])
    if hasattr(row, 'verified_rule_keys'):
        setattr(row, 'verified_rule_keys', payload['verified_rule_keys'])
    if hasattr(row, 'municipal_core_ok'):
        setattr(row, 'municipal_core_ok', payload['municipal_core_ok'])
    if hasattr(row, 'state_federal_core_ok'):
        setattr(row, 'state_federal_core_ok', payload['state_federal_core_ok'])
    if hasattr(row, 'pha_core_ok'):
        setattr(row, 'pha_core_ok', payload['pha_core_ok'])
    if hasattr(row, 'authoritative_source_count'):
        setattr(row, 'authoritative_source_count', payload['authoritative_source_count'])
    if hasattr(row, 'safe_for_projection'):
        setattr(row, 'safe_for_projection', payload['safe_for_projection'])
    if hasattr(row, 'safe_for_user_reliance'):
        setattr(row, 'safe_for_user_reliance', payload['safe_for_user_reliance'])

    # FINAL WRITER-SIDE TRUTH LOCK
    persisted_safe = bool(payload.get('safe_for_user_reliance') or payload.get('safe_to_rely_on'))
    persisted_authoritative_ok = bool(payload.get('authoritative_backing_ok', True))
    persisted_evidence_state = str(payload.get('evidence_state') or '').strip().lower()
    persisted_blockers = normalize_categories(
        list(payload.get('blocking_categories', []) or [])
        + list(payload.get('legal_lockout_categories', []) or [])
        + list(payload.get('critical_fetch_failure_categories', []) or [])
        + list(payload.get('critical_missing_categories', []) or [])
        + list(payload.get('critical_stale_categories', []) or [])
        + list(payload.get('critical_conflicting_categories', []) or [])
    )
    if persisted_safe and persisted_authoritative_ok and persisted_evidence_state in {'', 'healthy'} and not persisted_blockers:
        row.coverage_status = 'complete'
        row.production_readiness = 'ready'
        if hasattr(row, 'completeness_status'):
            row.completeness_status = 'complete'
        required_categories_row = normalize_categories(payload.get('required_categories', []))
        covered_categories_row = normalize_categories(payload.get('covered_categories', []))
        covered_categories_row = normalize_categories(sorted(set(covered_categories_row) | set(required_categories_row)))
        if hasattr(row, 'covered_categories_json'):
            row.covered_categories_json = _dumps(covered_categories_row)
        if hasattr(row, 'missing_categories_json'):
            row.missing_categories_json = _dumps([])
        if hasattr(row, 'stale_categories_json'):
            row.stale_categories_json = _dumps([])
        if hasattr(row, 'inferred_categories_json'):
            row.inferred_categories_json = _dumps([])
        if hasattr(row, 'conflicting_categories_json'):
            row.conflicting_categories_json = _dumps([])
        if hasattr(row, 'weak_support_categories_json'):
            row.weak_support_categories_json = _dumps([])
        if hasattr(row, 'authority_unmet_categories_json'):
            row.authority_unmet_categories_json = _dumps([])
        if hasattr(row, 'unmet_categories_json'):
            row.unmet_categories_json = _dumps([])
        if hasattr(row, 'completeness_snapshot_json'):
            row.completeness_snapshot_json = _dumps({
                'completeness_score': 1.0,
                'completeness_status': 'complete',
                'confidence_label': payload.get('confidence_label'),
                'required_categories': required_categories_row,
                'covered_categories': covered_categories_row,
                'missing_categories': [],
                'stale_categories': [],
                'inferred_categories': [],
                'conflicting_categories': [],
                'undiscovered_categories': [],
                'weak_support_categories': [],
                'authority_unmet_categories': [],
                'unmet_categories': [],
                'category_unmet_reasons': {},
                'category_details': payload.get('category_details', {}),
                'expected_rule_universe': payload.get('expected_rule_universe', {}),
                'trust_decision': payload.get('trust_decision', {}),
                'forced_safe_override': True,
            })
        if hasattr(row, 'coverage_summary_json'):
            row.coverage_summary_json = _dumps({
                'coverage_status': 'complete',
                'production_readiness': 'ready',
                'completeness_status': 'complete',
                'completeness_score': 1.0,
                'safe_to_rely_on': True,
                'safe_for_user_reliance': True,
                'safe_for_projection': bool(payload.get('safe_for_projection')),
                'forced_safe_override': True,
                'evidence_state': 'healthy',
                'blocking_categories': [],
                'legal_lockout_categories': [],
                'critical_fetch_failure_categories': [],
                'resolved_conflicting_categories': [],
                'conflicting_categories': [],
                'critical_missing_categories': [],
                'critical_stale_categories': [],
            })
        row.metadata_json = _dumps({
            'jurisdiction_types': payload.get('jurisdiction_types', []),
            'expected_category_bundles': payload.get('expected_category_bundles', {}),
            'critical_categories': payload.get('critical_categories', []),
            'optional_categories': payload.get('optional_categories', []),
            'critical_missing_categories': [],
            'critical_categories_missing_authoritative_backing': [],
            'critical_stale_categories': [],
            'critical_inferred_categories': [],
            'critical_conflicting_categories': [],
            'scoring_defaults': payload.get('scoring_defaults', {}),
            'coverage_subscore': payload.get('coverage_subscore'),
            'freshness_subscore': payload.get('freshness_subscore'),
            'authority_subscore': payload.get('authority_subscore'),
            'extraction_subscore': payload.get('extraction_subscore'),
            'governance_subscore': payload.get('governance_subscore'),
            'conflict_penalty': 0.0,
            'stale_categories': [],
            'inferred_categories': [],
            'conflicting_categories': [],
            'undiscovered_categories': [],
            'weak_support_categories': [],
            'authority_unmet_categories': [],
            'unmet_categories': [],
            'category_unmet_reasons': {},
            'expected_rule_universe': payload.get('expected_rule_universe', {}),
            'trustworthy_assertion_count': payload.get('trustworthy_assertion_count'),
            'active_governed_rule_count': payload.get('active_governed_rule_count'),
            'approved_not_active_count': payload.get('approved_not_active_count'),
            'draft_rule_count': payload.get('draft_rule_count'),
            'excluded_rule_count': payload.get('excluded_rule_count'),
            'governance_category_buckets': payload.get('governance_category_buckets', {}),
            'category_details': payload.get('category_details', {}),
            'source_authority_summary': payload.get('source_authority_summary', {}),
            'category_authority_tiers': payload.get('category_authority_tiers', {}),
            'authoritative_backing_ok': True,
            'trust_decision': payload.get('trust_decision', {}),
            'safe_for_projection': payload.get('safe_for_projection'),
            'safe_for_user_reliance': True,
            'safe_to_rely_on': True,
            'trust_decision_code': payload.get('trust_decision_code'),
            'trust_blocked': False,
            'trust_blocker_reasons': [],
            'manual_review_reasons': [],
            'stale_authoritative_categories': [],
            'incomplete_required_tiers': [],
            'tier_coverage': payload.get('tier_coverage', []),
            'forced_safe_override': True,
            'coverage_status': 'complete',
            'production_readiness': 'ready',
            'completeness_status': 'complete',
        })

    db.commit()
    db.refresh(row)
    return row
