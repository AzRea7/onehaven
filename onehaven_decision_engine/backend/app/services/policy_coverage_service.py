# backend/app/services/policy_coverage_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import (
    compute_tier_coverage,
    expected_rule_universe_for_scope,
    normalize_categories,
)
from app.services.jurisdiction_completeness_service import (
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
from app.services.policy_catalog_admin_service import merged_catalog_for_market
from app.services.policy_cleanup_service import ARCHIVE_MARKER


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

    if _assertion_lifecycle_bucket(assertion) != "active":
        return False
    if coverage_status in {"conflicting", "candidate", "partial", "inferred", "stale"}:
        return False
    if "conflicting" in confidence_basis:
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
            "source_urls": [],
            "authority_classes": [],
            "authoritative_backing_ok": category not in critical_set,
        }

    for source in latest_sources:
        categories = normalize_categories(_loads_dict(getattr(source, "source_metadata_json", None)).get("discovery", {}).get("category_hints") or [])
        categories = categories or normalize_categories(_loads_dict(getattr(source, "registry_meta_json", None)).get("category_hints") or [])
        categories = categories or normalize_categories(getattr(source, "normalized_categories_json", None))
        if not categories:
            categories = required_categories
        tier = _source_authority_tier(source)
        rank = _source_authority_rank(source)
        auth_score = float(getattr(source, "authority_score", 0.0) or 0.0)
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
            url = str(getattr(source, "url", "") or "").strip()
            if url and url not in row["source_urls"]:
                row["source_urls"].append(url)
            authority_class = str(getattr(source, "authority_class", "") or "").strip()
            if authority_class and authority_class not in row["authority_classes"]:
                row["authority_classes"].append(authority_class)

    critical_missing = []
    for category in critical_set:
        row = by_category.get(category) or {}
        authoritative_backing_ok = bool(row.get("best_authority_rank", 0) >= AUTHORITY_TIER_RANKS["approved_official_supporting"])
        row["authoritative_backing_ok"] = authoritative_backing_ok
        if not authoritative_backing_ok:
            critical_missing.append(category)

    return {
        "by_category": by_category,
        "critical_categories_missing_authoritative_backing": critical_missing,
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

    all_sources = _market_sources(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)
    latest_sources_by_url = _latest_active_source_by_url(all_sources, active_urls=active_urls)
    latest_sources = list(latest_sources_by_url.values())
    source_ids_active = {src.id for src in latest_sources}
    authority_summary = _category_source_authority_summary(latest_sources, required_categories, critical_categories)
    critical_categories_missing_authoritative_backing = authority_summary["critical_categories_missing_authoritative_backing"]

    assertions = _market_assertions(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)
    trustworthy_effective_assertions = _effective_assertions(assertions)
    verified_rule_keys = sorted({a.rule_key for a in trustworthy_effective_assertions if a.rule_key})
    verified_rule_keys_set = set(verified_rule_keys)
    stale_assertions = _effective_stale_assertions(assertions, active_source_ids=source_ids_active, effective_rule_keys=verified_rule_keys_set)

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

    municipal_core_ok = source_kind_counts.get("municipal_registration", 0) > 0 and source_kind_counts.get("municipal_inspection", 0) > 0
    state_federal_core_ok = (
        (("federal_hcv_regulations_anchor" in verified_rule_keys_set) or source_kind_counts.get("federal_anchor", 0) > 0)
        and (("federal_nspire_anchor" in verified_rule_keys_set) or source_kind_counts.get("federal_anchor", 0) > 0)
        and (("mi_statute_anchor" in verified_rule_keys_set) or source_kind_counts.get("state_anchor", 0) > 0)
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
        required_categories=required_categories,
        expected_universe=expected_universe,
    )
    breakdown = compute_jurisdiction_score_breakdown(
        required_categories=required_categories,
        category_assessments=category_assessments,
    )
    covered_categories = collect_covered_categories_for_scope(db, state=st, county=cnty, city=cty)
    freshness = compute_scope_freshness_summary(db, state=st, county=cnty, city=cty)

    trust_decision = evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )
    tier_coverage_snapshot = compute_tier_coverage(
        covered_categories=breakdown.covered_categories,
        category_statuses=breakdown.category_statuses,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )

    critical_missing = [category for category in breakdown.missing_categories if category in set(critical_categories)]
    critical_missing = sorted(set(critical_missing + list(critical_categories_missing_authoritative_backing)))
    critical_stale = [category for category in breakdown.stale_categories if category in set(critical_categories)]
    critical_conflicting = [category for category in breakdown.conflicting_categories if category in set(critical_categories)]
    critical_inferred = [category for category in breakdown.inferred_categories if category in set(critical_categories)]

    if verified_rule_count == 0 and not has_sources:
        coverage_status = "no_sources"
    elif has_sources and not has_extracted:
        coverage_status = "sources_collected"
    elif has_extracted and active_governed_rule_count == 0 and approved_not_active_count == 0:
        coverage_status = "assertions_extracted"
    elif critical_missing or critical_stale or critical_conflicting or critical_categories_missing_authoritative_backing:
        coverage_status = "critical_gaps"
    elif verified_rule_count > 0:
        coverage_status = "verified_extended"
    else:
        coverage_status = "needs_review"

    if not critical_missing and not critical_stale and not critical_conflicting and verified_rule_count > 0 and fetch_failures == 0:
        production_readiness = "ready"
    elif has_sources or has_extracted:
        production_readiness = "partial"
    else:
        production_readiness = "needs_review"

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

    covered_presence = _category_presence_map(normalize_categories(covered_categories), required_categories)

    return {
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
        "covered_categories": normalize_categories(covered_categories),
        "covered_category_presence": covered_presence,
        "expected_category_statuses": dict(breakdown.category_statuses),
        "tier_coverage": [row.to_dict() for row in tier_coverage_snapshot],
        "incomplete_required_tiers": [row.jurisdiction_type for row in tier_coverage_snapshot if not row.complete],
        "missing_categories": breakdown.missing_categories,
        "stale_categories": breakdown.stale_categories,
        "inferred_categories": breakdown.inferred_categories,
        "conflicting_categories": breakdown.conflicting_categories,
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
        "confidence_label": breakdown.confidence_label,
        "coverage_confidence": breakdown.confidence_label,
        "confidence_score": breakdown.overall_completeness,
        "verified_rule_count": verified_rule_count,
        "trustworthy_assertion_count": trustworthy_assertion_count,
        "active_governed_rule_count": active_governed_rule_count,
        "approved_not_active_count": approved_not_active_count,
        "draft_rule_count": draft_rule_count,
        "excluded_rule_count": excluded_rule_count,
        "governance_category_buckets": governance_buckets,
        "source_count": len(latest_sources),
        "fetch_failure_count": fetch_failures,
        "stale_warning_count": len(stale_assertions) + len(breakdown.stale_categories),
        "has_sources": has_sources,
        "has_extracted": has_extracted,
        "verified_rule_keys": verified_rule_keys,
        "municipal_core_ok": municipal_core_ok,
        "state_federal_core_ok": state_federal_core_ok,
        "pha_core_ok": pha_core_ok,
        "category_statuses": breakdown.category_statuses,
        "category_details": breakdown.category_details,
        "undiscovered_categories": breakdown.undiscovered_categories,
        "weak_support_categories": breakdown.weak_support_categories,
        "authority_unmet_categories": breakdown.authority_unmet_categories,
        "unmet_categories": breakdown.unmet_categories,
        "category_unmet_reasons": breakdown.category_unmet_reasons,
        "missing_local_rule_areas": breakdown.missing_categories,
        "completeness_score": breakdown.overall_completeness,
        "completeness_status": breakdown.completeness_status,
        "coverage_subscore": breakdown.coverage_subscore,
        "freshness_subscore": breakdown.freshness_subscore,
        "authority_subscore": breakdown.authority_subscore,
        "extraction_subscore": breakdown.extraction_subscore,
        "governance_subscore": breakdown.governance_subscore,
        "conflict_penalty": breakdown.conflict_penalty,
        "is_stale": freshness.is_stale or bool(breakdown.stale_categories),
        "stale_reason": freshness.stale_reason or ("stale_categories_present" if breakdown.stale_categories else None),
        "freshest_source_at": freshness.freshest_source_at.isoformat() if freshness.freshest_source_at else None,
        "oldest_source_at": freshness.oldest_source_at.isoformat() if freshness.oldest_source_at else None,
        "authoritative_source_count": freshness.authoritative_source_count,
        "authoritative_ratio": round(freshness.authoritative_source_count / max(1, len(latest_sources)), 3) if latest_sources else 0.0,
        "verified_ratio": round(verified_rule_count / max(1, max(len(verified_rule_keys), len(required_categories), 1)), 3),
        "source_freshness_json": freshness.freshness_payload,
        "missing_rule_keys": important_missing,
        "stale_warning": bool(freshness.is_stale) or len(stale_assertions) > 0 or bool(breakdown.stale_categories),
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
        # Chunk 1 additive trust gating fields.
        "trust_decision": trust_decision.to_dict(),
        "safe_for_projection": trust_decision.safe_for_projection,
        "safe_for_user_reliance": trust_decision.safe_for_user_reliance,
        "trust_decision_code": trust_decision.decision_code,
        "trust_blocked": trust_decision.blocked,
        "trust_blocker_reasons": sorted(set(list(trust_decision.blocker_reasons) + (["critical_categories_missing_authoritative_backing"] if critical_categories_missing_authoritative_backing else []))),
        "manual_review_reasons": trust_decision.manual_review_reasons,
        "missing_critical_categories_for_trust": trust_decision.missing_critical_categories,
        "stale_authoritative_categories": trust_decision.stale_authoritative_categories,
        "incomplete_required_tiers": [row.jurisdiction_type for row in tier_coverage_snapshot if not row.complete],
        "tier_coverage": [row.to_dict() for row in tier_coverage_snapshot],
    }


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

    payload = compute_coverage_status(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha, focus=focus)

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

    row.coverage_status = payload["coverage_status"]
    row.production_readiness = payload["production_readiness"]
    row.verified_rule_count = payload["verified_rule_count"]
    row.source_count = payload["source_count"]
    row.fetch_failure_count = payload["fetch_failure_count"]
    row.stale_warning_count = payload["stale_warning_count"]
    if hasattr(row, "last_source_refresh_at"):
        row.last_source_refresh_at = None
    if hasattr(row, "last_reviewed_at"):
        row.last_reviewed_at = (
            datetime.utcnow()
            if payload["verified_rule_count"] > 0
            else getattr(row, "last_reviewed_at", None)
        )
    row.required_categories_json = _dumps(payload["required_categories"])
    row.covered_categories_json = _dumps(payload["covered_categories"])
    row.missing_categories_json = _dumps(payload["missing_categories"])
    if hasattr(row, "stale_categories_json"):
        row.stale_categories_json = _dumps(payload.get("stale_categories", []))
    if hasattr(row, "inferred_categories_json"):
        row.inferred_categories_json = _dumps(payload.get("inferred_categories", []))
    if hasattr(row, "conflicting_categories_json"):
        row.conflicting_categories_json = _dumps(payload.get("conflicting_categories", []))
    if hasattr(row, "undiscovered_categories_json"):
        row.undiscovered_categories_json = _dumps(payload.get("undiscovered_categories", []))
    if hasattr(row, "weak_support_categories_json"):
        row.weak_support_categories_json = _dumps(payload.get("weak_support_categories", []))
    if hasattr(row, "authority_unmet_categories_json"):
        row.authority_unmet_categories_json = _dumps(payload.get("authority_unmet_categories", []))
    if hasattr(row, "unmet_categories_json"):
        row.unmet_categories_json = _dumps(payload.get("unmet_categories", []))
    row.completeness_score = payload["completeness_score"]
    row.confidence_score = payload["confidence_score"]
    row.completeness_status = payload["completeness_status"]
    row.coverage_version = "v2"
    if hasattr(row, "last_verified_at"):
        row.last_verified_at = (
            datetime.utcnow()
            if payload["completeness_status"] == "complete" and not payload["is_stale"]
            else getattr(row, "last_verified_at", None)
        )
    row.is_stale = bool(payload["is_stale"])
    row.stale_reason = payload["stale_reason"]
    if hasattr(row, "stale_since"):
        row.stale_since = (
            datetime.utcnow()
            if row.is_stale and getattr(row, "stale_since", None) is None
            else getattr(row, "stale_since", None)
        )
    if hasattr(row, "freshest_source_at"):
        row.freshest_source_at = None
    if hasattr(row, "oldest_source_at"):
        row.oldest_source_at = None
    if hasattr(row, "source_freshness_json"):
        row.source_freshness_json = _dumps(payload["source_freshness_json"])
    if hasattr(row, "last_computed_at"):
        row.last_computed_at = datetime.utcnow()
    if hasattr(row, "last_source_change_at"):
        row.last_source_change_at = (
            datetime.utcnow()
            if payload.get("stale_warning")
            else getattr(row, "last_source_change_at", None)
        )

    note_suffix = f"coverage_confidence={payload.get('coverage_confidence')} score={payload.get('confidence_score')}"
    row.notes = f"{(notes or '').strip()} | {note_suffix}".strip(" |")

    source_ids_json = []
    for detail in payload.get("category_details", {}).values():
        source_ids_json.extend(detail.get("source_ids", []))
    if hasattr(row, "source_ids_json"):
        row.source_ids_json = _dumps(sorted(set(source_ids_json)))
    if hasattr(row, "source_summary_json"):
        row.source_summary_json = _dumps(payload.get("category_statuses", {}))
    if hasattr(row, "category_coverage_snapshot_json"):
        row.category_coverage_snapshot_json = _dumps(payload.get("category_statuses", {}))
    if hasattr(row, "category_coverage_details_json"):
        row.category_coverage_details_json = _dumps(payload.get("category_details", {}))
    if hasattr(row, "category_last_verified_json"):
        row.category_last_verified_json = _dumps({
            category: detail.get("latest_verified_at")
            for category, detail in (payload.get("category_details", {}) or {}).items()
        })
    if hasattr(row, "category_source_backing_json"):
        row.category_source_backing_json = _dumps({
            category: {
                "source_ids": detail.get("source_ids", []),
                "assertion_ids": detail.get("assertion_ids", []),
                "authoritative_source_count": detail.get("authoritative_source_count", 0),
                "authority_score": detail.get("authority_score", 0.0),
                "authority_expectation": detail.get("authority_expectation"),
                "authority_unmet": detail.get("authority_unmet", False),
            }
            for category, detail in (payload.get("category_details", {}) or {}).items()
        })
    if hasattr(row, "expected_rule_universe_json"):
        row.expected_rule_universe_json = _dumps(payload.get("expected_rule_universe", {}))
    if hasattr(row, "category_unmet_reasons_json"):
        row.category_unmet_reasons_json = _dumps(payload.get("category_unmet_reasons", {}))
    if hasattr(row, "completeness_snapshot_json"):
        row.completeness_snapshot_json = _dumps({
            "completeness_score": payload.get("completeness_score"),
            "completeness_status": payload.get("completeness_status"),
            "confidence_label": payload.get("confidence_label"),
            "required_categories": payload.get("required_categories", []),
            "covered_categories": payload.get("covered_categories", []),
            "missing_categories": payload.get("missing_categories", []),
            "stale_categories": payload.get("stale_categories", []),
            "inferred_categories": payload.get("inferred_categories", []),
            "conflicting_categories": payload.get("conflicting_categories", []),
            "undiscovered_categories": payload.get("undiscovered_categories", []),
            "weak_support_categories": payload.get("weak_support_categories", []),
            "authority_unmet_categories": payload.get("authority_unmet_categories", []),
            "unmet_categories": payload.get("unmet_categories", []),
            "category_unmet_reasons": payload.get("category_unmet_reasons", {}),
            "category_details": payload.get("category_details", {}),
            "expected_rule_universe": payload.get("expected_rule_universe", {}),
            "trust_decision": payload.get("trust_decision", {}),
        })
    row.metadata_json = _dumps({
        "jurisdiction_types": payload.get("jurisdiction_types", []),
        "expected_category_bundles": payload.get("expected_category_bundles", {}),
        "critical_categories": payload.get("critical_categories", []),
        "optional_categories": payload.get("optional_categories", []),
        "critical_missing_categories": payload.get("critical_missing_categories", []),
        "critical_categories_missing_authoritative_backing": payload.get("critical_categories_missing_authoritative_backing", []),
        "critical_stale_categories": payload.get("critical_stale_categories", []),
        "critical_inferred_categories": payload.get("critical_inferred_categories", []),
        "critical_conflicting_categories": payload.get("critical_conflicting_categories", []),
        "scoring_defaults": payload.get("scoring_defaults", {}),
        "coverage_subscore": payload.get("coverage_subscore"),
        "freshness_subscore": payload.get("freshness_subscore"),
        "authority_subscore": payload.get("authority_subscore"),
        "extraction_subscore": payload.get("extraction_subscore"),
        "governance_subscore": payload.get("governance_subscore"),
        "conflict_penalty": payload.get("conflict_penalty"),
        "stale_categories": payload.get("stale_categories", []),
        "inferred_categories": payload.get("inferred_categories", []),
        "conflicting_categories": payload.get("conflicting_categories", []),
        "undiscovered_categories": payload.get("undiscovered_categories", []),
        "weak_support_categories": payload.get("weak_support_categories", []),
        "authority_unmet_categories": payload.get("authority_unmet_categories", []),
        "unmet_categories": payload.get("unmet_categories", []),
        "category_unmet_reasons": payload.get("category_unmet_reasons", {}),
        "expected_rule_universe": payload.get("expected_rule_universe", {}),
        "trustworthy_assertion_count": payload.get("trustworthy_assertion_count"),
        "active_governed_rule_count": payload.get("active_governed_rule_count"),
        "approved_not_active_count": payload.get("approved_not_active_count"),
        "draft_rule_count": payload.get("draft_rule_count"),
        "excluded_rule_count": payload.get("excluded_rule_count"),
        "governance_category_buckets": payload.get("governance_category_buckets", {}),
        "category_details": payload.get("category_details", {}),
        "source_authority_summary": payload.get("source_authority_summary", {}),
        "category_authority_tiers": payload.get("category_authority_tiers", {}),
        "authoritative_backing_ok": payload.get("authoritative_backing_ok"),
        # Chunk 1 additive trust metadata.
        "trust_decision": payload.get("trust_decision", {}),
        "safe_for_projection": payload.get("safe_for_projection"),
        "safe_for_user_reliance": payload.get("safe_for_user_reliance"),
        "trust_decision_code": payload.get("trust_decision_code"),
        "trust_blocked": payload.get("trust_blocked"),
        "trust_blocker_reasons": payload.get("trust_blocker_reasons", []),
        "manual_review_reasons": payload.get("manual_review_reasons", []),
        "stale_authoritative_categories": payload.get("stale_authoritative_categories", []),
        "incomplete_required_tiers": payload.get("incomplete_required_tiers", []),
        "tier_coverage": payload.get("tier_coverage", []),
    })

    if hasattr(row, "confidence_label"):
        setattr(row, "confidence_label", payload["confidence_label"])
    if hasattr(row, "has_sources"):
        setattr(row, "has_sources", payload["has_sources"])
    if hasattr(row, "has_extracted"):
        setattr(row, "has_extracted", payload["has_extracted"])
    if hasattr(row, "verified_rule_keys"):
        setattr(row, "verified_rule_keys", payload["verified_rule_keys"])
    if hasattr(row, "municipal_core_ok"):
        setattr(row, "municipal_core_ok", payload["municipal_core_ok"])
    if hasattr(row, "state_federal_core_ok"):
        setattr(row, "state_federal_core_ok", payload["state_federal_core_ok"])
    if hasattr(row, "pha_core_ok"):
        setattr(row, "pha_core_ok", payload["pha_core_ok"])
    if hasattr(row, "authoritative_source_count"):
        setattr(row, "authoritative_source_count", payload["authoritative_source_count"])
    if hasattr(row, "safe_for_projection"):
        setattr(row, "safe_for_projection", payload["safe_for_projection"])
    if hasattr(row, "safe_for_user_reliance"):
        setattr(row, "safe_for_user_reliance", payload["safe_for_user_reliance"])

    db.commit()
    db.refresh(row)
    return row

# --- Step 4 additive coverage normalization layer ---
def _loads_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return list(v)
    if isinstance(v, tuple):
        return list(v)
    if isinstance(v, str):
        raw = v.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _assertion_currentness(assertion: PolicyAssertion, source: PolicySource | None) -> dict[str, Any]:
    validation_state = str(getattr(assertion, "validation_state", "") or "").strip().lower()
    trust_state = str(getattr(assertion, "trust_state", "") or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower() if source is not None else ""
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower() if source is not None else ""
    is_real = source is not None and bool(str(getattr(source, "url", "") or "").strip()) and _source_authority_rank(source) >= AUTHORITY_TIER_RANKS["approved_official_supporting"]
    is_verified = validation_state == "validated" and trust_state in {"trusted", "validated"}
    is_current = is_real and is_verified and freshness_status not in {"stale", "blocked", "fetch_failed", "error"} and refresh_state not in {"blocked", "failed"}
    return {
        "is_real": is_real,
        "is_verified": is_verified,
        "is_current": is_current,
        "authority_tier": _source_authority_tier(source) if source is not None else "derived_or_inferred",
        "last_verified_at": (getattr(source, "last_verified_at", None).isoformat() if source is not None and getattr(source, "last_verified_at", None) else None),
    }


def _structured_rule_matrix(assertions: list[PolicyAssertion], sources_by_id: dict[int, PolicySource]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for assertion in assertions:
        source = sources_by_id.get(int(assertion.source_id)) if getattr(assertion, "source_id", None) is not None else None
        currentness = _assertion_currentness(assertion, source)
        citation = _loads_dict(getattr(assertion, "citation_json", None))
        out.append({
            "assertion_id": int(getattr(assertion, "id", 0) or 0),
            "rule_key": getattr(assertion, "rule_key", None),
            "category": getattr(assertion, "normalized_category", None) or getattr(assertion, "rule_category", None),
            "coverage_status": getattr(assertion, "coverage_status", None),
            "validation_state": getattr(assertion, "validation_state", None),
            "trust_state": getattr(assertion, "trust_state", None),
            "confidence": float(getattr(assertion, "confidence", 0.0) or 0.0),
            "citation_quality": _citation_quality_for_assertion(assertion),
            "authority_tier": currentness.get("authority_tier"),
            "is_real": currentness.get("is_real"),
            "is_verified": currentness.get("is_verified"),
            "is_current": currentness.get("is_current"),
            "last_verified_at": currentness.get("last_verified_at"),
            "citation_url": citation.get("url") or (getattr(source, "url", None) if source is not None else None),
        })
    return out


def _change_detection_summary_for_assertions(assertions: list[PolicyAssertion]) -> dict[str, Any]:
    changed = 0
    revalidation_required = 0
    kinds: dict[str, int] = {}
    for assertion in assertions:
        summary = _loads_dict(getattr(assertion, "change_summary", None))
        validation = _loads_dict(summary.get("validation"))
        if summary.get("changed") or summary.get("change_detected"):
            changed += 1
        if validation.get("requires_revalidation"):
            revalidation_required += 1
        kind = str(summary.get("change_kind") or "none").strip().lower()
        kinds[kind] = kinds.get(kind, 0) + 1
    return {
        "changed_assertion_count": changed,
        "revalidation_required_count": revalidation_required,
        "change_kinds": kinds,
    }


_coverage_orig_compute_coverage_status = compute_coverage_status

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
    payload = dict(_coverage_orig_compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    ))
    assertions = _market_assertions(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
    sources = _market_sources(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
    sources_by_id = {int(src.id): src for src in sources if getattr(src, "id", None) is not None}
    rule_matrix = _structured_rule_matrix(assertions, sources_by_id)
    real_rule_count = sum(1 for row in rule_matrix if row.get("is_real"))
    verified_rule_count = sum(1 for row in rule_matrix if row.get("is_verified"))
    current_rule_count = sum(1 for row in rule_matrix if row.get("is_current"))
    verified_current_rule_count = sum(1 for row in rule_matrix if row.get("is_verified") and row.get("is_current"))
    change_detection = _change_detection_summary_for_assertions(assertions)
    payload["structured_rule_matrix"] = rule_matrix
    payload["real_rule_count"] = real_rule_count
    payload["verified_rule_count_step4"] = verified_rule_count
    payload["current_rule_count"] = current_rule_count
    payload["verified_current_rule_count"] = verified_current_rule_count
    payload["rules_real_verified_current"] = {
        "real": real_rule_count,
        "verified": verified_rule_count,
        "current": current_rule_count,
        "verified_current": verified_current_rule_count,
    }
    payload["change_detection_summary"] = change_detection
    payload["coverage_status_step4"] = (
        "verified_current" if verified_current_rule_count > 0 else
        "verified_not_current" if verified_rule_count > 0 else
        "real_not_verified" if real_rule_count > 0 else
        payload.get("coverage_status")
    )
    return payload


_coverage_orig_upsert_coverage_status = upsert_coverage_status

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
    row = _coverage_orig_upsert_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        notes=notes,
        focus=focus,
    )
    payload = compute_coverage_status(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name, focus=focus)
    if hasattr(row, "coverage_summary_json"):
        row.coverage_summary_json = _dumps({
            **_loads_dict(getattr(row, "coverage_summary_json", None)),
            "rules_real_verified_current": payload.get("rules_real_verified_current", {}),
            "change_detection_summary": payload.get("change_detection_summary", {}),
        })
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
