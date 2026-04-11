# backend/app/services/policy_coverage_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import (
    expected_rule_universe_for_scope,
    normalize_categories,
)
from app.services.jurisdiction_completeness_service import (
    build_category_assessments,
    collect_covered_categories_for_scope,
    compute_jurisdiction_score_breakdown,
    compute_scope_freshness_summary,
)
from app.policy_models import JurisdictionCoverageStatus, PolicyAssertion, PolicySource
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
        required_categories=required_categories,
    )
    breakdown = compute_jurisdiction_score_breakdown(required_categories=required_categories, category_assessments=category_assessments)
    covered_categories = collect_covered_categories_for_scope(db, state=st, county=cnty, city=cty)
    freshness = compute_scope_freshness_summary(db, state=st, county=cnty, city=cty)

    critical_missing = [category for category in breakdown.missing_categories if category in set(critical_categories)]
    critical_stale = [category for category in breakdown.stale_categories if category in set(critical_categories)]
    critical_conflicting = [category for category in breakdown.conflicting_categories if category in set(critical_categories)]
    critical_inferred = [category for category in breakdown.inferred_categories if category in set(critical_categories)]

    if verified_rule_count == 0 and not has_sources:
        coverage_status = "no_sources"
    elif has_sources and not has_extracted:
        coverage_status = "sources_collected"
    elif has_extracted and active_governed_rule_count == 0 and approved_not_active_count == 0:
        coverage_status = "assertions_extracted"
    elif critical_missing or critical_stale or critical_conflicting:
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
        "covered_categories": normalize_categories(covered_categories),
        "covered_category_presence": covered_presence,
        "missing_categories": breakdown.missing_categories,
        "stale_categories": breakdown.stale_categories,
        "inferred_categories": breakdown.inferred_categories,
        "conflicting_categories": breakdown.conflicting_categories,
        "critical_missing_categories": critical_missing,
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
    row.last_source_refresh_at = None
    row.last_reviewed_at = datetime.utcnow() if payload["verified_rule_count"] > 0 else row.last_reviewed_at
    row.required_categories_json = _dumps(payload["required_categories"])
    row.covered_categories_json = _dumps(payload["covered_categories"])
    row.missing_categories_json = _dumps(payload["missing_categories"])
    row.completeness_score = payload["completeness_score"]
    row.confidence_score = payload["confidence_score"]
    row.completeness_status = payload["completeness_status"]
    row.coverage_version = "v2"
    row.last_verified_at = datetime.utcnow() if payload["completeness_status"] == "complete" and not payload["is_stale"] else row.last_verified_at
    row.is_stale = bool(payload["is_stale"])
    row.stale_reason = payload["stale_reason"]
    row.stale_since = datetime.utcnow() if row.is_stale and row.stale_since is None else row.stale_since
    row.freshest_source_at = None
    row.oldest_source_at = None
    row.source_freshness_json = _dumps(payload["source_freshness_json"])
    row.last_computed_at = datetime.utcnow()
    row.last_source_change_at = datetime.utcnow() if payload.get("stale_warning") else row.last_source_change_at

    note_suffix = f"coverage_confidence={payload.get('coverage_confidence')} score={payload.get('confidence_score')}"
    row.notes = f"{(notes or '').strip()} | {note_suffix}".strip(" |")

    source_ids_json = []
    for detail in payload.get("category_details", {}).values():
        source_ids_json.extend(detail.get("source_ids", []))
    row.source_ids_json = _dumps(sorted(set(source_ids_json)))
    row.source_summary_json = _dumps(payload.get("category_statuses", {}))
    row.metadata_json = _dumps(
        {
            "jurisdiction_types": payload.get("jurisdiction_types", []),
            "expected_category_bundles": payload.get("expected_category_bundles", {}),
            "critical_categories": payload.get("critical_categories", []),
            "optional_categories": payload.get("optional_categories", []),
            "critical_missing_categories": payload.get("critical_missing_categories", []),
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
            "trustworthy_assertion_count": payload.get("trustworthy_assertion_count"),
            "active_governed_rule_count": payload.get("active_governed_rule_count"),
            "approved_not_active_count": payload.get("approved_not_active_count"),
            "draft_rule_count": payload.get("draft_rule_count"),
            "excluded_rule_count": payload.get("excluded_rule_count"),
            "governance_category_buckets": payload.get("governance_category_buckets", {}),
            "category_details": payload.get("category_details", {}),
        }
    )

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

    db.commit()
    db.refresh(row)
    return row