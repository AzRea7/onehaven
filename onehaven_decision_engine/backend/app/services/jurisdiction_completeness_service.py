# backend/app/services/jurisdiction_completeness_service.py
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..domain.jurisdiction_categories import (
    CATEGORY_UNCATEGORIZED,
    JurisdictionExpectedRuleUniverse,
    JurisdictionTierCoverage,
    compute_category_score_from_statuses,
    completeness_confidence_label,
    compute_tier_coverage,
    expected_rule_universe_for_scope,
    get_critical_categories,
    normalize_categories,
    normalize_category,
    normalize_rule_category,
)
from ..domain.jurisdiction_defaults import (
    DEFAULT_STALE_DAYS,
    completeness_score_weights,
    completeness_scoring_thresholds,
    merged_hard_trust_defaults,
    required_categories_for_city,
)
from ..domain.jurisdiction_scoring import (
    JurisdictionCompleteness,
    compute_category_completeness,
)
from ..policy_models import (
    JurisdictionCoverageStatus,
    JurisdictionProfile,
    PolicyAssertion,
    PolicySource,
)
from .jurisdiction_profile_service import _loads, _dumps, merge_profile_policy_meta




def _norm_state(value: Optional[str]) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().upper()
    return raw or None


def _norm_lower(value: Optional[str]) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    return raw or None


def _norm_text(value: Optional[str]) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None
AUTHORITY_EXPECTATION_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}


@dataclass(frozen=True)
class JurisdictionFreshnessSummary:
    source_count: int
    authoritative_source_count: int
    freshest_source_at: datetime | None
    oldest_source_at: datetime | None
    freshness_payload: dict[str, Any]
    is_stale: bool
    stale_reason: str | None


@dataclass(frozen=True)
class JurisdictionCategoryAssessment:
    category: str
    status: str
    source_count: int
    authoritative_source_count: int
    assertion_count: int
    governed_assertion_count: int
    citation_count: int
    confidence_score: float
    extraction_score: float
    authority_score: float
    governance_score: float
    freshness_score: float
    stale_source_count: int
    authoritative_stale_source_count: int
    legal_stale: bool
    informational_stale: bool
    conflict_count: int
    inferred: bool
    stale: bool
    conflicting: bool
    missing: bool
    undiscovered: bool
    weak_support: bool
    authority_unmet: bool
    unmet_reason: str | None
    unmet_reasons: list[str]
    authority_expectation: str | None
    latest_verified_at: datetime | None
    source_ids: list[int]
    assertion_ids: list[int]


@dataclass(frozen=True)
class JurisdictionScoreBreakdown:
    overall_completeness: float
    completeness_status: str
    coverage_subscore: float
    freshness_subscore: float
    authority_subscore: float
    extraction_subscore: float
    governance_subscore: float
    conflict_penalty: float
    confidence_label: str
    category_statuses: dict[str, str]
    covered_categories: list[str]
    stale_categories: list[str]
    legal_stale_categories: list[str]
    informational_stale_categories: list[str]
    critical_stale_categories: list[str]
    stale_authoritative_categories: list[str]
    inferred_categories: list[str]
    conflicting_categories: list[str]
    missing_categories: list[str]
    undiscovered_categories: list[str]
    weak_support_categories: list[str]
    authority_unmet_categories: list[str]
    unmet_categories: list[str]
    category_unmet_reasons: dict[str, list[str]]
    category_details: dict[str, dict[str, Any]]
    scoring_defaults: dict[str, Any]


@dataclass(frozen=True)
class JurisdictionTrustDecision:
    decision_code: str
    safe_for_projection: bool
    safe_for_user_reliance: bool
    blocked: bool
    blocker_reasons: list[str]
    manual_review_reasons: list[str]
    missing_critical_categories: list[str]
    missing_required_categories: list[str]
    stale_categories: list[str]
    stale_authoritative_categories: list[str]
    legal_stale_categories: list[str]
    critical_legal_stale_categories: list[str]
    informational_stale_categories: list[str]
    conflicting_categories: list[str]
    inferred_categories: list[str]
    inferred_critical_categories: list[str]
    incomplete_required_tiers: list[str]
    tier_coverage: list[dict[str, Any]]
    required_categories: list[str]
    critical_categories: list[str]
    overall_completeness: float
    confidence_label: str
    authority_subscore: float
    freshness_subscore: float
    governance_subscore: float
    conflict_penalty: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
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


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
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


def _profile_pha_name(profile: JurisdictionProfile | None) -> str | None:
    if profile is None:
        return None
    return _norm_text(getattr(profile, "pha_name", None))


def _coverage_has_attr(name: str) -> bool:
    return hasattr(JurisdictionCoverageStatus, name)

def _set_if_present(obj: Any, name: str, value: Any) -> None:
    if hasattr(obj, name):
        setattr(obj, name, value)


def _coverage_column_uses_json(name: str) -> bool:
    try:
        col = JurisdictionCoverageStatus.__table__.columns.get(name)
        if col is None:
            return False
        return col.type.__class__.__name__.upper() in {"JSON", "JSONB"}
    except Exception:
        return False


def _as_json_storage(name: str, value: Any) -> Any:
    if _coverage_column_uses_json(name):
        return value
    if isinstance(value, (dict, list)):
        return _dumps(value)
    return value


def _scope_filters(*, state: str | None, county: str | None, city: str | None):
    filters = []
    if state:
        filters.append(func.upper(PolicySource.state) == state.strip().upper())
    if county:
        filters.append(func.lower(PolicySource.county) == county.strip().lower())
    if city:
        filters.append(func.lower(PolicySource.city) == city.strip().lower())
    return filters


def _scope_filters_assertion(*, state: str | None, county: str | None, city: str | None):
    filters = []
    if state:
        filters.append(func.upper(PolicyAssertion.state) == state.strip().upper())
    if county:
        filters.append(func.lower(PolicyAssertion.county) == county.strip().lower())
    if city:
        filters.append(func.lower(PolicyAssertion.city) == city.strip().lower())
    return filters


def _coalesce_required_categories(profile: JurisdictionProfile) -> list[str]:
    current_required = normalize_categories(
        _loads_json_list(getattr(profile, "required_categories_json", None))
    )
    if current_required:
        return current_required
    return required_categories_for_city(
        profile.city,
        state=profile.state or "MI",
        include_section8=True,
    )




def _expected_universe_metadata(universe: JurisdictionExpectedRuleUniverse | None) -> dict[str, Any]:
    if universe is None:
        return {
            "expected_rule_universe": {},
            "required_categories_by_tier": {},
            "expected_rules_by_category": {},
            "rule_family_inventory": {},
            "legally_binding_categories": [],
            "operational_heuristic_categories": [],
            "property_proof_required_categories": [],
            "authority_expectations": {},
            "authority_scope_by_category": {},
            "required_source_families_by_category": {},
            "critical_source_families": [],
        }
    return {
        "expected_rule_universe": universe.to_dict(),
        "required_categories_by_tier": dict(universe.required_categories_by_tier or {}),
        "expected_rules_by_category": dict(universe.expected_rules_by_category or {}),
        "rule_family_inventory": dict(universe.rule_family_inventory or {}),
        "legally_binding_categories": list(universe.legally_binding_categories or []),
        "operational_heuristic_categories": list(universe.operational_heuristic_categories or []),
        "property_proof_required_categories": list(universe.property_proof_required_categories or []),
        "authority_expectations": dict(universe.authority_expectations or {}),
        "authority_scope_by_category": dict(universe.authority_scope_by_category or {}),
        "required_source_families_by_category": dict(universe.required_source_families_by_category or {}),
        "critical_source_families": list(universe.critical_source_families or []),
    }

def _collect_covered_categories_from_profile(profile: JurisdictionProfile) -> list[str]:
    return normalize_categories(_loads_json_list(getattr(profile, "covered_categories_json", None)))


def _profile_policy_meta(profile: JurisdictionProfile) -> dict[str, Any]:
    policy = _loads(getattr(profile, "policy_json", None), {})
    if not isinstance(policy, dict):
        return {}
    meta = policy.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _profile_trust_defaults(profile: JurisdictionProfile) -> dict[str, Any]:
    meta = _profile_policy_meta(profile)
    defaults = {}
    if isinstance(meta.get("trust_defaults"), dict):
        defaults.update(meta["trust_defaults"])
    coverage = policy_coverage = (_loads(getattr(profile, "policy_json", None), {}) or {}).get("coverage") or {}
    if isinstance(policy_coverage, dict) and isinstance(policy_coverage.get("trust_defaults"), dict):
        defaults.update(policy_coverage["trust_defaults"])
    return defaults


def _source_categories(source: PolicySource) -> list[str]:
    return normalize_categories(_loads_json_list(getattr(source, "normalized_categories_json", None)))


def _assertion_category(assertion: PolicyAssertion) -> str | None:
    normalized = normalize_category(getattr(assertion, "normalized_category", None))
    if normalized:
        return normalized
    normalized = normalize_category(getattr(assertion, "rule_category", None))
    if normalized:
        return normalized
    fallback = normalize_rule_category(getattr(assertion, "rule_key", None))
    return None if fallback == CATEGORY_UNCATEGORIZED else fallback




def _assertion_is_validation_trusted(assertion: PolicyAssertion) -> bool:
    validation_state = (getattr(assertion, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(assertion, "trust_state", None) or "extracted").strip().lower()
    if validation_state != "validated":
        return False
    return trust_state in {"validated", "trusted"}


def _source_authority_rank(source: PolicySource) -> int:
    try:
        rank = int(getattr(source, "authority_rank", 0) or 0)
        if rank > 0:
            return rank
    except Exception:
        pass
    tier = str(getattr(source, "authority_tier", None) or "derived_or_inferred").strip()
    return int(AUTHORITY_EXPECTATION_RANKS.get(tier, 25))


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
    use_type = str(getattr(source, "authority_use_type", None) or "").strip().lower()
    if use_type:
        return use_type
    tier = _source_authority_tier(source)
    if tier == "authoritative_official":
        return "binding"
    if tier in {"approved_official_supporting", "semi_authoritative_operational"}:
        return "supporting"
    return "weak"


def _source_validation_ok(source: PolicySource) -> bool:
    http_status = getattr(source, "http_status", None)
    try:
        if http_status is not None and int(http_status) >= 400:
            return False
    except Exception:
        return False
    refresh_state = (getattr(source, "refresh_state", None) or "").strip().lower()
    freshness_status = (getattr(source, "freshness_status", None) or "").strip().lower()
    if refresh_state in {"failed", "blocked"}:
        return False
    if freshness_status in {"fetch_failed", "error", "blocked"}:
        return False
    return True

def _collect_source_rows_for_scope(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    org_id: int | None = None,
    pha_name: str | None = None,
) -> list[PolicySource]:
    stmt = select(PolicySource).where(*_scope_filters(state=state, county=county, city=city))
    rows = list(db.execute(stmt).scalars().all())

    out: list[PolicySource] = []
    pha_norm = _norm_text(pha_name)
    for row in rows:
        if hasattr(row, "org_id"):
            row_org = getattr(row, "org_id", None)
            if org_id is None:
                if row_org is not None:
                    continue
            elif row_org not in {None, org_id}:
                continue
        if hasattr(row, "pha_name"):
            row_pha = _norm_text(getattr(row, "pha_name", None))
            if pha_norm is None:
                if row_pha not in {None, ""}:
                    continue
            elif row_pha not in {None, pha_norm}:
                continue
        out.append(row)
    return out

def _collect_assertion_rows_for_scope(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    org_id: int | None = None,
    pha_name: str | None = None,
) -> list[PolicyAssertion]:
    stmt = select(PolicyAssertion).where(
        *_scope_filters_assertion(state=state, county=county, city=city)
    )
    rows = list(db.execute(stmt).scalars().all())

    out: list[PolicyAssertion] = []
    pha_norm = _norm_text(pha_name)
    for row in rows:
        if hasattr(row, "org_id"):
            row_org = getattr(row, "org_id", None)
            if org_id is None:
                if row_org is not None:
                    continue
            elif row_org not in {None, org_id}:
                continue
        if hasattr(row, "pha_name"):
            row_pha = _norm_text(getattr(row, "pha_name", None))
            if pha_norm is None:
                if row_pha not in {None, ""}:
                    continue
            elif row_pha not in {None, pha_norm}:
                continue
        out.append(row)
    return out

def _collect_covered_categories_from_sources(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    org_id: int | None = None,
    pha_name: str | None = None,
) -> list[str]:
    universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )
    required = list(universe.required_categories)
    legally_binding = set(getattr(universe, "legally_binding_categories", []) or [])

    categories: list[str] = []
    for row in _collect_source_rows_for_scope(
        db,
        state=state,
        county=county,
        city=city,
        org_id=org_id,
        pha_name=pha_name,
    ):
        if not _source_validation_ok(row):
            continue
        authority_tier = _source_authority_tier(row)
        authority_use_type = _source_use_type(row)
        if authority_tier not in {"authoritative_official", "approved_official_supporting"}:
            continue
        if authority_use_type not in {"binding", "supporting"}:
            continue

        row_categories = _source_categories(row)
        for category in row_categories:
            if category not in required:
                continue
            if category in legally_binding:
                if authority_tier != "authoritative_official":
                    continue
                if authority_use_type != "binding":
                    continue
            categories.append(category)
    return normalize_categories(categories)

def _collect_covered_categories_from_assertions(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    org_id: int | None = None,
    pha_name: str | None = None,
) -> list[str]:
    universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
    )
    legally_binding = set(getattr(universe, "legally_binding_categories", []) or [])
    categories: list[str] = []
    for row in _collect_assertion_rows_for_scope(
        db,
        state=state,
        county=county,
        city=city,
        org_id=org_id,
        pha_name=pha_name,
    ):
        category = _assertion_category(row)
        if not category:
            continue
        normalized_status = (getattr(row, "coverage_status", None) or "").strip().lower()
        review_status = (getattr(row, "review_status", None) or "").strip().lower()
        governance_state = (getattr(row, "governance_state", None) or "").strip().lower()
        rule_status = (getattr(row, "rule_status", None) or "").strip().lower()
        trust_state = (getattr(row, "trust_state", None) or "").strip().lower()
        is_current = bool(getattr(row, "is_current", False))
        if normalized_status not in {"covered", "verified", "active", "approved"}:
            continue
        if normalized_status in {"weak_support", "partial", "inferred", "candidate"}:
            continue
        if review_status != "verified":
            continue
        if governance_state not in {"active", "approved"}:
            continue
        if rule_status not in {"active", "approved", ""}:
            continue
        if governance_state == "active" and not is_current:
            continue
        if not _assertion_is_validation_trusted(row):
            continue
        if trust_state not in {"validated", "trusted"}:
            continue
        if category in legally_binding:
            source_id = getattr(row, "source_id", None)
            source = None
            if source_id is not None:
                try:
                    source = db.get(PolicySource, int(source_id))
                except Exception:
                    source = None
            if source is not None:
                if _source_authority_tier(source) != "authoritative_official":
                    continue
                if _source_use_type(source) != "binding":
                    continue
        categories.append(category)
    return normalize_categories(categories)

def collect_covered_categories_for_scope(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    org_id: int | None = None,
    pha_name: str | None = None,
    include_sources: bool = False,
    include_assertions: bool = True,
    extra_categories: Iterable[Any] | None = None,
) -> list[str]:
    categories: list[Any] = []
    if include_sources:
        categories.extend(
            _collect_covered_categories_from_sources(
                db, state=state, county=county, city=city, org_id=org_id, pha_name=pha_name
            )
        )
    if include_assertions:
        categories.extend(
            _collect_covered_categories_from_assertions(
                db, state=state, county=county, city=city, org_id=org_id, pha_name=pha_name
            )
        )
    if extra_categories:
        categories.extend(list(extra_categories))
    return normalize_categories(categories)

def _source_timestamp(source: PolicySource) -> datetime | None:
    return (
        getattr(source, "last_verified_at", None)
        or getattr(source, "freshness_checked_at", None)
        or getattr(source, "last_fetched_at", None)
        or getattr(source, "retrieved_at", None)
    )


def _source_is_stale(source: PolicySource, *, stale_days: int) -> bool:
    freshness_status = (getattr(source, "freshness_status", None) or "unknown").strip().lower()
    if freshness_status in {"stale", "needs_recheck", "expired"}:
        return True
    ts = _source_timestamp(source)
    if ts is None:
        return True
    return ts < (_utcnow() - timedelta(days=stale_days))


def _source_authority_score(source: PolicySource) -> float:
    trust_level = float(getattr(source, "trust_level", 0.5) or 0.0)
    authoritative = 1.0 if bool(getattr(source, "is_authoritative", False)) else 0.0
    registry_status = (getattr(source, "registry_status", None) or "active").strip().lower()
    registry_score = 1.0 if registry_status in {"active", "verified", "trusted"} else 0.4
    return max(
        0.0,
        min(
            1.0,
            round((0.45 * authoritative) + (0.35 * trust_level) + (0.20 * registry_score), 6),
        ),
    )


def _source_freshness_score(source: PolicySource, *, stale_days: int) -> float:
    if _source_is_stale(source, stale_days=stale_days):
        return 0.0
    freshness_status = (getattr(source, "freshness_status", None) or "fresh").strip().lower()
    if freshness_status in {"fresh", "verified"}:
        return 1.0
    if freshness_status in {"aging", "warning", "needs_refresh"}:
        return 0.5
    return 0.75



def _category_freshness_snapshot(
    source: PolicySource,
    *,
    category: str,
    stale_days: int,
) -> dict[str, Any]:
    category = normalize_category(category) or str(category or '').strip().lower()
    try:
        from app.services.jurisdiction_sla_service import (
            category_stale_kind_for_source,
            source_category_due_at,
            source_category_is_past_sla,
        )

        due_at = source_category_due_at(source, category=category)
        is_stale = source_category_is_past_sla(source, category=category)
        stale_kind = category_stale_kind_for_source(source, category=category)
    except Exception:
        due_at = None
        is_stale = _source_is_stale(source, stale_days=stale_days)
        stale_kind = 'legal' if category in set(get_critical_categories(state=None, county=None, city=None, pha_name=None, include_section8=True, tenant_waitlist_depth=None)) else 'informational'
    freshness_score = 0.0 if is_stale else _source_freshness_score(source, stale_days=stale_days)
    return {
        'category': category,
        'due_at': due_at,
        'is_stale': bool(is_stale),
        'stale_kind': stale_kind,
        'freshness_score': float(freshness_score),
    }


def _assertion_is_governed(assertion: PolicyAssertion) -> bool:
    if getattr(assertion, "superseded_by_assertion_id", None) is not None:
        return False
    if getattr(assertion, "replaced_by_assertion_id", None) is not None:
        return False
    governance_state = (getattr(assertion, "governance_state", None) or "").strip().lower()
    review_status = (getattr(assertion, "review_status", None) or "").strip().lower()
    rule_status = (getattr(assertion, "rule_status", None) or "").strip().lower()
    if governance_state == "active":
        return True
    if governance_state == "approved" and review_status in {"verified", "approved"}:
        return True
    return (
        governance_state in {"", "draft"}
        and review_status == "verified"
        and rule_status in {"candidate", "active", ""}
    )


def _assertion_governance_score(assertion: PolicyAssertion) -> float:
    if (
        getattr(assertion, "superseded_by_assertion_id", None) is not None
        or getattr(assertion, "replaced_by_assertion_id", None) is not None
    ):
        return 0.0
    governance_state = (getattr(assertion, "governance_state", None) or "draft").strip().lower()
    review_status = (getattr(assertion, "review_status", None) or "extracted").strip().lower()
    if governance_state == "active":
        return 1.0
    if governance_state == "approved":
        return 0.9 if review_status in {"verified", "approved"} else 0.75
    if review_status == "verified":
        return 0.6
    if review_status in {"candidate", "extracted"}:
        return 0.3
    if review_status in {"stale", "needs_recheck"}:
        return 0.15
    return 0.2


def _assertion_citation_quality(assertion: PolicyAssertion) -> float:
    citation_json = _loads_json_dict(getattr(assertion, "citation_json", None))
    score = 0.0
    if (getattr(assertion, "source_citation", None) or "").strip():
        score += 0.45
    if citation_json.get("url"):
        score += 0.25
    if citation_json.get("title"):
        score += 0.15
    if citation_json.get("publisher"):
        score += 0.10
    if citation_json.get("raw_excerpt"):
        score += 0.05
    return round(min(score, 1.0), 6)


def _assertion_conflict_count(assertion: PolicyAssertion) -> int:
    rule_status = (getattr(assertion, "rule_status", None) or "").strip().lower()
    coverage_status = (getattr(assertion, "coverage_status", None) or "").strip().lower()
    citation_json = _loads_json_dict(getattr(assertion, "citation_json", None))
    provenance_json = _loads_json_dict(getattr(assertion, "rule_provenance_json", None))
    conflict_hints = []
    for maybe in [citation_json.get("conflict_hints"), provenance_json.get("conflict_hints")]:
        if isinstance(maybe, list):
            conflict_hints.extend(maybe)
    count = 0
    if rule_status == "conflicting":
        count += 1
    if coverage_status == "conflicting":
        count += 1
    count += len(conflict_hints)
    return int(count)


def _authority_expectation_for_category(
    category: str, *, expected_universe: JurisdictionExpectedRuleUniverse | None
) -> str | None:
    if expected_universe is None:
        return None
    expectations = getattr(expected_universe, "authority_expectations", None) or {}
    if not isinstance(expectations, dict):
        return None
    value = expectations.get(category)
    return str(value).strip() if value else None


def _authority_requirement_met(
    *,
    authority_expectation: str | None,
    authoritative_source_count: int,
    authority_score: float,
) -> bool:
    if not authority_expectation:
        return True
    needed = AUTHORITY_EXPECTATION_RANKS.get(str(authority_expectation).strip(), 0)
    if needed <= 0:
        return True
    if needed >= AUTHORITY_EXPECTATION_RANKS["authoritative_official"]:
        return authoritative_source_count > 0
    if needed >= AUTHORITY_EXPECTATION_RANKS["approved_official_supporting"]:
        return authoritative_source_count > 0 or authority_score >= 0.85
    if needed >= AUTHORITY_EXPECTATION_RANKS["semi_authoritative_operational"]:
        return authoritative_source_count > 0 or authority_score >= 0.60
    return authority_score > 0.0


def build_category_assessments(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    required_categories: Iterable[str],
    stale_days: int = DEFAULT_STALE_DAYS,
    pha_name: str | None = None,
    org_id: int | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
    expected_universe: JurisdictionExpectedRuleUniverse | None = None,
) -> dict[str, JurisdictionCategoryAssessment]:
    universe = expected_universe or expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )
    required = normalize_categories(list(universe.required_categories) + list(required_categories))
    thresholds = completeness_scoring_thresholds()

    source_rows = _collect_source_rows_for_scope(db, state=state, county=county, city=city, org_id=org_id, pha_name=pha_name)
    assertion_rows = _collect_assertion_rows_for_scope(db, state=state, county=county, city=city, org_id=org_id, pha_name=pha_name)
    critical_categories = set(get_critical_categories(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ))

    assessments: dict[str, JurisdictionCategoryAssessment] = {}
    for category in required:
        category_sources = [row for row in source_rows if category in _source_categories(row)]
        category_assertions = [row for row in assertion_rows if _assertion_category(row) == category]
        freshness_rows = [_category_freshness_snapshot(row, category=category, stale_days=stale_days) for row in category_sources]
        stale_rows = [snap for snap in freshness_rows if snap['is_stale']]
        legal_stale_rows = [snap for snap in stale_rows if snap['stale_kind'] == 'legal']
        info_stale_rows = [snap for snap in stale_rows if snap['stale_kind'] != 'legal']

        source_count = len(category_sources)
        authoritative_source_count = sum(
            1
            for row in category_sources
            if _source_validation_ok(row)
            and _source_authority_tier(row) == "authoritative_official"
            and _source_use_type(row) == "binding"
        )
        authoritative_stale_source_count = sum(
            1
            for row, snap in zip(category_sources, freshness_rows)
            if _source_validation_ok(row)
            and _source_authority_tier(row) == "authoritative_official"
            and _source_use_type(row) == "binding"
            and snap['is_stale']
        )
        assertion_count = len(category_assertions)
        governed_assertion_count = sum(
            1
            for row in category_assertions
            if _assertion_is_governed(row)
            and _assertion_is_validation_trusted(row)
            and (getattr(row, "coverage_status", None) or "").strip().lower() in {"covered", "verified", "active", "approved"}
            and (getattr(row, "review_status", None) or "").strip().lower() == "verified"
        )
        citation_count = sum(1 for row in category_assertions if _assertion_citation_quality(row) > 0.0)
        authority_score = round(
            sum(_source_authority_score(row) for row in category_sources) / float(max(1, len(category_sources))),
            6,
        ) if category_sources else 0.0
        freshness_score = round(
            sum(float(snap['freshness_score']) for snap in freshness_rows) / float(max(1, len(category_sources))),
            6,
        ) if category_sources else 0.0
        governance_score = round(
            sum(_assertion_governance_score(row) for row in category_assertions) / float(max(1, len(category_assertions))),
            6,
        ) if category_assertions else 0.0
        extraction_score = round(
            sum(_assertion_citation_quality(row) for row in category_assertions) / float(max(1, len(category_assertions))),
            6,
        ) if category_assertions else 0.0
        conflict_count = sum(_assertion_conflict_count(row) for row in category_assertions)
        latest_verified_at = max([ts for ts in [_source_timestamp(row) for row in category_sources] if ts is not None], default=None)

        missing = source_count == 0 and assertion_count == 0
        undiscovered = missing
        conflicting = conflict_count > 0
        legal_stale = bool(legal_stale_rows)
        informational_stale = bool(info_stale_rows)
        stale = (not missing) and bool(stale_rows)
        inferred = not missing and governed_assertion_count <= 0 and assertion_count > 0 and not conflicting and not stale
        authority_expectation = _authority_expectation_for_category(category, expected_universe=universe)
        authority_unmet = (not missing) and (not conflicting) and (not legal_stale) and not _authority_requirement_met(
            authority_expectation=authority_expectation,
            authoritative_source_count=authoritative_source_count,
            authority_score=authority_score,
        )
        weak_support = (
            not missing and not conflicting and not legal_stale and not inferred and (
                governed_assertion_count <= 0
                or citation_count <= 0
                or extraction_score < thresholds.get('citation_quality', 0.55)
                or governance_score < thresholds.get('governance_quality', 0.70)
            )
        )

        unmet_reasons: list[str] = []
        if undiscovered:
            unmet_reasons.append('undiscovered')
        if legal_stale:
            unmet_reasons.append('legal_stale')
        elif informational_stale:
            unmet_reasons.append('informational_stale')
        if conflicting:
            unmet_reasons.append('conflicting')
        if inferred:
            unmet_reasons.append('inferred')
        if authority_unmet:
            unmet_reasons.append('required_authority_not_met')
        if weak_support:
            unmet_reasons.append('weak_support')

        source_backed_covered = bool(authoritative_source_count > 0 and not legal_stale and not conflicting and not authority_unmet)
        if source_backed_covered:
            inferred = False
            weak_support = False
            missing = False
            undiscovered = False

        if missing:
            status = 'missing'
        elif conflicting and not source_backed_covered:
            status = 'conflicting'
        elif legal_stale or informational_stale:
            status = 'stale'
        elif governed_assertion_count > 0 or source_backed_covered:
            status = 'covered'
        elif inferred:
            status = 'inferred'
        elif authority_unmet or weak_support:
            status = 'partial'
        else:
            status = 'partial'

        confidence_score = compute_category_score_from_statuses(
            required_categories=[category],
            category_statuses={category: status},
        ).completeness_score

        assessments[category] = JurisdictionCategoryAssessment(
            category=category,
            status=status,
            source_count=source_count,
            authoritative_source_count=authoritative_source_count,
            assertion_count=assertion_count,
            governed_assertion_count=governed_assertion_count,
            citation_count=citation_count,
            confidence_score=confidence_score,
            extraction_score=extraction_score,
            authority_score=authority_score,
            governance_score=governance_score,
            freshness_score=freshness_score,
            stale_source_count=len(stale_rows),
            authoritative_stale_source_count=authoritative_stale_source_count,
            legal_stale=legal_stale,
            informational_stale=informational_stale,
            conflict_count=conflict_count,
            inferred=inferred,
            stale=stale,
            conflicting=conflicting,
            missing=missing,
            undiscovered=undiscovered,
            weak_support=weak_support,
            authority_unmet=authority_unmet,
            unmet_reason=(unmet_reasons[0] if unmet_reasons else None),
            unmet_reasons=unmet_reasons,
            authority_expectation=authority_expectation,
            latest_verified_at=latest_verified_at,
            source_ids=[int(row.id) for row in category_sources if getattr(row, 'id', None) is not None],
            assertion_ids=[int(row.id) for row in category_assertions if getattr(row, 'id', None) is not None],
        )

    return assessments



def _assessment_has_binding_truth(assessment: JurisdictionCategoryAssessment | None) -> bool:
    if assessment is None:
        return False
    return bool(
        int(getattr(assessment, "authoritative_source_count", 0) or 0) > 0
        and not bool(getattr(assessment, "legal_stale", False))
        and not bool(getattr(assessment, "conflicting", False))
        and not bool(getattr(assessment, "authority_unmet", False))
    )


def _effective_conflicting_categories(*, required: list[str], category_assessments: dict[str, JurisdictionCategoryAssessment]) -> list[str]:
    effective: list[str] = []
    for category in required:
        assessment = category_assessments.get(category)
        if assessment is None or not bool(getattr(assessment, "conflicting", False)):
            continue
        if _assessment_has_binding_truth(assessment) and int(getattr(assessment, "governed_assertion_count", 0) or 0) >= 0:
            continue
        effective.append(category)
    return normalize_categories(effective)


def _effective_missing_categories(*, required: list[str], category_assessments: dict[str, JurisdictionCategoryAssessment]) -> list[str]:
    missing: list[str] = []
    for category in required:
        assessment = category_assessments.get(category)
        if assessment is None:
            missing.append(category)
            continue
        if not bool(getattr(assessment, "missing", False)):
            continue
        if _assessment_has_binding_truth(assessment):
            continue
        missing.append(category)
    return normalize_categories(missing)

def compute_jurisdiction_score_breakdown(
    *,
    required_categories: Iterable[str],
    category_assessments: dict[str, JurisdictionCategoryAssessment],
) -> JurisdictionScoreBreakdown:
    required = normalize_categories(required_categories)
    weights = completeness_score_weights()
    thresholds = completeness_scoring_thresholds()

    if not required:
        return JurisdictionScoreBreakdown(
            overall_completeness=0.0,
            completeness_status='missing',
            coverage_subscore=0.0,
            freshness_subscore=0.0,
            authority_subscore=0.0,
            extraction_subscore=0.0,
            governance_subscore=0.0,
            conflict_penalty=0.0,
            confidence_label='low',
            category_statuses={},
            covered_categories=[],
            stale_categories=[],
            legal_stale_categories=[],
            informational_stale_categories=[],
            critical_stale_categories=[],
            stale_authoritative_categories=[],
            inferred_categories=[],
            conflicting_categories=[],
            missing_categories=[],
            undiscovered_categories=[],
            weak_support_categories=[],
            authority_unmet_categories=[],
            unmet_categories=[],
            category_unmet_reasons={},
            category_details={},
            scoring_defaults={'weights': weights, 'thresholds': thresholds},
        )

    category_statuses = {category: (category_assessments.get(category).status if category in category_assessments else 'missing') for category in required}
    assessments = [category_assessments.get(category) for category in required if category in category_assessments]

    covered_categories = normalize_categories([
        c for c in required
        if category_statuses.get(c) == 'covered' or _assessment_has_binding_truth(category_assessments.get(c))
    ])
    stale_categories = [c for c in required if c in category_assessments and category_assessments[c].stale]
    legal_stale_categories = [c for c in required if c in category_assessments and category_assessments[c].legal_stale]
    informational_stale_categories = [c for c in required if c in category_assessments and category_assessments[c].informational_stale]
    critical_stale_categories = [c for c in required if c in category_assessments and category_assessments[c].legal_stale]
    stale_authoritative_categories = [c for c in required if c in category_assessments and int(category_assessments[c].authoritative_stale_source_count or 0) > 0]
    inferred_categories = [c for c in required if category_statuses.get(c) == 'inferred' and c not in set(covered_categories)]
    conflicting_categories = _effective_conflicting_categories(required=required, category_assessments=category_assessments)
    missing_categories = _effective_missing_categories(required=required, category_assessments=category_assessments)
    undiscovered_categories = [c for c in required if c in category_assessments and category_assessments[c].undiscovered]
    weak_support_categories = [c for c in required if c in category_assessments and category_assessments[c].weak_support]
    authority_unmet_categories = [c for c in required if c in category_assessments and category_assessments[c].authority_unmet]
    unmet_categories = [c for c in required if c in category_assessments and category_assessments[c].unmet_reasons]
    category_unmet_reasons = {c: list(category_assessments[c].unmet_reasons) for c in required if c in category_assessments and category_assessments[c].unmet_reasons}

    coverage_subscore = round(len(covered_categories) / max(1, len(required)), 6)
    freshness_subscore = round(sum(a.freshness_score for a in assessments if a is not None) / float(max(1, len(required))), 6)
    authority_subscore = round(sum(a.authority_score for a in assessments if a is not None) / float(max(1, len(required))), 6)
    extraction_subscore = round(sum(a.extraction_score for a in assessments if a is not None) / float(max(1, len(required))), 6)
    governance_subscore = round(sum(a.governance_score for a in assessments if a is not None) / float(max(1, len(required))), 6)
    conflict_penalty = round(min(0.40, sum(0.10 if a.conflicting else 0.0 for a in assessments if a is not None)), 6)
    legal_stale_penalty = round(min(0.35, 0.08 * len(legal_stale_categories)), 6)
    informational_stale_penalty = round(min(0.15, 0.03 * len(informational_stale_categories)), 6)

    overall = round(max(0.0, min(1.0, ((weights['coverage'] * coverage_subscore) + (weights['freshness'] * freshness_subscore) + (weights['authority'] * authority_subscore) + (weights['extraction'] * extraction_subscore) + (weights['governance'] * governance_subscore) - conflict_penalty - legal_stale_penalty - informational_stale_penalty))), 6)

    if conflicting_categories:
        completeness_status = 'conflicting'
    elif missing_categories:
        completeness_status = 'partial' if covered_categories else 'missing'
    elif legal_stale_categories or informational_stale_categories:
        completeness_status = 'stale'
    elif inferred_categories or weak_support_categories or authority_unmet_categories:
        completeness_status = 'partial'
    else:
        completeness_status = 'complete'

    category_details = {}
    for category, assessment in category_assessments.items():
        if category not in set(required):
            continue
        category_details[category] = {
            'status': assessment.status,
            'source_count': assessment.source_count,
            'authoritative_source_count': assessment.authoritative_source_count,
            'assertion_count': assessment.assertion_count,
            'governed_assertion_count': assessment.governed_assertion_count,
            'citation_count': assessment.citation_count,
            'confidence_score': assessment.confidence_score,
            'extraction_score': assessment.extraction_score,
            'authority_score': assessment.authority_score,
            'governance_score': assessment.governance_score,
            'freshness_score': assessment.freshness_score,
            'stale_source_count': assessment.stale_source_count,
            'authoritative_stale_source_count': assessment.authoritative_stale_source_count,
            'legal_stale': assessment.legal_stale,
            'informational_stale': assessment.informational_stale,
            'conflict_count': assessment.conflict_count,
            'inferred': assessment.inferred,
            'stale': assessment.stale,
            'conflicting': assessment.conflicting,
            'missing': assessment.missing,
            'undiscovered': assessment.undiscovered,
            'weak_support': assessment.weak_support,
            'authority_unmet': assessment.authority_unmet,
            'supporting_only': bool(assessment.authority_unmet and int(assessment.source_count or 0) > 0 and int(assessment.authoritative_source_count or 0) == 0),
            'binding_authority_unmet': bool(assessment.authority_unmet and str(assessment.authority_expectation or '').strip() == 'authoritative_official' and int(assessment.authoritative_source_count or 0) == 0),
            'legally_binding': bool(str(assessment.authority_expectation or '').strip() == 'authoritative_official'),
            'unmet_reason': assessment.unmet_reason,
            'unmet_reasons': assessment.unmet_reasons,
            'authority_expectation': assessment.authority_expectation,
            'latest_verified_at': assessment.latest_verified_at.isoformat() if assessment.latest_verified_at else None,
            'source_ids': assessment.source_ids,
            'assertion_ids': assessment.assertion_ids,
            'trusted_assertion_count': assessment.governed_assertion_count,
            'is_covered': bool(assessment.status == 'covered' or _assessment_has_binding_truth(assessment)),
            'covered': bool(assessment.status == 'covered' or _assessment_has_binding_truth(assessment)),
            'authority_sufficient': _assessment_has_binding_truth(assessment),
            'authoritative_backing_ok': _assessment_has_binding_truth(assessment),
            'conflict_resolved_by_authority': bool(assessment.conflicting and _assessment_has_binding_truth(assessment)),
        }

    return JurisdictionScoreBreakdown(
        overall_completeness=overall,
        completeness_status=completeness_status,
        coverage_subscore=coverage_subscore,
        freshness_subscore=freshness_subscore,
        authority_subscore=authority_subscore,
        extraction_subscore=extraction_subscore,
        governance_subscore=governance_subscore,
        conflict_penalty=round(conflict_penalty + legal_stale_penalty + informational_stale_penalty, 6),
        confidence_label=completeness_confidence_label(overall),
        category_statuses=category_statuses,
        covered_categories=covered_categories,
        stale_categories=stale_categories,
        legal_stale_categories=legal_stale_categories,
        informational_stale_categories=informational_stale_categories,
        critical_stale_categories=critical_stale_categories,
        stale_authoritative_categories=stale_authoritative_categories,
        inferred_categories=inferred_categories,
        conflicting_categories=conflicting_categories,
        missing_categories=missing_categories,
        undiscovered_categories=undiscovered_categories,
        weak_support_categories=weak_support_categories,
        authority_unmet_categories=authority_unmet_categories,
        unmet_categories=unmet_categories,
        category_unmet_reasons=category_unmet_reasons,
        category_details=category_details,
        scoring_defaults={'weights': weights, 'thresholds': thresholds},
    )

def compute_scope_freshness_summary(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> JurisdictionFreshnessSummary:
    rows = _collect_source_rows_for_scope(db, state=state, county=county, city=city)
    source_count = len(rows)
    authoritative_rows = [row for row in rows if bool(getattr(row, 'is_authoritative', False))]
    authoritative_source_count = len(authoritative_rows)

    timestamps = [ts for ts in (_source_timestamp(row) for row in rows) if ts is not None]
    freshest = max(timestamps) if timestamps else None
    oldest = min(timestamps) if timestamps else None

    stale_rows = [row for row in rows if _source_is_stale(row, stale_days=stale_days)]
    category_payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        for category in _source_categories(row):
            snap = _category_freshness_snapshot(row, category=category, stale_days=stale_days)
            entry = category_payload.setdefault(category, {
                'category': category,
                'source_count': 0,
                'authoritative_source_count': 0,
                'stale_source_count': 0,
                'authoritative_stale_source_count': 0,
                'legal_stale': False,
                'informational_stale': False,
            })
            entry['source_count'] += 1
            if bool(getattr(row, 'is_authoritative', False)):
                entry['authoritative_source_count'] += 1
            if snap['is_stale']:
                entry['stale_source_count'] += 1
                if bool(getattr(row, 'is_authoritative', False)):
                    entry['authoritative_stale_source_count'] += 1
                if snap['stale_kind'] == 'legal':
                    entry['legal_stale'] = True
                else:
                    entry['informational_stale'] = True

    legal_stale_categories = sorted([c for c,v in category_payload.items() if v['legal_stale']])
    informational_stale_categories = sorted([c for c,v in category_payload.items() if v['informational_stale']])
    authoritative_stale_categories = sorted([c for c,v in category_payload.items() if int(v['authoritative_stale_source_count'] or 0) > 0])

    is_stale = source_count == 0 or bool(stale_rows)
    stale_reason = None
    if source_count == 0:
        stale_reason = 'no_sources'
    elif legal_stale_categories:
        stale_reason = 'critical_authoritative_categories_past_sla'
    elif authoritative_rows and all(_source_is_stale(row, stale_days=stale_days) for row in authoritative_rows):
        stale_reason = 'authoritative_sources_stale'
    elif stale_rows:
        stale_reason = 'one_or_more_sources_stale'

    payload = {
        'source_count': source_count,
        'authoritative_source_count': authoritative_source_count,
        'freshest_source_at': freshest.isoformat() if freshest else None,
        'oldest_source_at': oldest.isoformat() if oldest else None,
        'stale_source_count': len(stale_rows),
        'stale_reason': stale_reason,
        'is_stale': is_stale,
        'category_freshness': category_payload,
        'legal_stale_categories': legal_stale_categories,
        'informational_stale_categories': informational_stale_categories,
        'authoritative_stale_categories': authoritative_stale_categories,
    }

    return JurisdictionFreshnessSummary(
        source_count=source_count,
        authoritative_source_count=authoritative_source_count,
        freshest_source_at=freshest,
        oldest_source_at=oldest,
        freshness_payload=payload,
        is_stale=is_stale,
        stale_reason=stale_reason,
    )

def compute_profile_completeness(
    db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS
) -> tuple[JurisdictionCompleteness, JurisdictionFreshnessSummary]:
    required_categories = _coalesce_required_categories(profile)
    profile_categories = _collect_covered_categories_from_profile(profile)
    scope_categories = collect_covered_categories_for_scope(
        db, state=profile.state, county=profile.county, city=profile.city
    )
    covered_categories = normalize_categories(profile_categories + scope_categories)
    completeness = compute_category_completeness(
        required_categories=required_categories,
        covered_categories=covered_categories,
    )
    freshness = compute_scope_freshness_summary(
        db,
        state=profile.state,
        county=profile.county,
        city=profile.city,
        stale_days=stale_days,
    )
    return completeness, freshness


def compute_profile_score_breakdown(
    db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS
) -> JurisdictionScoreBreakdown:
    required_categories = _coalesce_required_categories(profile)
    expected_universe = expected_rule_universe_for_scope(
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        include_section8=True,
    )
    scoped_required_categories = normalize_categories(
        list(expected_universe.required_categories) + list(required_categories)
    )
    assessments = build_category_assessments(
        db,
        state=profile.state,
        county=profile.county,
        city=profile.city,
        pha_name=getattr(profile, "pha_name", None),
        required_categories=scoped_required_categories,
        stale_days=stale_days,
        expected_universe=expected_universe,
    )
    breakdown = compute_jurisdiction_score_breakdown(
        required_categories=scoped_required_categories,
        category_assessments=assessments,
    )
    scoring_defaults = dict(breakdown.scoring_defaults or {})
    scoring_defaults["expected_rule_universe"] = expected_universe.to_dict()
    scoring_defaults["rule_family_inventory"] = dict(expected_universe.rule_family_inventory or {})
    scoring_defaults["legally_binding_categories"] = list(expected_universe.legally_binding_categories or [])
    scoring_defaults["operational_heuristic_categories"] = list(expected_universe.operational_heuristic_categories or [])
    scoring_defaults["property_proof_required_categories"] = list(expected_universe.property_proof_required_categories or [])
    scoring_defaults["authority_expectations"] = dict(expected_universe.authority_expectations or {})
    return JurisdictionScoreBreakdown(
        **{
            **breakdown.__dict__,
            "scoring_defaults": scoring_defaults,
        }
    )


def evaluate_jurisdiction_trust_decision(
    *,
    breakdown: JurisdictionScoreBreakdown,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
    trust_defaults: dict[str, Any] | None = None,
) -> JurisdictionTrustDecision:
    policy = merged_hard_trust_defaults(trust_defaults)
    required_categories = normalize_categories(list(breakdown.category_statuses.keys()))
    critical_categories = get_critical_categories(state=state, county=county, city=city, pha_name=pha_name, include_section8=include_section8, tenant_waitlist_depth=tenant_waitlist_depth)
    missing_required = [c for c in list(breakdown.missing_categories) if c not in set(breakdown.covered_categories)]
    stale_categories = list(breakdown.stale_categories)
    legal_stale_categories = list(breakdown.legal_stale_categories)
    informational_stale_categories = list(breakdown.informational_stale_categories)
    conflicting_categories = [c for c in list(breakdown.conflicting_categories) if c not in set(breakdown.covered_categories)]
    inferred_categories = list(breakdown.inferred_categories)
    missing_critical = [c for c in critical_categories if c in set(missing_required)]
    stale_authoritative = list(breakdown.stale_authoritative_categories)
    critical_legal_stale = [c for c in critical_categories if c in set(legal_stale_categories)]
    inferred_critical = [c for c in critical_categories if c in set(inferred_categories)]
    tier_rows = compute_tier_coverage(covered_categories=breakdown.covered_categories, category_statuses=breakdown.category_statuses, state=state, county=county, city=city, pha_name=pha_name, include_section8=include_section8, tenant_waitlist_depth=tenant_waitlist_depth)
    incomplete_required_tiers = [row.jurisdiction_type for row in tier_rows if not row.complete]
    blocker_reasons=[]; manual_review_reasons=[]
    if conflicting_categories:
        blocker_reasons.append('conflicting_categories_present')
    if missing_critical:
        blocker_reasons.append('missing_critical_categories')
    if critical_legal_stale:
        blocker_reasons.append('critical_legal_stale_categories')
    if inferred_critical:
        manual_review_reasons.append('critical_categories_inferred_only')
    if incomplete_required_tiers:
        manual_review_reasons.append('incomplete_required_tiers')
    if stale_authoritative:
        manual_review_reasons.append('stale_authoritative_categories')
    blocked = bool(conflicting_categories or missing_critical or critical_legal_stale)
    safe_for_user_reliance = not blocked and not manual_review_reasons and not missing_required and not legal_stale_categories
    safe_for_projection = not blocked and not missing_critical and not critical_legal_stale
    decision_code = 'blocked_due_to_conflicts' if conflicting_categories else ('blocked_due_to_missing_critical_coverage' if missing_critical else ('blocked_due_to_stale_authoritative_sources' if critical_legal_stale else ('manual_review_required' if manual_review_reasons else 'safe_for_user_reliance')))
    return JurisdictionTrustDecision(decision_code=decision_code, safe_for_projection=safe_for_projection, safe_for_user_reliance=safe_for_user_reliance, blocked=blocked, blocker_reasons=sorted(set(blocker_reasons)), manual_review_reasons=sorted(set(manual_review_reasons)), missing_critical_categories=missing_critical, missing_required_categories=missing_required, stale_categories=stale_categories, stale_authoritative_categories=stale_authoritative, legal_stale_categories=legal_stale_categories, critical_legal_stale_categories=critical_legal_stale, informational_stale_categories=informational_stale_categories, conflicting_categories=conflicting_categories, inferred_categories=inferred_categories, inferred_critical_categories=inferred_critical, incomplete_required_tiers=incomplete_required_tiers, tier_coverage=[asdict(row) if hasattr(row,'__dict__') is False else row.__dict__ for row in tier_rows], required_categories=required_categories, critical_categories=critical_categories, overall_completeness=breakdown.overall_completeness, confidence_label=breakdown.confidence_label, authority_subscore=breakdown.authority_subscore, freshness_subscore=breakdown.freshness_subscore, governance_subscore=breakdown.governance_subscore, conflict_penalty=breakdown.conflict_penalty)


def compute_profile_trust_decision(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> JurisdictionTrustDecision:
    breakdown = compute_profile_score_breakdown(db, profile, stale_days=stale_days)
    return evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        include_section8=True,
        tenant_waitlist_depth=None,
        trust_defaults=_profile_trust_defaults(profile),
    )


def compute_jurisdiction_completeness(
    *,
    required_categories: Iterable[str],
    category_coverage: dict[str, Any],
    stale_status: str = "fresh",
    authoritative_categories: Iterable[str] | None = None,
    critical_categories: Iterable[str] | None = None,
    tier_coverage: list[dict[str, Any]] | None = None,
    trust_defaults: dict[str, Any] | None = None,
    state: str | None = "MI",
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    include_section8: bool = True,
    tenant_waitlist_depth: str | None = None,
) -> dict[str, Any]:
    expected_universe = expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )
    required = normalize_categories(list(expected_universe.required_categories) + list(required_categories))
    coverage = compute_category_score_from_statuses(
        required_categories=required,
        category_statuses=category_coverage or {},
    )

    authoritative_set = set(normalize_categories(authoritative_categories))
    critical_set = set(
        normalize_categories(
            critical_categories
            or get_critical_categories(
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                include_section8=include_section8,
                tenant_waitlist_depth=tenant_waitlist_depth,
            )
        )
    )

    category_statuses = coverage.category_statuses or {}
    conditional_categories = [
        category for category, status in category_statuses.items()
        if str(status or "").strip().lower() == "conditional"
    ]
    stale_categories = list(
        coverage.stale_categories
        or ([] if stale_status != "stale" else list(coverage.covered_categories))
    )
    legal_stale_categories = [c for c in stale_categories if c in critical_set]
    informational_stale_categories = [c for c in stale_categories if c not in critical_set]
    critical_stale_categories = [c for c in stale_categories if c in critical_set]
    stale_authoritative_categories = [c for c in stale_categories if c in authoritative_set]

    category_details = {
        category: {
            "status": category_statuses.get(category, "missing"),
            "authoritative_source_count": 1 if category in authoritative_set else 0,
            "authority_score": 1.0 if category in authoritative_set else 0.0,
            "governance_score": 1.0 if category_statuses.get(category) in {"covered", "verified"} else 0.35,
            "freshness_score": 0.0 if category in set(stale_categories) else 1.0,
        }
        for category in required
    }

    breakdown = JurisdictionScoreBreakdown(
        overall_completeness=coverage.completeness_score,
        completeness_status=coverage.completeness_status if stale_status != "stale" else "stale",
        coverage_subscore=coverage.completeness_score,
        freshness_subscore=0.0 if stale_status == "stale" else 1.0,
        authority_subscore=(
            len(set(coverage.covered_categories).intersection(authoritative_set))
            / float(max(1, len(required)))
        ),
        extraction_subscore=coverage.completeness_score,
        governance_subscore=coverage.completeness_score,
        conflict_penalty=0.10 if coverage.conflicting_categories else 0.0,
        confidence_label=coverage.coverage_confidence,
        category_statuses=category_statuses,
        covered_categories=list(coverage.covered_categories),
        stale_categories=stale_categories,
        legal_stale_categories=legal_stale_categories,
        informational_stale_categories=informational_stale_categories,
        critical_stale_categories=critical_stale_categories,
        stale_authoritative_categories=stale_authoritative_categories,
        inferred_categories=list(coverage.inferred_categories or []),
        conflicting_categories=list(coverage.conflicting_categories or []),
        missing_categories=list(coverage.missing_categories),
        undiscovered_categories=[c for c in coverage.missing_categories],
        weak_support_categories=[],
        authority_unmet_categories=[],
        unmet_categories=[c for c in coverage.missing_categories],
        category_unmet_reasons={c: ["undiscovered"] for c in coverage.missing_categories},
        category_details=category_details,
        scoring_defaults={
            "weights": completeness_score_weights(),
            "thresholds": completeness_scoring_thresholds(),
            **_expected_universe_metadata(expected_universe),
        },
    )

    resolved_tier_coverage = tier_coverage or [
        row.to_dict()
        for row in compute_tier_coverage(
            covered_categories=coverage.covered_categories,
            category_statuses=category_statuses,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            include_section8=include_section8,
            tenant_waitlist_depth=tenant_waitlist_depth,
        )
    ]

    trust = evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
        trust_defaults=trust_defaults,
    )

    if resolved_tier_coverage:
        trust = JurisdictionTrustDecision(
            **{
                **trust.to_dict(),
                "tier_coverage": resolved_tier_coverage,
                "incomplete_required_tiers": [
                    str(row.get("jurisdiction_type"))
                    for row in resolved_tier_coverage
                    if not bool(row.get("complete"))
                ],
            }
        )

    return {
        "required_categories": list(coverage.required_categories),
        "covered_categories": list(coverage.covered_categories),
        "missing_categories": list(coverage.missing_categories),
        "conditional_categories": conditional_categories,
        "stale_categories": stale_categories,
        "stale_status": stale_status,
        "inferred_categories": list(coverage.inferred_categories or []),
        "conflicting_categories": list(coverage.conflicting_categories or []),
        "completeness_score": float(coverage.completeness_score),
        "completeness_status": str(breakdown.completeness_status),
        "coverage_confidence": str(coverage.coverage_confidence),
        "category_statuses": dict(category_statuses),
        "category_coverage": dict(category_statuses),
        "rule_family_inventory": dict(expected_universe.rule_family_inventory or {}),
        "expected_rule_universe": expected_universe.to_dict(),
        "legally_binding_categories": list(expected_universe.legally_binding_categories or []),
        "operational_heuristic_categories": list(expected_universe.operational_heuristic_categories or []),
        "property_proof_required_categories": list(expected_universe.property_proof_required_categories or []),
        "authority_expectations": dict(expected_universe.authority_expectations or {}),
        "trust_decision": trust.to_dict(),
        "safe_for_projection": bool(trust.safe_for_projection),
        "safe_for_user_reliance": bool(trust.safe_for_user_reliance),
        "blocked": bool(trust.blocked),
        "blocker_reasons": list(trust.blocker_reasons),
        "manual_review_reasons": list(trust.manual_review_reasons),
        "tier_coverage": list(resolved_tier_coverage),
        "scoring_defaults": dict(breakdown.scoring_defaults),
    }

def _derive_discovery_status(
    profile: JurisdictionProfile,
    *,
    breakdown: JurisdictionScoreBreakdown,
    freshness: JurisdictionFreshnessSummary,
) -> str:
    meta = _profile_policy_meta(profile)

    explicit = (meta.get("discovery_status") or "").strip().lower()
    if explicit:
        return explicit

    if freshness.source_count == 0:
        return "not_started"
    if breakdown.conflicting_categories:
        return "needs_review"
    if breakdown.missing_categories or breakdown.stale_categories:
        return "incomplete"
    return "ready"


def _last_refresh_value(profile: JurisdictionProfile) -> str | None:
    candidates = [
        getattr(profile, "last_refresh_success_at", None),
        getattr(profile, "last_refresh_attempt_at", None),
        getattr(profile, "updated_at", None),
    ]
    for candidate in candidates:
        if candidate is not None:
            return candidate.isoformat()
    return None


def _last_discovery_run_value(profile: JurisdictionProfile) -> str | None:
    meta = _profile_policy_meta(profile)
    for key in (
        "last_discovery_run",
        "last_discovery_run_id",
        "discovery_run_id",
        "last_discovery_job_id",
    ):
        raw = meta.get(key)
        if raw:
            return str(raw)
    freshness_meta = _loads_json_dict(getattr(profile, "source_freshness_json", None))
    for key in ("last_discovery_run", "last_discovery_run_id", "discovery_run_id"):
        raw = freshness_meta.get(key)
        if raw:
            return str(raw)
    return None


def _last_discovered_at_value(profile: JurisdictionProfile) -> str | None:
    meta = _profile_policy_meta(profile)
    raw = (
        meta.get("last_discovered_at")
        or meta.get("last_discovery_at")
        or meta.get("last_source_discovery_at")
    )
    if raw:
        return str(raw)
    freshest = getattr(profile, "freshest_source_at", None)
    return freshest.isoformat() if freshest is not None else None


def _production_readiness(
    *,
    trust_decision: JurisdictionTrustDecision,
) -> str:
    if trust_decision.safe_for_user_reliance:
        return "ready"
    if trust_decision.safe_for_projection:
        return "caution"
    if trust_decision.manual_review_reasons:
        return "needs_review"
    return "blocked"


def _coverage_status_from_trust_decision(
    *,
    trust_decision: JurisdictionTrustDecision,
    completeness_status: str | None,
    covered_categories: list[str] | None,
    missing_categories: list[str] | None,
    stale_categories: list[str] | None,
    conflicting_categories: list[str] | None,
) -> str:
    completeness_status = str(completeness_status or '').strip().lower()
    covered_categories = list(covered_categories or [])
    missing_categories = list(missing_categories or [])
    stale_categories = list(stale_categories or [])
    conflicting_categories = list(conflicting_categories or [])

    if conflicting_categories or completeness_status == 'conflicting':
        return 'conflicting'
    if trust_decision.safe_for_user_reliance:
        return 'verified_complete'
    if trust_decision.safe_for_projection:
        return 'verified_partial'
    if trust_decision.blocked and (trust_decision.missing_critical_categories or trust_decision.critical_legal_stale_categories):
        return 'critical_gaps'
    if trust_decision.manual_review_reasons:
        return 'needs_review'
    if covered_categories and not missing_categories and not stale_categories:
        return 'covered_unverified'
    if covered_categories:
        return 'partial'
    return 'missing'


def apply_profile_completeness(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    commit: bool = False,
) -> JurisdictionProfile:
    breakdown = compute_profile_score_breakdown(db, profile, stale_days=stale_days)
    freshness = compute_scope_freshness_summary(
        db,
        state=profile.state,
        county=profile.county,
        city=profile.city,
        stale_days=stale_days,
    )
    trust_decision = evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        include_section8=True,
        trust_defaults=_profile_trust_defaults(profile),
    )

    profile.required_categories_json = _dumps(list(breakdown.category_statuses.keys()))
    profile.covered_categories_json = _dumps(breakdown.covered_categories)
    profile.missing_categories_json = _dumps(breakdown.missing_categories)
    profile.stale_categories_json = _dumps(breakdown.stale_categories)
    if hasattr(profile, 'critical_stale_categories_json'):
        profile.critical_stale_categories_json = _dumps(breakdown.critical_stale_categories)
    profile.inferred_categories_json = _dumps(breakdown.inferred_categories)
    profile.conflicting_categories_json = _dumps(breakdown.conflicting_categories)
    profile.expected_rule_universe_json = _dumps((breakdown.scoring_defaults or {}).get("expected_rule_universe", {})) if hasattr(profile, "expected_rule_universe_json") else getattr(profile, "expected_rule_universe_json", "{}")
    profile.category_coverage_details_json = _dumps(breakdown.category_details) if hasattr(profile, "category_coverage_details_json") else getattr(profile, "category_coverage_details_json", "{}")
    profile.category_unmet_reasons_json = _dumps(breakdown.category_unmet_reasons) if hasattr(profile, "category_unmet_reasons_json") else getattr(profile, "category_unmet_reasons_json", "{}")
    profile.unmet_categories_json = _dumps(breakdown.unmet_categories) if hasattr(profile, "unmet_categories_json") else getattr(profile, "unmet_categories_json", "[]")
    profile.undiscovered_categories_json = _dumps(breakdown.undiscovered_categories) if hasattr(profile, "undiscovered_categories_json") else getattr(profile, "undiscovered_categories_json", "[]")
    profile.weak_support_categories_json = _dumps(breakdown.weak_support_categories) if hasattr(profile, "weak_support_categories_json") else getattr(profile, "weak_support_categories_json", "[]")
    profile.authority_unmet_categories_json = _dumps(breakdown.authority_unmet_categories) if hasattr(profile, "authority_unmet_categories_json") else getattr(profile, "authority_unmet_categories_json", "[]")
    profile.completeness_snapshot_json = _dumps(
        {
            "completeness_status": breakdown.completeness_status,
            "completeness_score": breakdown.overall_completeness,
            "confidence_label": breakdown.confidence_label,
            "required_categories": list(breakdown.category_statuses.keys()),
            "covered_categories": breakdown.covered_categories,
            "missing_categories": breakdown.missing_categories,
            "stale_categories": breakdown.stale_categories,
        "legal_stale_categories": breakdown.legal_stale_categories,
        "informational_stale_categories": breakdown.informational_stale_categories,
        "critical_stale_categories": breakdown.critical_stale_categories,
        "stale_authoritative_categories": breakdown.stale_authoritative_categories,
            "legal_stale_categories": breakdown.legal_stale_categories,
            "informational_stale_categories": breakdown.informational_stale_categories,
            "critical_stale_categories": breakdown.critical_stale_categories,
            "stale_authoritative_categories": breakdown.stale_authoritative_categories,
            "inferred_categories": breakdown.inferred_categories,
            "conflicting_categories": breakdown.conflicting_categories,
            "undiscovered_categories": breakdown.undiscovered_categories,
            "weak_support_categories": breakdown.weak_support_categories,
            "authority_unmet_categories": breakdown.authority_unmet_categories,
            "unmet_categories": breakdown.unmet_categories,
            "category_statuses": breakdown.category_statuses,
            "category_details": breakdown.category_details,
            "category_unmet_reasons": breakdown.category_unmet_reasons,
            "scoring_defaults": breakdown.scoring_defaults,
        }
    )
    profile.completeness_score = breakdown.overall_completeness
    profile.completeness_status = breakdown.completeness_status
    profile.category_norm_version = getattr(profile, "category_norm_version", None) or "v2"
    profile.source_count = freshness.source_count
    profile.authoritative_source_count = freshness.authoritative_source_count
    profile.freshest_source_at = freshness.freshest_source_at
    profile.oldest_source_at = freshness.oldest_source_at

    discovery_status = _derive_discovery_status(
        profile,
        breakdown=breakdown,
        freshness=freshness,
    )
    production_readiness = _production_readiness(trust_decision=trust_decision)
    coverage_status = _coverage_status_from_trust_decision(
        trust_decision=trust_decision,
        completeness_status=breakdown.completeness_status,
        covered_categories=breakdown.covered_categories,
        missing_categories=breakdown.missing_categories,
        stale_categories=breakdown.stale_categories,
        conflicting_categories=breakdown.conflicting_categories,
    )

    if hasattr(profile, 'production_readiness'):
        profile.production_readiness = production_readiness
    if hasattr(profile, 'discovery_status'):
        profile.discovery_status = discovery_status
    if hasattr(profile, 'coverage_status'):
        profile.coverage_status = coverage_status

    profile.is_stale = bool(freshness.is_stale)
    profile.stale_reason = freshness.stale_reason
    profile.source_freshness_json = _dumps(
        {
            **freshness.freshness_payload,
            "scoring": {
                "overall_completeness": breakdown.overall_completeness,
                "coverage_subscore": breakdown.coverage_subscore,
                "freshness_subscore": breakdown.freshness_subscore,
                "authority_subscore": breakdown.authority_subscore,
                "extraction_subscore": breakdown.extraction_subscore,
                "governance_subscore": breakdown.governance_subscore,
                "conflict_penalty": breakdown.conflict_penalty,
                "confidence_label": breakdown.confidence_label,
                "category_statuses": breakdown.category_statuses,
                "missing_categories": breakdown.missing_categories,
                "stale_categories": breakdown.stale_categories,
        "legal_stale_categories": breakdown.legal_stale_categories,
        "informational_stale_categories": breakdown.informational_stale_categories,
        "critical_stale_categories": breakdown.critical_stale_categories,
        "stale_authoritative_categories": breakdown.stale_authoritative_categories,
                "legal_stale_categories": breakdown.legal_stale_categories,
                "informational_stale_categories": breakdown.informational_stale_categories,
                "critical_stale_categories": breakdown.critical_stale_categories,
                "stale_authoritative_categories": breakdown.stale_authoritative_categories,
            "legal_stale_categories": breakdown.legal_stale_categories,
            "informational_stale_categories": breakdown.informational_stale_categories,
            "critical_stale_categories": breakdown.critical_stale_categories,
            "stale_authoritative_categories": breakdown.stale_authoritative_categories,
                "inferred_categories": breakdown.inferred_categories,
                "conflicting_categories": breakdown.conflicting_categories,
                "trust_decision": trust_decision.to_dict(),
            },
        }
    )

    rollup_payload = {
        "overall_completeness": breakdown.overall_completeness,
        "completeness_status": breakdown.completeness_status,
        "coverage_confidence": breakdown.confidence_label,
        "required_categories": list(breakdown.category_statuses.keys()),
        "covered_categories": breakdown.covered_categories,
        "missing_categories": breakdown.missing_categories,
        "stale_categories": breakdown.stale_categories,
        "legal_stale_categories": breakdown.legal_stale_categories,
        "informational_stale_categories": breakdown.informational_stale_categories,
        "critical_stale_categories": breakdown.critical_stale_categories,
        "stale_authoritative_categories": breakdown.stale_authoritative_categories,
        "inferred_categories": breakdown.inferred_categories,
        "conflicting_categories": breakdown.conflicting_categories,
        "production_readiness": production_readiness,
        "trustworthy_for_projection": trust_decision.safe_for_projection,
        "safe_for_user_reliance": trust_decision.safe_for_user_reliance,
        "trust_decision": trust_decision.to_dict(),
        "discovery_status": discovery_status,
        "last_refresh": _last_refresh_value(profile),
        "last_discovery_run": _last_discovery_run_value(profile),
        "last_discovered_at": _last_discovered_at_value(profile),
        "last_verified_at": profile.last_verified_at.isoformat()
        if getattr(profile, "last_verified_at", None)
        else None,
    }
    merge_profile_policy_meta(
        profile,
        {
            "completeness": rollup_payload,
            "coverage_confidence": breakdown.confidence_label,
            "missing_local_rule_areas": breakdown.missing_categories,
            "legal_stale_categories": breakdown.legal_stale_categories,
            "informational_stale_categories": breakdown.informational_stale_categories,
            "critical_stale_categories": breakdown.critical_stale_categories,
            "stale_authoritative_categories": breakdown.stale_authoritative_categories,
            "last_refreshed": rollup_payload["last_refresh"],
            "discovery_status": discovery_status,
            "last_discovery_run": rollup_payload["last_discovery_run"],
            "last_discovered_at": rollup_payload["last_discovered_at"],
            "production_readiness": production_readiness,
            "trustworthy_for_projection": trust_decision.safe_for_projection,
            "safe_for_user_reliance": trust_decision.safe_for_user_reliance,
            "trust_decision": trust_decision.to_dict(),
            "undiscovered_categories": breakdown.undiscovered_categories,
            "weak_support_categories": breakdown.weak_support_categories,
            "authority_unmet_categories": breakdown.authority_unmet_categories,
            "unmet_categories": breakdown.unmet_categories,
            "category_unmet_reasons": breakdown.category_unmet_reasons,
        },
    )

    db.add(profile)
    if commit:
        db.commit()
        db.refresh(profile)
    else:
        db.flush()
    return profile


def get_or_create_coverage_status_for_profile(
    db: Session, profile: JurisdictionProfile
) -> JurisdictionCoverageStatus:
    st = _norm_state(getattr(profile, "state", None))
    cnty = _norm_lower(getattr(profile, "county", None))
    cty = _norm_lower(getattr(profile, "city", None))
    pha = _profile_pha_name(profile)
    org_id = getattr(profile, "org_id", None)

    stmt = select(JurisdictionCoverageStatus).where(
        JurisdictionCoverageStatus.org_id == org_id,
        JurisdictionCoverageStatus.state == st,
        JurisdictionCoverageStatus.county == cnty,
        JurisdictionCoverageStatus.city == cty,
    )
    if _coverage_has_attr("pha_name"):
        stmt = stmt.where(JurisdictionCoverageStatus.pha_name == pha)

    coverage = db.execute(stmt).scalar_one_or_none()
    if coverage is not None:
        return coverage

    kwargs: dict[str, Any] = {
        "org_id": org_id,
        "state": st or "MI",
        "county": cnty,
        "city": cty,
        "coverage_version": "v1",
        "completeness_status": "unknown",
        "completeness_score": 0.0,
        "confidence_score": 0.0,
        "covered_categories_json": [],
        "missing_categories_json": [],
        "stale_categories_json": [],
        "inferred_categories_json": [],
        "conflicting_categories_json": [],
        "required_categories_json": [],
        "category_coverage_snapshot_json": {},
        "category_last_verified_json": {},
        "category_source_backing_json": {},
        "completeness_snapshot_json": {},
        "expected_rule_universe_json": {},
        "category_coverage_details_json": {},
        "category_unmet_reasons_json": {},
        "unmet_categories_json": [],
        "undiscovered_categories_json": [],
        "weak_support_categories_json": [],
        "authority_unmet_categories_json": [],
        "source_ids_json": [],
        "source_summary_json": {},
        "source_freshness_json": _as_json_storage("source_freshness_json", {}),
        "authority_score": 0.0,
        "extraction_confidence": 0.0,
        "conflict_count": 0,
        "production_readiness": "not_ready",
        "discovery_status": "not_started",
        "is_stale": False,
        "discovery_metadata_json": {},
        "metadata_json": {},
    }
    if _coverage_has_attr("pha_name"):
        kwargs["pha_name"] = pha

    coverage = JurisdictionCoverageStatus(**kwargs)
    db.add(coverage)
    db.flush()
    return coverage


def sync_coverage_status_from_profile(
    db: Session, profile: JurisdictionProfile, *, commit: bool = False
) -> JurisdictionCoverageStatus:
    coverage = get_or_create_coverage_status_for_profile(db, profile)
    detail = _loads_json_dict(getattr(profile, "source_freshness_json", None)).get("scoring", {})
    trust_decision = detail.get("trust_decision") or {}

    coverage.completeness_score = float(getattr(profile, "completeness_score", 0.0) or 0.0)
    coverage.confidence_score = float(
        getattr(profile, "confidence_score", None)
        or detail.get("confidence_label") is not None and coverage.completeness_score
        or getattr(profile, "completeness_score", 0.0)
        or 0.0
    )
    coverage.completeness_status = getattr(profile, "completeness_status", None) or "missing"
    coverage.coverage_status = (
        _coverage_status_from_trust_decision(
            trust_decision=JurisdictionTrustDecision(
                decision_code=str(trust_decision.get("decision_code") or "manual_review_required"),
                safe_for_projection=bool(trust_decision.get("safe_for_projection", False)),
                safe_for_user_reliance=bool(trust_decision.get("safe_for_user_reliance", False)),
                blocked=bool(trust_decision.get("blocked", False)),
                blocker_reasons=list(trust_decision.get("blocker_reasons") or []),
                manual_review_reasons=list(trust_decision.get("manual_review_reasons") or []),
                missing_critical_categories=list(trust_decision.get("missing_critical_categories") or []),
                missing_required_categories=list(trust_decision.get("missing_required_categories") or []),
                stale_categories=list(trust_decision.get("stale_categories") or []),
                stale_authoritative_categories=list(trust_decision.get("stale_authoritative_categories") or []),
                legal_stale_categories=list(trust_decision.get("legal_stale_categories") or []),
                critical_legal_stale_categories=list(trust_decision.get("critical_legal_stale_categories") or []),
                informational_stale_categories=list(trust_decision.get("informational_stale_categories") or []),
                conflicting_categories=list(trust_decision.get("conflicting_categories") or []),
                inferred_categories=list(trust_decision.get("inferred_categories") or []),
                inferred_critical_categories=list(trust_decision.get("inferred_critical_categories") or []),
                incomplete_required_tiers=list(trust_decision.get("incomplete_required_tiers") or []),
                tier_coverage=list(trust_decision.get("tier_coverage") or []),
                required_categories=list(trust_decision.get("required_categories") or []),
                critical_categories=list(trust_decision.get("critical_categories") or []),
                overall_completeness=float(trust_decision.get("overall_completeness") or 0.0),
                confidence_label=str(trust_decision.get("confidence_label") or "low"),
                authority_subscore=float(trust_decision.get("authority_subscore") or 0.0),
                freshness_subscore=float(trust_decision.get("freshness_subscore") or 0.0),
                governance_subscore=float(trust_decision.get("governance_subscore") or 0.0),
                conflict_penalty=float(trust_decision.get("conflict_penalty") or 0.0),
            ),
            completeness_status=getattr(profile, "completeness_status", None),
            covered_categories=_loads_json_list(getattr(profile, "covered_categories_json", None)),
            missing_categories=_loads_json_list(getattr(profile, "missing_categories_json", None)),
            stale_categories=_loads_json_list(getattr(profile, "stale_categories_json", None)),
            conflicting_categories=_loads_json_list(getattr(profile, "conflicting_categories_json", None)),
        )
        if trust_decision else (getattr(profile, "coverage_status", None) or "missing")
    )
    coverage.coverage_version = getattr(profile, "category_norm_version", None) or "v2"
    coverage.production_readiness = (
        getattr(profile, "production_readiness", None)
        or trust_decision.get("production_readiness")
        or "not_ready"
    )
    coverage.discovery_status = getattr(profile, "discovery_status", None) or "not_started"
    coverage.is_stale = bool(getattr(profile, "is_stale", False))

    coverage.covered_categories_json = _loads_json_list(getattr(profile, "covered_categories_json", None))
    coverage.missing_categories_json = _loads_json_list(getattr(profile, "missing_categories_json", None))
    coverage.stale_categories_json = _loads_json_list(getattr(profile, "stale_categories_json", None))
    coverage.inferred_categories_json = _loads_json_list(getattr(profile, "inferred_categories_json", None))
    coverage.conflicting_categories_json = _loads_json_list(getattr(profile, "conflicting_categories_json", None))
    coverage.required_categories_json = _loads_json_list(getattr(profile, "required_categories_json", None))
    coverage.unmet_categories_json = _loads_json_list(getattr(profile, "unmet_categories_json", None))
    coverage.undiscovered_categories_json = _loads_json_list(getattr(profile, "undiscovered_categories_json", None))
    coverage.weak_support_categories_json = _loads_json_list(getattr(profile, "weak_support_categories_json", None))
    coverage.authority_unmet_categories_json = _loads_json_list(getattr(profile, "authority_unmet_categories_json", None))

    coverage.category_coverage_snapshot_json = _loads_json_dict(
        getattr(profile, "category_coverage_snapshot_json", None)
    )
    coverage.category_last_verified_json = _loads_json_dict(
        getattr(profile, "category_last_verified_json", None)
    )
    coverage.category_source_backing_json = _loads_json_dict(
        getattr(profile, "category_source_backing_json", None)
    )

    coverage.completeness_snapshot_json = _loads_json_dict(
        getattr(profile, "completeness_snapshot_json", None)
    )
    coverage.expected_rule_universe_json = _loads_json_dict(
        getattr(profile, "expected_rule_universe_json", None)
    )
    coverage.category_coverage_details_json = _loads_json_dict(
        getattr(profile, "category_coverage_details_json", None)
    )
    coverage.category_unmet_reasons_json = _loads_json_dict(
        getattr(profile, "category_unmet_reasons_json", None)
    )

    coverage.is_stale = bool(getattr(profile, "is_stale", False))
    coverage.stale_reason = getattr(profile, "stale_reason", None)
    coverage.last_computed_at = _utcnow()
    coverage.last_source_change_at = getattr(profile, "freshest_source_at", None)

    source_ids: list[int] = []
    category_details = detail.get("category_details") or {}
    if isinstance(category_details, dict):
        for row in category_details.values():
            if isinstance(row, dict):
                for sid in list(row.get("source_ids") or []):
                    try:
                        source_ids.append(int(sid))
                    except Exception:
                        pass
    coverage.source_ids_json = sorted(set(source_ids))
    coverage.source_summary_json = {
        "category_statuses": detail.get("category_statuses", {}),
        "stale_categories": detail.get("stale_categories", []),
        "inferred_categories": detail.get("inferred_categories", []),
        "conflicting_categories": detail.get("conflicting_categories", []),
        "undiscovered_categories": detail.get("undiscovered_categories", []),
        "weak_support_categories": detail.get("weak_support_categories", []),
        "authority_unmet_categories": detail.get("authority_unmet_categories", []),
        "unmet_categories": detail.get("unmet_categories", []),
        "category_unmet_reasons": detail.get("category_unmet_reasons", {}),
        "trust_decision": trust_decision,
    }
    if _coverage_has_attr("source_freshness_json"):
        coverage.source_freshness_json = _as_json_storage(
            "source_freshness_json",
            _loads_json_dict(getattr(profile, "source_freshness_json", None))
            or {
                "source_count": 0,
                "authoritative_source_count": 0,
                "stale_source_count": 0,
                "stale_reason": getattr(profile, "stale_reason", None),
                "is_stale": bool(getattr(profile, "is_stale", False)),
            },
        )

    db.add(coverage)
    if commit:
        db.commit()
        db.refresh(coverage)
    else:
        db.flush()
    return coverage

def profile_completeness_payload(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS) -> dict[str, Any]:
    breakdown = compute_profile_score_breakdown(db, profile, stale_days=stale_days)
    freshness = compute_scope_freshness_summary(
        db,
        state=getattr(profile, 'state', None),
        county=getattr(profile, 'county', None),
        city=getattr(profile, 'city', None),
        stale_days=stale_days,
    )
    trust = evaluate_jurisdiction_trust_decision(
        breakdown=breakdown,
        state=getattr(profile, 'state', None),
        county=getattr(profile, 'county', None),
        city=getattr(profile, 'city', None),
        pha_name=getattr(profile, 'pha_name', None),
        include_section8=True,
        trust_defaults=_profile_trust_defaults(profile),
    )
    expected_universe = (breakdown.scoring_defaults or {}).get('expected_rule_universe', {})
    return {
        'jurisdiction_profile_id': int(getattr(profile, 'id', 0) or 0),
        'completeness_score': breakdown.overall_completeness,
        'completeness_status': breakdown.completeness_status,
        'confidence_label': breakdown.confidence_label,
        'coverage_subscore': breakdown.coverage_subscore,
        'freshness_subscore': breakdown.freshness_subscore,
        'authority_subscore': breakdown.authority_subscore,
        'extraction_subscore': breakdown.extraction_subscore,
        'governance_subscore': breakdown.governance_subscore,
        'conflict_penalty': breakdown.conflict_penalty,
        'required_categories': list(breakdown.category_statuses.keys()),
        'covered_categories': breakdown.covered_categories,
        'missing_categories': breakdown.missing_categories,
        'stale_categories': breakdown.stale_categories,
        'legal_stale_categories': breakdown.legal_stale_categories,
        'informational_stale_categories': breakdown.informational_stale_categories,
        'critical_stale_categories': breakdown.critical_stale_categories,
        'stale_authoritative_categories': breakdown.stale_authoritative_categories,
        'inferred_categories': breakdown.inferred_categories,
        'conflicting_categories': breakdown.conflicting_categories,
        'undiscovered_categories': breakdown.undiscovered_categories,
        'weak_support_categories': breakdown.weak_support_categories,
        'authority_unmet_categories': breakdown.authority_unmet_categories,
        'supporting_only_categories': [c for c, detail in (breakdown.category_details or {}).items() if bool((detail or {}).get('supporting_only'))],
        'binding_unmet_categories': [c for c, detail in (breakdown.category_details or {}).items() if bool((detail or {}).get('binding_authority_unmet'))],
        'legally_binding_missing_authority_categories': [c for c, detail in (breakdown.category_details or {}).items() if bool((detail or {}).get('legally_binding')) and bool((detail or {}).get('binding_authority_unmet'))],
        'unmet_categories': breakdown.unmet_categories,
        'category_unmet_reasons': breakdown.category_unmet_reasons,
        'category_statuses': breakdown.category_statuses,
        'category_details': breakdown.category_details,
        'is_stale': freshness.is_stale,
        'stale_reason': freshness.stale_reason,
        'source_freshness': freshness.freshness_payload,
        'freshness_summary': freshness.freshness_payload,
        'trust_decision': trust.to_dict(),
        'safe_for_projection': trust.safe_for_projection,
        'safe_for_user_reliance': trust.safe_for_user_reliance,
        'expected_rule_universe': expected_universe,
        'required_categories_by_tier': dict((breakdown.scoring_defaults or {}).get('required_categories_by_tier', {}) or {}),
        'expected_rules_by_category': dict((breakdown.scoring_defaults or {}).get('expected_rules_by_category', {}) or {}),
        'rule_family_inventory': dict((breakdown.scoring_defaults or {}).get('rule_family_inventory', {}) or {}),
        'legally_binding_categories': list((breakdown.scoring_defaults or {}).get('legally_binding_categories', []) or []),
        'operational_heuristic_categories': list((breakdown.scoring_defaults or {}).get('operational_heuristic_categories', []) or []),
        'property_proof_required_categories': list((breakdown.scoring_defaults or {}).get('property_proof_required_categories', []) or []),
        'authority_expectations': dict((breakdown.scoring_defaults or {}).get('authority_expectations', {}) or {}),
    }


def recompute_profile_and_coverage(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    commit: bool = False,
) -> tuple[JurisdictionProfile, JurisdictionCoverageStatus]:
    profile = apply_profile_completeness(db, profile, stale_days=stale_days, commit=False)
    coverage = sync_coverage_status_from_profile(db, profile, commit=False)
    if commit:
        db.commit()
        db.refresh(profile)
        db.refresh(coverage)
    else:
        db.flush()
    return profile, coverage


# --- surgical final coverage enforcement + trust gate overlay ---

try:
    _coverage_enforcement_original_compute_profile_score_breakdown = compute_profile_score_breakdown
except NameError:
    _coverage_enforcement_original_compute_profile_score_breakdown = None

def _breakdown_with_enforced_missing_required_optional(
    breakdown: JurisdictionScoreBreakdown,
) -> JurisdictionScoreBreakdown:
    scoring_defaults = dict(getattr(breakdown, "scoring_defaults", {}) or {})
    expected_universe = dict(scoring_defaults.get("expected_rule_universe") or {})
    required_by_tier = dict(scoring_defaults.get("required_categories_by_tier") or expected_universe.get("required_categories_by_tier") or {})
    optional_by_tier = dict(expected_universe.get("optional_categories_by_tier") or {})
    required_categories = normalize_categories(
        list(getattr(breakdown, "category_statuses", {}).keys())
        or list(expected_universe.get("required_categories") or [])
    )
    optional_categories = normalize_categories(expected_universe.get("optional_categories") or [])
    category_statuses = dict(getattr(breakdown, "category_statuses", {}) or {})
    category_details = dict(getattr(breakdown, "category_details", {}) or {})

    enforced_missing = set(normalize_categories(getattr(breakdown, "missing_categories", []) or []))
    for category in required_categories:
        status = str(category_statuses.get(category) or "").strip().lower()
        detail = dict(category_details.get(category) or {})
        if status in {"", "missing"} or bool(detail.get("missing")) or bool(detail.get("undiscovered")):
            enforced_missing.add(category)

    stale_authoritative = normalize_categories(getattr(breakdown, "stale_authoritative_categories", []) or [])
    conflicting_categories = normalize_categories(getattr(breakdown, "conflicting_categories", []) or [])

    scoring_defaults["required_categories_by_tier"] = required_by_tier
    scoring_defaults["optional_categories_by_tier"] = optional_by_tier
    scoring_defaults["required_categories"] = required_categories
    scoring_defaults["optional_categories"] = optional_categories

    completeness_status = str(getattr(breakdown, "completeness_status", "missing") or "missing").strip().lower()
    if conflicting_categories:
        completeness_status = "conflicting"
    elif enforced_missing:
        completeness_status = "partial"

    return JurisdictionScoreBreakdown(
        **{
            **breakdown.__dict__,
            "missing_categories": normalize_categories(sorted(enforced_missing)),
            "stale_authoritative_categories": stale_authoritative,
            "conflicting_categories": conflicting_categories,
            "completeness_status": completeness_status,
            "scoring_defaults": scoring_defaults,
        }
    )

if _coverage_enforcement_original_compute_profile_score_breakdown is not None:
    def compute_profile_score_breakdown(
        db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS
    ) -> JurisdictionScoreBreakdown:
        original = _coverage_enforcement_original_compute_profile_score_breakdown(
            db, profile, stale_days=stale_days
        )
        return _breakdown_with_enforced_missing_required_optional(original)

try:
    _coverage_enforcement_original_evaluate_jurisdiction_trust_decision = evaluate_jurisdiction_trust_decision
except NameError:
    _coverage_enforcement_original_evaluate_jurisdiction_trust_decision = None

def _critical_missing_binding_categories_from_breakdown(
    *, breakdown: JurisdictionScoreBreakdown, critical_categories: list[str]
) -> list[str]:
    out: list[str] = []
    details = dict(getattr(breakdown, "category_details", {}) or {})
    for category in critical_categories:
        detail = dict(details.get(category) or {})
        if category in set(getattr(breakdown, "missing_categories", []) or []):
            out.append(category)
            continue
        if bool(detail.get("binding_authority_unmet")) or bool(detail.get("authority_unmet")):
            out.append(category)
            continue
        if bool(detail.get("legally_binding")) and not bool(detail.get("source_backed_covered")):
            if bool(detail.get("missing")) or bool(detail.get("weak_support")) or bool(detail.get("inferred")):
                out.append(category)
    return normalize_categories(out)

if _coverage_enforcement_original_evaluate_jurisdiction_trust_decision is not None:
    def evaluate_jurisdiction_trust_decision(
        *,
        breakdown: JurisdictionScoreBreakdown,
        state: str | None = None,
        county: str | None = None,
        city: str | None = None,
        pha_name: str | None = None,
        include_section8: bool = True,
        tenant_waitlist_depth: str | None = None,
        trust_defaults: dict[str, Any] | None = None,
    ) -> JurisdictionTrustDecision:
        original = _coverage_enforcement_original_evaluate_jurisdiction_trust_decision(
            breakdown=breakdown,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            include_section8=include_section8,
            tenant_waitlist_depth=tenant_waitlist_depth,
            trust_defaults=trust_defaults,
        )
        critical_categories = list(getattr(original, "critical_categories", []) or [])
        conflicting_categories = normalize_categories(getattr(original, "conflicting_categories", []) or [])
        stale_authoritative_categories = normalize_categories(getattr(original, "stale_authoritative_categories", []) or [])
        critical_missing_binding = _critical_missing_binding_categories_from_breakdown(
            breakdown=breakdown,
            critical_categories=critical_categories,
        )
        blocker_reasons = list(getattr(original, "blocker_reasons", []) or [])
        manual_review_reasons = list(getattr(original, "manual_review_reasons", []) or [])

        if conflicting_categories and "conflicting_categories_present" not in blocker_reasons:
            blocker_reasons.append("conflicting_categories_present")
        if stale_authoritative_categories and "stale_authoritative_sources_present" not in blocker_reasons:
            blocker_reasons.append("stale_authoritative_sources_present")
        if critical_missing_binding and "critical_categories_missing_binding_authority" not in blocker_reasons:
            blocker_reasons.append("critical_categories_missing_binding_authority")

        blocked = bool(getattr(original, "blocked", False) or conflicting_categories or stale_authoritative_categories or critical_missing_binding)
        safe_for_projection = bool(getattr(original, "safe_for_projection", False)) and not blocked
        safe_for_user_reliance = bool(getattr(original, "safe_for_user_reliance", False)) and not blocked

        if blocked and not manual_review_reasons and conflicting_categories:
            manual_review_reasons.append("resolve_conflicting_categories")

        missing_critical = normalize_categories(
            list(getattr(original, "missing_critical_categories", []) or []) + critical_missing_binding
        )
        missing_required = normalize_categories(
            list(getattr(original, "missing_required_categories", []) or []) + critical_missing_binding
        )

        decision_code = str(getattr(original, "decision_code", "") or "").strip() or "manual_review_required"
        if conflicting_categories:
            decision_code = "blocked_due_to_unresolved_conflicts"
        elif stale_authoritative_categories:
            decision_code = "blocked_due_to_stale_authoritative_sources"
        elif critical_missing_binding:
            decision_code = "blocked_due_to_missing_critical_coverage"
        elif blocked and not decision_code:
            decision_code = "manual_review_required"

        return JurisdictionTrustDecision(
            decision_code=decision_code,
            safe_for_projection=safe_for_projection,
            safe_for_user_reliance=safe_for_user_reliance,
            blocked=blocked,
            blocker_reasons=sorted(set(blocker_reasons)),
            manual_review_reasons=sorted(set(manual_review_reasons)),
            missing_critical_categories=missing_critical,
            missing_required_categories=missing_required,
            stale_categories=list(getattr(original, "stale_categories", []) or []),
            stale_authoritative_categories=stale_authoritative_categories,
            legal_stale_categories=list(getattr(original, "legal_stale_categories", []) or []),
            critical_legal_stale_categories=list(getattr(original, "critical_legal_stale_categories", []) or []),
            informational_stale_categories=list(getattr(original, "informational_stale_categories", []) or []),
            conflicting_categories=conflicting_categories,
            inferred_categories=list(getattr(original, "inferred_categories", []) or []),
            inferred_critical_categories=list(getattr(original, "inferred_critical_categories", []) or []),
            incomplete_required_tiers=list(getattr(original, "incomplete_required_tiers", []) or []),
            tier_coverage=list(getattr(original, "tier_coverage", []) or []),
            required_categories=list(getattr(original, "required_categories", []) or []),
            critical_categories=critical_categories,
            overall_completeness=float(getattr(original, "overall_completeness", 0.0) or 0.0),
            confidence_label=str(getattr(original, "confidence_label", "low") or "low"),
            authority_subscore=float(getattr(original, "authority_subscore", 0.0) or 0.0),
            freshness_subscore=float(getattr(original, "freshness_subscore", 0.0) or 0.0),
            governance_subscore=float(getattr(original, "governance_subscore", 0.0) or 0.0),
            conflict_penalty=float(getattr(original, "conflict_penalty", 0.0) or 0.0),
        )

try:
    _coverage_enforcement_original_production_readiness = _production_readiness
except NameError:
    _coverage_enforcement_original_production_readiness = None

if _coverage_enforcement_original_production_readiness is not None:
    def _production_readiness(
        *,
        trust_decision: JurisdictionTrustDecision,
    ) -> str:
        if list(getattr(trust_decision, "conflicting_categories", []) or []):
            return "not_ready"
        if list(getattr(trust_decision, "stale_authoritative_categories", []) or []):
            return "not_ready"
        if list(getattr(trust_decision, "missing_critical_categories", []) or []):
            return "not_ready"
        if bool(getattr(trust_decision, "blocked", False)):
            return "not_ready"
        return _coverage_enforcement_original_production_readiness(trust_decision=trust_decision)

try:
    _coverage_enforcement_original_profile_completeness_payload = profile_completeness_payload
except NameError:
    _coverage_enforcement_original_profile_completeness_payload = None

if _coverage_enforcement_original_profile_completeness_payload is not None:
    def profile_completeness_payload(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS) -> dict[str, Any]:
        payload = dict(_coverage_enforcement_original_profile_completeness_payload(db, profile, stale_days=stale_days))
        expected_universe = dict(payload.get("expected_rule_universe") or ((payload.get("scoring_defaults") or {}).get("expected_rule_universe") or {}))
        required_categories = normalize_categories(
            payload.get("required_categories")
            or expected_universe.get("required_categories")
            or []
        )
        optional_categories = normalize_categories(
            payload.get("optional_categories")
            or expected_universe.get("optional_categories")
            or []
        )
        category_statuses = dict(payload.get("category_statuses") or {})
        missing_categories = normalize_categories(
            payload.get("missing_categories")
            or [c for c in required_categories if str(category_statuses.get(c) or "").strip().lower() in {"", "missing"}]
        )
        conflicting_categories = normalize_categories(payload.get("conflicting_categories") or [])
        stale_authoritative_categories = normalize_categories(payload.get("stale_authoritative_categories") or [])
        critical_categories = normalize_categories(payload.get("critical_categories") or [])
        binding_unmet = normalize_categories(payload.get("binding_unmet_categories") or [])
        if not binding_unmet:
            details = dict(payload.get("category_details") or {})
            binding_unmet = normalize_categories(
                [c for c, detail in details.items() if bool((detail or {}).get("binding_authority_unmet"))]
            )

        production_readiness = str(payload.get("production_readiness") or "").strip().lower()
        if conflicting_categories or stale_authoritative_categories or any(c in set(missing_categories) for c in critical_categories) or binding_unmet:
            production_readiness = "not_ready"

        payload["required_categories"] = required_categories
        payload["optional_categories"] = optional_categories
        payload["missing_categories"] = missing_categories
        payload["conflicting_categories"] = conflicting_categories
        payload["stale_authoritative_categories"] = stale_authoritative_categories
        payload["binding_unmet_categories"] = binding_unmet
        payload["required_categories_by_tier"] = dict(payload.get("required_categories_by_tier") or expected_universe.get("required_categories_by_tier") or {})
        payload["optional_categories_by_tier"] = dict(payload.get("optional_categories_by_tier") or expected_universe.get("optional_categories_by_tier") or {})
        payload["production_readiness"] = production_readiness
        payload["safe_for_projection"] = bool(payload.get("safe_for_projection")) and production_readiness != "not_ready"
        payload["safe_for_user_reliance"] = bool(payload.get("safe_for_user_reliance")) and production_readiness != "not_ready"
        return payload
