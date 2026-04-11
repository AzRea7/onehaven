# backend/app/services/jurisdiction_completeness_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..domain.jurisdiction_categories import (
    CATEGORY_UNCATEGORIZED,
    compute_category_score_from_statuses,
    completeness_confidence_label,
    normalize_categories,
    normalize_category,
    normalize_rule_category,
)
from ..domain.jurisdiction_defaults import (
    DEFAULT_STALE_DAYS,
    completeness_score_weights,
    completeness_scoring_thresholds,
    required_categories_for_city,
)
from ..domain.jurisdiction_scoring import JurisdictionCompleteness, compute_category_completeness
from ..policy_models import JurisdictionCoverageStatus, JurisdictionProfile, PolicyAssertion, PolicySource


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
    conflict_count: int
    inferred: bool
    stale: bool
    conflicting: bool
    missing: bool
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
    inferred_categories: list[str]
    conflicting_categories: list[str]
    missing_categories: list[str]
    category_details: dict[str, dict[str, Any]]
    scoring_defaults: dict[str, Any]


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


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


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
    current_required = normalize_categories(_loads_json_list(profile.required_categories_json))
    if current_required:
        return current_required
    return required_categories_for_city(profile.city, state=profile.state or "MI", include_section8=True)


def _collect_covered_categories_from_profile(profile: JurisdictionProfile) -> list[str]:
    return normalize_categories(_loads_json_list(profile.covered_categories_json))


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


def _collect_source_rows_for_scope(db: Session, *, state: str | None, county: str | None, city: str | None) -> list[PolicySource]:
    stmt = select(PolicySource).where(*_scope_filters(state=state, county=county, city=city))
    return list(db.execute(stmt).scalars().all())


def _collect_assertion_rows_for_scope(db: Session, *, state: str | None, county: str | None, city: str | None) -> list[PolicyAssertion]:
    stmt = select(PolicyAssertion).where(*_scope_filters_assertion(state=state, county=county, city=city))
    return list(db.execute(stmt).scalars().all())


def _collect_covered_categories_from_sources(db: Session, *, state: str | None, county: str | None, city: str | None) -> list[str]:
    categories: list[str] = []
    for row in _collect_source_rows_for_scope(db, state=state, county=county, city=city):
        categories.extend(_source_categories(row))
    return normalize_categories(categories)


def _collect_covered_categories_from_assertions(db: Session, *, state: str | None, county: str | None, city: str | None) -> list[str]:
    categories: list[str] = []
    for row in _collect_assertion_rows_for_scope(db, state=state, county=county, city=city):
        category = _assertion_category(row)
        if not category:
            continue
        normalized_status = (getattr(row, "coverage_status", None) or "").strip().lower()
        if normalized_status in {"covered", "verified", "accepted", "projected", "candidate", "conditional", "inferred"}:
            categories.append(category)
    return normalize_categories(categories)


def collect_covered_categories_for_scope(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    include_sources: bool = True,
    include_assertions: bool = True,
    extra_categories: Iterable[Any] | None = None,
) -> list[str]:
    categories: list[Any] = []
    if include_sources:
        categories.extend(_collect_covered_categories_from_sources(db, state=state, county=county, city=city))
    if include_assertions:
        categories.extend(_collect_covered_categories_from_assertions(db, state=state, county=county, city=city))
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
    return max(0.0, min(1.0, round((0.45 * authoritative) + (0.35 * trust_level) + (0.20 * registry_score), 6)))


def _source_freshness_score(source: PolicySource, *, stale_days: int) -> float:
    if _source_is_stale(source, stale_days=stale_days):
        return 0.0
    freshness_status = (getattr(source, "freshness_status", None) or "fresh").strip().lower()
    if freshness_status in {"fresh", "verified"}:
        return 1.0
    if freshness_status in {"aging", "warning", "needs_refresh"}:
        return 0.5
    return 0.75


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
    return governance_state in {"", "draft"} and review_status == "verified" and rule_status in {"candidate", "active", ""}


def _assertion_governance_score(assertion: PolicyAssertion) -> float:
    if getattr(assertion, "superseded_by_assertion_id", None) is not None or getattr(assertion, "replaced_by_assertion_id", None) is not None:
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
    has_citation_blob = bool(citation_json)
    has_string_citation = bool((getattr(assertion, "source_citation", None) or "").strip())
    has_excerpt = bool((getattr(assertion, "raw_excerpt", None) or "").strip())
    score = 0.0
    if has_string_citation:
        score += 0.45
    if has_excerpt:
        score += 0.35
    if has_citation_blob:
        score += 0.20
    return max(0.0, min(1.0, round(score, 6)))


def _assertion_extraction_score(assertion: PolicyAssertion) -> float:
    confidence = float(getattr(assertion, "confidence", 0.0) or 0.0)
    citation_quality = _assertion_citation_quality(assertion)
    return max(0.0, min(1.0, round((0.7 * confidence) + (0.3 * citation_quality), 6)))


def _assertion_is_stale(assertion: PolicyAssertion, *, stale_days: int) -> bool:
    review_status = (getattr(assertion, "review_status", None) or "").strip().lower()
    if review_status in {"stale", "needs_recheck"}:
        return True
    source_freshness_status = (getattr(assertion, "source_freshness_status", None) or "").strip().lower()
    if source_freshness_status in {"stale", "needs_recheck", "expired"}:
        return True
    stale_after = getattr(assertion, "stale_after", None)
    if stale_after is not None:
        return stale_after < _utcnow()
    anchor = getattr(assertion, "reviewed_at", None) or getattr(assertion, "approved_at", None) or getattr(assertion, "activated_at", None) or getattr(assertion, "extracted_at", None)
    if anchor is None:
        return True
    return anchor < (_utcnow() - timedelta(days=stale_days))


def _assertion_truth_marker(assertion: PolicyAssertion) -> str:
    for candidate in (
        getattr(assertion, "value_hash", None),
        getattr(assertion, "value_json", None),
        getattr(assertion, "source_citation", None),
        getattr(assertion, "raw_excerpt", None),
        getattr(assertion, "rule_key", None),
    ):
        raw = str(candidate or "").strip()
        if raw:
            return raw
    return f"assertion:{getattr(assertion, 'id', 'unknown')}"


def _category_status_from_assessment(assessment: JurisdictionCategoryAssessment, *, thresholds: dict[str, float]) -> str:
    if assessment.missing:
        return "missing"
    if assessment.conflicting:
        return "conflicting"
    if assessment.stale:
        return "stale"
    if assessment.inferred:
        return "inferred"
    authority_ok = assessment.authority_score >= float(thresholds.get("authoritative_source", 0.65))
    extraction_ok = assessment.extraction_score >= float(thresholds.get("extraction_confidence", 0.65))
    governance_ok = assessment.governance_score >= float(thresholds.get("governance_quality", 0.70))
    freshness_ok = assessment.freshness_score >= float(thresholds.get("freshness", 0.60))
    if authority_ok and extraction_ok and governance_ok and freshness_ok:
        return "covered"
    return "partial"


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
    authoritative_source_count = sum(1 for row in rows if bool(getattr(row, "is_authoritative", False)))
    timestamps = [ts for ts in (_source_timestamp(row) for row in rows) if ts is not None]
    freshest_source_at = max(timestamps) if timestamps else None
    oldest_source_at = min(timestamps) if timestamps else None

    freshness_status_counts: dict[str, int] = {}
    stale_source_ids: list[int] = []
    fresh_source_ids: list[int] = []
    for row in rows:
        status_key = (getattr(row, "freshness_status", None) or "unknown").strip().lower() or "unknown"
        freshness_status_counts[status_key] = freshness_status_counts.get(status_key, 0) + 1
        if _source_is_stale(row, stale_days=stale_days):
            stale_source_ids.append(row.id)
        else:
            fresh_source_ids.append(row.id)

    is_stale = False
    stale_reason: str | None = None
    if source_count == 0:
        is_stale = True
        stale_reason = "no_policy_sources"
    elif freshest_source_at is None:
        is_stale = True
        stale_reason = "no_freshness_timestamps"
    elif freshest_source_at < (_utcnow() - timedelta(days=stale_days)):
        is_stale = True
        stale_reason = f"latest_source_older_than_{stale_days}_days"
    elif stale_source_ids and not fresh_source_ids:
        is_stale = True
        stale_reason = "all_sources_stale"

    payload = {
        "source_count": source_count,
        "authoritative_source_count": authoritative_source_count,
        "freshest_source_at": freshest_source_at.isoformat() if freshest_source_at else None,
        "oldest_source_at": oldest_source_at.isoformat() if oldest_source_at else None,
        "freshness_status_counts": freshness_status_counts,
        "stale_source_ids": stale_source_ids,
        "fresh_source_ids": fresh_source_ids,
        "stale_days": stale_days,
    }

    return JurisdictionFreshnessSummary(
        source_count=source_count,
        authoritative_source_count=authoritative_source_count,
        freshest_source_at=freshest_source_at,
        oldest_source_at=oldest_source_at,
        freshness_payload=payload,
        is_stale=is_stale,
        stale_reason=stale_reason,
    )


def build_category_assessments(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    required_categories: Iterable[Any] | None,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, JurisdictionCategoryAssessment]:
    required = normalize_categories(required_categories)
    sources = _collect_source_rows_for_scope(db, state=state, county=county, city=city)
    assertions = _collect_assertion_rows_for_scope(db, state=state, county=county, city=city)

    categories = list(required)
    seen = set(categories)
    for source in sources:
        for category in _source_categories(source):
            if category not in seen:
                seen.add(category)
                categories.append(category)
    for assertion in assertions:
        category = _assertion_category(assertion)
        if category and category not in seen:
            seen.add(category)
            categories.append(category)

    thresholds = completeness_scoring_thresholds()
    assessments: dict[str, JurisdictionCategoryAssessment] = {}

    for category in categories:
        category_sources = [source for source in sources if category in _source_categories(source)]
        category_assertions = [assertion for assertion in assertions if _assertion_category(assertion) == category]
        governed_assertions = [assertion for assertion in category_assertions if _assertion_is_governed(assertion)]
        source_ids = sorted({int(source.id) for source in category_sources if getattr(source, "id", None) is not None})
        assertion_ids = sorted({int(assertion.id) for assertion in category_assertions if getattr(assertion, "id", None) is not None})
        authoritative_source_count = sum(1 for source in category_sources if bool(getattr(source, "is_authoritative", False)))
        authority_scores = [_source_authority_score(source) for source in category_sources]
        freshness_scores = [_source_freshness_score(source, stale_days=stale_days) for source in category_sources]
        extraction_scores = [_assertion_extraction_score(assertion) for assertion in category_assertions]
        governance_scores = [_assertion_governance_score(assertion) for assertion in category_assertions]
        citation_count = sum(1 for assertion in category_assertions if _assertion_citation_quality(assertion) > 0.0)
        latest_verified_candidates = [
            value
            for value in [
                *(_source_timestamp(source) for source in category_sources),
                *(getattr(assertion, "reviewed_at", None) or getattr(assertion, "approved_at", None) or getattr(assertion, "activated_at", None) or getattr(assertion, "extracted_at", None) for assertion in category_assertions),
            ]
            if value is not None
        ]
        latest_verified_at = max(latest_verified_candidates) if latest_verified_candidates else None

        conflict_markers = {
            _assertion_truth_marker(assertion)
            for assertion in governed_assertions or category_assertions
            if not _assertion_is_stale(assertion, stale_days=stale_days)
        }
        conflict_count = max(0, len(conflict_markers) - 1)
        missing = not category_sources and not category_assertions
        stale = (bool(category_sources or category_assertions) and all(_source_is_stale(source, stale_days=stale_days) for source in category_sources) if category_sources else False)
        stale = stale or (bool(category_assertions) and all(_assertion_is_stale(assertion, stale_days=stale_days) for assertion in category_assertions))

        authority_score = round(sum(authority_scores) / len(authority_scores), 6) if authority_scores else 0.0
        freshness_score = round(sum(freshness_scores) / len(freshness_scores), 6) if freshness_scores else 0.0
        extraction_score = round(sum(extraction_scores) / len(extraction_scores), 6) if extraction_scores else 0.0
        governance_score = round(sum(governance_scores) / len(governance_scores), 6) if governance_scores else 0.0
        confidence_score = extraction_score
        inferred = False
        conflicting = conflict_count > 0
        if not missing and not conflicting and not stale:
            inferred = not governed_assertions or extraction_score < float(thresholds.get("extraction_confidence", 0.65)) or citation_count == 0

        assessment = JurisdictionCategoryAssessment(
            category=category,
            status="missing",
            source_count=len(category_sources),
            authoritative_source_count=authoritative_source_count,
            assertion_count=len(category_assertions),
            governed_assertion_count=len(governed_assertions),
            citation_count=citation_count,
            confidence_score=confidence_score,
            extraction_score=extraction_score,
            authority_score=authority_score,
            governance_score=governance_score,
            freshness_score=freshness_score,
            conflict_count=conflict_count,
            inferred=inferred,
            stale=stale,
            conflicting=conflicting,
            missing=missing,
            latest_verified_at=latest_verified_at,
            source_ids=source_ids,
            assertion_ids=assertion_ids,
        )
        status = _category_status_from_assessment(assessment, thresholds=thresholds)
        assessments[category] = JurisdictionCategoryAssessment(**{**assessment.__dict__, "status": status})

    return assessments


def compute_jurisdiction_score_breakdown(
    *,
    required_categories: Iterable[Any] | None,
    category_assessments: dict[str, JurisdictionCategoryAssessment],
) -> JurisdictionScoreBreakdown:
    required = normalize_categories(required_categories)
    weights = completeness_score_weights()
    thresholds = completeness_scoring_thresholds()

    category_statuses = {category: category_assessments.get(category, JurisdictionCategoryAssessment(category=category, status="missing", source_count=0, authoritative_source_count=0, assertion_count=0, governed_assertion_count=0, citation_count=0, confidence_score=0.0, extraction_score=0.0, authority_score=0.0, governance_score=0.0, freshness_score=0.0, conflict_count=0, inferred=False, stale=False, conflicting=False, missing=True, latest_verified_at=None, source_ids=[], assertion_ids=[])).status for category in required}

    category_score = compute_category_score_from_statuses(required_categories=required, category_statuses=category_statuses)
    covered_categories = list(category_score.covered_categories)
    stale_categories = list(category_score.stale_categories or [])
    inferred_categories = list(category_score.inferred_categories or [])
    conflicting_categories = list(category_score.conflicting_categories or [])
    missing_categories = list(category_score.missing_categories)

    selected = [category_assessments.get(category) for category in required if category_assessments.get(category) is not None]
    if not selected:
        selected = []

    def _avg(values: list[float]) -> float:
        return round(sum(values) / len(values), 6) if values else 0.0

    freshness_subscore = _avg([item.freshness_score for item in selected])
    authority_subscore = _avg([item.authority_score for item in selected])
    extraction_subscore = _avg([item.extraction_score for item in selected])
    governance_subscore = _avg([item.governance_score for item in selected])
    coverage_subscore = float(category_score.completeness_score)
    conflict_penalty = round(min(0.35, 0.15 * len(conflicting_categories) + 0.05 * len(stale_categories)), 6)

    overall = (
        (coverage_subscore * float(weights.get("coverage", 0.35)))
        + (freshness_subscore * float(weights.get("freshness", 0.20)))
        + (authority_subscore * float(weights.get("authority", 0.15)))
        + (extraction_subscore * float(weights.get("extraction", 0.15)))
        + (governance_subscore * float(weights.get("governance", 0.15)))
        - conflict_penalty
    )
    overall = max(0.0, min(1.0, round(overall, 6)))

    if not required:
        completeness_status = "complete"
    elif conflicting_categories:
        completeness_status = "conflicting"
    elif missing_categories and overall <= 0.0:
        completeness_status = "missing"
    elif stale_categories and overall < 0.999:
        completeness_status = "stale"
    elif not missing_categories and not stale_categories and not inferred_categories and overall >= 0.95:
        completeness_status = "complete"
    else:
        completeness_status = "partial"

    category_details = {
        category: {
            "status": assessment.status,
            "source_count": assessment.source_count,
            "authoritative_source_count": assessment.authoritative_source_count,
            "assertion_count": assessment.assertion_count,
            "governed_assertion_count": assessment.governed_assertion_count,
            "citation_count": assessment.citation_count,
            "confidence_score": assessment.confidence_score,
            "extraction_score": assessment.extraction_score,
            "authority_score": assessment.authority_score,
            "governance_score": assessment.governance_score,
            "freshness_score": assessment.freshness_score,
            "conflict_count": assessment.conflict_count,
            "inferred": assessment.inferred,
            "stale": assessment.stale,
            "conflicting": assessment.conflicting,
            "missing": assessment.missing,
            "latest_verified_at": assessment.latest_verified_at.isoformat() if assessment.latest_verified_at else None,
            "source_ids": assessment.source_ids,
            "assertion_ids": assessment.assertion_ids,
        }
        for category, assessment in category_assessments.items()
        if category in set(required)
    }

    return JurisdictionScoreBreakdown(
        overall_completeness=overall,
        completeness_status=completeness_status,
        coverage_subscore=coverage_subscore,
        freshness_subscore=freshness_subscore,
        authority_subscore=authority_subscore,
        extraction_subscore=extraction_subscore,
        governance_subscore=governance_subscore,
        conflict_penalty=conflict_penalty,
        confidence_label=completeness_confidence_label(overall),
        category_statuses=category_statuses,
        covered_categories=covered_categories,
        stale_categories=stale_categories,
        inferred_categories=inferred_categories,
        conflicting_categories=conflicting_categories,
        missing_categories=missing_categories,
        category_details=category_details,
        scoring_defaults={"weights": weights, "thresholds": thresholds},
    )


def compute_profile_completeness(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS) -> tuple[JurisdictionCompleteness, JurisdictionFreshnessSummary]:
    required_categories = _coalesce_required_categories(profile)
    profile_categories = _collect_covered_categories_from_profile(profile)
    scope_categories = collect_covered_categories_for_scope(db, state=profile.state, county=profile.county, city=profile.city)
    covered_categories = normalize_categories(profile_categories + scope_categories)
    completeness = compute_category_completeness(required_categories=required_categories, covered_categories=covered_categories)
    freshness = compute_scope_freshness_summary(db, state=profile.state, county=profile.county, city=profile.city, stale_days=stale_days)
    return completeness, freshness


def compute_profile_score_breakdown(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS) -> JurisdictionScoreBreakdown:
    required_categories = _coalesce_required_categories(profile)
    assessments = build_category_assessments(db, state=profile.state, county=profile.county, city=profile.city, required_categories=required_categories, stale_days=stale_days)
    return compute_jurisdiction_score_breakdown(required_categories=required_categories, category_assessments=assessments)


def apply_profile_completeness(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS, commit: bool = False) -> JurisdictionProfile:
    breakdown = compute_profile_score_breakdown(db, profile, stale_days=stale_days)
    freshness = compute_scope_freshness_summary(db, state=profile.state, county=profile.county, city=profile.city, stale_days=stale_days)

    profile.required_categories_json = _dumps(list(breakdown.category_statuses.keys()))
    profile.covered_categories_json = _dumps(breakdown.covered_categories)
    profile.missing_categories_json = _dumps(breakdown.missing_categories)
    profile.completeness_score = breakdown.overall_completeness
    profile.completeness_status = breakdown.completeness_status
    profile.category_norm_version = getattr(profile, "category_norm_version", None) or "v2"
    profile.source_count = freshness.source_count
    profile.authoritative_source_count = freshness.authoritative_source_count
    profile.freshest_source_at = freshness.freshest_source_at
    profile.oldest_source_at = freshness.oldest_source_at
    profile.source_freshness_json = _dumps({
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
            "stale_categories": breakdown.stale_categories,
            "inferred_categories": breakdown.inferred_categories,
            "conflicting_categories": breakdown.conflicting_categories,
            "missing_categories": breakdown.missing_categories,
        },
    })
    profile.is_stale = freshness.is_stale or bool(breakdown.stale_categories)
    profile.stale_reason = freshness.stale_reason or ("stale_categories_present" if breakdown.stale_categories else None)
    if not profile.is_stale and breakdown.completeness_status == "complete":
        profile.last_verified_at = _utcnow()
    elif profile.last_verified_at is None and freshness.freshest_source_at is not None:
        profile.last_verified_at = freshness.freshest_source_at
    profile.last_refresh_attempt_at = _utcnow()
    if not freshness.is_stale:
        profile.last_refresh_success_at = _utcnow()
    db.add(profile)
    if commit:
        db.commit()
        db.refresh(profile)
    else:
        db.flush()
    return profile


def get_or_create_coverage_status_for_profile(db: Session, profile: JurisdictionProfile) -> JurisdictionCoverageStatus:
    stmt = select(JurisdictionCoverageStatus).where(
        JurisdictionCoverageStatus.org_id == profile.org_id,
        JurisdictionCoverageStatus.state == profile.state,
        JurisdictionCoverageStatus.county == profile.county,
        JurisdictionCoverageStatus.city == profile.city,
        JurisdictionCoverageStatus.pha_name == profile.pha_name,
    )
    coverage = db.execute(stmt).scalar_one_or_none()
    if coverage is not None:
        return coverage
    coverage = JurisdictionCoverageStatus(org_id=profile.org_id, state=profile.state or "MI", county=profile.county, city=profile.city, pha_name=profile.pha_name)
    db.add(coverage)
    db.flush()
    return coverage


def sync_coverage_status_from_profile(db: Session, profile: JurisdictionProfile, *, commit: bool = False) -> JurisdictionCoverageStatus:
    coverage = get_or_create_coverage_status_for_profile(db, profile)
    detail = _loads_json_dict(profile.source_freshness_json).get("scoring", {})
    coverage.completeness_score = float(profile.completeness_score or 0.0)
    coverage.confidence_score = float(profile.completeness_score or 0.0)
    coverage.completeness_status = profile.completeness_status or "missing"
    coverage.coverage_version = profile.category_norm_version or "v2"
    coverage.covered_categories_json = profile.covered_categories_json or "[]"
    coverage.missing_categories_json = profile.missing_categories_json or "[]"
    coverage.is_stale = bool(profile.is_stale)
    coverage.stale_reason = profile.stale_reason
    coverage.last_computed_at = _utcnow()
    coverage.last_source_change_at = profile.freshest_source_at
    coverage.source_summary_json = _dumps(detail.get("category_statuses", {}))
    coverage.metadata_json = _dumps({"profile_scoring": detail})
    if commit:
        db.commit()
        db.refresh(coverage)
    else:
        db.flush()
    return coverage


def recompute_profile_and_coverage(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS, commit: bool = False) -> tuple[JurisdictionProfile, JurisdictionCoverageStatus]:
    profile = apply_profile_completeness(db, profile, stale_days=stale_days, commit=False)
    coverage = sync_coverage_status_from_profile(db, profile, commit=False)
    if commit:
        db.commit()
        db.refresh(profile)
        db.refresh(coverage)
    return profile, coverage


def recompute_profile_by_id(db: Session, jurisdiction_profile_id: int, *, stale_days: int = DEFAULT_STALE_DAYS, commit: bool = False) -> tuple[JurisdictionProfile | None, JurisdictionCoverageStatus | None]:
    profile = db.get(JurisdictionProfile, jurisdiction_profile_id)
    if profile is None:
        return None, None
    return recompute_profile_and_coverage(db, profile, stale_days=stale_days, commit=commit)


def recompute_all_profiles(db: Session, *, stale_days: int = DEFAULT_STALE_DAYS, org_id: int | None = None, commit_every: int = 100) -> list[int]:
    stmt = select(JurisdictionProfile.id).order_by(JurisdictionProfile.id)
    if org_id is not None:
        stmt = stmt.where(JurisdictionProfile.org_id == org_id)
    ids = [row[0] for row in db.execute(stmt).all()]
    touched: list[int] = []
    for idx, profile_id in enumerate(ids, start=1):
        profile, _ = recompute_profile_by_id(db, profile_id, stale_days=stale_days, commit=False)
        if profile is not None:
            touched.append(profile.id)
        if idx % max(1, commit_every) == 0:
            db.commit()
    db.commit()
    return touched


def profile_completeness_payload(db: Session, profile: JurisdictionProfile, *, stale_days: int = DEFAULT_STALE_DAYS) -> dict[str, Any]:
    breakdown = compute_profile_score_breakdown(db, profile, stale_days=stale_days)
    freshness = compute_scope_freshness_summary(db, state=profile.state, county=profile.county, city=profile.city, stale_days=stale_days)
    return {
        "jurisdiction_profile_id": profile.id,
        "org_id": profile.org_id,
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": profile.pha_name,
        "required_categories": list(breakdown.category_statuses.keys()),
        "covered_categories": breakdown.covered_categories,
        "missing_categories": breakdown.missing_categories,
        "stale_categories": breakdown.stale_categories,
        "inferred_categories": breakdown.inferred_categories,
        "conflicting_categories": breakdown.conflicting_categories,
        "category_statuses": breakdown.category_statuses,
        "category_details": breakdown.category_details,
        "completeness_score": breakdown.overall_completeness,
        "completeness_status": breakdown.completeness_status,
        "coverage_subscore": breakdown.coverage_subscore,
        "freshness_subscore": breakdown.freshness_subscore,
        "authority_subscore": breakdown.authority_subscore,
        "extraction_subscore": breakdown.extraction_subscore,
        "governance_subscore": breakdown.governance_subscore,
        "conflict_penalty": breakdown.conflict_penalty,
        "coverage_confidence": breakdown.confidence_label,
        "coverage_confidence_score": breakdown.overall_completeness,
        "source_count": freshness.source_count,
        "authoritative_source_count": freshness.authoritative_source_count,
        "freshest_source_at": freshness.freshest_source_at.isoformat() if freshness.freshest_source_at else None,
        "oldest_source_at": freshness.oldest_source_at.isoformat() if freshness.oldest_source_at else None,
        "source_freshness": freshness.freshness_payload,
        "is_stale": freshness.is_stale or bool(breakdown.stale_categories),
        "stale_reason": freshness.stale_reason or ("stale_categories_present" if breakdown.stale_categories else None),
        "stale_days": stale_days,
        "resolved_rule_version": f"jp:{getattr(profile, 'id', 'new')}:{breakdown.completeness_status}:{freshness.source_count}:{freshness.authoritative_source_count}",
        "stale_warning": freshness.is_stale or bool(breakdown.stale_categories),
        "missing_local_rule_areas": breakdown.missing_categories,
        "scoring_defaults": breakdown.scoring_defaults,
    }


def compute_jurisdiction_completeness(*, required_categories: Iterable[Any] | None, category_coverage: dict[str, Any] | None, stale_status: str | None = None) -> dict[str, Any]:
    required = normalize_categories(required_categories)
    raw_map = category_coverage or {}
    normalized_statuses = {category: str(raw_map.get(category, "missing")).strip().lower() for category in required}
    coverage = compute_category_score_from_statuses(required_categories=required, category_statuses=normalized_statuses)
    freshness_subscore = 0.0 if (stale_status or "").strip().lower() == "stale" else 1.0
    overall = max(0.0, min(1.0, round((coverage.completeness_score * 0.8) + (freshness_subscore * 0.2), 6)))
    completeness_status = coverage.completeness_status
    if (stale_status or "").strip().lower() == "stale" and completeness_status == "complete":
        completeness_status = "stale"
    return {
        "required_categories": coverage.required_categories,
        "covered_categories": coverage.covered_categories,
        "missing_categories": coverage.missing_categories,
        "conditional_categories": list(coverage.inferred_categories or []),
        "stale_categories": list(coverage.stale_categories or []),
        "conflicting_categories": list(coverage.conflicting_categories or []),
        "category_statuses": coverage.category_statuses or {},
        "coverage_subscore": coverage.completeness_score,
        "freshness_subscore": freshness_subscore,
        "authority_subscore": 1.0,
        "extraction_subscore": 1.0,
        "governance_subscore": 1.0,
        "conflict_penalty": 0.0,
        "completeness_score": overall,
        "completeness_status": completeness_status,
        "stale_status": "stale" if (stale_status or "").strip().lower() == "stale" else "fresh",
    }