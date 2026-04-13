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
        county=getattr(profile, "county", None),
        pha_name=getattr(profile, "pha_name", None),
        include_section8=True,
    )


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


def _collect_source_rows_for_scope(
    db: Session, *, state: str | None, county: str | None, city: str | None
) -> list[PolicySource]:
    stmt = select(PolicySource).where(*_scope_filters(state=state, county=county, city=city))
    return list(db.execute(stmt).scalars().all())


def _collect_assertion_rows_for_scope(
    db: Session, *, state: str | None, county: str | None, city: str | None
) -> list[PolicyAssertion]:
    stmt = select(PolicyAssertion).where(
        *_scope_filters_assertion(state=state, county=county, city=city)
    )
    return list(db.execute(stmt).scalars().all())


def _collect_covered_categories_from_sources(
    db: Session, *, state: str | None, county: str | None, city: str | None
) -> list[str]:
    categories: list[str] = []
    for row in _collect_source_rows_for_scope(db, state=state, county=county, city=city):
        categories.extend(_source_categories(row))
    return normalize_categories(categories)


def _collect_covered_categories_from_assertions(
    db: Session, *, state: str | None, county: str | None, city: str | None
) -> list[str]:
    categories: list[str] = []
    for row in _collect_assertion_rows_for_scope(db, state=state, county=county, city=city):
        category = _assertion_category(row)
        if not category:
            continue
        normalized_status = (getattr(row, "coverage_status", None) or "").strip().lower()
        if normalized_status in {
            "covered",
            "verified",
            "accepted",
            "projected",
            "candidate",
            "conditional",
            "inferred",
        }:
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
        categories.extend(
            _collect_covered_categories_from_sources(db, state=state, county=county, city=city)
        )
    if include_assertions:
        categories.extend(
            _collect_covered_categories_from_assertions(
                db, state=state, county=county, city=city
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


def build_category_assessments(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    required_categories: Iterable[str],
    stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, JurisdictionCategoryAssessment]:
    required = normalize_categories(required_categories)
    thresholds = completeness_scoring_thresholds()

    source_rows = _collect_source_rows_for_scope(db, state=state, county=county, city=city)
    assertion_rows = _collect_assertion_rows_for_scope(db, state=state, county=county, city=city)

    assessments: dict[str, JurisdictionCategoryAssessment] = {}
    for category in required:
        category_sources = [row for row in source_rows if category in _source_categories(row)]
        category_assertions = [
            row for row in assertion_rows if _assertion_category(row) == category
        ]

        source_count = len(category_sources)
        authoritative_source_count = sum(
            1 for row in category_sources if bool(getattr(row, "is_authoritative", False))
        )
        assertion_count = len(category_assertions)
        governed_assertion_count = sum(
            1 for row in category_assertions if _assertion_is_governed(row)
        )
        citation_count = sum(
            1 for row in category_assertions if _assertion_citation_quality(row) > 0.0
        )
        authority_score = round(
            (
                sum(_source_authority_score(row) for row in category_sources)
                / float(max(1, len(category_sources)))
            ),
            6,
        ) if category_sources else 0.0
        freshness_score = round(
            (
                sum(_source_freshness_score(row, stale_days=stale_days) for row in category_sources)
                / float(max(1, len(category_sources)))
            ),
            6,
        ) if category_sources else 0.0
        governance_score = round(
            (
                sum(_assertion_governance_score(row) for row in category_assertions)
                / float(max(1, len(category_assertions)))
            ),
            6,
        ) if category_assertions else 0.0
        extraction_score = round(
            (
                sum(_assertion_citation_quality(row) for row in category_assertions)
                / float(max(1, len(category_assertions)))
            ),
            6,
        ) if category_assertions else 0.0
        conflict_count = sum(_assertion_conflict_count(row) for row in category_assertions)
        latest_verified_at = max(
            [ts for ts in [_source_timestamp(row) for row in category_sources] if ts is not None],
            default=None,
        )

        missing = source_count == 0 and assertion_count == 0
        conflicting = conflict_count > 0
        stale = (not missing) and freshness_score < thresholds.get("freshness", 0.60)
        inferred = (
            not missing
            and governed_assertion_count <= 0
            and assertion_count > 0
            and not conflicting
            and not stale
        )

        if missing:
            status = "missing"
        elif conflicting:
            status = "conflicting"
        elif stale:
            status = "stale"
        elif inferred:
            status = "inferred"
        elif governed_assertion_count > 0 or authority_score >= thresholds.get("authoritative_source", 0.65):
            status = "covered"
        else:
            status = "partial"

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
            conflict_count=conflict_count,
            inferred=inferred,
            stale=stale,
            conflicting=conflicting,
            missing=missing,
            latest_verified_at=latest_verified_at,
            source_ids=[int(row.id) for row in category_sources if getattr(row, "id", None) is not None],
            assertion_ids=[
                int(row.id) for row in category_assertions if getattr(row, "id", None) is not None
            ],
        )

    return assessments


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
            completeness_status="missing",
            coverage_subscore=0.0,
            freshness_subscore=0.0,
            authority_subscore=0.0,
            extraction_subscore=0.0,
            governance_subscore=0.0,
            conflict_penalty=0.0,
            confidence_label="low",
            category_statuses={},
            covered_categories=[],
            stale_categories=[],
            inferred_categories=[],
            conflicting_categories=[],
            missing_categories=[],
            category_details={},
            scoring_defaults={"weights": weights, "thresholds": thresholds},
        )

    category_statuses = {
        category: (
            category_assessments.get(category).status
            if category in category_assessments
            else "missing"
        )
        for category in required
    }

    covered_categories = [
        c for c in required if category_statuses.get(c) in {"covered", "partial"}
    ]
    stale_categories = [c for c in required if category_statuses.get(c) == "stale"]
    inferred_categories = [c for c in required if category_statuses.get(c) == "inferred"]
    conflicting_categories = [c for c in required if category_statuses.get(c) == "conflicting"]
    missing_categories = [c for c in required if category_statuses.get(c) == "missing"]

    assessments = [category_assessments.get(category) for category in required if category in category_assessments]

    coverage_subscore = round(len(covered_categories) / max(1, len(required)), 6)
    freshness_subscore = round(
        sum(a.freshness_score for a in assessments if a is not None) / float(max(1, len(required))),
        6,
    )
    authority_subscore = round(
        sum(a.authority_score for a in assessments if a is not None) / float(max(1, len(required))),
        6,
    )
    extraction_subscore = round(
        sum(a.extraction_score for a in assessments if a is not None) / float(max(1, len(required))),
        6,
    )
    governance_subscore = round(
        sum(a.governance_score for a in assessments if a is not None) / float(max(1, len(required))),
        6,
    )
    conflict_penalty = round(
        min(
            0.40,
            sum(
                0.10 if a.conflicting else 0.0
                for a in assessments
                if a is not None
            ),
        ),
        6,
    )

    overall = round(
        max(
            0.0,
            min(
                1.0,
                (
                    (weights["coverage"] * coverage_subscore)
                    + (weights["freshness"] * freshness_subscore)
                    + (weights["authority"] * authority_subscore)
                    + (weights["extraction"] * extraction_subscore)
                    + (weights["governance"] * governance_subscore)
                    - conflict_penalty
                ),
            ),
        ),
        6,
    )

    if conflicting_categories:
        completeness_status = "conflicting"
    elif stale_categories:
        completeness_status = "stale"
    elif missing_categories and covered_categories:
        completeness_status = "partial"
    elif missing_categories:
        completeness_status = "missing"
    elif inferred_categories:
        completeness_status = "partial"
    else:
        completeness_status = "complete"

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
            "latest_verified_at": assessment.latest_verified_at.isoformat()
            if assessment.latest_verified_at
            else None,
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
    authoritative_rows = [row for row in rows if bool(getattr(row, "is_authoritative", False))]
    authoritative_source_count = len(authoritative_rows)

    timestamps = [ts for ts in (_source_timestamp(row) for row in rows) if ts is not None]
    freshest = max(timestamps) if timestamps else None
    oldest = min(timestamps) if timestamps else None

    stale_rows = [row for row in rows if _source_is_stale(row, stale_days=stale_days)]
    is_stale = source_count == 0 or bool(stale_rows)
    stale_reason = None
    if source_count == 0:
        stale_reason = "no_sources"
    elif authoritative_rows and all(_source_is_stale(row, stale_days=stale_days) for row in authoritative_rows):
        stale_reason = "authoritative_sources_stale"
    elif stale_rows:
        stale_reason = "one_or_more_sources_stale"

    payload = {
        "source_count": source_count,
        "authoritative_source_count": authoritative_source_count,
        "freshest_source_at": freshest.isoformat() if freshest else None,
        "oldest_source_at": oldest.isoformat() if oldest else None,
        "stale_source_count": len(stale_rows),
        "stale_reason": stale_reason,
        "is_stale": is_stale,
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
        required_categories=scoped_required_categories,
        stale_days=stale_days,
    )
    return compute_jurisdiction_score_breakdown(
        required_categories=scoped_required_categories,
        category_assessments=assessments,
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
    critical_categories = get_critical_categories(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )

    missing_required = list(breakdown.missing_categories)
    stale_categories = list(breakdown.stale_categories)
    conflicting_categories = list(breakdown.conflicting_categories)
    inferred_categories = list(breakdown.inferred_categories)

    missing_critical = [c for c in critical_categories if c in set(missing_required)]
    stale_authoritative = [
        c
        for c, detail in (breakdown.category_details or {}).items()
        if detail.get("status") == "stale" and int(detail.get("authoritative_source_count", 0) or 0) > 0
    ]
    inferred_critical = [c for c in critical_categories if c in set(inferred_categories)]

    tier_rows = compute_tier_coverage(
        covered_categories=breakdown.covered_categories,
        category_statuses=breakdown.category_statuses,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    )
    incomplete_required_tiers = [
        row.jurisdiction_type
        for row in tier_rows
        if not row.complete
    ]

    blocker_reasons: list[str] = []
    manual_review_reasons: list[str] = []

    if policy.get("block_on_missing_critical_categories", True) and missing_critical:
        blocker_reasons.append("missing_critical_categories")
    if policy.get("block_on_unresolved_conflicts", True) and conflicting_categories:
        blocker_reasons.append("unresolved_conflicts")
    if (
        policy.get("block_on_incomplete_required_tiers", True)
        and incomplete_required_tiers
    ):
        blocker_reasons.append("incomplete_required_tiers")
    if breakdown.overall_completeness < float(policy.get("projection_min_completeness_score", 0.80)):
        blocker_reasons.append("completeness_below_projection_threshold")

    user_reliance_blockers: list[str] = []
    if breakdown.overall_completeness < float(policy.get("user_reliance_min_completeness_score", 0.90)):
        user_reliance_blockers.append("completeness_below_user_reliance_threshold")
    if (
        policy.get("block_on_stale_authoritative_sources_for_user_reliance", True)
        and stale_authoritative
    ):
        user_reliance_blockers.append("stale_authoritative_sources")
    if breakdown.authority_subscore < float(policy.get("authority_min_score_for_user_reliance", 0.70)):
        user_reliance_blockers.append("authority_below_user_reliance_threshold")
    if breakdown.governance_subscore < float(policy.get("governance_min_score_for_user_reliance", 0.75)):
        user_reliance_blockers.append("governance_below_user_reliance_threshold")
    if conflicting_categories:
        user_reliance_blockers.append("unresolved_conflicts")

    if (
        policy.get("manual_review_on_inferred_critical_categories", True)
        and inferred_critical
    ):
        manual_review_reasons.append("inferred_critical_categories")
    if (
        policy.get("manual_review_on_any_inferred_required_categories", False)
        and inferred_categories
    ):
        manual_review_reasons.append("inferred_required_categories")
    if (
        policy.get("manual_review_on_low_authority_required_categories", True)
        and breakdown.authority_subscore < float(policy.get("authority_min_score_for_user_reliance", 0.70))
        and not blocker_reasons
    ):
        manual_review_reasons.append("low_authority_required_coverage")

    safe_for_projection = not bool(blocker_reasons)
    safe_for_user_reliance = safe_for_projection and not bool(user_reliance_blockers)

    if policy.get("block_on_missing_critical_categories", True) and missing_critical:
        decision_code = "blocked_due_to_missing_critical_coverage"
    elif conflicting_categories:
        decision_code = "blocked_due_to_unresolved_conflicts"
    elif stale_authoritative and policy.get("block_on_stale_authoritative_sources_for_user_reliance", True):
        decision_code = "blocked_due_to_stale_authoritative_sources"
    elif incomplete_required_tiers and policy.get("block_on_incomplete_required_tiers", True):
        decision_code = "blocked_due_to_incomplete_required_tiers"
    elif manual_review_reasons:
        decision_code = "manual_review_required"
    else:
        decision_code = "safe_for_projection" if safe_for_projection else "manual_review_required"

    if safe_for_projection and safe_for_user_reliance:
        decision_code = "safe_for_user_reliance"

    combined_blockers = blocker_reasons + [reason for reason in user_reliance_blockers if reason not in blocker_reasons]

    return JurisdictionTrustDecision(
        decision_code=decision_code,
        safe_for_projection=safe_for_projection,
        safe_for_user_reliance=safe_for_user_reliance,
        blocked=not safe_for_projection,
        blocker_reasons=combined_blockers,
        manual_review_reasons=manual_review_reasons,
        missing_critical_categories=missing_critical,
        missing_required_categories=missing_required,
        stale_categories=stale_categories,
        stale_authoritative_categories=stale_authoritative,
        conflicting_categories=conflicting_categories,
        inferred_categories=inferred_categories,
        inferred_critical_categories=inferred_critical,
        incomplete_required_tiers=incomplete_required_tiers,
        tier_coverage=[row.to_dict() for row in tier_rows],
        required_categories=required_categories,
        critical_categories=critical_categories,
        overall_completeness=breakdown.overall_completeness,
        confidence_label=breakdown.confidence_label,
        authority_subscore=breakdown.authority_subscore,
        freshness_subscore=breakdown.freshness_subscore,
        governance_subscore=breakdown.governance_subscore,
        conflict_penalty=breakdown.conflict_penalty,
    )


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
    required = normalize_categories(required_categories)
    coverage = compute_category_score_from_statuses(
        required_categories=required,
        category_statuses=category_coverage or {},
    )

    category_statuses = coverage.category_statuses or {}
    category_details = {
        category: {
            "status": category_statuses.get(category, "missing"),
            "authoritative_source_count": 1 if category in set(normalize_categories(authoritative_categories)) else 0,
            "authority_score": 1.0 if category in set(normalize_categories(authoritative_categories)) else 0.0,
            "governance_score": 1.0 if category_statuses.get(category) in {"covered", "verified"} else 0.35,
            "freshness_score": 0.0 if stale_status == "stale" else 1.0,
        }
        for category in required
    }

    breakdown = JurisdictionScoreBreakdown(
        overall_completeness=coverage.completeness_score,
        completeness_status=coverage.completeness_status if stale_status != "stale" else "stale",
        coverage_subscore=coverage.completeness_score,
        freshness_subscore=0.0 if stale_status == "stale" else 1.0,
        authority_subscore=(
            len(set(coverage.covered_categories).intersection(set(normalize_categories(authoritative_categories))))
            / float(max(1, len(required)))
        ),
        extraction_subscore=coverage.completeness_score,
        governance_subscore=coverage.completeness_score,
        conflict_penalty=0.10 if coverage.conflicting_categories else 0.0,
        confidence_label=coverage.coverage_confidence,
        category_statuses=category_statuses,
        covered_categories=list(coverage.covered_categories),
        stale_categories=list(coverage.stale_categories or ([] if stale_status != "stale" else coverage.covered_categories)),
        inferred_categories=list(coverage.inferred_categories or []),
        conflicting_categories=list(coverage.conflicting_categories or []),
        missing_categories=list(coverage.missing_categories),
        category_details=category_details,
        scoring_defaults={
            "weights": completeness_score_weights(),
            "thresholds": completeness_scoring_thresholds(),
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
        "conditional_categories": list(coverage.inferred_categories or []),
        "stale_categories": list(coverage.stale_categories or []),
        "conflicting_categories": list(coverage.conflicting_categories or []),
        "category_coverage": dict(category_statuses),
        "completeness_score": float(coverage.completeness_score),
        "completeness_status": breakdown.completeness_status,
        "coverage_confidence": coverage.coverage_confidence,
        "stale_status": stale_status,
        "trust_decision": trust.to_dict(),
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
            "last_refreshed": rollup_payload["last_refresh"],
            "discovery_status": discovery_status,
            "last_discovery_run": rollup_payload["last_discovery_run"],
            "last_discovered_at": rollup_payload["last_discovered_at"],
            "production_readiness": production_readiness,
            "trustworthy_for_projection": trust_decision.safe_for_projection,
            "safe_for_user_reliance": trust_decision.safe_for_user_reliance,
            "trust_decision": trust_decision.to_dict(),
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
    coverage = JurisdictionCoverageStatus(
        org_id=profile.org_id,
        state=profile.state or "MI",
        county=profile.county,
        city=profile.city,
        pha_name=profile.pha_name,
    )
    db.add(coverage)
    db.flush()
    return coverage


def sync_coverage_status_from_profile(
    db: Session, profile: JurisdictionProfile, *, commit: bool = False
) -> JurisdictionCoverageStatus:
    coverage = get_or_create_coverage_status_for_profile(db, profile)
    detail = _loads_json_dict(profile.source_freshness_json).get("scoring", {})
    trust_decision = detail.get("trust_decision") or {}
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
    coverage.source_summary_json = _dumps(
        {
            "category_statuses": detail.get("category_statuses", {}),
            "stale_categories": detail.get("stale_categories", []),
            "inferred_categories": detail.get("inferred_categories", []),
            "conflicting_categories": detail.get("conflicting_categories", []),
            "trust_decision": trust_decision,
        }
    )

    db.add(coverage)
    if commit:
        db.commit()
        db.refresh(coverage)
    else:
        db.flush()
    return coverage