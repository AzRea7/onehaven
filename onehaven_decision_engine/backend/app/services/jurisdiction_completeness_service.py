from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..domain.jurisdiction_categories import normalize_categories
from ..domain.jurisdiction_defaults import required_categories_for_city
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


DEFAULT_STALE_DAYS = 90


@dataclass(frozen=True)
class JurisdictionFreshnessSummary:
    source_count: int
    authoritative_source_count: int
    freshest_source_at: datetime | None
    oldest_source_at: datetime | None
    freshness_payload: dict[str, Any]
    is_stale: bool
    stale_reason: str | None


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
    return json.dumps(value, sort_keys=True)


def _scope_filters(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
):
    filters = []

    if state:
        filters.append(func.upper(PolicySource.state) == state.strip().upper())
    if county:
        filters.append(func.lower(PolicySource.county) == county.strip().lower())
    if city:
        filters.append(func.lower(PolicySource.city) == city.strip().lower())

    return filters


def _scope_filters_assertion(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
):
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

    return required_categories_for_city(
        profile.city,
        state=profile.state or "MI",
        include_section8=True,
    )


def _collect_covered_categories_from_profile(profile: JurisdictionProfile) -> list[str]:
    direct = normalize_categories(_loads_json_list(profile.covered_categories_json))
    return direct


def _collect_covered_categories_from_sources(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
) -> list[str]:
    stmt = select(
        PolicySource.normalized_categories_json,
    ).where(*_scope_filters(state=state, county=county, city=city))

    categories: list[str] = []
    for (raw_json,) in db.execute(stmt).all():
        categories.extend(_loads_json_list(raw_json))

    return normalize_categories(categories)


def _collect_covered_categories_from_assertions(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
) -> list[str]:
    stmt = select(
        PolicyAssertion.normalized_category,
        PolicyAssertion.coverage_status,
    ).where(*_scope_filters_assertion(state=state, county=county, city=city))

    categories: list[str] = []
    for normalized_category, coverage_status in db.execute(stmt).all():
        if not normalized_category:
            continue
        normalized_status = (coverage_status or "").strip().lower()
        if normalized_status in {"covered", "verified", "accepted", "projected", "candidate"}:
            categories.append(normalized_category)

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
            _collect_covered_categories_from_sources(
                db,
                state=state,
                county=county,
                city=city,
            )
        )

    if include_assertions:
        categories.extend(
            _collect_covered_categories_from_assertions(
                db,
                state=state,
                county=county,
                city=city,
            )
        )

    if extra_categories:
        categories.extend(list(extra_categories))

    return normalize_categories(categories)


def compute_scope_freshness_summary(
    db: Session,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> JurisdictionFreshnessSummary:
    stmt = select(
        PolicySource.is_authoritative,
        PolicySource.retrieved_at,
        PolicySource.last_verified_at,
        PolicySource.freshness_status,
        PolicySource.url,
        PolicySource.publisher,
        PolicySource.title,
    ).where(*_scope_filters(state=state, county=county, city=city))

    rows = db.execute(stmt).all()

    source_count = len(rows)
    authoritative_source_count = 0
    timestamps: list[datetime] = []
    freshness_status_counts: dict[str, int] = {}

    for is_authoritative, retrieved_at, last_verified_at, freshness_status, url, publisher, title in rows:
        if bool(is_authoritative):
            authoritative_source_count += 1

        ts = last_verified_at or retrieved_at
        if ts is not None:
            timestamps.append(ts)

        status_key = (freshness_status or "unknown").strip().lower() or "unknown"
        freshness_status_counts[status_key] = freshness_status_counts.get(status_key, 0) + 1

    freshest_source_at = max(timestamps) if timestamps else None
    oldest_source_at = min(timestamps) if timestamps else None

    is_stale = False
    stale_reason: str | None = None

    if source_count == 0:
        is_stale = True
        stale_reason = "no_policy_sources"
    elif freshest_source_at is None:
        is_stale = True
        stale_reason = "no_freshness_timestamps"
    else:
        cutoff = _utcnow() - timedelta(days=stale_days)
        if freshest_source_at < cutoff:
            is_stale = True
            stale_reason = f"latest_source_older_than_{stale_days}_days"

    payload = {
        "source_count": source_count,
        "authoritative_source_count": authoritative_source_count,
        "freshest_source_at": freshest_source_at.isoformat() if freshest_source_at else None,
        "oldest_source_at": oldest_source_at.isoformat() if oldest_source_at else None,
        "freshness_status_counts": freshness_status_counts,
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


def compute_profile_completeness(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> tuple[JurisdictionCompleteness, JurisdictionFreshnessSummary]:
    required_categories = _coalesce_required_categories(profile)

    profile_categories = _collect_covered_categories_from_profile(profile)
    scope_categories = collect_covered_categories_for_scope(
        db,
        state=profile.state,
        county=profile.county,
        city=profile.city,
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


def apply_profile_completeness(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    commit: bool = False,
) -> JurisdictionProfile:
    completeness, freshness = compute_profile_completeness(
        db,
        profile,
        stale_days=stale_days,
    )

    profile.required_categories_json = _dumps(completeness.required_categories)
    profile.covered_categories_json = _dumps(completeness.covered_categories)
    profile.missing_categories_json = _dumps(completeness.missing_categories)
    profile.completeness_score = completeness.completeness_score
    profile.completeness_status = completeness.completeness_status
    profile.category_norm_version = getattr(profile, "category_norm_version", None) or "v1"

    profile.source_count = freshness.source_count
    profile.authoritative_source_count = freshness.authoritative_source_count
    profile.freshest_source_at = freshness.freshest_source_at
    profile.oldest_source_at = freshness.oldest_source_at
    profile.source_freshness_json = _dumps(freshness.freshness_payload)

    profile.is_stale = freshness.is_stale
    profile.stale_reason = freshness.stale_reason

    if not freshness.is_stale and completeness.completeness_status == "complete":
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


def get_or_create_coverage_status_for_profile(
    db: Session,
    profile: JurisdictionProfile,
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
    db: Session,
    profile: JurisdictionProfile,
    *,
    commit: bool = False,
) -> JurisdictionCoverageStatus:
    coverage = get_or_create_coverage_status_for_profile(db, profile)

    coverage.completeness_score = float(profile.completeness_score or 0.0)
    coverage.completeness_status = profile.completeness_status or "missing"
    coverage.required_categories_json = profile.required_categories_json or "[]"
    coverage.covered_categories_json = profile.covered_categories_json or "[]"
    coverage.missing_categories_json = profile.missing_categories_json or "[]"
    coverage.category_norm_version = profile.category_norm_version or "v1"

    coverage.last_verified_at = profile.last_verified_at
    coverage.is_stale = bool(profile.is_stale)
    coverage.stale_reason = profile.stale_reason

    coverage.source_count = int(profile.source_count or 0)
    coverage.freshest_source_at = profile.freshest_source_at
    coverage.oldest_source_at = profile.oldest_source_at
    coverage.source_freshness_json = profile.source_freshness_json or "{}"
    coverage.last_source_refresh_at = profile.last_refresh_success_at
    coverage.last_reviewed_at = profile.last_verified_at

    covered_categories = normalize_categories(_loads_json_list(profile.covered_categories_json))
    coverage.verified_rule_count = len(covered_categories)
    coverage.stale_warning_count = 1 if profile.is_stale else 0

    if coverage.completeness_status == "complete" and not coverage.is_stale:
        coverage.coverage_status = "covered"
        coverage.production_readiness = "ready"
    elif coverage.completeness_status == "partial":
        coverage.coverage_status = "partial"
        coverage.production_readiness = "partial"
    else:
        coverage.coverage_status = "not_started" if not covered_categories else "partial"
        coverage.production_readiness = "blocked" if coverage.is_stale else "partial"

    db.add(coverage)
    if commit:
        db.commit()
        db.refresh(coverage)
    else:
        db.flush()

    return coverage


def recompute_profile_and_coverage(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    commit: bool = False,
) -> tuple[JurisdictionProfile, JurisdictionCoverageStatus]:
    profile = apply_profile_completeness(
        db,
        profile,
        stale_days=stale_days,
        commit=False,
    )
    coverage = sync_coverage_status_from_profile(
        db,
        profile,
        commit=False,
    )

    if commit:
        db.commit()
        db.refresh(profile)
        db.refresh(coverage)

    return profile, coverage


def recompute_profile_by_id(
    db: Session,
    jurisdiction_profile_id: int,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    commit: bool = False,
) -> tuple[JurisdictionProfile | None, JurisdictionCoverageStatus | None]:
    profile = db.get(JurisdictionProfile, jurisdiction_profile_id)
    if profile is None:
        return None, None

    return recompute_profile_and_coverage(
        db,
        profile,
        stale_days=stale_days,
        commit=commit,
    )


def recompute_all_profiles(
    db: Session,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    org_id: int | None = None,
    commit_every: int = 100,
) -> list[int]:
    stmt = select(JurisdictionProfile.id).order_by(JurisdictionProfile.id)
    if org_id is not None:
        stmt = stmt.where(JurisdictionProfile.org_id == org_id)

    ids = [row[0] for row in db.execute(stmt).all()]
    touched: list[int] = []

    for idx, profile_id in enumerate(ids, start=1):
        profile, _ = recompute_profile_by_id(
            db,
            profile_id,
            stale_days=stale_days,
            commit=False,
        )
        if profile is not None:
            touched.append(profile.id)

        if idx % max(1, commit_every) == 0:
            db.commit()

    db.commit()
    return touched


def profile_completeness_payload(
    db: Session,
    profile: JurisdictionProfile,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, Any]:
    completeness, freshness = compute_profile_completeness(
        db,
        profile,
        stale_days=stale_days,
    )

    return {
        "jurisdiction_profile_id": profile.id,
        "org_id": profile.org_id,
        "state": profile.state,
        "county": profile.county,
        "city": profile.city,
        "pha_name": profile.pha_name,
        "required_categories": completeness.required_categories,
        "covered_categories": completeness.covered_categories,
        "missing_categories": completeness.missing_categories,
        "completeness_score": completeness.completeness_score,
        "completeness_status": completeness.completeness_status,
        "source_count": freshness.source_count,
        "authoritative_source_count": freshness.authoritative_source_count,
        "freshest_source_at": freshness.freshest_source_at.isoformat() if freshness.freshest_source_at else None,
        "oldest_source_at": freshness.oldest_source_at.isoformat() if freshness.oldest_source_at else None,
        "source_freshness": freshness.freshness_payload,
        "is_stale": freshness.is_stale,
        "stale_reason": freshness.stale_reason,
        "stale_days": stale_days,
    }
