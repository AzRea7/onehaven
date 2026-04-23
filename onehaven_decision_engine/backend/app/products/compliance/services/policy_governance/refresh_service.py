from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.products.compliance.services.policy_coverage.completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.products.compliance.services.policy_governance.notification_service import (
    build_jurisdiction_profile_stale_notification,
    build_review_queue_payload,
    build_rule_change_notification,
    build_source_refresh_notification,
    build_stale_source_notification,
    notify_if_jurisdiction_stale,
    record_notification_event,
)
from app.products.compliance.services.policy_assertions.extractor_service import extract_assertions_for_source, mark_assertions_stale_for_source
from app.products.compliance.services.policy_assertions.review_service import apply_governance_lifecycle, diff_active_rules_for_source
from app.products.compliance.services.policy_sources.source_service import (
    collect_catalog_for_market,
    discover_policy_sources_for_market,
    inventory_summary_for_market,
    list_sources_for_market,
    policy_source_needs_refresh,
    refresh_policy_source_and_detect_changes,
)
from app.services.policy_change_detection_service import (
    compute_next_retry_due,
    determine_profile_refresh_state as _policy_change_determine_profile_refresh_state,
 )


DEFAULT_JURISDICTION_STALE_DAYS = 90


@dataclass(frozen=True)
class JurisdictionRefreshTarget:
    jurisdiction_profile_id: int
    org_id: int | None
    state: str
    county: str | None
    city: str | None
    pha_name: str | None
    stale_reason: str | None
    last_refresh_success_at: datetime | None
    last_verified_at: datetime | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "jurisdiction_profile_id": self.jurisdiction_profile_id,
            "org_id": self.org_id,
            "state": self.state,
            "county": self.county,
            "city": self.city,
            "pha_name": self.pha_name,
            "stale_reason": self.stale_reason,
            "last_refresh_success_at": self.last_refresh_success_at.isoformat()
            if self.last_refresh_success_at
            else None,
            "last_verified_at": self.last_verified_at.isoformat()
            if self.last_verified_at
            else None,
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

def determine_profile_refresh_state(
    *,
    refresh_runs: list[dict[str, Any]] | None = None,
    recompute_ok: bool | None = None,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    overdue_categories: list[str] | None = None,
    critical_overdue_categories: list[str] | None = None,
    profile_is_stale: bool | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Compatibility wrapper for older policy_change_detection signatures.

    Newer callers in this file pass overdue_categories / critical_overdue_categories.
    Older implementations of policy_change_detection_service.determine_profile_refresh_state
    may not accept those keyword args. This wrapper preserves backward compatibility
    and guarantees refresh_state / status_reason / next_step keys.
    """
    try:
        return _policy_change_determine_profile_refresh_state(
            refresh_runs=refresh_runs,
            recompute_ok=recompute_ok,
            missing_categories=missing_categories,
            stale_categories=stale_categories,
            overdue_categories=overdue_categories,
            critical_overdue_categories=critical_overdue_categories,
            profile_is_stale=profile_is_stale,
            **kwargs,
        )
    except TypeError:
        base = _policy_change_determine_profile_refresh_state(
            refresh_runs=refresh_runs,
            recompute_ok=recompute_ok,
            missing_categories=missing_categories,
            stale_categories=stale_categories,
            profile_is_stale=profile_is_stale,
        )
        out = dict(base or {})
        refresh_state = str(out.get("refresh_state") or "").strip().lower()

        missing = list(missing_categories or [])
        stale = list(stale_categories or [])
        overdue = list(overdue_categories or [])
        critical_overdue = list(critical_overdue_categories or [])

        if critical_overdue:
            if not refresh_state or refresh_state == "healthy":
                refresh_state = "blocked"
            out["status_reason"] = out.get("status_reason") or "critical_refresh_overdue"
            out["next_step"] = out.get("next_step") or "refresh"
        elif overdue or stale or bool(profile_is_stale):
            if not refresh_state or refresh_state == "healthy":
                refresh_state = "degraded"
            out["status_reason"] = out.get("status_reason") or "sources_or_categories_overdue"
            out["next_step"] = out.get("next_step") or "refresh"
        elif missing:
            if not refresh_state:
                refresh_state = "pending"
            out["status_reason"] = out.get("status_reason") or "coverage_incomplete"
            out["next_step"] = out.get("next_step") or "review"
        elif recompute_ok and not refresh_state:
            refresh_state = "healthy"
            out["status_reason"] = None
            out["next_step"] = "monitor"

        out["refresh_state"] = refresh_state or "pending"
        out.setdefault("status_reason", None)
        out.setdefault("next_step", "refresh")
        return out



def _stale_cutoff(stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS) -> datetime:
    return _utcnow() - timedelta(days=int(stale_days))


def _loads_json_dict(value: Any) -> dict[str, Any]:
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


def _loads_json_list(value: Any) -> list[Any]:
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


def _missing_categories_for_profile(
    db: Session,
    *,
    profile: JurisdictionProfile,
    stale_days: int,
) -> list[str]:
    try:
        payload = profile_completeness_payload(db, profile, stale_days=stale_days)
        missing = payload.get("missing_categories") or []
        if missing:
            return [str(item).strip().lower() for item in missing if str(item).strip()]
    except Exception:
        pass

    direct = _loads_json_list(getattr(profile, "missing_categories_json", None))
    if direct:
        return [str(item).strip().lower() for item in direct if str(item).strip()]

    freshness = _loads_json_dict(getattr(profile, "source_freshness_json", None))
    scoring = freshness.get("scoring") if isinstance(freshness.get("scoring"), dict) else {}
    missing = scoring.get("missing_categories") if isinstance(scoring.get("missing_categories"), list) else []
    return [str(item).strip().lower() for item in missing if str(item).strip()]


def _assertion_to_candidate_payload(row: PolicyAssertion) -> dict[str, Any]:
    return {
        "rule_key": getattr(row, "rule_key", None),
        "rule_category": getattr(row, "rule_category", None) or getattr(row, "normalized_category", None),
        "source_level": getattr(row, "source_level", None),
        "property_type": getattr(row, "property_type", None),
        "required": getattr(row, "required", True),
        "blocking": getattr(row, "blocking", False),
        "confidence": getattr(row, "confidence", 0.0),
        "governance_state": getattr(row, "governance_state", None),
        "rule_status": getattr(row, "rule_status", None),
        "normalized_version": getattr(row, "normalized_version", None),
        "version_group": getattr(row, "version_group", None),
        "value_json": getattr(row, "value_json", None),
        "source_citation": getattr(row, "source_citation", None),
        "raw_excerpt": getattr(row, "raw_excerpt", None),
        "source_id": getattr(row, "source_id", None),
        "source_version_id": getattr(row, "source_version_id", None),
        "state": getattr(row, "state", None),
        "county": getattr(row, "county", None),
        "city": getattr(row, "city", None),
        "pha_name": getattr(row, "pha_name", None),
        "program_type": getattr(row, "program_type", None),
        "source_freshness_status": getattr(row, "source_freshness_status", None),
    }


def _refresh_source_batch(
    db: Session,
    *,
    sources: list[PolicySource],
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    force: bool,
) -> dict[str, Any]:
    refresh_results: list[dict[str, Any]] = []
    extraction_results: list[dict[str, Any]] = []
    diff_results: list[dict[str, Any]] = []
    changed_source_ids: list[int] = []
    failed_source_ids: list[int] = []
    total_changed_rules = 0
    total_new_rules = 0
    total_missing_rules = 0

    for source in sources:
        if not policy_source_needs_refresh(source, force=force):
            refresh_results.append(
                {
                    "source_id": int(source.id),
                    "url": source.url,
                    "ok": True,
                    "skipped": True,
                    "reason": "fresh_enough",
                    "changed": False,
                    "change_detected": False,
                }
            )
            continue

        fetch_result = refresh_policy_source_and_detect_changes(
            db,
            source=source,
            force=force,
        )
        refresh_results.append(
            {
                "source_id": int(source.id),
                "url": source.url,
                **fetch_result,
            }
        )

        if not fetch_result.get("ok"):
            failed_source_ids.append(int(source.id))
            stale_update = mark_assertions_stale_for_source(
                db,
                source_id=int(source.id),
                reason="source_fetch_failed",
            )
            extraction_results.append(
                {
                    "source_id": int(source.id),
                    "url": source.url,
                    "refresh_ok": False,
                    "changed": False,
                    "change_detected": False,
                    "assertion_ids": [],
                    "stale_update": stale_update,
                }
            )
            continue

        content_changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))
        created_rows: list[PolicyAssertion] = []
        stale_update: dict[str, Any] | None = None

        if content_changed or force:
            changed_source_ids.append(int(source.id))
            stale_update = mark_assertions_stale_for_source(
                db,
                source_id=int(source.id),
                reason="source_content_changed",
            )
            created_rows = extract_assertions_for_source(
                db,
                source=source,
                org_id=org_id,
                org_scope=(org_id is not None),
            )

        extraction_results.append(
            {
                "source_id": int(source.id),
                "url": source.url,
                "refresh_ok": True,
                "changed": content_changed,
                "change_detected": content_changed,
                "stale_update": stale_update,
                "extracted_count": len(created_rows),
                "assertion_ids": [int(a.id) for a in created_rows],
            }
        )

        candidate_rows = [
            row
            for row in _scope_assertions_for_source(
                db,
                source_id=int(source.id),
                org_id=org_id,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
            )
            if (getattr(row, "governance_state", None) or "").lower() in {"draft", "approved"}
        ]

        if content_changed or force:
            diff_result = diff_active_rules_for_source(
                db,
                org_id=org_id,
                source_id=int(source.id),
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                raw_candidates=[_assertion_to_candidate_payload(row) for row in candidate_rows],
            )
        else:
            diff_result = {
                "ok": True,
                "source_id": int(source.id),
                "changed_count": 0,
                "new_count": 0,
                "missing_count": 0,
                "skipped": True,
                "reason": "no_content_change",
            }

        diff_results.append(
            {
                "source_id": int(source.id),
                "url": source.url,
                **diff_result,
            }
        )

        total_changed_rules += int(diff_result.get("changed_count") or 0)
        total_new_rules += int(diff_result.get("new_count") or 0)
        total_missing_rules += int(diff_result.get("missing_count") or 0)

    return {
        "refresh_results": refresh_results,
        "extraction_results": extraction_results,
        "diff_results": diff_results,
        "changed_source_ids": sorted(set(changed_source_ids)),
        "failed_source_ids": sorted(set(failed_source_ids)),
        "summary": {
            "changed_source_count": len(set(changed_source_ids)),
            "failed_source_count": len(set(failed_source_ids)),
            "changed_rule_count": total_changed_rules,
            "new_rule_count": total_new_rules,
            "missing_rule_count": total_missing_rules,
        },
    }

def list_markets_needing_refresh(
    db: Session,
    org_id: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    rows = list_refresh_targets(
        db,
        org_id=org_id,
        limit=limit or 100,
    )
    return [
        {
            "jurisdiction_profile_id": int(row.jurisdiction_profile_id),
            "org_id": row.org_id,
            "state": row.state,
            "county": row.county,
            "city": row.city,
            "pha_name": row.pha_name,
            "stale_reason": row.stale_reason,
            "last_refresh_success_at": row.last_refresh_success_at.isoformat() if row.last_refresh_success_at else None,
            "last_verified_at": row.last_verified_at.isoformat() if row.last_verified_at else None,
        }
        for row in rows
    ]


def list_jurisdictions_needing_refresh(
    db: Session,
    *,
    org_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> list[JurisdictionRefreshTarget]:
    cutoff = _stale_cutoff(stale_days)

    stmt = select(JurisdictionProfile).where(
        or_(
            JurisdictionProfile.is_stale.is_(True),
            JurisdictionProfile.last_verified_at.is_(None),
            JurisdictionProfile.last_refresh_success_at.is_(None),
            JurisdictionProfile.last_refresh_success_at < cutoff,
            JurisdictionProfile.last_verified_at < cutoff,
        )
    ).order_by(JurisdictionProfile.id.asc())

    if org_id is not None:
        stmt = stmt.where(
            or_(
                JurisdictionProfile.org_id == int(org_id),
                JurisdictionProfile.org_id.is_(None),
            )
        )

    if batch_size is not None:
        stmt = stmt.limit(max(1, int(batch_size)))

    rows = list(db.scalars(stmt).all())
    out: list[JurisdictionRefreshTarget] = []

    for row in rows:
        out.append(
            JurisdictionRefreshTarget(
                jurisdiction_profile_id=int(row.id),
                org_id=getattr(row, "org_id", None),
                state=_norm_state(getattr(row, "state", None)),
                county=getattr(row, "county", None),
                city=getattr(row, "city", None),
                pha_name=getattr(row, "pha_name", None),
                stale_reason=getattr(row, "stale_reason", None),
                last_refresh_success_at=getattr(row, "last_refresh_success_at", None),
                last_verified_at=getattr(row, "last_verified_at", None),
            )
        )

    return out


def build_jurisdiction_refresh_payload(
    *,
    org_id: int | None,
    jurisdiction_profile_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    reason: str | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> dict[str, Any]:
    return {
        "trigger_type": "jurisdiction_refresh",
        "org_id": org_id,
        "jurisdiction_profile_id": jurisdiction_profile_id,
        "state": _norm_state(state),
        "county": _norm_lower(county),
        "city": _norm_lower(city),
        "pha_name": _norm_text(pha_name),
        "reason": reason,
        "force": bool(force),
        "stale_days": int(stale_days),
    }


def _needs_refresh(
    profile: JurisdictionProfile,
    *,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> bool:
    if force:
        return True

    if bool(getattr(profile, "is_stale", False)):
        return True

    cutoff = _stale_cutoff(stale_days)
    last_success = getattr(profile, "last_refresh_success_at", None)
    last_verified = getattr(profile, "last_verified_at", None)

    if last_success is None:
        return True
    if last_success < cutoff:
        return True
    if last_verified is None:
        return True
    if last_verified < cutoff:
        return True

    return False


def _scope_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    stmt = select(PolicyAssertion).where(PolicyAssertion.state == _norm_state(state))
    if org_id is None:
        stmt = stmt.where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicyAssertion.org_id == int(org_id), PolicyAssertion.org_id.is_(None)))

    rows = list(db.scalars(stmt).all())
    out: list[PolicyAssertion] = []
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    for row in rows:
        if getattr(row, "county", None) is not None and getattr(row, "county", None) != cnty:
            continue
        if getattr(row, "city", None) is not None and getattr(row, "city", None) != cty:
            continue
        if getattr(row, "pha_name", None) is not None and getattr(row, "pha_name", None) != pha:
            continue
        out.append(row)
    return out


def _scope_assertions_for_source(
    db: Session,
    *,
    source_id: int,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    rows = _scope_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return [row for row in rows if int(getattr(row, "source_id", 0) or 0) == int(source_id)]


def _refresh_confidence_from_profile(profile_payload: dict[str, Any]) -> dict[str, Any]:
    confidence_score = 0.0
    if profile_payload.get("completeness_score") is not None:
        confidence_score += 0.5 * float(profile_payload.get("completeness_score") or 0.0)
    if not profile_payload.get("is_stale"):
        confidence_score += 0.25
    if profile_payload.get("last_refresh_success_at"):
        confidence_score += 0.25
    confidence_score = max(0.0, min(1.0, round(confidence_score, 3)))
    return {
        "coverage_confidence": "high" if confidence_score >= 0.75 else ("medium" if confidence_score >= 0.45 else "low"),
        "confidence_score": confidence_score,
    }


def _set_profile_refresh_state(
    profile: JurisdictionProfile,
    *,
    refresh_state: str,
    reason: str | None,
    now: datetime,
    blocked_reason: str | None = None,
    next_step: str | None = None,
    requirements: dict[str, Any] | None = None,
    retry_count: int | None = None,
    run_id: str | None = None,
) -> JurisdictionProfile:
    profile.refresh_state = refresh_state
    profile.refresh_status_reason = reason
    profile.refresh_blocked_reason = blocked_reason
    profile.last_refresh_state_transition_at = now
    if retry_count is not None:
        profile.refresh_retry_count = int(retry_count)
    if run_id is not None:
        profile.current_refresh_run_id = run_id
    existing_requirements = _loads_json_dict(getattr(profile, "refresh_requirements_json", None))
    if next_step is not None:
        existing_requirements["next_step"] = next_step
    if isinstance(requirements, dict) and requirements:
        truth_keys = {
            "overdue_categories",
            "critical_overdue_categories",
            "legal_overdue_categories",
            "informational_overdue_categories",
            "stale_authoritative_categories",
            "critical_fetch_failure_categories",
            "legal_lockout_categories",
            "review_required_categories",
            "rejected_source_count",
            "guessed_source_count",
            "blocked_source_count",
            "fetch_failed_source_count",
            "failed_binding_source_count",
            "safe_to_rely_on",
            "next_due_at",
            "inventory_summary",
            "missing_categories",
            "stale_categories",
            "next_search_retry_due_at",
            "last_refresh_completed_at",
            "refresh_state",
        }
        for key in truth_keys:
            existing_requirements.pop(key, None)
        existing_requirements.update(requirements)
    profile.refresh_requirements_json = json.dumps(existing_requirements, ensure_ascii=False, sort_keys=True, default=str)
    return profile



def refresh_jurisdiction_profile(
    db: Session,
    *,
    jurisdiction_profile_id: int,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        return {
            "ok": False,
            "error": "jurisdiction_profile_not_found",
            "jurisdiction_profile_id": int(jurisdiction_profile_id),
        }

    now = _utcnow()
    run_id = f"jurisdiction-refresh-{int(profile.id)}-{int(now.timestamp())}"
    state = _norm_state(getattr(profile, "state", None))
    county = _norm_lower(getattr(profile, "county", None))
    city = _norm_lower(getattr(profile, "city", None))
    pha_name = _norm_text(getattr(profile, "pha_name", None))
    org_id = getattr(profile, "org_id", None)

    if not _needs_refresh(profile, force=force, stale_days=stale_days):
        profile_obj, coverage = recompute_profile_and_coverage(
            db,
            profile,
            stale_days=stale_days,
            commit=True,
        )
        profile_payload = {
            "id": int(profile_obj.id),
            "org_id": profile_obj.org_id,
            "state": profile_obj.state,
            "county": profile_obj.county,
            "city": profile_obj.city,
            "pha_name": getattr(profile_obj, "pha_name", None),
            "completeness_status": profile_obj.completeness_status,
            "completeness_score": float(profile_obj.completeness_score or 0.0),
            "is_stale": bool(profile_obj.is_stale),
            "stale_reason": profile_obj.stale_reason,
            "last_refresh_success_at": profile_obj.last_refresh_success_at.isoformat()
            if profile_obj.last_refresh_success_at
            else None,
            "refresh_state": getattr(profile_obj, "refresh_state", None),
        }
        return {
            "ok": True,
            "skipped": True,
            "reason": "not_stale",
            "jurisdiction_profile_id": int(profile_obj.id),
            "refresh_payload": build_jurisdiction_refresh_payload(
                org_id=org_id,
                jurisdiction_profile_id=int(profile_obj.id),
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
                reason="not_stale_recompute",
                force=force,
                stale_days=stale_days,
            ),
            "profile": profile_payload,
            "coverage": {
                "id": int(coverage.id),
                "coverage_status": getattr(coverage, "coverage_status", None),
                "production_readiness": getattr(coverage, "production_readiness", None),
                "completeness_status": getattr(coverage, "completeness_status", None),
                "is_stale": bool(getattr(coverage, "is_stale", False)),
            },
            "refresh_confidence": _refresh_confidence_from_profile(profile_payload),
        }

    profile.last_refresh_started_at = now
    profile.last_refresh_attempt_at = now
    profile.last_refresh_error = None
    _set_profile_refresh_state(
        profile,
        refresh_state="pending",
        reason="refresh_started",
        now=now,
        next_step="discover",
        retry_count=int(getattr(profile, "refresh_retry_count", 0) or 0),
        run_id=run_id,
    )
    db.add(profile)
    db.commit()

    missing_categories = _missing_categories_for_profile(
        db,
        profile=profile,
        stale_days=stale_days,
    )
    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    _set_profile_refresh_state(profile, refresh_state="crawling", reason="discovery_running", now=_utcnow(), next_step="crawl", run_id=run_id)
    db.add(profile)
    db.commit()

    discovery_result = discover_policy_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        missing_categories=missing_categories,
        focus=focus,
        probe=True,
    )

    collect_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    sources = list_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    batch_result = _refresh_source_batch(
        db,
        sources=sources,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        force=force,
    )

    refresh_results = batch_result["refresh_results"]
    extraction_results = batch_result["extraction_results"]
    diff_results = batch_result["diff_results"]
    notification_results: list[dict[str, Any]] = []

    for source, refresh_result in zip(
        [row for row in sources if policy_source_needs_refresh(row, force=force) or force],
        [row for row in refresh_results if not row.get("skipped")],
    ):
        notification_results.append(
            record_notification_event(
                db,
                payload=build_source_refresh_notification(source=source, refresh_result=refresh_result),
            )
        )
        if not refresh_result.get("ok"):
            notification_results.append(
                record_notification_event(
                    db,
                    payload=build_stale_source_notification(source=source),
                )
            )

    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    _set_profile_refresh_state(profile, refresh_state="validating", reason="governance_validation_running", now=_utcnow(), next_step="recompute", run_id=run_id)
    db.add(profile)
    db.commit()

    governance_result = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )

    summary = batch_result["summary"]
    if (
        governance_result.get("active_count", 0)
        or governance_result.get("replaced_count", 0)
        or summary.get("changed_rule_count", 0)
        or summary.get("new_rule_count", 0)
        or summary.get("missing_rule_count", 0)
    ):
        for source in sources:
            notification_results.append(
                record_notification_event(
                    db,
                    payload=build_rule_change_notification(source=source, governance_result=governance_result),
                )
            )

    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    _set_profile_refresh_state(profile, refresh_state="recomputing", reason="recompute_running", now=_utcnow(), next_step="finalize", run_id=run_id)
    db.add(profile)
    db.commit()

    refreshed_profile = db.get(JurisdictionProfile, int(profile.id))
    refreshed_profile, coverage = recompute_profile_and_coverage(
        db,
        refreshed_profile,
        stale_days=stale_days,
        commit=True,
    )

    assertions = _scope_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    review_queue = build_review_queue_payload(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        assertions=assertions,
    )
    notification_results.append(
        record_notification_event(
            db,
            payload={
                "kind": "jurisdiction_review_queue_updated",
                "entity_type": "jurisdiction_profile",
                "entity_id": str(int(refreshed_profile.id)),
                "jurisdiction_profile_id": int(refreshed_profile.id),
                "org_id": org_id,
                "message": "Jurisdiction review queue updated after refresh.",
                "review_queue": review_queue,
            },
        )
    )

    stale_profile_notice = notify_if_jurisdiction_stale(
        db,
        profile=refreshed_profile,
    )

    inventory_summary = inventory_summary_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type="section8" if "section8" in set(missing_categories) else None,
    )
    profile_payload = profile_completeness_payload(db, refreshed_profile, stale_days=stale_days)
    sla_summary = _chunk3_collect_profile_source_sla_summary(db, profile=refreshed_profile)
    state_payload = determine_profile_refresh_state(
        refresh_runs=[{"refresh": row} for row in refresh_results],
        recompute_ok=True,
        missing_categories=list(profile_payload.get("missing_categories") or []),
        stale_categories=list(profile_payload.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        profile_is_stale=bool(profile_payload.get("is_stale", False)),
    )
    unresolved_missing = list(profile_payload.get("missing_categories") or [])
    retry_due_at = None
    if unresolved_missing or state_payload["refresh_state"] in {"pending", "degraded", "validating", "failed", "blocked"}:
        retry_due_at = compute_next_retry_due(
            retry_count=int(getattr(refreshed_profile, "refresh_retry_count", 0) or 0),
            base_dt=_utcnow(),
            min_hours=24,
            max_days=14,
        )
    requirements = _chunk3_build_refresh_requirements(
        refreshed_profile,
        next_step=state_payload["next_step"],
        missing_categories=unresolved_missing,
        stale_categories=list(profile_payload.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        legal_overdue_categories=list(sla_summary.get("legal_overdue_categories") or []),
        informational_overdue_categories=list(sla_summary.get("informational_overdue_categories") or []),
        stale_authoritative_categories=list(sla_summary.get("stale_authoritative_categories") or []),
        inventory_summary={
            **dict(sla_summary),
            "inventory_summary": {
                "inventory_count": inventory_summary.get("inventory_count"),
                "lifecycle_counts": inventory_summary.get("lifecycle_counts"),
                "crawl_counts": inventory_summary.get("crawl_counts"),
            },
        },
        retry_due_at=retry_due_at,
    )

    _set_profile_refresh_state(
        refreshed_profile,
        refresh_state=state_payload["refresh_state"],
        reason=state_payload["status_reason"],
        now=_utcnow(),
        blocked_reason="blocked_sources_present" if (state_payload["refresh_state"] == "blocked" and unresolved_missing) else None,
        next_step=state_payload["next_step"],
        requirements=requirements,
        retry_count=(0 if state_payload["refresh_state"] == "healthy" else int(getattr(refreshed_profile, "refresh_retry_count", 0) or 0) + (1 if unresolved_missing else 0)),
        run_id=run_id,
    )
    refreshed_profile.last_refresh_completed_at = _utcnow()
    refreshed_profile.last_refresh_success_at = _utcnow() if state_payload["refresh_state"] in {"healthy", "degraded", "pending", "validating"} else getattr(refreshed_profile, "last_refresh_success_at", None)
    refreshed_profile.last_refresh_changed_source_count = int(summary.get("changed_source_count") or 0)
    refreshed_profile.last_refresh_changed_rule_count = int(summary.get("changed_rule_count") or 0)
    refreshed_profile.last_refresh_outcome_json = json.dumps(
        {
            "ok": True,
            "run_id": run_id,
            "refresh_state": state_payload["refresh_state"],
            "status_reason": state_payload["status_reason"],
            "summary": summary,
            "governance_result": governance_result,
            "inventory_summary": {
                "inventory_count": inventory_summary.get("inventory_count"),
                "lifecycle_counts": inventory_summary.get("lifecycle_counts"),
                "crawl_counts": inventory_summary.get("crawl_counts"),
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    db.add(refreshed_profile)
    db.commit()

    profile_payload = {
        "id": int(refreshed_profile.id),
        "org_id": refreshed_profile.org_id,
        "state": refreshed_profile.state,
        "county": refreshed_profile.county,
        "city": refreshed_profile.city,
        "pha_name": getattr(refreshed_profile, "pha_name", None),
        "completeness_status": refreshed_profile.completeness_status,
        "completeness_score": float(refreshed_profile.completeness_score or 0.0),
        "is_stale": bool(refreshed_profile.is_stale),
        "stale_reason": refreshed_profile.stale_reason,
        "last_refresh_success_at": refreshed_profile.last_refresh_success_at.isoformat()
        if refreshed_profile.last_refresh_success_at
        else None,
        "refresh_state": getattr(refreshed_profile, "refresh_state", None),
        "refresh_status_reason": getattr(refreshed_profile, "refresh_status_reason", None),
    }

    return {
        "ok": True,
        "jurisdiction_profile_id": int(refreshed_profile.id),
        "refresh_payload": build_jurisdiction_refresh_payload(
            org_id=org_id,
            jurisdiction_profile_id=int(refreshed_profile.id),
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            reason="refreshed",
            force=force,
            stale_days=stale_days,
        ),
        "profile": profile_payload,
        "coverage": {
            "id": int(coverage.id),
            "coverage_status": getattr(coverage, "coverage_status", None),
            "production_readiness": getattr(coverage, "production_readiness", None),
            "completeness_status": getattr(coverage, "completeness_status", None),
            "is_stale": bool(getattr(coverage, "is_stale", False)),
        },
        "refresh_confidence": _refresh_confidence_from_profile(profile_payload),
        "missing_categories": missing_categories,
        "discovery_result": discovery_result,
        "sources_total": len(sources),
        "refresh_results": refresh_results,
        "extraction_results": extraction_results,
        "diff_results": diff_results,
        "governance_result": governance_result,
        "review_queue": review_queue,
        "notification_results": notification_results,
        "stale_profile_notification": stale_profile_notice,
        "inventory_summary": inventory_summary,
        "summary": {
            **summary,
            "discovered_source_count": int(discovery_result.get("created_count") or 0),
            "refresh_state": state_payload["refresh_state"],
            "next_step": state_payload["next_step"],
        },
    }


def refresh_due_jurisdictions(

    db: Session,
    *,
    org_id: int | None = None,
    reviewer_user_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    targets = list_jurisdictions_needing_refresh(
        db,
        org_id=org_id,
        batch_size=batch_size,
        stale_days=stale_days,
    )

    refreshed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    for target in targets:
        try:
            result = refresh_jurisdiction_profile(
                db,
                jurisdiction_profile_id=target.jurisdiction_profile_id,
                reviewer_user_id=reviewer_user_id,
                force=False,
                stale_days=stale_days,
                focus=focus,
            )
            if result.get("ok"):
                refreshed.append(
                    {
                        "jurisdiction_profile_id": target.jurisdiction_profile_id,
                        "state": target.state,
                        "county": target.county,
                        "city": target.city,
                        "pha_name": target.pha_name,
                        "skipped": bool(result.get("skipped", False)),
                        "discovered_source_count": int((result.get("discovery_result") or {}).get("created_count") or 0),
                        "changed_source_count": int((result.get("summary") or {}).get("changed_source_count") or 0),
                    }
                )
            else:
                failed.append(
                    {
                        "jurisdiction_profile_id": target.jurisdiction_profile_id,
                        "state": target.state,
                        "county": target.county,
                        "city": target.city,
                        "pha_name": target.pha_name,
                        "error": result.get("error", "refresh_failed"),
                    }
                )
        except Exception as exc:
            failed.append(
                {
                    "jurisdiction_profile_id": target.jurisdiction_profile_id,
                    "state": target.state,
                    "county": target.county,
                    "city": target.city,
                    "pha_name": target.pha_name,
                    "error": str(exc),
                }
            )

    return {
        "ok": len(failed) == 0,
        "org_id": org_id,
        "target_count": len(targets),
        "refreshed_count": len(refreshed),
        "failed_count": len(failed),
        "refreshed": refreshed,
        "failed": failed,
    }


def list_stale_policy_sources(
    db: Session,
    *,
    org_id: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    batch_size: int | None = None,
) -> list[PolicySource]:
    cutoff = _stale_cutoff(stale_days)

    stmt = select(PolicySource).where(
        or_(
            PolicySource.freshness_status == "stale",
            PolicySource.freshness_status == "fetch_failed",
            PolicySource.last_verified_at.is_(None),
            PolicySource.retrieved_at.is_(None),
            PolicySource.retrieved_at < cutoff,
            PolicySource.next_refresh_due_at < _utcnow(),
        )
    ).order_by(PolicySource.id.asc())

    if org_id is not None:
        stmt = stmt.where(
            or_(
                PolicySource.org_id == int(org_id),
                PolicySource.org_id.is_(None),
            )
        )

    if batch_size is not None:
        stmt = stmt.limit(max(1, int(batch_size)))

    return list(db.scalars(stmt).all())


def notify_stale_policy_sources(
    db: Session,
    *,
    org_id: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    batch_size: int | None = None,
) -> dict[str, Any]:
    rows = list_stale_policy_sources(
        db,
        org_id=org_id,
        stale_days=stale_days,
        batch_size=batch_size,
    )

    recorded: list[dict[str, Any]] = []
    for source in rows:
        recorded.append(
            record_notification_event(
                db,
                payload=build_stale_source_notification(source=source),
            )
        )

    return {
        "ok": True,
        "org_id": org_id,
        "stale_source_count": len(rows),
        "recorded_count": len(recorded),
        "results": recorded,
    }


def mark_profile_stale_if_needed(
    db: Session,
    *,
    profile: JurisdictionProfile,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
) -> dict[str, Any]:
    cutoff = _stale_cutoff(stale_days)
    latest = getattr(profile, "last_refresh_success_at", None) or getattr(profile, "last_verified_at", None)

    if latest is None or latest < cutoff:
        profile.is_stale = True
        profile.stale_reason = "refresh_overdue"
        db.add(profile)
        db.commit()
        payload = build_jurisdiction_profile_stale_notification(profile=profile)
        record_notification_event(db, payload=payload)
        return {
            "ok": True,
            "marked_stale": True,
            "jurisdiction_profile_id": int(profile.id),
            "payload": payload,
        }

    return {
        "ok": True,
        "marked_stale": False,
        "jurisdiction_profile_id": int(profile.id),
    }

from app.config import settings as _chunk8_settings
from app.products.compliance.services.policy_coverage.health_service import get_jurisdiction_health as _chunk8_get_jurisdiction_health
from app.products.compliance.services.policy_coverage.lockout_service import profile_lockout_payload as _chunk8_profile_lockout_payload
from app.products.compliance.services.policy_governance.notification_service import notify_if_profile_locked as _chunk8_notify_if_profile_locked
from app.products.compliance.services.policy_coverage.sla_service import source_due_at as _chunk8_source_due_at, source_is_past_sla as _chunk8_source_is_past_sla

_chunk8_original_refresh_jurisdiction_profile = refresh_jurisdiction_profile
_chunk8_original_refresh_due_jurisdictions = refresh_due_jurisdictions


def refresh_jurisdiction_profile(
    db: Session,
    *,
    jurisdiction_profile_id: int,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    result = _chunk8_original_refresh_jurisdiction_profile(
        db,
        jurisdiction_profile_id=jurisdiction_profile_id,
        reviewer_user_id=reviewer_user_id,
        force=force,
        stale_days=stale_days,
        focus=focus,
    )

    if not result.get("ok"):
        return result

    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        return result

    profile_payload = result.get("profile") or {}
    lockout = _chunk8_profile_lockout_payload(profile, profile_payload)

    # Enforce next-search retry when there are unresolved gaps.
    requirements = _loads_json_dict(getattr(profile, "refresh_requirements_json", None))
    if result.get("missing_categories"):
        requirements["next_search_retry_due_at"] = compute_next_retry_due(
            retry_count=int(getattr(profile, "refresh_retry_count", 0) or 0),
            base_dt=_utcnow(),
            min_hours=int(getattr(_chunk8_settings, "jurisdiction_sla_discovery_retry_hours", 24) or 24),
            max_days=14,
        ).isoformat()
        requirements.setdefault("next_step", "retry_discovery")
        profile.refresh_requirements_json = json.dumps(requirements, ensure_ascii=False, sort_keys=True, default=str)

    # Apply source-level SLA due dates as an operational hint.
    try:
        sources = list_sources_for_market(
            db,
            org_id=getattr(profile, "org_id", None),
            state=_norm_state(getattr(profile, "state", None)),
            county=_norm_lower(getattr(profile, "county", None)),
            city=_norm_lower(getattr(profile, "city", None)),
            pha_name=_norm_text(getattr(profile, "pha_name", None)),
        )
        for source in sources:
            if getattr(source, "next_refresh_due_at", None) is None:
                source.next_refresh_due_at = _chunk8_source_due_at(source)
            if _chunk8_source_is_past_sla(source) and getattr(source, "refresh_state", None) == "healthy":
                source.refresh_state = "degraded"
                source.refresh_status_reason = "source_past_sla"
            db.add(source)
    except Exception:
        pass

    if bool(lockout.get("lockout_active")) and bool(getattr(_chunk8_settings, "jurisdiction_critical_stale_lockout_enabled", True)):
        profile.refresh_state = "blocked"
        profile.refresh_status_reason = str(lockout.get("lockout_reason") or "critical_authoritative_data_stale")
        profile.refresh_blocked_reason = str(lockout.get("lockout_reason") or "critical_authoritative_data_stale")
        db.add(profile)
        db.commit()
        _chunk8_notify_if_profile_locked(
            db,
            profile=profile,
            categories=list(lockout.get("critical_stale_categories") or []),
        )
    else:
        db.add(profile)
        db.commit()

    result["health"] = _chunk8_get_jurisdiction_health(db, profile_id=int(profile.id))
    result["lockout"] = lockout
    return result


def refresh_due_jurisdictions(
    db: Session,
    *,
    org_id: int | None = None,
    reviewer_user_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    result = _chunk8_original_refresh_due_jurisdictions(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        batch_size=batch_size,
        stale_days=stale_days,
        focus=focus,
    )
    if not result.get("ok"):
        return result

    health_items = []
    for item in result.get("refreshed", []):
        profile_id = item.get("jurisdiction_profile_id")
        if profile_id is None:
            continue
        try:
            health_items.append(_chunk8_get_jurisdiction_health(db, profile_id=int(profile_id)))
        except Exception:
            continue

    result["health_items"] = health_items
    result["blocked_count"] = sum(1 for h in health_items if bool((h.get("lockout") or {}).get("lockout_active")))
    result["degraded_count"] = sum(1 for h in health_items if h.get("refresh_state") == "degraded")
    return result



from app.products.compliance.services.policy_coverage.health_service import get_jurisdiction_health as _chunk3_get_jurisdiction_health
from app.products.compliance.services.policy_coverage.lockout_service import profile_lockout_payload as _chunk3_profile_lockout_payload
from app.products.compliance.services.policy_coverage.sla_service import (
    build_refresh_requirements as _chunk3_build_refresh_requirements,
    collect_profile_source_sla_summary as _chunk3_collect_profile_source_sla_summary,
    source_due_at as _chunk3_source_due_at,
    source_is_past_sla as _chunk3_source_is_past_sla,
)

_chunk3_original_refresh_jurisdiction_profile = refresh_jurisdiction_profile
_chunk3_original_refresh_due_jurisdictions = refresh_due_jurisdictions


def _chunk3_refresh_rows(refresh_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in refresh_results or []:
        if isinstance(row, dict):
            rows.append({"refresh": dict(row)})
    return rows


def finalize_jurisdiction_profile_lifecycle(
    db: Session,
    *,
    profile: JurisdictionProfile,
    refresh_results: list[dict[str, Any]] | None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    run_id: str | None = None,
    discovery_result: dict[str, Any] | None = None,
    governance_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    refreshed_profile, coverage = recompute_profile_and_coverage(
        db,
        profile,
        stale_days=stale_days,
        commit=True,
    )
    completeness = profile_completeness_payload(db, refreshed_profile, stale_days=stale_days)
    sla_summary = _chunk3_collect_profile_source_sla_summary(db, profile=refreshed_profile)
    refresh_rows = _chunk3_refresh_rows(refresh_results or [])
    state_payload = determine_profile_refresh_state(
        refresh_runs=refresh_rows,
        recompute_ok=True,
        missing_categories=list(completeness.get("missing_categories") or []),
        stale_categories=list(completeness.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        profile_is_stale=bool(completeness.get("is_stale", False)),
    )

    inventory_summary = inventory_summary_for_market(
        db,
        org_id=getattr(refreshed_profile, "org_id", None),
        state=_norm_state(getattr(refreshed_profile, "state", None)),
        county=_norm_lower(getattr(refreshed_profile, "county", None)),
        city=_norm_lower(getattr(refreshed_profile, "city", None)),
        pha_name=_norm_text(getattr(refreshed_profile, "pha_name", None)),
        program_type="section8" if "section8" in set(completeness.get("missing_categories") or []) else None,
    )
    retry_due_at = None
    if state_payload["refresh_state"] in {"pending", "degraded", "validating", "failed"}:
        retry_due_at = compute_next_retry_due(
            retry_count=int(getattr(refreshed_profile, "refresh_retry_count", 0) or 0),
            base_dt=_utcnow(),
            min_hours=24,
            max_days=14,
        )

    requirements = _chunk3_build_refresh_requirements(
        refreshed_profile,
        next_step=state_payload["next_step"],
        missing_categories=list(completeness.get("missing_categories") or []),
        stale_categories=list(completeness.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        legal_overdue_categories=list(sla_summary.get("legal_overdue_categories") or []),
        informational_overdue_categories=list(sla_summary.get("informational_overdue_categories") or []),
        stale_authoritative_categories=list(sla_summary.get("stale_authoritative_categories") or []),
        inventory_summary={
            **dict(sla_summary),
            "inventory_summary": {
                "inventory_count": inventory_summary.get("inventory_count"),
                "lifecycle_counts": inventory_summary.get("lifecycle_counts"),
                "crawl_counts": inventory_summary.get("crawl_counts"),
            },
        },
        retry_due_at=retry_due_at,
    )

    _set_profile_refresh_state(
        refreshed_profile,
        refresh_state=state_payload["refresh_state"],
        reason=state_payload["status_reason"],
        now=_utcnow(),
        blocked_reason="critical_authoritative_data_stale" if state_payload["refresh_state"] == "blocked" else None,
        next_step=state_payload["next_step"],
        requirements=requirements,
        retry_count=(0 if state_payload["refresh_state"] == "healthy" else int(getattr(refreshed_profile, "refresh_retry_count", 0) or 0) + 1),
        run_id=run_id,
    )

    lockout = _chunk3_profile_lockout_payload(refreshed_profile, completeness)
    if bool(lockout.get("lockout_active")):
        _set_profile_refresh_state(
            refreshed_profile,
            refresh_state="blocked",
            reason=str(lockout.get("lockout_reason") or "critical_authoritative_data_stale"),
            now=_utcnow(),
            blocked_reason=str(lockout.get("lockout_reason") or "critical_authoritative_data_stale"),
            next_step="manual_review",
            requirements=requirements,
            retry_count=int(getattr(refreshed_profile, "refresh_retry_count", 0) or 0),
            run_id=run_id,
        )

    refreshed_profile.last_refresh_completed_at = _utcnow()
    if getattr(refreshed_profile, "refresh_state", None) in {"healthy", "degraded", "pending", "validating", "blocked"}:
        refreshed_profile.last_refresh_success_at = _utcnow()
    refreshed_profile.last_refresh_outcome_json = json.dumps(
        {
            "ok": True,
            "run_id": run_id,
            "refresh_state": getattr(refreshed_profile, "refresh_state", None),
            "status_reason": getattr(refreshed_profile, "refresh_status_reason", None),
            "completeness_status": completeness.get("completeness_status"),
            "missing_categories": list(completeness.get("missing_categories") or []),
            "stale_categories": list(completeness.get("stale_categories") or []),
            "overdue_categories": list(sla_summary.get("overdue_categories") or []),
            "critical_overdue_categories": list(sla_summary.get("critical_overdue_categories") or []),
            "discovery_result": discovery_result or {},
            "governance_result": governance_result or {},
            "inventory_summary": {
                "inventory_count": inventory_summary.get("inventory_count"),
                "lifecycle_counts": inventory_summary.get("lifecycle_counts"),
                "crawl_counts": inventory_summary.get("crawl_counts"),
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    db.add(refreshed_profile)
    db.commit()

    for source in list_sources_for_market(
        db,
        org_id=getattr(refreshed_profile, "org_id", None),
        state=_norm_state(getattr(refreshed_profile, "state", None)),
        county=_norm_lower(getattr(refreshed_profile, "county", None)),
        city=_norm_lower(getattr(refreshed_profile, "city", None)),
        pha_name=_norm_text(getattr(refreshed_profile, "pha_name", None)),
    ):
        if getattr(source, "next_refresh_due_at", None) is None:
            source.next_refresh_due_at = _chunk3_source_due_at(source)
        if _chunk3_source_is_past_sla(source):
            source.refresh_state = getattr(source, "refresh_state", None) or "degraded"
            source.refresh_status_reason = getattr(source, "refresh_status_reason", None) or "source_past_sla"
        db.add(source)
    db.commit()

    health = _chunk3_get_jurisdiction_health(db, profile_id=int(refreshed_profile.id))
    return {
        "profile": {
            "id": int(refreshed_profile.id),
            "org_id": refreshed_profile.org_id,
            "state": refreshed_profile.state,
            "county": refreshed_profile.county,
            "city": refreshed_profile.city,
            "pha_name": getattr(refreshed_profile, "pha_name", None),
            "completeness_status": refreshed_profile.completeness_status,
            "completeness_score": float(refreshed_profile.completeness_score or 0.0),
            "is_stale": bool(refreshed_profile.is_stale),
            "stale_reason": refreshed_profile.stale_reason,
            "last_refresh_success_at": refreshed_profile.last_refresh_success_at.isoformat() if refreshed_profile.last_refresh_success_at else None,
            "refresh_state": getattr(refreshed_profile, "refresh_state", None),
            "refresh_status_reason": getattr(refreshed_profile, "refresh_status_reason", None),
        },
        "coverage": {
            "id": int(coverage.id),
            "coverage_status": getattr(coverage, "coverage_status", None),
            "production_readiness": getattr(coverage, "production_readiness", None),
            "completeness_status": getattr(coverage, "completeness_status", None),
            "is_stale": bool(getattr(coverage, "is_stale", False)),
        },
        "completeness": completeness,
        "lockout": lockout,
        "health": health,
        "sla_summary": sla_summary,
        "requirements": requirements,
    }


def refresh_jurisdiction_profile(
    db: Session,
    *,
    jurisdiction_profile_id: int,
    reviewer_user_id: int | None = None,
    force: bool = False,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    result = _chunk3_original_refresh_jurisdiction_profile(
        db,
        jurisdiction_profile_id=jurisdiction_profile_id,
        reviewer_user_id=reviewer_user_id,
        force=force,
        stale_days=stale_days,
        focus=focus,
    )
    if not result.get("ok"):
        return result

    profile = db.get(JurisdictionProfile, int(jurisdiction_profile_id))
    if profile is None:
        return result

    finalized = finalize_jurisdiction_profile_lifecycle(
        db,
        profile=profile,
        refresh_results=list(result.get("refresh_results") or []),
        stale_days=stale_days,
        run_id=_loads_json_dict(getattr(profile, "last_refresh_outcome_json", None)).get("run_id"),
        discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
        governance_result=result.get("governance_result") if isinstance(result.get("governance_result"), dict) else None,
    )
    result["profile"] = finalized["profile"]
    result["coverage"] = finalized["coverage"]
    result["lockout"] = finalized["lockout"]
    result["health"] = finalized["health"]
    result["sla_summary"] = finalized["sla_summary"]
    result["requirements"] = finalized["requirements"]
    result["refresh_confidence"] = _refresh_confidence_from_profile(finalized["profile"])
    if isinstance(result.get("summary"), dict):
        result["summary"] = {**result["summary"], "refresh_state": finalized["profile"].get("refresh_state"), "next_step": finalized["requirements"].get("next_step")}
    return result


def refresh_due_jurisdictions(
    db: Session,
    *,
    org_id: int | None = None,
    reviewer_user_id: int | None = None,
    batch_size: int | None = None,
    stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS,
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    result = _chunk3_original_refresh_due_jurisdictions(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        batch_size=batch_size,
        stale_days=stale_days,
        focus=focus,
    )
    if not result.get("ok"):
        return result

    health_items = []
    blocked_count = 0
    degraded_count = 0
    for item in result.get("refreshed", []):
        profile_id = item.get("jurisdiction_profile_id")
        if profile_id is None:
            continue
        health = _chunk3_get_jurisdiction_health(db, profile_id=int(profile_id))
        health_items.append(health)
        if bool((health.get("lockout") or {}).get("lockout_active")):
            blocked_count += 1
        if health.get("refresh_state") == "degraded":
            degraded_count += 1
    result["health_items"] = health_items
    result["blocked_count"] = blocked_count
    result["degraded_count"] = degraded_count
    return result
