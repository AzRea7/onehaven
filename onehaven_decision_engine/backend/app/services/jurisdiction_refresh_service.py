from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import recompute_profile_and_coverage
from app.services.jurisdiction_notification_service import (
    build_jurisdiction_profile_stale_notification,
    build_review_queue_payload,
    build_rule_change_notification,
    build_source_refresh_notification,
    build_stale_source_notification,
    notify_if_jurisdiction_stale,
    record_notification_event,
)
from app.services.policy_extractor_service import extract_assertions_for_source, mark_assertions_stale_for_source
from app.services.policy_review_service import apply_governance_lifecycle, diff_active_rules_for_source
from app.services.policy_source_service import (
    collect_catalog_for_market,
    fetch_policy_source,
    list_sources_for_market,
    policy_source_needs_refresh,
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


def _stale_cutoff(stale_days: int = DEFAULT_JURISDICTION_STALE_DAYS) -> datetime:
    return _utcnow() - timedelta(days=int(stale_days))


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

    profile.last_refresh_started_at = _utcnow()
    profile.last_refresh_error = None
    db.add(profile)
    db.commit()

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

    refresh_results: list[dict[str, Any]] = []
    extraction_results: list[dict[str, Any]] = []
    diff_results: list[dict[str, Any]] = []
    notification_results: list[dict[str, Any]] = []

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
                }
            )
            continue

        fetch_result = fetch_policy_source(
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
        notification_results.append(
            record_notification_event(
                db,
                payload=build_source_refresh_notification(source=source, refresh_result=fetch_result),
            )
        )

        if not fetch_result.get("ok"):
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
                    "assertion_ids": [],
                    "stale_update": stale_update,
                }
            )
            notification_results.append(
                record_notification_event(
                    db,
                    payload=build_stale_source_notification(source=source),
                )
            )
            continue

        created_rows: list[PolicyAssertion] = []
        stale_update: dict[str, Any] | None = None

        if bool(fetch_result.get("changed")) or force:
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
                "changed": bool(fetch_result.get("changed")),
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

    governance_result = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )

    if (
        governance_result.get("active_count", 0)
        or governance_result.get("replaced_count", 0)
        or total_changed_rules
        or total_new_rules
        or total_missing_rules
    ):
        for source in sources:
            notification_results.append(
                record_notification_event(
                    db,
                    payload=build_rule_change_notification(source=source, governance_result=governance_result),
                )
            )

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
        "sources_total": len(sources),
        "refresh_results": refresh_results,
        "extraction_results": extraction_results,
        "diff_results": diff_results,
        "governance_result": governance_result,
        "review_queue": review_queue,
        "notification_results": notification_results,
        "stale_profile_notification": stale_profile_notice,
        "summary": {
            "changed_rule_count": total_changed_rules,
            "new_rule_count": total_new_rules,
            "missing_rule_count": total_missing_rules,
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