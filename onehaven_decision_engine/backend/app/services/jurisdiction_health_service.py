from __future__ import annotations

import json
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile
from app.services.jurisdiction_completeness_service import profile_completeness_payload
from app.services.jurisdiction_lockout_service import profile_lockout_payload
from app.services.jurisdiction_sla_service import collect_profile_source_sla_summary, profile_next_actions
from app.services.policy_review_service import summarize_policy_overrides


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = str(item or '').strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _health_status(*, lockout: dict[str, Any], completeness: dict[str, Any], sla_summary: dict[str, Any], override_summary: dict[str, Any] | None = None) -> str:
    override_summary = override_summary or {}
    if bool(lockout.get('lockout_active')):
        return 'blocked'
    if list(sla_summary.get('legal_lockout_categories') or []) or list(sla_summary.get('critical_fetch_failure_categories') or []):
        return 'blocked'
    if bool(override_summary.get('carrying_critical_override')):
        return 'degraded'
    if list(completeness.get('critical_stale_categories') or []) or list(sla_summary.get('legal_overdue_categories') or []) or int(sla_summary.get('failed_binding_source_count') or 0) > 0:
        return 'degraded'
    if bool(completeness.get('is_stale')) or bool(sla_summary.get('has_overdue_sources')) or bool(override_summary.get('review_required')) or int(sla_summary.get('rejected_source_count') or 0) > 0:
        return 'warning'
    return 'ok'


def _operational_reason(completeness: dict[str, Any], lockout: dict[str, Any], sla_summary: dict[str, Any], override_summary: dict[str, Any]) -> str | None:
    if bool(lockout.get('lockout_active')):
        return str(lockout.get('lockout_reason') or 'jurisdiction_lockout_active')
    if list(sla_summary.get('legal_lockout_categories') or []):
        return 'legal_source_failure_lockout'
    if list(sla_summary.get('critical_fetch_failure_categories') or []):
        return 'critical_binding_sources_fetch_failed'
    if list(lockout.get('lockout_causing_categories') or []):
        return 'critical_categories_block_safe_reliance'
    if list(lockout.get('validation_pending_categories') or []):
        return 'validation_pending_for_required_categories'
    if list(lockout.get('authority_gap_categories') or []):
        return 'required_authority_gaps_present'
    if bool(override_summary.get('carrying_critical_override')):
        return 'critical_override_requires_review'
    if list(sla_summary.get('legal_overdue_categories') or []):
        return 'legal_freshness_overdue'
    if bool(completeness.get('is_stale')):
        return str(completeness.get('stale_reason') or 'informational_staleness')
    return None


def get_jurisdiction_health(
    db: Session,
    *,
    profile_id: int | None = None,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
) -> dict[str, Any]:
    profile = None
    if profile_id is not None:
        profile = db.get(JurisdictionProfile, int(profile_id))
    else:
        stmt = select(JurisdictionProfile)
        if state:
            stmt = stmt.where(JurisdictionProfile.state == str(state).strip().upper())
        if county is not None:
            stmt = stmt.where(JurisdictionProfile.county == (county.strip().lower() or None))
        if city is not None:
            stmt = stmt.where(JurisdictionProfile.city == (city.strip().lower() or None))
        if pha_name is not None:
            stmt = stmt.where(JurisdictionProfile.pha_name == (pha_name.strip() or None))
        if org_id is None:
            stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
        else:
            stmt = stmt.where(or_(JurisdictionProfile.org_id == int(org_id), JurisdictionProfile.org_id.is_(None)))
        profile = db.scalars(stmt.order_by(JurisdictionProfile.org_id.desc().nulls_last(), JurisdictionProfile.id.desc())).first()

    if profile is None:
        return {'ok': False, 'error': 'jurisdiction_profile_not_found'}

    completeness = profile_completeness_payload(db, profile)
    sla_summary = collect_profile_source_sla_summary(db, profile=profile)
    lockout = profile_lockout_payload(profile, completeness)
    next_actions = profile_next_actions(profile)
    refresh_outcome = _loads_json_dict(getattr(profile, 'last_refresh_outcome_json', None))
    override_summary = summarize_policy_overrides(
        db,
        org_id=getattr(profile, 'org_id', None),
        state=getattr(profile, 'state', None),
        county=getattr(profile, 'county', None),
        city=getattr(profile, 'city', None),
        pha_name=getattr(profile, 'pha_name', None),
        jurisdiction_profile_id=int(profile.id),
    )
    if bool(override_summary.get('carrying_critical_override')):
        lockout['override_review_required'] = True
    lockout_causing_categories = _dedupe(list(lockout.get('lockout_causing_categories') or []) + list(sla_summary.get('legal_lockout_categories') or []) + list(sla_summary.get('critical_fetch_failure_categories') or []))
    health_status = _health_status(lockout=lockout, completeness=completeness, sla_summary=sla_summary, override_summary=override_summary)
    validation_pending_categories = _dedupe(list(lockout.get('validation_pending_categories') or []) + list(completeness.get('validation_pending_categories') or []) + list(sla_summary.get('review_required_categories') or []))
    authority_gap_categories = _dedupe(list(lockout.get('authority_gap_categories') or []) + list(completeness.get('authority_unmet_categories') or []))
    informational_gap_categories = _dedupe(list(lockout.get('informational_gap_categories') or []) + list(completeness.get('inferred_categories') or []))
    operational_reason = _operational_reason(completeness, lockout, sla_summary, override_summary)

    return {
        'ok': True,
        'jurisdiction_profile_id': int(profile.id),
        'org_id': getattr(profile, 'org_id', None),
        'state': getattr(profile, 'state', None),
        'county': getattr(profile, 'county', None),
        'city': getattr(profile, 'city', None),
        'pha_name': getattr(profile, 'pha_name', None),
        'health_status': health_status,
        'operational_state': health_status,
        'operational_reason': operational_reason,
        'safe_for_user_reliance': bool(completeness.get('safe_for_user_reliance')) and not bool(override_summary.get('carrying_critical_override')) and not bool(lockout.get('lockout_active')),
        'safe_for_projection': bool(completeness.get('safe_for_projection')) and not bool(lockout.get('lockout_active')),
        'safe_to_rely_on': bool(completeness.get('safe_for_user_reliance')) and bool(sla_summary.get('safe_to_rely_on', True)) and not bool(override_summary.get('carrying_critical_override')) and not bool(lockout.get('lockout_active')) and not bool(validation_pending_categories) and not bool(authority_gap_categories) and not bool(lockout_causing_categories),
        'completeness': completeness,
        'lockout': lockout,
        'next_actions': next_actions,
        'sla_summary': sla_summary,
        'critical_stale_categories': list(completeness.get('critical_stale_categories') or []),
        'lockout_causing_categories': lockout_causing_categories,
        'critical_fetch_failure_categories': list(sla_summary.get('critical_fetch_failure_categories') or []),
        'legal_lockout_categories': list(sla_summary.get('legal_lockout_categories') or []),
        'rejected_source_count': int(sla_summary.get('rejected_source_count') or 0),
        'guessed_source_count': int(sla_summary.get('guessed_source_count') or 0),
        'failed_binding_source_count': int(sla_summary.get('failed_binding_source_count') or 0),
        'informational_gap_categories': informational_gap_categories,
        'validation_pending_categories': validation_pending_categories,
        'authority_gap_categories': authority_gap_categories,
        'legal_stale_categories': list(completeness.get('legal_stale_categories') or sla_summary.get('legal_overdue_categories') or []),
        'informational_stale_categories': list(completeness.get('informational_stale_categories') or sla_summary.get('informational_overdue_categories') or []),
        'stale_authoritative_categories': list(completeness.get('stale_authoritative_categories') or sla_summary.get('stale_authoritative_categories') or []),
        'override_summary': override_summary,
        'review_required': bool(override_summary.get('review_required')) or bool(override_summary.get('carrying_critical_override')) or not bool(completeness.get('safe_for_user_reliance')) or bool(sla_summary.get('review_required_categories')) or int(sla_summary.get('rejected_source_count') or 0) > 0,
        'refresh_state': getattr(profile, 'refresh_state', None),
        'last_validation_at': (sla_summary.get('latest_validated_at') if isinstance(sla_summary, dict) else None),
        'next_due_step': (next_actions.get('next_step') if isinstance(next_actions, dict) else None),
        'next_due_at': ((next_actions.get('next_search_retry_due_at') if isinstance(next_actions, dict) else None) or (sla_summary.get('next_validation_due_at') if isinstance(sla_summary, dict) else None) or (sla_summary.get('next_refresh_due_at') if isinstance(sla_summary, dict) else None)),
        'refresh_status_reason': getattr(profile, 'refresh_status_reason', None),
        'last_refresh_success_at': getattr(profile, 'last_refresh_success_at', None).isoformat() if getattr(profile, 'last_refresh_success_at', None) else None,
        'last_refresh_completed_at': getattr(profile, 'last_refresh_completed_at', None).isoformat() if getattr(profile, 'last_refresh_completed_at', None) else None,
        'refresh_retry_count': int(getattr(profile, 'refresh_retry_count', 0) or 0),
        'refresh_outcome': refresh_outcome,
    }


def _due_now_reasons(health: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if list(health.get('lockout_causing_categories') or []):
        reasons.append('blocking legal categories need manual action')
    if list(health.get('stale_authoritative_categories') or []):
        reasons.append('authoritative categories are stale')
    if list(health.get('validation_pending_categories') or []):
        reasons.append('validation is still pending')
    conflicting = list(((health.get('completeness') or {}).get('conflicting_categories') or []))
    if conflicting:
        reasons.append('conflicts need reviewer resolution')
    if list(health.get('authority_gap_categories') or []):
        reasons.append('binding authority gaps remain')
    if not reasons and bool(health.get('review_required')):
        reasons.append('manual review is still required')
    if not reasons and bool((health.get('completeness') or {}).get('missing_categories')):
        reasons.append('jurisdiction coverage is incomplete')
    return _dedupe(reasons)


def _dashboard_tags(health: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    health_state = str(health.get('health_status') or health.get('operational_state') or '').strip().lower()
    if health_state in {'blocked', 'degraded', 'warning', 'ok'}:
        tags.append(health_state)
    if bool(health.get('review_required')):
        tags.append('review-required')
    if bool(health.get('stale_authoritative_categories')) or bool(health.get('legal_stale_categories')) or bool(health.get('informational_stale_categories')):
        tags.append('stale')
    if bool(((health.get('completeness') or {}).get('missing_categories') or [])) or bool(health.get('authority_gap_categories')) or bool(health.get('lockout_causing_categories')):
        tags.append('missing-proof')
    return _dedupe(tags)


def get_manual_stale_review_dashboard(
    db: Session,
    *,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    status_filter: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    stmt = select(JurisdictionProfile)
    if state:
        stmt = stmt.where(JurisdictionProfile.state == str(state).strip().upper())
    if county is not None:
        stmt = stmt.where(JurisdictionProfile.county == (county.strip().lower() or None))
    if city is not None:
        stmt = stmt.where(JurisdictionProfile.city == (city.strip().lower() or None))
    if pha_name is not None:
        stmt = stmt.where(JurisdictionProfile.pha_name == (pha_name.strip() or None))
    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionProfile.org_id == int(org_id), JurisdictionProfile.org_id.is_(None)))

    rows = list(db.scalars(stmt.order_by(JurisdictionProfile.id.desc())).all())
    items: list[dict[str, Any]] = []
    counts = {
        'blocked': 0,
        'degraded': 0,
        'warning': 0,
        'ok': 0,
        'review_required': 0,
        'stale': 0,
        'missing_proof': 0,
    }

    wanted = str(status_filter or 'all').strip().lower()
    for profile in rows:
        health = get_jurisdiction_health(db, profile_id=int(profile.id), org_id=org_id)
        if not health.get('ok'):
            continue
        tags = _dashboard_tags(health)
        state_key = str(health.get('health_status') or 'ok').strip().lower()
        if state_key in counts:
            counts[state_key] += 1
        if 'review-required' in tags:
            counts['review_required'] += 1
        if 'stale' in tags:
            counts['stale'] += 1
        if 'missing-proof' in tags:
            counts['missing_proof'] += 1

        if wanted not in {'', 'all'} and wanted not in tags:
            continue

        completeness = health.get('completeness') or {}
        due_now_reasons = _due_now_reasons(health)
        items.append({
            'jurisdiction_profile_id': int(profile.id),
            'state': getattr(profile, 'state', None),
            'county': getattr(profile, 'county', None),
            'city': getattr(profile, 'city', None),
            'pha_name': getattr(profile, 'pha_name', None),
            'health_status': health.get('health_status'),
            'operational_state': health.get('operational_state'),
            'operational_reason': health.get('operational_reason'),
            'review_required': bool(health.get('review_required')),
            'safe_to_rely_on': bool(health.get('safe_to_rely_on')),
            'tags': tags,
            'why_due_now': due_now_reasons,
            'what_to_do_next': health.get('next_due_step') or ((health.get('next_actions') or {}).get('next_step')) or 'review_jurisdiction_state',
            'next_due_at': health.get('next_due_at'),
            'last_refresh_success_at': health.get('last_refresh_success_at'),
            'last_validation_at': health.get('last_validation_at'),
            'last_refresh_completed_at': health.get('last_refresh_completed_at'),
            'refresh_state': health.get('refresh_state'),
            'refresh_status_reason': health.get('refresh_status_reason'),
            'lockout_causing_categories': list(health.get('lockout_causing_categories') or []),
            'stale_authoritative_categories': list(health.get('stale_authoritative_categories') or []),
            'validation_pending_categories': list(health.get('validation_pending_categories') or []),
            'authority_gap_categories': list(health.get('authority_gap_categories') or []),
            'informational_gap_categories': list(health.get('informational_gap_categories') or []),
            'conflicting_categories': list(completeness.get('conflicting_categories') or []),
            'missing_categories': list(completeness.get('missing_categories') or []),
            'completeness_status': completeness.get('completeness_status'),
            'completeness_score': completeness.get('completeness_score'),
        })

    severity_rank = {'blocked': 0, 'degraded': 1, 'warning': 2, 'ok': 3}
    items.sort(key=lambda row: (
        severity_rank.get(str(row.get('health_status') or 'ok').lower(), 9),
        0 if row.get('review_required') else 1,
        0 if row.get('next_due_at') else 1,
        str(row.get('next_due_at') or ''),
        -(int(row.get('jurisdiction_profile_id') or 0)),
    ))

    return {
        'ok': True,
        'filter': wanted or 'all',
        'count': len(items[:limit]),
        'summary': counts,
        'rows': items[:limit],
    }
