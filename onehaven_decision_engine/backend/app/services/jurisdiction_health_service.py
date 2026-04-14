from __future__ import annotations

import json
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile
from app.services.jurisdiction_completeness_service import profile_completeness_payload
from app.services.jurisdiction_lockout_service import profile_lockout_payload
from app.services.jurisdiction_sla_service import collect_profile_source_sla_summary, profile_next_actions


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
        return {"ok": False, "error": "jurisdiction_profile_not_found"}

    completeness = profile_completeness_payload(db, profile)
    lockout = profile_lockout_payload(profile, completeness)
    next_actions = profile_next_actions(profile)
    sla_summary = collect_profile_source_sla_summary(db, profile=profile)
    refresh_outcome = _loads_json_dict(getattr(profile, "last_refresh_outcome_json", None))

    return {
        "ok": True,
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "completeness": completeness,
        "lockout": lockout,
        "next_actions": next_actions,
        "sla_summary": sla_summary,
        "refresh_state": getattr(profile, "refresh_state", None),
        "refresh_status_reason": getattr(profile, "refresh_status_reason", None),
        "last_refresh_success_at": getattr(profile, "last_refresh_success_at", None).isoformat() if getattr(profile, "last_refresh_success_at", None) else None,
        "last_refresh_completed_at": getattr(profile, "last_refresh_completed_at", None).isoformat() if getattr(profile, "last_refresh_completed_at", None) else None,
        "refresh_retry_count": int(getattr(profile, "refresh_retry_count", 0) or 0),
        "refresh_outcome": refresh_outcome,
    }
