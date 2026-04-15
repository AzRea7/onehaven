from __future__ import annotations

from app.config import settings
from app.policy_models import JurisdictionProfile


def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
    critical_stale = list(completeness.get("critical_stale_categories") or [])
    stale = list(completeness.get("stale_categories") or [])
    missing = list(completeness.get("missing_categories") or [])
    authority_unmet = list(completeness.get("authority_unmet_categories") or [])
    supporting_only = list(completeness.get("supporting_only_categories") or [])
    weak_support = list(completeness.get("weak_support_categories") or [])
    is_stale = bool(completeness.get("is_stale"))
    refresh_state = getattr(profile, "refresh_state", None)

    hard_lock = bool(getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)) and (
        bool(critical_stale)
        or bool(authority_unmet)
        or bool(supporting_only)
        or (is_stale and refresh_state == "blocked")
        or (refresh_state == "blocked" and bool(stale))
    )

    lockout_reason = None
    if hard_lock:
        if authority_unmet:
            lockout_reason = "critical_categories_missing_binding_authority"
        elif supporting_only:
            lockout_reason = "critical_categories_supported_only_by_nonbinding_sources"
        else:
            lockout_reason = "critical_authoritative_data_stale"

    return {
        "lockout_active": hard_lock,
        "lockout_reason": lockout_reason,
        "critical_stale_categories": critical_stale,
        "stale_categories": stale,
        "missing_categories": missing,
        "authority_unmet_categories": authority_unmet,
        "supporting_only_categories": supporting_only,
        "weak_support_categories": weak_support,
        "refresh_state": refresh_state,
        "blocking_state": refresh_state in {"blocked", "failed"} or hard_lock,
    }
