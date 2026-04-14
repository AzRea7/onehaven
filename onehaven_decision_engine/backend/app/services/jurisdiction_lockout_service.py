from __future__ import annotations

from app.config import settings
from app.policy_models import JurisdictionProfile


def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
    critical_stale = list(completeness.get("critical_stale_categories") or [])
    stale = list(completeness.get("stale_categories") or [])
    missing = list(completeness.get("missing_categories") or [])
    is_stale = bool(completeness.get("is_stale"))
    refresh_state = getattr(profile, "refresh_state", None)

    hard_lock = bool(getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)) and (
        bool(critical_stale)
        or (is_stale and refresh_state == "blocked")
        or (refresh_state == "blocked" and bool(stale))
    )

    return {
        "lockout_active": hard_lock,
        "lockout_reason": "critical_authoritative_data_stale" if hard_lock else None,
        "critical_stale_categories": critical_stale,
        "stale_categories": stale,
        "missing_categories": missing,
        "refresh_state": refresh_state,
        "blocking_state": refresh_state in {"blocked", "failed"},
    }
