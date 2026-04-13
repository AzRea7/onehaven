
from __future__ import annotations

from app.config import settings
from app.policy_models import JurisdictionProfile


def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
    critical_stale = list(completeness.get("critical_stale_categories") or [])
    stale = list(completeness.get("stale_categories") or [])
    is_stale = bool(completeness.get("is_stale"))

    hard_lock = bool(getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)) and (
        bool(critical_stale) or (is_stale and getattr(profile, "refresh_state", None) == "blocked")
    )

    return {
        "lockout_active": hard_lock,
        "lockout_reason": "critical_authoritative_data_stale" if hard_lock else None,
        "critical_stale_categories": critical_stale,
        "stale_categories": stale,
        "refresh_state": getattr(profile, "refresh_state", None),
    }
