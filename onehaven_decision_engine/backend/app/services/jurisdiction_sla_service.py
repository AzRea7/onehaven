
from __future__ import annotations

from datetime import datetime, timedelta

from app.config import settings
from app.policy_models import JurisdictionProfile, PolicySource


def _utcnow() -> datetime:
    return datetime.utcnow()


def source_sla_hours(source: PolicySource) -> int:
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    categories = []
    try:
        import json
        categories = json.loads(getattr(source, "normalized_categories_json", None) or "[]")
        if not isinstance(categories, list):
            categories = []
    except Exception:
        categories = []
    categories = {str(x).strip().lower() for x in categories if str(x).strip()}

    if "section8" in categories or source_type == "program":
        return int(getattr(settings, "jurisdiction_sla_program_overlay_hours", 24 * 14))
    if authority_tier == "authoritative_official":
        if {"inspection", "safety", "registration", "occupancy", "lead"} & categories:
            return int(getattr(settings, "jurisdiction_sla_critical_authoritative_hours", 24 * 14))
        return int(getattr(settings, "jurisdiction_sla_authoritative_hours", 24 * 21))
    return int(getattr(settings, "jurisdiction_sla_default_hours", 24 * 30))


def source_due_at(source: PolicySource) -> datetime:
    base = (
        getattr(source, "last_verified_at", None)
        or getattr(source, "last_fetched_at", None)
        or getattr(source, "retrieved_at", None)
        or _utcnow()
    )
    return base + timedelta(hours=source_sla_hours(source))


def source_is_past_sla(source: PolicySource, *, now: datetime | None = None) -> bool:
    now = now or _utcnow()
    return source_due_at(source) <= now


def profile_next_actions(profile: JurisdictionProfile) -> dict:
    import json
    requirements = {}
    try:
        requirements = json.loads(getattr(profile, "refresh_requirements_json", None) or "{}")
        if not isinstance(requirements, dict):
            requirements = {}
    except Exception:
        requirements = {}
    return {
        "next_step": requirements.get("next_step") or "refresh",
        "next_search_retry_due_at": requirements.get("next_search_retry_due_at"),
        "refresh_state": getattr(profile, "refresh_state", None),
    }
