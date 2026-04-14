from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from app.config import settings
from app.policy_models import JurisdictionProfile, PolicySource


CRITICAL_CATEGORY_SET = {"inspection", "safety", "registration", "occupancy", "lead"}


def _utcnow() -> datetime:
    return datetime.utcnow()


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


def source_categories(source: PolicySource) -> set[str]:
    return {
        str(x).strip().lower()
        for x in _loads_json_list(getattr(source, "normalized_categories_json", None))
        if str(x).strip()
    }


def source_sla_hours(source: PolicySource) -> int:
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    categories = source_categories(source)

    if "section8" in categories or source_type == "program":
        return int(getattr(settings, "jurisdiction_sla_program_overlay_hours", 24 * 14))
    if authority_tier == "authoritative_official":
        if CRITICAL_CATEGORY_SET & categories:
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


def collect_profile_source_sla_summary(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> dict[str, Any]:
    now = _utcnow()
    sources = list(
        db.query(PolicySource)
        .filter(PolicySource.state == getattr(profile, "state", None))
        .all()
    )

    scoped: list[PolicySource] = []
    for source in sources:
        source_org_id = getattr(source, "org_id", None)
        profile_org_id = getattr(profile, "org_id", None)
        if profile_org_id is None and source_org_id is not None:
            continue
        if profile_org_id is not None and source_org_id not in {None, profile_org_id}:
            continue
        if getattr(source, "county", None) is not None and getattr(source, "county", None) != getattr(profile, "county", None):
            continue
        if getattr(source, "city", None) is not None and getattr(source, "city", None) != getattr(profile, "city", None):
            continue
        if getattr(source, "pha_name", None) is not None and getattr(source, "pha_name", None) != getattr(profile, "pha_name", None):
            continue
        scoped.append(source)

    overdue_categories: set[str] = set()
    critical_overdue_categories: set[str] = set()
    due_soon_categories: set[str] = set()
    sources_payload: list[dict[str, Any]] = []

    for source in scoped:
        categories = source_categories(source)
        due_at = source_due_at(source)
        is_overdue = due_at <= now
        is_due_soon = not is_overdue and due_at <= (now + timedelta(hours=24))
        if is_overdue:
            overdue_categories.update(categories)
            if CRITICAL_CATEGORY_SET & categories:
                critical_overdue_categories.update(CRITICAL_CATEGORY_SET & categories)
        elif is_due_soon:
            due_soon_categories.update(categories)
        sources_payload.append(
            {
                "source_id": int(getattr(source, "id", 0) or 0),
                "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
                "authority_tier": getattr(source, "authority_tier", None),
                "categories": sorted(categories),
                "due_at": due_at.isoformat() if due_at else None,
                "is_overdue": is_overdue,
                "is_due_soon": is_due_soon,
                "refresh_state": getattr(source, "refresh_state", None),
                "freshness_status": getattr(source, "freshness_status", None),
            }
        )

    next_due_at = None
    if sources_payload:
        due_values = [item["due_at"] for item in sources_payload if item.get("due_at")]
        next_due_at = min(due_values) if due_values else None

    return {
        "source_count": len(scoped),
        "sources": sources_payload,
        "overdue_categories": sorted(overdue_categories),
        "critical_overdue_categories": sorted(critical_overdue_categories),
        "due_soon_categories": sorted(due_soon_categories),
        "has_overdue_sources": bool(overdue_categories),
        "has_critical_overdue_sources": bool(critical_overdue_categories),
        "next_due_at": next_due_at,
    }


def build_refresh_requirements(
    profile: JurisdictionProfile,
    *,
    next_step: str,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    overdue_categories: list[str] | None = None,
    critical_overdue_categories: list[str] | None = None,
    inventory_summary: dict[str, Any] | None = None,
    retry_due_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "next_step": next_step,
        "refresh_state": getattr(profile, "refresh_state", None),
        "missing_categories": list(missing_categories or []),
        "stale_categories": list(stale_categories or []),
        "overdue_categories": list(overdue_categories or []),
        "critical_overdue_categories": list(critical_overdue_categories or []),
        "inventory_summary": dict(inventory_summary or {}),
        "next_search_retry_due_at": retry_due_at.isoformat() if retry_due_at else None,
        "last_refresh_completed_at": getattr(profile, "last_refresh_completed_at", None).isoformat() if getattr(profile, "last_refresh_completed_at", None) else None,
    }


def profile_next_actions(profile: JurisdictionProfile) -> dict[str, Any]:
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
        "missing_categories": list(requirements.get("missing_categories") or []),
        "stale_categories": list(requirements.get("stale_categories") or []),
        "overdue_categories": list(requirements.get("overdue_categories") or []),
        "critical_overdue_categories": list(requirements.get("critical_overdue_categories") or []),
        "refresh_state": getattr(profile, "refresh_state", None),
    }
