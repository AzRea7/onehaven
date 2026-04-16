from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from app.config import settings
from app.policy_models import JurisdictionProfile, PolicySource


LEGAL_BLOCKING_CATEGORIES = {"registration", "inspection", "occupancy", "lead", "section8", "program_overlay", "safety"}
CRITICAL_CATEGORY_SET = set(LEGAL_BLOCKING_CATEGORIES)
PROGRAM_CATEGORIES = {"section8", "program_overlay", "subsidy_overlay"}


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


def _source_use_type(source: PolicySource) -> str:
    value = str(getattr(source, "authority_use_type", None) or "").strip().lower()
    if value:
        return value
    authority_tier = str(getattr(source, "authority_tier", None) or "").strip().lower()
    authority_rank = int(getattr(source, "authority_rank", 0) or 0)
    if bool(getattr(source, "is_authoritative", False)) or authority_tier == "authoritative_official" or authority_rank >= 100:
        return "binding"
    if authority_tier in {"approved_official_supporting", "semi_authoritative_operational"} or authority_rank >= 60:
        return "supporting"
    return "weak"


def _category_sla_hours(*, category: str, source_type: str, authority_tier: str, use_type: str) -> int:
    category = str(category or "").strip().lower()
    source_type = str(source_type or "").strip().lower()
    authority_tier = str(authority_tier or "").strip().lower()
    use_type = str(use_type or "").strip().lower()

    if category in PROGRAM_CATEGORIES or source_type == "program":
        return int(getattr(settings, "jurisdiction_sla_program_overlay_hours", 24 * 14))
    if category in LEGAL_BLOCKING_CATEGORIES:
        if use_type == "binding" or authority_tier == "authoritative_official":
            return int(getattr(settings, "jurisdiction_sla_critical_authoritative_hours", 24 * 14))
        if use_type == "supporting":
            return int(getattr(settings, "jurisdiction_sla_supporting_critical_hours", 24 * 10))
        return int(getattr(settings, "jurisdiction_sla_default_hours", 24 * 30))
    if use_type == "binding" or authority_tier == "authoritative_official":
        return int(getattr(settings, "jurisdiction_sla_authoritative_hours", 24 * 21))
    if use_type == "supporting":
        return int(getattr(settings, "jurisdiction_sla_supporting_hours", 24 * 30))
    return int(getattr(settings, "jurisdiction_sla_default_hours", 24 * 30))


def source_sla_hours(source: PolicySource) -> int:
    categories = sorted(source_categories(source))
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    use_type = _source_use_type(source)
    if not categories:
        return _category_sla_hours(category="", source_type=source_type, authority_tier=authority_tier, use_type=use_type)
    return min(_category_sla_hours(category=c, source_type=source_type, authority_tier=authority_tier, use_type=use_type) for c in categories)


def _category_due_at(source: PolicySource, category: str) -> datetime:
    base = (
        getattr(source, "last_verified_at", None)
        or getattr(source, "freshness_checked_at", None)
        or getattr(source, "last_fetched_at", None)
        or getattr(source, "retrieved_at", None)
        or _utcnow()
    )
    return base + timedelta(hours=_category_sla_hours(
        category=category,
        source_type=str(getattr(source, "source_type", "") or ""),
        authority_tier=str(getattr(source, "authority_tier", "") or ""),
        use_type=_source_use_type(source),
    ))


def source_due_at(source: PolicySource) -> datetime:
    categories = sorted(source_categories(source))
    if not categories:
        return _category_due_at(source, "")
    return min(_category_due_at(source, category) for category in categories)


def source_is_past_sla(source: PolicySource, *, now: datetime | None = None) -> bool:
    now = now or _utcnow()
    return source_due_at(source) <= now


def _iter_scoped_sources(db: Session, *, profile: JurisdictionProfile) -> list[PolicySource]:
    rows = list(db.query(PolicySource).filter(PolicySource.state == getattr(profile, "state", None)).all())
    scoped: list[PolicySource] = []
    for source in rows:
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
    return scoped


def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
    now = _utcnow()
    scoped = _iter_scoped_sources(db, profile=profile)

    overdue_categories: set[str] = set()
    critical_overdue_categories: set[str] = set()
    legal_overdue_categories: set[str] = set()
    informational_overdue_categories: set[str] = set()
    stale_authoritative_categories: set[str] = set()
    due_soon_categories: set[str] = set()
    category_rollup: dict[str, dict[str, Any]] = {}
    sources_payload: list[dict[str, Any]] = []

    for source in scoped:
        categories = sorted(source_categories(source))
        authority_tier = getattr(source, "authority_tier", None)
        use_type = _source_use_type(source)
        source_due = source_due_at(source)
        source_overdue = source_due <= now
        source_due_soon = (not source_overdue) and source_due <= (now + timedelta(hours=24))
        per_category: list[dict[str, Any]] = []

        for category in categories or [""]:
            due_at = _category_due_at(source, category)
            is_overdue = due_at <= now
            is_due_soon = (not is_overdue) and due_at <= (now + timedelta(hours=24))
            is_legal = category in LEGAL_BLOCKING_CATEGORIES
            entry = category_rollup.setdefault(category, {
                "category": category,
                "is_legal_lockout_category": is_legal,
                "source_ids": [],
                "binding_source_ids": [],
                "overdue_source_ids": [],
                "authoritative_overdue_source_ids": [],
                "next_due_at": None,
            })
            entry["source_ids"].append(int(getattr(source, "id", 0) or 0))
            if use_type == "binding":
                entry["binding_source_ids"].append(int(getattr(source, "id", 0) or 0))
            if is_overdue:
                overdue_categories.add(category)
                entry["overdue_source_ids"].append(int(getattr(source, "id", 0) or 0))
                if use_type == "binding":
                    stale_authoritative_categories.add(category)
                    entry["authoritative_overdue_source_ids"].append(int(getattr(source, "id", 0) or 0))
                if is_legal:
                    legal_overdue_categories.add(category)
                    critical_overdue_categories.add(category)
                else:
                    informational_overdue_categories.add(category)
            elif is_due_soon:
                due_soon_categories.add(category)
            if entry["next_due_at"] is None or (due_at and due_at.isoformat() < entry["next_due_at"]):
                entry["next_due_at"] = due_at.isoformat() if due_at else None
            per_category.append({
                "category": category,
                "due_at": due_at.isoformat() if due_at else None,
                "is_overdue": is_overdue,
                "is_due_soon": is_due_soon,
                "is_legal_lockout_category": is_legal,
                "authority_use_type": use_type,
            })

        sources_payload.append({
            "source_id": int(getattr(source, "id", 0) or 0),
            "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
            "authority_tier": authority_tier,
            "authority_use_type": use_type,
            "categories": categories,
            "due_at": source_due.isoformat() if source_due else None,
            "is_overdue": source_overdue,
            "is_due_soon": source_due_soon,
            "refresh_state": getattr(source, "refresh_state", None),
            "freshness_status": getattr(source, "freshness_status", None),
            "category_freshness": per_category,
        })

    category_freshness = []
    for category, payload in sorted(category_rollup.items()):
        category_freshness.append({
            **payload,
            "source_count": len(set(payload["source_ids"])),
            "binding_source_count": len(set(payload["binding_source_ids"])),
            "overdue_source_count": len(set(payload["overdue_source_ids"])),
            "authoritative_overdue_source_count": len(set(payload["authoritative_overdue_source_ids"])),
            "legal_stale": category in legal_overdue_categories,
            "informational_stale": category in informational_overdue_categories,
        })

    next_due_at = None
    due_values = [item.get("due_at") for item in sources_payload if item.get("due_at")]
    if due_values:
        next_due_at = min(due_values)

    return {
        "source_count": len(scoped),
        "sources": sources_payload,
        "category_freshness": category_freshness,
        "overdue_categories": sorted(c for c in overdue_categories if c),
        "critical_overdue_categories": sorted(c for c in critical_overdue_categories if c),
        "legal_overdue_categories": sorted(c for c in legal_overdue_categories if c),
        "informational_overdue_categories": sorted(c for c in informational_overdue_categories if c),
        "stale_authoritative_categories": sorted(c for c in stale_authoritative_categories if c),
        "due_soon_categories": sorted(c for c in due_soon_categories if c),
        "has_overdue_sources": bool(overdue_categories),
        "has_critical_overdue_sources": bool(critical_overdue_categories),
        "has_legal_overdue_sources": bool(legal_overdue_categories),
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
    legal_overdue_categories: list[str] | None = None,
    informational_overdue_categories: list[str] | None = None,
    stale_authoritative_categories: list[str] | None = None,
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
        "legal_overdue_categories": list(legal_overdue_categories or []),
        "informational_overdue_categories": list(informational_overdue_categories or []),
        "stale_authoritative_categories": list(stale_authoritative_categories or []),
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
        "legal_overdue_categories": list(requirements.get("legal_overdue_categories") or []),
        "informational_overdue_categories": list(requirements.get("informational_overdue_categories") or []),
        "stale_authoritative_categories": list(requirements.get("stale_authoritative_categories") or []),
        "refresh_state": getattr(profile, "refresh_state", None),
    }
