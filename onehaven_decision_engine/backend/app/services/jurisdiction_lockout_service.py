from __future__ import annotations

from app.config import settings
from app.policy_models import JurisdictionProfile


LEGAL_BLOCKING_CATEGORIES = {"registration", "inspection", "occupancy", "lead", "section8", "program_overlay", "safety"}


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _critical_binding_failures(completeness: dict) -> list[str]:
    category_details = completeness.get("category_details") or {}
    out: list[str] = []
    for category, detail in category_details.items():
        if category not in LEGAL_BLOCKING_CATEGORIES:
            continue
        if not isinstance(detail, dict):
            continue
        if bool(detail.get("binding_authority_unmet")) or bool(detail.get("supporting_only")):
            out.append(str(category))
    return sorted(set(out))


def _validation_pending_categories(completeness: dict) -> list[str]:
    pending = list(completeness.get("validation_pending_categories") or [])
    category_details = completeness.get("category_details") or {}
    for category, detail in category_details.items():
        if not isinstance(detail, dict):
            continue
        if bool(detail.get("validation_pending")) or str(detail.get("validation_state") or "").strip().lower() in {"pending", "needs_validation", "validating"}:
            pending.append(str(category))
    return _dedupe(pending)


def _authority_gap_categories(completeness: dict) -> list[str]:
    categories = []
    for key in (
        "authority_unmet_categories",
        "binding_unmet_categories",
        "legally_binding_missing_authority_categories",
        "supporting_only_categories",
    ):
        categories.extend(_as_list(completeness.get(key)))
    return _dedupe([str(x) for x in categories])


def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
    critical_stale = list(completeness.get("critical_stale_categories") or [])
    stale = list(completeness.get("stale_categories") or [])
    missing = list(completeness.get("missing_categories") or [])
    authority_unmet = list(completeness.get("authority_unmet_categories") or [])
    supporting_only = list(completeness.get("supporting_only_categories") or [])
    weak_support = list(completeness.get("weak_support_categories") or [])
    binding_unmet = list(completeness.get("binding_unmet_categories") or [])
    legally_binding_missing = list(completeness.get("legally_binding_missing_authority_categories") or [])
    critical_binding_failures = _critical_binding_failures(completeness)
    validation_pending = _validation_pending_categories(completeness)
    authority_gap_categories = _authority_gap_categories(completeness)
    is_stale = bool(completeness.get("is_stale"))
    refresh_state = getattr(profile, "refresh_state", None)

    lockout_causing_categories = _dedupe(
        [c for c in critical_stale if c in LEGAL_BLOCKING_CATEGORIES]
        + [c for c in authority_gap_categories if c in LEGAL_BLOCKING_CATEGORIES]
        + [c for c in critical_binding_failures if c in LEGAL_BLOCKING_CATEGORIES]
    )
    informational_gap_categories = _dedupe(
        [c for c in stale if c not in set(lockout_causing_categories)]
        + [c for c in missing if c not in set(lockout_causing_categories)]
        + [c for c in weak_support if c not in set(lockout_causing_categories)]
        + [c for c in validation_pending if c not in set(lockout_causing_categories)]
    )

    hard_lock = bool(getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)) and (
        bool(critical_stale)
        or bool(authority_unmet)
        or bool(supporting_only)
        or bool(binding_unmet)
        or bool(legally_binding_missing)
        or bool(critical_binding_failures)
        or (is_stale and refresh_state == "blocked")
        or (refresh_state == "blocked" and bool(stale))
    )

    lockout_reason = None
    if hard_lock:
        if binding_unmet or legally_binding_missing or critical_binding_failures:
            lockout_reason = "critical_categories_missing_binding_authority"
        elif authority_unmet:
            lockout_reason = "critical_categories_missing_required_authority"
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
        "binding_unmet_categories": binding_unmet,
        "legally_binding_missing_authority_categories": legally_binding_missing,
        "critical_binding_failure_categories": critical_binding_failures,
        "validation_pending_categories": validation_pending,
        "authority_gap_categories": authority_gap_categories,
        "lockout_causing_categories": lockout_causing_categories,
        "informational_gap_categories": informational_gap_categories,
        "refresh_state": refresh_state,
        "blocking_state": refresh_state in {"blocked", "failed"} or hard_lock,
    }
