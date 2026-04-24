from __future__ import annotations

import json

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.policy_models import JurisdictionProfile


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
    artifact_snapshot = _artifact_snapshot_from_completeness(completeness)
    repo_pdf_count = int((artifact_snapshot.get('pdfs') or {}).get('count') or 0)
    repo_policy_raw_count = int((artifact_snapshot.get('policy_raw') or {}).get('count') or 0)

    lockout_causing_categories = _dedupe(
        [c for c in critical_stale if c in LEGAL_BLOCKING_CATEGORIES]
        + [c for c in authority_gap_categories if c in LEGAL_BLOCKING_CATEGORIES]
        + [c for c in critical_binding_failures if c in LEGAL_BLOCKING_CATEGORIES]
    )
    artifact_gap_categories = []
    if repo_pdf_count <= 0 and repo_policy_raw_count <= 0:
        for category in ('section8', 'program_overlay'):
            if category in LEGAL_BLOCKING_CATEGORIES and category in authority_gap_categories:
                artifact_gap_categories.append(category)

    informational_gap_categories = _dedupe(
        [c for c in stale if c not in set(lockout_causing_categories)]
        + [c for c in missing if c not in set(lockout_causing_categories)]
        + [c for c in weak_support if c not in set(lockout_causing_categories)]
        + [c for c in validation_pending if c not in set(lockout_causing_categories)]
        + [c for c in artifact_gap_categories if c not in set(lockout_causing_categories)]
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
        "artifact_gap_categories": _dedupe(artifact_gap_categories),
        "repo_artifact_snapshot": artifact_snapshot,
        "repo_pdf_count": int(repo_pdf_count),
        "repo_policy_raw_count": int(repo_policy_raw_count),
        "refresh_state": refresh_state,
        "blocking_state": refresh_state in {"blocked", "failed"} or hard_lock,
    }


def _artifact_snapshot_from_completeness(completeness: dict) -> dict:
    sla_summary = completeness.get('sla_summary') or {}
    if isinstance(sla_summary, dict) and isinstance(sla_summary.get('repo_artifact_snapshot'), dict):
        return dict(sla_summary.get('repo_artifact_snapshot') or {})
    return {}



# --- surgical final lockout override ---

LEGAL_BLOCKING_CATEGORIES = {
    "registration", "inspection", "occupancy", "lead", "section8", "program_overlay",
    "safety", "source_of_income", "permits", "rental_license",
}

def _critical_missing_binding_categories(completeness: dict) -> list[str]:
    required = set(str(x).strip() for x in list(completeness.get("critical_categories") or []) if str(x).strip())
    legally_binding = set(str(x).strip() for x in list(completeness.get("legally_binding_categories") or []) if str(x).strip())
    missing = set(str(x).strip() for x in list(completeness.get("missing_categories") or []) if str(x).strip())
    return sorted((required | legally_binding | LEGAL_BLOCKING_CATEGORIES).intersection(missing))


def _lockout_truth_basis(payload: dict) -> dict:
    lockout_causing_categories = _dedupe(list(payload.get("lockout_causing_categories") or []))
    authority_gap_categories = _dedupe(list(payload.get("authority_gap_categories") or []))
    critical_binding_failure_categories = _dedupe(list(payload.get("critical_binding_failure_categories") or []))
    validation_pending_categories = _dedupe(list(payload.get("validation_pending_categories") or []))
    safe = not bool(lockout_causing_categories or critical_binding_failure_categories)
    return {
        "safe_to_rely_on": bool(safe),
        "lockout_active": bool(payload.get("lockout_active")),
        "lockout_reason": payload.get("lockout_reason"),
        "lockout_causing_categories": lockout_causing_categories,
        "authority_gap_categories": authority_gap_categories,
        "critical_binding_failure_categories": critical_binding_failure_categories,
        "validation_pending_categories": validation_pending_categories,
    }

try:
    _surgical_lockout_original_profile_lockout_payload = profile_lockout_payload
except NameError:
    _surgical_lockout_original_profile_lockout_payload = None

if _surgical_lockout_original_profile_lockout_payload is not None:
    def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
        payload = dict(_surgical_lockout_original_profile_lockout_payload(profile, completeness))
        critical_missing_binding = _critical_missing_binding_categories(completeness)
        authority_gap_categories = _dedupe(
            list(payload.get("authority_gap_categories") or [])
            + list(critical_missing_binding)
        )
        lockout_causing_categories = _dedupe(
            [c for c in list(payload.get("lockout_causing_categories") or []) if c in LEGAL_BLOCKING_CATEGORIES]
            + [c for c in authority_gap_categories if c in LEGAL_BLOCKING_CATEGORIES]
            + [c for c in list(payload.get("critical_binding_failure_categories") or []) if c in LEGAL_BLOCKING_CATEGORIES]
        )
        informational_gap_categories = _dedupe(
            [c for c in list(payload.get("informational_gap_categories") or []) if c not in set(lockout_causing_categories)]
            + [c for c in list(payload.get("missing_categories") or []) if c not in set(lockout_causing_categories)]
            + [c for c in list(payload.get("weak_support_categories") or []) if c not in set(lockout_causing_categories)]
            + [c for c in list(payload.get("validation_pending_categories") or []) if c not in set(lockout_causing_categories)]
        )
        hard_lock = bool(payload.get("lockout_active")) or bool(lockout_causing_categories)
        lockout_reason = payload.get("lockout_reason")
        if hard_lock and not lockout_reason:
            lockout_reason = "critical_categories_missing_binding_authority"

        payload["authority_gap_categories"] = authority_gap_categories
        payload["lockout_causing_categories"] = lockout_causing_categories
        payload["informational_gap_categories"] = informational_gap_categories
        payload["critical_missing_binding_categories"] = critical_missing_binding
        payload["lockout_active"] = hard_lock
        payload["blocking_state"] = bool(payload.get("blocking_state")) or hard_lock
        payload["lockout_reason"] = lockout_reason
        payload["current_truth_basis"] = _lockout_truth_basis(payload)
        payload["safe_to_rely_on"] = bool(payload["current_truth_basis"].get("safe_to_rely_on"))
        return payload


# --- surgical final stale-authoritative + conflict lockout overlay ---

try:
    _final_lockout_original_profile_lockout_payload = profile_lockout_payload
except NameError:
    _final_lockout_original_profile_lockout_payload = None

if _final_lockout_original_profile_lockout_payload is not None:
    def profile_lockout_payload(profile: JurisdictionProfile, completeness: dict) -> dict:
        payload = dict(_final_lockout_original_profile_lockout_payload(profile, completeness))
        conflicting_categories = _dedupe(list(completeness.get("conflicting_categories") or []))
        stale_authoritative_categories = _dedupe(list(completeness.get("stale_authoritative_categories") or []))
        critical_missing_binding = _dedupe(
            list(payload.get("critical_missing_binding_categories") or [])
            + _critical_missing_binding_categories(completeness)
        )
        authority_gap_categories = _dedupe(
            list(payload.get("authority_gap_categories") or [])
            + list(critical_missing_binding)
        )

        lockout_causing_categories = _dedupe(
            list(payload.get("lockout_causing_categories") or [])
            + [c for c in conflicting_categories if c in LEGAL_BLOCKING_CATEGORIES]
            + [c for c in stale_authoritative_categories if c in LEGAL_BLOCKING_CATEGORIES]
            + [c for c in authority_gap_categories if c in LEGAL_BLOCKING_CATEGORIES]
        )

        hard_lock = bool(payload.get("lockout_active")) or bool(lockout_causing_categories)
        lockout_reason = payload.get("lockout_reason")
        if conflicting_categories:
            lockout_reason = "unresolved_conflicting_categories"
        elif stale_authoritative_categories:
            lockout_reason = "critical_authoritative_data_stale"
        elif authority_gap_categories and not lockout_reason:
            lockout_reason = "critical_categories_missing_binding_authority"

        payload["conflicting_categories"] = conflicting_categories
        payload["stale_authoritative_categories"] = stale_authoritative_categories
        payload["critical_missing_binding_categories"] = critical_missing_binding
        payload["authority_gap_categories"] = authority_gap_categories
        payload["lockout_causing_categories"] = lockout_causing_categories
        payload["lockout_active"] = hard_lock
        payload["blocking_state"] = bool(payload.get("blocking_state")) or hard_lock
        payload["lockout_reason"] = lockout_reason
        payload["current_truth_basis"] = _lockout_truth_basis(payload)
        payload["safe_to_rely_on"] = bool(payload["current_truth_basis"].get("safe_to_rely_on"))
        return payload
