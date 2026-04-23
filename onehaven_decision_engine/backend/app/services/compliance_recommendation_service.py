from __future__ import annotations

from typing import Any, Iterable


def build_compliance_recommendation(
    *,
    safe_for_projection: bool,
    safe_for_user_reliance: bool,
    lockout_active: bool,
    missing_categories: Iterable[str] | None = None,
    missing_critical_categories: Iterable[str] | None = None,
    stale_authoritative_categories: Iterable[str] | None = None,
    conflicting_categories: Iterable[str] | None = None,
    manual_review_reasons: Iterable[str] | None = None,
    inspection_risk_level: str | None = None,
) -> dict[str, Any]:
    missing_categories = [str(x).strip().lower() for x in (missing_categories or []) if str(x).strip()]
    missing_critical_categories = [str(x).strip().lower() for x in (missing_critical_categories or []) if str(x).strip()]
    stale_authoritative_categories = [str(x).strip().lower() for x in (stale_authoritative_categories or []) if str(x).strip()]
    conflicting_categories = [str(x).strip().lower() for x in (conflicting_categories or []) if str(x).strip()]
    manual_review_reasons = [str(x).strip() for x in (manual_review_reasons or []) if str(x).strip()]

    recommendation = "safe_to_operate"
    status = "safe"
    reasons: list[str] = []

    if conflicting_categories:
        recommendation = "do_not_rely"
        status = "unsafe"
        reasons.append("Material conflicts remain unresolved.")
    elif lockout_active:
        recommendation = "hold_and_remediate"
        status = "unsafe"
        reasons.append("Jurisdiction or compliance lockout is active.")
    elif missing_critical_categories or stale_authoritative_categories:
        recommendation = "incomplete_authority_coverage"
        status = "incomplete"
        if missing_critical_categories:
            reasons.append("Critical authority coverage is missing.")
        if stale_authoritative_categories:
            reasons.append("Authoritative categories are stale.")
    elif manual_review_reasons or not safe_for_user_reliance:
        recommendation = "manual_review_required"
        status = "manual_review_required"
        reasons.append("Evidence or authority still requires manual review.")
    elif inspection_risk_level == "high":
        recommendation = "hold_and_remediate"
        status = "at_risk"
        reasons.append("Inspection risk is high.")
    elif inspection_risk_level == "medium" or missing_categories:
        recommendation = "monitor_and_fix"
        status = "at_risk"
        reasons.append("Compliance is usable but active remediation is still needed.")
    elif not safe_for_projection:
        recommendation = "manual_review_required"
        status = "manual_review_required"
        reasons.append("Projection safety is not satisfied.")
    else:
        reasons.append("Coverage, authority, and inspection posture support operation.")

    if manual_review_reasons:
        reasons.extend(manual_review_reasons[:5])

    return {
        "status": status,
        "recommendation": recommendation,
        "safe_to_operate": recommendation == "safe_to_operate",
        "why": reasons,
    }
