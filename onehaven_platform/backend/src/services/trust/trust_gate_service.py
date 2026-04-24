from __future__ import annotations

from typing import Any

from .authority_service import evaluate_authority
from .completeness_service import evaluate_completeness
from .freshness_service import evaluate_freshness
from .types import TrustGateResult


def evaluate_trust_gate(payload: dict[str, Any]) -> TrustGateResult:
    completeness = evaluate_completeness(payload)
    authority = evaluate_authority(payload)
    freshness = evaluate_freshness(payload)

    reasons: list[str] = []
    required_actions: list[str] = []

    for check in [completeness, authority, freshness]:
        if not check.get("passed") and check.get("reason"):
            reasons.append(str(check["reason"]))

    if "missing_required_categories" in reasons:
        required_actions.append("Complete required jurisdiction category coverage.")
    if "missing_binding_authority" in reasons:
        required_actions.append("Attach binding authoritative source evidence.")
    if "stale_authoritative_sources" in reasons:
        required_actions.append("Refresh stale authoritative sources.")

    blocked = bool(reasons)

    return {
        "status": "BLOCKED" if blocked else "SAFE",
        "safe_for_projection": not blocked,
        "safe_for_user_reliance": not blocked,
        "blocked_reason": reasons[0] if reasons else None,
        "confidence": "low" if blocked else "high",
        "reasons": reasons,
        "required_actions": required_actions,
        "financial_impact": payload.get("financial_impact") or {},
    }
