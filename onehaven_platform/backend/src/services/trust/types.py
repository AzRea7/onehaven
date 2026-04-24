from __future__ import annotations

from typing import Literal, TypedDict

TrustStatus = Literal["SAFE", "WARNING", "BLOCKED"]


class TrustGateResult(TypedDict, total=False):
    status: TrustStatus
    safe_for_projection: bool
    safe_for_user_reliance: bool
    blocked_reason: str | None
    confidence: str
    reasons: list[str]
    required_actions: list[str]
    financial_impact: dict
