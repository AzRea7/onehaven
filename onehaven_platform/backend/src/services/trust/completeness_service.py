from __future__ import annotations

from typing import Any


def evaluate_completeness(payload: dict[str, Any]) -> dict[str, Any]:
    missing = payload.get("missing_required_categories") or []
    score = payload.get("completeness_score")

    return {
        "passed": not missing and (score is None or score >= 0.85),
        "score": score,
        "missing": missing,
        "reason": "missing_required_categories" if missing else None,
    }
