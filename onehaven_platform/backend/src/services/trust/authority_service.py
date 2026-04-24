from __future__ import annotations

from typing import Any


def evaluate_authority(payload: dict[str, Any]) -> dict[str, Any]:
    missing_binding = payload.get("missing_binding_authority") or []
    weak_sources = payload.get("weak_authority_categories") or []

    return {
        "passed": not missing_binding,
        "missing_binding_authority": missing_binding,
        "weak_authority_categories": weak_sources,
        "reason": "missing_binding_authority" if missing_binding else None,
    }
