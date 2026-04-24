from __future__ import annotations

from typing import Any


def build_deal_intelligence(*, db: Any, property_id: int, org_id: int | None = None) -> dict[str, Any]:
    """
    Product-facing investor/deal intelligence entrypoint.

    This should become the single service used by investor routers/pages for
    scoring, ranked deals, compliance drag, and buy/caution/avoid decisions.
    """
    return {
        "property_id": property_id,
        "org_id": org_id,
        "status": "needs_integration",
        "recommendation": "unknown",
        "score": None,
        "risks": [],
        "compliance_drag": None,
    }
