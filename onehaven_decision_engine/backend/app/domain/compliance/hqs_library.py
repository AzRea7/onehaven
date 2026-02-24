# onehaven_decision_engine/backend/app/domain/compliance/hqs_library.py
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import HqsRule, HqsAddendum, JurisdictionProfile


def _loads(s: str | None, default):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _baseline_hqs_items() -> list[dict[str, Any]]:
    """
    Minimal-but-operational baseline HQS-ish library.
    Expand over time (this is your canonical internal list).
    """
    return [
        {"code": "SMOKE_CO", "description": "Smoke + CO detectors present and functional", "category": "safety", "severity": "fail", "suggested_fix": "Install/replace detectors; test and document."},
        {"code": "GFCI_KITCHEN", "description": "GFCI present at kitchen counter outlets", "category": "electrical", "severity": "fail", "suggested_fix": "Install GFCI receptacle or breaker."},
        {"code": "GFCI_BATH", "description": "GFCI present at bathroom outlets", "category": "electrical", "severity": "fail", "suggested_fix": "Install GFCI receptacle or breaker."},
        {"code": "HANDRAILS", "description": "Handrails secure on stairs (where required)", "category": "safety", "severity": "fail", "suggested_fix": "Install/secure handrails; verify stability."},
        {"code": "HEAT", "description": "Permanent heat source operational", "category": "interior", "severity": "fail", "suggested_fix": "Repair furnace/boiler; verify thermostat control."},
        {"code": "HOT_WATER", "description": "Hot water available; no unsafe leaks", "category": "plumbing", "severity": "fail", "suggested_fix": "Repair water heater/leaks; confirm safe venting."},
        {"code": "LEAKS_ROOF", "description": "No active roof leaks / ceiling damage", "category": "exterior", "severity": "fail", "suggested_fix": "Repair roof; replace damaged interior materials."},
        {"code": "WINDOWS_LOCKS", "description": "Windows intact and lockable", "category": "egress", "severity": "fail", "suggested_fix": "Repair/replace sash/locks; ensure emergency egress works."},
        {"code": "ELECT_PANEL", "description": "Electrical panel safe (no exposed live parts)", "category": "electrical", "severity": "fail", "suggested_fix": "Install blanks/covers; correct unsafe wiring."},
    ]


def get_effective_hqs_items(db: Session, *, org_id: int, prop: Property) -> dict[str, Any]:
    """
    Effective HQS items = baseline + policy table overrides (HqsRule) + local addendum.

    NOTE (current design):
      - HqsRule is treated as GLOBAL policy (no org_id column on the policy model).
      - org_id is still passed because the rest of the compliance stack is org-scoped,
        but we do not filter HqsRule by org_id.
    """
    items = {i["code"]: dict(i) for i in _baseline_hqs_items()}
    sources: list[dict[str, Any]] = [{"type": "baseline_internal", "name": "OneHaven HQS baseline"}]

    # ✅ HqsRule overrides/extends baseline (GLOBAL rules)
    rules = db.scalars(select(HqsRule)).all()
    for r in rules:
        code = (getattr(r, "code", None) or "").strip()
        if not code:
            continue
        items[code] = {
            "code": code,
            "description": getattr(r, "description", None) or items.get(code, {}).get("description") or "",
            "category": getattr(r, "category", None) or items.get(code, {}).get("category") or "other",
            "severity": getattr(r, "severity", None) or items.get(code, {}).get("severity") or "fail",
            "suggested_fix": getattr(r, "suggested_fix", None) or items.get(code, {}).get("suggested_fix"),
        }
    if rules:
        sources.append({"type": "policy_table", "table": "HqsRule", "scope": "global", "count": len(rules)})

    # Optional addendum (your policy model currently appears org-scoped)
    # If HqsAddendum also lacks org_id in policy_models, remove org filter similarly.
    try:
        addenda = db.scalars(select(HqsAddendum).where(HqsAddendum.org_id == org_id)).all()
    except Exception:
        addenda = db.scalars(select(HqsAddendum)).all()

    if addenda:
        sources.append({"type": "policy_table", "table": "HqsAddendum", "count": len(addenda)})

    return {"items": list(items.values()), "sources": sources}
