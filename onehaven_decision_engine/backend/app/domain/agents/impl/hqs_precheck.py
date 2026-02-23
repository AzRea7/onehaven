# onehaven_decision_engine/backend/app/domain/agents/impl/hqs_precheck.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property, PropertyChecklistItem
from app.domain.compliance.hqs_library import get_effective_hqs_items


def run_hqs_precheck(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic HQS precheck:
    - pulls effective HQS item library (baseline + local addenda)
    - merges with PropertyChecklistItems (if any)
    - emits rehab-task recommendations for likely FAIL items
    """
    if not property_id:
        return {"summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    # Get effective HQS items (policy truth layer)
    lib = get_effective_hqs_items(db, org_id=org_id, prop=prop)

    # Existing checklist items (if you already generated templates)
    existing = db.scalars(
        select(PropertyChecklistItem)
        .where(PropertyChecklistItem.org_id == org_id)
        .where(PropertyChecklistItem.property_id == property_id)
    ).all()

    by_code = {str(getattr(i, "code", "")).strip(): i for i in existing if getattr(i, "code", None)}
    likely_fails: list[dict[str, Any]] = []

    # Determine "likely fail" if:
    # - existing item status == failed/blocked
    # - OR missing entirely (we treat as "unknown" and recommend creating checklist)
    missing_codes = 0
    for item in lib["items"]:
        code = item["code"]
        ex = by_code.get(code)
        if ex is None:
            missing_codes += 1
            continue
        st = (getattr(ex, "status", None) or "").strip().lower()
        if st in {"failed", "blocked"}:
            likely_fails.append(
                {
                    "code": code,
                    "description": item["description"],
                    "category": item["category"],
                    "severity": item["severity"],
                    "status": st,
                    "suggested_fix": item.get("suggested_fix"),
                }
            )

    actions: list[dict[str, Any]] = []

    if missing_codes > 0:
        actions.append(
            {
                "op": "recommend",
                "entity_type": "ComplianceChecklistRequest",
                "entity_id": None,
                "payload": {
                    "property_id": prop.id,
                    "reason": f"{missing_codes} HQS items are not present as checklist rows yet; generate checklist first.",
                },
                "priority": "high",
            }
        )

    # Convert likely fails to rehab recommendations (NOT auto-mutate)
    for lf in likely_fails[:30]:
        actions.append(
            {
                "op": "recommend",
                "entity_type": "RehabTask",
                "entity_id": None,
                "payload": {
                    "property_id": prop.id,
                    "title": f"HQS fix: {lf['code']}",
                    "description": lf["description"],
                    "category": lf["category"],
                    "priority": "high" if lf.get("severity") == "fail" else "medium",
                    "suggested_fix": lf.get("suggested_fix"),
                    "source": "hqs_precheck",
                },
                "priority": "high" if lf.get("severity") == "fail" else "medium",
            }
        )

    citations = []
    if lib.get("sources"):
        citations.append({"type": "hqs_policy_sources", "sources": lib["sources"]})

    return {
        "summary": f"HQS precheck: {len(likely_fails)} likely fail items found; {len(actions)} recommendations emitted.",
        "facts": {
            "property_id": prop.id,
            "hqs_items_total": len(lib["items"]),
            "existing_checklist_items": len(existing),
            "likely_fails": likely_fails,
            "missing_codes": missing_codes,
        },
        "actions": actions,
        "citations": citations,
    }