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
    Deterministic HQS precheck (approval-required mutation agent):
    - pulls effective HQS item library (baseline + policy tables/addenda)
    - checks existing PropertyChecklistItems
    - emits actionable proposed actions in *contract format* for human approval:
        - workflow_event.create (to log missing checklist rows)
        - rehab_task.create (to propose fixes for failed/blocked items)
    """
    if not property_id:
        return {"agent_key": "hqs_precheck", "summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"agent_key": "hqs_precheck", "summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    # Effective HQS items (policy truth layer)
    lib = get_effective_hqs_items(db, org_id=org_id, prop=prop)

    # Existing checklist items (if present)
    existing = db.scalars(
        select(PropertyChecklistItem)
        .where(PropertyChecklistItem.org_id == org_id)
        .where(PropertyChecklistItem.property_id == property_id)
    ).all()

    # NOTE: your PropertyChecklistItem may store code as `code` or `item_code`
    # We'll support both.
    def _item_code(row: Any) -> str:
        return str(getattr(row, "code", None) or getattr(row, "item_code", None) or "").strip()

    by_code = {_item_code(i): i for i in existing if _item_code(i)}

    likely_fails: list[dict[str, Any]] = []
    missing_codes = 0

    for item in lib.get("items", []):
        code = str(item.get("code", "")).strip()
        if not code:
            continue

        ex = by_code.get(code)
        if ex is None:
            missing_codes += 1
            continue

        st = (getattr(ex, "status", None) or "").strip().lower()
        if st in {"failed", "blocked"}:
            likely_fails.append(
                {
                    "code": code,
                    "description": item.get("description", ""),
                    "category": item.get("category", "other"),
                    "severity": item.get("severity", "fail"),
                    "status": st,
                    "suggested_fix": item.get("suggested_fix"),
                }
            )

    # Contract requires non-empty actions[] for mutate_requires_approval agents.
    actions: list[dict[str, Any]] = []

    # If checklist items are missing, we emit a workflow event (allowed by contract)
    # This does NOT mutate checklist rows; it logs a deterministic need.
    if missing_codes > 0:
        actions.append(
            {
                "entity_type": "workflow_event",
                "op": "create",
                "data": {
                    "property_id": int(prop.id),
                    "event_type": "hqs_checklist_missing",
                    "payload": {
                        "property_id": int(prop.id),
                        "missing_codes_count": int(missing_codes),
                        "note": "HQS library items exist but checklist rows are missing; generate checklist rows before relying on status-driven fails.",
                    },
                },
                "reason": f"{missing_codes} HQS items are not present as checklist rows yet.",
            }
        )

    # For each likely fail, propose a rehab task create (allowed by contract)
    for lf in likely_fails[:30]:
        sev = str(lf.get("severity") or "").lower()
        priority = "high" if sev in {"fail", "critical", "high"} else "medium"

        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(prop.id),
                    "title": f"HQS fix: {lf['code']}",
                    "category": str(lf.get("category") or "other"),
                    "status": "todo",
                    "priority": priority,
                    "description": str(lf.get("description") or ""),
                    "notes": str(lf.get("suggested_fix") or ""),
                    "source": "hqs_precheck",
                },
                "reason": "Convert failed/blocked HQS checklist items into rehab tasks (approval required).",
            }
        )

    citations: list[dict[str, Any]] = []
    if lib.get("sources"):
        citations.append({"type": "hqs_policy_sources", "sources": lib["sources"]})

    return {
        "agent_key": "hqs_precheck",
        "summary": f"HQS precheck: {len(likely_fails)} likely fail items; {missing_codes} missing checklist rows; {len(actions)} proposed actions.",
        "facts": {
            "property_id": int(prop.id),
            "hqs_items_total": int(len(lib.get("items", []))),
            "existing_checklist_items": int(len(existing)),
            "likely_fails": likely_fails,
            "missing_codes": int(missing_codes),
        },
        "actions": actions,  # ✅ contract-compliant schema
        "citations": citations,
    }
