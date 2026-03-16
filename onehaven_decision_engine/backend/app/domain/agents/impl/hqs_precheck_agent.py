from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.compliance.hqs_library import get_effective_hqs_items
from app.models import Property


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    prop = None
    if property_id is not None:
        prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "hqs_precheck",
            "summary": "No property found for HQS precheck.",
            "facts": {"property_id": property_id},
            "actions": [],
        }

    lib = get_effective_hqs_items(db, org_id=int(org_id), prop=prop)
    items = list((lib or {}).get("items") or [])
    likely_failures = [i for i in items if str(i.get("severity") or "").lower() in {"fail", "critical", "high"}]

    actions: list[dict[str, Any]] = []
    for item in likely_failures[:10]:
        code = str(item.get("code") or item.get("title") or "HQS item").strip()
        category = str(item.get("category") or "safety")
        fix = str(item.get("suggested_fix") or item.get("title") or "Review and remediate")
        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(property_id),
                    "title": f"HQS: {code}",
                    "category": category,
                    "status": "todo",
                    "inspection_relevant": True,
                    "cost_estimate": float(item.get("default_cost_estimate") or 0.0),
                    "notes": fix,
                },
                "reason": "Likely HQS fail converted into a proposed remediation task.",
            }
        )

    actions.append(
        {
            "entity_type": "workflow_event",
            "op": "create",
            "data": {
                "event_type": "agent_hqs_precheck_completed",
                "payload": {
                    "property_id": int(property_id),
                    "likely_fail_count": len(likely_failures),
                },
            },
            "reason": "Persist HQS precheck completion to workflow history.",
        }
    )

    return {
        "agent_key": "hqs_precheck",
        "summary": "HQS precheck identified likely fail points and proposed approval-gated remediation actions.",
        "facts": {
            "property_id": property_id,
            "total_library_items": len(items),
            "likely_fail_count": len(likely_failures),
        },
        "actions": actions,
        "recommendations": [
            {
                "type": "inspection_readiness",
                "title": "Clear life-safety items first",
                "reason": "Life-safety defects create the nastiest inspection surprises and tenant delays.",
                "priority": "high",
            }
        ],
    }
