# backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ...models import Inspection, InspectionItem, Property


@dataclass(frozen=True)
class AgentResult:
    status: str  # done|failed
    output: dict[str, Any]


def _loads(s: Optional[str], default: Any):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def execute_agent(db: Session, *, org_id: int, agent_key: str, property_id: Optional[int], input_json: Optional[str]) -> AgentResult:
    """
    Deterministic agent execution (Phase 5 starter).
    Later: swap internals to call LM Studio via an adapter, without changing router contracts.
    """
    payload = _loads(input_json, {})

    if agent_key == "hqs_precheck":
        if not property_id:
            return AgentResult(status="failed", output={"error": "property_id required"})

        prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
        if not prop:
            return AgentResult(status="failed", output={"error": "property not found"})

        insp = db.scalar(
            select(Inspection)
            .where(Inspection.property_id == property_id)
            .order_by(desc(Inspection.inspection_date), desc(Inspection.id))
            .limit(1)
        )

        items_out: list[dict[str, Any]] = []
        if insp:
            fails = db.scalars(
                select(InspectionItem).where(
                    InspectionItem.inspection_id == insp.id,
                    InspectionItem.failed.is_(True),
                    InspectionItem.resolved_at.is_(None),
                )
            ).all()
            for f in fails[: int(payload.get("max_items", 15))]:
                items_out.append(
                    {
                        "type": "inspection",
                        "priority": "high",
                        "code": (f.code or "FAIL").upper(),
                        "category": "inspection_fail",
                        "text": (f.details or "Resolve inspection failure").strip(),
                    }
                )

        if not items_out:
            items_out.append(
                {"type": "compliance", "priority": "med", "code": "BASELINE", "category": "hqs", "text": "Run baseline HQS checklist."}
            )

        return AgentResult(
            status="done",
            output={
                "summary": f"HQS precheck: {len(items_out)} prioritized items.",
                "items": items_out,
            },
        )

    if agent_key == "deal_intake":
        # deterministic validator: flags missing basics in property payload
        missing = []
        for k in ["address", "city", "zip", "bedrooms", "bathrooms", "asking_price"]:
            if not payload.get(k):
                missing.append(k)
        return AgentResult(
            status="done",
            output={"summary": "Deal intake scan complete.", "missing_fields": missing, "ok": len(missing) == 0},
        )

    if agent_key == "rent_reasonableness":
        # placeholder deterministic packager (real comps later)
        beds = payload.get("bedrooms")
        z = payload.get("zip")
        return AgentResult(
            status="done",
            output={
                "summary": "Rent pack placeholder (no external calls yet).",
                "inputs": {"zip": z, "bedrooms": beds},
                "note": "Later: plug in LM Studio + capped comp API calls (your 50-call rule).",
            },
        )

    return AgentResult(status="failed", output={"error": f"unknown agent_key: {agent_key}"})