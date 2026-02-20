# backend/app/domain/agents/executor.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional, List

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...models import (
    Property,
    Deal,
    UnderwritingResult,
    RentAssumption,
    PropertyChecklistItem,
    Inspection,
    InspectionItem,
    RehabTask,
)
from ...services.property_state_machine import get_state_payload


def execute_agent(
    db: Session,
    *,
    org_id: int,
    agent_key: str,
    property_id: Optional[int],
    input_obj: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Deterministic agent executor (Phase 5 starter).
    Output is a JSON object stored in AgentRun.output_json.
    """
    if property_id is None:
        return {"summary": "No property_id provided.", "items": []}

    prop = db.scalar(select(Property).where(Property.id == property_id, Property.org_id == org_id))
    if not prop:
        return {"summary": "Property not found.", "items": []}

    if agent_key == "deal_intake":
        # Evidence-driven intake check
        d = db.scalar(
            select(Deal).where(Deal.org_id == org_id, Deal.property_id == property_id).order_by(Deal.id.desc()).limit(1)
        )
        ra = db.scalar(
            select(RentAssumption).where(RentAssumption.org_id == org_id, RentAssumption.property_id == property_id).limit(1)
        )

        flags = []
        if d is None:
            flags.append({"severity": "high", "flag": "missing_deal", "message": "No Deal row exists."})
        else:
            if (d.strategy or "").strip().lower() not in {"section8", "market"}:
                flags.append({"severity": "med", "flag": "unknown_strategy", "message": f"Deal.strategy='{d.strategy}'"})
            if float(d.asking_price or 0.0) <= 0:
                flags.append({"severity": "high", "flag": "invalid_asking_price", "message": "Deal.asking_price <= 0"})

        if ra is None:
            flags.append({"severity": "high", "flag": "missing_rent_assumption", "message": "No RentAssumption exists."})

        st = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=True)

        return {
            "summary": f"Deal intake scan for {prop.address}, {prop.city}: {len(flags)} flags.",
            "flags": flags,
            "state": st,
        }

    if agent_key == "rent_reasonableness":
        ra = db.scalar(
            select(RentAssumption).where(RentAssumption.org_id == org_id, RentAssumption.property_id == property_id).limit(1)
        )
        if ra is None:
            return {"summary": "Rent reasonableness: missing RentAssumption.", "packet": None}

        # Deterministic “packet skeleton”
        packet = {
            "property": {"address": prop.address, "city": prop.city, "state": prop.state, "zip": prop.zip},
            "bedrooms": prop.bedrooms,
            "assumption": {
                "rent_used": getattr(ra, "rent_used", None),
                "notes": getattr(ra, "notes", None),
            },
            "narrative_stub": "TODO: attach comps + HA packet narrative (LLM optional later).",
            "checklist": [
                "Attach 3–5 comparable rentals (same zip/nearby, similar beds/baths).",
                "Include photos and condition notes.",
                "Explain adjustments (garage, basement, updated kitchen, etc.).",
                "Confirm utilities responsibility and include in packet.",
            ],
        }

        return {"summary": "Rent reasonableness packet skeleton generated.", "packet": packet}

    if agent_key == "hqs_precheck":
        max_items = int(input_obj.get("max_items", 25))

        items = db.scalars(
            select(PropertyChecklistItem).where(
                PropertyChecklistItem.org_id == org_id,
                PropertyChecklistItem.property_id == property_id,
                PropertyChecklistItem.status.in_(["todo", "in_progress", "blocked", "failed"]),
            )
        ).all()

        inspections = db.scalars(select(Inspection).where(Inspection.property_id == property_id)).all()
        insp_ids = [i.id for i in inspections]
        fails: List[InspectionItem] = []
        if insp_ids:
            fails = db.scalars(
                select(InspectionItem).where(
                    InspectionItem.inspection_id.in_(insp_ids),
                    InspectionItem.failed.is_(True),
                    InspectionItem.resolved_at.is_(None),
                )
            ).all()

        out = []
        for it in items:
            out.append(
                {
                    "type": "checklist",
                    "priority": "high" if int(getattr(it, "severity", 2) or 2) >= 4 else "med",
                    "code": it.item_code,
                    "category": it.category,
                    "text": it.description,
                }
            )
        for f in fails:
            out.append(
                {
                    "type": "inspection",
                    "priority": "high" if int(getattr(f, "severity", 1) or 1) >= 3 else "med",
                    "code": f.code,
                    "category": "inspection_fail",
                    "text": f"Resolve {f.code} at {f.location or 'unknown'}",
                }
            )

        out = sorted(out, key=lambda x: (0 if x["priority"] == "high" else 1, x["type"], x["code"]))[:max_items]
        return {"summary": f"HQS precheck: {len(out)} prioritized items.", "items": out}

    return {"summary": f"Agent '{agent_key}' not implemented.", "items": []}