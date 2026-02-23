# onehaven_decision_engine/backend/app/domain/agents/impl/public_records_check.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property


def run_public_records_check(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic v1:
    - does NOT scrape the internet
    - produces a checklist of what to collect and store (parcel id, taxes, last sale, lot size, etc.)
    - later you can connect county APIs / paid data with your API budget enforcement
    """
    if not property_id:
        return {"summary": "No property_id provided.", "facts": {}, "actions": [], "citations": []}

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    requests = [
        "parcel_id",
        "tax_assessed_value",
        "taxes_last_year",
        "ownership_name",
        "last_sale_date",
        "last_sale_price",
        "lot_size",
        "zoning",
        "flood_zone",
        "lead_paint_risk (pre-1978)",
    ]

    actions = [
        {
            "op": "recommend",
            "entity_type": "PublicRecordRequest",
            "entity_id": None,
            "payload": {"property_id": prop.id, "requested_fields": requests},
            "priority": "medium",
        }
    ]

    facts = {
        "property": {"id": prop.id, "address": prop.address, "city": prop.city, "state": prop.state, "zip": prop.zip},
        "note": "This agent is deterministic v1; it produces a public-records collection checklist, not an automated pull.",
    }

    return {
        "summary": "Public records checklist generated (deterministic v1).",
        "facts": facts,
        "actions": actions,
        "citations": [],
    }