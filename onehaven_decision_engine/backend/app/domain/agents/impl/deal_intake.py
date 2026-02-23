# onehaven_decision_engine/backend/app/domain/agents/impl/deal_intake.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property, Deal, RentAssumption
from app.services.jurisdiction_profile_service import resolve_jurisdiction_profile


def run_deal_intake(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic intake agent:
    - pulls Property + latest Deal + RentAssumption
    - resolves jurisdiction profile
    - emits missing-field actions + disqualifier flags
    """
    if not property_id:
        return {
            "summary": "No property_id provided; cannot run deal_intake.",
            "facts": {},
            "actions": [],
            "citations": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        return {"summary": "Property not found.", "facts": {}, "actions": [], "citations": []}

    deal = db.scalar(
        select(Deal)
        .where(Deal.org_id == org_id, Deal.property_id == property_id)
        .order_by(Deal.id.desc())
    )
    rent = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.org_id == org_id, RentAssumption.property_id == property_id)
        .order_by(RentAssumption.id.desc())
    )

    jp = resolve_jurisdiction_profile(db, org_id=org_id, prop=prop)

    missing: list[str] = []
    # Property essentials
    if not prop.address:
        missing.append("property.address")
    if not prop.city:
        missing.append("property.city")
    if not prop.state:
        missing.append("property.state")
    if not prop.zip:
        missing.append("property.zip")
    if not prop.bedrooms:
        missing.append("property.bedrooms")

    # Deal essentials
    if not deal:
        missing.append("deal (none exists)")
    else:
        if getattr(deal, "purchase_price", None) in (None, 0):
            missing.append("deal.purchase_price")
        if getattr(deal, "strategy", None) is None:
            missing.append("deal.strategy")

    # Rent assumptions (for later stages)
    if not rent:
        missing.append("rent_assumption (none exists)")
    else:
        if getattr(rent, "target_rent", None) in (None, 0):
            missing.append("rent_assumption.target_rent")

    actions: list[dict[str, Any]] = []
    for field in missing:
        actions.append(
            {
                "op": "recommend",
                "entity_type": "FieldRequest",
                "entity_id": None,
                "payload": {"field": field, "reason": "Required for intake completeness"},
                "priority": "high",
            }
        )

    # Disqualifier-ish flags (deterministic, using your operating truth defaults indirectly)
    flags: list[str] = []
    if prop.bedrooms is not None and int(prop.bedrooms) <= 0:
        flags.append("invalid_bedrooms")

    facts = {
        "property": {
            "id": prop.id,
            "address": prop.address,
            "city": prop.city,
            "state": prop.state,
            "zip": prop.zip,
            "bedrooms": prop.bedrooms,
            "bathrooms": prop.bathrooms,
            "square_feet": prop.square_feet,
        },
        "deal_exists": bool(deal),
        "rent_assumption_exists": bool(rent),
        "jurisdiction_profile": (jp["profile"]["name"] if jp["profile"] else None),
        "jurisdiction_effective_date": (jp["profile"]["effective_date"] if jp["profile"] else None),
        "flags": flags,
        "missing_fields": missing,
    }

    summary = "Deal intake complete: "
    if missing:
        summary += f"{len(missing)} missing inputs."
    else:
        summary += "no missing inputs detected."

    citations = []
    if jp["profile"] and jp["profile"].get("source_urls"):
        citations.append({"type": "jurisdiction_profile_sources", "urls": jp["profile"]["source_urls"]})

    return {"summary": summary, "facts": facts, "actions": actions, "citations": citations}