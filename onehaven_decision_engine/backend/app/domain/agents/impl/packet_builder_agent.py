from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import JurisdictionProfile


DEFAULT_PACKET = [
    "RFTA / moving paperwork",
    "Ownership / management identity",
    "W-9 or payee setup",
    "Lead disclosure / required notices",
    "Inspection readiness checklist",
    "Rent reasonableness explanation",
]


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    prop = None
    if property_id is not None:
        prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "packet_builder",
            "summary": "No property found for packet builder.",
            "facts": {"property_id": property_id},
            "recommendations": [],
        }

    profile = db.scalar(
        select(JurisdictionProfile).where(
            JurisdictionProfile.org_id == int(org_id),
            JurisdictionProfile.state == getattr(prop, "state", None),
            JurisdictionProfile.city == getattr(prop, "city", None),
        )
    )
    checklist = list(getattr(profile, "packet_requirements_json", None) or DEFAULT_PACKET)

    return {
        "agent_key": "packet_builder",
        "summary": "Packet requirements assembled from jurisdiction profile with a sensible fallback checklist.",
        "facts": {
            "property_id": property_id,
            "jurisdiction_profile_found": profile is not None,
            "packet_checklist": checklist,
        },
        "recommendations": [
            {
                "type": "packet_checklist",
                "title": "Finish the packet before inspection day chaos",
                "reason": "A boring complete packet beats heroic last-minute scrambling every time.",
                "priority": "high",
            }
        ],
    }
