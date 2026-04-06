from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Inspection, Property


def _must_get_inspection(db: Session, *, org_id: int, inspection_id: int) -> Inspection:
    row = db.scalar(
        select(Inspection).where(
            Inspection.org_id == int(org_id),
            Inspection.id == int(inspection_id),
        )
    )
    if row is None:
        raise ValueError("inspection not found")
    return row


def _maybe_get_property(db: Session, *, org_id: int, property_id: int | None) -> Property | None:
    if property_id is None:
        return None
    return db.scalar(
        select(Property).where(
            Property.org_id == int(org_id),
            Property.id == int(property_id),
        )
    )


def _appointment_address(prop: Property | None, inspection: Inspection) -> str:
    if prop is None:
        return f"Property #{getattr(inspection, 'property_id', '')}"
    parts = [
        getattr(prop, "address", None),
        getattr(prop, "city", None),
        getattr(prop, "state", None),
        getattr(prop, "zip", None),
    ]
    return ", ".join([str(p).strip() for p in parts if p])


def _friendly_when(dt: datetime | None) -> str:
    if dt is None:
        return "TBD"
    return dt.strftime("%A, %B %d, %Y at %I:%M %p UTC")


def build_inspector_contact_payload(
    db: Session,
    *,
    org_id: int,
    inspection_id: int,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
    scheduled_for = getattr(inspection, "scheduled_for", None) or getattr(inspection, "inspection_date", None)

    subject = f"Inspection appointment for {_appointment_address(prop, inspection)}"
    body_lines = [
        f"Property: {_appointment_address(prop, inspection)}",
        f"When: {_friendly_when(scheduled_for)}",
        f"Inspector: {getattr(inspection, 'inspector_name', None) or 'TBD'}",
        f"Company: {getattr(inspection, 'inspector_company', None) or 'TBD'}",
        f"Phone: {getattr(inspection, 'inspector_phone', None) or 'TBD'}",
        f"Notes: {getattr(inspection, 'appointment_notes', None) or getattr(inspection, 'notes', None) or ''}",
    ]

    return {
        "inspection_id": int(inspection.id),
        "property_id": int(getattr(inspection, "property_id", 0) or 0),
        "scheduled_for": scheduled_for,
        "inspector": {
            "name": getattr(inspection, "inspector_name", None),
            "company": getattr(inspection, "inspector_company", None),
            "email": getattr(inspection, "inspector_email", None),
            "phone": getattr(inspection, "inspector_phone", None),
        },
        "property": (
            {
                "id": int(getattr(prop, "id", 0) or 0),
                "address": getattr(prop, "address", None),
                "city": getattr(prop, "city", None),
                "state": getattr(prop, "state", None),
                "zip": getattr(prop, "zip", None),
            }
            if prop is not None
            else None
        ),
        "email": {
            "to": getattr(inspection, "inspector_email", None),
            "subject": subject,
            "body": "\n".join(body_lines).strip(),
        },
        "sms": {
            "to": getattr(inspection, "inspector_phone", None),
            "body": f"{subject} on {_friendly_when(scheduled_for)}.",
        },
    }


def build_inspection_reminder_message(
    db: Session,
    *,
    org_id: int,
    inspection_id: int,
    reminder_offset_minutes: int,
) -> dict[str, Any]:
    payload = build_inspector_contact_payload(db, org_id=org_id, inspection_id=inspection_id)
    when_text = _friendly_when(payload.get("scheduled_for"))
    lead = int(reminder_offset_minutes)

    email_subject = f"Reminder: inspection in {lead} minutes"
    email_body = "\n".join(
        [
            f"This is a reminder for the inspection appointment.",
            f"When: {when_text}",
            f"Property: {((payload.get('property') or {}).get('address')) or 'TBD'}",
            f"Inspector: {((payload.get('inspector') or {}).get('name')) or 'TBD'}",
            f"Company: {((payload.get('inspector') or {}).get('company')) or 'TBD'}",
            "",
            ((payload.get("email") or {}).get("body")) or "",
        ]
    ).strip()

    sms_body = (
        f"Reminder: inspection at {when_text}. "
        f"Property: {((payload.get('property') or {}).get('address')) or 'TBD'}. "
        f"Inspector: {((payload.get('inspector') or {}).get('name')) or 'TBD'}."
    )

    return {
        "inspection_id": payload["inspection_id"],
        "property_id": payload["property_id"],
        "reminder_offset_minutes": lead,
        "email": {
            "to": ((payload.get("email") or {}).get("to")),
            "subject": email_subject,
            "body": email_body,
        },
        "sms": {
            "to": ((payload.get("sms") or {}).get("to")),
            "body": sms_body,
        },
    }
