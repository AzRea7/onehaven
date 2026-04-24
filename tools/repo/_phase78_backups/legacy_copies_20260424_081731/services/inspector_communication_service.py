from __future__ import annotations

from typing import Any, Dict


def build_inspector_contact_payload(
    *,
    property_id: int | None = None,
    inspector_name: str | None = None,
    inspector_phone: str | None = None,
    inspector_email: str | None = None,
    notes: str | None = None,
) -> Dict[str, Any]:
    """
    Normalized payload used across the app for inspector contact info.
    Keep this simple + safe so it doesn't break callers.
    """
    return {
        "property_id": property_id,
        "inspector_name": inspector_name,
        "inspector_phone": inspector_phone,
        "inspector_email": inspector_email,
        "notes": notes,
    }


def build_inspection_reminder_message(
    *,
    property_address: str | None = None,
    inspection_date: str | None = None,
    inspector_name: str | None = None,
) -> str:
    """
    Generates a human-readable reminder message.
    Keep minimal for now — expand later.
    """
    parts = ["Inspection Reminder"]

    if property_address:
        parts.append(f"for {property_address}")

    if inspection_date:
        parts.append(f"on {inspection_date}")

    if inspector_name:
        parts.append(f"(Inspector: {inspector_name})")

    return " ".join(parts)