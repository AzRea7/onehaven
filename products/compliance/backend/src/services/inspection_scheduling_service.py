from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import AuditEvent, Inspection, Property
from onehaven_platform.backend.src.services.events_facade import wf
from products.compliance.backend.src.services.inspector_communication_service import (
    build_inspection_reminder_message,
    build_inspector_contact_payload,
)


_ALLOWED_APPOINTMENT_STATUS = {
    "draft",
    "scheduled",
    "confirmed",
    "completed",
    "canceled",
    "failed",
    "passed",
}


def _now() -> datetime:
    return datetime.utcnow()


def _j(value: Any) -> str:
    try:
        return json.dumps(value, default=str)
    except Exception:
        return "{}"


def _safe_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _normalize_status(value: str | None) -> str:
    status = str(value or "draft").strip().lower()
    return status if status in _ALLOWED_APPOINTMENT_STATUS else "draft"


def _normalize_offsets(values: list[int] | None) -> list[int]:
    raw = list(values or [1440, 120, 30])
    out: list[int] = []
    for value in raw:
        try:
            minutes = int(value)
        except Exception:
            continue
        if minutes < 0:
            continue
        if minutes not in out:
            out.append(minutes)
    return sorted(out, reverse=True)


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


def _set_if_present(row: Any, field_name: str, value: Any) -> None:
    if hasattr(row, field_name):
        setattr(row, field_name, value)


def _mark_updated(row: Any) -> None:
    if hasattr(row, "updated_at"):
        setattr(row, "updated_at", _now())


def _inspection_payload(row: Inspection, *, property_obj: Property | None = None) -> dict[str, Any]:
    offsets = _safe_loads(getattr(row, "reminder_offsets_json", None), [])
    sent_offsets = _safe_loads(getattr(row, "reminder_sent_offsets_json", None), [])
    return {
        "inspection_id": int(row.id),
        "property_id": int(getattr(row, "property_id", 0) or 0),
        "status": _normalize_status(getattr(row, "status", None) or getattr(row, "result_status", None) or "draft"),
        "scheduled_for": getattr(row, "scheduled_for", None),
        "inspection_date": getattr(row, "inspection_date", None),
        "inspector_name": getattr(row, "inspector_name", None) or getattr(row, "inspector", None),
        "inspector_company": getattr(row, "inspector_company", None),
        "inspector_email": getattr(row, "inspector_email", None),
        "inspector_phone": getattr(row, "inspector_phone", None),
        "calendar_event_id": getattr(row, "calendar_event_id", None),
        "calendar_provider": getattr(row, "calendar_provider", None),
        "calendar_export_url": getattr(row, "calendar_export_url", None),
        "appointment_notes": getattr(row, "appointment_notes", None) or getattr(row, "notes", None),
        "reminder_offsets": offsets,
        "reminder_sent_offsets": sent_offsets,
        "last_reminder_sent_at": getattr(row, "last_reminder_sent_at", None),
        "completed_at": getattr(row, "completed_at", None),
        "passed": getattr(row, "passed", None),
        "reinspect_required": getattr(row, "reinspect_required", None),
        "template_key": getattr(row, "template_key", None),
        "template_version": getattr(row, "template_version", None),
        "result_status": getattr(row, "result_status", None),
        "readiness_score": getattr(row, "readiness_score", None),
        "readiness_status": getattr(row, "readiness_status", None),
        "property": (
            {
                "id": int(getattr(property_obj, "id", 0) or 0),
                "address": getattr(property_obj, "address", None),
                "city": getattr(property_obj, "city", None),
                "state": getattr(property_obj, "state", None),
                "zip": getattr(property_obj, "zip", None),
            }
            if property_obj is not None
            else None
        ),
    }


def _ics_escape(value: str | None) -> str:
    return str(value or "").replace("\\", "\\\\").replace(";", r"\;").replace(",", r"\,").replace("\n", r"\n")


def build_inspection_ics_payload(
    db: Session,
    *,
    org_id: int,
    inspection_id: int,
    duration_minutes: int = 60,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
    scheduled_for = getattr(inspection, "scheduled_for", None) or getattr(inspection, "inspection_date", None)
    if scheduled_for is None:
        raise ValueError("inspection has no scheduled datetime")

    start = scheduled_for.strftime("%Y%m%dT%H%M%SZ")
    end = (scheduled_for + timedelta(minutes=max(15, int(duration_minutes)))).strftime("%Y%m%dT%H%M%SZ")
    summary = f"Inspection - {getattr(prop, 'address', None) or f'Property #{getattr(inspection, 'property_id', '')}'}"
    description = "\n".join(
        [
            f"Inspector: {getattr(inspection, 'inspector_name', None) or ''}",
            f"Company: {getattr(inspection, 'inspector_company', None) or ''}",
            f"Email: {getattr(inspection, 'inspector_email', None) or ''}",
            f"Phone: {getattr(inspection, 'inspector_phone', None) or ''}",
            f"Notes: {getattr(inspection, 'appointment_notes', None) or getattr(inspection, 'notes', None) or ''}",
        ]
    ).strip()

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//OneHaven//Inspection Scheduling//EN",
        "BEGIN:VEVENT",
        f"UID:inspection-{int(inspection.id)}@onehaven",
        f"DTSTAMP:{_now().strftime('%Y%m%dT%H%M%SZ')}",
        f"DTSTART:{start}",
        f"DTEND:{end}",
        f"SUMMARY:{_ics_escape(summary)}",
        f"DESCRIPTION:{_ics_escape(description)}",
        f"LOCATION:{_ics_escape(getattr(prop, 'address', None) or '')}",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    return {
        "inspection_id": int(inspection.id),
        "scheduled_for": scheduled_for,
        "filename": f"inspection_{int(inspection.id)}.ics",
        "content_type": "text/calendar",
        "ics": "\r\n".join(lines) + "\r\n",
    }


def schedule_inspection_appointment(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    inspection_id: int,
    scheduled_for: datetime | None,
    inspector_name: str | None = None,
    inspector_company: str | None = None,
    inspector_email: str | None = None,
    inspector_phone: str | None = None,
    reminder_offsets: list[int] | None = None,
    appointment_notes: str | None = None,
    status: str | None = None,
    calendar_provider: str | None = None,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))

    if scheduled_for is not None:
        _set_if_present(inspection, "scheduled_for", scheduled_for)
        if hasattr(inspection, "inspection_date"):
            inspection.inspection_date = scheduled_for

    if inspector_name is not None:
        _set_if_present(inspection, "inspector_name", inspector_name.strip() or None)
    if inspector_company is not None:
        _set_if_present(inspection, "inspector_company", inspector_company.strip() or None)
    if inspector_email is not None:
        _set_if_present(inspection, "inspector_email", inspector_email.strip() or None)
    if inspector_phone is not None:
        _set_if_present(inspection, "inspector_phone", inspector_phone.strip() or None)
    if appointment_notes is not None:
        _set_if_present(inspection, "appointment_notes", appointment_notes)
    if calendar_provider is not None:
        _set_if_present(inspection, "calendar_provider", calendar_provider)

    offsets = _normalize_offsets(reminder_offsets)
    _set_if_present(inspection, "reminder_offsets_json", _j(offsets))
    _set_if_present(inspection, "reminder_sent_offsets_json", _j([]))
    normalized_status = _normalize_status(status or "scheduled")
    _set_if_present(inspection, "status", normalized_status)
    _mark_updated(inspection)
    db.add(inspection)

    wf.emit_inspection_event(
        db,
        org_id=int(org_id),
        property_id=int(getattr(inspection, "property_id", 0) or 0),
        actor_user_id=actor_user_id,
        inspection_id=int(inspection.id),
        event_type="inspection.appointment.scheduled",
        payload={
            "scheduled_for": getattr(inspection, "scheduled_for", None),
            "status": normalized_status,
            "inspector_name": getattr(inspection, "inspector_name", None),
            "inspector_email": getattr(inspection, "inspector_email", None),
            "reminder_offsets": offsets,
        },
    )

    db.add(
        AuditEvent(
            org_id=int(org_id),
            actor_user_id=actor_user_id,
            action="inspection.appointment.schedule",
            entity_type="inspection",
            entity_id=str(int(inspection.id)),
            before_json=None,
            after_json=_j(_inspection_payload(inspection, property_obj=prop)),
            created_at=_now(),
        )
    )
    db.flush()
    return {
        "ok": True,
        "appointment": _inspection_payload(inspection, property_obj=prop),
        "contact_payload": build_inspector_contact_payload(
            db,
            org_id=org_id,
            inspection_id=int(inspection.id),
        ),
    }


def cancel_inspection_appointment(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    inspection_id: int,
    reason: str | None = None,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
    _set_if_present(inspection, "status", "canceled")
    _set_if_present(inspection, "canceled_at", _now())
    if reason:
        existing = getattr(inspection, "appointment_notes", None) or getattr(inspection, "notes", None) or ""
        note = f"{existing}\n\nCancellation reason: {reason}".strip()
        _set_if_present(inspection, "appointment_notes", note)
    _mark_updated(inspection)
    db.add(inspection)

    wf.emit_inspection_event(
        db,
        org_id=int(org_id),
        property_id=int(getattr(inspection, "property_id", 0) or 0),
        actor_user_id=actor_user_id,
        inspection_id=int(inspection.id),
        event_type="inspection.appointment.canceled",
        payload={"reason": reason},
    )
    db.flush()
    return {"ok": True, "appointment": _inspection_payload(inspection, property_obj=prop)}


def mark_inspection_completed(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    inspection_id: int,
    status: str | None = None,
    passed: bool | None = None,
    reinspect_required: bool | None = None,
    notes: str | None = None,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
    completion_ts = completed_at or _now()

    normalized_status = _normalize_status(status or ("passed" if passed else "failed" if passed is False else "completed"))
    _set_if_present(inspection, "status", normalized_status)
    _set_if_present(inspection, "completed_at", completion_ts)
    if hasattr(inspection, "passed") and passed is not None:
        inspection.passed = bool(passed)
    if hasattr(inspection, "reinspect_required") and reinspect_required is not None:
        inspection.reinspect_required = bool(reinspect_required)
    if notes:
        _set_if_present(inspection, "notes", notes)
        _set_if_present(inspection, "appointment_notes", notes)
    _mark_updated(inspection)
    db.add(inspection)

    wf.emit_inspection_event(
        db,
        org_id=int(org_id),
        property_id=int(getattr(inspection, "property_id", 0) or 0),
        actor_user_id=actor_user_id,
        inspection_id=int(inspection.id),
        event_type="inspection.appointment.completed",
        payload={
            "status": normalized_status,
            "passed": passed,
            "reinspect_required": reinspect_required,
            "completed_at": completion_ts,
        },
    )
    db.flush()
    return {"ok": True, "appointment": _inspection_payload(inspection, property_obj=prop)}


def build_inspection_timeline(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    limit: int = 100,
) -> dict[str, Any]:
    property_obj = _maybe_get_property(db, org_id=org_id, property_id=property_id)
    inspections = db.scalars(
        select(Inspection)
        .where(
            Inspection.org_id == int(org_id),
            Inspection.property_id == int(property_id),
        )
        .order_by(
            desc(getattr(Inspection, "scheduled_for", Inspection.inspection_date)),
            desc(Inspection.inspection_date),
            desc(Inspection.id),
        )
        .limit(int(limit))
    ).all()

    rows = []
    for inspection in inspections:
        rows.append(
            {
                "appointment": _inspection_payload(inspection, property_obj=property_obj),
                "events": wf.inspection_timeline(
                    db,
                    org_id=int(org_id),
                    property_id=int(property_id),
                    inspection_id=int(inspection.id),
                    limit=limit,
                ),
            }
        )
    return {
        "ok": True,
        "property_id": int(property_id),
        "property": (
            {
                "id": int(getattr(property_obj, "id", 0) or 0),
                "address": getattr(property_obj, "address", None),
                "city": getattr(property_obj, "city", None),
                "state": getattr(property_obj, "state", None),
                "zip": getattr(property_obj, "zip", None),
            }
            if property_obj is not None
            else None
        ),
        "count": len(rows),
        "rows": rows,
    }


def build_property_schedule_summary(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    property_obj = _maybe_get_property(db, org_id=org_id, property_id=property_id)
    rows = db.scalars(
        select(Inspection)
        .where(
            Inspection.org_id == int(org_id),
            Inspection.property_id == int(property_id),
        )
        .order_by(desc(getattr(Inspection, "scheduled_for", Inspection.inspection_date)), desc(Inspection.id))
    ).all()

    next_appointment = None
    last_completed = None
    scheduled_count = 0
    completed_count = 0
    canceled_count = 0

    for row in rows:
        payload = _inspection_payload(row, property_obj=property_obj)
        status = str(payload.get("status") or "").lower()
        scheduled_for = payload.get("scheduled_for")
        if status in {"draft", "scheduled", "confirmed"}:
            scheduled_count += 1
            if next_appointment is None and scheduled_for is not None and scheduled_for >= _now():
                next_appointment = payload
        if status in {"completed", "failed", "passed"}:
            completed_count += 1
            if last_completed is None:
                last_completed = payload
        if status == "canceled":
            canceled_count += 1

    due_reminders = list_due_inspection_reminders(db, org_id=org_id, property_id=property_id)
    return {
        "ok": True,
        "property_id": int(property_id),
        "property": (
            {
                "id": int(getattr(property_obj, "id", 0) or 0),
                "address": getattr(property_obj, "address", None),
                "city": getattr(property_obj, "city", None),
                "state": getattr(property_obj, "state", None),
                "zip": getattr(property_obj, "zip", None),
            }
            if property_obj is not None
            else None
        ),
        "counts": {
            "total_inspections": len(rows),
            "scheduled": scheduled_count,
            "completed": completed_count,
            "canceled": canceled_count,
            "due_reminders": len(due_reminders),
        },
        "next_appointment": next_appointment,
        "last_completed": last_completed,
        "due_reminders": due_reminders,
    }


def list_due_inspection_reminders(
    db: Session,
    *,
    org_id: int,
    before: datetime | None = None,
    property_id: int | None = None,
) -> list[dict[str, Any]]:
    cutoff = before or _now()
    stmt = (
        select(Inspection)
        .where(Inspection.org_id == int(org_id))
        .order_by(desc(getattr(Inspection, "scheduled_for", Inspection.inspection_date)), desc(Inspection.id))
    )
    if property_id is not None:
        stmt = stmt.where(Inspection.property_id == int(property_id))

    rows = db.scalars(stmt).all()
    out: list[dict[str, Any]] = []
    for inspection in rows:
        status = _normalize_status(getattr(inspection, "status", None))
        if status not in {"draft", "scheduled", "confirmed"}:
            continue

        scheduled_for = getattr(inspection, "scheduled_for", None) or getattr(inspection, "inspection_date", None)
        if scheduled_for is None:
            continue

        offsets = _normalize_offsets(_safe_loads(getattr(inspection, "reminder_offsets_json", None), [1440, 120, 30]))
        sent_offsets = set(_normalize_offsets(_safe_loads(getattr(inspection, "reminder_sent_offsets_json", None), [])))
        for offset in offsets:
            send_at = scheduled_for - timedelta(minutes=int(offset))
            if send_at <= cutoff and offset not in sent_offsets:
                prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
                payload = _inspection_payload(inspection, property_obj=prop)
                out.append(
                    {
                        "inspection_id": int(inspection.id),
                        "property_id": int(getattr(inspection, "property_id", 0) or 0),
                        "status": status,
                        "scheduled_for": scheduled_for,
                        "reminder_offset_minutes": int(offset),
                        "reminder_due_at": send_at,
                        "appointment": payload,
                    }
                )

    out.sort(key=lambda row: (row["reminder_due_at"], row["scheduled_for"], row["inspection_id"]))
    return out


def send_inspection_reminder(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int | None,
    inspection_id: int,
    reminder_offset_minutes: int | None = None,
) -> dict[str, Any]:
    inspection = _must_get_inspection(db, org_id=org_id, inspection_id=inspection_id)
    prop = _maybe_get_property(db, org_id=org_id, property_id=getattr(inspection, "property_id", None))
    scheduled_for = getattr(inspection, "scheduled_for", None) or getattr(inspection, "inspection_date", None)
    if scheduled_for is None:
        raise ValueError("inspection has no scheduled datetime")

    due_rows = list_due_inspection_reminders(
        db,
        org_id=org_id,
        before=_now(),
        property_id=getattr(inspection, "property_id", None),
    )
    due_for_inspection = [row for row in due_rows if int(row["inspection_id"]) == int(inspection_id)]
    if reminder_offset_minutes is None:
        reminder_offset_minutes = int(due_for_inspection[0]["reminder_offset_minutes"]) if due_for_inspection else 0

    sent_offsets = _normalize_offsets(_safe_loads(getattr(inspection, "reminder_sent_offsets_json", None), []))
    if int(reminder_offset_minutes) not in sent_offsets:
        sent_offsets.append(int(reminder_offset_minutes))
    sent_offsets = _normalize_offsets(sent_offsets)
    _set_if_present(inspection, "reminder_sent_offsets_json", _j(sent_offsets))
    _set_if_present(inspection, "last_reminder_sent_at", _now())
    _mark_updated(inspection)
    db.add(inspection)

    contact_payload = build_inspector_contact_payload(
        db,
        org_id=org_id,
        inspection_id=int(inspection.id),
    )
    reminder_message = build_inspection_reminder_message(
        db,
        org_id=org_id,
        inspection_id=int(inspection.id),
        reminder_offset_minutes=int(reminder_offset_minutes),
    )

    wf.emit_reminder_event(
        db,
        org_id=int(org_id),
        property_id=int(getattr(inspection, "property_id", 0) or 0),
        actor_user_id=actor_user_id,
        inspection_id=int(inspection.id),
        reminder_type="inspection_upcoming",
        payload={
            "scheduled_for": scheduled_for,
            "reminder_offset_minutes": int(reminder_offset_minutes),
            "contact_payload": contact_payload,
        },
    )
    db.flush()
    return {
        "ok": True,
        "inspection_id": int(inspection.id),
        "property_id": int(getattr(inspection, "property_id", 0) or 0),
        "scheduled_for": scheduled_for,
        "reminder_offset_minutes": int(reminder_offset_minutes),
        "contact_payload": contact_payload,
        "message_payload": reminder_message,
        "appointment": _inspection_payload(inspection, property_obj=prop),
    }
