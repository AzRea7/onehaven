from __future__ import annotations

from datetime import datetime, timedelta

from products.compliance.backend.src.services.inspection_scheduling_service import (
    list_due_inspection_reminders,
    schedule_inspection_appointment,
    send_inspection_reminder,
)


class _NoopWorkflow:
    def emit_inspection_event(self, *args, **kwargs):
        return None

    def emit_reminder_event(self, *args, **kwargs):
        return None

    def inspection_timeline(self, *args, **kwargs):
        return []


def test_list_due_reminders_filters_by_property_and_status(step19_seed, db_session, monkeypatch):
    monkeypatch.setattr("products.compliance.backend.src.services.inspection_scheduling_service.wf", _NoopWorkflow())

    org = step19_seed["org"]
    inspection_a = step19_seed["inspection_a"]
    inspection_b = step19_seed["inspection_b"]
    property_a = step19_seed["property_a"]
    property_b = step19_seed["property_b"]

    soon = datetime.utcnow() + timedelta(minutes=20)
    later = datetime.utcnow() + timedelta(days=1)

    schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=11,
        inspection_id=inspection_a.id,
        scheduled_for=soon,
        inspector_name="Reminder A",
        reminder_offsets=[60, 15],
        status="scheduled",
    )
    schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=11,
        inspection_id=inspection_b.id,
        scheduled_for=later,
        inspector_name="Reminder B",
        reminder_offsets=[60],
        status="scheduled",
    )

    due_for_a = list_due_inspection_reminders(
        db_session,
        org_id=org.id,
        before=datetime.utcnow(),
        property_id=property_a.id,
    )
    due_for_b = list_due_inspection_reminders(
        db_session,
        org_id=org.id,
        before=datetime.utcnow(),
        property_id=property_b.id,
    )

    assert len(due_for_a) >= 1
    assert all(row["property_id"] == property_a.id for row in due_for_a)
    assert due_for_b == []


def test_send_inspection_reminder_marks_offset_as_sent(step19_seed, db_session, monkeypatch):
    monkeypatch.setattr("products.compliance.backend.src.services.inspection_scheduling_service.wf", _NoopWorkflow())

    org = step19_seed["org"]
    inspection = step19_seed["inspection_b"]

    scheduled_for = datetime.utcnow() + timedelta(minutes=25)
    schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=44,
        inspection_id=inspection.id,
        scheduled_for=scheduled_for,
        inspector_name="Dana Reminder",
        inspector_email="dana@example.com",
        inspector_phone="555-888-9999",
        reminder_offsets=[60, 30, 10],
        status="confirmed",
    )

    result = send_inspection_reminder(
        db_session,
        org_id=org.id,
        actor_user_id=44,
        inspection_id=inspection.id,
        reminder_offset_minutes=30,
    )

    assert result["ok"] is True
    assert result["inspection_id"] == inspection.id
    assert result["reminder_offset_minutes"] == 30
    assert result["contact_payload"]["email"]["to"] == "dana@example.com"
    assert "Reminder:" in result["message_payload"]["email"]["subject"]
    assert 30 in result["appointment"]["reminder_sent_offsets"]
