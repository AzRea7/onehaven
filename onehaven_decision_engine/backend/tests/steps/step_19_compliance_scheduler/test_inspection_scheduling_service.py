from __future__ import annotations

from datetime import datetime, timedelta

from app.services.inspection_scheduling_service import (
    build_inspection_ics_payload,
    build_property_schedule_summary,
    cancel_inspection_appointment,
    mark_inspection_completed,
    schedule_inspection_appointment,
)


class _NoopWorkflow:
    def emit_inspection_event(self, *args, **kwargs):
        return None

    def emit_reminder_event(self, *args, **kwargs):
        return None

    def inspection_timeline(self, *args, **kwargs):
        return []


def test_schedule_inspection_appointment_persists_contact_and_offsets(step19_seed, db_session, monkeypatch):
    monkeypatch.setattr("app.services.inspection_scheduling_service.wf", _NoopWorkflow())

    org = step19_seed["org"]
    inspection = step19_seed["inspection_a"]
    prop = step19_seed["property_a"]
    scheduled_for = datetime.utcnow() + timedelta(days=1, hours=4)

    result = schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=77,
        inspection_id=inspection.id,
        scheduled_for=scheduled_for,
        inspector_name="Jane Inspector",
        inspector_company="Motor City HQS",
        inspector_email="jane@example.com",
        inspector_phone="555-222-3333",
        reminder_offsets=[15, 120, 15, 30],
        appointment_notes="Bring ladder for attic access.",
        status="scheduled",
        calendar_provider="ics",
    )

    appointment = result["appointment"]
    assert result["ok"] is True
    assert appointment["inspection_id"] == inspection.id
    assert appointment["property_id"] == prop.id
    assert appointment["status"] == "scheduled"
    assert appointment["inspector_name"] == "Jane Inspector"
    assert appointment["inspector_company"] == "Motor City HQS"
    assert appointment["inspector_email"] == "jane@example.com"
    assert appointment["calendar_provider"] == "ics"
    assert appointment["reminder_offsets"] == [120, 30, 15]
    assert appointment["inspection_date"] == scheduled_for
    assert result["contact_payload"]["email"]["to"] == "jane@example.com"


def test_build_ics_and_schedule_summary_return_expected_shape(step19_seed, db_session, monkeypatch):
    monkeypatch.setattr("app.services.inspection_scheduling_service.wf", _NoopWorkflow())

    org = step19_seed["org"]
    inspection = step19_seed["inspection_a"]
    prop = step19_seed["property_a"]
    scheduled_for = datetime.utcnow() + timedelta(days=1)

    schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=5,
        inspection_id=inspection.id,
        scheduled_for=scheduled_for,
        inspector_name="Pat Schedule",
        reminder_offsets=[60, 15],
        status="confirmed",
    )

    ics_payload = build_inspection_ics_payload(
        db_session,
        org_id=org.id,
        inspection_id=inspection.id,
        duration_minutes=75,
    )
    summary = build_property_schedule_summary(db_session, org_id=org.id, property_id=prop.id)

    assert ics_payload["inspection_id"] == inspection.id
    assert ics_payload["content_type"] == "text/calendar"
    assert "BEGIN:VCALENDAR" in ics_payload["ics"]
    assert "SUMMARY:Inspection - 101 Scheduler St" in ics_payload["ics"]

    assert summary["ok"] is True
    assert summary["property_id"] == prop.id
    assert summary["counts"]["scheduled"] >= 1
    assert summary["next_appointment"]["inspection_id"] == inspection.id


def test_cancel_and_complete_appointment_update_terminal_state(step19_seed, db_session, monkeypatch):
    monkeypatch.setattr("app.services.inspection_scheduling_service.wf", _NoopWorkflow())

    org = step19_seed["org"]
    inspection = step19_seed["inspection_a"]

    schedule_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=2,
        inspection_id=inspection.id,
        scheduled_for=datetime.utcnow() + timedelta(hours=8),
        inspector_name="Pat Schedule",
        status="scheduled",
    )

    canceled = cancel_inspection_appointment(
        db_session,
        org_id=org.id,
        actor_user_id=2,
        inspection_id=inspection.id,
        reason="Tenant requested a later time.",
    )
    assert canceled["ok"] is True
    assert canceled["appointment"]["status"] == "canceled"
    assert "Cancellation reason" in (canceled["appointment"]["appointment_notes"] or "")

    completed = mark_inspection_completed(
        db_session,
        org_id=org.id,
        actor_user_id=2,
        inspection_id=inspection.id,
        status="passed",
        passed=True,
        reinspect_required=False,
        notes="Passed after same-day repair verification.",
    )
    assert completed["ok"] is True
    assert completed["appointment"]["status"] == "passed"
    assert completed["appointment"]["passed"] is True
    assert completed["appointment"]["reinspect_required"] is False
