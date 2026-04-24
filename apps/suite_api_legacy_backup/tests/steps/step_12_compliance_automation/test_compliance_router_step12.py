from __future__ import annotations

from datetime import datetime

from fastapi.testclient import TestClient

from app.models import (
    Inspection,
    Organization,
    Property,
    PropertyChecklistItem,
    PropertyState,
)
from app.services.property_state_machine import sync_property_state


def _seed_property(db):
    org = Organization(slug="step12-router-org", name="Step12 Router Org")
    db.add(org)
    db.commit()
    db.refresh(org)

    prop = Property(
        org_id=org.id,
        address="123 Compliance Ave",
        city="Warren",
        state="MI",
        zip="48091",
        county="Macomb",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1958,
        has_garage=True,
        property_type="single_family",
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)

    db.add(
        PropertyState(
            org_id=org.id,
            property_id=prop.id,
            current_stage="compliance",
            constraints_json="{}",
            outstanding_tasks_json="{}",
            updated_at=datetime.utcnow(),
        )
    )
    db.commit()
    return org, prop


def _seed_checklist_rows(db, *, org_id: int, property_id: int):
    rows = [
        PropertyChecklistItem(
            org_id=org_id,
            property_id=property_id,
            item_code="SMOKE_DETECTORS",
            category="safety",
            description="Smoke detectors present",
            severity=3,
            common_fail=True,
            status="done",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        ),
        PropertyChecklistItem(
            org_id=org_id,
            property_id=property_id,
            item_code="GFCI_KITCHEN",
            category="electrical",
            description="Kitchen GFCI present",
            severity=3,
            common_fail=True,
            status="failed",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        ),
        PropertyChecklistItem(
            org_id=org_id,
            property_id=property_id,
            item_code="LOCAL_INSPECTION_REQUIRED",
            category="jurisdiction",
            description="Local inspection required",
            severity=3,
            common_fail=True,
            status="blocked",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        ),
    ]
    db.add_all(rows)
    db.commit()


def _seed_inspection(db, *, org_id: int, property_id: int, passed: bool = False):
    insp = Inspection(
        org_id=org_id,
        property_id=property_id,
        inspection_date=datetime.utcnow(),
        passed=passed,
        reinspect_required=not passed,
        notes="seed inspection",
    )
    if hasattr(insp, "template_key"):
        insp.template_key = "hud_52580a"
    if hasattr(insp, "template_version"):
        insp.template_version = "hud_52580a_2019"
    if hasattr(insp, "result_status"):
        insp.result_status = "pass" if passed else "fail"
    if hasattr(insp, "readiness_status"):
        insp.readiness_status = "ready" if passed else "needs_work"
    if hasattr(insp, "readiness_score"):
        insp.readiness_score = 92.0 if passed else 41.0
    db.add(insp)
    db.commit()
    db.refresh(insp)
    return insp


def test_compliance_status_returns_real_readiness_payload(
    client: TestClient,
    db_session,
    auth_headers,
):
    org, prop = _seed_property(db_session)
    _seed_checklist_rows(db_session, org_id=org.id, property_id=prop.id)
    _seed_inspection(db_session, org_id=org.id, property_id=prop.id, passed=False)

    resp = client.get(f"/compliance/status/{prop.id}", headers=auth_headers(org.id))
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["property_id"] == prop.id
    assert "overall_status" in body
    assert "score_pct" in body
    assert "readiness" in body
    assert "counts" in body
    assert "blocking_items" in body
    assert "recommended_actions" in body

    assert isinstance(body["blocking_items"], list)
    assert isinstance(body["recommended_actions"], list)
    assert body["readiness"]["hqs_ready"] is False


def test_property_inspection_readiness_endpoint_returns_new_shape(
    client: TestClient,
    db_session,
    auth_headers,
):
    org, prop = _seed_property(db_session)
    _seed_checklist_rows(db_session, org_id=org.id, property_id=prop.id)
    _seed_inspection(db_session, org_id=org.id, property_id=prop.id, passed=False)

    resp = client.get(
        f"/compliance/property/{prop.id}/inspection-readiness",
        headers=auth_headers(org.id),
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["ok"] is True
    assert body["property"]["id"] == prop.id
    assert "score_pct" in body
    assert "completion_pct" in body
    assert "completion_projection_pct" in body
    assert "posture" in body
    assert "readiness_summary" in body
    assert "inspection_failure_actions" in body
    assert isinstance(body["results"], list)
    assert isinstance(body["recommended_actions"], list)


def test_run_property_compliance_automation_returns_summary_and_workflow(
    client: TestClient,
    db_session,
    auth_headers,
):
    org, prop = _seed_property(db_session)
    _seed_checklist_rows(db_session, org_id=org.id, property_id=prop.id)
    _seed_inspection(db_session, org_id=org.id, property_id=prop.id, passed=False)
    sync_property_state(db_session, org_id=org.id, property_id=prop.id)
    db_session.commit()

    resp = client.post(
        f"/compliance/property/{prop.id}/automation/run?create_tasks=true",
        headers=auth_headers(org.id),
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["ok"] is True
    assert body["property_id"] == prop.id
    assert "inspection_readiness" in body
    assert "task_generation" in body
    assert "readiness_summary" in body
    assert "workflow" in body

    workflow = body["workflow"]
    assert workflow["property_id"] == prop.id
    assert "current_stage" in workflow
    assert "gate_status" in workflow


def test_checklist_generation_persists_template_backed_items(
    client: TestClient,
    db_session,
    auth_headers,
):
    org, prop = _seed_property(db_session)

    resp = client.post(
        f"/compliance/checklist/{prop.id}?persist=true&include_policy=true",
        headers=auth_headers(org.id),
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["property_id"] == prop.id
    assert isinstance(body["items"], list)
    assert len(body["items"]) > 0

    codes = {row["item_code"] for row in body["items"]}
    assert "SMOKE_DETECTORS" in codes or "SMOKE_CO" in codes
    