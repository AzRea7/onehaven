# backend/tests/routers/test_compliance_phase7_router.py
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers import compliance as compliance_router


@pytest.fixture()
def app_client(monkeypatch: pytest.MonkeyPatch):
    app = FastAPI()
    app.include_router(compliance_router.router)

    fake_principal = SimpleNamespace(org_id=101, user_id=7)

    def fake_get_db():
        yield object()

    app.dependency_overrides[compliance_router.get_db] = fake_get_db
    app.dependency_overrides[compliance_router.get_principal] = lambda: fake_principal
    app.dependency_overrides[compliance_router.require_owner] = lambda: fake_principal

    monkeypatch.setattr(
        compliance_router,
        "_must_get_property",
        lambda db, org_id, property_id: SimpleNamespace(
            id=property_id,
            org_id=org_id,
            address="123 Main St",
            city="Detroit",
            county="Wayne",
            state="MI",
            property_type="single_family",
            program_type="section8",
            current_stage="under_contract",
            current_pane="acquisition",
        ),
    )
    monkeypatch.setattr(
        compliance_router,
        "build_property_compliance_brief",
        lambda db, org_id, state, county, city, pha_name, property_id, property=None: {
            "coverage": {
                "completeness_status": "partial",
                "coverage_confidence": "medium",
                "production_readiness": "warning",
                "is_stale": False,
                "required_categories": ["registration", "inspection"],
                "covered_categories": ["inspection"],
                "missing_categories": ["registration"],
                "resolved_rule_version": "v2026.04.11",
            },
            "required_actions": [{"title": "Complete city registration"}],
            "blocking_items": [{"rule_key": "rental_registration_required", "title": "Registration missing"}],
            "verified_rules": [{"rule_key": "inspection_required", "status": "active"}],
            "projection": {
                "projection_status": "blocked",
                "blocking_count": 1,
                "unknown_count": 0,
                "stale_count": 0,
                "conflicting_count": 0,
                "evidence_gap_count": 1,
                "confirmed_count": 2,
                "inferred_count": 0,
                "failing_count": 1,
                "readiness_score": 61.0,
                "projected_compliance_cost": 1200.0,
                "projected_days_to_rent": 7,
                "confidence_score": 0.73,
                "rules_version": "v2026.04.11",
                "impacted_rules": [{"rule_key": "rental_registration_required"}],
                "unresolved_evidence_gaps": [{"rule_key": "rental_registration_required", "gap": "No registration proof"}],
                "last_projected_at": "2026-04-11T12:00:00Z",
            },
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "build_workflow_summary",
        lambda db, org_id, property_id, principal=None, recompute=False: {
            "current_stage": "under_contract",
            "current_stage_label": "Under Contract",
            "current_pane": "acquisition",
            "current_pane_label": "Acquisition",
            "next_actions": ["Resolve city registration"],
            "primary_action": {"title": "Resolve compliance blockers", "pane": "compliance"},
            "compliance_projection": {
                "projection_status": "blocked",
                "blocking_count": 1,
                "stale_count": 0,
                "confidence_score": 0.73,
                "rules_version": "v2026.04.11",
                "last_projected_at": "2026-04-11T12:00:00Z",
            },
            "compliance_gate": {
                "ok": False,
                "severity": "high",
                "status": "blocked",
                "blocked_reason": "Pre-close compliance blocker(s) remain unresolved.",
                "warning_reason": None,
                "warnings": ["1 blocking compliance requirement(s) remain unresolved."],
                "blockers": [{"rule_key": "rental_registration_required", "title": "Registration missing"}],
                "blocking_count": 1,
                "unknown_count": 0,
                "stale_count": 0,
                "conflicting_count": 0,
                "readiness_score": 61.0,
                "confidence_score": 0.73,
                "projected_compliance_cost": 1200.0,
                "projected_days_to_rent": 7,
            },
            "pre_close_risk": {
                "active": True,
                "status": "blocked",
                "severity": "high",
                "blocking": True,
                "warnings": ["1 blocking compliance requirement(s) remain unresolved."],
                "summary": "Pre-close compliance blocker(s) remain unresolved.",
                "projected_compliance_cost": 1200.0,
                "projected_days_to_rent": 7,
            },
            "post_close_recheck": {
                "active": False,
                "status": "not_applicable",
                "needed": False,
                "reason": None,
            },
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "build_property_document_stack_snapshot",
        lambda db, org_id, property_id: {
            "documents": [
                {
                    "id": 301,
                    "label": "Pass certificate",
                    "category": "pass_certificate",
                }
            ]
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "build_property_projection_snapshot",
        lambda db, org_id, property_id: {
            "projection": {
                "projection_status": "blocked",
                "blocking_count": 1,
                "readiness_score": 61.0,
                "confidence_score": 0.73,
            },
            "blockers": [{"rule_key": "rental_registration_required"}],
            "evidence": [{"id": 1, "rule_key": "rental_registration_required", "evidence_status": "missing"}],
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "rebuild_property_projection",
        lambda db, org_id, property_id, property=None: {
            "projection": {
                "projection_status": "warning",
                "blocking_count": 0,
                "readiness_score": 74.0,
                "confidence_score": 0.81,
            },
            "blockers": [],
            "evidence": [],
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "list_compliance_documents",
        lambda db, org_id, property_id: [
            SimpleNamespace(
                id=301,
                property_id=property_id,
                inspection_id=401,
                checklist_item_id=None,
                category="pass_certificate",
                label="Pass certificate",
                original_filename="pass.pdf",
                storage_key="docs/pass.pdf",
                content_type="application/pdf",
                size_bytes=2048,
                scan_status="clean",
                parse_status="parsed",
                extracted_text_preview="Inspection passed.",
                metadata_json="{}",
                parser_meta_json="{}",
                created_at=None,
                updated_at=None,
            )
        ],
    )
    monkeypatch.setattr(
        compliance_router,
        "build_impacted_property_notifications",
        lambda db, org_id, jurisdiction_slug, changed_rules=None, trigger_payload=None, limit=None: {
            "ok": True,
            "org_id": org_id,
            "jurisdiction_slug": jurisdiction_slug,
            "count": 1,
            "notifications": [
                {
                    "kind": "property_rule_change_impact",
                    "property_id": 55,
                    "jurisdiction_slug": jurisdiction_slug,
                }
            ],
        },
    )
    monkeypatch.setattr(
        compliance_router,
        "notify_impacted_properties_for_rule_change",
        lambda db, org_id, jurisdiction_slug, changed_rules=None, trigger_payload=None, limit=None: {
            "ok": True,
            "processed_count": 1,
            "created_count": 1,
            "jurisdiction_slug": jurisdiction_slug,
            "results": [{"recorded": True}],
        },
    )

    return TestClient(app)


def test_get_property_compliance_brief_endpoint(app_client: TestClient):
    response = app_client.get("/compliance/properties/55/brief")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["property"]["id"] == 55
    assert payload["brief"]["projection"]["projection_status"] == "blocked"
    assert payload["workflow"]["compliance_gate"]["status"] == "blocked"
    assert payload["documents"]["documents"][0]["label"] == "Pass certificate"


def test_get_property_compliance_projection_endpoint(app_client: TestClient):
    response = app_client.get("/compliance/properties/55/projection")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["property"]["address"] == "123 Main St"
    assert payload["projection"]["projection_status"] == "blocked"
    assert payload["blockers"][0]["rule_key"] == "rental_registration_required"


def test_get_property_compliance_workflow_endpoint(app_client: TestClient):
    response = app_client.get("/compliance/properties/55/workflow")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["workflow"]["current_stage"] == "under_contract"
    assert payload["workflow"]["pre_close_risk"]["blocking"] is True


def test_get_property_compliance_documents_endpoint(app_client: TestClient):
    response = app_client.get("/compliance/properties/55/documents")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["documents"][0]["category"] == "pass_certificate"
    assert payload["documents"][0]["parse_status"] == "parsed"


def test_preview_impacted_property_notifications_endpoint(app_client: TestClient):
    response = app_client.get("/compliance/notifications/impacted-properties?jurisdiction_slug=detroit-wayne")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["count"] == 1
    assert payload["notifications"][0]["kind"] == "property_rule_change_impact"


def test_create_impacted_property_notifications_endpoint(app_client: TestClient):
    response = app_client.post(
        "/compliance/notifications/impacted-properties",
        json={
            "jurisdiction_slug": "detroit-wayne",
            "changed_rules": [{"rule_key": "rental_registration_required"}],
            "trigger_payload": {"source_id": 91},
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["processed_count"] == 1
    assert payload["created_count"] == 1