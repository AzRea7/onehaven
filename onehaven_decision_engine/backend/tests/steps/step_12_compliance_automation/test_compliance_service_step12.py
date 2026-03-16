from __future__ import annotations

import json

from sqlalchemy import select

from app.models import (
    AuditEvent,
    Inspection,
    PropertyChecklistItem,
    RehabTask,
    WorkflowEvent,
)
from app.services import compliance_service


def _seed_checklist_item(
    db,
    *,
    org_id: int,
    property_id: int,
    item_code: str,
    category: str = "safety",
    description: str | None = None,
    status: str = "todo",
    is_completed: bool | None = None,
):
    row = PropertyChecklistItem(
        org_id=org_id,
        property_id=property_id,
        item_code=item_code,
        category=category,
        description=description or item_code.replace("_", " ").title(),
        severity=3,
        common_fail=True,
        status=status,
        is_completed=is_completed,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_build_property_inspection_readiness_returns_policy_driven_shape(monkeypatch, db, seed_org_user, seed_property):
    org = seed_org_user["org"]
    prop = seed_property

    _seed_checklist_item(
        db,
        org_id=org.id,
        property_id=prop.id,
        item_code="SMOKE_DETECTORS",
        status="done",
        is_completed=True,
    )
    _seed_checklist_item(
        db,
        org_id=org.id,
        property_id=prop.id,
        item_code="HEAT",
        status="failed",
        is_completed=False,
    )

    db.add(
        Inspection(
            org_id=org.id,
            property_id=prop.id,
            passed=False,
        )
    )
    db.commit()

    monkeypatch.setattr(
        compliance_service,
        "resolve_operational_policy",
        lambda *args, **kwargs: {
            "scope": "global",
            "match_level": "city",
            "profile_id": 100,
            "friction_multiplier": 1.3,
            "pha_name": None,
            "coverage": {
                "coverage_status": "verified_extended",
                "confidence_label": "high",
                "production_readiness": "ready",
            },
            "required_actions": [
                {
                    "code": "WARREN_RENTAL_LICENSE_REQUIRED",
                    "title": "Warren rental license required",
                    "severity": "fail",
                    "category": "licensing",
                    "blocks_local": True,
                    "blocks_voucher": True,
                    "blocks_lease_up": True,
                    "suggested_fix": "Complete Warren rental license process.",
                }
            ],
            "blocking_items": [
                {
                    "code": "WARREN_BIENNIAL_INSPECTION_REQUIRED",
                    "title": "Warren biennial rental inspection required",
                    "severity": "fail",
                    "category": "inspection",
                    "blocks_local": True,
                    "blocks_voucher": True,
                    "blocks_lease_up": True,
                    "suggested_fix": "Schedule and pass the municipal inspection.",
                }
            ],
            "rules": [
                {
                    "rule_key": "MI_SOURCE_OF_INCOME_PROTECTION",
                    "label": "Michigan SOI protections apply",
                    "severity": "warn",
                    "status": "warn",
                    "category": "fair_housing",
                    "suggested_fix": "Review tenant screening workflow.",
                }
            ],
            "policy": {
                "compliance": {
                    "inspection_required": "yes",
                    "certificate_required_before_occupancy": "yes",
                    "local_agent_required": "yes",
                },
                "hqs_addenda": [
                    {
                        "code": "WARREN_PACKET_READY",
                        "description": "Municipal rental packet should be ready",
                        "category": "documents",
                        "severity": "warn",
                        "suggested_fix": "Prepare packet.",
                    }
                ],
            },
        },
    )

    monkeypatch.setattr(
        compliance_service,
        "build_property_compliance_brief",
        lambda *args, **kwargs: {
            "coverage": {
                "coverage_status": "verified_extended",
                "confidence_label": "high",
                "production_readiness": "ready",
            },
            "compliance": {
                "market_label": "Warren, Macomb County, MI",
            },
            "required_actions": [],
            "blocking_items": [],
            "evidence_links": [
                {"title": "City page", "url": "https://example.test/warren"}
            ],
        },
    )

    out = compliance_service.build_property_inspection_readiness(
        db,
        org_id=org.id,
        property_id=prop.id,
    )

    assert out["ok"] is True
    assert out["property"]["id"] == prop.id
    assert out["market"]["match_level"] == "city"
    assert "readiness" in out
    assert "counts" in out
    assert "results" in out
    assert "blocking_items" in out
    assert "recommended_actions" in out
    assert out["counts"]["total_rules"] >= 1
    assert out["counts"]["blocking"] >= 1
    assert out["overall_status"] in {"blocked", "attention", "ready"}

    result_keys = {r["rule_key"] for r in out["results"]}
    assert "SMOKE_DETECTORS" in result_keys
    assert "HEAT" in result_keys
    assert "LATEST_INSPECTION_PASSED" in result_keys
    assert "POLICY_CONFIDENCE_SUFFICIENT" in result_keys
    assert "WARREN_RENTAL_LICENSE_REQUIRED" in result_keys
    assert "WARREN_BIENNIAL_INSPECTION_REQUIRED" in result_keys


def test_generate_policy_tasks_for_property_creates_tasks_and_audit_rows(monkeypatch, db, seed_org_user, seed_property):
    org = seed_org_user["org"]
    prop = seed_property
    user = seed_org_user["user"]

    fake_readiness = {
        "ok": True,
        "overall_status": "blocked",
        "score_pct": 42.0,
        "readiness": {
            "hqs_ready": False,
            "local_ready": False,
            "voucher_ready": False,
            "lease_up_ready": False,
        },
        "blocking_items": [
            {
                "rule_key": "WARREN_RENTAL_LICENSE_REQUIRED",
                "label": "Warren rental license required",
                "status": "fail",
                "source": "jurisdiction_policy",
                "suggested_fix": "Complete Warren rental license process.",
            },
            {
                "rule_key": "HEAT",
                "label": "Permanent heat source is present and operational",
                "status": "fail",
                "source": "hqs_library",
                "suggested_fix": "Repair furnace.",
            },
        ],
        "warning_items": [
            {
                "rule_key": "LEAD_SAFE_SURFACES",
                "label": "Pre-1978 lead-safe review",
                "status": "warn",
                "source": "contextual_rule",
                "suggested_fix": "Review lead-safe workflow.",
            }
        ],
    }

    monkeypatch.setattr(
        compliance_service,
        "build_property_inspection_readiness",
        lambda *args, **kwargs: fake_readiness,
    )

    out = compliance_service.generate_policy_tasks_for_property(
        db,
        org_id=org.id,
        actor_user_id=user.id,
        property_id=prop.id,
    )
    db.commit()

    assert out["ok"] is True
    assert out["property_id"] == prop.id
    assert out["created"] == 3

    tasks = list(
        db.scalars(
            select(RehabTask).where(
                RehabTask.org_id == org.id,
                RehabTask.property_id == prop.id,
            )
        ).all()
    )
    task_titles = {t.title for t in tasks}

    assert "Compliance: Warren rental license required" in task_titles
    assert "Compliance: Permanent heat source is present and operational" in task_titles
    assert "Review: Pre-1978 lead-safe review" in task_titles

    workflow_events = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.org_id == org.id,
                WorkflowEvent.property_id == prop.id,
            )
        ).all()
    )
    assert any(e.event_type == "compliance.tasks.generated" for e in workflow_events)

    audit_rows = list(
        db.scalars(
            select(AuditEvent).where(
                AuditEvent.org_id == org.id,
                AuditEvent.entity_type == "property",
                AuditEvent.entity_id == str(prop.id),
            )
        ).all()
    )
    assert any(a.action == "compliance.tasks.generated" for a in audit_rows)


def test_run_hqs_returns_readiness_and_does_not_duplicate_existing_tasks(monkeypatch, db, seed_org_user, seed_property):
    org = seed_org_user["org"]
    prop = seed_property
    user = seed_org_user["user"]

    first_readiness = {
        "ok": True,
        "overall_status": "blocked",
        "score_pct": 50.0,
        "counts": {"total_rules": 5, "blocking": 1},
        "readiness": {
            "hqs_ready": False,
            "local_ready": False,
            "voucher_ready": False,
            "lease_up_ready": False,
        },
        "blocking_items": [
            {
                "rule_key": "HEAT",
                "label": "Permanent heat source is present and operational",
                "status": "fail",
                "source": "hqs_library",
                "suggested_fix": "Repair furnace.",
            }
        ],
        "warning_items": [],
        "run_summary": {
            "passed": 2,
            "failed": 1,
            "blocked": 1,
            "not_yet": 2,
            "score_pct": 50.0,
        },
    }

    monkeypatch.setattr(
        compliance_service,
        "build_property_inspection_readiness",
        lambda *args, **kwargs: first_readiness,
    )

    out1 = compliance_service.run_hqs(
        db,
        org_id=org.id,
        actor_user_id=user.id,
        property_id=prop.id,
        create_tasks=True,
    )
    db.commit()

    assert out1["ok"] is True
    assert out1["inspection_readiness"]["overall_status"] == "blocked"
    assert out1["task_generation"]["created"] == 1

    out2 = compliance_service.run_hqs(
        db,
        org_id=org.id,
        actor_user_id=user.id,
        property_id=prop.id,
        create_tasks=True,
    )
    db.commit()

    assert out2["ok"] is True
    assert out2["task_generation"]["created"] == 0

    tasks = list(
        db.scalars(
            select(RehabTask).where(
                RehabTask.org_id == org.id,
                RehabTask.property_id == prop.id,
            )
        ).all()
    )
    assert len(tasks) == 1

    workflow_events = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.org_id == org.id,
                WorkflowEvent.property_id == prop.id,
            )
        ).all()
    )
    assert any(e.event_type == "compliance.automation.run" for e in workflow_events)
    