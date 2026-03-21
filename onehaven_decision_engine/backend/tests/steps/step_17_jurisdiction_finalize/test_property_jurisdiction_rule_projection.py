from __future__ import annotations

import json
from types import SimpleNamespace

from app.services import policy_projection_service as svc


def test_build_property_compliance_brief_exposes_category_coverage(monkeypatch):
    monkeypatch.setattr(
        svc,
        "_query_inherited_assertions",
        lambda db, org_id, state, county, city, pha_name, statuses=None: [],
    )
    monkeypatch.setattr(
        svc,
        "build_policy_summary",
        lambda db, assertions, org_id, state, county, city, pha_name: {
            "coverage": {
                "coverage_status": "verified_extended",
                "production_readiness": "partial",
                "confidence_label": "medium",
            },
            "verified_rules": [
                {"rule_key": "rental_registration_required"},
                {"rule_key": "inspection_program_exists"},
            ],
            "required_actions": [
                {"code": "REGISTER_RENTAL", "title": "Register rental property"}
            ],
            "blocking_items": [
                {"code": "CERT_REQUIRED", "title": "Certificate required before occupancy"}
            ],
            "evidence_links": [],
            "local_rule_statuses": {
                "rental_registration_required": "yes",
                "inspection_program_exists": "yes",
                "certificate_required_before_occupancy": "conditional",
            },
            "verified_rule_count_local": 2,
            "verified_rule_count_effective": 2,
            "required_categories": [
                "rental_registration",
                "inspection",
                "certificate_of_occupancy",
            ],
            "category_coverage": {
                "rental_registration": "verified",
                "inspection": "verified",
                "certificate_of_occupancy": "conditional",
            },
        },
    )

    out = svc.build_property_compliance_brief(
        db=None,
        org_id=1,
        state="MI",
        county="macomb",
        city="warren",
        pha_name=None,
    )

    assert out["ok"] is True
    assert out["coverage"]["coverage_status"] == "verified_extended"
    assert out["local_rule_statuses"]["certificate_required_before_occupancy"] == "conditional"
    assert out["required_categories"] == [
        "rental_registration",
        "inspection",
        "certificate_of_occupancy",
    ]
    assert out["category_coverage"]["inspection"] == "verified"
    assert out["category_coverage"]["certificate_of_occupancy"] == "conditional"


def test_project_verified_assertions_to_profile_persists_completeness_metadata(monkeypatch):
    row = SimpleNamespace(
        id=700,
        org_id=None,
        state="MI",
        county="macomb",
        city="warren",
        friction_multiplier=1.0,
        pha_name=None,
        policy_json=None,
        notes=None,
        updated_at=None,
        completeness_status=None,
        completeness_score=None,
        stale_status=None,
        required_categories_json=None,
        category_coverage_json=None,
    )

    class FakeQuery:
        def filter(self, *args, **kwargs):
            return self

        def first(self):
            return row

    class FakeDB:
        def query(self, model):
            return FakeQuery()

        def commit(self):
            pass

        def refresh(self, obj):
            pass

    monkeypatch.setattr(
        svc,
        "_query_inherited_assertions",
        lambda db, org_id, state, county, city, pha_name, statuses=None: [],
    )
    monkeypatch.setattr(
        svc,
        "build_policy_summary",
        lambda db, assertions, org_id, state, county, city, pha_name: {
            "coverage": {
                "coverage_status": "verified_extended",
                "production_readiness": "partial",
                "confidence_label": "medium",
            },
            "verified_rules": [],
            "required_actions": [],
            "blocking_items": [],
            "evidence_links": [],
            "local_rule_statuses": {
                "rental_registration_required": "yes",
                "inspection_program_exists": "yes",
                "certificate_required_before_occupancy": "conditional",
            },
            "verified_rule_count_local": 2,
            "verified_rule_count_effective": 2,
            "required_categories": [
                "rental_registration",
                "inspection",
                "certificate_of_occupancy",
            ],
            "category_coverage": {
                "rental_registration": "verified",
                "inspection": "verified",
                "certificate_of_occupancy": "conditional",
            },
            "completeness_status": "partial",
            "completeness_score": 0.83,
            "stale_status": "fresh",
        },
    )

    out = svc.project_verified_assertions_to_profile(
        FakeDB(),
        org_id=None,
        state="MI",
        county="macomb",
        city="warren",
        pha_name=None,
        notes="Projected for test.",
    )

    policy = json.loads(out.policy_json)
    assert policy["completeness_status"] == "partial"
    assert policy["stale_status"] == "fresh"
    assert row.completeness_status == "partial"
    assert row.completeness_score == 0.83
    assert row.stale_status == "fresh"