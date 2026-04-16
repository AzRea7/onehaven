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


from app.services.jurisdiction_rules_service import assertion_governance_summary, governed_assertions_for_scope, is_assertion_governed_active


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows
    def filter(self, *args, **kwargs):
        return self
    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows
    def query(self, model):
        return _FakeQuery(self.rows)


def _assertion(**kwargs):
    base = dict(
        id=1,
        org_id=None,
        state="MI",
        county="wayne",
        city="detroit",
        pha_name=None,
        rule_key="inspection_program_exists",
        normalized_category="inspection",
        rule_category="inspection",
        governance_state="active",
        rule_status="active",
        review_status="verified",
        coverage_status="verified",
        validation_state="validated",
        trust_state="trusted",
        is_current=True,
        replaced_by_assertion_id=None,
        superseded_by_assertion_id=None,
        citation_json="{}",
        rule_provenance_json="{}",
        confidence_basis="",
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_replaced_rule_not_projectable():
    row = _assertion(replaced_by_assertion_id=10)
    summary = assertion_governance_summary(row)
    assert summary["safe_for_projection"] is False
    assert "replaced" in summary["lifecycle_blockers"]


def test_manual_review_conflict_not_projectable():
    row = _assertion(validation_state="conflicting", review_status="needs_manual_review", coverage_status="conflicting")
    assert is_assertion_governed_active(row) is False
    summary = assertion_governance_summary(row)
    assert "manual_review_required" in summary["lifecycle_blockers"]


def test_governed_assertions_for_scope_only_counts_active_safe_truth():
    rows = [
        _assertion(id=1, rule_key="inspection_program_exists"),
        _assertion(id=2, rule_key="inspection_program_exists", governance_state="approved", rule_status="approved", review_status="approved", coverage_status="approved", trust_state="validated", is_current=False),
        _assertion(id=3, rule_key="inspection_program_exists", governance_state="active", rule_status="active", review_status="needs_manual_review", coverage_status="conflicting", validation_state="conflicting", trust_state="needs_review", is_current=True),
        _assertion(id=4, rule_key="inspection_program_exists", governance_state="replaced", rule_status="superseded", coverage_status="superseded", is_current=False, superseded_by_assertion_id=1),
    ]
    db = _FakeSession(rows)
    result = governed_assertions_for_scope(db, org_id=None, state="MI", county="wayne", city="detroit", pha_name=None)
    assert result["safe_count"] == 1
    assert result["partial_count"] == 1
    assert result["excluded_count"] == 2
    assert result["manual_review_count"] == 1
    assert result["safe_assertion_ids"] == [1]
