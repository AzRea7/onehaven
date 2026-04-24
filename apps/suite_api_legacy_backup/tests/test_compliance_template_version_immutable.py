from __future__ import annotations

from datetime import date

from app.models import Organization, Property
from app.policy_models import HqsAddendumRule, HqsRule
from products.compliance.backend.src.domain.compliance.hqs_library import get_effective_hqs_items


def _seed(db):
    org = Organization(slug="step12-hqs-org", name="Step12 HQS Org")
    db.add(org)
    db.commit()
    db.refresh(org)

    prop = Property(
        org_id=org.id,
        address="789 Library Ln",
        city="Warren",
        state="MI",
        zip="48091",
        county="Macomb",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1100,
        year_built=1962,
        has_garage=False,
        property_type="single_family",
    )
    db.add(prop)
    db.commit()
    db.refresh(prop)
    return org, prop


def test_hqs_library_returns_baseline_plus_contextual_items(db_session):
    org, prop = _seed(db_session)

    result = get_effective_hqs_items(
        db_session,
        org_id=org.id,
        prop=prop,
        profile_summary={},
    )

    assert "items" in result
    assert "sources" in result
    assert "counts" in result

    items = result["items"]
    codes = {row["code"] for row in items}

    assert len(items) > 0
    assert "SMOKE_DETECTORS" in codes
    assert "LEAD_SAFE_SURFACES" in codes  # contextual because pre-1978


def test_hqs_library_applies_rule_and_addendum_overrides(db_session):
    org, prop = _seed(db_session)

    db_session.add(
        HqsRule(
            code="SMOKE_DETECTORS",
            category="life_safety",
            severity="critical",
            description="Enhanced smoke detector requirement",
            effective_date=date(2026, 1, 1),
        )
    )
    db_session.add(
        HqsAddendumRule(
            org_id=org.id,
            jurisdiction_profile_id=1,  # safe enough for tests if FK not enforced in sqlite memory
            code="LOCAL_AGENT_DOCUMENTATION",
            category="documents",
            severity="warn",
            description="Provide local agent documentation",
            effective_date=date(2026, 1, 1),
        )
    )
    db_session.commit()

    result = get_effective_hqs_items(
        db_session,
        org_id=org.id,
        prop=prop,
        profile_summary={},
    )

    items = {row["code"]: row for row in result["items"]}

    assert items["SMOKE_DETECTORS"]["category"] == "life_safety"
    assert items["SMOKE_DETECTORS"]["severity"] == "critical"
    assert "LOCAL_AGENT_DOCUMENTATION" in items


def test_hqs_library_uses_profile_policy_items(db_session):
    org, prop = _seed(db_session)

    profile_summary = {
        "policy": {
            "hqs_addenda": [
                {
                    "code": "WARREN_WINDOW_GLAZING",
                    "description": "All window glazing must be intact",
                    "category": "exterior",
                    "severity": "fail",
                    "suggested_fix": "Replace cracked panes.",
                }
            ],
            "compliance": {
                "inspection_required": "yes",
                "certificate_required_before_occupancy": "yes",
                "local_agent_required": "yes",
            },
        }
    }

    result = get_effective_hqs_items(
        db_session,
        org_id=org.id,
        prop=prop,
        profile_summary=profile_summary,
    )

    codes = {row["code"] for row in result["items"]}

    assert "WARREN_WINDOW_GLAZING" in codes
    assert "LOCAL_INSPECTION_REQUIRED" in codes
    assert "LOCAL_CERTIFICATE_BEFORE_OCCUPANCY" in codes
    assert "LOCAL_AGENT_DOCUMENTATION" in codes
    