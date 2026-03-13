from __future__ import annotations

from types import SimpleNamespace

from app.services import jurisdiction_profile_service as svc


class DummyDB:
    pass


def test_resolve_operational_policy_merges_profile_and_projection(monkeypatch):
    monkeypatch.setattr(
        svc,
        "resolve_profile",
        lambda db, org_id, city, county, state: {
            "matched": True,
            "scope": "org",
            "match_level": "city",
            "friction_multiplier": 1.2,
            "pha_name": None,
            "policy": {
                "rules": [
                    {"code": "LOCAL_AGENT_50_MILES"},
                    {"code": "NO_PO_BOX"},
                ],
                "required_actions": [
                    {"code": "CITY_DEBT_CLEARANCE", "title": "Provide proof no city fees are owed"}
                ],
                "blocking_items": [
                    {"code": "NO_PO_BOX", "title": "Owner legal address cannot be a P.O. Box"}
                ],
            },
            "rules": [
                {"code": "LOCAL_AGENT_50_MILES"},
                {"code": "NO_PO_BOX"},
            ],
            "notes": "Warren special rules",
            "profile_id": 501,
        },
    )

    monkeypatch.setitem(
        __import__("sys").modules,
        "app.services.policy_projection_service",
        SimpleNamespace(
            build_property_compliance_brief=lambda db, org_id, state, county, city, pha_name: {
                "coverage": {"coverage_status": "verified"},
                "compliance": {"summary": "ok"},
                "blocking_items": [
                    {"code": "NO_PO_BOX", "title": "Owner legal address cannot be a P.O. Box"}
                ],
                "required_actions": [
                    {"code": "SOURCE_OF_INCOME", "title": "Do not discriminate based on lawful source of income"}
                ],
                "evidence_links": [{"url": "https://example.test/evidence"}],
            }
        ),
    )

    out = svc.resolve_operational_policy(
        DummyDB(),
        org_id=42,
        city="Warren",
        county="Macomb",
        state="MI",
    )

    assert out["profile_id"] == 501
    assert out["scope"] == "org"
    assert len(out["rules"]) == 2

    req_codes = {x.get("code") for x in out["required_actions"]}
    block_codes = {x.get("code") for x in out["blocking_items"]}

    assert "CITY_DEBT_CLEARANCE" in req_codes
    assert "SOURCE_OF_INCOME" in req_codes
    assert "NO_PO_BOX" in block_codes