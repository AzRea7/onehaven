from __future__ import annotations

from types import SimpleNamespace

from app.routers import compliance as router_mod


class DummyDB:
    pass


def test_property_compliance_brief_returns_rich_jurisdiction_payload(monkeypatch):
    fake_prop = SimpleNamespace(
        id=101,
        org_id=7,
        address="123 Test Ave",
        city="Warren",
        county="Macomb",
        state="MI",
    )
    fake_principal = SimpleNamespace(org_id=7)

    monkeypatch.setattr(router_mod, "_must_get_property", lambda db, org_id, property_id: fake_prop)

    # This is the behavior Step 111 should expose through the brief route.
    monkeypatch.setattr(
        router_mod,
        "resolve_operational_policy",
        lambda db, org_id, city, county, state: {
            "matched": True,
            "scope": "org",
            "match_level": "city",
            "profile_id": 501,
            "required_actions": [
                {"code": "LOCAL_AGENT_50_MILES", "title": "Designate local agent within 50 miles"},
                {"code": "CITY_DEBT_CLEARANCE", "title": "Provide proof no city fees are owed"},
            ],
            "blocking_items": [
                {"code": "NO_PO_BOX", "title": "No P.O. box legal address"}
            ],
            "coverage": {"coverage_status": "verified", "confidence_label": "high"},
            "evidence_links": [{"url": "https://example.test/evidence"}],
            "pha_name": None,
        },
    )

    monkeypatch.setattr(
        router_mod,
        "build_property_compliance_brief",
        lambda db, org_id, state, county, city, pha_name: {
            "coverage": {"coverage_status": "verified", "confidence_label": "high"},
            "compliance": {"summary": "jurisdiction aware"},
            "blocking_items": [],
            "required_actions": [],
            "evidence_links": [{"url": "https://example.test/evidence"}],
        },
    )

    out = router_mod.property_compliance_brief(
        property_id=101,
        db=DummyDB(),
        p=fake_principal,
    )

    # This will fail until you upgrade the brief route to return the merged jurisdiction-aware payload.
    assert out["property_id"] == 101
    assert out["city"] == "Warren"
    assert out["county"] == "Macomb"
    assert out["state"] == "MI"
    assert out["jurisdiction"]["profile_id"] == 501
    assert len(out["required_actions"]) == 2
    assert len(out["blocking_items"]) == 1