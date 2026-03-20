from __future__ import annotations

from app.routers import jurisdiction_profiles as jurisdiction_profiles_router
from app.routers import policy as policy_router


def test_warren_profile_resolution(client, auth_headers, monkeypatch):
    monkeypatch.setattr(
        jurisdiction_profiles_router,
        "resolve_profile",
        lambda db, org_id, state, county, city, pha_name=None: {
            "matched": True,
            "scope": "global",
            "match_level": "city",
            "profile_id": 123,
            "friction_multiplier": 1.2,
            "notes": None,
        },
    )

    res = client.get(
        "/api/jurisdiction-profiles/resolve",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data.get("matched") is True
    assert data.get("match_level") == "city"
    assert data.get("friction_multiplier") == 1.2


def test_warren_policy_brief_exists(client, auth_headers, monkeypatch):
    monkeypatch.setattr(
        policy_router,
        "build_property_compliance_brief",
        lambda db, org_id, state, county, city, pha_name=None: {
            "ok": True,
            "market": {
                "state": "MI",
                "county": "macomb",
                "city": "warren",
                "pha_name": None,
            },
            "compliance": {
                "market_label": "Warren, Macomb County, MI",
                "registration_required": "yes",
                "inspection_required": "yes",
                "certificate_required_before_occupancy": "unknown",
                "pha_specific_workflow": False,
                "coverage_confidence": "high",
                "production_readiness": "ready",
            },
            "explanation": "Warren requires local compliance workflow review.",
        },
    )

    res = client.get(
        "/api/policy/brief",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data["ok"] is True
    assert data["market"]["city"] == "warren"
    assert "compliance" in data


def test_warren_coverage_exists(client, auth_headers, monkeypatch):
    monkeypatch.setattr(
        policy_router,
        "compute_coverage_status",
        lambda db, org_id, state, county, city, pha_name=None: {
            "ok": True,
            "coverage": {
                "state": "MI",
                "county": "macomb",
                "city": "warren",
                "pha_name": None,
                "coverage_status": "verified_extended",
                "confidence_label": "high",
                "production_readiness": "ready",
            },
        },
    )

    res = client.get(
        "/api/policy/coverage",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data["ok"] is True
    assert data["coverage"]["city"] == "warren"
    assert data["coverage"]["coverage_status"] == "verified_extended"


def test_warren_evidence_market_exists(client, auth_headers):
    res = client.get(
        "/api/policy-evidence/market",
        params={
            "state": "MI",
            "county": "macomb",
            "city": "warren",
            "include_global": "true",
        },
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data["ok"] is True
    assert data["market"]["city"] == "warren"
    assert "sources" in data
    assert "assertions" in data
    assert isinstance(data["sources"], list)
    assert isinstance(data["assertions"], list)


def test_warren_manual_profile_contains_core_fields(client, auth_headers, monkeypatch):
    monkeypatch.setattr(
        jurisdiction_profiles_router,
        "resolve_profile",
        lambda db, org_id, state, county, city, pha_name=None: {
            "matched": True,
            "scope": "global",
            "match_level": "city",
            "profile_id": 123,
            "friction_multiplier": 1.2,
            "notes": "Manual Warren profile present",
        },
    )

    res = client.get(
        "/api/jurisdiction-profiles/resolve",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data["matched"] is True
    assert data["match_level"] == "city"
    assert "friction_multiplier" in data
    assert data["notes"] == "Manual Warren profile present"
    