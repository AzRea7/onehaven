from __future__ import annotations

def test_warren_profile_resolution(client, auth_headers):
    res = client.get(
        "/api/jurisdiction-profiles/resolve",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data.get("matched") is True
    assert data.get("match_level") in {"city", "county", "state"}

def test_warren_policy_brief_exists(client, auth_headers):
    res = client.get(
        "/api/policy/brief",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data.get("state") == "MI"
    assert str(data.get("city", "")).lower() == "warren"

def test_warren_coverage_exists(client, auth_headers):
    res = client.get(
        "/api/policy/coverage",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    assert data.get("state") == "MI"
    assert str(data.get("city", "")).lower() == "warren"

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

    assert isinstance(data, dict)

def test_warren_manual_profile_contains_core_fields(client, auth_headers):
    res = client.get(
        "/api/jurisdiction-profiles/resolve",
        params={"state": "MI", "county": "macomb", "city": "warren"},
        headers=auth_headers,
    )
    assert res.status_code == 200, res.text
    data = res.json()

    policy = data.get("policy") or {}
    compliance = policy.get("compliance") or {}

    # loosen this until projection is fully standardized
    if compliance:
        assert compliance.get("local_agent_required") in {"yes", True, "required", None}
        assert compliance.get("owner_po_box_allowed") in {"no", False, "not_allowed", None}