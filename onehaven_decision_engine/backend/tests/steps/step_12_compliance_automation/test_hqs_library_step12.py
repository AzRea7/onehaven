from __future__ import annotations

from app.domain.compliance.hqs_library import get_effective_hqs_items


def test_effective_hqs_items_include_baseline_profile_and_contextual_rules(db, seed_org_user, seed_property):
    org = seed_org_user["org"]
    prop = seed_property

    profile_summary = {
        "scope": "global",
        "match_level": "city",
        "profile_id": 99,
        "friction_multiplier": 1.25,
        "pha_name": None,
        "coverage": {
            "confidence_label": "high",
            "production_readiness": "ready",
        },
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
                    "suggested_fix": "Prepare the municipal packet before inspection.",
                }
            ],
        },
    }

    out = get_effective_hqs_items(
        db,
        org_id=org.id,
        prop=prop,
        profile_summary=profile_summary,
    )

    items = out["items"]
    codes = {x["code"] for x in items}
    source_types = {s.get("type") for s in out["sources"]}

    # baseline internal items
    assert "SMOKE_DETECTORS" in codes
    assert "HEAT" in codes

    # jurisdiction profile additions
    assert "WARREN_PACKET_READY" in codes
    assert "LOCAL_INSPECTION_REQUIRED" in codes
    assert "LOCAL_CERTIFICATE_BEFORE_OCCUPANCY" in codes

    # contextual additions
    assert "LEAD_SAFE_SURFACES" in codes
    assert "LOCAL_AGENT_DOCUMENTATION" in codes

    assert "baseline_internal" in source_types
    assert "jurisdiction_policy" in source_types
    assert "contextual_rules" in source_types

    counts = out["counts"]
    assert counts["total"] >= counts["baseline"]
    assert counts["profile_items"] >= 1
    assert counts["contextual_items"] >= 1
    