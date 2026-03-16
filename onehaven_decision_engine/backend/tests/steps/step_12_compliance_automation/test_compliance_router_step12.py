from __future__ import annotations

from app.services import compliance_service


def test_inspection_readiness_route_returns_policy_driven_payload(
    monkeypatch,
    client,
    seed_property,
):
    prop = seed_property

    monkeypatch.setattr(
        compliance_service,
        "build_property_inspection_readiness",
        lambda *args, **kwargs: {
            "ok": True,
            "property": {
                "id": prop.id,
                "address": prop.address,
                "city": prop.city,
                "county": prop.county,
                "state": prop.state,
                "zip": prop.zip,
                "pha_name": None,
            },
            "market": {
                "scope": "global",
                "match_level": "city",
                "profile_id": 123,
                "friction_multiplier": 1.2,
            },
            "coverage": {
                "coverage_status": "verified_extended",
                "confidence_label": "high",
                "production_readiness": "ready",
            },
            "overall_status": "blocked",
            "score_pct": 61.0,
            "readiness": {
                "hqs_ready": False,
                "local_ready": False,
                "voucher_ready": False,
                "lease_up_ready": False,
            },
            "counts": {
                "total_rules": 8,
                "passed": 3,
                "failing": 2,
                "unknown": 1,
                "warnings": 2,
                "blocking": 2,
            },
            "results": [],
            "blocking_items": [
                {
                    "rule_key": "HEAT",
                    "label": "Permanent heat source is present and operational",
                }
            ],
            "warning_items": [],
            "recommended_actions": [
                {
                    "rule_key": "HEAT",
                    "label": "Permanent heat source is present and operational",
                }
            ],
            "effective_hqs_sources": [],
            "effective_hqs_counts": {"total": 8},
            "policy_brief": {},
            "jurisdiction": {},
            "latest_inspection": {"id": None, "passed": None},
            "run_summary": {
                "passed": 3,
                "failed": 2,
                "blocked": 2,
                "not_yet": 1,
                "score_pct": 61.0,
            },
        },
    )

    r = client.get(f"/api/compliance/property/{prop.id}/inspection-readiness")
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["ok"] is True
    assert body["property"]["id"] == prop.id
    assert body["overall_status"] == "blocked"
    assert body["readiness"]["lease_up_ready"] is False
    assert body["counts"]["blocking"] == 2
    assert len(body["recommended_actions"]) == 1


def test_status_route_uses_readiness_engine_and_returns_condensed_status(
    monkeypatch,
    client,
    seed_property,
):
    prop = seed_property

    monkeypatch.setattr(
        compliance_service,
        "build_property_inspection_readiness",
        lambda *args, **kwargs: {
            "ok": True,
            "overall_status": "attention",
            "score_pct": 88.0,
            "readiness": {
                "hqs_ready": True,
                "local_ready": True,
                "voucher_ready": True,
                "lease_up_ready": True,
            },
            "counts": {
                "total_rules": 10,
                "passed": 8,
                "failing": 0,
                "unknown": 0,
                "warnings": 2,
                "blocking": 0,
            },
            "blocking_items": [],
            "warning_items": [{"rule_key": "LEAD_SAFE_SURFACES"}],
            "recommended_actions": [{"rule_key": "LEAD_SAFE_SURFACES"}],
            "coverage": {
                "coverage_status": "verified_extended",
                "confidence_label": "high",
                "production_readiness": "ready",
            },
        },
    )

    r = client.get(f"/api/compliance/status/{prop.id}")
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["property_id"] == prop.id
    assert body["passed"] is True
    assert body["overall_status"] == "attention"
    assert body["score_pct"] == 88.0
    assert body["counts"]["blocking"] == 0
    assert len(body["warning_items"]) == 1


def test_automation_run_route_returns_summary_and_workflow(
    monkeypatch,
    client,
    seed_property,
):
    prop = seed_property

    monkeypatch.setattr(
        compliance_service,
        "run_hqs",
        lambda *args, **kwargs: {
            "ok": True,
            "property_id": prop.id,
            "legacy_summary": {
                "total": 10,
                "done": 7,
                "failed": 1,
            },
            "top_fix_candidates": [
                {"item_code": "HEAT", "priority": "high"}
            ],
            "inspection_readiness": {
                "ok": True,
                "overall_status": "blocked",
                "score_pct": 70.0,
                "readiness": {
                    "hqs_ready": False,
                    "local_ready": False,
                    "voucher_ready": False,
                    "lease_up_ready": False,
                },
                "counts": {
                    "total_rules": 10,
                    "passed": 7,
                    "failing": 1,
                    "unknown": 1,
                    "warnings": 1,
                    "blocking": 2,
                },
                "run_summary": {
                    "passed": 7,
                    "failed": 1,
                    "blocked": 2,
                    "not_yet": 1,
                    "score_pct": 70.0,
                },
            },
            "task_generation": {
                "ok": True,
                "created": 2,
                "titles": [
                    "Compliance: Warren rental license required",
                    "Compliance: Permanent heat source is present and operational",
                ],
            },
        },
    )

    r = client.post(f"/api/compliance/property/{prop.id}/automation/run?create_tasks=true")
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["ok"] is True
    assert body["property_id"] == prop.id
    assert body["task_generation"]["created"] == 2
    assert body["summary"]["score_pct"] == 70.0
    assert body["workflow"]["stage"] == "compliance"
    assert body["inspection_readiness"]["overall_status"] == "blocked"