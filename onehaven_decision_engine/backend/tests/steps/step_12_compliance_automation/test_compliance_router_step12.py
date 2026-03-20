from __future__ import annotations

from app.routers import compliance as compliance_router

def test_automation_run_route_returns_summary_and_workflow(
    monkeypatch,
    client,
    seed_property,
):
    prop = seed_property

    monkeypatch.setattr(compliance_router, "require_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        compliance_router,
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
    assert body["inspection_readiness"]["overall_status"] == "blocked"
    assert body["task_generation"]["created"] == 2
