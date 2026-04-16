from __future__ import annotations

from types import SimpleNamespace

from app.routers import jurisdictions as jurisdictions_router


def test_jurisdiction_health_router_exposes_operational_visibility(monkeypatch):
    fake_profile = SimpleNamespace(id=11, org_id=1)

    monkeypatch.setattr(
        jurisdictions_router,
        "get_jurisdiction_health",
        lambda *args, **kwargs: {
            "ok": True,
            "jurisdiction_profile_id": 11,
            "health_status": "blocked",
            "refresh_state": "degraded",
            "refresh_status_reason": "critical_authoritative_data_stale",
            "safe_to_rely_on": False,
            "lockout": {"lockout_active": True, "lockout_reason": "critical_authoritative_data_stale"},
            "next_actions": {"next_step": "retry_validation"},
            "validation_pending_categories": ["inspection"],
            "authority_gap_categories": ["registration"],
            "lockout_causing_categories": ["inspection", "registration"],
            "informational_gap_categories": ["contacts"],
        },
    )
    monkeypatch.setattr(
        jurisdictions_router,
        "_operational_status_payload",
        lambda *args, **kwargs: {
            "health_state": "blocked",
            "refresh_state": "degraded",
            "refresh_status_reason": "critical_authoritative_data_stale",
            "safe_to_rely_on": False,
            "reasons": ["critical_authoritative_data_stale"],
            "source_summary": {"freshness_counts": {"stale": 1}, "validation_state_counts": {"pending": 1}},
            "next_actions": {"next_step": "retry_validation"},
            "lockout_causing_categories": ["inspection", "registration"],
            "informational_gap_categories": ["contacts"],
            "validation_pending_categories": ["inspection"],
            "authority_gap_categories": ["registration"],
        },
    )

    class FakeDB:
        def get(self, model, id_):
            return fake_profile

    result = jurisdictions_router.jurisdiction_health(
        profile_id=11,
        state=None,
        county=None,
        city=None,
        pha_name=None,
        db=FakeDB(),
        principal=SimpleNamespace(org_id=1),
    )

    assert result["ok"] is True
    assert result["health_status"] == "blocked"
    assert result["source_summary"]["freshness_counts"]["stale"] == 1
    assert result["safe_to_rely_on"] is False
    assert result["lockout_causing_categories"] == ["inspection", "registration"]
    assert result["validation_pending_categories"] == ["inspection"]
    assert result["authority_gap_categories"] == ["registration"]


def test_visibility_endpoint_returns_health_and_operational_status(monkeypatch):
    fake_profile = SimpleNamespace(id=12, org_id=1)

    monkeypatch.setattr(
        jurisdictions_router,
        "_profile_resolution_payload",
        lambda *args, **kwargs: {
            "profile_id": 12,
            "safe_to_rely_on": False,
            "unsafe_reasons": ["jurisdiction_lockout_active"],
        },
    )
    monkeypatch.setattr(
        jurisdictions_router,
        "_coverage_matrix_payload",
        lambda *args, **kwargs: {"jurisdiction_profile_id": 12, "category_matrix": []},
    )
    monkeypatch.setattr(
        jurisdictions_router,
        "get_jurisdiction_health",
        lambda *args, **kwargs: {"ok": True, "jurisdiction_profile_id": 12, "health_status": "degraded"},
    )
    monkeypatch.setattr(
        jurisdictions_router,
        "_operational_status_payload",
        lambda *args, **kwargs: {
            "health_state": "degraded",
            "safe_to_rely_on": False,
            "next_due_step": "retry_validation",
            "lockout_causing_categories": ["inspection"],
        },
    )

    class FakeDB:
        def get(self, model, id_):
            return fake_profile

    result = jurisdictions_router.get_jurisdiction_visibility(
        jurisdiction_profile_id=12,
        recompute=False,
        db=FakeDB(),
        p=SimpleNamespace(org_id=1),
    )

    assert result["ok"] is True
    assert result["resolved_profile"]["safe_to_rely_on"] is False
    assert result["health"]["health_status"] == "degraded"
    assert result["operational_status"]["next_due_step"] == "retry_validation"
