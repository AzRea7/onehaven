from __future__ import annotations

from types import SimpleNamespace

from app.routers import jurisdictions as jurisdictions_router


def test_manual_refresh_flow_updates_visible_status_when_automation_is_off(monkeypatch):
    fake_profile = SimpleNamespace(id=31, org_id=1)

    health_states = iter(
        [
            {
                "ok": True,
                "jurisdiction_profile_id": 31,
                "health_status": "blocked",
                "refresh_state": "blocked",
                "safe_to_rely_on": False,
                "next_due_step": "refresh",
            },
            {
                "ok": True,
                "jurisdiction_profile_id": 31,
                "health_status": "ok",
                "refresh_state": "healthy",
                "safe_to_rely_on": True,
                "next_due_step": "monitor",
            },
        ]
    )

    monkeypatch.setattr(jurisdictions_router, "get_jurisdiction_health", lambda *args, **kwargs: next(health_states))
    monkeypatch.setattr(
        jurisdictions_router,
        "manual_refresh_stale_profiles",
        lambda: {
            "ok": True,
            "task": "jurisdiction.refresh_stale_profiles",
            "manual_mode": True,
            "automation_enabled": False,
            "changed_profile_ids": [31],
        },
    )

    class FakeDB:
        def get(self, model, id_):
            return fake_profile

    before = jurisdictions_router.post_manual_profile_health_recompute(
        profile_id=31,
        db=FakeDB(),
        principal=SimpleNamespace(org_id=1),
    )
    action = jurisdictions_router.post_manual_refresh_stale(
        principal=SimpleNamespace(org_id=1, user_id=1),
    )
    after = jurisdictions_router.post_manual_profile_health_recompute(
        profile_id=31,
        db=FakeDB(),
        principal=SimpleNamespace(org_id=1),
    )

    assert before["health"]["safe_to_rely_on"] is False
    assert action["automation_enabled"] is False
    assert action["manual_mode"] is True
    assert after["health"]["safe_to_rely_on"] is True
    assert after["health"]["health_status"] == "ok"


def test_review_required_state_remains_not_safe_until_resolved(monkeypatch):
    fake_profile = SimpleNamespace(id=32, org_id=1)

    monkeypatch.setattr(
        jurisdictions_router,
        "get_jurisdiction_health",
        lambda *args, **kwargs: {
            "ok": True,
            "jurisdiction_profile_id": 32,
            "health_status": "degraded",
            "refresh_state": "healthy",
            "safe_to_rely_on": False,
            "review_required": True,
            "validation_pending_categories": ["inspection"],
            "next_due_step": "retry_validation",
        },
    )

    class FakeDB:
        def get(self, model, id_):
            return fake_profile

    result = jurisdictions_router.post_manual_profile_health_recompute(
        profile_id=32,
        db=FakeDB(),
        principal=SimpleNamespace(org_id=1),
    )

    assert result["health"]["safe_to_rely_on"] is False
    assert result["health"]["review_required"] is True
    assert result["health"]["next_due_step"] == "retry_validation"
