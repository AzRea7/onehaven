from __future__ import annotations

from types import SimpleNamespace

import pytest


def _deny(detail: str = "Plan limit reached", code: str = "plan_limit_reached"):
    from fastapi import HTTPException

    raise HTTPException(
        status_code=403,
        detail={
            "ok": False,
            "error": code,
            "message": detail,
        },
    )


def test_free_org_denied_premium_action_at_service_level(
    premium_vs_base_org,
    monkeypatch,
):
    from app.services import plan_service

    base_org = premium_vs_base_org["base_org"]
    premium_org = premium_vs_base_org["premium_org"]

    def fake_require_feature(db, *, org_id: int, feature: str):
        if int(org_id) == int(base_org.id) and feature == "premium_reports":
            _deny("Premium reports require a paid plan.")
        return {"ok": True, "feature": feature, "org_id": int(org_id)}

    monkeypatch.setattr(plan_service, "require_feature", fake_require_feature, raising=False)

    with pytest.raises(Exception) as exc:
        plan_service.require_feature(None, org_id=base_org.id, feature="premium_reports")

    assert getattr(exc.value, "status_code", None) == 403
    detail = getattr(exc.value, "detail", {})
    assert detail["ok"] is False
    assert detail["error"] == "plan_limit_reached"
    assert "paid plan" in detail["message"].lower()

    allowed = plan_service.require_feature(None, org_id=premium_org.id, feature="premium_reports")
    assert allowed["ok"] is True
    assert allowed["org_id"] == premium_org.id


def test_plan_limit_blocks_new_actions_at_threshold_but_allows_under_limit(
    premium_vs_base_org,
    usage_snapshot_factory,
    monkeypatch,
):
    from app.services import usage_service

    base_org = premium_vs_base_org["base_org"]
    premium_org = premium_vs_base_org["premium_org"]

    usage_snapshot_factory(
        org_id=base_org.id,
        metric="external_call",
        provider="rentcast",
        used=50,
        limit=50,
        remaining=0,
        plan_code="base",
    )
    usage_snapshot_factory(
        org_id=premium_org.id,
        metric="external_call",
        provider="rentcast",
        used=12,
        limit=500,
        remaining=488,
        plan_code="premium",
    )

    def fake_check_limit(db, *, org_id: int, metric: str, provider: str | None = None):
        if int(org_id) == int(base_org.id):
            _deny("Daily external API quota exhausted.")
        return {
            "ok": True,
            "org_id": int(org_id),
            "metric": metric,
            "provider": provider,
        }

    monkeypatch.setattr(usage_service, "require_within_limit", fake_check_limit, raising=False)

    with pytest.raises(Exception) as exc:
        usage_service.require_within_limit(
            None,
            org_id=base_org.id,
            metric="external_call",
            provider="rentcast",
        )

    assert getattr(exc.value, "status_code", None) == 403
    detail = getattr(exc.value, "detail", {})
    assert detail["ok"] is False
    assert detail["error"] == "plan_limit_reached"
    assert "quota" in detail["message"].lower()

    ok = usage_service.require_within_limit(
        None,
        org_id=premium_org.id,
        metric="external_call",
        provider="rentcast",
    )
    assert ok["ok"] is True
    assert ok["org_id"] == premium_org.id


def test_response_shape_for_limit_denial_is_consistent_and_readable(
    monkeypatch,
):
    from app.services import usage_service

    def fake_check_limit(db, *, org_id: int, metric: str, provider: str | None = None):
        _deny("You have reached the maximum number of external calls for this billing period.")

    monkeypatch.setattr(usage_service, "require_within_limit", fake_check_limit, raising=False)

    with pytest.raises(Exception) as exc:
        usage_service.require_within_limit(
            None,
            org_id=1,
            metric="external_call",
            provider="rentcast",
        )

    assert getattr(exc.value, "status_code", None) == 403
    detail = getattr(exc.value, "detail", {})
    assert set(detail.keys()) >= {"ok", "error", "message"}
    assert detail["ok"] is False
    assert isinstance(detail["message"], str)
    assert len(detail["message"]) > 10


def test_router_and_service_can_share_same_plan_enforcement_contract(
    premium_vs_base_org,
    monkeypatch,
):
    from app.services import plan_service

    base_org = premium_vs_base_org["base_org"]
    premium_org = premium_vs_base_org["premium_org"]

    def fake_enforce(db, *, org_id: int, action: str):
        if int(org_id) == int(base_org.id):
            _deny(f"{action} is not available on the current plan.")
        return {"ok": True, "org_id": int(org_id), "action": action}

    monkeypatch.setattr(plan_service, "enforce_action", fake_enforce, raising=False)

    principal_base = SimpleNamespace(org_id=base_org.id, plan_code="base")
    principal_premium = SimpleNamespace(org_id=premium_org.id, plan_code="premium")

    with pytest.raises(Exception) as exc:
        plan_service.enforce_action(None, org_id=principal_base.org_id, action="premium_reports.export")

    assert getattr(exc.value, "status_code", None) == 403
    assert "current plan" in getattr(exc.value, "detail", {}).get("message", "").lower()

    ok = plan_service.enforce_action(None, org_id=principal_premium.org_id, action="premium_reports.export")
    assert ok["ok"] is True
    assert ok["action"] == "premium_reports.export"