# backend/tests/steps/step_20_platform_hardening/conftest.py
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

BACKEND_ROOT = Path(__file__).resolve().parents[3]

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.auth import Principal, get_principal
from app.db import SessionLocal
from app.main import app
from app.models import (
    ApiKey,
    AppUser,
    Deal,
    IngestionSource,
    Organization,
    OrgMembership,
    Property,
    PropertyState,
    UnderwritingResult,
)

try:
    from app.models import Plan
except Exception:  # pragma: no cover
    Plan = None  # type: ignore

try:
    from app.models import Subscription as OrgSubscription
except Exception:  # pragma: no cover
    try:
        from app.models import OrgSubscription  # type: ignore
    except Exception:  # pragma: no cover
        OrgSubscription = None  # type: ignore

try:
    from app.models import UsageSnapshot
except Exception:  # pragma: no cover
    UsageSnapshot = None  # type: ignore


@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def org_factory(db_session):
    created: list[Organization] = []

    def _make(*, slug: str | None = None, name: str | None = None) -> Organization:
        suffix = uuid4().hex[:8]
        org = Organization(
            slug=slug or f"step20-org-{suffix}",
            name=name or f"Step20 Org {suffix}",
        )
        db_session.add(org)
        db_session.commit()
        db_session.refresh(org)
        created.append(org)
        return org

    return _make


@pytest.fixture
def user_factory(db_session):
    created: list[AppUser] = []

    def _make(*, email: str | None = None, display_name: str | None = None) -> AppUser:
        suffix = uuid4().hex[:8]
        user = AppUser(
            email=email or f"step20-{suffix}@example.com",
            display_name=display_name or f"step20-{suffix}",
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)
        created.append(user)
        return user

    return _make


@pytest.fixture
def membership_factory(db_session):
    def _make(*, org_id: int, user_id: int, role: str = "owner") -> OrgMembership:
        row = OrgMembership(
            org_id=int(org_id),
            user_id=int(user_id),
            role=role,
            created_at=datetime.utcnow() if hasattr(OrgMembership, "created_at") else None,
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def principal_factory(org_factory, user_factory, membership_factory):
    def _make(
        *,
        org: Organization | None = None,
        user: AppUser | None = None,
        role: str = "owner",
        plan_code: str = "free",
        scopes: tuple[str, ...] = ("org:full",),
        principal_type: str = "user",
        api_key_id: int | None = None,
    ) -> Principal:
        org = org or org_factory()
        user = user or user_factory()
        membership_factory(org_id=org.id, user_id=user.id, role=role)
        return Principal(
            org_id=int(org.id),
            org_slug=str(org.slug),
            user_id=int(user.id),
            email=str(user.email),
            role=str(role),
            plan_code=str(plan_code),
            principal_type=str(principal_type),
            api_key_id=api_key_id,
            scopes=scopes,
        )

    return _make


@pytest.fixture
def client_with_principal(db_session):
    created_overrides = []

    def _make(principal: Principal):
        def _override():
            return principal

        app.dependency_overrides[get_principal] = _override
        created_overrides.append(True)
        client = TestClient(app)
        headers = {"X-Org-Slug": principal.org_slug}
        return client, headers

    yield _make

    app.dependency_overrides.pop(get_principal, None)


@pytest.fixture
def api_key_factory(db_session):
    def _make(
        *,
        org_id: int,
        created_by_user_id: int | None = None,
        key_prefix: str | None = None,
        key_hash: str | None = None,
        scopes_json=None,
        name: str | None = None,
    ):
        row = ApiKey(
            org_id=int(org_id),
            created_by_user_id=created_by_user_id,
            name=name or f"step20-key-{uuid4().hex[:6]}",
            key_prefix=key_prefix or f"step20{uuid4().hex[:4]}",
            key_hash=key_hash or "step20-hash",
            scopes_json=scopes_json or ["org:full"],
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def plan_factory(db_session):
    def _make(
        *,
        code: str | None = None,
        name: str | None = None,
        monthly_price: int | float | None = None,
        limits_json: dict | None = None,
        features_json: dict | None = None,
    ):
        if Plan is None:
            return SimpleNamespace(
                code=code or "free",
                name=name or "Free",
                monthly_price=monthly_price if monthly_price is not None else 0,
                limits_json=limits_json or {},
                features_json=features_json or {},
            )

        row = Plan(
            code=code or f"plan_{uuid4().hex[:6]}",
            name=name or "Step20 Plan",
            monthly_price=monthly_price if monthly_price is not None else 0,
            limits_json=limits_json or {},
            features_json=features_json or {},
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def subscription_factory(db_session):
    def _make(
        *,
        org_id: int,
        plan_code: str,
        status: str = "active",
    ):
        if OrgSubscription is None:
            return SimpleNamespace(org_id=org_id, plan_code=plan_code, status=status)

        row = OrgSubscription(
            org_id=int(org_id),
            plan_code=str(plan_code),
            status=str(status),
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def usage_snapshot_factory(db_session):
    def _make(
        *,
        org_id: int,
        metric: str = "external_call",
        provider: str = "rentcast",
        used: int = 0,
        limit: int | None = 50,
        remaining: int | None = None,
        plan_code: str = "free",
    ):
        if UsageSnapshot is None:
            return SimpleNamespace(
                org_id=org_id,
                metric=metric,
                provider=provider,
                used=used,
                limit=limit,
                remaining=remaining if remaining is not None else (None if limit is None else max(limit - used, 0)),
                plan_code=plan_code,
            )

        row = UsageSnapshot(
            org_id=int(org_id),
            metric=str(metric),
            provider=str(provider),
            used=int(used),
            limit=limit,
            remaining=remaining if remaining is not None else (None if limit is None else max(limit - used, 0)),
            plan_code=str(plan_code),
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def seeded_property_bundle(db_session, org_factory):
    def _make(*, org: Organization | None = None, address: str = "123 Main St") -> dict:
        org = org or org_factory()

        prop = Property(
            org_id=org.id,
            address=address,
            city="Detroit",
            state="MI",
            zip="48226",
            county="Wayne",
            bedrooms=3,
            bathrooms=1.0,
            square_feet=1200,
            year_built=1950,
            property_type="single_family",
        )
        db_session.add(prop)
        db_session.commit()
        db_session.refresh(prop)

        deal = Deal(
            org_id=org.id,
            property_id=prop.id,
            strategy="section8",
            asking_price=85000,
        )
        db_session.add(deal)
        db_session.commit()
        db_session.refresh(deal)

        uw = UnderwritingResult(
            org_id=org.id,
            deal_id=deal.id,
            decision="GOOD",
            score=87,
            dscr=1.31,
            cash_flow=525.0,
            gross_rent_used=1550.0,
        )
        db_session.add(uw)

        state = PropertyState(
            org_id=org.id,
            property_id=prop.id,
            current_stage="deal",
            constraints_json="{}",
            outstanding_tasks_json="{}",
            updated_at=datetime.utcnow(),
        )
        db_session.add(state)
        db_session.commit()
        db_session.refresh(state)

        return {
            "org": org,
            "property": prop,
            "deal": deal,
            "underwriting": uw,
            "state": state,
        }

    return _make


@pytest.fixture
def ingestion_source_factory(db_session):
    def _make(
        *,
        org_id: int,
        provider: str = "rentcast",
        slug: str | None = None,
        display_name: str | None = None,
        sample_rows: list[dict] | None = None,
        is_enabled: bool = True,
    ) -> IngestionSource:
        row = IngestionSource(
            org_id=int(org_id),
            provider=str(provider),
            slug=slug or f"{provider}-{uuid4().hex[:6]}",
            display_name=display_name or "Step20 Source",
            status="connected",
            is_enabled=bool(is_enabled),
            config_json={"sample_rows": sample_rows or []},
        )
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    return _make


@pytest.fixture
def sample_listing_payload():
    return {
        "external_record_id": "ext-step20-1",
        "address": "123 Main St",
        "city": "Detroit",
        "state": "MI",
        "zip": "48226",
        "county": "Wayne",
        "bedrooms": 3,
        "bathrooms": 1.0,
        "square_feet": 1200,
        "year_built": 1950,
        "property_type": "single_family",
        "asking_price": 85000,
        "market_rent_estimate": 1550,
        "photos": [{"url": "https://example.com/front.jpg", "kind": "exterior"}],
    }


@pytest.fixture
def multi_org_isolation(org_factory):
    org_a = org_factory(slug=f"org-a-{uuid4().hex[:6]}", name="Org A")
    org_b = org_factory(slug=f"org-b-{uuid4().hex[:6]}", name="Org B")
    return {"org_a": org_a, "org_b": org_b}


@pytest.fixture
def premium_vs_base_org(
    org_factory,
    plan_factory,
    subscription_factory,
):
    base_org = org_factory(slug=f"base-{uuid4().hex[:6]}", name="Base Org")
    premium_org = org_factory(slug=f"premium-{uuid4().hex[:6]}", name="Premium Org")

    base_plan = plan_factory(
        code=f"base_{uuid4().hex[:6]}",
        name="Base",
        monthly_price=0,
        limits_json={"external_calls_per_day": 50},
        features_json={"premium_reports": False},
    )
    premium_plan = plan_factory(
        code=f"premium_{uuid4().hex[:6]}",
        name="Premium",
        monthly_price=199,
        limits_json={"external_calls_per_day": 500},
        features_json={"premium_reports": True},
    )

    subscription_factory(org_id=base_org.id, plan_code=base_plan.code, status="active")
    subscription_factory(org_id=premium_org.id, plan_code=premium_plan.code, status="active")

    return {
        "base_org": base_org,
        "premium_org": premium_org,
        "base_plan": base_plan,
        "premium_plan": premium_plan,
    }


@pytest.fixture
def scheduler_task_spy(monkeypatch):
    calls: list[dict] = []

    class DummyDelayResult:
        id = "step20-task-id"

    class DummyTask:
        @staticmethod
        def delay(*args, **kwargs):
            calls.append({"args": args, "kwargs": kwargs})
            return DummyDelayResult()

    return {
        "calls": calls,
        "task": DummyTask,
        "patch": lambda target: monkeypatch.setattr(target, DummyTask),
    }


@pytest.fixture
def lock_state():
    return {
        "held": set(),
        "acquire_calls": [],
        "release_calls": [],
        "completed_keys": set(),
    }


@pytest.fixture
def lock_mocks(monkeypatch, lock_state):
    def _acquire(*args, **kwargs):
        key = kwargs.get("lock_key") or kwargs.get("key") or str(args)
        lock_state["acquire_calls"].append({"args": args, "kwargs": kwargs, "key": key})
        if key in lock_state["held"]:
            return False
        lock_state["held"].add(key)
        return True

    def _release(*args, **kwargs):
        key = kwargs.get("lock_key") or kwargs.get("key") or str(args)
        lock_state["release_calls"].append({"args": args, "kwargs": kwargs, "key": key})
        lock_state["held"].discard(key)
        return True

    def _has_completed(*args, **kwargs):
        key = kwargs.get("completion_key") or kwargs.get("key") or str(args)
        return key in lock_state["completed_keys"]

    def _mark_completed(*args, **kwargs):
        key = kwargs.get("completion_key") or kwargs.get("key") or str(args)
        lock_state["completed_keys"].add(key)
        return True

    monkeypatch.setattr("app.services.ingestion_run_execute.acquire_ingestion_execution_lock", _acquire)
    monkeypatch.setattr("app.services.ingestion_run_execute.release_lock", _release)
    monkeypatch.setattr("app.services.ingestion_run_execute.has_completed_ingestion_dataset", _has_completed)
    monkeypatch.setattr("app.services.ingestion_run_execute.mark_ingestion_dataset_completed", _mark_completed)

    return lock_state


@pytest.fixture
def fake_post_pipeline(monkeypatch):
    calls: list[dict] = []

    def _ok(db_session, *, org_id: int, property_id: int, actor_user_id=None, emit_events=False):
        calls.append(
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "actor_user_id": actor_user_id,
                "emit_events": emit_events,
            }
        )
        prop = db_session.get(Property, property_id)
        if prop is not None:
            prop.normalized_address = f"{prop.address}, {prop.city}, {prop.state} {prop.zip}"
            prop.lat = 42.3314
            prop.lng = -83.0458
            prop.geocode_source = "google"
            prop.geocode_confidence = 0.99
            db_session.add(prop)
            db_session.flush()
        return {
            "geo_ok": True,
            "risk_ok": True,
            "rent_ok": True,
            "evaluate_ok": True,
            "state_ok": True,
            "workflow_ok": True,
            "next_actions_ok": True,
            "partial": False,
            "errors": [],
        }

    monkeypatch.setattr(
        "app.services.ingestion_run_execute.execute_post_ingestion_pipeline",
        _ok,
    )

    return calls


@pytest.fixture
def external_api_failure(monkeypatch):
    calls: list[dict] = []

    def _fail(db_session, *, org_id: int, property_id: int, actor_user_id=None, emit_events=False):
        calls.append(
            {
                "org_id": int(org_id),
                "property_id": int(property_id),
                "actor_user_id": actor_user_id,
                "emit_events": emit_events,
            }
        )
        return {
            "geo_ok": False,
            "risk_ok": False,
            "rent_ok": False,
            "evaluate_ok": False,
            "state_ok": True,
            "workflow_ok": True,
            "next_actions_ok": False,
            "partial": True,
            "errors": [
                {
                    "stage": "rent_enrichment",
                    "code": "external_api_failure",
                    "provider": "rentcast",
                    "message": "RentCast unavailable",
                }
            ],
        }

    monkeypatch.setattr(
        "app.services.ingestion_run_execute.execute_post_ingestion_pipeline",
        _fail,
    )

    return calls