from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import select

from app.models import ApiKey, AuditEvent, Property
from app.routers import audit as audit_router
from app.routers import api_keys as api_keys_router


def test_org_a_cannot_read_org_b_property_via_org_scoped_query(
    db_session,
    org_factory,
    seeded_property_bundle,
):
    org_a = org_factory(slug="step20-iso-a", name="Isolation A")
    org_b = org_factory(slug="step20-iso-b", name="Isolation B")

    bundle_a = seeded_property_bundle(org=org_a, address="111 A St")
    bundle_b = seeded_property_bundle(org=org_b, address="222 B St")

    prop_a = db_session.scalar(
        select(Property).where(
            Property.id == bundle_a["property"].id,
            Property.org_id == org_a.id,
        )
    )
    prop_b_from_a_scope = db_session.scalar(
        select(Property).where(
            Property.id == bundle_b["property"].id,
            Property.org_id == org_a.id,
        )
    )

    assert prop_a is not None
    assert prop_a.address == "111 A St"
    assert prop_b_from_a_scope is None


def test_api_key_from_org_a_cannot_revoke_org_b_key(
    db_session,
    org_factory,
    user_factory,
    api_key_factory,
):
    org_a = org_factory(slug="step20-key-a", name="Key A")
    org_b = org_factory(slug="step20-key-b", name="Key B")
    user_a = user_factory(email="owner-a@example.com")
    user_b = user_factory(email="owner-b@example.com")

    key_a = api_key_factory(org_id=org_a.id, created_by_user_id=user_a.id)
    key_b = api_key_factory(org_id=org_b.id, created_by_user_id=user_b.id)

    principal_a = SimpleNamespace(
        org_id=org_a.id,
        org_slug=org_a.slug,
        user_id=user_a.id,
        email=user_a.email,
        role="owner",
        plan_code="premium",
    )

    # route contract visible in repo:
    # select(ApiKey).where(ApiKey.id == key_id, ApiKey.org_id == principal.org_id)
    # => org A should not be able to revoke org B key
    try:
        api_keys_router.revoke_key(int(key_b.id), db=db_session, principal=principal_a)
        assert False, "Expected cross-org revoke to fail"
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        assert status_code == 404

    still_exists = db_session.get(ApiKey, key_b.id)
    assert still_exists is not None
    assert getattr(still_exists, "revoked_at", None) is None

    own_key = api_keys_router.revoke_key(int(key_a.id), db=db_session, principal=principal_a)
    assert own_key["ok"] is True
    assert own_key["id"] == int(key_a.id)


def test_scheduler_service_queries_should_remain_org_filtered(
    db_session,
    multi_org_isolation,
    ingestion_source_factory,
):
    org_a = multi_org_isolation["org_a"]
    org_b = multi_org_isolation["org_b"]

    src_a = ingestion_source_factory(org_id=org_a.id, slug="iso-src-a")
    src_b = ingestion_source_factory(org_id=org_b.id, slug="iso-src-b")

    from app.models import IngestionSource

    rows_a = db_session.scalars(
        select(IngestionSource).where(IngestionSource.org_id == org_a.id)
    ).all()
    rows_b = db_session.scalars(
        select(IngestionSource).where(IngestionSource.org_id == org_b.id)
    ).all()

    assert {r.id for r in rows_a} == {src_a.id}
    assert {r.id for r in rows_b} == {src_b.id}


def test_usage_counters_are_org_local(
    usage_snapshot_factory,
    multi_org_isolation,
):
    org_a = multi_org_isolation["org_a"]
    org_b = multi_org_isolation["org_b"]

    snap_a = usage_snapshot_factory(
        org_id=org_a.id,
        metric="external_call",
        provider="rentcast",
        used=12,
        limit=50,
        plan_code="free",
    )
    snap_b = usage_snapshot_factory(
        org_id=org_b.id,
        metric="external_call",
        provider="rentcast",
        used=3,
        limit=50,
        plan_code="free",
    )

    assert snap_a.org_id != snap_b.org_id
    assert snap_a.used == 12
    assert snap_b.used == 3


def test_audit_logs_include_correct_org_context_and_list_is_org_filtered(
    db_session,
    multi_org_isolation,
):
    org_a = multi_org_isolation["org_a"]
    org_b = multi_org_isolation["org_b"]

    db_session.add_all(
        [
            AuditEvent(
                org_id=org_a.id,
                event_type="property.updated",
                entity_type="property",
                entity_id="101",
                payload_json={"message": "org a update"},
            ),
            AuditEvent(
                org_id=org_b.id,
                event_type="property.updated",
                entity_type="property",
                entity_id="202",
                payload_json={"message": "org b update"},
            ),
        ]
    )
    db_session.commit()

    principal_a = SimpleNamespace(org_id=org_a.id)
    rows_a = audit_router.list_audit(
        entity_type=None,
        entity_id=None,
        limit=200,
        db=db_session,
        p=principal_a,
    )

    assert len(rows_a) >= 1
    assert all(int(r.org_id) == int(org_a.id) for r in rows_a)
    assert not any(str(r.entity_id) == "202" for r in rows_a)