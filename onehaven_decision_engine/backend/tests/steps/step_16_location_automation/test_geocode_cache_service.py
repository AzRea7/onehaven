# backend/tests/steps/step_16_location_automation/test_geocode_cache_service.py
from __future__ import annotations

from datetime import datetime, timedelta

from app.models import GeocodeCache
from app.services.geocode_cache_service import (
    GeocodeCacheService,
    build_geocode_cache_payload,
)


def test_make_key_uses_normalized_address(db_session) -> None:
    service = GeocodeCacheService(db_session)

    key = service.make_key(
        address="123 north main street",
        city="detroit",
        state="michigan",
        postal_code="48226",
    )

    assert key == "123 N Main St, Detroit, MI 48226"


def test_upsert_creates_new_cache_entry(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="123 north main street",
        city="Detroit",
        state="MI",
        postal_code="48226",
        county="Wayne",
        lat=42.3314,
        lng=-83.0458,
        source="google",
        confidence=0.97,
        formatted_address="123 N Main St, Detroit, MI 48226, USA",
        provider_response_json={"provider": "google"},
    )

    entry = service.upsert(payload)

    assert entry.id is not None
    assert entry.normalized_address == "123 N Main St, Detroit, MI 48226"
    assert entry.county == "Wayne"
    assert entry.lat == 42.3314
    assert entry.lng == -83.0458
    assert entry.source == "google"
    assert entry.confidence == 0.97
    assert entry.hit_count == 0
    assert entry.expires_at is not None


def test_upsert_updates_existing_entry_instead_of_creating_duplicate(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload1 = build_geocode_cache_payload(
        address="123 N Main St",
        city="Detroit",
        state="MI",
        postal_code="48226",
        county="Wayne",
        lat=42.0,
        lng=-83.0,
        source="nominatim",
        confidence=0.70,
    )
    first = service.upsert(payload1)

    payload2 = build_geocode_cache_payload(
        address="123 North Main Street",
        city="Detroit",
        state="Michigan",
        postal_code="48226",
        county="Wayne",
        lat=42.3314,
        lng=-83.0458,
        source="google",
        confidence=0.95,
    )
    second = service.upsert(payload2)

    assert first.id == second.id
    assert second.source == "google"
    assert second.confidence == 0.95
    assert second.lat == 42.3314
    assert second.lng == -83.0458

    count = db_session.query(GeocodeCache).count()
    assert count == 1


def test_get_fresh_by_components_returns_entry_and_increments_hit_count(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="456 W Maple Ave Apt 3B",
        city="Dearborn",
        state="MI",
        postal_code="48124",
        lat=42.3,
        lng=-83.2,
        source="google",
    )
    created = service.upsert(payload)

    assert created.hit_count == 0

    fetched = service.get_fresh_by_components(
        address="456 west maple avenue apartment 3b",
        city="dearborn",
        state="michigan",
        postal_code="48124",
    )

    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.hit_count == 1


def test_get_fresh_by_components_returns_none_for_expired_entry(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="789 Elm Rd",
        city="Detroit",
        state="MI",
        postal_code="48201",
        lat=42.1,
        lng=-83.1,
        source="google",
    )
    entry = service.upsert(payload)

    entry.expires_at = datetime.utcnow() - timedelta(minutes=1)
    db_session.add(entry)
    db_session.commit()

    fetched = service.get_fresh_by_components(
        address="789 Elm Road",
        city="Detroit",
        state="MI",
        postal_code="48201",
    )

    assert fetched is None


def test_is_stale_returns_true_when_last_refreshed_too_old(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="1000 Oak St",
        city="Detroit",
        state="MI",
        postal_code="48202",
        lat=42.2,
        lng=-83.2,
        source="google",
    )
    entry = service.upsert(payload)

    entry.last_refreshed_at = datetime.utcnow() - timedelta(hours=13)
    db_session.add(entry)
    db_session.commit()
    db_session.refresh(entry)

    assert service.is_stale(entry) is True


def test_is_stale_returns_false_when_recent(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="1001 Oak St",
        city="Detroit",
        state="MI",
        postal_code="48202",
        lat=42.2,
        lng=-83.2,
        source="google",
    )
    entry = service.upsert(payload)

    entry.last_refreshed_at = datetime.utcnow() - timedelta(hours=2)
    db_session.add(entry)
    db_session.commit()
    db_session.refresh(entry)

    assert service.is_stale(entry) is False


def test_invalidate_marks_entry_as_expired(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="2000 Pine St",
        city="Detroit",
        state="MI",
        postal_code="48203",
        source="google",
    )
    created = service.upsert(payload)

    ok = service.invalidate(normalized_address=created.normalized_address)
    assert ok is True

    refreshed = service.get_by_normalized_address(created.normalized_address)
    assert refreshed is not None
    assert refreshed.expires_at is not None
    assert service.is_expired(refreshed) is True


def test_delete_removes_entry(db_session) -> None:
    service = GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12)

    payload = build_geocode_cache_payload(
        address="3000 Cedar St",
        city="Detroit",
        state="MI",
        postal_code="48204",
        source="google",
    )
    created = service.upsert(payload)

    deleted = service.delete(normalized_address=created.normalized_address)
    assert deleted is True

    found = service.get_by_normalized_address(created.normalized_address)
    assert found is None