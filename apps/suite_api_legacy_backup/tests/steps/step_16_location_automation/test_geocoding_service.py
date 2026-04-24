# backend/tests/steps/step_16_location_automation/test_geocoding_service.py
from __future__ import annotations

from dataclasses import dataclass

from app.models import Property
from app.services.geocoding_service import GeocodingService
from app.services.geocode_cache_service import GeocodeCacheService


@dataclass
class FakeProviderResult:
    source: str
    formatted_address: str | None
    lat: float | None
    lng: float | None
    city: str | None
    state: str | None
    postal_code: str | None
    county: str | None
    confidence: float | None
    provider_status: str | None
    raw_json: dict


class FakeProvider:
    def __init__(self, source: str, result: FakeProviderResult | None = None, should_raise: bool = False) -> None:
        self.source = source
        self._result = result
        self._should_raise = should_raise
        self.calls: list[str] = []

    @property
    def is_enabled(self) -> bool:
        return True

    def geocode(self, address: str):
        self.calls.append(address)
        if self._should_raise:
            raise RuntimeError(f"{self.source} failed")
        return self._result


def test_geocoding_service_uses_google_and_writes_cache(db_session) -> None:
    google = FakeProvider(
        "google",
        FakeProviderResult(
            source="google",
            formatted_address="123 N Main St, Detroit, MI 48226, USA",
            lat=42.3314,
            lng=-83.0458,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.99,
            provider_status="OK",
            raw_json={"provider": "google"},
        ),
    )
    nominatim = FakeProvider("nominatim", None)

    service = GeocodingService(
        db_session,
        cache_service=GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12),
        google_client=google,         # type: ignore[arg-type]
        nominatim_client=nominatim,   # type: ignore[arg-type]
        provider_order=["google", "nominatim"],
    )

    result = service.geocode(
        address="123 north main street",
        city="detroit",
        state="michigan",
        postal_code="48226",
    )

    assert result is not None
    assert result.is_success is True
    assert result.cache_hit is False
    assert result.source == "google"
    assert result.lat == 42.3314
    assert result.lng == -83.0458
    assert result.normalized_address == "123 N Main St, Detroit, MI 48226"

    cached = service.cache_service.get_by_normalized_address("123 N Main St, Detroit, MI 48226")
    assert cached is not None
    assert cached.source == "google"
    assert cached.lat == 42.3314
    assert cached.lng == -83.0458


def test_geocoding_service_uses_cache_before_provider(db_session) -> None:
    google = FakeProvider(
        "google",
        FakeProviderResult(
            source="google",
            formatted_address="123 N Main St, Detroit, MI 48226, USA",
            lat=42.3314,
            lng=-83.0458,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.99,
            provider_status="OK",
            raw_json={"provider": "google"},
        ),
    )

    service = GeocodingService(
        db_session,
        cache_service=GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12),
        google_client=google,         # type: ignore[arg-type]
        nominatim_client=FakeProvider("nominatim", None),  # type: ignore[arg-type]
        provider_order=["google", "nominatim"],
    )

    first = service.geocode(
        address="123 north main street",
        city="detroit",
        state="michigan",
        postal_code="48226",
    )
    second = service.geocode(
        address="123 N Main St",
        city="Detroit",
        state="MI",
        postal_code="48226",
    )

    assert first is not None
    assert second is not None
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert len(google.calls) == 1


def test_geocoding_service_falls_back_to_nominatim_when_google_fails(db_session) -> None:
    google = FakeProvider("google", should_raise=True)
    nominatim = FakeProvider(
        "nominatim",
        FakeProviderResult(
            source="nominatim",
            formatted_address="123 N Main St, Detroit, Wayne County, Michigan, 48226, United States",
            lat=42.3314,
            lng=-83.0458,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.78,
            provider_status="OK",
            raw_json={"provider": "nominatim"},
        ),
    )

    service = GeocodingService(
        db_session,
        cache_service=GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12),
        google_client=google,         # type: ignore[arg-type]
        nominatim_client=nominatim,   # type: ignore[arg-type]
        provider_order=["google", "nominatim"],
    )

    result = service.geocode(
        address="123 N Main St",
        city="Detroit",
        state="MI",
        postal_code="48226",
    )

    assert result is not None
    assert result.is_success is True
    assert result.source == "nominatim"
    assert len(google.calls) == 1
    assert len(nominatim.calls) == 1


def test_geocoding_service_prefers_higher_confidence_result_when_fallback_allowed(db_session) -> None:
    google = FakeProvider(
        "google",
        FakeProviderResult(
            source="google",
            formatted_address="123 N Main St, Detroit, MI 48226, USA",
            lat=42.3314,
            lng=-83.0458,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.60,
            provider_status="OK",
            raw_json={"provider": "google"},
        ),
    )
    nominatim = FakeProvider(
        "nominatim",
        FakeProviderResult(
            source="nominatim",
            formatted_address="123 N Main St, Detroit, Wayne County, Michigan, 48226, United States",
            lat=42.3315,
            lng=-83.0459,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.84,
            provider_status="OK",
            raw_json={"provider": "nominatim"},
        ),
    )

    service = GeocodingService(
        db_session,
        cache_service=GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12),
        google_client=google,         # type: ignore[arg-type]
        nominatim_client=nominatim,   # type: ignore[arg-type]
        provider_order=["google", "nominatim"],
    )

    result = service.geocode(
        address="123 N Main St",
        city="Detroit",
        state="MI",
        postal_code="48226",
        min_confidence=0.90,
        allow_fallback_providers=True,
    )

    assert result is not None
    assert result.source == "nominatim"
    assert result.confidence == 0.84


def test_refresh_property_location_updates_property_fields(db_session) -> None:
    prop = Property(
        org_id=1,
        address="123 North Main Street",
        city="Detroit",
        state="MI",
        zip="48226",
        bedrooms=3,
        bathrooms=1.0,
    )
    db_session.add(prop)
    db_session.commit()
    db_session.refresh(prop)

    google = FakeProvider(
        "google",
        FakeProviderResult(
            source="google",
            formatted_address="123 N Main St, Detroit, MI 48226, USA",
            lat=42.3314,
            lng=-83.0458,
            city="Detroit",
            state="MI",
            postal_code="48226",
            county="Wayne County",
            confidence=0.99,
            provider_status="OK",
            raw_json={"provider": "google"},
        ),
    )

    service = GeocodingService(
        db_session,
        cache_service=GeocodeCacheService(db_session, ttl_hours=24, stale_after_hours=12),
        google_client=google,  # type: ignore[arg-type]
        nominatim_client=FakeProvider("nominatim", None),  # type: ignore[arg-type]
        provider_order=["google", "nominatim"],
    )

    result = service.refresh_property_location(property_obj=prop, force_refresh=True)

    assert result is not None
    assert prop.normalized_address == "123 N Main St, Detroit, MI 48226"
    assert prop.lat == 42.3314
    assert prop.lng == -83.0458
    assert prop.county == "Wayne County"
    assert prop.geocode_source == "google"
    assert prop.geocode_confidence == 0.99
    assert prop.geocode_last_refreshed is not None