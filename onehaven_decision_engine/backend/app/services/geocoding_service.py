# backend/app/services/geocoding_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Protocol

from sqlalchemy.orm import Session

from ..config import settings
from .address_normalization import normalize_full_address
from .geocode_cache_service import (
    GeocodeCachePayload,
    GeocodeCacheService,
    build_geocode_cache_payload,
)
from ..clients.google_geocode import GoogleGeocodeClient, GoogleGeocodeResult
from ..clients.nominatim import NominatimClient, NominatimGeocodeResult


class GeocodeProvider(Protocol):
    source: str

    @property
    def is_enabled(self) -> bool: ...

    def geocode(self, address: str) -> Any | None: ...


@dataclass(frozen=True)
class GeocodingResult:
    normalized_address: str
    raw_input_address: str
    formatted_address: str | None
    lat: float | None
    lng: float | None
    city: str | None
    state: str | None
    postal_code: str | None
    county: str | None
    source: str
    confidence: float | None
    cache_hit: bool
    is_stale_cache: bool
    provider_status: str | None
    raw_json: dict[str, Any] | list[dict[str, Any]] | None

    @property
    def is_success(self) -> bool:
        return self.lat is not None and self.lng is not None

    def to_cache_payload(self) -> GeocodeCachePayload:
        return GeocodeCachePayload(
            normalized_address=self.normalized_address,
            raw_address=self.raw_input_address,
            city=self.city,
            state=self.state,
            zip=self.postal_code,
            county=self.county,
            lat=self.lat,
            lng=self.lng,
            source=self.source,
            confidence=self.confidence,
            formatted_address=self.formatted_address,
            provider_response_json=self.raw_json if isinstance(self.raw_json, dict) else {"results": self.raw_json},
        )


class GeocodingService:
    def __init__(
        self,
        db: Session,
        *,
        cache_service: GeocodeCacheService | None = None,
        google_client: GoogleGeocodeClient | None = None,
        nominatim_client: NominatimClient | None = None,
        provider_order: list[str] | None = None,
    ) -> None:
        self.db = db
        self.cache_service = cache_service or GeocodeCacheService(db)
        self.google_client = google_client or GoogleGeocodeClient()
        self.nominatim_client = nominatim_client or NominatimClient()
        self.provider_order = provider_order or list(settings.geocode_provider_order_list)

    def geocode(
        self,
        *,
        address: str | None,
        city: str | None,
        state: str | None,
        postal_code: str | None,
        force_refresh: bool = False,
        allow_fallback_providers: bool | None = None,
        min_confidence: float | None = None,
    ) -> GeocodingResult | None:
        normalized = normalize_full_address(address, city, state, postal_code)
        if not normalized.full_address:
            return None

        raw_input_address = ", ".join([p for p in [address, city, state, postal_code] if p])

        allow_fallback = (
            settings.geocode_allow_fallback_providers
            if allow_fallback_providers is None
            else bool(allow_fallback_providers)
        )
        confidence_threshold = (
            float(settings.geocode_min_confidence) if min_confidence is None else float(min_confidence)
        )

        cache_entry = self.cache_service.get_by_normalized_address(normalized.full_address)
        if not force_refresh and cache_entry is not None and not self.cache_service.is_expired(cache_entry):
            self.cache_service.touch_hit(cache_entry)
            return GeocodingResult(
                normalized_address=cache_entry.normalized_address,
                raw_input_address=cache_entry.raw_address or raw_input_address,
                formatted_address=cache_entry.formatted_address,
                lat=cache_entry.lat,
                lng=cache_entry.lng,
                city=cache_entry.city,
                state=cache_entry.state,
                postal_code=cache_entry.zip,
                county=cache_entry.county,
                source=cache_entry.source,
                confidence=cache_entry.confidence,
                cache_hit=True,
                is_stale_cache=self.cache_service.is_stale(cache_entry),
                provider_status="CACHE_HIT",
                raw_json=cache_entry.provider_response_json,
            )

        provider_names = self._resolve_provider_names()
        best_result: GeocodingResult | None = None

        for index, provider_name in enumerate(provider_names):
            provider = self._get_provider(provider_name)
            if provider is None or not provider.is_enabled:
                continue

            provider_result = self._safe_provider_geocode(provider, normalized.full_address)
            if provider_result is None:
                continue

            mapped = self._map_provider_result(
                normalized_address=normalized.full_address,
                raw_input_address=raw_input_address,
                provider_result=provider_result,
            )

            if mapped is None:
                continue

            if mapped.is_success:
                self.cache_service.upsert(mapped.to_cache_payload())
                if (mapped.confidence or 0.0) >= confidence_threshold:
                    return mapped
                best_result = self._prefer_result(best_result, mapped)

                if not allow_fallback:
                    return mapped
                continue

            if best_result is None:
                best_result = mapped

            if not allow_fallback:
                break

        if best_result is not None and best_result.is_success:
            self.cache_service.upsert(best_result.to_cache_payload())

        return best_result

    def refresh_property_location(
        self,
        *,
        property_obj: Any,
        force_refresh: bool = False,
    ) -> GeocodingResult | None:
        result = self.geocode(
            address=getattr(property_obj, "address", None),
            city=getattr(property_obj, "city", None),
            state=getattr(property_obj, "state", None),
            postal_code=getattr(property_obj, "zip", None),
            force_refresh=force_refresh,
        )
        if result is None:
            return None

        setattr(property_obj, "normalized_address", result.normalized_address)
        setattr(property_obj, "lat", result.lat)
        setattr(property_obj, "lng", result.lng)
        setattr(property_obj, "county", result.county or getattr(property_obj, "county", None))
        setattr(property_obj, "geocode_source", result.source)
        setattr(property_obj, "geocode_confidence", result.confidence)
        setattr(property_obj, "geocode_last_refreshed", datetime.utcnow())

        self.db.add(property_obj)
        self.db.commit()
        self.db.refresh(property_obj)

        return result

    def _resolve_provider_names(self) -> list[str]:
        ordered: list[str] = []
        for name in self.provider_order:
            normalized = (name or "").strip().lower()
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return ordered or ["nominatim"]

    def _get_provider(self, provider_name: str) -> GeocodeProvider | None:
        name = (provider_name or "").strip().lower()
        if name == "google":
            setattr(self.google_client, "source", "google")
            return self.google_client
        if name == "nominatim":
            setattr(self.nominatim_client, "source", "nominatim")
            return self.nominatim_client
        return None

    def _safe_provider_geocode(self, provider: GeocodeProvider, full_address: str) -> Any | None:
        try:
            return provider.geocode(full_address)
        except Exception:
            if settings.geocode_fail_open:
                return None
            raise

    def _map_provider_result(
        self,
        *,
        normalized_address: str,
        raw_input_address: str,
        provider_result: GoogleGeocodeResult | NominatimGeocodeResult | Any,
    ) -> GeocodingResult | None:
        if provider_result is None:
            return None

        return GeocodingResult(
            normalized_address=normalized_address,
            raw_input_address=raw_input_address,
            formatted_address=getattr(provider_result, "formatted_address", None),
            lat=getattr(provider_result, "lat", None),
            lng=getattr(provider_result, "lng", None),
            city=getattr(provider_result, "city", None),
            state=getattr(provider_result, "state", None),
            postal_code=getattr(provider_result, "postal_code", None),
            county=getattr(provider_result, "county", None),
            source=getattr(provider_result, "source", "unknown"),
            confidence=getattr(provider_result, "confidence", None),
            cache_hit=False,
            is_stale_cache=False,
            provider_status=getattr(provider_result, "provider_status", None),
            raw_json=getattr(provider_result, "raw_json", None),
        )

    @staticmethod
    def _prefer_result(
        current: GeocodingResult | None,
        candidate: GeocodingResult | None,
    ) -> GeocodingResult | None:
        if candidate is None:
            return current
        if current is None:
            return candidate

        current_score = float(current.confidence or 0.0)
        candidate_score = float(candidate.confidence or 0.0)

        if candidate.is_success and not current.is_success:
            return candidate
        if current.is_success and not candidate.is_success:
            return current
        if candidate_score > current_score:
            return candidate
        return current


def build_cache_payload_from_geocoding_result(result: GeocodingResult) -> GeocodeCachePayload:
    return build_geocode_cache_payload(
        address=result.raw_input_address,
        city=result.city,
        state=result.state,
        postal_code=result.postal_code,
        county=result.county,
        lat=result.lat,
        lng=result.lng,
        source=result.source,
        confidence=result.confidence,
        formatted_address=result.formatted_address,
        provider_response_json=result.raw_json if isinstance(result.raw_json, dict) else {"results": result.raw_json},
    )