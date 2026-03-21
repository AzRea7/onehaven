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

        # 🔥 SAFE SETTINGS ACCESS (FIX)
        default_order = ["google", "nominatim"]

        try:
            cfg = getattr(settings, "geocode_provider_order_list", default_order)
            if isinstance(cfg, str):
                cfg = [p.strip() for p in cfg.split(",") if p.strip()]
            self.provider_order = provider_order or list(cfg)
        except Exception:
            self.provider_order = provider_order or default_order

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

        # 🔥 SAFE SETTINGS ACCESS
        allow_fallback = getattr(settings, "geocode_allow_fallback_providers", True)
        confidence_threshold = float(getattr(settings, "geocode_min_confidence", 0.0))

        if allow_fallback_providers is not None:
            allow_fallback = bool(allow_fallback_providers)

        if min_confidence is not None:
            confidence_threshold = float(min_confidence)

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

        for provider_name in provider_names:
            provider = self._get_provider(provider_name)
            if provider is None or not provider.is_enabled:
                continue

            try:
                provider_result = provider.geocode(normalized.full_address)
            except Exception:
                if getattr(settings, "geocode_fail_open", True):
                    continue
                raise

            mapped = self._map_provider_result(
                normalized_address=normalized.full_address,
                raw_input_address=raw_input_address,
                provider_result=provider_result,
            )

            if not mapped:
                continue

            if mapped.is_success:
                self.cache_service.upsert(mapped.to_cache_payload())

                if (mapped.confidence or 0.0) >= confidence_threshold:
                    return mapped

                best_result = self._prefer_result(best_result, mapped)

                if not allow_fallback:
                    return mapped

        return best_result

    def _resolve_provider_names(self) -> list[str]:
        ordered: list[str] = []
        for name in self.provider_order:
            n = (name or "").strip().lower()
            if n and n not in ordered:
                ordered.append(n)
        return ordered or ["nominatim"]

    def _get_provider(self, provider_name: str) -> GeocodeProvider | None:
        if provider_name == "google":
            self.google_client.source = "google"
            return self.google_client
        if provider_name == "nominatim":
            self.nominatim_client.source = "nominatim"
            return self.nominatim_client
        return None

    def _map_provider_result(
        self,
        *,
        normalized_address: str,
        raw_input_address: str,
        provider_result: Any,
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
    def _prefer_result(current, candidate):
        if not candidate:
            return current
        if not current:
            return candidate
        if (candidate.confidence or 0) > (current.confidence or 0):
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