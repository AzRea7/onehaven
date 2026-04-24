from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.services.address_normalization import normalize_full_address, normalize_address_line1
from onehaven_platform.backend.src.services.geocode_cache_service import (
    GeocodeCachePayload,
    GeocodeCacheService,
    build_geocode_cache_payload,
)
from onehaven_platform.backend.src.adapters.intelligence_adapter import RentCastClient, RentCastSaleListingResult


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
            provider_response_json=(
                self.raw_json if isinstance(self.raw_json, dict) else {"results": self.raw_json}
            ),
        )


class GeocodingService:
    def __init__(
        self,
        db: Session,
        *,
        cache_service: GeocodeCacheService | None = None,
        rentcast_client: RentCastClient | None = None,
        provider_order: list[str] | None = None,
    ) -> None:
        self.db = db
        self.cache_service = cache_service or GeocodeCacheService(db)
        self.rentcast_client = rentcast_client or self._build_rentcast_client()
        self.provider_order = ["rentcast"]

    def _build_rentcast_client(self) -> RentCastClient | None:
        api_key = (
            getattr(settings, "rentcast_api_key", None)
            or getattr(settings, "rentcast_ingestion_api_key", None)
            or ""
        )
        api_key = str(api_key or "").strip()
        if not api_key:
            return None
        try:
            return RentCastClient(api_key)
        except Exception:
            return None

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

        raw_input_address = normalized.full_address

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

        mapped = self._try_rentcast_lookup(
            normalized_address=normalized.full_address,
            raw_input_address=raw_input_address,
            address=normalized.address_line1,
            city=normalized.city or city,
            state=normalized.state or state,
            postal_code=normalized.postal_code or postal_code,
        )

        if not mapped or not mapped.is_success:
            return None

        self.cache_service.upsert(mapped.to_cache_payload())
        return mapped

    def _resolve_provider_names(self) -> list[str]:
        return ["rentcast"]

    def _try_rentcast_lookup(
        self,
        *,
        normalized_address: str,
        raw_input_address: str,
        address: str | None,
        city: str | None,
        state: str | None,
        postal_code: str | None,
    ) -> GeocodingResult | None:
        if self.rentcast_client is None:
            return None

        try:
            clean_address = normalize_address_line1(address or normalized_address)
            listing = self.rentcast_client.sale_listing_lookup(
                address=str(clean_address or normalized_address).strip(),
                city=city,
                state=state,
                zip_code=postal_code,
                limit=10,
                status="Active",
                allow_status_fallback=True,
                allow_location_fallback=True,
            )
        except Exception:
            if getattr(settings, "geocode_fail_open", True):
                return None
            raise

        if not listing or listing.latitude is None or listing.longitude is None:
            return None

        return self._map_rentcast_listing_result(
            normalized_address=normalized_address,
            raw_input_address=raw_input_address,
            listing=listing,
        )

    def _map_rentcast_listing_result(
        self,
        *,
        normalized_address: str,
        raw_input_address: str,
        listing: RentCastSaleListingResult,
    ) -> GeocodingResult:
        return GeocodingResult(
            normalized_address=normalized_address,
            raw_input_address=raw_input_address,
            formatted_address=listing.formatted_address,
            lat=listing.latitude,
            lng=listing.longitude,
            city=listing.city,
            state=listing.state,
            postal_code=listing.zip_code,
            county=listing.county,
            source="rentcast",
            confidence=0.80,
            cache_hit=False,
            is_stale_cache=False,
            provider_status="RENTCAST_LISTING_MATCH",
            raw_json=listing.raw_json,
        )

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
        provider_response_json=(
            result.raw_json if isinstance(result.raw_json, dict) else {"results": result.raw_json}
        ),
    )