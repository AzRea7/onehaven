# backend/app/clients/google_geocode.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import requests

from ..config import settings


@dataclass(frozen=True)
class GoogleGeocodeResult:
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
    raw_json: dict[str, Any]


class GoogleGeocodeClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: int | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.google_geocode_api_key
        self.base_url = base_url if base_url is not None else settings.google_geocode_base_url
        self.timeout_seconds = (
            int(timeout_seconds) if timeout_seconds is not None else int(settings.geocode_timeout_seconds)
        )
        self.session = session or requests.Session()

    @property
    def is_enabled(self) -> bool:
        return bool((self.api_key or "").strip())

    def geocode(self, address: str) -> GoogleGeocodeResult | None:
        if not self.is_enabled:
            return None
        if not address or not address.strip():
            return None

        response = self.session.get(
            self.base_url,
            params={
                "address": address,
                "key": self.api_key,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        return self.parse_response(payload)

    def parse_response(self, payload: dict[str, Any]) -> GoogleGeocodeResult | None:
        status = payload.get("status")
        results = payload.get("results") or []

        if status != "OK" or not results:
            return GoogleGeocodeResult(
                source="google",
                formatted_address=None,
                lat=None,
                lng=None,
                city=None,
                state=None,
                postal_code=None,
                county=None,
                confidence=0.0,
                provider_status=str(status) if status is not None else None,
                raw_json=payload,
            )

        best = results[0]
        geometry = best.get("geometry") or {}
        location = geometry.get("location") or {}
        location_type = (geometry.get("location_type") or "").strip()

        city = None
        state = None
        postal_code = None
        county = None

        for component in best.get("address_components") or []:
            long_name = component.get("long_name")
            short_name = component.get("short_name")
            types = set(component.get("types") or [])

            if "locality" in types:
                city = long_name
            elif "postal_town" in types and not city:
                city = long_name
            elif "administrative_area_level_2" in types:
                county = long_name
            elif "administrative_area_level_1" in types:
                state = short_name or long_name
            elif "postal_code" in types:
                postal_code = long_name

        confidence = self._map_confidence(location_type)

        return GoogleGeocodeResult(
            source="google",
            formatted_address=best.get("formatted_address"),
            lat=self._as_float(location.get("lat")),
            lng=self._as_float(location.get("lng")),
            city=city,
            state=state,
            postal_code=postal_code,
            county=county,
            confidence=confidence,
            provider_status=str(status),
            raw_json=payload,
        )

    @staticmethod
    def _map_confidence(location_type: str) -> float:
        normalized = location_type.strip().upper()
        if normalized == "ROOFTOP":
            return 0.99
        if normalized == "RANGE_INTERPOLATED":
            return 0.90
        if normalized == "GEOMETRIC_CENTER":
            return 0.75
        if normalized == "APPROXIMATE":
            return 0.60
        return 0.50

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            return float(value)
        except Exception:
            return None
        