# backend/app/clients/nominatim.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

from ..config import settings


@dataclass(frozen=True)
class NominatimGeocodeResult:
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
    raw_json: dict[str, Any] | list[dict[str, Any]]


class NominatimClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        user_agent: str | None = None,
        email: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        self.base_url = (
            (base_url if base_url is not None else getattr(settings, "nominatim_base_url", "https://nominatim.openstreetmap.org"))
            .rstrip("/")
        )
        self.user_agent = user_agent or getattr(settings, "nominatim_user_agent", "OneHaven-Geocoder/1.0")
        self.email = email if email is not None else getattr(settings, "nominatim_email", None)
        self.timeout_seconds = int(
            timeout_seconds if timeout_seconds is not None else getattr(settings, "geocode_timeout_seconds", 10)
        )

    @property
    def is_enabled(self) -> bool:
        return bool((self.base_url or "").strip())

    def geocode(self, address: str) -> NominatimGeocodeResult | None:
        if not self.is_enabled:
            return None
        if not address or not address.strip():
            return None

        params = {
            "q": address,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
            "countrycodes": str(getattr(settings, "geocode_default_country_code", "us")).lower(),
        }

        if self.email:
            params["email"] = self.email

        query = urlencode(params)
        url = f"{self.base_url}/search?{query}"

        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
            method="GET",
        )

        with urlopen(request, timeout=self.timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
            payload = json.loads(raw_body)

        return self.parse_response(payload)

    def parse_response(self, payload: list[dict[str, Any]] | dict[str, Any]) -> NominatimGeocodeResult | None:
        if isinstance(payload, dict):
            data = [payload] if payload else []
        else:
            data = payload or []

        if not data:
            return NominatimGeocodeResult(
                source="nominatim",
                formatted_address=None,
                lat=None,
                lng=None,
                city=None,
                state=None,
                postal_code=None,
                county=None,
                confidence=0.0,
                provider_status="ZERO_RESULTS",
                raw_json=payload,
            )

        best = data[0]
        address = best.get("address") or {}

        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
        )

        state = address.get("state_code") or address.get("state")
        county = address.get("county")
        postal_code = address.get("postcode")

        confidence = self._map_confidence(best)

        return NominatimGeocodeResult(
            source="nominatim",
            formatted_address=best.get("display_name"),
            lat=self._as_float(best.get("lat")),
            lng=self._as_float(best.get("lon")),
            city=city,
            state=state,
            postal_code=postal_code,
            county=county,
            confidence=confidence,
            provider_status="OK",
            raw_json=payload,
        )

    @staticmethod
    def _map_confidence(item: dict[str, Any]) -> float:
        klass = str(item.get("class") or "").lower()
        item_type = str(item.get("type") or "").lower()
        importance = item.get("importance")

        try:
            importance_f = float(importance)
        except Exception:
            importance_f = 0.0

        if klass == "building":
            return 0.92
        if item_type in {"house", "residential", "apartments"}:
            return 0.90
        if klass == "place" and item_type in {"house", "address"}:
            return 0.88
        if importance_f >= 0.7:
            return 0.82
        if importance_f >= 0.4:
            return 0.74
        if importance_f >= 0.2:
            return 0.66
        return 0.58

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            return float(value)
        except Exception:
            return None