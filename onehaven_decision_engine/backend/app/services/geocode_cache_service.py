
# backend/app/services/geocode_cache_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import GeocodeCache
from .address_normalization import make_normalized_cache_key


def _clean_text(value: Any) -> str | None:
    s = str(value or "").strip()
    return s or None


def _normalize_state(value: Any) -> str | None:
    s = _clean_text(value)
    if not s:
        return None
    if len(s) == 2:
        return s.upper()
    state_map = {
        "michigan": "MI",
        "ohio": "OH",
        "indiana": "IN",
        "illinois": "IL",
        "wisconsin": "WI",
        "pennsylvania": "PA",
        "new york": "NY",
        "california": "CA",
        "texas": "TX",
        "florida": "FL",
    }
    return state_map.get(s.lower()) or s[:2].upper()


def _normalize_county(value: Any) -> str | None:
    s = _clean_text(value)
    if not s:
        return None
    if s.lower().endswith(" county"):
        s = s[:-7].strip()
    return s or None


def _normalize_zip(value: Any) -> str | None:
    s = _clean_text(value)
    if not s:
        return None
    return s[:10]


@dataclass
class GeocodeCachePayload:
    normalized_address: str
    raw_address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    county: str | None = None
    lat: float | None = None
    lng: float | None = None
    source: str = "unknown"
    confidence: float | None = None
    formatted_address: str | None = None
    provider_response_json: dict[str, Any] | None = None


class GeocodeCacheService:
    def __init__(
        self,
        db: Session,
        *,
        ttl_hours: int | None = None,
        stale_after_hours: int | None = None,
    ) -> None:
        self.db = db
        self.ttl_hours = ttl_hours if ttl_hours is not None else int(settings.geocode_cache_ttl_hours)
        self.stale_after_hours = (
            stale_after_hours if stale_after_hours is not None else int(settings.geocode_stale_after_hours)
        )

    def make_key(
        self,
        *,
        address: str | None,
        city: str | None,
        state: str | None,
        postal_code: str | None,
    ) -> str:
        return make_normalized_cache_key(address, city, state, postal_code)

    def get_by_normalized_address(self, normalized_address: str | None) -> GeocodeCache | None:
        if not normalized_address:
            return None
        stmt = select(GeocodeCache).where(GeocodeCache.normalized_address == normalized_address)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_components(
        self,
        *,
        address: str | None,
        city: str | None,
        state: str | None,
        postal_code: str | None,
    ) -> GeocodeCache | None:
        key = self.make_key(address=address, city=city, state=state, postal_code=postal_code)
        return self.get_by_normalized_address(key)

    def is_expired(self, entry: GeocodeCache | None, *, now: datetime | None = None) -> bool:
        if entry is None:
            return True
        if entry.expires_at is None:
            return False
        now = now or datetime.utcnow()
        return entry.expires_at <= now

    def is_stale(self, entry: GeocodeCache | None, *, now: datetime | None = None) -> bool:
        if entry is None:
            return True
        now = now or datetime.utcnow()
        if entry.last_refreshed_at is None:
            return True
        return entry.last_refreshed_at <= now - timedelta(hours=self.stale_after_hours)

    def get_fresh_by_components(
        self,
        *,
        address: str | None,
        city: str | None,
        state: str | None,
        postal_code: str | None,
        touch_hit: bool = True,
    ) -> GeocodeCache | None:
        entry = self.get_by_components(
            address=address,
            city=city,
            state=state,
            postal_code=postal_code,
        )
        if entry is None or self.is_expired(entry):
            return None

        if touch_hit:
            self.touch_hit(entry)

        return entry

    def touch_hit(self, entry: GeocodeCache, *, commit: bool = True) -> GeocodeCache:
        entry.hit_count = int(entry.hit_count or 0) + 1
        entry.updated_at = datetime.utcnow()
        self.db.add(entry)
        if commit:
            try:
                self.db.commit()
                self.db.refresh(entry)
            except Exception:
                self.db.rollback()
                raise
        return entry

    def upsert(self, payload: GeocodeCachePayload, *, commit: bool = True) -> GeocodeCache:
        now = datetime.utcnow()
        expires_at = now + timedelta(hours=self.ttl_hours)

        payload = GeocodeCachePayload(
            normalized_address=payload.normalized_address,
            raw_address=_clean_text(payload.raw_address),
            city=_clean_text(payload.city),
            state=_normalize_state(payload.state),
            zip=_normalize_zip(payload.zip),
            county=_normalize_county(payload.county),
            lat=payload.lat,
            lng=payload.lng,
            source=_clean_text(payload.source) or "unknown",
            confidence=payload.confidence,
            formatted_address=_clean_text(payload.formatted_address),
            provider_response_json=payload.provider_response_json,
        )

        existing = self.get_by_normalized_address(payload.normalized_address)

        if existing is None:
            existing = GeocodeCache(
                normalized_address=payload.normalized_address,
                raw_address=payload.raw_address,
                city=payload.city,
                state=payload.state,
                zip=payload.zip,
                county=payload.county,
                lat=payload.lat,
                lng=payload.lng,
                source=payload.source or "unknown",
                confidence=payload.confidence,
                formatted_address=payload.formatted_address,
                provider_response_json=payload.provider_response_json,
                hit_count=0,
                last_refreshed_at=now,
                expires_at=expires_at,
                created_at=now,
                updated_at=now,
            )
            self.db.add(existing)
        else:
            existing.raw_address = payload.raw_address
            existing.city = payload.city
            existing.state = payload.state
            existing.zip = payload.zip
            existing.county = payload.county
            existing.lat = payload.lat
            existing.lng = payload.lng
            existing.source = payload.source or existing.source or "unknown"
            existing.confidence = payload.confidence
            existing.formatted_address = payload.formatted_address
            existing.provider_response_json = payload.provider_response_json
            existing.last_refreshed_at = now
            existing.expires_at = expires_at
            existing.updated_at = now
            self.db.add(existing)

        if commit:
            try:
                self.db.commit()
                self.db.refresh(existing)
            except Exception:
                self.db.rollback()
                raise

        return existing

    def invalidate(
        self,
        *,
        normalized_address: str | None = None,
        address: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        commit: bool = True,
    ) -> bool:
        key = normalized_address or self.make_key(
            address=address,
            city=city,
            state=state,
            postal_code=postal_code,
        )
        entry = self.get_by_normalized_address(key)
        if entry is None:
            return False

        entry.expires_at = datetime.utcnow() - timedelta(seconds=1)
        entry.updated_at = datetime.utcnow()
        self.db.add(entry)

        if commit:
            try:
                self.db.commit()
                self.db.refresh(entry)
            except Exception:
                self.db.rollback()
                raise

        return True

    def delete(
        self,
        *,
        normalized_address: str | None = None,
        address: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        commit: bool = True,
    ) -> bool:
        key = normalized_address or self.make_key(
            address=address,
            city=city,
            state=state,
            postal_code=postal_code,
        )
        entry = self.get_by_normalized_address(key)
        if entry is None:
            return False

        self.db.delete(entry)
        if commit:
            try:
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        return True


def build_geocode_cache_payload(
    *,
    address: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
    county: str | None = None,
    lat: float | None = None,
    lng: float | None = None,
    source: str = "unknown",
    confidence: float | None = None,
    formatted_address: str | None = None,
    provider_response_json: dict[str, Any] | None = None,
) -> GeocodeCachePayload:
    normalized_address = make_normalized_cache_key(address, city, state, postal_code)
    raw_address_parts = [p for p in [address, city, state, postal_code] if p]
    raw_address = ", ".join(raw_address_parts) if raw_address_parts else None

    return GeocodeCachePayload(
        normalized_address=normalized_address,
        raw_address=raw_address,
        city=_clean_text(city),
        state=_normalize_state(state),
        zip=_normalize_zip(postal_code),
        county=_normalize_county(county),
        lat=lat,
        lng=lng,
        source=_clean_text(source) or "unknown",
        confidence=confidence,
        formatted_address=_clean_text(formatted_address),
        provider_response_json=provider_response_json,
    )
