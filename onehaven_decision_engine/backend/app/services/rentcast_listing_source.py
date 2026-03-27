from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from ..config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RentCastListingFetchResult:
    rows: list[dict[str, Any]]
    next_cursor: dict[str, Any]
    raw_count: int
    page_scanned: int = 1
    shard_scanned: int = 1
    sort_mode: str = "newest"
    exhausted: bool = False
    page_fingerprint: str | None = None
    page_changed: bool = True
    provider_cursor: dict[str, Any] | None = None
    query_variant: dict[str, Any] | None = None


class RentCastListingSource:
    provider = "rentcast"
    SALE_LISTINGS_URL = "https://api.rentcast.io/v1/listings/sale"

    def _get_api_key(self, credentials: dict[str, Any]) -> str:
        key = (
            (credentials or {}).get("api_key")
            or os.getenv("RENTCAST_INGESTION_API_KEY")
            or os.getenv("RENTCAST_API_KEY")
            or getattr(settings, "rentcast_api_key", None)
            or ""
        ).strip()
        if not key:
            raise ValueError(
                "Missing RentCast API key. "
                "Set credentials_json.api_key on the ingestion source "
                "or define RENTCAST_INGESTION_API_KEY / RENTCAST_API_KEY."
            )
        return key

    def _headers(self, api_key: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "X-Api-Key": api_key,
        }

    def _safe_int(self, value: Any, default: int | None = None) -> int | None:
        if value is None or value == "":
            return default
        try:
            return int(float(value))
        except Exception:
            return default

    def _safe_float(self, value: Any, default: float | None = None) -> float | None:
        if value is None or value == "":
            return default
        try:
            return float(value)
        except Exception:
            return default

    def _normalize_optional_text(self, value: Any) -> str | None:
        s = str(value or "").strip()
        if not s:
            return None
        if s.lower() in {"none", "null", "all", "any"}:
            return None
        return s

    def _normalize_property_type(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return "single_family"

        raw = raw.replace("-", " ").replace("_", " ").strip()

        if raw in {
            "single family",
            "single family home",
            "single family residential",
            "house",
            "detached",
            "sfh",
            "residential",
        }:
            return "single_family"

        if raw in {
            "multi family",
            "multifamily",
            "duplex",
            "triplex",
            "fourplex",
            "2 family",
            "3 family",
            "4 family",
        }:
            return "multi_family"

        return raw.replace(" ", "_")

    def _to_provider_property_type(self, value: str) -> str | None:
        norm = self._normalize_property_type(value)
        if norm == "single_family":
            return "Single Family"
        if norm == "multi_family":
            return "Multi Family"
        if norm == "condo":
            return "Condo"
        if norm == "townhouse":
            return "Townhouse"
        if norm == "manufactured":
            return "Manufactured"
        if norm == "land":
            return "Land"
        return None

    def _provider_property_type_param(
        self,
        config: dict[str, Any],
    ) -> str | None:
        property_types = config.get("property_types")
        if isinstance(property_types, str):
            property_types = [x.strip() for x in property_types.split(",") if x.strip()]

        provider_types: list[str] = []
        seen: set[str] = set()

        for item in property_types or []:
            mapped = self._to_provider_property_type(str(item))
            if mapped and mapped not in seen:
                seen.add(mapped)
                provider_types.append(mapped)

        if not provider_types:
            single_property_type = self._normalize_optional_text(config.get("property_type"))
            if single_property_type:
                mapped = self._to_provider_property_type(single_property_type)
                if mapped:
                    provider_types.append(mapped)

        return "|".join(provider_types) if provider_types else None

    def _provider_price_param(self, config: dict[str, Any]) -> str | None:
        max_price = self._safe_int(config.get("max_price"), None)
        if max_price is None or max_price <= 0:
            return None
        return f"0:{max_price}"

    def _coerce_zip_codes(self, config: dict[str, Any]) -> list[str]:
        out: list[str] = []

        single = self._normalize_optional_text(config.get("zip_code"))
        if single:
            out.append(single)

        raw_many = config.get("zip_codes")
        if isinstance(raw_many, str):
            raw_many = [x.strip() for x in raw_many.split(",") if x.strip()]

        if isinstance(raw_many, list):
            for z in raw_many:
                zs = self._normalize_optional_text(z)
                if zs:
                    out.append(zs)

        deduped: list[str] = []
        seen: set[str] = set()
        for z in out:
            if z not in seen:
                deduped.append(z)
                seen.add(z)

        return deduped

    def _coerce_photo_rows(self, raw: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not raw:
            return out

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    url = item.strip()
                    if url:
                        out.append({"url": url, "kind": "unknown"})
                    continue

                if isinstance(item, dict):
                    url = str(
                        item.get("url")
                        or item.get("href")
                        or item.get("photoUrl")
                        or item.get("imageUrl")
                        or item.get("src")
                        or ""
                    ).strip()
                    if not url:
                        continue

                    kind = str(
                        item.get("kind")
                        or item.get("category")
                        or item.get("type")
                        or "unknown"
                    ).strip() or "unknown"
                    out.append({"url": url, "kind": kind})

        return out

    def _pick_address(self, item: dict[str, Any]) -> str:
        return str(
            item.get("formattedAddress")
            or item.get("address")
            or item.get("addressLine1")
            or item.get("streetAddress")
            or ""
        ).strip()

    def _pick_city(self, item: dict[str, Any]) -> str:
        return str(item.get("city") or "").strip()

    def _pick_county(self, item: dict[str, Any]) -> Optional[str]:
        county = str(
            item.get("county")
            or item.get("countyName")
            or item.get("county_name")
            or ""
        ).strip()
        if not county:
            return None
        if county.lower().endswith(" county"):
            county = county[:-7].strip()
        return county or None

    def _pick_state(self, item: dict[str, Any]) -> str:
        return str(item.get("state") or "MI").strip() or "MI"

    def _pick_zip(self, item: dict[str, Any]) -> str:
        return str(
            item.get("zipCode")
            or item.get("postalCode")
            or item.get("zip")
            or ""
        ).strip()

    def _pick_external_id(self, item: dict[str, Any]) -> str:
        return str(
            item.get("id")
            or item.get("listingId")
            or item.get("mlsNumber")
            or item.get("propertyId")
            or item.get("formattedAddress")
            or ""
        ).strip()

    def _pick_external_url(self, item: dict[str, Any]) -> Optional[str]:
        val = str(
            item.get("listingUrl")
            or item.get("url")
            or item.get("propertyUrl")
            or ""
        ).strip()
        return val or None

    def _pick_update_marker(self, item: dict[str, Any]) -> str | None:
        for key in (
            "lastSeen",
            "updatedAt",
            "updatedDate",
            "modifiedDate",
            "lastModified",
            "priceChangeDate",
            "listedDate",
            "daysOnMarket",
        ):
            value = item.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    def _hash_payload(self, payload: Any) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_page_fingerprint(self, raw_rows: list[dict[str, Any]]) -> str:
        digest_rows: list[dict[str, Any]] = []
        for item in raw_rows:
            digest_rows.append(
                {
                    "id": self._pick_external_id(item),
                    "address": self._pick_address(item),
                    "city": self._pick_city(item),
                    "state": self._pick_state(item),
                    "zip": self._pick_zip(item),
                    "price": self._safe_float(item.get("price") or item.get("listPrice"), None),
                    "updated": self._pick_update_marker(item),
                }
            )
        return self._hash_payload(digest_rows)

    def _resolve_cursor(
        self,
        *,
        runtime_config: dict[str, Any],
        cursor: dict[str, Any] | None,
        explicit_page: int | None,
        explicit_shard: int | None,
        explicit_sort_mode: str | None,
    ) -> dict[str, Any]:
        runtime_cursor = runtime_config.get("market_cursor")
        base = {}
        if isinstance(runtime_cursor, dict):
            base.update(runtime_cursor)
        if isinstance(cursor, dict):
            base.update(cursor)

        page = self._safe_int(
            explicit_page if explicit_page is not None else base.get("page"),
            1,
        ) or 1
        shard = self._safe_int(
            explicit_shard if explicit_shard is not None else base.get("shard"),
            1,
        ) or 1
        sort_mode = str(
            explicit_sort_mode or base.get("sort_mode") or "newest"
        ).strip().lower() or "newest"

        return {
            "page": max(1, page),
            "shard": max(1, shard),
            "sort_mode": sort_mode,
            "market_slug": str(
                base.get("market_slug") or runtime_config.get("market_slug") or ""
            ).strip().lower() or None,
            "page_fingerprint": str(base.get("page_fingerprint") or "").strip() or None,
            "provider_cursor": (
                dict(base.get("provider_cursor") or {})
                if isinstance(base.get("provider_cursor"), dict)
                else None
            ),
        }

    def _to_canonical_row(self, item: dict[str, Any]) -> dict[str, Any]:
        photos = self._coerce_photo_rows(
            item.get("photos")
            or item.get("images")
            or item.get("media")
        )

        property_type_raw = item.get("propertyType") or item.get("type") or "single_family"

        return {
            "external_record_id": self._pick_external_id(item),
            "external_url": self._pick_external_url(item),
            "address": self._pick_address(item),
            "city": self._pick_city(item),
            "county": self._pick_county(item),
            "state": self._pick_state(item),
            "zip": self._pick_zip(item),
            "bedrooms": self._safe_int(item.get("bedrooms") or item.get("beds"), 0) or 0,
            "bathrooms": self._safe_float(item.get("bathrooms") or item.get("baths"), 0.0) or 0.0,
            "square_feet": self._safe_int(
                item.get("squareFootage") or item.get("livingArea") or item.get("sqft"),
                None,
            ),
            "year_built": self._safe_int(item.get("yearBuilt"), None),
            "property_type": self._normalize_property_type(property_type_raw),
            "asking_price": self._safe_float(item.get("price") or item.get("listPrice"), 0.0) or 0.0,
            "estimated_purchase_price": self._safe_float(item.get("price") or item.get("listPrice"), None),
            "rehab_estimate": 0,
            "market_rent_estimate": None,
            "section8_fmr": None,
            "approved_rent_ceiling": None,
            "inventory_count": None,
            "photos": photos,
            "source": "rentcast",
            "raw_json": item,
        }

    def _build_query_params(
        self,
        config: dict[str, Any],
        page: int,
        limit: int,
        sort_mode: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "page": max(1, int(page)),
            "limit": max(1, min(int(limit), 500)),
            "status": "Active",
        }

        state = self._normalize_optional_text(config.get("state")) or "MI"
        city = self._normalize_optional_text(config.get("city"))
        county = self._normalize_optional_text(config.get("county"))

        params["state"] = state
        if city:
            params["city"] = city
        elif county:
            params["county"] = county

        provider_property_type = self._provider_property_type_param(config)
        if provider_property_type:
            params["propertyType"] = provider_property_type

        provider_price = self._provider_price_param(config)
        if provider_price:
            params["price"] = provider_price

        _ = sort_mode
        return params

    def load_rows_page(
        self,
        *,
        credentials: dict[str, Any],
        runtime_config: dict[str, Any],
        cursor: dict[str, Any] | None,
        market_slug: str | None = None,
        city: str | None = None,
        county: str | None = None,
        state: str | None = None,
        page: int | None = None,
        shard: int | None = None,
        sort_mode: str | None = None,
        limit: int | None = None,
    ) -> RentCastListingFetchResult:
        api_key = self._get_api_key(credentials or {})
        config = dict(runtime_config or {})

        if market_slug is not None:
            config["market_slug"] = market_slug
        if city is not None:
            config["city"] = city
        if county is not None:
            config["county"] = county
        if state is not None:
            config["state"] = state

        resolved_cursor = self._resolve_cursor(
            runtime_config=config,
            cursor=cursor,
            explicit_page=page,
            explicit_shard=shard,
            explicit_sort_mode=sort_mode,
        )

        page_num = self._safe_int(resolved_cursor.get("page"), 1) or 1
        shard_num = self._safe_int(resolved_cursor.get("shard"), 1) or 1
        sort_mode_value = str(resolved_cursor.get("sort_mode") or "newest")
        limit_value = self._safe_int(
            limit if limit is not None else config.get("limit"),
            getattr(settings, "ingestion_provider_page_limit", 50),
        ) or 50

        params = self._build_query_params(config, page_num, limit_value, sort_mode_value)

        logger.info(
            "rentcast_fetch params=%s cursor=%s market_slug=%s",
            params,
            resolved_cursor,
            config.get("market_slug"),
        )

        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                self.SALE_LISTINGS_URL,
                params=params,
                headers=self._headers(api_key),
            )
            response.raise_for_status()
            payload = response.json()

        raw_rows: list[dict[str, Any]]
        if isinstance(payload, list):
            raw_rows = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            raw_rows = [x for x in payload.get("listings", []) if isinstance(x, dict)]
        else:
            raw_rows = []

        rows = [self._to_canonical_row(item) for item in raw_rows]
        page_fingerprint = self._build_page_fingerprint(raw_rows)
        prior_fingerprint = str(resolved_cursor.get("page_fingerprint") or "").strip() or None
        page_changed = prior_fingerprint != page_fingerprint if prior_fingerprint else True
        exhausted = len(raw_rows) < int(limit_value)

        next_cursor = {
            "market_slug": str(config.get("market_slug") or "").strip().lower() or None,
            "page": page_num if exhausted else page_num + 1,
            "shard": shard_num,
            "sort_mode": sort_mode_value,
            "page_fingerprint": page_fingerprint,
            "page_changed": page_changed,
            "provider_cursor": None,
        }

        return RentCastListingFetchResult(
            rows=rows,
            next_cursor=next_cursor,
            raw_count=len(raw_rows),
            page_scanned=page_num,
            shard_scanned=shard_num,
            sort_mode=sort_mode_value,
            exhausted=exhausted,
            page_fingerprint=page_fingerprint,
            page_changed=page_changed,
            provider_cursor=None,
            query_variant=dict(config.get("_query_variant") or {}) or None,
        )