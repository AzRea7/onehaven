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

    def lookup_exact_address(
        self,
        *,
        credentials: dict[str, Any],
        address: str,
        city: str | None = None,
        state: str | None = None,
        zip_code: str | None = None,
        limit: int = 10,
        status: str | None = "Active",
        allow_status_fallback: bool = True,
        allow_location_fallback: bool = True,
    ) -> dict[str, Any] | None:
        """
        Exact-address lookup with explicit listing-status truth.

        Important behavior:
        - We never do a "status omitted" fallback when status truth matters.
        - If caller asks for Active and allows fallback, we only try Inactive next.
        - Location fallback is still allowed, but remains status-explicit.
        """
        api_key = self._get_api_key(credentials or {})
        attempts: list[dict[str, Any]] = []

        normalized_status = str(status or "").strip() or None

        search_variants: list[dict[str, Any]] = [
            {
                "address": str(address or "").strip(),
                "city": str(city).strip() if city else None,
                "state": str(state).strip() if state else None,
                "zip_code": str(zip_code).strip() if zip_code else None,
                "status": normalized_status,
                "label": "strict_active" if normalized_status == "Active" else "strict_any_status",
            }
        ]

        if allow_status_fallback and normalized_status == "Active":
            search_variants.append(
                {
                    "address": str(address or "").strip(),
                    "city": str(city).strip() if city else None,
                    "state": str(state).strip() if state else None,
                    "zip_code": str(zip_code).strip() if zip_code else None,
                    "status": "Inactive",
                    "label": "strict_inactive",
                }
            )

        if allow_location_fallback:
            search_variants.append(
                {
                    "address": str(address or "").strip(),
                    "city": None,
                    "state": None,
                    "zip_code": None,
                    "status": normalized_status,
                    "label": "address_only_active" if normalized_status == "Active" else "address_only_any_status",
                }
            )

            if allow_status_fallback and normalized_status == "Active":
                search_variants.append(
                    {
                        "address": str(address or "").strip(),
                        "city": None,
                        "state": None,
                        "zip_code": None,
                        "status": "Inactive",
                        "label": "address_only_inactive",
                    }
                )

        seen_fingerprints: set[tuple[str, str, str, str, str]] = set()
        deduped_variants: list[dict[str, Any]] = []

        for variant in search_variants:
            fp = (
                str(variant.get("address") or "").strip().lower(),
                str(variant.get("city") or "").strip().lower(),
                str(variant.get("state") or "").strip().lower(),
                str(variant.get("zip_code") or "").strip().lower(),
                str(variant.get("status") or "").strip().lower(),
            )
            if fp in seen_fingerprints:
                continue
            seen_fingerprints.add(fp)
            deduped_variants.append(variant)

        for variant in deduped_variants:
            rows = self._fetch_exact_address_rows(
                api_key=api_key,
                address=variant["address"],
                city=variant.get("city"),
                state=variant.get("state"),
                zip_code=variant.get("zip_code"),
                limit=limit,
                status=variant.get("status"),
            )

            attempts.append(
                {
                    "label": variant["label"],
                    "status": variant.get("status"),
                    "city": variant.get("city"),
                    "state": variant.get("state"),
                    "zip_code": variant.get("zip_code"),
                    "row_count": len(rows),
                }
            )

            if not rows:
                continue

            ranked = sorted(
                rows,
                key=lambda row: self._score_exact_address_match(
                    row=row,
                    address=address,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                ),
                reverse=True,
            )

            best = ranked[0]
            if self._score_exact_address_match(
                row=best,
                address=address,
                city=city,
                state=state,
                zip_code=zip_code,
            ) <= 0:
                continue

            enriched = dict(best)
            enriched.setdefault("_lookup_attempts", attempts)
            enriched["_resolved_lookup_status"] = variant.get("status")
            return enriched

        return None

    def _fetch_exact_address_rows(
        self,
        *,
        api_key: str,
        address: str,
        city: str | None,
        state: str | None,
        zip_code: str | None,
        limit: int,
        status: str | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "address": str(address or "").strip(),
            "limit": max(1, min(int(limit or 10), 50)),
        }
        if status:
            params["status"] = status
        if city:
            params["city"] = str(city).strip()
        if state:
            params["state"] = str(state).strip()
        if zip_code:
            params["zipCode"] = str(zip_code).strip()

        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                self.SALE_LISTINGS_URL,
                params=params,
                headers=self._headers(api_key),
            )
            response.raise_for_status()
            payload = response.json()

        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            return [x for x in payload.get("listings", []) if isinstance(x, dict)]
        return []

    def _normalize_match_string(self, value: Any) -> str:
        return " ".join(str(value or "").strip().lower().replace(",", " ").split())

    def _score_exact_address_match(
        self,
        *,
        row: dict[str, Any],
        address: str,
        city: str | None,
        state: str | None,
        zip_code: str | None,
    ) -> int:
        target_address = self._normalize_match_string(address)
        target_city = self._normalize_match_string(city)
        target_state = self._normalize_match_string(state)
        target_zip = str(zip_code or "").strip()

        formatted = self._normalize_match_string(row.get("formattedAddress"))
        line1 = self._normalize_match_string(row.get("addressLine1"))
        row_city = self._normalize_match_string(row.get("city"))
        row_state = self._normalize_match_string(row.get("state"))
        row_zip = str(row.get("zipCode") or "").strip()

        score = 0
        if formatted == target_address or line1 == target_address:
            score += 5
        if target_address and target_address in formatted:
            score += 3
        if target_city and row_city == target_city:
            score += 2
        if target_state and row_state == target_state:
            score += 1
        if target_zip and row_zip == target_zip:
            score += 2
        return score

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
        return None

    def _provider_property_type_param(self, config: dict[str, Any]) -> str | None:
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

    def _normalize_query_strategy(self, value: Any) -> str:
        raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        allowed = {
            "city_then_zip",
            "zip_then_city",
            "zip_only",
            "city_only",
            "city_and_zip",
            "broad_then_zip",
        }
        if raw in allowed:
            return raw
        return "city_then_zip"

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
            "status",
            "statusText",
            "lastSeenDate",
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
                    "status": str(item.get("status") or item.get("statusText") or "").strip() or None,
                    "updated": self._pick_update_marker(item),
                    "url": self._pick_external_url(item),
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
        base: dict[str, Any] = {}
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

    def _append_variant(
        self,
        variants: list[dict[str, Any]],
        *,
        key: str,
        scope: str,
        query: dict[str, Any],
        post_filter: dict[str, Any],
        strict_zip_match: bool = False,
    ) -> None:
        variants.append(
            {
                "key": key,
                "scope": scope,
                "query": query,
                "post_filter": post_filter,
                "strict_zip_match": bool(strict_zip_match),
            }
        )

    def _build_query_variants(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        state = self._normalize_optional_text(config.get("state")) or "MI"
        city = self._normalize_optional_text(config.get("city"))
        county = self._normalize_optional_text(config.get("county"))
        zip_codes = self._coerce_zip_codes(config)
        query_strategy = self._normalize_query_strategy(config.get("query_strategy"))

        variants: list[dict[str, Any]] = []

        def add_city(strict_zip_match: bool = False, broad: bool = False) -> None:
            if not city:
                return
            suffix = ":broad" if broad else ""
            self._append_variant(
                variants,
                key=f"city:{state.lower()}:{city.lower()}{suffix}",
                scope="city",
                query={"state": state, "city": city},
                post_filter={
                    "state": state,
                    "city": city,
                    "county": county,
                    "zip_codes": zip_codes,
                    "broad_city_match": bool(broad),
                },
                strict_zip_match=strict_zip_match,
            )

        def add_zip() -> None:
            for zip_code in zip_codes:
                self._append_variant(
                    variants,
                    key=f"zip:{state.lower()}:{zip_code}",
                    scope="zip",
                    query={"state": state, "zipCode": zip_code},
                    post_filter={
                        "state": state,
                        "zip_code": zip_code,
                        "city": city,
                        "county": county,
                    },
                    strict_zip_match=True,
                )

        if query_strategy == "zip_only":
            add_zip()
        elif query_strategy == "city_only":
            add_city(strict_zip_match=False)
        elif query_strategy == "zip_then_city":
            add_zip()
            add_city(strict_zip_match=False)
        elif query_strategy == "city_and_zip":
            add_city(strict_zip_match=True)
            add_zip()
        elif query_strategy == "broad_then_zip":
            add_city(strict_zip_match=False, broad=True)
            add_zip()
        else:
            add_city(strict_zip_match=False)
            add_zip()

        if not variants:
            self._append_variant(
                variants,
                key=f"state:{state.lower()}",
                scope="state",
                query={"state": state},
                post_filter={"state": state, "city": city, "county": county, "zip_codes": zip_codes},
                strict_zip_match=False,
            )

        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for variant in variants:
            key = str(variant.get("key") or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(variant)

        return deduped

    def _variant_for_shard(self, variants: list[dict[str, Any]], shard: int) -> dict[str, Any]:
        if not variants:
            raise ValueError("No query variants available for RentCast fetch.")
        index = max(0, int(shard) - 1)
        if index >= len(variants):
            index = len(variants) - 1
        return variants[index]

    def _row_matches_variant(self, item: dict[str, Any], variant: dict[str, Any]) -> bool:
        post_filter = dict(variant.get("post_filter") or {})
        expected_state = self._normalize_optional_text(post_filter.get("state"))
        expected_city = self._normalize_optional_text(post_filter.get("city"))
        expected_county = self._normalize_optional_text(post_filter.get("county"))
        expected_zip = self._normalize_optional_text(post_filter.get("zip_code"))
        expected_zip_codes = {
            str(z).strip() for z in (post_filter.get("zip_codes") or []) if str(z).strip()
        }
        scope = str(variant.get("scope") or "").strip().lower()
        strict_zip_match = bool(variant.get("strict_zip_match", False))

        row_state = self._normalize_optional_text(self._pick_state(item))
        row_city = self._normalize_optional_text(self._pick_city(item))
        row_county = self._normalize_optional_text(self._pick_county(item))
        row_zip = self._normalize_optional_text(self._pick_zip(item))

        if expected_state and (row_state or "").lower() != expected_state.lower():
            return False

        if scope == "city":
            if expected_city and (row_city or "").lower() != expected_city.lower():
                return False
            if strict_zip_match and expected_zip_codes and (row_zip or "") not in expected_zip_codes:
                return False
            if expected_county and row_county and row_county.lower() != expected_county.lower():
                return False
            return True

        if scope == "zip":
            if expected_zip and (row_zip or "") != expected_zip:
                return False
            if expected_city and row_city and row_city.lower() != expected_city.lower():
                return False
            return True

        if expected_city and row_city and row_city.lower() != expected_city.lower():
            return False
        if expected_county and row_county and row_county.lower() != expected_county.lower():
            return False
        if strict_zip_match and expected_zip_codes and (row_zip or "") not in expected_zip_codes:
            return False

        return True

    def _parse_total_count(self, response: httpx.Response) -> int | None:
        raw = response.headers.get("X-Total-Count")
        if raw is None:
            return None
        return self._safe_int(raw, None)

    def _build_query_params(
        self,
        config: dict[str, Any],
        variant: dict[str, Any],
        page: int,
        limit: int,
        sort_mode: str | None = None,
    ) -> dict[str, Any]:
        limit_value = max(1, min(int(limit), 500))
        offset = max(0, (max(1, int(page)) - 1) * limit_value)
        params: dict[str, Any] = {
            "limit": limit_value,
            "offset": offset,
            "status": "Active",
            "includeTotalCount": "true",
        }

        query = dict(variant.get("query") or {})
        state = self._normalize_optional_text(query.get("state")) or "MI"
        city = self._normalize_optional_text(query.get("city"))
        zip_code = self._normalize_optional_text(query.get("zipCode"))

        params["state"] = state
        if city:
            params["city"] = city
        if zip_code:
            params["zipCode"] = zip_code

        provider_property_type = self._provider_property_type_param(config)
        if provider_property_type:
            params["propertyType"] = provider_property_type

        provider_price = self._provider_price_param(config)
        if provider_price:
            params["price"] = provider_price

        normalized_sort = str(sort_mode or "newest").strip().lower()
        if normalized_sort in {"newest", "latest"}:
            params["sort"] = "listedDate:desc"
        elif normalized_sort in {"oldest"}:
            params["sort"] = "listedDate:asc"
        elif normalized_sort in {"price_desc", "highest_price"}:
            params["sort"] = "price:desc"
        elif normalized_sort in {"price_asc", "lowest_price"}:
            params["sort"] = "price:asc"

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
        sort_mode_value = str(resolved_cursor.get("sort_mode") or "newest").strip().lower() or "newest"
        limit_value = self._safe_int(
            limit if limit is not None else config.get("limit"),
            getattr(settings, "ingestion_provider_page_limit", 50),
        ) or 50

        variants = self._build_query_variants(config)
        active_variant = self._variant_for_shard(variants, shard_num)

        params = self._build_query_params(
            config=config,
            variant=active_variant,
            page=page_num,
            limit=limit_value,
            sort_mode=sort_mode_value,
        )

        logger.info(
            "rentcast_fetch params=%s cursor=%s market_slug=%s variant=%s variant_count=%s",
            params,
            resolved_cursor,
            config.get("market_slug"),
            active_variant,
            len(variants),
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

        matched_raw_rows = [item for item in raw_rows if self._row_matches_variant(item, active_variant)]
        rows = [self._to_canonical_row(item) for item in matched_raw_rows]

        page_fingerprint = self._build_page_fingerprint(matched_raw_rows)

        prior_fingerprint = str(resolved_cursor.get("page_fingerprint") or "").strip() or None
        prior_provider_cursor = dict(resolved_cursor.get("provider_cursor") or {})
        prior_variant_key = str(prior_provider_cursor.get("variant_key") or "").strip() or None
        current_variant_key = str(active_variant.get("key") or "").strip() or None

        if prior_variant_key and prior_variant_key != current_variant_key:
            page_changed = True
        else:
            page_changed = prior_fingerprint != page_fingerprint if prior_fingerprint else True

        total_count = self._parse_total_count(response)
        offset = max(0, (page_num - 1) * limit_value)

        variant_exhausted = False
        if total_count is not None:
            variant_exhausted = (offset + len(raw_rows)) >= total_count
        if len(raw_rows) < int(limit_value):
            variant_exhausted = True
        if len(raw_rows) == 0:
            variant_exhausted = True

        has_more_variants = shard_num < len(variants)

        base_provider_cursor = {
            "variant_key": current_variant_key,
            "variant_count": len(variants),
            "total_count": total_count,
            "offset": offset,
            "matched_count": len(matched_raw_rows),
            "raw_count": len(raw_rows),
        }

        if variant_exhausted and has_more_variants:
            next_cursor = {
                "market_slug": str(config.get("market_slug") or "").strip().lower() or None,
                "page": 1,
                "shard": shard_num + 1,
                "sort_mode": sort_mode_value,
                "page_fingerprint": None,
                "page_changed": True,
                "provider_cursor": {
                    **base_provider_cursor,
                    "variant_key": str(variants[shard_num].get("key") or "").strip() or None,
                    "offset": 0,
                },
            }
            exhausted = False
        elif variant_exhausted:
            next_cursor = {
                "market_slug": str(config.get("market_slug") or "").strip().lower() or None,
                "page": page_num,
                "shard": shard_num,
                "sort_mode": sort_mode_value,
                "page_fingerprint": page_fingerprint,
                "page_changed": page_changed,
                "provider_cursor": base_provider_cursor,
            }
            exhausted = True
        else:
            next_cursor = {
                "market_slug": str(config.get("market_slug") or "").strip().lower() or None,
                "page": page_num + 1,
                "shard": shard_num,
                "sort_mode": sort_mode_value,
                "page_fingerprint": page_fingerprint,
                "page_changed": page_changed,
                "provider_cursor": {
                    **base_provider_cursor,
                    "offset": offset + limit_value,
                },
            }
            exhausted = False

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
            provider_cursor=dict(next_cursor.get("provider_cursor") or {}),
            query_variant=dict(active_variant),
        )
