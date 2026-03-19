from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RentCastListingFetchResult:
    rows: list[dict[str, Any]]
    next_cursor: dict[str, Any]
    raw_count: int


class RentCastListingSource:
    provider = "rentcast"

    SALE_LISTINGS_URL = "https://api.rentcast.io/v1/listings/sale"

    def _get_api_key(self, credentials: dict[str, Any]) -> str:
        key = (
            (credentials or {}).get("api_key")
            or os.getenv("RENTCAST_INGESTION_API_KEY")
            or os.getenv("RENTCAST_API_KEY")
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

    def _coerce_price_buckets(self, config: dict[str, Any]) -> list[tuple[float | None, float | None]]:
        raw = config.get("price_buckets")
        out: list[tuple[float | None, float | None]] = []

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    lo = self._safe_float(item[0], None)
                    hi = self._safe_float(item[1], None)
                    out.append((lo, hi))

        if out:
            return out

        min_price = self._safe_float(config.get("min_price"), None)
        max_price = self._safe_float(config.get("max_price"), None)

        if min_price is not None or max_price is not None:
            return [(min_price, max_price)]

        return [(None, None)]

    def _coerce_photo_rows(self, raw: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not raw:
            return out

        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    url = item.strip()
                    if not url:
                        continue
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
            "raw": item,
        }

    def _extract_items(self, payload: Any) -> tuple[list[dict[str, Any]], str]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)], "list"

        if not isinstance(payload, dict):
            return [], "non_dict"

        candidate_keys = [
            "results",
            "items",
            "rows",
            "data",
            "listings",
            "saleListings",
            "properties",
            "records",
        ]

        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)], key

        nested = payload.get("data")
        if isinstance(nested, dict):
            for key in candidate_keys:
                value = nested.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)], f"data.{key}"

        for key, value in payload.items():
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                return [x for x in value if isinstance(x, dict)], key

        return [], "empty"

    def _base_request_values(self, *, config: dict[str, Any], cursor: dict[str, Any]) -> dict[str, Any]:
        city = self._normalize_optional_text(config.get("city"))
        state = self._normalize_optional_text(config.get("state")) or "MI"
        limit = self._safe_int(config.get("limit"), 100) or 100
        page = self._safe_int((cursor or {}).get("page"), 1) or 1
        min_bedrooms = self._safe_float(config.get("min_bedrooms"), None)
        min_bathrooms = self._safe_float(config.get("min_bathrooms"), None)
        property_type = self._normalize_optional_text(config.get("property_type"))

        if limit < 1:
            limit = 1

        return {
            "city": city,
            "state": state,
            "limit": limit,
            "page": page,
            "min_bedrooms": min_bedrooms,
            "min_bathrooms": min_bathrooms,
            "property_type": property_type,
        }

    def _build_params_for_shard(
        self,
        *,
        config: dict[str, Any],
        cursor: dict[str, Any],
        zip_code: str | None,
        min_price: float | None,
        max_price: float | None,
    ) -> dict[str, Any]:
        base = self._base_request_values(config=config, cursor=cursor)

        params: dict[str, Any] = {
            "limit": base["limit"],
            "page": base["page"],
        }

        if base["city"]:
            params["city"] = base["city"]
        if base["state"]:
            params["state"] = base["state"]

        if zip_code:
            params["zipCode"] = zip_code

        if min_price is not None:
            params["minPrice"] = int(min_price)
        if max_price is not None:
            params["maxPrice"] = int(max_price)
        if base["min_bedrooms"] is not None:
            params["minBeds"] = int(base["min_bedrooms"])
        if base["min_bathrooms"] is not None:
            params["minBaths"] = float(base["min_bathrooms"])
        if base["property_type"]:
            params["propertyType"] = base["property_type"]

        return params

    def _fetch_one(
        self,
        *,
        client: httpx.Client,
        params: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
        res = client.get(self.SALE_LISTINGS_URL, params=params)
        res.raise_for_status()
        payload = res.json()

        items, extracted_from = self._extract_items(payload)

        if not items:
            logger.warning(
                "rentcast_sale_listings_empty_results",
                extra={
                    "event": "rentcast_sale_listings_empty_results",
                    "params": params,
                    "payload_type": type(payload).__name__,
                    "payload_keys": list(payload.keys()) if isinstance(payload, dict) else None,
                    "extracted_from": extracted_from,
                },
            )

        rows: list[dict[str, Any]] = []
        for item in items:
            row = self._to_canonical_row(item)
            if not row["external_record_id"]:
                continue
            if not row["address"]:
                continue
            rows.append(row)

        return rows, len(items), payload

    def fetch_incremental(
        self,
        *,
        credentials: dict[str, Any],
        config: dict[str, Any],
        cursor: dict[str, Any],
    ) -> dict[str, Any]:
        api_key = self._get_api_key(credentials)

        zip_codes = self._coerce_zip_codes(config)
        price_buckets = self._coerce_price_buckets(config)

        # If no ZIPs provided, still run one city/state shard.
        zip_shards = zip_codes if zip_codes else [None]

        # Keep provider-side search bounded.
        pages_per_shard = self._safe_int(config.get("pages_per_shard"), 1) or 1
        pages_per_shard = max(1, min(3, pages_per_shard))

        requested_limit = self._safe_int(config.get("limit"), 100) or 100
        current_page = self._safe_int((cursor or {}).get("page"), 1) or 1

        rows_out: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        total_raw_count = 0

        # Cursor for next call:
        # we keep advancing the base page if at least one shard looked "full".
        should_advance = False

        with httpx.Client(timeout=30.0, headers=self._headers(api_key)) as client:
            for zip_code in zip_shards:
                for (bucket_min, bucket_max) in price_buckets:
                    for page_offset in range(pages_per_shard):
                        shard_cursor = {"page": current_page + page_offset}
                        params = self._build_params_for_shard(
                            config=config,
                            cursor=shard_cursor,
                            zip_code=zip_code,
                            min_price=bucket_min,
                            max_price=bucket_max,
                        )

                        rows, raw_count, payload = self._fetch_one(client=client, params=params)
                        total_raw_count += raw_count

                        if raw_count >= requested_limit:
                            should_advance = True

                        for row in rows:
                            ext_id = str(row.get("external_record_id") or "").strip()
                            if not ext_id:
                                continue
                            if ext_id in seen_ids:
                                continue
                            seen_ids.add(ext_id)
                            rows_out.append(row)

                        # Early stop once we already have a healthy provider batch.
                        if len(rows_out) >= requested_limit:
                            break

                        # If this shard page came back small, deeper paging in this shard
                        # is unlikely to help much, so stop this shard early.
                        if raw_count < requested_limit:
                            break

                    if len(rows_out) >= requested_limit:
                        break
                if len(rows_out) >= requested_limit:
                    break

        next_cursor = {"page": current_page + 1} if should_advance else {"page": current_page}

        result = RentCastListingFetchResult(
            rows=rows_out,
            next_cursor=next_cursor,
            raw_count=total_raw_count,
        )

        return {
            "rows": result.rows,
            "next_cursor": result.next_cursor,
            "raw_count": result.raw_count,
        }
    