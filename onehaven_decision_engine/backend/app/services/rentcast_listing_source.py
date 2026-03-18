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

        return {
            "external_record_id": self._pick_external_id(item),
            "external_url": self._pick_external_url(item),
            "address": self._pick_address(item),
            "city": self._pick_city(item),
            "state": self._pick_state(item),
            "zip": self._pick_zip(item),
            "bedrooms": item.get("bedrooms") or item.get("beds") or 0,
            "bathrooms": item.get("bathrooms") or item.get("baths") or 0,
            "square_feet": item.get("squareFootage") or item.get("livingArea") or item.get("sqft"),
            "year_built": item.get("yearBuilt"),
            "property_type": item.get("propertyType") or item.get("type") or "single_family",
            "asking_price": item.get("price") or item.get("listPrice") or 0,
            "estimated_purchase_price": item.get("price") or item.get("listPrice"),
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
                return value, key

        return [], "empty"

    def fetch_incremental(
        self,
        *,
        credentials: dict[str, Any],
        config: dict[str, Any],
        cursor: dict[str, Any],
    ) -> dict[str, Any]:
        api_key = self._get_api_key(credentials)

        city = config.get("city")
        state = config.get("state", "MI")
        limit = int(config.get("limit") or 100)
        page = int((cursor or {}).get("page") or 1)

        if limit < 1:
            limit = 1

        params: dict[str, Any] = {
            "limit": limit,
            "page": page,
        }

        if city:
            params["city"] = city
        if state:
            params["state"] = state

        with httpx.Client(timeout=30.0, headers=self._headers(api_key)) as client:
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

        next_cursor = {"page": page + 1 if len(items) >= limit else 1}

        result = RentCastListingFetchResult(
            rows=rows,
            next_cursor=next_cursor,
            raw_count=len(items),
        )
        return {
            "rows": result.rows,
            "next_cursor": result.next_cursor,
            "raw_count": result.raw_count,
        }
    