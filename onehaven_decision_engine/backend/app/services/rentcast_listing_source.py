from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass(frozen=True)
class RentCastListingFetchResult:
    rows: list[dict[str, Any]]
    next_cursor: dict[str, Any]


class RentCastListingSource:
    """
    RentCast adapter for ingestion.

    Design goals:
    - Keep RentCast-specific request/response weirdness here
    - Emit OneHaven canonical rows only
    - Reuse existing RENTCAST_API_KEY by default
    - Allow optional per-source override via credentials_json["api_key"]

    Notes:
    - Uses sale listings endpoint for deal-funnel ingestion
    - Cursor strategy here is page-based to keep it simple and predictable
    """

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
                if isinstance(item, str) and item.strip():
                    out.append({"url": item.strip(), "kind": "unknown"})
                elif isinstance(item, dict):
                    url = str(
                        item.get("url")
                        or item.get("href")
                        or item.get("photoUrl")
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
        photos = self._coerce_photo_rows(item.get("photos"))

        return {
            "external_record_id": self._pick_external_id(item),
            "external_url": self._pick_external_url(item),
            "address": self._pick_address(item),
            "city": self._pick_city(item),
            "state": self._pick_state(item),
            "zip": self._pick_zip(item),
            "bedrooms": item.get("bedrooms") or 0,
            "bathrooms": item.get("bathrooms") or 0,
            "square_feet": item.get("squareFootage") or item.get("livingArea"),
            "year_built": item.get("yearBuilt"),
            "property_type": item.get("propertyType") or "single_family",
            "asking_price": item.get("price") or 0,
            "estimated_purchase_price": item.get("price"),
            "rehab_estimate": 0,
            "market_rent_estimate": None,
            "section8_fmr": None,
            "approved_rent_ceiling": None,
            "inventory_count": None,
            "photos": photos,
            "raw": item,
        }

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
        zip_code = config.get("zip_code")
        address = config.get("address")
        limit = int(config.get("limit") or 50)

        # Keep this modest because you already care about external-call budgets.
        if limit < 1:
            limit = 1
        if limit > 100:
            limit = 100

        page = int((cursor or {}).get("page") or 1)

        params: dict[str, Any] = {
            "limit": limit,
            "page": page,
        }

        # RentCast docs show geo/address search capability for sale listings.
        if address:
            params["address"] = address
        if city:
            params["city"] = city
        if state:
            params["state"] = state
        if zip_code:
            params["zipCode"] = zip_code

        with httpx.Client(timeout=30.0, headers=self._headers(api_key)) as client:
            res = client.get(self.SALE_LISTINGS_URL, params=params)
            res.raise_for_status()
            payload = res.json()

        items: list[dict[str, Any]]
        if isinstance(payload, list):
            items = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            raw_items = (
                payload.get("results")
                or payload.get("items")
                or payload.get("rows")
                or payload.get("data")
                or []
            )
            items = [x for x in raw_items if isinstance(x, dict)]
        else:
            items = []

        rows: list[dict[str, Any]] = []
        for item in items:
            row = self._to_canonical_row(item)
            if not row["external_record_id"]:
                # skip unusable records instead of poisoning the pipeline
                continue
            if not row["address"]:
                continue
            rows.append(row)

        next_cursor = {"page": page + 1 if len(items) >= limit else 1}

        result = RentCastListingFetchResult(rows=rows, next_cursor=next_cursor)
        return {
            "rows": result.rows,
            "next_cursor": result.next_cursor,
        }