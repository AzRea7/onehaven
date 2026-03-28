# backend/app/services/rentcast_service.py
from __future__ import annotations

import json
import statistics
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.models import RentComp


@dataclass(frozen=True)
class HttpResp:
    status: int
    data: Any


@dataclass(frozen=True)
class RentCastSaleListingResult:
    id: str | None
    formatted_address: str | None
    address_line1: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    county: str | None
    latitude: float | None
    longitude: float | None
    status: str | None
    price: float | None
    raw_json: dict[str, Any]


def _http_get_json(url: str, headers: dict[str, str], timeout_s: int = 20) -> HttpResp:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                payload = {"_raw": raw}
            return HttpResp(status=int(resp.status), data=payload)
    except Exception as e:
        return HttpResp(status=0, data={"error": str(e), "url": url})


class RentCastClient:
    """
    Small, dependency-free RentCast client.
    Semantics:
      - tries X-Api-Key header first
      - falls back to Authorization: Bearer
    """

    RENT_BASE = "https://api.rentcast.io/v1/avm/rent/long-term"
    SALE_LISTINGS_BASE = "https://api.rentcast.io/v1/listings/sale"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("RENTCAST_API_KEY is missing")
        self.api_key = api_key

    def _request_json(self, url: str) -> dict[str, Any] | list[Any] | None:
        resp1 = _http_get_json(url, {"X-Api-Key": self.api_key})
        if resp1.status == 200:
            return resp1.data

        resp2 = _http_get_json(url, {"Authorization": f"Bearer {self.api_key}"})
        if resp2.status == 200:
            return resp2.data

        raise RuntimeError(
            "RentCast request failed. "
            f"X-Api-Key status={resp1.status} body={resp1.data} | "
            f"Bearer status={resp2.status} body={resp2.data}"
        )

    def rent_estimate(
        self,
        *,
        address: str,
        city: str,
        state: str,
        zip_code: str,
        bedrooms: int,
        bathrooms: float,
        square_feet: Optional[int],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "bedrooms": int(bedrooms or 0),
            "bathrooms": float(bathrooms or 0),
        }
        if square_feet:
            params["squareFootage"] = int(square_feet)

        qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self.RENT_BASE}?{qs}"
        payload = self._request_json(url)
        return payload if isinstance(payload, dict) else {"data": payload}

    def sale_listing_lookup(
        self,
        *,
        address: str,
        city: str | None = None,
        state: str | None = None,
        zip_code: str | None = None,
        limit: int = 10,
        status: str | None = "Active",
        allow_status_fallback: bool = True,
        allow_location_fallback: bool = True,
    ) -> RentCastSaleListingResult | None:
        attempts: list[dict[str, Any]] = []

        search_variants: list[dict[str, Any]] = [
            {
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "status": status,
                "label": "strict_active" if status else "strict_any_status",
            }
        ]

        normalized_status = str(status or "").strip().lower() or None

        if allow_status_fallback and normalized_status == "active":
            search_variants.append(
                {
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "status": "Inactive",
                    "label": "strict_inactive",
                }
            )

        if allow_location_fallback:
            search_variants.append(
                {
                    "address": address,
                    "city": None,
                    "state": None,
                    "zip_code": None,
                    "status": status,
                    "label": "address_only_active" if status else "address_only_any_status",
                }
            )
            if allow_status_fallback and normalized_status == "active":
                search_variants.append(
                    {
                        "address": address,
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
            rows = self._fetch_sale_listing_rows(
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

            best = self._pick_best_listing_match(
                rows,
                address=address,
                city=city,
                state=state,
                zip_code=zip_code,
            )
            if not best:
                continue

            raw_json = dict(best)
            raw_json.setdefault("_lookup_attempts", attempts)

            return RentCastSaleListingResult(
                id=str(best.get("id") or best.get("listingId") or best.get("mlsNumber") or "").strip() or None,
                formatted_address=str(best.get("formattedAddress") or "").strip() or None,
                address_line1=str(best.get("addressLine1") or "").strip() or None,
                city=str(best.get("city") or "").strip() or None,
                state=str(best.get("state") or "").strip() or None,
                zip_code=str(best.get("zipCode") or "").strip() or None,
                county=str(best.get("county") or "").strip() or None,
                latitude=self._safe_float(best.get("latitude")),
                longitude=self._safe_float(best.get("longitude")),
                status=str(best.get("status") or "").strip() or None,
                price=self._safe_float(best.get("price")),
                raw_json=raw_json,
            )

        return None

    def _fetch_sale_listing_rows(
        self,
        *,
        address: str,
        city: str | None,
        state: str | None,
        zip_code: str | None,
        limit: int,
        status: str | None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "address": address,
            "limit": max(1, min(int(limit or 10), 50)),
        }
        if status:
            params["status"] = status
        if city:
            params["city"] = city
        if state:
            params["state"] = state
        if zip_code:
            params["zipCode"] = zip_code

        qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None and str(v).strip()})
        url = f"{self.SALE_LISTINGS_BASE}?{qs}"
        payload = self._request_json(url)

        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            return [x for x in payload.get("listings", []) if isinstance(x, dict)]
        return []

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _norm(s: Any) -> str:
        return " ".join(str(s or "").strip().lower().replace(",", " ").split())

    @classmethod
    def _pick_best_listing_match(
        cls,
        rows: list[dict[str, Any]],
        *,
        address: str,
        city: str | None,
        state: str | None,
        zip_code: str | None,
    ) -> dict[str, Any] | None:
        target_address = cls._norm(address)
        target_city = cls._norm(city)
        target_state = cls._norm(state)
        target_zip = str(zip_code or "").strip()

        def score(row: dict[str, Any]) -> tuple[int, int]:
            formatted = cls._norm(row.get("formattedAddress"))
            line1 = cls._norm(row.get("addressLine1"))
            row_city = cls._norm(row.get("city"))
            row_state = cls._norm(row.get("state"))
            row_zip = str(row.get("zipCode") or "").strip()

            exact = 0
            if formatted == target_address or line1 == target_address:
                exact += 4
            if target_address and target_address in formatted:
                exact += 3
            if target_city and row_city == target_city:
                exact += 2
            if target_state and row_state == target_state:
                exact += 1
            if target_zip and row_zip == target_zip:
                exact += 2
            return exact, len(formatted)

        ranked = sorted(rows, key=score, reverse=True)
        best = ranked[0] if ranked else None
        if not best:
            return None
        best_score = score(best)[0]
        return best if best_score > 0 else None

    @staticmethod
    def pick_estimated_rent(payload: dict[str, Any]) -> Optional[float]:
        if not isinstance(payload, dict):
            return None
        for key in ["rent", "rentEstimate", "estimatedRent", "value"]:
            if key in payload:
                try:
                    return float(payload[key])
                except Exception:
                    pass
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ["rent", "rentEstimate", "estimatedRent", "value"]:
                if key in data:
                    try:
                        return float(data[key])
                    except Exception:
                        pass
        return None

    @staticmethod
    def _extract_comparables(payload: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        comps = payload.get("comparables")
        if isinstance(comps, list):
            return [c for c in comps if isinstance(c, dict)]

        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("comparables"), list):
            return [c for c in data.get("comparables") if isinstance(c, dict)]

        for k in ("comps", "rent_comps", "comparablesList"):
            v = payload.get(k)
            if isinstance(v, list):
                return [c for c in v if isinstance(c, dict)]

        return []

    @staticmethod
    def _extract_comp_rents(payload: dict[str, Any]) -> list[float]:
        out: list[float] = []
        comps = RentCastClient._extract_comparables(payload)
        for c in comps:
            for k in ("rent", "price", "rentEstimate", "estimatedRent", "value", "monthlyRent"):
                v = c.get(k)
                if v is None:
                    continue
                try:
                    fv = float(v)
                    if fv > 0:
                        out.append(fv)
                        break
                except Exception:
                    continue
        return out

    @staticmethod
    def pick_rent_reasonableness_proxy(payload: dict[str, Any]) -> Optional[float]:
        rents = RentCastClient._extract_comp_rents(payload)
        if rents:
            try:
                return float(statistics.median(rents))
            except Exception:
                pass
        return RentCastClient.pick_estimated_rent(payload)

    @staticmethod
    def derive_hud_entityid_from_comps(payload: dict[str, Any]) -> Optional[str]:
        comps = RentCastClient._extract_comparables(payload)
        if not comps:
            return None

        for c in comps:
            st = str(c.get("stateFips") or "").strip()
            co = str(c.get("countyFips") or "").strip()
            if not (st.isdigit() and co.isdigit()):
                continue
            st = st.zfill(2)
            co = co.zfill(3)
            return f"{st}{co}99999"

        return None


def persist_rentcast_comps_and_get_median(
    db: Session,
    *,
    property_id: int,
    payload: dict[str, Any],
    replace_existing: bool = True,
) -> Optional[float]:
    comps = RentCastClient._extract_comparables(payload)
    if not comps:
        return None

    normalized: list[dict[str, Any]] = []
    for c in comps:
        r = c.get("rent") or c.get("price") or c.get("monthlyRent") or c.get("rentEstimate") or c.get("value")
        try:
            rent = float(r)
        except Exception:
            continue
        if rent <= 0:
            continue

        normalized.append(
            {
                "rent": rent,
                "address": c.get("address"),
                "url": c.get("url") or c.get("listingUrl") or c.get("link"),
                "bedrooms": c.get("bedrooms"),
                "bathrooms": c.get("bathrooms"),
                "square_feet": c.get("squareFeet") or c.get("squareFootage") or c.get("sqft"),
                "notes": None,
            }
        )

    if not normalized:
        return None

    if replace_existing:
        db.execute(delete(RentComp).where(RentComp.property_id == property_id, RentComp.source == "rentcast"))

    rents: list[float] = []
    for c in normalized:
        db.add(
            RentComp(
                property_id=property_id,
                rent=float(c["rent"]),
                source="rentcast",
                address=c.get("address"),
                url=c.get("url"),
                bedrooms=int(c["bedrooms"]) if c.get("bedrooms") is not None else None,
                bathrooms=float(c["bathrooms"]) if c.get("bathrooms") is not None else None,
                square_feet=int(c["square_feet"]) if c.get("square_feet") is not None else None,
                notes=c.get("notes"),
            )
        )
        rents.append(float(c["rent"]))

    try:
        return float(statistics.median(rents)) if rents else None
    except Exception:
        return None


__all__ = [
    "RentCastClient",
    "RentCastSaleListingResult",
    "persist_rentcast_comps_and_get_median",
]
