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

    BASE = "https://api.rentcast.io/v1/avm/rent/long-term"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("RENTCAST_API_KEY is missing")
        self.api_key = api_key

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
        url = f"{self.BASE}?{qs}"

        resp1 = _http_get_json(url, {"X-Api-Key": self.api_key})
        if resp1.status == 200:
            return resp1.data if isinstance(resp1.data, dict) else {"data": resp1.data}

        resp2 = _http_get_json(url, {"Authorization": f"Bearer {self.api_key}"})
        if resp2.status == 200:
            return resp2.data if isinstance(resp2.data, dict) else {"data": resp2.data}

        raise RuntimeError(
            "RentCast rent estimate failed. "
            f"X-Api-Key status={resp1.status} body={resp1.data} | "
            f"Bearer status={resp2.status} body={resp2.data}"
        )

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
    "persist_rentcast_comps_and_get_median",
]
