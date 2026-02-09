from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db
from ..models import Property, RentAssumption

router = APIRouter(prefix="/rent", tags=["rent"])


# ---------------------------- Response schema ----------------------------

class RentEnrichOut(BaseModel):
    property_id: int

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None

    # Where the numbers came from (debuggable “audit trail”)
    hud: dict[str, Any] = Field(default_factory=dict)
    rentcast: dict[str, Any] = Field(default_factory=dict)

    # What we updated in DB
    updated_fields: list[str] = Field(default_factory=list)


# ---------------------------- Small HTTP helper ----------------------------

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
        # urllib errors are inconsistent; wrap into a consistent shape
        return HttpResp(status=0, data={"error": str(e), "url": url})


# ---------------------------- HUD USER clients ----------------------------

class HudUserClient:
    """
    Uses:
      - USPS Crosswalk API (ZIP -> County GEOID) via type=2 zip-county
      - FMR API (county entityid -> FMR by bedroom)

    USPS docs show:
      base: https://www.huduser.gov/hudapi/public/usps
      params: type (required), query (required)
      zip-county is type=2 and query is 5-digit ZIP :contentReference[oaicite:3]{index=3}
    """

    USPS_BASE = "https://www.huduser.gov/hudapi/public/usps"
    FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"

    def __init__(self, token: str):
        if not token:
            raise ValueError("HUD_USER_TOKEN is missing")
        self._headers = {"Authorization": f"Bearer {token}"}

    def zip_to_primary_county_geoid(self, zip5: str) -> dict[str, Any]:
        """
        Returns best county GEOID (5 digits: stateFIPS+countyFIPS) by max residential ratio.
        HUD USPS returns an array of results with ratios; we pick the county with highest res_ratio.

        NOTE: USPS response field names vary by type; for zip-county it includes a county geoid. :contentReference[oaicite:4]{index=4}
        """
        zip5 = (zip5 or "").strip()[:5]
        if len(zip5) != 5 or not zip5.isdigit():
            raise ValueError(f"Invalid ZIP: {zip5!r}")

        qs = urllib.parse.urlencode({"type": 2, "query": zip5})
        url = f"{self.USPS_BASE}?{qs}"

        resp = _http_get_json(url, self._headers)
        if resp.status != 200:
            raise RuntimeError(f"HUD USPS zip->county failed (status={resp.status}): {resp.data}")

        # HUD examples show data is inside "data" (sometimes array), but USPS docs show "data":[{...}] :contentReference[oaicite:5]{index=5}
        payload = resp.data
        data = payload.get("data") if isinstance(payload, dict) else payload
        if not data:
            raise RuntimeError(f"HUD USPS zip->county empty response: {payload}")

        # Most HUD endpoints wrap as a list with a single object containing "results"
        if isinstance(data, list) and data and isinstance(data[0], dict) and "results" in data[0]:
            results = data[0].get("results") or []
            meta = {k: v for k, v in data[0].items() if k != "results"}
        elif isinstance(data, dict) and "results" in data:
            results = data.get("results") or []
            meta = {k: v for k, v in data.items() if k != "results"}
        else:
            # fallback: sometimes results are directly "results"
            results = payload.get("results") if isinstance(payload, dict) else []
            meta = {}

        if not isinstance(results, list) or not results:
            raise RuntimeError(f"HUD USPS zip->county has no results: {payload}")

        def _res_ratio(x: dict[str, Any]) -> float:
            v = x.get("res_ratio", 0) or 0
            try:
                return float(v)
            except Exception:
                return 0.0

        best = max((r for r in results if isinstance(r, dict)), key=_res_ratio)
        geoid = best.get("geoid") or best.get("county")  # depending on HUD fields
        if not geoid:
            raise RuntimeError(f"HUD USPS zip->county missing geoid: best={best}")

        geoid = str(geoid).strip()
        if len(geoid) != 5 or not geoid.isdigit():
            # County GEOID should be 5 digits (state+county) :contentReference[oaicite:6]{index=6}
            raise RuntimeError(f"Unexpected county GEOID format: {geoid!r} (best={best})")

        return {
            "zip": zip5,
            "county_geoid_5": geoid,
            "picked": best,
            "meta": meta,
            "all_results_count": len(results),
        }

    @staticmethod
    def county_geoid_to_fmr_entityid(county_geoid_5: str) -> str:
        """
        HUD FMR county entity IDs are commonly the 5-digit county GEOID + '99999'.
        Example in HUD docs: listCounties shows fips_code like '5100199999' (county 51001 + 99999). :contentReference[oaicite:7]{index=7}
        """
        county_geoid_5 = str(county_geoid_5).strip()
        return f"{county_geoid_5}99999"

    def fmr_for_county_entityid(self, entityid: str, year: Optional[int] = None) -> dict[str, Any]:
        """
        Calls: /fmr/data/{entityid}?year=YYYY (year optional; default is latest) :contentReference[oaicite:8]{index=8}
        """
        entityid = str(entityid).strip()
        if not entityid:
            raise ValueError("entityid missing")

        if year:
            url = f"{self.FMR_BASE}/data/{entityid}?{urllib.parse.urlencode({'year': year})}"
        else:
            url = f"{self.FMR_BASE}/data/{entityid}"

        resp = _http_get_json(url, self._headers)
        if resp.status != 200:
            raise RuntimeError(f"HUD FMR failed (status={resp.status}): {resp.data}")

        payload = resp.data
        data = payload.get("data") if isinstance(payload, dict) else payload
        if not data:
            raise RuntimeError(f"HUD FMR empty response: {payload}")
        return data

    @staticmethod
    def pick_bedroom_fmr(fmr_data: dict[str, Any], bedrooms: int) -> Optional[float]:
        """
        HUD FMR "basicdata" may be:
          - dict with keys "One-Bedroom", "Two-Bedroom", ... :contentReference[oaicite:9]{index=9}
          - list when smallarea_status=1 (ZIP-level SAFMR), but MI usually isn't SAFMR; we still guard.
        """
        b = int(bedrooms or 0)

        # Clamp “reasonable” bedroom counts to HUD keys
        if b <= 0:
            key = "Efficiency"
        elif b == 1:
            key = "One-Bedroom"
        elif b == 2:
            key = "Two-Bedroom"
        elif b == 3:
            key = "Three-Bedroom"
        else:
            key = "Four-Bedroom"

        basic = fmr_data.get("basicdata")

        # Standard county FMR shape: dict
        if isinstance(basic, dict):
            val = basic.get(key)
            try:
                return float(val) if val is not None else None
            except Exception:
                return None

        # SAFMR shape: list of dicts with zip_code rows :contentReference[oaicite:10]{index=10}
        if isinstance(basic, list):
            # Prefer “MSA level” if available; otherwise just take the first row.
            row = None
            for r in basic:
                if isinstance(r, dict) and str(r.get("zip_code", "")).lower() == "msa level":
                    row = r
                    break
            if row is None and basic and isinstance(basic[0], dict):
                row = basic[0]

            if row:
                val = row.get(key)
                try:
                    return float(val) if val is not None else None
                except Exception:
                    return None

        return None


# ---------------------------- RentCast client ----------------------------

class RentCastClient:
    """
    RentCast rent estimate endpoint:
      GET https://api.rentcast.io/v1/avm/rent/long-term :contentReference[oaicite:11]{index=11}

    Auth header is not fully visible in the clipped doc view, so we attempt both common styles:
      - X-Api-Key: <key>
      - Authorization: Bearer <key>
    (One will succeed; if both fail you’ll see the status + body in error.)
    """

    BASE = "https://api.rentcast.io/v1/avm/rent/long-term"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("RENTCAST_API_KEY is missing")
        self.api_key = api_key

    def rent_estimate(self, *, address: str, city: str, state: str, zip_code: str,
                      bedrooms: int, bathrooms: float, square_feet: Optional[int]) -> dict[str, Any]:
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

        # Try header style #1
        resp1 = _http_get_json(url, {"X-Api-Key": self.api_key})
        if resp1.status == 200:
            return resp1.data if isinstance(resp1.data, dict) else {"data": resp1.data}

        # Try header style #2
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
        """
        RentCast response typically includes an estimated rent value plus comps.
        Because response fields can evolve, we robustly check common keys.
        """
        if not isinstance(payload, dict):
            return None

        # Common patterns in AVM APIs
        for key in ["rent", "rentEstimate", "estimatedRent", "price", "value"]:
            if key in payload:
                try:
                    return float(payload[key])
                except Exception:
                    pass

        # Sometimes nested
        for path in [("data", "rent"), ("data", "rentEstimate"), ("data", "estimatedRent")]:
            cur: Any = payload
            ok = True
            for p in path:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    ok = False
                    break
            if ok:
                try:
                    return float(cur)
                except Exception:
                    pass

        return None


# ---------------------------- Enrichment logic ----------------------------

def _get_or_create_rent_assumption(db: Session, property_id: int) -> RentAssumption:
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if ra is None:
        ra = RentAssumption(property_id=property_id)
        db.add(ra)
        db.commit()
        db.refresh(ra)
    return ra


@router.post("/enrich/{property_id}", response_model=RentEnrichOut)
def enrich_rent(property_id: int, db: Session = Depends(get_db)):
    """
    One-shot enrichment endpoint:
      - RentCast -> market_rent_estimate
      - HUD USPS (ZIP->county) + HUD FMR (county->FMR) -> section8_fmr

    This makes your “Rent math” deterministic and auditable:
      - Market rent is a measured input (RentCast AVM + comps payload)
      - Section 8 ceiling is a measured input (HUD FMR by bedroom)
      - Underwriting can safely take min(...) or whichever priority you defined
    """
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = _get_or_create_rent_assumption(db, property_id)

    updated_fields: list[str] = []
    hud_debug: dict[str, Any] = {}
    rentcast_debug: dict[str, Any] = {}

    # --- RentCast market rent ---
    try:
        rc = RentCastClient(getattr(settings, "rentcast_api_key", "") or "")
        rc_payload = rc.rent_estimate(
            address=prop.address,
            city=prop.city,
            state=prop.state,
            zip_code=prop.zip,
            bedrooms=prop.bedrooms,
            bathrooms=prop.bathrooms,
            square_feet=prop.square_feet,
        )
        rentcast_debug = {
            "endpoint": RentCastClient.BASE,
            "request": {
                "address": prop.address,
                "city": prop.city,
                "state": prop.state,
                "zip": prop.zip,
                "bedrooms": prop.bedrooms,
                "bathrooms": prop.bathrooms,
                "square_feet": prop.square_feet,
            },
            "raw": rc_payload,
        }

        est = rc.pick_estimated_rent(rc_payload)
        if est is not None and est > 0:
            ra.market_rent_estimate = float(est)
            updated_fields.append("market_rent_estimate")
    except Exception as e:
        rentcast_debug = {"error": str(e)}

    # --- HUD FMR by bedroom (ZIP -> county -> entityid -> FMR) ---
    try:
        hud = HudUserClient(getattr(settings, "hud_user_token", "") or "")

        # Step 1: ZIP -> primary county GEOID (5 digits), by highest res_ratio
        zip_meta = hud.zip_to_primary_county_geoid(prop.zip)

        # Step 2: county GEOID -> FMR entityid
        entityid = hud.county_geoid_to_fmr_entityid(zip_meta["county_geoid_5"])

        # Step 3: call FMR
        fmr_data = hud.fmr_for_county_entityid(entityid)

        # Step 4: pick correct bedroom bucket
        fmr_value = hud.pick_bedroom_fmr(fmr_data, prop.bedrooms)

        hud_debug = {
            "usps": zip_meta,
            "fmr": {
                "entityid": entityid,
                "raw": fmr_data,
                "bedrooms": prop.bedrooms,
                "picked_value": fmr_value,
            },
        }

        if fmr_value is not None and fmr_value > 0:
            ra.section8_fmr = float(fmr_value)
            updated_fields.append("section8_fmr")
    except Exception as e:
        hud_debug = {"error": str(e)}

    if updated_fields:
        db.commit()
        db.refresh(ra)

    return RentEnrichOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        hud=hud_debug,
        rentcast=rentcast_debug,
        updated_fields=updated_fields,
    )
