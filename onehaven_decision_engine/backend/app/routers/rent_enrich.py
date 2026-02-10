# backend/app/routers/rent_enrich.py
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional, Iterable

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db
from ..models import Deal, Property, RentAssumption

router = APIRouter(prefix="/rent", tags=["rent"])


# ---------------------------- Response schema ----------------------------

class RentEnrichOut(BaseModel):
    property_id: int

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None

    # New: conservative “rent reasonableness” proxy & safe ceiling
    rent_reasonableness_comp: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None

    # Debug payloads
    hud: dict[str, Any] = Field(default_factory=dict)
    rentcast: dict[str, Any] = Field(default_factory=dict)

    updated_fields: list[str] = Field(default_factory=list)


class RentEnrichBatchOut(BaseModel):
    snapshot_id: int
    attempted: int
    enriched: int
    errors: list[dict[str, Any]] = Field(default_factory=list)


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
        return HttpResp(status=0, data={"error": str(e), "url": url})


# ---------------------------- HUD FMR client (no USPS) ----------------------------

class HudUserClient:
    """
    Uses HUD USER FMR API only:
      Base URL: https://www.huduser.gov/hudapi/public/fmr
      Endpoint: fmr/data/{entityid}  (optional ?year=YYYY)

    We DO NOT call the USPS endpoint (it can 403 depending on token/dataset access).
    We derive the county entityid from RentCast comparables (stateFips + countyFips).
    """
    FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"

    def __init__(self, token: str):
        if not token:
            raise ValueError("HUD_USER_TOKEN is missing")
        self._headers = {"Authorization": f"Bearer {token}"}

    def fmr_for_entityid(self, entityid: str, year: Optional[int] = None) -> dict[str, Any]:
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
        b = int(bedrooms or 0)
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

        if isinstance(basic, dict):
            val = basic.get(key)
            try:
                return float(val) if val is not None else None
            except Exception:
                return None

        # SAFMR list shape (rare for MI but guard anyway)
        if isinstance(basic, list):
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

        # Try X-Api-Key first (common)
        resp1 = _http_get_json(url, {"X-Api-Key": self.api_key})
        if resp1.status == 200:
            return resp1.data if isinstance(resp1.data, dict) else {"data": resp1.data}

        # Try Authorization: Bearer fallback
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
        return None

    @staticmethod
    def _extract_comp_rents(payload: dict[str, Any]) -> list[float]:
        """
        Pull comparable rents from RentCast response (best-effort).
        RentCast comparables items usually have "price" (rent).
        """
        out: list[float] = []
        comps = payload.get("comparables")
        if not isinstance(comps, list):
            return out

        for c in comps:
            if not isinstance(c, dict):
                continue
            for k in ("price", "rent", "rentEstimate", "value"):
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
    def _median(xs: list[float]) -> Optional[float]:
        if not xs:
            return None
        ys = sorted(xs)
        n = len(ys)
        mid = n // 2
        if n % 2 == 1:
            return float(ys[mid])
        return float((ys[mid - 1] + ys[mid]) / 2.0)

    @staticmethod
    def pick_rent_reasonableness_proxy(payload: dict[str, Any]) -> Optional[float]:
        """
        Conservative “rent reasonableness” proxy:
          median(comparable rents)
        If comps missing, fall back to the AVM rent itself (less ideal, but keeps pipeline flowing).
        """
        if not isinstance(payload, dict):
            return None
        comps = RentCastClient._extract_comp_rents(payload)
        med = RentCastClient._median(comps)
        if med is not None:
            return med
        return RentCastClient.pick_estimated_rent(payload)

    @staticmethod
    def derive_hud_entityid_from_comps(payload: dict[str, Any]) -> Optional[str]:
        """
        Build HUD FMR county entityid:
          stateFips(2) + countyFips(3) + '99999'
        Uses first comparable that contains both fields.
        """
        comps = payload.get("comparables")
        if not isinstance(comps, list) or not comps:
            return None

        for c in comps:
            if not isinstance(c, dict):
                continue
            st = str(c.get("stateFips") or "").strip()
            co = str(c.get("countyFips") or "").strip()
            if not (st.isdigit() and co.isdigit()):
                continue
            st = st.zfill(2)
            co = co.zfill(3)
            return f"{st}{co}99999"

        return None


# ---------------------------- DB helpers ----------------------------

def _get_or_create_rent_assumption(db: Session, property_id: int) -> RentAssumption:
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if ra is None:
        ra = RentAssumption(property_id=property_id)
        db.add(ra)
        db.commit()
        db.refresh(ra)
    return ra


def _enrich_one(db: Session, property_id: int) -> RentEnrichOut:
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = _get_or_create_rent_assumption(db, property_id)

    updated_fields: list[str] = []
    hud_debug: dict[str, Any] = {}
    rentcast_debug: dict[str, Any] = {}

    rc_payload: Optional[dict[str, Any]] = None

    # ---- RentCast market rent + RR proxy ----
    try:
        rc = RentCastClient(getattr(settings, "rentcast_api_key", "") or "")
        rc_payload = rc.rent_estimate(
            address=prop.address,
            city=prop.city,
            state=prop.state,
            zip_code=prop.zip,
            bedrooms=int(prop.bedrooms or 0),
            bathrooms=float(prop.bathrooms or 0),
            square_feet=prop.square_feet,
        )

        rentcast_debug = {
            "endpoint": RentCastClient.BASE,
            "request": {
                "address": prop.address,
                "city": prop.city,
                "state": prop.state,
                "zip": prop.zip,
                "bedrooms": int(prop.bedrooms or 0),
                "bathrooms": float(prop.bathrooms or 0),
                "square_feet": prop.square_feet,
            },
            "raw": rc_payload,
        }

        est_market = rc.pick_estimated_rent(rc_payload)
        if est_market is not None and est_market > 0:
            if ra.market_rent_estimate != float(est_market):
                ra.market_rent_estimate = float(est_market)
                updated_fields.append("market_rent_estimate")

        rr_proxy = rc.pick_rent_reasonableness_proxy(rc_payload)
        if rr_proxy is not None and rr_proxy > 0:
            # Store into your existing column
            if ra.rent_reasonableness_comp != float(rr_proxy):
                ra.rent_reasonableness_comp = float(rr_proxy)
                updated_fields.append("rent_reasonableness_comp")

    except Exception as e:
        rentcast_debug = {"error": str(e)}

    # ---- HUD FMR (derive entityid from RentCast comps) ----
    try:
        hud = HudUserClient(getattr(settings, "hud_user_token", "") or "")
        if not isinstance(rc_payload, dict):
            raise RuntimeError("HUD FMR requires RentCast payload to derive county FIPS (no USPS crosswalk).")

        entityid = RentCastClient.derive_hud_entityid_from_comps(rc_payload)
        if not entityid:
            raise RuntimeError("Could not derive HUD entityid from RentCast comparables (missing stateFips/countyFips).")

        fmr_data = hud.fmr_for_entityid(entityid)
        fmr_value = hud.pick_bedroom_fmr(fmr_data, int(prop.bedrooms or 0))

        hud_debug = {
            "entityid": entityid,
            "bedrooms": int(prop.bedrooms or 0),
            "picked_value": fmr_value,
            "raw": fmr_data,
        }

        if fmr_value is not None and fmr_value > 0:
            if ra.section8_fmr != float(fmr_value):
                ra.section8_fmr = float(fmr_value)
                updated_fields.append("section8_fmr")

    except Exception as e:
        hud_debug = {"error": str(e)}

    # ---- Compute conservative “approved_rent_ceiling” (your Section 8 safe cap) ----
    # This is the number you should feed into Section 8 underwriting/eval, not raw market rent.
    try:
        ceiling_candidates: list[float] = []
        if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
            ceiling_candidates.append(float(ra.section8_fmr))
        if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
            ceiling_candidates.append(float(ra.rent_reasonableness_comp))

        new_ceiling = min(ceiling_candidates) if ceiling_candidates else None
        if new_ceiling is not None and new_ceiling > 0:
            if ra.approved_rent_ceiling != float(new_ceiling):
                ra.approved_rent_ceiling = float(new_ceiling)
                updated_fields.append("approved_rent_ceiling")
    except Exception:
        # Keep enrichment resilient; don't fail the whole request.
        pass

    if updated_fields:
        db.commit()
        db.refresh(ra)

    return RentEnrichOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        hud=hud_debug,
        rentcast=rentcast_debug,
        updated_fields=updated_fields,
    )


# ---------------------------- ROUTES ----------------------------
# IMPORTANT: batch must appear before /enrich/{property_id}

@router.post("/enrich/batch", response_model=RentEnrichBatchOut)
def enrich_rent_batch(
    snapshot_id: int = Query(...),
    limit: int = Query(50, ge=1, le=500),
    sleep_ms: int = Query(0, ge=0, le=5000),
    db: Session = Depends(get_db),
):
    deals = db.scalars(select(Deal).where(Deal.snapshot_id == snapshot_id).limit(limit)).all()

    # distinct property ids preserving order
    seen: set[int] = set()
    pids: list[int] = []
    for d in deals:
        if d.property_id not in seen:
            seen.add(d.property_id)
            pids.append(d.property_id)

    enriched = 0
    errors: list[dict[str, Any]] = []

    for pid in pids:
        try:
            _enrich_one(db, pid)
            enriched += 1
        except Exception as e:
            errors.append({"property_id": pid, "error": str(e)})

        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

    return RentEnrichBatchOut(snapshot_id=snapshot_id, attempted=len(pids), enriched=enriched, errors=errors)


@router.post("/enrich/{property_id}", response_model=RentEnrichOut)
def enrich_rent(property_id: int, db: Session = Depends(get_db)):
    return _enrich_one(db, property_id)
