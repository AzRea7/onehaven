from __future__ import annotations

import json
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..config import settings
from ..db import get_db
from ..models import Deal, Property, RentAssumption, RentComp
from ..services.api_budget import ApiBudgetExceeded, consume, get_remaining

router = APIRouter(prefix="/rent", tags=["rent"])


# ---------------------------- Response schema ----------------------------

class RentEnrichOut(BaseModel):
    property_id: int
    strategy: str = "section8"

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None

    rent_reasonableness_comp: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None

    rent_used: Optional[float] = None

    rentcast_budget: dict[str, Any] = Field(default_factory=dict)

    hud: dict[str, Any] = Field(default_factory=dict)
    rentcast: dict[str, Any] = Field(default_factory=dict)

    updated_fields: list[str] = Field(default_factory=list)


class RentEnrichBatchOut(BaseModel):
    snapshot_id: int
    attempted: int
    enriched: int
    errors: list[dict[str, Any]] = Field(default_factory=list)
    stopped_early: bool = False
    stop_reason: Optional[str] = None
    rentcast_budget: dict[str, Any] = Field(default_factory=dict)


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


# ---------------------------- HUD FMR client ----------------------------

class HudUserClient:
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


# ---------------------------- Budget helpers ----------------------------

def _rentcast_daily_limit() -> int:
    try:
        v = getattr(settings, "rentcast_daily_limit", None)
        if v is None:
            return 50
        return int(v)
    except Exception:
        return 50


def _consume_rentcast_call(db: Session) -> dict[str, Any]:
    provider = "rentcast"
    today = date.today()
    limit = _rentcast_daily_limit()
    remaining_before = get_remaining(db, provider=provider, day=today, daily_limit=limit)
    remaining_after = consume(db, provider=provider, day=today, daily_limit=limit, calls=1)

    return {
        "provider": provider,
        "day": today.isoformat(),
        "daily_limit": limit,
        "remaining_before": remaining_before,
        "remaining_after": remaining_after,
    }


@router.get("/enrich/budget")
def get_rentcast_budget(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    provider = "rentcast"
    today = date.today()
    limit = _rentcast_daily_limit()
    remaining = get_remaining(db, provider=provider, day=today, daily_limit=limit)
    used = max(0, limit - remaining)
    return {
        "provider": provider,
        "day": today.isoformat(),
        "daily_limit": limit,
        "used": used,
        "remaining": remaining,
    }


# ---------------------------- Comps persistence ----------------------------

def _extract_normalized_comps(payload: dict[str, Any]) -> list[dict[str, Any]]:
    comps = RentCastClient._extract_comparables(payload)
    out: list[dict[str, Any]] = []

    for c in comps:
        r = c.get("rent") or c.get("price") or c.get("monthlyRent") or c.get("rentEstimate") or c.get("value")
        try:
            rent = float(r)
        except Exception:
            continue
        if rent <= 0:
            continue

        out.append(
            {
                "rent": rent,
                "source": "rentcast",
                "address": c.get("address"),
                "url": c.get("url") or c.get("listingUrl") or c.get("link"),
                "bedrooms": c.get("bedrooms"),
                "bathrooms": c.get("bathrooms"),
                "square_feet": c.get("squareFeet") or c.get("squareFootage") or c.get("sqft"),
                "notes": None,
            }
        )
    return out


def _persist_rentcast_comps_and_get_median(
    db: Session,
    *,
    property_id: int,
    payload: dict[str, Any],
    replace_existing: bool = True,
) -> Optional[float]:
    comps = _extract_normalized_comps(payload)
    if not comps:
        return None

    if replace_existing:
        db.execute(delete(RentComp).where(RentComp.property_id == property_id, RentComp.source == "rentcast"))

    rents: list[float] = []
    for c in comps:
        rc = RentComp(
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
        db.add(rc)
        rents.append(float(c["rent"]))

    try:
        return float(statistics.median(rents)) if rents else None
    except Exception:
        return None


# ---------------------------- DB helpers (FIXED for multitenancy) ----------------------------

def _get_or_create_rent_assumption(db: Session, property_id: int, org_id: int) -> RentAssumption:
    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == property_id)
        .where(RentAssumption.org_id == org_id)
    )
    if ra is None:
        ra = RentAssumption(property_id=property_id, org_id=org_id)
        db.add(ra)
        db.commit()
        db.refresh(ra)
    return ra


def _rent_used(strategy: str, market: Optional[float], ceiling: Optional[float]) -> Optional[float]:
    strategy = (strategy or "section8").strip().lower()
    if strategy == "market":
        return market
    if market is None and ceiling is None:
        return None
    if market is None:
        return ceiling
    if ceiling is None:
        return market
    return min(market, ceiling)


def _compute_approved_ceiling(ra: RentAssumption) -> Optional[float]:
    try:
        if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0:
            return float(ra.approved_rent_ceiling)

        candidates: list[float] = []
        if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
            candidates.append(float(ra.section8_fmr))
        if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
            candidates.append(float(ra.rent_reasonableness_comp))

        return min(candidates) if candidates else None
    except Exception:
        return None


def _enrich_one(db: Session, property_id: int, org_id: int, strategy: str = "section8") -> RentEnrichOut:
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != org_id:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = _get_or_create_rent_assumption(db, property_id, org_id)

    updated_fields: list[str] = []
    hud_debug: dict[str, Any] = {}
    rentcast_debug: dict[str, Any] = {}
    budget_debug: dict[str, Any] = {}

    rc_payload: Optional[dict[str, Any]] = None

    # ---- RentCast market rent + comps + RR proxy (BUDGETED) ----
    try:
        budget_debug = _consume_rentcast_call(db)

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

        rr_median = None
        if isinstance(rc_payload, dict):
            rr_median = _persist_rentcast_comps_and_get_median(db, property_id=property_id, payload=rc_payload)

        if rr_median is not None and rr_median > 0:
            if ra.rent_reasonableness_comp != float(rr_median):
                ra.rent_reasonableness_comp = float(rr_median)
                updated_fields.append("rent_reasonableness_comp")
        else:
            rr_proxy = rc.pick_rent_reasonableness_proxy(rc_payload if isinstance(rc_payload, dict) else {})
            if rr_proxy is not None and rr_proxy > 0:
                if ra.rent_reasonableness_comp != float(rr_proxy):
                    ra.rent_reasonableness_comp = float(rr_proxy)
                    updated_fields.append("rent_reasonableness_comp")

    except ApiBudgetExceeded as e:
        raise HTTPException(status_code=429, detail=str(e))
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

    # ---- Compute approved_rent_ceiling (only if not manually set) ----
    try:
        if ra.approved_rent_ceiling is None:
            new_ceiling = _compute_approved_ceiling(ra)
            if new_ceiling is not None and new_ceiling > 0:
                ra.approved_rent_ceiling = float(new_ceiling)
                updated_fields.append("approved_rent_ceiling")
    except Exception:
        pass

    ceiling = _compute_approved_ceiling(ra)
    rent_used = _rent_used(strategy, ra.market_rent_estimate, ceiling)

    # persist rent_used
    try:
        if getattr(ra, "rent_used", None) != rent_used:
            ra.rent_used = rent_used
            updated_fields.append("rent_used")
    except Exception:
        pass

    db.commit()
    db.refresh(ra)

    return RentEnrichOut(
        property_id=property_id,
        strategy=strategy,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ceiling,
        rent_used=rent_used,
        rentcast_budget=budget_debug,
        hud=hud_debug,
        rentcast=rentcast_debug,
        updated_fields=updated_fields,
    )


# ---------------------------- ROUTES ----------------------------

@router.post("/enrich/batch", response_model=RentEnrichBatchOut)
def enrich_rent_batch(
    snapshot_id: int = Query(...),
    limit: int = Query(50, ge=1, le=500),
    strategy: str = Query("section8"),
    sleep_ms: int = Query(0, ge=0, le=5000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    # org-scoped
    deals = db.scalars(
        select(Deal)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == p.org_id)
        .limit(limit)
    ).all()

    seen: set[int] = set()
    pids: list[int] = []
    for d in deals:
        if d.property_id not in seen:
            seen.add(d.property_id)
            pids.append(d.property_id)

    enriched = 0
    errors: list[dict[str, Any]] = []
    stopped_early = False
    stop_reason: Optional[str] = None

    provider = "rentcast"
    today = date.today()
    daily_limit = _rentcast_daily_limit()
    budget_summary = {
        "provider": provider,
        "day": today.isoformat(),
        "daily_limit": daily_limit,
        "remaining_before_batch": get_remaining(db, provider=provider, day=today, daily_limit=daily_limit),
    }

    for pid in pids:
        try:
            _enrich_one(db, pid, org_id=p.org_id, strategy=strategy)
            enriched += 1
        except HTTPException as he:
            if he.status_code == 429:
                errors.append({"property_id": pid, "error": he.detail, "type": "budget_exceeded"})
                stopped_early = True
                stop_reason = "rentcast_budget_exceeded"
                break
            errors.append({"property_id": pid, "error": he.detail, "type": "http"})
        except Exception as e:
            errors.append({"property_id": pid, "error": str(e), "type": "exception"})

        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

    budget_summary["remaining_after_batch"] = get_remaining(db, provider=provider, day=today, daily_limit=daily_limit)

    return RentEnrichBatchOut(
        snapshot_id=snapshot_id,
        attempted=len(pids),
        enriched=enriched,
        errors=errors,
        stopped_early=stopped_early,
        stop_reason=stop_reason,
        rentcast_budget=budget_summary,
    )


@router.post("/enrich/{property_id}", response_model=RentEnrichOut)
def enrich_rent(
    property_id: int,
    strategy: str = Query("section8"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return _enrich_one(db, property_id, org_id=p.org_id, strategy=strategy)
