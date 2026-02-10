from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Property, Deal, RentAssumption
from ..clients.rentcast import RentcastClient
from ..clients.hud_user import HudUserClient

router = APIRouter(prefix="/rent", tags=["rent"])


def _upsert_rent_assumption(
    db: Session,
    *,
    property_id: int,
    market_rent_estimate: Optional[float],
    section8_fmr: Optional[float],
) -> list[str]:
    updated: list[str] = []
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))

    if ra is None:
        ra = RentAssumption(property_id=property_id)
        db.add(ra)

    if market_rent_estimate is not None and ra.market_rent_estimate != market_rent_estimate:
        ra.market_rent_estimate = market_rent_estimate
        updated.append("market_rent_estimate")

    if section8_fmr is not None and ra.section8_fmr != section8_fmr:
        ra.section8_fmr = section8_fmr
        updated.append("section8_fmr")

    db.commit()
    db.refresh(ra)
    return updated


def _entityid_from_rentcast_raw(raw: dict[str, Any]) -> Optional[str]:
    """
    RentCast comparables often include stateFips + countyFips.
    We'll grab the first comparable and build:
      entityid = stateFips(2) + countyFips(3) + "99999"
    Example: 26 + 163 + 99999 => 2616399999
    """
    comps = raw.get("comparables")
    if not isinstance(comps, list) or not comps:
        return None

    c0 = comps[0]
    if not isinstance(c0, dict):
        return None

    state_fips = c0.get("stateFips")
    county_fips = c0.get("countyFips")
    if not (isinstance(state_fips, str) and isinstance(county_fips, str)):
        return None

    county_fips = county_fips.zfill(3)
    return f"{state_fips}{county_fips}99999"


@router.post("/enrich/batch")
def enrich_batch(snapshot_id: int, limit: int = 50, db: Session = Depends(get_db)):
    """
    Batch enrich rents for a snapshot.
    NOTE: This must be declared BEFORE /enrich/{property_id} to avoid route collision.
    """
    deals = db.scalars(select(Deal).where(Deal.snapshot_id == snapshot_id).limit(limit)).all()  # type: ignore[attr-defined]
    if not deals:
        return {"snapshot_id": snapshot_id, "processed": 0, "updated": 0, "errors": []}

    updated_count = 0
    errors: list[dict[str, Any]] = []
    processed = 0

    for d in deals:
        processed += 1
        try:
            _ = enrich_single(d.property_id, db=db)
            if _ and isinstance(_, dict) and _.get("updated_fields"):
                updated_count += 1
        except Exception as e:
            errors.append({"deal_id": getattr(d, "id", None), "property_id": d.property_id, "error": str(e)})

    return {"snapshot_id": snapshot_id, "processed": processed, "updated": updated_count, "errors": errors}


@router.post("/enrich/{property_id}")
def enrich_single(property_id: int, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    rentcast = RentcastClient()
    hud = HudUserClient()

    debug: dict[str, Any] = {"rentcast": None, "hud": None}
    market_rent = None
    section8_fmr = None

    # ---- RentCast market rent ----
    if rentcast.enabled():
        rc = rentcast.estimate_long_term_rent(
            address=prop.address,
            city=prop.city,
            state=prop.state,
            zip_code=prop.zip,
            bedrooms=prop.bedrooms,
            bathrooms=float(prop.bathrooms),
            square_feet=prop.square_feet,
        )
        market_rent = rc.rent
        debug["rentcast"] = {
            "endpoint": f"{rentcast.base}/avm/rent/long-term",
            "request": {
                "address": prop.address,
                "city": prop.city,
                "state": prop.state,
                "zip": prop.zip,
                "bedrooms": prop.bedrooms,
                "bathrooms": float(prop.bathrooms),
                "square_feet": prop.square_feet,
            },
            "raw": rc.raw,
        }
    else:
        debug["rentcast"] = {"error": "RentCast disabled (rentcast_api_key not set)"}

    # ---- HUD FMR (fallback path that avoids USPS endpoint) ----
    if hud.enabled():
        entityid = None
        if isinstance(debug.get("rentcast"), dict):
            raw = debug["rentcast"].get("raw")
            if isinstance(raw, dict):
                entityid = _entityid_from_rentcast_raw(raw)

        if entityid:
            hr = hud.fmr_by_entityid(entityid=entityid, bedrooms=prop.bedrooms)
            section8_fmr = hr.fmr
            debug["hud"] = {"entityid": entityid, "raw": hr.raw}
        else:
            debug["hud"] = {"error": "Could not derive HUD entityid from RentCast comparables"}
    else:
        debug["hud"] = {"error": "HUD disabled (hud_user_token not set)"}

    updated_fields = _upsert_rent_assumption(db, property_id=property_id, market_rent_estimate=market_rent, section8_fmr=section8_fmr)

    return {
        "property_id": property_id,
        "market_rent_estimate": market_rent,
        "section8_fmr": section8_fmr,
        "hud": debug["hud"],
        "rentcast": debug["rentcast"],
        "updated_fields": updated_fields,
    }
