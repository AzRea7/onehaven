# backend/app/routers/rent_enrich.py
from __future__ import annotations

import time
from datetime import date
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..config import settings
from ..db import get_db
from ..models import Deal, Property, RentAssumption

from ..services.api_budget import ApiBudgetExceeded, get_remaining
from ..services.fmr import HudUserClient
from ..services.rentcast_service import (
    RentCastClient,
    consume_rentcast_call,
    persist_rentcast_comps_and_get_median,
)

# Optional trust wiring (no hard dependency)
try:
    from ..services.trust_service import (
        record_signal,
        recompute_and_persist,
        record_dispersion_signal,  # optional helper you mentioned
    )  # type: ignore
except Exception:  # pragma: no cover
    def record_signal(*args, **kwargs):  # type: ignore
        return None

    def recompute_and_persist(*args, **kwargs):  # type: ignore
        return None

    def record_dispersion_signal(*args, **kwargs):  # type: ignore
        return None


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


# ---------------------------- Budget helpers ----------------------------
def _rentcast_daily_limit() -> int:
    try:
        v = getattr(settings, "rentcast_daily_limit", None)
        if v is None:
            return 50
        return int(v)
    except Exception:
        return 50


# ---------------------------- DB helpers (multitenancy-safe) ----------------------------
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
        # If manually set, treat that as authoritative ceiling.
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


def _emit_rent_trust_signals(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    rentcast_ok: bool | None,
    rentcast_comps_count: int | None,
    rentcast_dispersion: float | None,
    hud_ok: bool | None,
    has_ceiling: bool,
    has_market: bool,
):
    """
    This is the “trust loop” wiring:
    - record raw signals
    - recompute trust for the property
    Keep this *best-effort*; enrich should still succeed even if trust system is missing.
    """
    try:
        if rentcast_ok is not None:
            record_signal(
                db,
                org_id=org_id,
                entity_type="provider",
                entity_id="rentcast",
                signal_key="rentcast.success",
                value=1.0 if rentcast_ok else 0.0,
                meta={"property_id": property_id},
            )
            record_signal(
                db,
                org_id=org_id,
                entity_type="property",
                entity_id=str(property_id),
                signal_key="rentcast.success",
                value=1.0 if rentcast_ok else 0.0,
                meta={},
            )

        if rentcast_comps_count is not None:
            # Normalize: 0 comps => 0, >=10 comps => 1
            v = max(0.0, min(1.0, float(rentcast_comps_count) / 10.0))
            record_signal(
                db,
                org_id=org_id,
                entity_type="property",
                entity_id=str(property_id),
                signal_key="rentcast.comps.count",
                value=v,
                meta={"count": int(rentcast_comps_count)},
            )

        if rentcast_dispersion is not None:
            # If you have a helper, use it; otherwise store a generic signal.
            try:
                record_dispersion_signal(
                    db,
                    org_id=org_id,
                    entity_type="property",
                    entity_id=str(property_id),
                    signal_key="rentcast.comps.dispersion",
                    dispersion=float(rentcast_dispersion),
                    meta={},
                )
            except Exception:
                # fallback: invert dispersion into a “quality-ish” score.
                # heuristic: dispersion 0.0 => 1.0, dispersion 0.5 => 0.5, dispersion >=1.0 => 0.0
                q = max(0.0, min(1.0, 1.0 - float(rentcast_dispersion)))
                record_signal(
                    db,
                    org_id=org_id,
                    entity_type="property",
                    entity_id=str(property_id),
                    signal_key="rentcast.comps.dispersion",
                    value=q,
                    meta={"dispersion": float(rentcast_dispersion)},
                )

        if hud_ok is not None:
            record_signal(
                db,
                org_id=org_id,
                entity_type="provider",
                entity_id="hud",
                signal_key="hud.success",
                value=1.0 if hud_ok else 0.0,
                meta={"property_id": property_id},
            )
            record_signal(
                db,
                org_id=org_id,
                entity_type="property",
                entity_id=str(property_id),
                signal_key="hud.success",
                value=1.0 if hud_ok else 0.0,
                meta={},
            )

        record_signal(
            db,
            org_id=org_id,
            entity_type="property",
            entity_id=str(property_id),
            signal_key="rent.pipeline.has_ceiling",
            value=1.0 if has_ceiling else 0.0,
            meta={},
        )
        record_signal(
            db,
            org_id=org_id,
            entity_type="property",
            entity_id=str(property_id),
            signal_key="rent.pipeline.has_market",
            value=1.0 if has_market else 0.0,
            meta={},
        )

        recompute_and_persist(db, org_id, "property", str(property_id))
        # Optional: keep provider-level trust up-to-date too
        recompute_and_persist(db, org_id, "provider", "rentcast")
        recompute_and_persist(db, org_id, "provider", "hud")
    except Exception:
        pass


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

    # Trust capture vars
    rentcast_ok: bool | None = None
    hud_ok: bool | None = None
    comps_count: int | None = None
    dispersion: float | None = None

    # ---- RentCast market rent + comps + RR proxy (BUDGETED) ----
    try:
        provider = "rentcast"
        today = date.today()
        limit = _rentcast_daily_limit()
        budget_debug = consume_rentcast_call(db, provider=provider, day=today, daily_limit=limit)

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

        rentcast_ok = True

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
            # Persist comps and derive median
            rr_median = persist_rentcast_comps_and_get_median(db, property_id=property_id, payload=rc_payload)

            # Try to infer comps_count + dispersion from payload shape (best-effort).
            # Different RentCast payloads differ; we keep this defensive.
            comps = rc_payload.get("comparables") or rc_payload.get("comps") or rc_payload.get("rentComparables")
            if isinstance(comps, list):
                comps_count = len(comps)
                # compute dispersion as iqr/median using “rent” if available
                rents: list[float] = []
                for c in comps:
                    if not isinstance(c, dict):
                        continue
                    v = c.get("rent") or c.get("price") or c.get("estimatedRent")
                    try:
                        fv = float(v)
                        if fv > 0:
                            rents.append(fv)
                    except Exception:
                        continue
                rents.sort()
                if len(rents) >= 4:
                    q1 = rents[int(0.25 * (len(rents) - 1))]
                    q3 = rents[int(0.75 * (len(rents) - 1))]
                    med = rents[int(0.50 * (len(rents) - 1))]
                    if med > 0:
                        dispersion = float(q3 - q1) / float(med)

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
        rentcast_ok = False
        _emit_rent_trust_signals(
            db,
            org_id=org_id,
            property_id=property_id,
            rentcast_ok=rentcast_ok,
            rentcast_comps_count=comps_count,
            rentcast_dispersion=dispersion,
            hud_ok=hud_ok,
            has_ceiling=bool(_compute_approved_ceiling(ra)),
            has_market=bool(ra.market_rent_estimate),
        )
        raise HTTPException(status_code=429, detail=str(e))
    except Exception as e:
        rentcast_ok = False
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

        hud_ok = True

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
        hud_ok = False
        hud_debug = {"error": str(e)}

    # ---- Compute approved_rent_ceiling (only if not manually set) ----
    try:
        if ra.approved_rent_ceiling is None:
            new_ceiling = _compute_approved_ceiling(ra)
            if new_ceiling is not None and new_ceiling > 0:
                ra.approved_rent_ceiling = float(new_ceiling)
                updated_fields.append("approved_rent_ceiling")
        else:
            # Trust: manual override exists (do not hide it)
            try:
                record_signal(
                    db,
                    org_id=org_id,
                    entity_type="property",
                    entity_id=str(property_id),
                    signal_key="property.manual_override.rent_ceiling",
                    value=0.4,
                    meta={"approved_rent_ceiling": float(ra.approved_rent_ceiling)},
                )
            except Exception:
                pass
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

    # ---- Trust wiring (post-commit so “truth exists” when trust is computed) ----
    _emit_rent_trust_signals(
        db,
        org_id=org_id,
        property_id=property_id,
        rentcast_ok=rentcast_ok,
        rentcast_comps_count=comps_count,
        rentcast_dispersion=dispersion,
        hud_ok=hud_ok,
        has_ceiling=bool(ceiling),
        has_market=bool(ra.market_rent_estimate),
    )

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


@router.post("/enrich/batch", response_model=RentEnrichBatchOut)
def enrich_rent_batch(
    snapshot_id: int = Query(...),
    limit: int = Query(50, ge=1, le=500),
    strategy: str = Query("section8"),
    sleep_ms: int = Query(0, ge=0, le=5000),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    deals = db.scalars(
        select(Deal).where(Deal.snapshot_id == snapshot_id).where(Deal.org_id == p.org_id).limit(limit)
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
