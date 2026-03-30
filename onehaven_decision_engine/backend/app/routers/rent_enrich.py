# backend/app/routers/rent_enrich.py
from __future__ import annotations

import time
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..config import settings
from ..db import get_db
from ..models import Property, RentAssumption
from ..services.budget_service import consume_external_budget, get_external_budget_status
from ..services.fmr import HudUserClient
from ..services.rentcast_service import (
    RentCastClient,
    persist_rentcast_comps_and_get_median,
)
from ..domain.rent_learning import recompute_rent_fields
from ..domain.underwriting import describe_rent_cap_reason

# Optional trust wiring (no hard dependency)
try:
    from ..services.trust_service import (
        record_signal,
        recompute_and_persist,
        record_dispersion_signal,
    )  # type: ignore
except Exception:  # pragma: no cover
    def record_signal(*args, **kwargs):  # type: ignore
        return None

    def recompute_and_persist(*args, **kwargs):  # type: ignore
        return None

    def record_dispersion_signal(*args, **kwargs):  # type: ignore
        return None


router = APIRouter(prefix="/rent", tags=["rent"])


class RentEnrichOut(BaseModel):
    property_id: int
    strategy: str = "section8"

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None

    approved_rent_ceiling: Optional[float] = None
    rent_used: Optional[float] = None
    rent_cap_reason: str = "missing_rent_inputs"

    cap_reason: str = "none"
    explanation: str = ""
    ceiling_candidates: list[dict[str, Any]] = Field(default_factory=list)

    external_budget: dict[str, Any] = Field(default_factory=dict)

    hud: dict[str, Any] = Field(default_factory=dict)
    rentcast: dict[str, Any] = Field(default_factory=dict)

    updated_fields: list[str] = Field(default_factory=list)


class RentEnrichBatchIn(BaseModel):
    property_ids: list[int] = Field(default_factory=list)
    strategy: str = "section8"
    sleep_ms: int = Field(default=0, ge=0, le=5000)


class RentEnrichBatchOut(BaseModel):
    attempted: int
    enriched: int
    property_ids: list[int] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    stopped_early: bool = False
    stop_reason: Optional[str] = None
    external_budget: dict[str, Any] = Field(default_factory=dict)
    completed_property_ids: list[int] = Field(default_factory=list)
    failed_property_ids: list[int] = Field(default_factory=list)


def _payment_standard_setting() -> float:
    try:
        v = getattr(settings, "default_payment_standard_pct", None)
        return float(v) if v is not None else 1.10
    except Exception:
        return 1.10


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
        if rentcast_comps_count is not None:
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

        recompute_and_persist(db, org_id=org_id, entity_type="property", entity_id=str(property_id))
        recompute_and_persist(db, org_id=org_id, entity_type="provider", entity_id="rentcast")
        recompute_and_persist(db, org_id=org_id, entity_type="provider", entity_id="hud")
    except Exception:
        pass


def _candidates_to_dicts(cands: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cands or []:
        try:
            ctype = str(c.get("type") or "").strip()
            cvalue = float(c.get("value"))
            if ctype:
                out.append({"type": ctype, "value": cvalue})
        except Exception:
            continue
    return out


def _enrich_one(db: Session, property_id: int, org_id: int, strategy: str = "section8") -> RentEnrichOut:
    """
    Admin/backfill helper only.

    Normal ingestion should reach rent enrichment through the property-first
    ingestion pipeline, not through manual button flows.
    """
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != org_id:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = _get_or_create_rent_assumption(db, property_id, org_id)

    updated_fields: list[str] = []
    hud_debug: dict[str, Any] = {}
    rentcast_debug: dict[str, Any] = {}
    budget_debug: dict[str, Any] = {}

    rc_payload: Optional[dict[str, Any]] = None

    rentcast_ok: bool | None = None
    hud_ok: bool | None = None
    comps_count: int | None = None
    dispersion: float | None = None

    try:
        provider = "rentcast"

        status = consume_external_budget(
            db,
            org_id=org_id,
            provider=provider,
            units=1,
            meta={"endpoint": "rent_estimate", "property_id": property_id},
            metric_key="external_calls_per_day",
        )
        budget_debug = {
            "code": "ok",
            "metric": status.metric,
            "provider": status.provider,
            "limit": status.limit,
            "used": status.used,
            "remaining": status.remaining,
            "reset_at": status.reset_at,
        }

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
            rr_median = persist_rentcast_comps_and_get_median(
                db,
                property_id=property_id,
                payload=rc_payload,
            )

            comps = rc_payload.get("comparables") or rc_payload.get("comps") or rc_payload.get("rentComparables")
            if isinstance(comps, list):
                comps_count = len(comps)
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
            rr_proxy = rc.pick_rent_reasonableness_proxy(
                rc_payload if isinstance(rc_payload, dict) else {}
            )
            if rr_proxy is not None and rr_proxy > 0:
                if ra.rent_reasonableness_comp != float(rr_proxy):
                    ra.rent_reasonableness_comp = float(rr_proxy)
                    updated_fields.append("rent_reasonableness_comp")

        db.commit()

    except HTTPException:
        rentcast_ok = False
        _emit_rent_trust_signals(
            db,
            org_id=org_id,
            property_id=property_id,
            rentcast_ok=rentcast_ok,
            rentcast_comps_count=comps_count,
            rentcast_dispersion=dispersion,
            hud_ok=hud_ok,
            has_ceiling=bool(ra.approved_rent_ceiling),
            has_market=bool(ra.market_rent_estimate),
        )
        raise
    except Exception as e:
        rentcast_ok = False
        rentcast_debug = {"error": str(e)}

    try:
        hud = HudUserClient(getattr(settings, "hud_user_token", "") or "")
        if not isinstance(rc_payload, dict):
            raise RuntimeError("HUD FMR requires RentCast payload to derive county FIPS.")

        entityid = RentCastClient.derive_hud_entityid_from_comps(rc_payload)
        if not entityid:
            raise RuntimeError("Could not derive HUD entityid from RentCast comparables.")

        fmr_data = hud.fmr_for_entityid(entityid)

        units = max(int(getattr(prop, "units", 0) or 0), 0)
        total_bedrooms = max(int(getattr(prop, "bedrooms", 0) or 0), 0)
        is_multifamily = ("multi" in str(getattr(prop, "property_type", "") or "").lower()) and units > 1

        if is_multifamily:
            per_unit_bedrooms = max(int(round(total_bedrooms / float(units))) if units > 0 else 0, 1)
            per_unit_fmr = hud.pick_bedroom_fmr(fmr_data, per_unit_bedrooms)
            fmr_value = round(float(per_unit_fmr) * float(units), 2) if per_unit_fmr is not None else None
            hud_pick_payload = {
                "bedrooms_mode": "average_per_unit",
                "total_bedrooms": total_bedrooms,
                "units": units,
                "picked_per_unit_bedrooms": per_unit_bedrooms,
                "picked_per_unit_fmr": per_unit_fmr,
                "picked_total_fmr": fmr_value,
            }
        else:
            bedroom_pick = max(total_bedrooms, 0)
            fmr_value = hud.pick_bedroom_fmr(fmr_data, bedroom_pick)
            hud_pick_payload = {
                "bedrooms_mode": "whole_property",
                "picked_bedrooms": bedroom_pick,
                "picked_total_fmr": fmr_value,
            }

        hud_ok = True
        hud_debug = {
            "entityid": entityid,
            **hud_pick_payload,
            "raw": fmr_data,
        }

        if fmr_value is not None and fmr_value > 0:
            if ra.section8_fmr != float(fmr_value):
                ra.section8_fmr = float(fmr_value)
                updated_fields.append("section8_fmr")

    except Exception as e:
        hud_ok = False
        hud_debug = {"error": str(e)}

    computed = recompute_rent_fields(
        db,
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=_payment_standard_setting(),
    )

    approved = computed.get("approved_rent_ceiling")
    rent_used = computed.get("rent_used")
    rent_cap_reason = str(computed.get("rent_cap_reason") or "missing_rent_inputs")
    explanation = str(computed.get("explanation") or describe_rent_cap_reason(rent_cap_reason, strategy=strategy))

    ceiling_candidates = []
    if approved is not None:
        ceiling_candidates.append({"type": "approved_fmr_ceiling", "value": float(approved)})
    if ra.rent_reasonableness_comp is not None:
        try:
            ceiling_candidates.append({"type": "rent_reasonableness_comp", "value": float(ra.rent_reasonableness_comp)})
        except Exception:
            pass

    if ra.approved_rent_ceiling != approved:
        ra.approved_rent_ceiling = float(approved) if approved is not None else None
        updated_fields.append("approved_rent_ceiling")

    if getattr(ra, "rent_used", None) != rent_used:
        ra.rent_used = float(rent_used) if rent_used is not None else None
        updated_fields.append("rent_used")

    if hasattr(ra, "rent_cap_reason"):
        current_reason = getattr(ra, "rent_cap_reason", None)
        if current_reason != rent_cap_reason:
            setattr(ra, "rent_cap_reason", rent_cap_reason)
            updated_fields.append("rent_cap_reason")

    db.commit()
    db.refresh(ra)

    _emit_rent_trust_signals(
        db,
        org_id=org_id,
        property_id=property_id,
        rentcast_ok=rentcast_ok,
        rentcast_comps_count=comps_count,
        rentcast_dispersion=dispersion,
        hud_ok=hud_ok,
        has_ceiling=bool(approved),
        has_market=bool(ra.market_rent_estimate),
    )

    return RentEnrichOut(
        property_id=property_id,
        strategy=strategy,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=approved,
        rent_used=rent_used,
        rent_cap_reason=rent_cap_reason,
        cap_reason=rent_cap_reason,
        explanation=explanation,
        ceiling_candidates=_candidates_to_dicts(ceiling_candidates),
        external_budget=budget_debug,
        hud=hud_debug,
        rentcast=rentcast_debug,
        updated_fields=updated_fields,
    )


@router.get("/enrich/budget")
def get_external_budget(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    status = get_external_budget_status(
        db,
        org_id=p.org_id,
        provider="rentcast",
        metric_key="external_calls_per_day",
    )
    return {
        "code": "ok",
        "metric": status.metric,
        "provider": status.provider,
        "limit": status.limit,
        "used": status.used,
        "remaining": status.remaining,
        "reset_at": status.reset_at,
    }


@router.post("/enrich/batch", response_model=RentEnrichBatchOut)
def enrich_rent_batch(
    payload: RentEnrichBatchIn = Body(...),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Admin/backfill endpoint only.
    Normal ingestion should enrich rent inside the ingestion pipeline.
    """
    seen: set[int] = set()
    property_ids: list[int] = []
    for pid in payload.property_ids:
        if int(pid) in seen:
            continue
        seen.add(int(pid))
        property_ids.append(int(pid))

    enriched = 0
    errors: list[dict[str, Any]] = []
    stopped_early = False
    stop_reason: Optional[str] = None
    completed_property_ids: list[int] = []
    failed_property_ids: list[int] = []

    before = get_external_budget_status(
        db,
        org_id=p.org_id,
        provider="rentcast",
        metric_key="external_calls_per_day",
    )
    budget_summary = {
        "provider": before.provider,
        "metric": before.metric,
        "limit": before.limit,
        "used_before": before.used,
        "remaining_before": before.remaining,
        "reset_at": before.reset_at,
    }

    for pid in property_ids:
        try:
            _enrich_one(db, pid, org_id=p.org_id, strategy=payload.strategy)
            enriched += 1
            completed_property_ids.append(int(pid))
        except HTTPException as he:
            failed_property_ids.append(int(pid))
            if he.status_code == 402 and isinstance(he.detail, dict) and he.detail.get("code") == "plan_limit_exceeded":
                errors.append({"property_id": pid, "error": he.detail, "type": "budget_exceeded"})
                stopped_early = True
                stop_reason = "external_budget_exceeded"
                break
            errors.append({"property_id": pid, "error": he.detail, "type": "http"})
        except Exception as e:
            failed_property_ids.append(int(pid))
            errors.append({"property_id": pid, "error": str(e), "type": "exception"})

        if payload.sleep_ms:
            time.sleep(payload.sleep_ms / 1000.0)

    after = get_external_budget_status(
        db,
        org_id=p.org_id,
        provider="rentcast",
        metric_key="external_calls_per_day",
    )
    budget_summary.update({"used_after": after.used, "remaining_after": after.remaining})

    return RentEnrichBatchOut(
        attempted=len(property_ids),
        enriched=enriched,
        property_ids=property_ids,
        errors=errors,
        stopped_early=stopped_early,
        stop_reason=stop_reason,
        external_budget=budget_summary,
        completed_property_ids=completed_property_ids,
        failed_property_ids=failed_property_ids,
    )


@router.post("/enrich/{property_id}", response_model=RentEnrichOut)
def enrich_rent(
    property_id: int,
    strategy: str = Query("section8"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Admin/backfill endpoint only.
    Normal ingestion should enrich rent automatically.
    """
    return _enrich_one(db, property_id, org_id=p.org_id, strategy=strategy)
