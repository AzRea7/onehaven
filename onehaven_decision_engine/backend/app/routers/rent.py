from __future__ import annotations

import json
from datetime import datetime
from typing import List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..config import settings
from ..db import get_db
from ..models import (
    Deal,
    Property,
    RentAssumption,
    RentComp,
    RentObservation,
    RentCalibration,
    AuditEvent,
    RentExplainRun,
)
from ..schemas import (
    RentAssumptionOut,
    RentAssumptionUpsert,
    RentCompsBatchIn,
    RentCompOut,
    RentCompsSummaryOut,
    RentObservationCreate,
    RentObservationOut,
    RentCalibrationOut,
    RentRecomputeOut,
    RentExplainOut,
    RentExplainBatchOut,
)
from ..domain.rent_learning import (
    get_or_create_rent_assumption,
    summarize_comps,
    update_calibration_from_observation,
    recompute_rent_fields,
)
from ..domain.events import emit_workflow_event

router = APIRouter(prefix="/rent", tags=["rent"])


def _norm_strategy(strategy: Optional[str]) -> str:
    s = (strategy or "section8").strip().lower()
    return s if s in {"section8", "market"} else "section8"


def _to_pos_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        return f if f > 0 else None
    except Exception:
        return None


def _audit(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict],
    after: Optional[dict],
) -> None:
    ev = AuditEvent(
        org_id=org_id,
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        before_json=json.dumps(before) if before is not None else None,
        after_json=json.dumps(after) if after is not None else None,
        created_at=datetime.utcnow(),
    )
    db.add(ev)


def _persist_rent_explain_run(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: str,
    cap_reason: str,
    payment_standard_pct_used: float,
    explain_payload: dict,
) -> RentExplainRun:
    run = RentExplainRun(
        org_id=org_id,
        property_id=property_id,
        strategy=strategy,
        cap_reason=str(cap_reason),
        explain_json=json.dumps(explain_payload, sort_keys=True),
        decision_version=str(getattr(settings, "decision_version", "unknown")),
        payment_standard_pct_used=float(payment_standard_pct_used),
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.flush()
    return run


@router.get("/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = db.execute(
        select(RentAssumption)
        .where(RentAssumption.property_id == property_id)
        .where(RentAssumption.org_id == p.org_id)
    ).scalar_one_or_none()

    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")
    return ra


@router.get("/assumption/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption_alias(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    return get_rent_assumption(property_id=property_id, db=db, p=p)


@router.post("/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption(
    property_id: int,
    payload: RentAssumptionUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    before = {
        "market_rent_estimate": ra.market_rent_estimate,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": ra.approved_rent_ceiling,
        "rent_used": ra.rent_used,
    }

    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(ra, k, v)

    after = {
        "market_rent_estimate": ra.market_rent_estimate,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": ra.approved_rent_ceiling,
        "rent_used": ra.rent_used,
    }

    if before.get("approved_rent_ceiling") != after.get("approved_rent_ceiling"):
        _audit(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="rent_override_set",
            entity_type="rent_assumption",
            entity_id=str(property_id),
            before={"approved_rent_ceiling": before.get("approved_rent_ceiling")},
            after={"approved_rent_ceiling": after.get("approved_rent_ceiling")},
        )

    db.add(ra)
    db.commit()
    db.refresh(ra)
    return ra


@router.post("/assumption/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption_alias(
    property_id: int,
    payload: RentAssumptionUpsert,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    return upsert_rent_assumption(property_id=property_id, payload=payload, db=db, p=p)


@router.post("/comps/{property_id}", response_model=RentCompsSummaryOut)
def add_comps_batch(property_id: int, payload: RentCompsBatchIn, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    rents: List[float] = []
    for c in payload.comps:
        comp = RentComp(
            property_id=property_id,
            source=c.source,
            address=c.address,
            url=c.url,
            rent=float(c.rent),
            bedrooms=c.bedrooms,
            bathrooms=c.bathrooms,
            square_feet=c.square_feet,
            notes=c.notes,
            created_at=datetime.utcnow(),
        )
        db.add(comp)
        rents.append(float(c.rent))

    summary = summarize_comps(rents)
    ra.rent_reasonableness_comp = summary.median_rent
    db.add(ra)
    db.commit()

    return RentCompsSummaryOut(
        property_id=property_id,
        count=summary.count,
        median_rent=summary.median_rent,
        mean_rent=summary.mean_rent,
        min_rent=summary.min_rent,
        max_rent=summary.max_rent,
    )


@router.get("/comps/{property_id}", response_model=list[RentCompOut])
def list_comps(property_id: int, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    rows = (
        db.execute(
            select(RentComp)
            .where(RentComp.property_id == property_id)
            .order_by(RentComp.created_at.desc(), RentComp.id.desc())
        )
        .scalars()
        .all()
    )
    return rows


@router.post("/observe", response_model=RentObservationOut)
def add_rent_observation(payload: RentObservationCreate, db: Session = Depends(get_db), p=Depends(get_principal)):
    prop = db.get(Property, payload.property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, payload.property_id)
    ra.org_id = p.org_id

    strategy = _norm_strategy(payload.strategy)

    obs = RentObservation(
        property_id=payload.property_id,
        strategy=strategy,
        achieved_rent=float(payload.achieved_rent),
        tenant_portion=payload.tenant_portion,
        hap_portion=payload.hap_portion,
        lease_start=payload.lease_start,
        lease_end=payload.lease_end,
        notes=payload.notes,
        created_at=datetime.utcnow(),
    )
    db.add(obs)

    update_calibration_from_observation(
        db,
        property_row=prop,
        strategy=strategy,
        predicted_market_rent=ra.market_rent_estimate,
        achieved_rent=float(payload.achieved_rent),
    )

    db.commit()
    db.refresh(obs)
    return obs


@router.get("/calibration", response_model=list[RentCalibrationOut])
def list_calibration(
    zip: str | None = Query(default=None),
    bedrooms: int | None = Query(default=None),
    strategy: str | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    q = select(RentCalibration).order_by(RentCalibration.updated_at.desc())
    if zip:
        q = q.where(RentCalibration.zip == zip)
    if bedrooms is not None:
        q = q.where(RentCalibration.bedrooms == bedrooms)
    if strategy:
        q = q.where(RentCalibration.strategy == _norm_strategy(strategy))
    return db.execute(q).scalars().all()


@router.post("/recompute/{property_id}", response_model=RentRecomputeOut)
def recompute(
    property_id: int,
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    strategy = _norm_strategy(strategy)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    computed = recompute_rent_fields(
        db,
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=pct,
    )

    ra = db.execute(
        select(RentAssumption).where(
            RentAssumption.property_id == property_id,
            RentAssumption.org_id == p.org_id,
        )
    ).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    override = _to_pos_float(ra.approved_rent_ceiling)
    computed_ceiling = _to_pos_float(computed.get("approved_rent_ceiling"))
    computed_rent_used = computed.get("rent_used", None)

    if override is None and computed_ceiling is not None:
        ra.approved_rent_ceiling = float(computed_ceiling)

    ra.rent_used = float(computed_rent_used) if computed_rent_used is not None else None

    db.add(ra)
    db.commit()
    db.refresh(ra)

    return RentRecomputeOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        calibrated_market_rent=computed.get("calibrated_market_rent"),
        strategy=strategy,
        rent_used=ra.rent_used,
    )


@router.get("/explain/batch", response_model=RentExplainBatchOut)
def explain_rent_batch(
    snapshot_id: int = Query(...),
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    limit: int = Query(default=50, ge=1, le=500),
    persist: bool = Query(default=True),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    strategy = _norm_strategy(strategy)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    deals = db.scalars(
        select(Deal)
        .where(Deal.snapshot_id == snapshot_id, Deal.org_id == p.org_id)
        .limit(limit)
    ).all()

    attempted = len(deals)
    explained = 0
    errors: list[dict] = []

    for d in deals:
        try:
            pid = int(d.property_id)
            prop = db.get(Property, pid)
            if not prop or prop.org_id != p.org_id:
                continue

            _ = explain_rent(
                property_id=pid,
                strategy=strategy,
                payment_standard_pct=pct,
                persist=persist,
                db=db,
                p=p,
            )
            explained += 1
        except Exception as e:
            errors.append(
                {
                    "deal_id": getattr(d, "id", None),
                    "property_id": getattr(d, "property_id", None),
                    "error": f"{type(e).__name__}: {e}",
                }
            )

    return RentExplainBatchOut(
        snapshot_id=snapshot_id,
        strategy=strategy,
        attempted=attempted,
        explained=explained,
        errors=errors,
    )


@router.get("/explain/{property_id}", response_model=RentExplainOut)
def explain_rent(
    property_id: int,
    strategy: str = Query("section8"),
    payment_standard_pct: float | None = Query(default=None, ge=0.5, le=1.5),
    persist: bool = Query(default=True, description="If true, persist rent_used/approved ceiling when appropriate"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    prop = db.get(Property, property_id)
    if not prop or prop.org_id != p.org_id:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    ra.org_id = p.org_id

    strategy = _norm_strategy(strategy)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.default_payment_standard_pct)

    ceiling_candidates: list[dict] = []
    caps: list[float] = []

    fmr = _to_pos_float(ra.section8_fmr)
    fmr_adjusted: Optional[float] = None
    if fmr is not None:
        fmr_adjusted = float(fmr) * float(pct)
        caps.append(float(fmr_adjusted))
        ceiling_candidates.append({"type": "payment_standard", "value": float(fmr_adjusted)})

    rr = _to_pos_float(ra.rent_reasonableness_comp)
    if rr is not None:
        caps.append(float(rr))
        ceiling_candidates.append({"type": "rent_reasonableness", "value": float(rr)})

    computed_ceiling = min(caps) if caps else None

    manual = _to_pos_float(ra.approved_rent_ceiling)
    approved = manual if manual is not None else computed_ceiling

    market = _to_pos_float(ra.market_rent_estimate)

    rent_used: Optional[float]
    explanation: str
    cap_reason: str = "none"

    if strategy == "market":
        if market is None:
            rent_used = None
            explanation = "Market strategy: market_rent_estimate is missing, so rent_used cannot be computed."
        else:
            rent_used = float(market)
            explanation = "Market strategy uses market_rent_estimate (no Section 8 ceiling cap applied)."
    else:
        if market is None and approved is None:
            rent_used = None
            explanation = "Section 8 strategy: both market_rent_estimate and ceiling inputs are missing; cannot compute rent_used."
        elif market is None:
            rent_used = float(approved)  # type: ignore[arg-type]
            cap_reason = "ceiling_only"
            explanation = "Section 8 strategy: market_rent_estimate missing; using approved ceiling only."
        elif approved is None:
            rent_used = float(market)
            cap_reason = "market_only"
            explanation = "Section 8 strategy: ceiling missing; using market_rent_estimate only."
        else:
            rent_used = float(min(float(market), float(approved)))
            cap_reason = "capped" if float(market) > float(approved) else "uncapped"
            explanation = "Section 8 strategy caps rent by the strictest limit (approved ceiling vs market estimate)."

    explain_payload = {
        "property_id": property_id,
        "strategy": strategy,
        "payment_standard_pct": float(pct),
        "fmr_adjusted": fmr_adjusted,
        "market_rent_estimate": market,
        "section8_fmr": ra.section8_fmr,
        "rent_reasonableness_comp": ra.rent_reasonableness_comp,
        "approved_rent_ceiling": approved,
        "rent_used": rent_used,
        "ceiling_candidates": ceiling_candidates,
        "explanation": explanation,
        "cap_reason": cap_reason,
    }
    run = _persist_rent_explain_run(
        db,
        org_id=p.org_id,
        property_id=property_id,
        strategy=strategy,
        cap_reason=cap_reason,
        payment_standard_pct_used=float(pct),
        explain_payload=explain_payload,
    )

    if persist:
        ra.rent_used = float(rent_used) if rent_used is not None else None
        if manual is None and approved is not None:
            ra.approved_rent_ceiling = float(approved)
        db.add(ra)

    emit_workflow_event(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        event_type="rent_explained",
        property_id=property_id,
        payload={"property_id": property_id, "run_id": int(run.id), "strategy": strategy},
    )

    db.commit()

    return RentExplainOut(
        property_id=property_id,
        strategy=strategy,
        payment_standard_pct=float(pct),
        fmr_adjusted=fmr_adjusted,
        market_rent_estimate=market,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=approved,
        calibrated_market_rent=None,
        rent_used=rent_used,
        ceiling_candidates=ceiling_candidates,
        cap_reason=cap_reason,
        explanation=explanation,
        run_id=int(run.id),
        created_at=run.created_at,
    )
