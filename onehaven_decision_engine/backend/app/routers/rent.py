from __future__ import annotations

from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Property, RentAssumption, RentComp, RentObservation, RentCalibration
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
)
from ..domain.rent_learning import (
    get_or_create_rent_assumption,
    summarize_comps,
    update_calibration_from_observation,
    recompute_rent_fields,
    compute_approved_ceiling,
)

router = APIRouter(prefix="/rent", tags=["rent"])


@router.get("/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption(property_id: int, db: Session = Depends(get_db)):
    ra = db.execute(select(RentAssumption).where(RentAssumption.property_id == property_id)).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")
    return ra


@router.get("/assumption/{property_id}", response_model=RentAssumptionOut)
def get_rent_assumption_alias(property_id: int, db: Session = Depends(get_db)):
    return get_rent_assumption(property_id=property_id, db=db)


@router.post("/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)
    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(ra, k, v)

    db.add(ra)
    db.commit()
    db.refresh(ra)
    return ra


@router.post("/assumption/{property_id}", response_model=RentAssumptionOut)
def upsert_rent_assumption_alias(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    return upsert_rent_assumption(property_id=property_id, payload=payload, db=db)


@router.post("/comps/{property_id}", response_model=RentCompsSummaryOut)
def add_comps_batch(property_id: int, payload: RentCompsBatchIn, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, property_id)

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
def list_comps(property_id: int, db: Session = Depends(get_db)):
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
def add_rent_observation(payload: RentObservationCreate, db: Session = Depends(get_db)):
    prop = db.get(Property, payload.property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="property not found")

    ra = get_or_create_rent_assumption(db, payload.property_id)

    obs = RentObservation(
        property_id=payload.property_id,
        strategy=payload.strategy,
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
        strategy=payload.strategy,
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
):
    q = select(RentCalibration).order_by(RentCalibration.updated_at.desc())
    if zip:
        q = q.where(RentCalibration.zip == zip)
    if bedrooms is not None:
        q = q.where(RentCalibration.bedrooms == bedrooms)
    if strategy:
        q = q.where(RentCalibration.strategy == strategy)
    return db.execute(q).scalars().all()


@router.post("/recompute/{property_id}", response_model=RentRecomputeOut)
def recompute(
    property_id: int,
    strategy: str = Query(default="section8"),
    payment_standard_pct: float | None = None,
    db: Session = Depends(get_db),
):
    try:
        computed = recompute_rent_fields(
            db,
            property_id=property_id,
            strategy=strategy,
            payment_standard_pct=payment_standard_pct,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    ra = db.execute(select(RentAssumption).where(RentAssumption.property_id == property_id)).scalar_one_or_none()
    if not ra:
        raise HTTPException(status_code=404, detail="rent assumption not found")

    # Persist computed approved ceiling only if user didn't manually override it.
    if ra.approved_rent_ceiling is None:
        ra.approved_rent_ceiling = computed["approved_rent_ceiling"]

    # ALWAYS persist rent_used; underwriting consumes this.
    ra.rent_used = computed["rent_used"]

    db.add(ra)
    db.commit()
    db.refresh(ra)

    return RentRecomputeOut(
        property_id=property_id,
        market_rent_estimate=ra.market_rent_estimate,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=ra.approved_rent_ceiling,
        calibrated_market_rent=computed["calibrated_market_rent"],
        strategy=strategy,
        rent_used=ra.rent_used,
    )


@router.get("/explain/{property_id}", response_model=RentExplainOut)
def explain_rent(
    property_id: int,
    strategy: str = Query("section8"),
    payment_standard_pct: float = Query(1.0),
    db: Session = Depends(get_db),
):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(404, "property not found")

    ra = get_or_create_rent_assumption(db, property_id)

    strategy = (strategy or "section8").strip().lower()
    if strategy not in {"section8", "market"}:
        strategy = "section8"

    ceiling_candidates: list[dict] = []
    caps: list[float] = []

    # payment standard candidate (FMR * pct)
    if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
        ps = float(ra.section8_fmr) * float(payment_standard_pct)
        caps.append(ps)
        ceiling_candidates.append({"type": "payment_standard", "value": ps})

    # rent reasonableness candidate (median comps)
    if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
        rr = float(ra.rent_reasonableness_comp)
        caps.append(rr)
        ceiling_candidates.append({"type": "rent_reasonableness", "value": rr})

    computed_ceiling = min(caps) if caps else None

    # approved ceiling: manual override wins
    approved = (
        float(ra.approved_rent_ceiling)
        if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0
        else computed_ceiling
    )

    market = float(ra.market_rent_estimate) if ra.market_rent_estimate is not None else None

    if strategy == "market":
        rent_used = market
        explanation = "Market strategy uses the market rent estimate (no Section 8 ceiling cap applied)."
    else:
        # section8
        if market is not None and approved is not None:
            rent_used = float(min(market, approved))
            explanation = "Section 8 strategy caps rent by the strictest limit (approved ceiling vs market estimate)."
        elif approved is not None:
            rent_used = approved
            explanation = "Section 8 strategy: market estimate missing; using approved ceiling only."
        else:
            rent_used = market
            explanation = "Section 8 strategy: ceiling missing; using market estimate only."

    # persist computed fields (optional but useful for later)
    ra.rent_used = rent_used
    if ra.approved_rent_ceiling is None and approved is not None:
        ra.approved_rent_ceiling = approved
    db.commit()

    return RentExplainOut(
        property_id=property_id,
        strategy=strategy,
        market_rent_estimate=market,
        section8_fmr=ra.section8_fmr,
        rent_reasonableness_comp=ra.rent_reasonableness_comp,
        approved_rent_ceiling=approved,
        rent_used=rent_used,
        explanation=explanation,
        ceiling_candidates=ceiling_candidates,
    )

